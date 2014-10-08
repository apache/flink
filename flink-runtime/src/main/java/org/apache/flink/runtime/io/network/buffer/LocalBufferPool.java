/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.buffer;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.runtime.util.event.EventListener;

import java.util.ArrayDeque;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A buffer pool used to manage a number of {@link Buffer} instances from the
 * {@link NetworkBufferPool}.
 * <p/>
 * Buffer requests are mediated to the network buffer pool to ensure dead-lock
 * free operation of the network stack by limiting the number of buffers per
 * local buffer pool. It also implements the default mechanism for buffer
 * recycling, which ensures that every buffer is ultimately returned to the
 * network buffer pool.
 * <p/>
 * Instance of a local buffer pool should only be created by the network buffer
 * pool to ensure that enough buffers are available {@link BufferPoolFactory}.
 * <p/>
 * This class is thread-safe.
 */
class LocalBufferPool implements BufferPool {

	private final Queue<MemorySegment> availableMemorySegments = new ArrayDeque<MemorySegment>();

	private final NetworkBufferPool networkBufferPool;

	private final int memorySegmentSize;

	private final Queue<BufferFuture> pendingBufferRequests = new ArrayDeque<BufferFuture>();

	private final MemorySegmentListener memorySegmentListener = new MemorySegmentListener();

	private boolean isDestroyed;

	private int numRequiredBuffers;

	private int numBuffers;

	private int numRequestedBuffers;

	private BufferPoolOwner owner;

	LocalBufferPool(NetworkBufferPool networkBufferPool, int numRequiredBuffers) {
		this.networkBufferPool = networkBufferPool;
		this.memorySegmentSize = networkBufferPool.getMemorySegmentSize();
		this.numRequiredBuffers = numRequiredBuffers;
		this.numBuffers = numRequiredBuffers;
	}

	@Override
	public void setBufferPoolOwner(BufferPoolOwner owner) {
		checkState(owner != null, "Buffer pool owner has already been set.");
		this.owner = checkNotNull(owner);
	}

	@Override
	public BufferFuture requestBuffer() {
		return requestBuffer(memorySegmentSize);
	}

	@Override
	public BufferFuture requestBuffer(int size) {
		checkBufferRequestSize(size);

		synchronized (availableMemorySegments) {
			if (isDestroyed) {
				return null;
			}

			// ----------------------------------------------------------------
			// (1) Check local pool
			// ----------------------------------------------------------------
			MemorySegment segment = availableMemorySegments.poll();

			if (segment != null) {
				return new BufferFuture(new Buffer(segment, size, this));
			}

			// ----------------------------------------------------------------
			// (2) Check memory segment pool
			// ----------------------------------------------------------------
			MemorySegmentFuture request = null;

			if (numRequestedBuffers < numBuffers) {
				request = networkBufferPool.requestMemorySegment();
				numRequestedBuffers++;

				segment = request.getMemorySegment();

				if (segment != null) {
					return new BufferFuture(new Buffer(segment, size, this));
				}
			}

			BufferFuture future = new BufferFuture(size);
			pendingBufferRequests.add(future);

			if (owner != null) {
				owner.recycleBuffers(1);
			}

			if (request != null) {
				// Add the listener at this point after adding the pending
				// buffer request to ensure that it is notified after if a
				// memory segment became available in the mean time.
				request.addListener(memorySegmentListener);
			}

			return future;
		}
	}

	@Override
	public void recycle(MemorySegment memorySegment) {
		synchronized (availableMemorySegments) {
			if (isDestroyed) {
				networkBufferPool.recycle(memorySegment);
				numRequestedBuffers--;
			}
			else {
				// If the number of pooled buffers changed in the meantime, make sure
				// to return the recycled MemorySegment to its MemorySegmentPool.
				if (numRequestedBuffers > numBuffers) {
					networkBufferPool.recycle(memorySegment);
					numRequestedBuffers--;
				}
				else if (!pendingBufferRequests.isEmpty()) {
					BufferFuture future = pendingBufferRequests.poll();
					if (future.isCancelled()) {
						recycle(memorySegment);
					}
					else {
						future.handInBuffer(new Buffer(memorySegment, future.getBufferSize(), this));
					}
				}
				else {
					availableMemorySegments.add(memorySegment);
				}
			}
		}
	}

	public void destroy() {
		synchronized (availableMemorySegments) {
			checkState(!isDestroyed, "Buffer pool already destroyed.");

			if (owner != null) {
				owner.recycleAllBuffers();
			}

			while (!availableMemorySegments.isEmpty()) {
				networkBufferPool.recycle(availableMemorySegments.remove());
				numRequestedBuffers--;
			}

			while (!pendingBufferRequests.isEmpty()) {
				pendingBufferRequests.poll().cancel();
			}

			isDestroyed = true;
		}

		// Don't do this in the synchronized block, because there is circular
		// interaction between the local and network buffer pool, which might
		// otherwise lead to deadlocks.
		networkBufferPool.destroyBufferPool(this);
	}

	public boolean isDestroyed() {
		synchronized (availableMemorySegments) {
			return isDestroyed;
		}
	}

	@Override
	public int getNumRequiredBuffers() {
		return numRequiredBuffers;
	}

	public int getNumAvailableBuffers() {
		synchronized (availableMemorySegments) {
			return availableMemorySegments.size();
		}
	}

	public int getNumBuffers() {
		synchronized (availableMemorySegments) {
			return numBuffers;
		}
	}

	public void setNumBuffers(int numBuffers) {
		synchronized (availableMemorySegments) {
			checkArgument(numBuffers >= numRequiredBuffers, "Buffer pool needs at least " + numRequiredBuffers + " buffers, but tried to set to " + numBuffers + ".");

			this.numBuffers = numBuffers;

			// Return excess buffers to the MemorySegmentPool
			while (numRequestedBuffers > numBuffers) {
				final MemorySegment segment = availableMemorySegments.poll();
				if (segment == null) {
					break;
				}

				networkBufferPool.recycle(segment);
				numRequestedBuffers--;
			}

			if (numRequestedBuffers > numBuffers) {
				owner.recycleBuffers(numRequestedBuffers - numBuffers);
			}

			while (numRequestedBuffers < numBuffers && !pendingBufferRequests.isEmpty()) {
				BufferFuture future = pendingBufferRequests.poll();
				if (future.isDone()) {
					continue;
				}

				MemorySegmentFuture request = networkBufferPool.requestMemorySegment();
				numRequestedBuffers++;

				MemorySegment segment = request.getMemorySegment();

				if (segment != null) {
					future.handInBuffer(new Buffer(segment, future.getBufferSize(), this));
				}
				else {
					request.addListener(memorySegmentListener);
				}
			}
		}
	}

	private void checkBufferRequestSize(int requestedSize) {
		if (requestedSize <= 0 || requestedSize > memorySegmentSize) {
			throw new IllegalArgumentException("Requested too large buffer (requested: "
					+ requestedSize + ", maximum: " + memorySegmentSize + ").");
		}
	}

	// ------------------------------------------------------------------------

	private class MemorySegmentListener implements EventListener<MemorySegmentFuture> {

		@Override
		public void onEvent(MemorySegmentFuture future) {
			if (future.isCancelled()) {
				return;
			}

			if (future.isSuccess()) {
				synchronized (availableMemorySegments) {
					MemorySegment segment = future.getMemorySegment();

					if (numRequestedBuffers > numBuffers) {
						networkBufferPool.recycle(segment);
						numRequestedBuffers--;
					}
					else {
						BufferFuture request = pendingBufferRequests.poll();
						if (request != null) {
							request.handInBuffer(new Buffer(segment, request.getBufferSize(), LocalBufferPool.this));
						}
						else {
							numRequestedBuffers--;
							networkBufferPool.recycle(segment);
						}
					}
				}
			}
		}
	}

	@Override
	public String toString() {
		synchronized (availableMemorySegments) {
			return String.format("LocalBufferPool %s [number of buffers: %d, required: %d, " +
							"available: %d, requested: %d, pending requests: %d, destroyed: %s]",
					owner, numBuffers, numRequiredBuffers, numRequestedBuffers,
					availableMemorySegments.size(), pendingBufferRequests.size(), isDestroyed);
		}
	}
}
