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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A buffer pool used to manage a number of {@link Buffer} instances from the
 * {@link NetworkBufferPool}.
 *
 * <p> Compared with {@link LocalBufferPool}, this is a fixed size (non-rebalancing) buffer pool
 * type which will not be involved in distributing the left available buffers in network buffer pool.
 *
 * <p> This buffer pool can be used to manage the floating buffers for input gate.
 */
class FixedBufferPool implements BufferPool {
	private static final Logger LOG = LoggerFactory.getLogger(FixedBufferPool.class);

	/** Global network buffer pool to get buffers from. */
	private final NetworkBufferPool networkBufferPool;

	/** The required number of segments for this pool. */
	private final int numberOfRequiredMemorySegments;

	/**
	 * The currently available memory segments. These are segments, which have been requested from
	 * the network buffer pool and are currently not handed out as Buffer instances.
	 */
	private final Queue<MemorySegment> availableMemorySegments = new ArrayDeque<>();

	/**
	 * Buffer availability listeners, which need to be notified when a Buffer becomes available.
	 * Listeners can only be registered at a time/state where no Buffer instance was available.
	 */
	private final Queue<BufferPoolListener> registeredListeners = new ArrayDeque<>();

	/**
	 * Number of all memory segments, which have been requested from the network buffer pool and are
	 * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available segments).
	 */
	private int numberOfRequestedMemorySegments;

	private boolean isDestroyed;

	private BufferPoolOwner owner;

	/**
	 * Fixed buffer pool based on the given <tt>networkBufferPool</tt> with a number of
	 * network buffers being available.
	 *
	 * @param networkBufferPool
	 * 		global network buffer pool to get buffers from
	 * @param numberOfRequiredMemorySegments
	 * 		fixed number of network buffers
	 */
	FixedBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments) {
		checkArgument(numberOfRequiredMemorySegments >= 0,
			"Required number of memory segments (%s) should not be less than 0.",
			numberOfRequiredMemorySegments);

		LOG.debug("Using a fixed buffer pool with {} buffers", numberOfRequiredMemorySegments);

		this.networkBufferPool = networkBufferPool;
		this.numberOfRequiredMemorySegments = numberOfRequiredMemorySegments;
	}

	// ------------------------------------------------------------------------
	// Properties
	// ------------------------------------------------------------------------

	@Override
	public boolean isDestroyed() {
		synchronized (availableMemorySegments) {
			return isDestroyed;
		}
	}

	@Override
	public int getMemorySegmentSize() {
		return networkBufferPool.getMemorySegmentSize();
	}

	@Override
	public int getNumberOfRequiredMemorySegments() {
		return numberOfRequiredMemorySegments;
	}

	@Override
	public int getMaxNumberOfMemorySegments() {
		return numberOfRequiredMemorySegments;
	}

	@Override
	public int getNumberOfAvailableMemorySegments() {
		synchronized (availableMemorySegments) {
			return availableMemorySegments.size();
		}
	}

	@Override
	public int getNumBuffers() {
		return numberOfRequiredMemorySegments;
	}

	@Override
	public int bestEffortGetNumOfUsedBuffers() {
		return Math.max(0, numberOfRequestedMemorySegments - availableMemorySegments.size());
	}

	@Override
	public void setBufferPoolOwner(BufferPoolOwner owner) {
		synchronized (availableMemorySegments) {
			checkState(this.owner == null, "Buffer pool owner has already been set.");
			this.owner = checkNotNull(owner);
		}
	}

	@Override
	public Buffer requestBuffer() throws IOException {
		try {
			return requestBuffer(false);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public Buffer requestBufferBlocking() throws IOException, InterruptedException {
		return requestBuffer(true);
	}

	private Buffer requestBuffer(boolean isBlocking) throws InterruptedException, IOException {
		synchronized (availableMemorySegments) {
			boolean askToRecycle = owner != null;

			while (availableMemorySegments.isEmpty()) {
				if (isDestroyed) {
					throw new IllegalStateException("Buffer pool is destroyed.");
				}

				if (numberOfRequestedMemorySegments < numberOfRequiredMemorySegments) {
					final MemorySegment segment = networkBufferPool.requestMemorySegment();

					if (segment != null) {
						numberOfRequestedMemorySegments++;
						availableMemorySegments.add(segment);

						continue;
					}
				}

				if (askToRecycle) {
					owner.releaseMemory(1);
				}

				if (isBlocking) {
					availableMemorySegments.wait(2000);
				} else {
					return null;
				}
			}

			return new Buffer(availableMemorySegments.poll(), this);
		}
	}

	@Override
	public void recycle(MemorySegment segment) {
		synchronized (availableMemorySegments) {
			if (isDestroyed) {
				returnMemorySegment(segment);
			} else {
				BufferPoolListener listener = registeredListeners.poll();
				if (listener == null) {
					availableMemorySegments.add(segment);
					availableMemorySegments.notify();
				} else {
					try {
						boolean needMoreBuffers = listener.notifyBufferAvailable(new Buffer(segment, this));
						if (needMoreBuffers) {
							registeredListeners.add(listener);
						}
					} catch (Throwable ignored) {
						availableMemorySegments.add(segment);
						availableMemorySegments.notify();
					}
				}
			}
		}
	}

	/**
	 * Destroy is called after the produce or consume phase of a task finishes.
	 */
	@Override
	public void lazyDestroy() {
		synchronized (availableMemorySegments) {
			if (!isDestroyed) {
				MemorySegment segment;
				while ((segment = availableMemorySegments.poll()) != null) {
					returnMemorySegment(segment);
				}

				registeredListeners.clear();

				isDestroyed = true;
			}
		}

		networkBufferPool.destroyBufferPool(this);
	}

	@Override
	public boolean addListener(EventListener<Buffer> listener) {
		return false;
	}

	@Override
	public boolean addBufferPoolListener(BufferPoolListener listener) {
		synchronized (availableMemorySegments) {
			if (!availableMemorySegments.isEmpty() || isDestroyed) {
				return false;
			}

			registeredListeners.add(listener);
			return true;
		}
	}

	@Override
	public void setNumBuffers(int numBuffers) throws IOException {
		checkArgument(numBuffers == numberOfRequiredMemorySegments,
			"Buffer pool needs a fixed size of %s buffers, but tried to set to %s",
			numberOfRequiredMemorySegments, numBuffers);
	}

	@Override
	public String toString() {
		synchronized (availableMemorySegments) {
			return String.format("[required: %d, requested: %d, available: %d, listeners: %d, destroyed: %s]",
				numberOfRequiredMemorySegments, numberOfRequestedMemorySegments,
				availableMemorySegments.size(), registeredListeners.size(), isDestroyed);
		}
	}

	// ------------------------------------------------------------------------

	private void returnMemorySegment(MemorySegment segment) {
		numberOfRequestedMemorySegments--;
		networkBufferPool.recycle(segment);
	}

}
