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

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

/**
 * A buffer pool used to manage a number of {@link Buffer} instances from the
 * {@link NetworkBufferPool}.
 * <p>
 * Buffer requests are mediated to the network buffer pool to ensure dead-lock
 * free operation of the network stack by limiting the number of buffers per
 * local buffer pool. It also implements the default mechanism for buffer
 * recycling, which ensures that every buffer is ultimately returned to the
 * network buffer pool.
 *
 * <p> The size of this pool can be dynamically changed at runtime ({@link #setNumBuffers(int)}. It
 * will then lazily return the required number of buffers to the {@link NetworkBufferPool} to
 * match its new size.
 */
class LocalBufferPool implements BufferPool {

	private final NetworkBufferPool networkBufferPool;

	// The minimum number of required segments for this pool
	private final int numberOfRequiredMemorySegments;

	// The currently available memory segments. These are segments, which have been requested from
	// the network buffer pool and are currently not handed out as Buffer instances.
	private final Queue<MemorySegment> availableMemorySegments = new ArrayDeque<MemorySegment>();

	// Buffer availability listeners, which need to be notified when a Buffer becomes available.
	// Listeners can only be registered at a time/state where no Buffer instance was available.
	private final Queue<EventListener<Buffer>> registeredListeners = new ArrayDeque<EventListener<Buffer>>();

	// The current size of this pool
	private int currentPoolSize;

	// Number of all memory segments, which have been requested from the network buffer pool and are
	// somehow referenced through this pool (e.g. wrapped in Buffer instances or as available segments).
	private int numberOfRequestedMemorySegments;

	private boolean isDestroyed;

	private BufferPoolOwner owner;

	LocalBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments) {
		this.networkBufferPool = networkBufferPool;
		this.numberOfRequiredMemorySegments = numberOfRequiredMemorySegments;
		this.currentPoolSize = numberOfRequiredMemorySegments;
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
	public int getNumberOfAvailableMemorySegments() {
		synchronized (availableMemorySegments) {
			return availableMemorySegments.size();
		}
	}

	@Override
	public int getNumBuffers() {
		synchronized (availableMemorySegments) {
			return currentPoolSize;
		}
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
		}
		catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public Buffer requestBufferBlocking() throws IOException, InterruptedException {
		return requestBuffer(true);
	}

	private Buffer requestBuffer(boolean isBlocking) throws InterruptedException, IOException {
		synchronized (availableMemorySegments) {
			returnExcessMemorySegments();

			boolean askToRecycle = owner != null;

			while (availableMemorySegments.isEmpty()) {
				if (isDestroyed) {
					throw new IllegalStateException("Buffer pool is destroyed.");
				}

				if (numberOfRequestedMemorySegments < currentPoolSize) {
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
				}
				else {
					return null;
				}
			}

			return new Buffer(availableMemorySegments.poll(), this);
		}
	}

	@Override
	public void recycle(MemorySegment segment) {
		synchronized (availableMemorySegments) {
			if (isDestroyed || numberOfRequestedMemorySegments > currentPoolSize) {
				returnMemorySegment(segment);
			}
			else {
				EventListener<Buffer> listener = registeredListeners.poll();

				if (listener == null) {
					availableMemorySegments.add(segment);
					availableMemorySegments.notify();
				}
				else {
					try {
						listener.onEvent(new Buffer(segment, this));
					}
					catch (Throwable ignored) {
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

				EventListener<Buffer> listener;
				while ((listener = registeredListeners.poll()) != null) {
					listener.onEvent(null);
				}

				isDestroyed = true;
			}
		}

		networkBufferPool.destroyBufferPool(this);
	}

	@Override
	public boolean addListener(EventListener<Buffer> listener) {
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
		synchronized (availableMemorySegments) {
			checkArgument(numBuffers >= numberOfRequiredMemorySegments, "Buffer pool needs at least " + numberOfRequiredMemorySegments + " buffers, but tried to set to " + numBuffers + ".");

			currentPoolSize = numBuffers;

			returnExcessMemorySegments();

			// If there is a registered owner and we have still requested more buffers than our
			// size, trigger a recycle via the owner.
			if (owner != null && numberOfRequestedMemorySegments > currentPoolSize) {
				owner.releaseMemory(numberOfRequestedMemorySegments - numBuffers);
			}
		}
	}

	@Override
	public String toString() {
		synchronized (availableMemorySegments) {
			return String.format("[size: %d, required: %d, requested: %d, available: %d, listeners: %d, destroyed: %s]", currentPoolSize, numberOfRequiredMemorySegments, numberOfRequestedMemorySegments, availableMemorySegments.size(), registeredListeners.size(), isDestroyed);
		}
	}

	// ------------------------------------------------------------------------

	private void returnMemorySegment(MemorySegment segment) {
		numberOfRequestedMemorySegments--;
		networkBufferPool.recycle(segment);
	}

	private void returnExcessMemorySegments() {
		while (numberOfRequestedMemorySegments > currentPoolSize) {
			MemorySegment segment = availableMemorySegments.poll();
			if (segment == null) {
				return;
			}

			networkBufferPool.recycle(segment);
			numberOfRequestedMemorySegments--;
		}
	}

}
