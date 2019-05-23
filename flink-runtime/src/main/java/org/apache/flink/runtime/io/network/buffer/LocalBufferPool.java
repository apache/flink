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
import org.apache.flink.runtime.io.network.buffer.BufferListener.NotificationResult;
import org.apache.flink.util.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * A buffer pool used to manage a number of {@link Buffer} instances from the
 * {@link NetworkBufferPool}.
 *
 * <p>Buffer requests are mediated to the network buffer pool to ensure dead-lock
 * free operation of the network stack by limiting the number of buffers per
 * local buffer pool. It also implements the default mechanism for buffer
 * recycling, which ensures that every buffer is ultimately returned to the
 * network buffer pool.
 *
 * <p>The size of this pool can be dynamically changed at runtime ({@link #setNumBuffers(int)}. It
 * will then lazily return the required number of buffers to the {@link NetworkBufferPool} to
 * match its new size.
 */
class LocalBufferPool implements BufferPool {
	private static final Logger LOG = LoggerFactory.getLogger(LocalBufferPool.class);

	/** Global network buffer pool to get buffers from. */
	private final NetworkBufferPool networkBufferPool;

	/** The minimum number of required segments for this pool. */
	private final int numberOfRequiredMemorySegments;

	/**
	 * The currently available memory segments. These are segments, which have been requested from
	 * the network buffer pool and are currently not handed out as Buffer instances.
	 *
	 * <p><strong>BEWARE:</strong> Take special care with the interactions between this lock and
	 * locks acquired before entering this class vs. locks being acquired during calls to external
	 * code inside this class, e.g. with
	 * {@link org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel#bufferQueue}
	 * via the {@link #registeredListeners} callback.
	 */
	private final ArrayDeque<MemorySegment> availableMemorySegments = new ArrayDeque<MemorySegment>();

	/**
	 * Buffer availability listeners, which need to be notified when a Buffer becomes available.
	 * Listeners can only be registered at a time/state where no Buffer instance was available.
	 */
	private final ArrayDeque<BufferListener> registeredListeners = new ArrayDeque<>();

	/** Maximum number of network buffers to allocate. */
	private final int maxNumberOfMemorySegments;

	/** The current size of this pool. */
	private int currentPoolSize;

	/**
	 * Number of all memory segments, which have been requested from the network buffer pool and are
	 * somehow referenced through this pool (e.g. wrapped in Buffer instances or as available segments).
	 */
	private int numberOfRequestedMemorySegments;

	private boolean isDestroyed;

	private final Optional<BufferPoolOwner> owner;

	/**
	 * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal number of
	 * network buffers being available.
	 *
	 * @param networkBufferPool
	 * 		global network buffer pool to get buffers from
	 * @param numberOfRequiredMemorySegments
	 * 		minimum number of network buffers
	 */
	LocalBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments) {
		this(networkBufferPool, numberOfRequiredMemorySegments, Integer.MAX_VALUE, Optional.empty());
	}

	/**
	 * Local buffer pool based on the given <tt>networkBufferPool</tt> with a minimal and maximal
	 * number of network buffers being available.
	 *
	 * @param networkBufferPool
	 * 		global network buffer pool to get buffers from
	 * @param numberOfRequiredMemorySegments
	 * 		minimum number of network buffers
	 * @param maxNumberOfMemorySegments
	 * 		maximum number of network buffers to allocate
	 */
	LocalBufferPool(NetworkBufferPool networkBufferPool, int numberOfRequiredMemorySegments,
			int maxNumberOfMemorySegments) {
		this(networkBufferPool, numberOfRequiredMemorySegments, maxNumberOfMemorySegments, Optional.empty());
	}

	/**
	 * Local buffer pool based on the given <tt>networkBufferPool</tt> and <tt>bufferPoolOwner</tt>
	 * with a minimal and maximal number of network buffers being available.
	 *
	 * @param networkBufferPool
	 * 		global network buffer pool to get buffers from
	 * @param numberOfRequiredMemorySegments
	 * 		minimum number of network buffers
	 * @param maxNumberOfMemorySegments
	 * 		maximum number of network buffers to allocate
	 * 	@param owner
	 * 		the optional owner of this buffer pool to release memory when needed
	 */
	LocalBufferPool(
		NetworkBufferPool networkBufferPool,
		int numberOfRequiredMemorySegments,
		int maxNumberOfMemorySegments,
		Optional<BufferPoolOwner> owner) {
		checkArgument(maxNumberOfMemorySegments >= numberOfRequiredMemorySegments,
			"Maximum number of memory segments (%s) should not be smaller than minimum (%s).",
			maxNumberOfMemorySegments, numberOfRequiredMemorySegments);

		checkArgument(maxNumberOfMemorySegments > 0,
			"Maximum number of memory segments (%s) should be larger than 0.",
			maxNumberOfMemorySegments);

		LOG.debug("Using a local buffer pool with {}-{} buffers",
			numberOfRequiredMemorySegments, maxNumberOfMemorySegments);

		this.networkBufferPool = networkBufferPool;
		this.numberOfRequiredMemorySegments = numberOfRequiredMemorySegments;
		this.currentPoolSize = numberOfRequiredMemorySegments;
		this.maxNumberOfMemorySegments = maxNumberOfMemorySegments;
		this.owner = owner;
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
		return maxNumberOfMemorySegments;
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
	public int bestEffortGetNumOfUsedBuffers() {
		return Math.max(0, numberOfRequestedMemorySegments - availableMemorySegments.size());
	}

	@Override
	public Buffer requestBuffer() throws IOException {
		try {
			return toBuffer(requestMemorySegment(false));
		}
		catch (InterruptedException e) {
			throw new IOException(e);
		}
	}

	@Override
	public Buffer requestBufferBlocking() throws IOException, InterruptedException {
		return toBuffer(requestMemorySegment(true));
	}

	@Override
	public BufferBuilder requestBufferBuilderBlocking() throws IOException, InterruptedException {
		return toBufferBuilder(requestMemorySegment(true));
	}

	private Buffer toBuffer(MemorySegment memorySegment) {
		if (memorySegment == null) {
			return null;
		}
		return new NetworkBuffer(memorySegment, this);
	}

	private BufferBuilder toBufferBuilder(MemorySegment memorySegment) {
		if (memorySegment == null) {
			return null;
		}
		return new BufferBuilder(memorySegment, this);
	}

	private MemorySegment requestMemorySegment(boolean isBlocking) throws InterruptedException, IOException {
		synchronized (availableMemorySegments) {
			returnExcessMemorySegments();

			boolean askToRecycle = owner.isPresent();

			// fill availableMemorySegments with at least one element, wait if required
			while (availableMemorySegments.isEmpty()) {
				if (isDestroyed) {
					throw new IllegalStateException("Buffer pool is destroyed.");
				}

				if (numberOfRequestedMemorySegments < currentPoolSize) {
					final MemorySegment segment = networkBufferPool.requestMemorySegment();

					if (segment != null) {
						numberOfRequestedMemorySegments++;
						return segment;
					}
				}

				if (askToRecycle) {
					owner.get().releaseMemory(1);
				}

				if (isBlocking) {
					availableMemorySegments.wait(2000);
				}
				else {
					return null;
				}
			}

			return availableMemorySegments.poll();
		}
	}

	@Override
	public void recycle(MemorySegment segment) {
		BufferListener listener;
		NotificationResult notificationResult = NotificationResult.BUFFER_NOT_USED;
		while (!notificationResult.isBufferUsed()) {
			synchronized (availableMemorySegments) {
				if (isDestroyed || numberOfRequestedMemorySegments > currentPoolSize) {
					returnMemorySegment(segment);
					return;
				} else {
					listener = registeredListeners.poll();
					if (listener == null) {
						availableMemorySegments.add(segment);
						availableMemorySegments.notify();
						return;
					}
				}
			}
			notificationResult = fireBufferAvailableNotification(listener, segment);
		}
	}

	private NotificationResult fireBufferAvailableNotification(BufferListener listener, MemorySegment segment) {
		// We do not know which locks have been acquired before the recycle() or are needed in the
		// notification and which other threads also access them.
		// -> call notifyBufferAvailable() outside of the synchronized block to avoid a deadlock (FLINK-9676)
		NotificationResult notificationResult = listener.notifyBufferAvailable(new NetworkBuffer(segment, this));
		if (notificationResult.needsMoreBuffers()) {
			synchronized (availableMemorySegments) {
				if (isDestroyed) {
					// cleanup tasks how they would have been done if we only had one synchronized block
					listener.notifyBufferDestroyed();
				} else {
					registeredListeners.add(listener);
				}
			}
		}
		return notificationResult;
	}

	/**
	 * Destroy is called after the produce or consume phase of a task finishes.
	 */
	@Override
	public void lazyDestroy() {
		// NOTE: if you change this logic, be sure to update recycle() as well!
		synchronized (availableMemorySegments) {
			if (!isDestroyed) {
				MemorySegment segment;
				while ((segment = availableMemorySegments.poll()) != null) {
					returnMemorySegment(segment);
				}

				BufferListener listener;
				while ((listener = registeredListeners.poll()) != null) {
					listener.notifyBufferDestroyed();
				}

				isDestroyed = true;
			}
		}

		try {
			networkBufferPool.destroyBufferPool(this);
		} catch (IOException e) {
			ExceptionUtils.rethrow(e);
		}
	}

	@Override
	public boolean addBufferListener(BufferListener listener) {
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
		int numExcessBuffers;
		synchronized (availableMemorySegments) {
			checkArgument(numBuffers >= numberOfRequiredMemorySegments,
					"Buffer pool needs at least %s buffers, but tried to set to %s",
					numberOfRequiredMemorySegments, numBuffers);

			if (numBuffers > maxNumberOfMemorySegments) {
				currentPoolSize = maxNumberOfMemorySegments;
			} else {
				currentPoolSize = numBuffers;
			}

			returnExcessMemorySegments();

			numExcessBuffers = numberOfRequestedMemorySegments - currentPoolSize;
		}

		// If there is a registered owner and we have still requested more buffers than our
		// size, trigger a recycle via the owner.
		if (owner.isPresent() && numExcessBuffers > 0) {
			owner.get().releaseMemory(numExcessBuffers);
		}
	}

	@Override
	public String toString() {
		synchronized (availableMemorySegments) {
			return String.format(
				"[size: %d, required: %d, requested: %d, available: %d, max: %d, listeners: %d, destroyed: %s]",
				currentPoolSize, numberOfRequiredMemorySegments, numberOfRequestedMemorySegments,
				availableMemorySegments.size(), maxNumberOfMemorySegments, registeredListeners.size(), isDestroyed);
		}
	}

	// ------------------------------------------------------------------------

	private void returnMemorySegment(MemorySegment segment) {
		assert Thread.holdsLock(availableMemorySegments);

		numberOfRequestedMemorySegments--;
		networkBufferPool.recycle(segment);
	}

	private void returnExcessMemorySegments() {
		assert Thread.holdsLock(availableMemorySegments);

		while (numberOfRequestedMemorySegments > currentPoolSize) {
			MemorySegment segment = availableMemorySegments.poll();
			if (segment == null) {
				return;
			}

			returnMemorySegment(segment);
		}
	}
}
