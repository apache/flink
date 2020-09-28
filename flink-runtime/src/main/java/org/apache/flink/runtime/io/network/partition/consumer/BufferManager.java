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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentProvider;
import org.apache.flink.runtime.execution.CancelTaskException;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The general buffer manager used by {@link InputChannel} to request/recycle
 * exclusive or floating buffers.
 */
public class BufferManager implements BufferListener, BufferRecycler {

	/** The available buffer queue wraps both exclusive and requested floating buffers. */
	private final AvailableBufferQueue bufferQueue = new AvailableBufferQueue();

	/** The buffer provider for requesting exclusive buffers. */
	private final MemorySegmentProvider globalPool;

	/** The input channel to own this buffer manager. */
	private final InputChannel inputChannel;

	/** The tag indicates whether it is waiting for additional floating buffers from the buffer pool. */
	@GuardedBy("bufferQueue")
	private boolean isWaitingForFloatingBuffers;

	/** The total number of required buffers for the respective input channel. */
	@GuardedBy("bufferQueue")
	private int numRequiredBuffers;

	public BufferManager(
		MemorySegmentProvider globalPool,
		InputChannel inputChannel,
		int numRequiredBuffers) {

		this.globalPool = checkNotNull(globalPool);
		this.inputChannel = checkNotNull(inputChannel);
		checkArgument(numRequiredBuffers >= 0);
		this.numRequiredBuffers = numRequiredBuffers;
	}

	// ------------------------------------------------------------------------
	// Buffer request
	// ------------------------------------------------------------------------

	@Nullable
	Buffer requestBuffer() {
		synchronized (bufferQueue) {
			return bufferQueue.takeBuffer();
		}
	}

	Buffer requestBufferBlocking() throws InterruptedException {
		synchronized (bufferQueue) {
			Buffer buffer;
			while ((buffer = bufferQueue.takeBuffer()) == null) {
				if (inputChannel.isReleased()) {
					throw new CancelTaskException("Input channel [" + inputChannel.channelInfo + "] has already been released.");
				}
				if (!isWaitingForFloatingBuffers) {
					BufferPool bufferPool = inputChannel.inputGate.getBufferPool();
					buffer = bufferPool.requestBuffer();
					if (buffer == null && shouldContinueRequest(bufferPool)) {
						continue;
					}
				}

				if (buffer != null) {
					return buffer;
				}
				bufferQueue.wait();
			}
			return buffer;
		}
	}

	private boolean shouldContinueRequest(BufferPool bufferPool) {
		if (bufferPool.addBufferListener(this)) {
			isWaitingForFloatingBuffers = true;
			numRequiredBuffers = 1;
			return false;
		} else if (bufferPool.isDestroyed()) {
			throw new CancelTaskException("Local buffer pool has already been released.");
		} else {
			return true;
		}
	}

	/**
	 * Requests exclusive buffers from the provider.
	 */
	void requestExclusiveBuffers(int numExclusiveBuffers) throws IOException {
		Collection<MemorySegment> segments = globalPool.requestMemorySegments(numExclusiveBuffers);
		checkArgument(!segments.isEmpty(), "The number of exclusive buffers per channel should be larger than 0.");

		synchronized (bufferQueue) {
			for (MemorySegment segment : segments) {
				bufferQueue.addExclusiveBuffer(new NetworkBuffer(segment, this), numRequiredBuffers);
			}
		}
	}

	/**
	 * Requests floating buffers from the buffer pool based on the given required amount, and returns the actual
	 * requested amount. If the required amount is not fully satisfied, it will register as a listener.
	 */
	int requestFloatingBuffers(int numRequired) {
		int numRequestedBuffers = 0;
		synchronized (bufferQueue) {
			// Similar to notifyBufferAvailable(), make sure that we never add a buffer after channel
			// released all buffers via releaseAllResources().
			if (inputChannel.isReleased()) {
				return numRequestedBuffers;
			}

			numRequiredBuffers = numRequired;

			while (bufferQueue.getAvailableBufferSize() < numRequiredBuffers && !isWaitingForFloatingBuffers) {
				BufferPool bufferPool = inputChannel.inputGate.getBufferPool();
				Buffer buffer = bufferPool.requestBuffer();
				if (buffer != null) {
					bufferQueue.addFloatingBuffer(buffer);
					numRequestedBuffers++;
				} else if (bufferPool.addBufferListener(this)) {
					isWaitingForFloatingBuffers = true;
					break;
				}
			}
		}
		return numRequestedBuffers;
	}

	// ------------------------------------------------------------------------
	// Buffer recycle
	// ------------------------------------------------------------------------

	/**
	 * Exclusive buffer is recycled to this channel manager directly and it may trigger return extra
	 * floating buffer based on <tt>numRequiredBuffers</tt>.
	 *
	 * @param segment The exclusive segment of this channel.
	 */
	@Override
	public void recycle(MemorySegment segment) {
		int numAddedBuffers = 0;
		synchronized (bufferQueue) {
			try {
				// Similar to notifyBufferAvailable(), make sure that we never add a buffer
				// after channel released all buffers via releaseAllResources().
				if (inputChannel.isReleased()) {
					globalPool.recycleMemorySegments(Collections.singletonList(segment));
				} else {
					numAddedBuffers = bufferQueue.addExclusiveBuffer(new NetworkBuffer(segment, this), numRequiredBuffers);
				}
			} catch (Throwable t) {
				ExceptionUtils.rethrow(t);
			} finally {
				bufferQueue.notifyAll();
			}
		}

		try {
			inputChannel.notifyBufferAvailable(numAddedBuffers);
		} catch (Throwable t) {
			ExceptionUtils.rethrow(t);
		}
	}

	void releaseFloatingBuffers() {
		synchronized (bufferQueue) {
			numRequiredBuffers = 0;
			bufferQueue.releaseFloatingBuffers();
		}
	}

	/**
	 * Recycles all the exclusive and floating buffers from the given buffer queue.
	 */
	void releaseAllBuffers(ArrayDeque<Buffer> buffers) throws IOException {
		// Gather all exclusive buffers and recycle them to global pool in batch, because
		// we do not want to trigger redistribution of buffers after each recycle.
		final List<MemorySegment> exclusiveRecyclingSegments = new ArrayList<>();

		Buffer buffer;
		while ((buffer = buffers.poll()) != null) {
			if (buffer.getRecycler() == this) {
				exclusiveRecyclingSegments.add(buffer.getMemorySegment());
			} else {
				buffer.recycleBuffer();
			}
		}
		synchronized (bufferQueue) {
			bufferQueue.releaseAll(exclusiveRecyclingSegments);
			bufferQueue.notifyAll();
		}

		if (exclusiveRecyclingSegments.size() > 0) {
			globalPool.recycleMemorySegments(exclusiveRecyclingSegments);
		}
	}

	// ------------------------------------------------------------------------
	// Buffer listener notification
	// ------------------------------------------------------------------------

	/**
	 * The buffer pool notifies this listener of an available floating buffer. If the listener is released or
	 * currently does not need extra buffers, the buffer should be returned to the buffer pool. Otherwise,
	 * the buffer will be added into the <tt>bufferQueue</tt>.
	 *
	 * @param buffer Buffer that becomes available in buffer pool.
	 * @return NotificationResult indicates whether this channel accepts the buffer and is waiting for
	 * more floating buffers.
	 */
	@Override
	public BufferListener.NotificationResult notifyBufferAvailable(Buffer buffer) {
		BufferListener.NotificationResult notificationResult = BufferListener.NotificationResult.BUFFER_NOT_USED;

		// Assuming two remote channels with respective buffer managers as listeners inside LocalBufferPool.
		// While canceler thread calling ch1#releaseAllResources, it might trigger bm2#notifyBufferAvaialble.
		// Concurrently if task thread is recycling exclusive buffer, it might trigger bm1#notifyBufferAvailable.
		// Then these two threads will both occupy the respective bufferQueue lock and wait for other side's
		// bufferQueue lock to cause deadlock. So we check the isReleased state out of synchronized to resolve it.
		if (inputChannel.isReleased()) {
			return notificationResult;
		}

		try {
			synchronized (bufferQueue) {
				checkState(isWaitingForFloatingBuffers, "This channel should be waiting for floating buffers.");

				// Important: make sure that we never add a buffer after releaseAllResources()
				// released all buffers. Following scenarios exist:
				// 1) releaseAllBuffers() already released buffers inside bufferQueue
				// -> while isReleased is set correctly in InputChannel
				// 2) releaseAllBuffers() did not yet release buffers from bufferQueue
				// -> we may or may not have set isReleased yet but will always wait for the
				// lock on bufferQueue to release buffers
				if (inputChannel.isReleased() || bufferQueue.getAvailableBufferSize() >= numRequiredBuffers) {
					isWaitingForFloatingBuffers = false;
					return notificationResult;
				}

				bufferQueue.addFloatingBuffer(buffer);
				bufferQueue.notifyAll();

				if (bufferQueue.getAvailableBufferSize() == numRequiredBuffers) {
					isWaitingForFloatingBuffers = false;
					notificationResult = BufferListener.NotificationResult.BUFFER_USED_NO_NEED_MORE;
				} else {
					notificationResult = BufferListener.NotificationResult.BUFFER_USED_NEED_MORE;
				}
			}

			if (notificationResult != NotificationResult.BUFFER_NOT_USED) {
				inputChannel.notifyBufferAvailable(1);
			}
		} catch (Throwable t) {
			inputChannel.setError(t);
		}

		return notificationResult;
	}

	@Override
	public void notifyBufferDestroyed() {
		// Nothing to do actually.
	}

	// ------------------------------------------------------------------------
	// Getter properties
	// ------------------------------------------------------------------------

	@VisibleForTesting
	int unsynchronizedGetNumberOfRequiredBuffers() {
		return numRequiredBuffers;
	}

	@VisibleForTesting
	boolean unsynchronizedIsWaitingForFloatingBuffers() {
		return isWaitingForFloatingBuffers;
	}

	@VisibleForTesting
	int getNumberOfAvailableBuffers() {
		synchronized (bufferQueue) {
			return bufferQueue.getAvailableBufferSize();
		}
	}

	int unsynchronizedGetAvailableExclusiveBuffers() {
		return bufferQueue.exclusiveBuffers.size();
	}

	int unsynchronizedGetFloatingBuffersAvailable() {
		return bufferQueue.floatingBuffers.size();
	}

	/**
	 * Manages the exclusive and floating buffers of this channel, and handles the
	 * internal buffer related logic.
	 */
	static final class AvailableBufferQueue {

		/**
		 * The current available floating buffers from the fixed buffer pool.
		 */
		final ArrayDeque<Buffer> floatingBuffers;

		/**
		 * The current available exclusive buffers from the global buffer pool.
		 */
		final ArrayDeque<Buffer> exclusiveBuffers;

		AvailableBufferQueue() {
			this.exclusiveBuffers = new ArrayDeque<>();
			this.floatingBuffers = new ArrayDeque<>();
		}

		/**
		 * Adds an exclusive buffer (back) into the queue and recycles one floating buffer if the
		 * number of available buffers in queue is more than the required amount.
		 *
		 * @param buffer             The exclusive buffer to add
		 * @param numRequiredBuffers The number of required buffers
		 * @return How many buffers were added to the queue
		 */
		int addExclusiveBuffer(Buffer buffer, int numRequiredBuffers) {
			exclusiveBuffers.add(buffer);
			if (getAvailableBufferSize() > numRequiredBuffers) {
				Buffer floatingBuffer = floatingBuffers.poll();
				if (floatingBuffer != null) {
					floatingBuffer.recycleBuffer();
					return 0;
				}
			}
			return 1;
		}

		void addFloatingBuffer(Buffer buffer) {
			floatingBuffers.add(buffer);
		}

		/**
		 * Takes the floating buffer first in order to make full use of floating
		 * buffers reasonably.
		 *
		 * @return An available floating or exclusive buffer, may be null
		 * if the channel is released.
		 */
		@Nullable
		Buffer takeBuffer() {
			if (floatingBuffers.size() > 0) {
				return floatingBuffers.poll();
			} else {
				return exclusiveBuffers.poll();
			}
		}

		/**
		 * The floating buffer is recycled to local buffer pool directly, and the
		 * exclusive buffer will be gathered to return to global buffer pool later.
		 *
		 * @param exclusiveSegments The list that we will add exclusive segments into.
		 */
		void releaseAll(List<MemorySegment> exclusiveSegments) {
			Buffer buffer;
			while ((buffer = floatingBuffers.poll()) != null) {
				buffer.recycleBuffer();
			}
			while ((buffer = exclusiveBuffers.poll()) != null) {
				exclusiveSegments.add(buffer.getMemorySegment());
			}
		}

		void releaseFloatingBuffers() {
			Buffer buffer;
			while ((buffer = floatingBuffers.poll()) != null) {
				buffer.recycleBuffer();
			}
		}

		int getAvailableBufferSize() {
			return floatingBuffers.size() + exclusiveBuffers.size();
		}
	}
}
