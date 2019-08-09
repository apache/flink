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
import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.PartitionRequestClient;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.BufferListener;
import org.apache.flink.runtime.io.network.buffer.BufferProvider;
import org.apache.flink.runtime.io.network.buffer.BufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.util.ExceptionUtils;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * An input channel, which requests a remote partition queue.
 */
public class RemoteInputChannel extends InputChannel implements BufferRecycler, BufferListener {

	/** ID to distinguish this channel from other channels sharing the same TCP connection. */
	private final InputChannelID id = new InputChannelID();

	/** The connection to use to request the remote partition. */
	private final ConnectionID connectionId;

	/** The connection manager to use connect to the remote partition provider. */
	private final ConnectionManager connectionManager;

	/**
	 * The received buffers. Received buffers are enqueued by the network I/O thread and the queue
	 * is consumed by the receiving task thread.
	 */
	private final ArrayDeque<Buffer> receivedBuffers = new ArrayDeque<>();

	/**
	 * Flag indicating whether this channel has been released. Either called by the receiving task
	 * thread or the task manager actor.
	 */
	private final AtomicBoolean isReleased = new AtomicBoolean();

	/** Client to establish a (possibly shared) TCP connection and request the partition. */
	private volatile PartitionRequestClient partitionRequestClient;

	/**
	 * The next expected sequence number for the next buffer. This is modified by the network
	 * I/O thread only.
	 */
	private int expectedSequenceNumber = 0;

	/** The initial number of exclusive buffers assigned to this channel. */
	private int initialCredit;

	/** The available buffer queue wraps both exclusive and requested floating buffers. */
	private final AvailableBufferQueue bufferQueue = new AvailableBufferQueue();

	/** The number of available buffers that have not been announced to the producer yet. */
	private final AtomicInteger unannouncedCredit = new AtomicInteger(0);

	/** The number of required buffers that equals to sender's backlog plus initial credit. */
	@GuardedBy("bufferQueue")
	private int numRequiredBuffers;

	/** The tag indicates whether this channel is waiting for additional floating buffers from the buffer pool. */
	@GuardedBy("bufferQueue")
	private boolean isWaitingForFloatingBuffers;

	/** Global memory segment provider to request and recycle exclusive buffers (only for credit-based). */
	@Nonnull
	private final MemorySegmentProvider memorySegmentProvider;

	public RemoteInputChannel(
		SingleInputGate inputGate,
		int channelIndex,
		ResultPartitionID partitionId,
		ConnectionID connectionId,
		ConnectionManager connectionManager,
		int initialBackOff,
		int maxBackoff,
		InputChannelMetrics metrics,
		@Nonnull MemorySegmentProvider memorySegmentProvider) {

		super(inputGate, channelIndex, partitionId, initialBackOff, maxBackoff,
			metrics.getNumBytesInRemoteCounter(), metrics.getNumBuffersInRemoteCounter());

		this.connectionId = checkNotNull(connectionId);
		this.connectionManager = checkNotNull(connectionManager);
		this.memorySegmentProvider = memorySegmentProvider;
	}

	/**
	 * Assigns exclusive buffers to this input channel, and this method should be called only once
	 * after this input channel is created.
	 */
	void assignExclusiveSegments() throws IOException {
		checkState(initialCredit == 0, "Bug in input channel setup logic: exclusive buffers have " +
			"already been set for this input channel.");

		Collection<MemorySegment> segments = checkNotNull(memorySegmentProvider.requestMemorySegments());
		checkArgument(!segments.isEmpty(), "The number of exclusive buffers per channel should be larger than 0.");

		initialCredit = segments.size();
		numRequiredBuffers = segments.size();

		synchronized (bufferQueue) {
			for (MemorySegment segment : segments) {
				bufferQueue.addExclusiveBuffer(new NetworkBuffer(segment, this), numRequiredBuffers);
			}
		}
	}

	// ------------------------------------------------------------------------
	// Consume
	// ------------------------------------------------------------------------

	/**
	 * Requests a remote subpartition.
	 */
	@VisibleForTesting
	@Override
	public void requestSubpartition(int subpartitionIndex) throws IOException, InterruptedException {
		if (partitionRequestClient == null) {
			// Create a client and request the partition
			try {
				partitionRequestClient = connectionManager.createPartitionRequestClient(connectionId);
			} catch (IOException e) {
				// IOExceptions indicate that we could not open a connection to the remote TaskExecutor
				throw new PartitionConnectionException(partitionId, e);
			}

			partitionRequestClient.requestSubpartition(partitionId, subpartitionIndex, this, 0);
		}
	}

	/**
	 * Retriggers a remote subpartition request.
	 */
	void retriggerSubpartitionRequest(int subpartitionIndex) throws IOException {
		checkState(partitionRequestClient != null, "Missing initial subpartition request.");

		if (increaseBackoff()) {
			partitionRequestClient.requestSubpartition(
				partitionId, subpartitionIndex, this, getCurrentBackoff());
		} else {
			failPartitionRequest();
		}
	}

	@Override
	Optional<BufferAndAvailability> getNextBuffer() throws IOException {
		checkState(!isReleased.get(), "Queried for a buffer after channel has been closed.");
		checkState(partitionRequestClient != null, "Queried for a buffer before requesting a queue.");

		checkError();

		final Buffer next;
		final boolean moreAvailable;

		synchronized (receivedBuffers) {
			next = receivedBuffers.poll();
			moreAvailable = !receivedBuffers.isEmpty();
		}

		numBytesIn.inc(next.getSize());
		numBuffersIn.inc();
		return Optional.of(new BufferAndAvailability(next, moreAvailable, getSenderBacklog()));
	}

	// ------------------------------------------------------------------------
	// Task events
	// ------------------------------------------------------------------------

	@Override
	void sendTaskEvent(TaskEvent event) throws IOException {
		checkState(!isReleased.get(), "Tried to send task event to producer after channel has been released.");
		checkState(partitionRequestClient != null, "Tried to send task event to producer before requesting a queue.");

		checkError();

		partitionRequestClient.sendTaskEvent(partitionId, event, this);
	}

	// ------------------------------------------------------------------------
	// Life cycle
	// ------------------------------------------------------------------------

	@Override
	public boolean isReleased() {
		return isReleased.get();
	}

	@Override
	void notifySubpartitionConsumed() {
		// Nothing to do
	}

	/**
	 * Releases all exclusive and floating buffers, closes the partition request client.
	 */
	@Override
	void releaseAllResources() throws IOException {
		if (isReleased.compareAndSet(false, true)) {

			// Gather all exclusive buffers and recycle them to global pool in batch, because
			// we do not want to trigger redistribution of buffers after each recycle.
			final List<MemorySegment> exclusiveRecyclingSegments = new ArrayList<>();

			synchronized (receivedBuffers) {
				Buffer buffer;
				while ((buffer = receivedBuffers.poll()) != null) {
					if (buffer.getRecycler() == this) {
						exclusiveRecyclingSegments.add(buffer.getMemorySegment());
					} else {
						buffer.recycleBuffer();
					}
				}
			}
			synchronized (bufferQueue) {
				bufferQueue.releaseAll(exclusiveRecyclingSegments);
			}

			if (exclusiveRecyclingSegments.size() > 0) {
				memorySegmentProvider.recycleMemorySegments(exclusiveRecyclingSegments);
			}

			// The released flag has to be set before closing the connection to ensure that
			// buffers received concurrently with closing are properly recycled.
			if (partitionRequestClient != null) {
				partitionRequestClient.close(this);
			} else {
				connectionManager.closeOpenChannelConnections(connectionId);
			}
		}
	}

	private void failPartitionRequest() {
		setError(new PartitionNotFoundException(partitionId));
	}

	@Override
	public String toString() {
		return "RemoteInputChannel [" + partitionId + " at " + connectionId + "]";
	}

	// ------------------------------------------------------------------------
	// Credit-based
	// ------------------------------------------------------------------------

	/**
	 * Enqueue this input channel in the pipeline for notifying the producer of unannounced credit.
	 */
	private void notifyCreditAvailable() {
		checkState(partitionRequestClient != null, "Tried to send task event to producer before requesting a queue.");

		partitionRequestClient.notifyCreditAvailable(this);
	}

	/**
	 * Exclusive buffer is recycled to this input channel directly and it may trigger return extra
	 * floating buffer and notify increased credit to the producer.
	 *
	 * @param segment The exclusive segment of this channel.
	 */
	@Override
	public void recycle(MemorySegment segment) {
		int numAddedBuffers;

		synchronized (bufferQueue) {
			// Similar to notifyBufferAvailable(), make sure that we never add a buffer
			// after releaseAllResources() released all buffers (see below for details).
			if (isReleased.get()) {
				try {
					memorySegmentProvider.recycleMemorySegments(Collections.singletonList(segment));
					return;
				} catch (Throwable t) {
					ExceptionUtils.rethrow(t);
				}
			}
			numAddedBuffers = bufferQueue.addExclusiveBuffer(new NetworkBuffer(segment, this), numRequiredBuffers);
		}

		if (numAddedBuffers > 0 && unannouncedCredit.getAndAdd(numAddedBuffers) == 0) {
			notifyCreditAvailable();
		}
	}

	public int getNumberOfAvailableBuffers() {
		synchronized (bufferQueue) {
			return bufferQueue.getAvailableBufferSize();
		}
	}

	public int getNumberOfRequiredBuffers() {
		return numRequiredBuffers;
	}

	public int getSenderBacklog() {
		return numRequiredBuffers - initialCredit;
	}

	@VisibleForTesting
	boolean isWaitingForFloatingBuffers() {
		return isWaitingForFloatingBuffers;
	}

	/**
	 * The Buffer pool notifies this channel of an available floating buffer. If the channel is released or
	 * currently does not need extra buffers, the buffer should be returned to the buffer pool. Otherwise,
	 * the buffer will be added into the <tt>bufferQueue</tt> and the unannounced credit is increased
	 * by one.
	 *
	 * @param buffer Buffer that becomes available in buffer pool.
	 * @return NotificationResult indicates whether this channel accepts the buffer and is waiting for
	 *  	more floating buffers.
	 */
	@Override
	public NotificationResult notifyBufferAvailable(Buffer buffer) {
		NotificationResult notificationResult = NotificationResult.BUFFER_NOT_USED;
		try {
			synchronized (bufferQueue) {
				checkState(isWaitingForFloatingBuffers,
					"This channel should be waiting for floating buffers.");

				// Important: make sure that we never add a buffer after releaseAllResources()
				// released all buffers. Following scenarios exist:
				// 1) releaseAllResources() already released buffers inside bufferQueue
				// -> then isReleased is set correctly
				// 2) releaseAllResources() did not yet release buffers from bufferQueue
				// -> we may or may not have set isReleased yet but will always wait for the
				// lock on bufferQueue to release buffers
				if (isReleased.get() || bufferQueue.getAvailableBufferSize() >= numRequiredBuffers) {
					isWaitingForFloatingBuffers = false;
					return notificationResult;
				}

				bufferQueue.addFloatingBuffer(buffer);

				if (bufferQueue.getAvailableBufferSize() == numRequiredBuffers) {
					isWaitingForFloatingBuffers = false;
					notificationResult = NotificationResult.BUFFER_USED_NO_NEED_MORE;
				} else {
					notificationResult = NotificationResult.BUFFER_USED_NEED_MORE;
				}
			}

			if (unannouncedCredit.getAndAdd(1) == 0) {
				notifyCreditAvailable();
			}
		} catch (Throwable t) {
			setError(t);
		}
		return notificationResult;
	}

	@Override
	public void notifyBufferDestroyed() {
		// Nothing to do actually.
	}

	// ------------------------------------------------------------------------
	// Network I/O notifications (called by network I/O thread)
	// ------------------------------------------------------------------------

	/**
	 * Gets the currently unannounced credit.
	 *
	 * @return Credit which was not announced to the sender yet.
	 */
	public int getUnannouncedCredit() {
		return unannouncedCredit.get();
	}

	/**
	 * Gets the unannounced credit and resets it to <tt>0</tt> atomically.
	 *
	 * @return Credit which was not announced to the sender yet.
	 */
	public int getAndResetUnannouncedCredit() {
		return unannouncedCredit.getAndSet(0);
	}

	/**
	 * Gets the current number of received buffers which have not been processed yet.
	 *
	 * @return Buffers queued for processing.
	 */
	public int getNumberOfQueuedBuffers() {
		synchronized (receivedBuffers) {
			return receivedBuffers.size();
		}
	}

	@Override
	public int unsynchronizedGetNumberOfQueuedBuffers() {
		return Math.max(0, receivedBuffers.size());
	}

	public int unsynchronizedGetExclusiveBuffersUsed() {
		return Math.max(0, initialCredit - bufferQueue.exclusiveBuffers.size());
	}

	public int unsynchronizedGetFloatingBuffersAvailable() {
		return Math.max(0, bufferQueue.floatingBuffers.size());
	}

	public InputChannelID getInputChannelId() {
		return id;
	}

	public int getInitialCredit() {
		return initialCredit;
	}

	public BufferProvider getBufferProvider() throws IOException {
		if (isReleased.get()) {
			return null;
		}

		return inputGate.getBufferProvider();
	}

	/**
	 * Requests buffer from input channel directly for receiving network data.
	 * It should always return an available buffer in credit-based mode unless
	 * the channel has been released.
	 *
	 * @return The available buffer.
	 */
	@Nullable
	public Buffer requestBuffer() {
		synchronized (bufferQueue) {
			return bufferQueue.takeBuffer();
		}
	}

	/**
	 * Receives the backlog from the producer's buffer response. If the number of available
	 * buffers is less than backlog + initialCredit, it will request floating buffers from the buffer
	 * pool, and then notify unannounced credits to the producer.
	 *
	 * @param backlog The number of unsent buffers in the producer's sub partition.
	 */
	void onSenderBacklog(int backlog) throws IOException {
		int numRequestedBuffers = 0;

		synchronized (bufferQueue) {
			// Similar to notifyBufferAvailable(), make sure that we never add a buffer
			// after releaseAllResources() released all buffers (see above for details).
			if (isReleased.get()) {
				return;
			}

			numRequiredBuffers = backlog + initialCredit;
			while (bufferQueue.getAvailableBufferSize() < numRequiredBuffers && !isWaitingForFloatingBuffers) {
				Buffer buffer = inputGate.getBufferPool().requestBuffer();
				if (buffer != null) {
					bufferQueue.addFloatingBuffer(buffer);
					numRequestedBuffers++;
				} else if (inputGate.getBufferProvider().addBufferListener(this)) {
					// If the channel has not got enough buffers, register it as listener to wait for more floating buffers.
					isWaitingForFloatingBuffers = true;
					break;
				}
			}
		}

		if (numRequestedBuffers > 0 && unannouncedCredit.getAndAdd(numRequestedBuffers) == 0) {
			notifyCreditAvailable();
		}
	}

	public void onBuffer(Buffer buffer, int sequenceNumber, int backlog) throws IOException {
		boolean recycleBuffer = true;

		try {

			final boolean wasEmpty;
			synchronized (receivedBuffers) {
				// Similar to notifyBufferAvailable(), make sure that we never add a buffer
				// after releaseAllResources() released all buffers from receivedBuffers
				// (see above for details).
				if (isReleased.get()) {
					return;
				}

				if (expectedSequenceNumber != sequenceNumber) {
					onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
					return;
				}

				wasEmpty = receivedBuffers.isEmpty();
				receivedBuffers.add(buffer);
				recycleBuffer = false;
			}

			++expectedSequenceNumber;

			if (wasEmpty) {
				notifyChannelNonEmpty();
			}

			if (backlog >= 0) {
				onSenderBacklog(backlog);
			}
		} finally {
			if (recycleBuffer) {
				buffer.recycleBuffer();
			}
		}
	}

	public void onEmptyBuffer(int sequenceNumber, int backlog) throws IOException {
		boolean success = false;

		synchronized (receivedBuffers) {
			if (!isReleased.get()) {
				if (expectedSequenceNumber == sequenceNumber) {
					expectedSequenceNumber++;
					success = true;
				} else {
					onError(new BufferReorderingException(expectedSequenceNumber, sequenceNumber));
				}
			}
		}

		if (success && backlog >= 0) {
			onSenderBacklog(backlog);
		}
	}

	public void onFailedPartitionRequest() {
		inputGate.triggerPartitionStateCheck(partitionId);
	}

	public void onError(Throwable cause) {
		setError(cause);
	}

	private static class BufferReorderingException extends IOException {

		private static final long serialVersionUID = -888282210356266816L;

		private final int expectedSequenceNumber;

		private final int actualSequenceNumber;

		BufferReorderingException(int expectedSequenceNumber, int actualSequenceNumber) {
			this.expectedSequenceNumber = expectedSequenceNumber;
			this.actualSequenceNumber = actualSequenceNumber;
		}

		@Override
		public String getMessage() {
			return String.format("Buffer re-ordering: expected buffer with sequence number %d, but received %d.",
				expectedSequenceNumber, actualSequenceNumber);
		}
	}

	/**
	 * Manages the exclusive and floating buffers of this channel, and handles the
	 * internal buffer related logic.
	 */
	private static class AvailableBufferQueue {

		/** The current available floating buffers from the fixed buffer pool. */
		private final ArrayDeque<Buffer> floatingBuffers;

		/** The current available exclusive buffers from the global buffer pool. */
		private final ArrayDeque<Buffer> exclusiveBuffers;

		AvailableBufferQueue() {
			this.exclusiveBuffers = new ArrayDeque<>();
			this.floatingBuffers = new ArrayDeque<>();
		}

		/**
		 * Adds an exclusive buffer (back) into the queue and recycles one floating buffer if the
		 * number of available buffers in queue is more than the required amount.
		 *
		 * @param buffer The exclusive buffer to add
		 * @param numRequiredBuffers The number of required buffers
		 *
		 * @return How many buffers were added to the queue
		 */
		int addExclusiveBuffer(Buffer buffer, int numRequiredBuffers) {
			exclusiveBuffers.add(buffer);
			if (getAvailableBufferSize() > numRequiredBuffers) {
				Buffer floatingBuffer = floatingBuffers.poll();
				floatingBuffer.recycleBuffer();
				return 0;
			} else {
				return 1;
			}
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

		int getAvailableBufferSize() {
			return floatingBuffers.size() + exclusiveBuffers.size();
		}
	}
}
