/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The barrier buffer is {@link CheckpointBarrierHandler} that blocks inputs with barriers until
 * all inputs have received the barrier for a given checkpoint.
 *
 * <p>To avoid back-pressuring the input streams (which may cause distributed deadlocks), the
 * BarrierBuffer continues receiving buffers from the blocked channels and stores them internally until
 * the blocks are released.
 */
@Internal
public class BarrierBuffer implements CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(BarrierBuffer.class);

	/** The gate that the buffer draws its input from. */
	private final InputGate inputGate;

	/** Flags that indicate whether a channel is currently blocked/buffered. */
	private final boolean[] blockedChannels;

	/** The total number of channels that this buffer handles data from. */
	private final int totalNumberOfInputChannels;

	/** To utility to write blocked data to a file channel. */
	private final BufferStorage bufferStorage;

	private final String taskName;

	@Nullable
	private final AbstractInvokable toNotifyOnCheckpoint;

	/** The ID of the checkpoint for which we expect barriers. */
	private long currentCheckpointId = -1L;

	/**
	 * The number of received barriers (= number of blocked/buffered channels) IMPORTANT: A canceled
	 * checkpoint must always have 0 barriers.
	 */
	private int numBarriersReceived;

	/** The number of already closed channels. */
	private int numClosedChannels;

	/** The timestamp as in {@link System#nanoTime()} at which the last alignment started. */
	private long startOfAlignmentTimestamp;

	/** The time (in nanoseconds) that the latest alignment took. */
	private long latestAlignmentDurationNanos;

	/** Flag to indicate whether we have drawn all available input. */
	private boolean endOfStream;

	/** Indicate end of the input. Set to true after encountering {@link #endOfStream} and depleting
	 * {@link #bufferStorage}. */
	private boolean isFinished;

	/**
	 * Creates a new checkpoint stream aligner.
	 *
	 * <p>There is no limit to how much data may be buffered during an alignment.
	 *
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param bufferStorage The storage to hold the buffers and events for blocked channels.
	 */
	@VisibleForTesting
	BarrierBuffer(InputGate inputGate, BufferStorage bufferStorage) {
		this (inputGate, bufferStorage, "Testing: No task associated", null);
	}

	/**
	 * Creates a new checkpoint stream aligner.
	 *
	 * <p>The aligner will allow only alignments that buffer up to the given number of bytes.
	 * When that number is exceeded, it will stop the alignment and notify the task that the
	 * checkpoint has been cancelled.
	 *
	 * @param inputGate The input gate to draw the buffers and events from.
	 * @param bufferStorage The storage to hold the buffers and events for blocked channels.
	 * @param taskName The task name for logging.
	 * @param toNotifyOnCheckpoint optional Handler that receives the checkpoint notifications.
	 */
	BarrierBuffer(
		InputGate inputGate,
		BufferStorage bufferStorage,
		String taskName,
		@Nullable AbstractInvokable toNotifyOnCheckpoint) {

		this.inputGate = inputGate;
		this.totalNumberOfInputChannels = inputGate.getNumberOfInputChannels();
		this.blockedChannels = new boolean[this.totalNumberOfInputChannels];

		this.bufferStorage = checkNotNull(bufferStorage);

		this.taskName = taskName;
		this.toNotifyOnCheckpoint = toNotifyOnCheckpoint;
	}

	@Override
	public CompletableFuture<?> isAvailable() {
		if (bufferStorage.isEmpty()) {
			return inputGate.isAvailable();
		}
		return AVAILABLE;
	}

	// ------------------------------------------------------------------------
	//  Buffer and barrier handling
	// ------------------------------------------------------------------------

	@Override
	public Optional<BufferOrEvent> pollNext() throws Exception {
		while (true) {
			// process buffered BufferOrEvents before grabbing new ones
			Optional<BufferOrEvent> next;
			if (bufferStorage.isEmpty()) {
				next = inputGate.pollNext();
			}
			else {
				// TODO: FLINK-12536 for non credit-based flow control, getNext method is blocking
				next = bufferStorage.pollNext();
				if (!next.isPresent()) {
					return pollNext();
				}
			}

			if (!next.isPresent()) {
				return handleEmptyBuffer();
			}

			BufferOrEvent bufferOrEvent = next.get();
			if (isBlocked(bufferOrEvent.getChannelIndex())) {
				// if the channel is blocked, we just store the BufferOrEvent
				bufferStorage.add(bufferOrEvent);
				if (bufferStorage.isFull()) {
					sizeLimitExceeded();
				}
			}
			else if (bufferOrEvent.isBuffer()) {
				return next;
			}
			else if (bufferOrEvent.getEvent().getClass() == CheckpointBarrier.class) {
				if (!endOfStream) {
					// process barriers only if there is a chance of the checkpoint completing
					processBarrier((CheckpointBarrier) bufferOrEvent.getEvent(), bufferOrEvent.getChannelIndex());
				}
			}
			else if (bufferOrEvent.getEvent().getClass() == CancelCheckpointMarker.class) {
				processCancellationBarrier((CancelCheckpointMarker) bufferOrEvent.getEvent());
			}
			else {
				if (bufferOrEvent.getEvent().getClass() == EndOfPartitionEvent.class) {
					processEndOfPartition();
				}
				return next;
			}
		}
	}

	private Optional<BufferOrEvent> handleEmptyBuffer() throws Exception {
		if (!inputGate.isFinished()) {
			return Optional.empty();
		}

		if (endOfStream) {
			isFinished = true;
			return Optional.empty();
		} else {
			// end of input stream. stream continues with the buffered data
			endOfStream = true;
			releaseBlocksAndResetBarriers();
			return pollNext();
		}
	}

	private void processBarrier(CheckpointBarrier receivedBarrier, int channelIndex) throws Exception {
		final long barrierId = receivedBarrier.getId();

		// fast path for single channel cases
		if (totalNumberOfInputChannels == 1) {
			if (barrierId > currentCheckpointId) {
				// new checkpoint
				currentCheckpointId = barrierId;
				notifyCheckpoint(receivedBarrier);
			}
			return;
		}

		// -- general code path for multiple input channels --

		if (numBarriersReceived > 0) {
			// this is only true if some alignment is already progress and was not canceled

			if (barrierId == currentCheckpointId) {
				// regular case
				onBarrier(channelIndex);
			}
			else if (barrierId > currentCheckpointId) {
				// we did not complete the current checkpoint, another started before
				LOG.warn("{}: Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. " +
						"Skipping current checkpoint.",
					taskName,
					barrierId,
					currentCheckpointId);

				// let the task know we are not completing this
				notifyAbort(currentCheckpointId,
					new CheckpointException(
						"Barrier id: " + barrierId,
						CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED));

				// abort the current checkpoint
				releaseBlocksAndResetBarriers();

				// begin a the new checkpoint
				beginNewAlignment(barrierId, channelIndex);
			}
			else {
				// ignore trailing barrier from an earlier checkpoint (obsolete now)
				return;
			}
		}
		else if (barrierId > currentCheckpointId) {
			// first barrier of a new checkpoint
			beginNewAlignment(barrierId, channelIndex);
		}
		else {
			// either the current checkpoint was canceled (numBarriers == 0) or
			// this barrier is from an old subsumed checkpoint
			return;
		}

		// check if we have all barriers - since canceled checkpoints always have zero barriers
		// this can only happen on a non canceled checkpoint
		if (numBarriersReceived + numClosedChannels == totalNumberOfInputChannels) {
			// actually trigger checkpoint
			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Received all barriers, triggering checkpoint {} at {}.",
					taskName,
					receivedBarrier.getId(),
					receivedBarrier.getTimestamp());
			}

			releaseBlocksAndResetBarriers();
			notifyCheckpoint(receivedBarrier);
		}
	}

	private void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
		final long barrierId = cancelBarrier.getCheckpointId();

		// fast path for single channel cases
		if (totalNumberOfInputChannels == 1) {
			if (barrierId > currentCheckpointId) {
				// new checkpoint
				currentCheckpointId = barrierId;
				notifyAbortOnCancellationBarrier(barrierId);
			}
			return;
		}

		// -- general code path for multiple input channels --

		if (numBarriersReceived > 0) {
			// this is only true if some alignment is in progress and nothing was canceled

			if (barrierId == currentCheckpointId) {
				// cancel this alignment
				if (LOG.isDebugEnabled()) {
					LOG.debug("{}: Checkpoint {} canceled, aborting alignment.", taskName, barrierId);
				}

				releaseBlocksAndResetBarriers();
				notifyAbortOnCancellationBarrier(barrierId);
			}
			else if (barrierId > currentCheckpointId) {
				// we canceled the next which also cancels the current
				LOG.warn("{}: Received cancellation barrier for checkpoint {} before completing current checkpoint {}. " +
						"Skipping current checkpoint.",
					taskName,
					barrierId,
					currentCheckpointId);

				// this stops the current alignment
				releaseBlocksAndResetBarriers();

				// the next checkpoint starts as canceled
				currentCheckpointId = barrierId;
				startOfAlignmentTimestamp = 0L;
				latestAlignmentDurationNanos = 0L;

				notifyAbort(currentCheckpointId,
					new CheckpointException(
						"Barrier id: " + barrierId,
						CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED
					));

				notifyAbortOnCancellationBarrier(barrierId);
			}

			// else: ignore trailing (cancellation) barrier from an earlier checkpoint (obsolete now)

		}
		else if (barrierId > currentCheckpointId) {
			// first barrier of a new checkpoint is directly a cancellation

			// by setting the currentCheckpointId to this checkpoint while keeping the numBarriers
			// at zero means that no checkpoint barrier can start a new alignment
			currentCheckpointId = barrierId;

			startOfAlignmentTimestamp = 0L;
			latestAlignmentDurationNanos = 0L;

			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Checkpoint {} canceled, skipping alignment.", taskName, barrierId);
			}

			notifyAbortOnCancellationBarrier(barrierId);
		}

		// else: trailing barrier from either
		//   - a previous (subsumed) checkpoint
		//   - the current checkpoint if it was already canceled
	}

	private void processEndOfPartition() throws Exception {
		numClosedChannels++;

		if (numBarriersReceived > 0) {
			// let the task know we skip a checkpoint
			notifyAbort(currentCheckpointId,
				new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM));

			// no chance to complete this checkpoint
			releaseBlocksAndResetBarriers();
		}
	}

	private void notifyCheckpoint(CheckpointBarrier checkpointBarrier) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			CheckpointMetaData checkpointMetaData =
					new CheckpointMetaData(checkpointBarrier.getId(), checkpointBarrier.getTimestamp());

			CheckpointMetrics checkpointMetrics = new CheckpointMetrics()
					.setBytesBufferedInAlignment(bufferStorage.currentBufferedSize())
					.setAlignmentDurationNanos(latestAlignmentDurationNanos);

			toNotifyOnCheckpoint.triggerCheckpointOnBarrier(
				checkpointMetaData,
				checkpointBarrier.getCheckpointOptions(),
				checkpointMetrics);
		}
	}

	private void notifyAbortOnCancellationBarrier(long checkpointId) throws Exception {
		notifyAbort(checkpointId,
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
	}

	private void notifyAbort(long checkpointId, CheckpointException cause) throws Exception {
		if (toNotifyOnCheckpoint != null) {
			toNotifyOnCheckpoint.abortCheckpointOnBarrier(checkpointId, cause);
		}
	}

	private void sizeLimitExceeded() throws Exception {
		long maxBufferedBytes = bufferStorage.getMaxBufferedBytes();
		// exceeded our limit - abort this checkpoint
		LOG.info("{}: Checkpoint {} aborted because alignment volume limit ({} bytes) exceeded.",
			taskName,
			currentCheckpointId,
			maxBufferedBytes);

		releaseBlocksAndResetBarriers();
		notifyAbort(currentCheckpointId,
			new CheckpointException(
				"Max buffered bytes: " + maxBufferedBytes,
				CheckpointFailureReason.CHECKPOINT_DECLINED_ALIGNMENT_LIMIT_EXCEEDED));
	}

	@Override
	public boolean isEmpty() {
		return bufferStorage.isEmpty();
	}

	@Override
	public boolean isFinished() {
		return isFinished;
	}

	@Override
	public void cleanup() throws IOException {
		bufferStorage.close();
	}

	private void beginNewAlignment(long checkpointId, int channelIndex) throws IOException {
		currentCheckpointId = checkpointId;
		onBarrier(channelIndex);

		startOfAlignmentTimestamp = System.nanoTime();

		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Starting stream alignment for checkpoint {}.", taskName, checkpointId);
		}
	}

	/**
	 * Checks whether the channel with the given index is blocked.
	 *
	 * @param channelIndex The channel index to check.
	 * @return True if the channel is blocked, false if not.
	 */
	private boolean isBlocked(int channelIndex) {
		return blockedChannels[channelIndex];
	}

	/**
	 * Blocks the given channel index, from which a barrier has been received.
	 *
	 * @param channelIndex The channel index to block.
	 */
	private void onBarrier(int channelIndex) throws IOException {
		if (!blockedChannels[channelIndex]) {
			blockedChannels[channelIndex] = true;

			numBarriersReceived++;

			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Received barrier from channel {}.", taskName, channelIndex);
			}
		}
		else {
			throw new IOException("Stream corrupt: Repeated barrier for same checkpoint on input " + channelIndex);
		}
	}

	/**
	 * Releases the blocks on all channels and resets the barrier count.
	 * Makes sure the just written data is the next to be consumed.
	 */
	private void releaseBlocksAndResetBarriers() throws IOException {
		LOG.debug("{}: End of stream alignment, feeding buffered data back.", taskName);

		for (int i = 0; i < blockedChannels.length; i++) {
			blockedChannels[i] = false;
		}

		bufferStorage.rollOver();

		// the next barrier that comes must assume it is the first
		numBarriersReceived = 0;

		if (startOfAlignmentTimestamp > 0) {
			latestAlignmentDurationNanos = System.nanoTime() - startOfAlignmentTimestamp;
			startOfAlignmentTimestamp = 0;
		}
	}

	// ------------------------------------------------------------------------
	//  Properties
	// ------------------------------------------------------------------------

	/**
	 * Gets the ID defining the current pending, or just completed, checkpoint.
	 *
	 * @return The ID of the pending of completed checkpoint.
	 */
	public long getCurrentCheckpointId() {
		return this.currentCheckpointId;
	}

	@Override
	public long getAlignmentDurationNanos() {
		long start = this.startOfAlignmentTimestamp;
		if (start <= 0) {
			return latestAlignmentDurationNanos;
		} else {
			return System.nanoTime() - start;
		}
	}

	@Override
	public int getNumberOfInputChannels() {
		return totalNumberOfInputChannels;
	}

	// ------------------------------------------------------------------------
	// Utilities
	// ------------------------------------------------------------------------

	@Override
	public String toString() {
		return String.format("%s: last checkpoint: %d, current barriers: %d, closed channels: %d",
			taskName,
			currentCheckpointId,
			numBarriersReceived,
			numClosedChannels);
	}
}
