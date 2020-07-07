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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.InputGate;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * {@link CheckpointBarrierAligner} keep tracks of received {@link CheckpointBarrier} on given
 * channels and controls the alignment, by deciding which channels should be blocked and when to
 * release blocked channels.
 */
@Internal
public class CheckpointBarrierAligner extends CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointBarrierAligner.class);

	/** Flags that indicate whether a channel is currently blocked/buffered. */
	private final Map<InputChannelInfo, Boolean> blockedChannels;

	/** The total number of channels that this buffer handles data from. */
	private final int totalNumberOfInputChannels;

	private final String taskName;

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

	private final InputGate[] inputGates;

	CheckpointBarrierAligner(
			String taskName,
			AbstractInvokable toNotifyOnCheckpoint,
			InputGate... inputGates) {
		super(toNotifyOnCheckpoint);

		this.taskName = taskName;
		this.inputGates = inputGates;
		blockedChannels = Arrays.stream(inputGates)
			.flatMap(gate -> gate.getChannelInfos().stream())
			.collect(Collectors.toMap(Function.identity(), info -> false));
		totalNumberOfInputChannels = blockedChannels.size();
	}

	@Override
	public void abortPendingCheckpoint(long checkpointId, CheckpointException exception) throws IOException {
		if (checkpointId > currentCheckpointId && isCheckpointPending()) {
			releaseBlocksAndResetBarriers();
			notifyAbort(currentCheckpointId, exception);
		}
	}

	@Override
	public void releaseBlocksAndResetBarriers() throws IOException {
		LOG.debug("{}: End of stream alignment, feeding buffered data back.", taskName);

		for (Map.Entry<InputChannelInfo, Boolean> blockedChannel : blockedChannels.entrySet()) {
			if (blockedChannel.getValue()) {
				resumeConsumption(blockedChannel.getKey());
			}
			blockedChannel.setValue(false);
		}

		// the next barrier that comes must assume it is the first
		numBarriersReceived = 0;

		if (startOfAlignmentTimestamp > 0) {
			latestAlignmentDurationNanos = System.nanoTime() - startOfAlignmentTimestamp;
			startOfAlignmentTimestamp = 0;
		}
	}

	@Override
	public boolean isBlocked(InputChannelInfo channelInfo) {
		return blockedChannels.get(channelInfo);
	}

	@Override
	public void processBarrier(CheckpointBarrier receivedBarrier, InputChannelInfo channelInfo) throws Exception {
		final long barrierId = receivedBarrier.getId();

		// fast path for single channel cases
		if (totalNumberOfInputChannels == 1) {
			resumeConsumption(channelInfo);
			if (barrierId > currentCheckpointId) {
				// new checkpoint
				currentCheckpointId = barrierId;
				notifyCheckpoint(receivedBarrier, latestAlignmentDurationNanos);
			}
			return;
		}

		// -- general code path for multiple input channels --

		if (isCheckpointPending()) {
			// this is only true if some alignment is already progress and was not canceled

			if (barrierId == currentCheckpointId) {
				// regular case
				onBarrier(channelInfo);
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

				// begin a new checkpoint
				beginNewAlignment(barrierId, channelInfo, receivedBarrier.getTimestamp());
			}
			else {
				// ignore trailing barrier from an earlier checkpoint (obsolete now)
				resumeConsumption(channelInfo);
			}
		}
		else if (barrierId > currentCheckpointId) {
			// first barrier of a new checkpoint
			beginNewAlignment(barrierId, channelInfo, receivedBarrier.getTimestamp());
		}
		else {
			// either the current checkpoint was canceled (numBarriers == 0) or
			// this barrier is from an old subsumed checkpoint
			resumeConsumption(channelInfo);
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
			notifyCheckpoint(receivedBarrier, latestAlignmentDurationNanos);
		}
	}

	protected void beginNewAlignment(
			long checkpointId,
			InputChannelInfo channelInfo,
			long checkpointTimestamp) throws IOException {
		markCheckpointStart(checkpointTimestamp);
		currentCheckpointId = checkpointId;
		onBarrier(channelInfo);

		startOfAlignmentTimestamp = System.nanoTime();

		if (LOG.isDebugEnabled()) {
			LOG.debug("{}: Starting stream alignment for checkpoint {}.", taskName, checkpointId);
		}
	}

	/**
	 * Blocks the given channel index, from which a barrier has been received.
	 *
	 * @param channelInfo The channel to block.
	 */
	protected void onBarrier(InputChannelInfo channelInfo) throws IOException {
		if (!blockedChannels.get(channelInfo)) {
			blockedChannels.put(channelInfo, true);

			numBarriersReceived++;

			if (LOG.isDebugEnabled()) {
				LOG.debug("{}: Received barrier from channel {}.", taskName, channelInfo);
			}
		}
		else {
			throw new IOException("Stream corrupt: Repeated barrier for same checkpoint on input " + channelInfo);
		}
	}

	@Override
	public void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws Exception {
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

		if (isCheckpointPending()) {
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

	@Override
	public void processEndOfPartition() throws Exception {
		numClosedChannels++;

		if (isCheckpointPending()) {
			// let the task know we skip a checkpoint
			notifyAbort(currentCheckpointId,
				new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM));
			// no chance to complete this checkpoint
			releaseBlocksAndResetBarriers();
		}
	}

	@Override
	public long getLatestCheckpointId() {
		return currentCheckpointId;
	}

	@Override
	public long getAlignmentDurationNanos() {
		if (startOfAlignmentTimestamp <= 0) {
			return latestAlignmentDurationNanos;
		} else {
			return System.nanoTime() - startOfAlignmentTimestamp;
		}
	}

	@Override
	protected boolean isCheckpointPending() {
		return numBarriersReceived > 0;
	}

	private void resumeConsumption(InputChannelInfo channelInfo) throws IOException {
		InputGate inputGate = inputGates[channelInfo.getGateIdx()];
		checkState(!inputGate.isFinished(), "InputGate already finished.");

		inputGate.resumeConsumption(channelInfo.getInputChannelIdx());
	}

	@VisibleForTesting
	public int getNumClosedChannels() {
		return numClosedChannels;
	}

	@Override
	public String toString() {
		return String.format("%s: last checkpoint: %d, current barriers: %d, closed channels: %d",
			taskName,
			currentCheckpointId,
			numBarriersReceived,
			numClosedChannels);
	}
}
