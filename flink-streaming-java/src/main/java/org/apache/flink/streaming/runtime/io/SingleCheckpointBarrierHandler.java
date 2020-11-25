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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED;

/**
 * {@link SingleCheckpointBarrierHandler} is used for triggering checkpoint while reading the first barrier
 * and keeping track of the number of received barriers and consumed barriers. It can handle/track
 * just single checkpoint at a time. The behaviour when to actually trigger the checkpoint and
 * what the {@link CheckpointableInput} should do is controlled by {@link CheckpointBarrierBehaviourController}.
 */
@Internal
@NotThreadSafe
public class SingleCheckpointBarrierHandler extends CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(SingleCheckpointBarrierHandler.class);

	private final String taskName;

	private final CheckpointBarrierBehaviourController controller;

	private int numBarriersReceived;

	/**
	 * The checkpoint id to guarantee that we would trigger only one checkpoint when reading the same barrier from
	 * different channels.
	 */
	private long currentCheckpointId = -1L;

	private int numOpenChannels;

	private CompletableFuture<Void> allBarriersReceivedFuture = FutureUtils.completedVoidFuture();

	@VisibleForTesting
	static SingleCheckpointBarrierHandler createUnalignedCheckpointBarrierHandler(
			SubtaskCheckpointCoordinator checkpointCoordinator,
			String taskName,
			AbstractInvokable toNotifyOnCheckpoint,
			CheckpointableInput... inputs) {
		return new SingleCheckpointBarrierHandler(
			taskName,
			toNotifyOnCheckpoint,
			(int) Arrays.stream(inputs).flatMap(gate -> gate.getChannelInfos().stream()).count(),
			new UnalignedController(checkpointCoordinator, inputs));
	}

	SingleCheckpointBarrierHandler(
			String taskName,
			AbstractInvokable toNotifyOnCheckpoint,
			int numOpenChannels,
			CheckpointBarrierBehaviourController controller) {
		super(toNotifyOnCheckpoint);

		this.taskName = taskName;
		this.numOpenChannels = numOpenChannels;
		this.controller = controller;
	}

	@Override
	public void processBarrier(CheckpointBarrier barrier, InputChannelInfo channelInfo) throws IOException {
		long barrierId = barrier.getId();
		LOG.debug("{}: Received barrier from channel {} @ {}.", taskName, channelInfo, barrierId);

		if (currentCheckpointId > barrierId || (currentCheckpointId == barrierId && !isCheckpointPending())) {
			controller.obsoleteBarrierReceived(channelInfo, barrier);
			return;
		}

		if (currentCheckpointId < barrierId) {
			if (isCheckpointPending()) {
				cancelSubsumedCheckpoint(barrierId);
			}

			if (getNumOpenChannels() == 1) {
				markAlignmentStartAndEnd(barrier.getTimestamp());
			} else {
				markAlignmentStart(barrier.getTimestamp());
			}
			currentCheckpointId = barrierId;
			numBarriersReceived = 0;
			allBarriersReceivedFuture = new CompletableFuture<>();
			try {
				if (controller.preProcessFirstBarrier(channelInfo, barrier)) {
					LOG.debug("{}: Triggering checkpoint {} on the first barrier at {}.",
						taskName,
						barrier.getId(),
						barrier.getTimestamp());
					notifyCheckpoint(barrier);
				}
			} catch (CheckpointException e) {
				abortInternal(barrier.getId(), e);
				return;
			}
		}

		controller.barrierReceived(channelInfo, barrier);

		if (currentCheckpointId == barrierId) {
			if (++numBarriersReceived == numOpenChannels) {
				if (getNumOpenChannels() > 1) {
					markAlignmentEnd();
				}
				numBarriersReceived = 0;
				if (controller.postProcessLastBarrier(channelInfo, barrier)) {
					LOG.debug("{}: Triggering checkpoint {} on the last barrier at {}.",
						taskName,
						barrier.getId(),
						barrier.getTimestamp());
					notifyCheckpoint(barrier);
				}
				allBarriersReceivedFuture.complete(null);
			}
		}
	}

	@Override
	public void processBarrierAnnouncement(
			CheckpointBarrier announcedBarrier,
			int sequenceNumber,
			InputChannelInfo channelInfo) throws IOException {
		// TODO: FLINK-19681
	}

	@Override
	public void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws IOException {
		final long cancelledId = cancelBarrier.getCheckpointId();
		if (cancelledId > currentCheckpointId || (cancelledId == currentCheckpointId && numBarriersReceived > 0)) {
			abortInternal(cancelledId, new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
		}
	}

	private void abortInternal(long cancelledId, CheckpointException exception) throws IOException {
		// by setting the currentCheckpointId to this checkpoint while keeping the numBarriers
		// at zero means that no checkpoint barrier can start a new alignment
		currentCheckpointId = Math.max(cancelledId, currentCheckpointId);
		numBarriersReceived = 0;
		controller.abortPendingCheckpoint(cancelledId, exception);
		allBarriersReceivedFuture.completeExceptionally(exception);
		notifyAbort(cancelledId, exception);
	}

	@Override
	public void processEndOfPartition() throws IOException {
		numOpenChannels--;

		if (isCheckpointPending()) {
			LOG.warn(
				"{}: Received EndOfPartition(-1) before completing current checkpoint {}. Skipping current checkpoint.",
				taskName,
				currentCheckpointId);
			numBarriersReceived = 0;
			CheckpointException exception = new CheckpointException(CHECKPOINT_DECLINED_INPUT_END_OF_STREAM);
			controller.abortPendingCheckpoint(currentCheckpointId, exception);
			allBarriersReceivedFuture.completeExceptionally(exception);
			notifyAbort(currentCheckpointId, exception);
		}
	}

	@Override
	public long getLatestCheckpointId() {
		return currentCheckpointId;
	}

	@Override
	public void close() throws IOException {
		allBarriersReceivedFuture.cancel(false);
		super.close();
	}

	@Override
	protected boolean isCheckpointPending() {
		return numBarriersReceived > 0;
	}

	private void cancelSubsumedCheckpoint(long barrierId) throws IOException {
		CheckpointException exception = new CheckpointException("Barrier id: " + barrierId,
			CHECKPOINT_DECLINED_SUBSUMED);
		// we did not complete the current checkpoint, another started before
		LOG.warn("{}: Received checkpoint barrier for checkpoint {} before completing current checkpoint {}. " +
				"Skipping current checkpoint.",
			taskName,
			barrierId,
			currentCheckpointId);

		// let the task know we are not completing this
		controller.abortPendingCheckpoint(currentCheckpointId, exception);
		allBarriersReceivedFuture.completeExceptionally(exception);
		notifyAbort(currentCheckpointId, exception);
	}

	public CompletableFuture<Void> getAllBarriersReceivedFuture(long checkpointId) {
		if (checkpointId < currentCheckpointId) {
			return FutureUtils.completedVoidFuture();
		}
		if (checkpointId > currentCheckpointId) {
			throw new IllegalStateException("Checkpoint " + checkpointId + " has not been started at all");
		}
		return allBarriersReceivedFuture;
	}

	@VisibleForTesting
	int getNumOpenChannels() {
		return numOpenChannels;
	}

	@Override
	public String toString() {
		return String.format("%s: current checkpoint: %d, current barriers: %d, open channels: %d",
			taskName,
			currentCheckpointId,
			numBarriersReceived,
			numOpenChannels);
	}
}
