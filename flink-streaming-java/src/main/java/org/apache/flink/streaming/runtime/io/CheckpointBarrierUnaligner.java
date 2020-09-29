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

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED_SUBSUMED;

/**
 * {@link CheckpointBarrierUnaligner} is used for triggering checkpoint while reading the first barrier
 * and keeping track of the number of received barriers and consumed barriers.
 */
@Internal
@NotThreadSafe
public class CheckpointBarrierUnaligner extends CheckpointBarrierHandler {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointBarrierUnaligner.class);

	private final String taskName;

	private int numBarriersReceived;

	/** A future indicating that all barriers of the a given checkpoint have been read. */
	private CompletableFuture<Void> allBarriersReceivedFuture = FutureUtils.completedVoidFuture();

	/**
	 * The checkpoint id to guarantee that we would trigger only one checkpoint when reading the same barrier from
	 * different channels.
	 */
	private long currentCheckpointId = -1L;

	private int numOpenChannels;

	private final SubtaskCheckpointCoordinator checkpointCoordinator;

	private final CheckpointableInput[] inputs;

	CheckpointBarrierUnaligner(
			SubtaskCheckpointCoordinator checkpointCoordinator,
			String taskName,
			AbstractInvokable toNotifyOnCheckpoint,
			CheckpointableInput... inputs) {
		super(toNotifyOnCheckpoint);

		this.taskName = taskName;
		this.inputs = inputs;
		numOpenChannels = (int) Arrays.stream(inputs).flatMap(gate -> gate.getChannelInfos().stream()).count();
		this.checkpointCoordinator = checkpointCoordinator;
	}

	@Override
	public void processBarrier(CheckpointBarrier barrier, InputChannelInfo channelInfo) throws IOException {
		long barrierId = barrier.getId();
		if (currentCheckpointId > barrierId || (currentCheckpointId == barrierId && !isCheckpointPending())) {
			// ignore old and cancelled barriers
			return;
		}
		if (currentCheckpointId < barrierId) {
			if (isCheckpointPending()) {
				cancelSubsumedCheckpoint(barrierId);
			}

			markCheckpointStart(barrier.getTimestamp());
			currentCheckpointId = barrierId;
			numBarriersReceived = 0;
			allBarriersReceivedFuture = new CompletableFuture<>();
			checkpointCoordinator.initCheckpoint(barrierId, barrier.getCheckpointOptions());

			for (final CheckpointableInput input : inputs) {
				input.checkpointStarted(barrier);
			}
			notifyCheckpoint(barrier, 0);
		}
		if (currentCheckpointId == barrierId) {
			LOG.debug("{}: Received barrier from channel {} @ {}.", taskName, channelInfo, barrierId);

			if (++numBarriersReceived == numOpenChannels) {
				allBarriersReceivedFuture.complete(null);
				resetPendingCheckpoint(barrierId);
			}
		}
	}

	@Override
	public void abortPendingCheckpoint(long checkpointId, CheckpointException exception) throws IOException {
		if (checkpointId > currentCheckpointId) {
			resetPendingCheckpoint(checkpointId);
			notifyAbort(currentCheckpointId, exception);
		}
	}

	@Override
	public void processCancellationBarrier(CancelCheckpointMarker cancelBarrier) throws IOException {
		final long cancelledId = cancelBarrier.getCheckpointId();
		if (currentCheckpointId > cancelledId || (currentCheckpointId == cancelledId && numBarriersReceived == 0)) {
			return;
		}

		resetPendingCheckpoint(cancelledId);
		currentCheckpointId = cancelledId;
		notifyAbort(cancelledId,
			new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_ON_CANCELLATION_BARRIER));
	}

	@Override
	public void processEndOfPartition() throws IOException {
		numOpenChannels--;

		if (isCheckpointPending()) {
			LOG.warn(
				"{}: Received EndOfPartition(-1) before completing current checkpoint {}. Skipping current checkpoint.",
				taskName,
				currentCheckpointId);
			resetPendingCheckpoint(currentCheckpointId);
			notifyAbort(
				currentCheckpointId,
				new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED_INPUT_END_OF_STREAM));
		}
	}

	private void resetPendingCheckpoint(long cancelledId) {
		numBarriersReceived = 0;

		for (final CheckpointableInput input : inputs) {
			input.checkpointStopped(cancelledId);
		}
	}

	@Override
	public long getLatestCheckpointId() {
		return currentCheckpointId;
	}

	@Override
	public String toString() {
		return String.format("%s: last checkpoint: %d", taskName, currentCheckpointId);
	}

	@Override
	public void close() throws IOException {
		super.close();
		allBarriersReceivedFuture.cancel(false);
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
		final long currentCheckpointId = this.currentCheckpointId;
		notifyAbort(currentCheckpointId, exception);
		allBarriersReceivedFuture.completeExceptionally(exception);
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
}
