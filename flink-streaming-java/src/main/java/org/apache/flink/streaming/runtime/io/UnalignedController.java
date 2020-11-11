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
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.streaming.runtime.tasks.SubtaskCheckpointCoordinator;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.Optional;

/**
 * Controller for unaligned checkpoints.
 */
@Internal
public class UnalignedController implements CheckpointBarrierBehaviourController {

	private final SubtaskCheckpointCoordinator checkpointCoordinator;
	private final CheckpointableInput[] inputs;

	public UnalignedController(
			SubtaskCheckpointCoordinator checkpointCoordinator,
			CheckpointableInput... inputs) {
		this.checkpointCoordinator = checkpointCoordinator;
		this.inputs = inputs;
	}

	@Override
	public void preProcessFirstBarrierOrAnnouncement(CheckpointBarrier barrier) {
	}

	@Override
	public void barrierAnnouncement(
			InputChannelInfo channelInfo,
			CheckpointBarrier announcedBarrier,
			int sequenceNumber) throws IOException {
		Preconditions.checkState(announcedBarrier.isCheckpoint());
		inputs[channelInfo.getGateIdx()].convertToPriorityEvent(
			channelInfo.getInputChannelIdx(),
			sequenceNumber);
	}

	@Override
	public Optional<CheckpointBarrier> barrierReceived(InputChannelInfo channelInfo, CheckpointBarrier barrier) {
		return Optional.empty();
	}

	@Override
	public Optional<CheckpointBarrier> preProcessFirstBarrier(InputChannelInfo channelInfo, CheckpointBarrier barrier) throws IOException, CheckpointException {
		Preconditions.checkArgument(barrier.getCheckpointOptions().isUnalignedCheckpoint(), "Aligned barrier not expected");
		checkpointCoordinator.initCheckpoint(barrier.getId(), barrier.getCheckpointOptions());
		for (final CheckpointableInput input : inputs) {
			input.checkpointStarted(barrier);
		}
		return Optional.of(barrier);
	}

	@Override
	public Optional<CheckpointBarrier> postProcessLastBarrier(InputChannelInfo channelInfo, CheckpointBarrier barrier) {
		// note that barrier can be aligned if checkpoint timed out in between; event is not converted
		resetPendingCheckpoint(barrier.getId());
		return Optional.empty();
	}

	private void resetPendingCheckpoint(long cancelledId) {
		for (final CheckpointableInput input : inputs) {
			input.checkpointStopped(cancelledId);
		}
	}

	@Override
	public void abortPendingCheckpoint(
			long cancelledId,
			CheckpointException exception) {
		resetPendingCheckpoint(cancelledId);
	}

	@Override
	public void obsoleteBarrierReceived(
			InputChannelInfo channelInfo,
			CheckpointBarrier barrier) {
	}
}
