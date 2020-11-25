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

import java.io.IOException;

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
	public void barrierReceived(InputChannelInfo channelInfo, CheckpointBarrier barrier) {
	}

	@Override
	public boolean preProcessFirstBarrier(InputChannelInfo channelInfo, CheckpointBarrier barrier) throws IOException, CheckpointException {
		checkpointCoordinator.initCheckpoint(barrier.getId(), barrier.getCheckpointOptions());
		for (final CheckpointableInput input : inputs) {
			input.checkpointStarted(barrier);
		}
		return true;
	}

	@Override
	public boolean postProcessLastBarrier(InputChannelInfo channelInfo, CheckpointBarrier barrier) {
		resetPendingCheckpoint(barrier.getId());
		return false;
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
