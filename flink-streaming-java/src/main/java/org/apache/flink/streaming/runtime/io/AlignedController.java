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

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Controller for aligned checkpoints.
 */
@Internal
public class AlignedController implements CheckpointBarrierBehaviourController {
	private final CheckpointableInput[] inputs;

	private final Map<InputChannelInfo, Boolean> blockedChannels;

	public AlignedController(CheckpointableInput... inputs) {
		this.inputs = inputs;
		blockedChannels = Arrays.stream(inputs)
			.flatMap(gate -> gate.getChannelInfos().stream())
			.collect(Collectors.toMap(Function.identity(), info -> false));
	}

	@Override
	public void barrierReceived(
			InputChannelInfo channelInfo,
			CheckpointBarrier barrier) {
		checkState(!blockedChannels.put(channelInfo, true), "Stream corrupt: Repeated barrier for same checkpoint on input " + channelInfo);
		CheckpointableInput input = inputs[channelInfo.getGateIdx()];
		input.blockConsumption(channelInfo);
	}

	@Override
	public boolean preProcessFirstBarrier(
			InputChannelInfo channelInfo,
			CheckpointBarrier barrier) {
		return false;
	}

	@Override
	public boolean postProcessLastBarrier(
			InputChannelInfo channelInfo,
			CheckpointBarrier barrier) throws IOException {
		resumeConsumption();
		return true;
	}

	@Override
	public void abortPendingCheckpoint(
			long cancelledId,
			CheckpointException exception) throws IOException {
		resumeConsumption();
	}

	@Override
	public void obsoleteBarrierReceived(
			InputChannelInfo channelInfo,
			CheckpointBarrier barrier) throws IOException {
		resumeConsumption(channelInfo);
	}

	private void resumeConsumption() throws IOException {
		for (Map.Entry<InputChannelInfo, Boolean> blockedChannel : blockedChannels.entrySet()) {
			if (blockedChannel.getValue()) {
				resumeConsumption(blockedChannel.getKey());
			}
			blockedChannel.setValue(false);
		}
	}

	private void resumeConsumption(InputChannelInfo channelInfo) throws IOException {
		CheckpointableInput input = inputs[channelInfo.getGateIdx()];
		input.resumeConsumption(channelInfo);
	}
}
