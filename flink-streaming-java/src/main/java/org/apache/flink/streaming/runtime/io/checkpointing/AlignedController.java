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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Controller for aligned checkpoints. */
@Internal
public class AlignedController implements CheckpointBarrierBehaviourController {
    private final CheckpointableInput[] inputs;

    /**
     * {@link #blockedChannels} are the ones for which we have already processed {@link
     * CheckpointBarrier} (via {@link #barrierReceived(InputChannelInfo, CheckpointBarrier)}. {@link
     * #sequenceNumberInAnnouncedChannels} on the other hand, are the ones that we have processed
     * {@link #barrierAnnouncement(InputChannelInfo, CheckpointBarrier, int,
     * DelayedActionRegistration)} but not yet {@link #barrierReceived(InputChannelInfo,
     * CheckpointBarrier)}.
     */
    private final Map<InputChannelInfo, Boolean> blockedChannels;

    private final Map<InputChannelInfo, Integer> sequenceNumberInAnnouncedChannels;

    public AlignedController(CheckpointableInput... inputs) {
        this.inputs = inputs;
        blockedChannels =
                Arrays.stream(inputs)
                        .flatMap(gate -> gate.getChannelInfos().stream())
                        .collect(Collectors.toMap(Function.identity(), info -> false));
        sequenceNumberInAnnouncedChannels = new HashMap<>();
    }

    @Override
    public Optional<CheckpointBarrier> barrierAnnouncement(
            InputChannelInfo channelInfo,
            CheckpointBarrier announcedBarrier,
            int sequenceNumber,
            DelayedActionRegistration delayedActionRegistration) {
        Integer previousValue = sequenceNumberInAnnouncedChannels.put(channelInfo, sequenceNumber);
        checkState(
                previousValue == null,
                "Stream corrupt: Repeated barrierAnnouncement [%s] overwriting [%s] for the same checkpoint on input %s",
                announcedBarrier,
                sequenceNumber,
                channelInfo);
        return Optional.empty();
    }

    @Override
    public void preProcessFirstBarrierOrAnnouncement(CheckpointBarrier barrier) {
        sequenceNumberInAnnouncedChannels.clear();
    }

    @Override
    public Optional<CheckpointBarrier> barrierReceived(
            InputChannelInfo channelInfo, CheckpointBarrier barrier) {
        checkState(
                !blockedChannels.put(channelInfo, true),
                "Stream corrupt: Repeated barrier for same checkpoint on input " + channelInfo);
        sequenceNumberInAnnouncedChannels.remove(channelInfo);
        CheckpointableInput input = inputs[channelInfo.getGateIdx()];
        input.blockConsumption(channelInfo);
        return Optional.empty();
    }

    @Override
    public Optional<CheckpointBarrier> preProcessFirstBarrier(
            InputChannelInfo channelInfo, CheckpointBarrier barrier) {
        checkArgument(
                !barrier.getCheckpointOptions().isUnalignedCheckpoint(),
                "Unaligned barrier is not expected");
        return Optional.empty();
    }

    @Override
    public Optional<CheckpointBarrier> postProcessLastBarrier(
            InputChannelInfo channelInfo, CheckpointBarrier barrier) throws IOException {
        checkState(!barrier.getCheckpointOptions().isUnalignedCheckpoint());
        resetPendingCheckpoint(barrier.getId());
        resumeConsumption();
        return Optional.of(barrier);
    }

    @Override
    public void abortPendingCheckpoint(long cancelledId, CheckpointException exception)
            throws IOException {
        resetPendingCheckpoint(cancelledId);
        resumeConsumption();
    }

    @Override
    public void obsoleteBarrierReceived(InputChannelInfo channelInfo, CheckpointBarrier barrier)
            throws IOException {
        resumeConsumption(channelInfo);
    }

    protected void resetPendingCheckpoint(long cancelledId) {
        for (final CheckpointableInput input : inputs) {
            input.checkpointStopped(cancelledId);
        }
    }

    public List<InputChannelInfo> getBlockedChannels() {
        return blockedChannels.entrySet().stream()
                .filter(Map.Entry::getValue)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }

    Map<InputChannelInfo, Integer> getSequenceNumberInAnnouncedChannels() {
        return Collections.unmodifiableMap(sequenceNumberInAnnouncedChannels);
    }

    public void resumeConsumption() throws IOException {
        for (Map.Entry<InputChannelInfo, Boolean> blockedChannel : blockedChannels.entrySet()) {
            if (blockedChannel.getValue()) {
                resumeConsumption(blockedChannel.getKey());
            }
            blockedChannel.setValue(false);
        }
        sequenceNumberInAnnouncedChannels.clear();
    }

    void resumeConsumption(InputChannelInfo channelInfo) throws IOException {
        CheckpointableInput input = inputs[channelInfo.getGateIdx()];
        input.resumeConsumption(channelInfo);
    }
}
