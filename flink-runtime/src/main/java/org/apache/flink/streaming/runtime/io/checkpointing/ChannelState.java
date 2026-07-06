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

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.FetchedChannelStateReader;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecoveryCheckpointTrigger;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.util.ExceptionUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * A controller for keeping track of channels state in {@link AbstractAlignedBarrierHandlerState}
 * and {@link AbstractAlternatingAlignedBarrierHandlerState}.
 */
final class ChannelState {

    private final Map<InputChannelInfo, Integer> sequenceNumberInAnnouncedChannels =
            new HashMap<>();

    /**
     * {@link #blockedChannels} are the ones for which we have already processed {@link
     * CheckpointBarrier}. {@link #sequenceNumberInAnnouncedChannels} on the other hand, are the
     * ones that we have processed announcement but not yet a barrier.
     */
    private final Set<InputChannelInfo> blockedChannels = new HashSet<>();

    private final CheckpointableInput[] inputs;

    private final RecoveryCheckpointTrigger recoveryCheckpointTrigger;

    private final ChannelStateWriter channelStateWriter;

    public ChannelState(CheckpointableInput[] inputs) {
        this(inputs, RecoveryCheckpointTrigger.NO_OP, ChannelStateWriter.NO_OP);
    }

    public ChannelState(
            CheckpointableInput[] inputs,
            RecoveryCheckpointTrigger recoveryCheckpointTrigger,
            ChannelStateWriter channelStateWriter) {
        this.inputs = inputs;
        this.recoveryCheckpointTrigger = checkNotNull(recoveryCheckpointTrigger);
        this.channelStateWriter = checkNotNull(channelStateWriter);
    }

    public void blockChannel(InputChannelInfo channelInfo) {
        inputs[channelInfo.getGateIdx()].blockConsumption(channelInfo);
        blockedChannels.add(channelInfo);
    }

    public void channelFinished(InputChannelInfo channelInfo) {
        blockedChannels.remove(channelInfo);
        sequenceNumberInAnnouncedChannels.remove(channelInfo);
    }

    public void prioritizeAllAnnouncements() throws IOException {
        for (Map.Entry<InputChannelInfo, Integer> announcedNumberInChannel :
                sequenceNumberInAnnouncedChannels.entrySet()) {
            InputChannelInfo channelInfo = announcedNumberInChannel.getKey();
            inputs[channelInfo.getGateIdx()].convertToPriorityEvent(
                    channelInfo.getInputChannelIdx(), announcedNumberInChannel.getValue());
        }
        sequenceNumberInAnnouncedChannels.clear();
    }

    public void unblockAllChannels() throws IOException {
        for (InputChannelInfo blockedChannel : blockedChannels) {
            inputs[blockedChannel.getGateIdx()].resumeConsumption(blockedChannel);
        }
        blockedChannels.clear();
    }

    public CheckpointableInput[] getInputs() {
        return inputs;
    }

    public void addSeenAnnouncement(InputChannelInfo channelInfo, int sequenceNumber) {
        this.sequenceNumberInAnnouncedChannels.put(channelInfo, sequenceNumber);
    }

    public void removeSeenAnnouncement(InputChannelInfo channelInfo) {
        this.sequenceNumberInAnnouncedChannels.remove(channelInfo);
    }

    public ChannelState emptyState() {
        checkState(
                blockedChannels.isEmpty(),
                "We should not reset to an empty state if there are blocked channels: %s",
                blockedChannels);
        sequenceNumberInAnnouncedChannels.clear();
        return this;
    }

    /**
     * Transfers spill-snapshot ownership to the writer after all inputs observe checkpoint start.
     */
    public void onCheckpointStartedForAllInputs(CheckpointBarrier barrier)
            throws CheckpointException, IOException {
        long cpId = barrier.getId();
        FetchedChannelStateReader snap = null;
        try {
            snap = recoveryCheckpointTrigger.snapshotAndInsertBarriers(cpId);

            for (CheckpointableInput input : inputs) {
                input.checkpointStarted(barrier);
            }

            channelStateWriter.addInputDataFromSpill(cpId, snap);
        } catch (Throwable t) {
            if (snap != null) {
                try {
                    snap.close();
                } catch (Exception suppressed) {
                    t.addSuppressed(suppressed);
                }
            }
            if (t instanceof CheckpointException) {
                throw (CheckpointException) t;
            }
            ExceptionUtils.rethrowIOException(t);
        }
    }
}
