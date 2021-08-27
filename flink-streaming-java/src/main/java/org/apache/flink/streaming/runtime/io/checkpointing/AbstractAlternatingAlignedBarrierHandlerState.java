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
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;

import java.io.IOException;

/**
 * Actions to be taken when processing aligned checkpoints and possibly switching to unaligned
 * checkpoints.
 */
abstract class AbstractAlternatingAlignedBarrierHandlerState implements BarrierHandlerState {

    protected final ChannelState state;

    protected AbstractAlternatingAlignedBarrierHandlerState(ChannelState state) {
        this.state = state;
    }

    @Override
    public final BarrierHandlerState announcementReceived(
            Controller controller, InputChannelInfo channelInfo, int sequenceNumber) {
        state.addSeenAnnouncement(channelInfo, sequenceNumber);
        return this;
    }

    @Override
    public final BarrierHandlerState barrierReceived(
            Controller controller,
            InputChannelInfo channelInfo,
            CheckpointBarrier checkpointBarrier,
            boolean markChannelBlocked)
            throws IOException, CheckpointException {
        if (checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint()) {
            BarrierHandlerState unalignedState = alignmentTimeout(controller, checkpointBarrier);
            return unalignedState.barrierReceived(
                    controller, channelInfo, checkpointBarrier, markChannelBlocked);
        }

        state.removeSeenAnnouncement(channelInfo);

        if (markChannelBlocked) {
            state.blockChannel(channelInfo);
        }

        if (controller.allBarriersReceived()) {
            controller.triggerGlobalCheckpoint(checkpointBarrier);
            return finishCheckpoint();
        } else if (controller.isTimedOut(checkpointBarrier)) {
            return alignmentTimeout(controller, checkpointBarrier)
                    .barrierReceived(
                            controller,
                            channelInfo,
                            checkpointBarrier.asUnaligned(),
                            markChannelBlocked);
        }

        return transitionAfterBarrierReceived(state);
    }

    protected abstract BarrierHandlerState transitionAfterBarrierReceived(ChannelState state);

    @Override
    public final BarrierHandlerState abort(long cancelledId) throws IOException {
        return finishCheckpoint();
    }

    protected BarrierHandlerState finishCheckpoint() throws IOException {
        state.unblockAllChannels();
        return new AlternatingWaitingForFirstBarrier(state.emptyState());
    }
}
