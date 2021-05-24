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

import static org.apache.flink.util.Preconditions.checkState;

/** Actions to be taken when processing aligned checkpoints. */
abstract class AbstractAlignedBarrierHandlerState implements BarrierHandlerState {

    private final ChannelState state;

    protected AbstractAlignedBarrierHandlerState(ChannelState state) {
        this.state = state;
    }

    @Override
    public final BarrierHandlerState alignmentTimeout(
            Controller controller, CheckpointBarrier checkpointBarrier)
            throws IOException, CheckpointException {
        throw new IllegalStateException(
                "Alignment should not be timed out if we are not alternating.");
    }

    @Override
    public final BarrierHandlerState announcementReceived(
            Controller controller, InputChannelInfo channelInfo, int sequenceNumber) {
        return this;
    }

    @Override
    public final BarrierHandlerState barrierReceived(
            Controller controller,
            InputChannelInfo channelInfo,
            CheckpointBarrier checkpointBarrier)
            throws IOException, CheckpointException {
        checkState(!checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint());
        state.blockChannel(channelInfo);
        if (controller.allBarriersReceived()) {
            controller.triggerGlobalCheckpoint(checkpointBarrier);
            state.unblockAllChannels();
            return new WaitingForFirstBarrier(state.getInputs());
        }

        return convertAfterBarrierReceived(state);
    }

    protected abstract BarrierHandlerState convertAfterBarrierReceived(ChannelState state);

    @Override
    public final BarrierHandlerState abort(long cancelledId) throws IOException {
        state.unblockAllChannels();
        return new WaitingForFirstBarrier(state.getInputs());
    }
}
