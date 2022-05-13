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
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * We are performing aligned checkpoints with time out. We have seen at least a single aligned
 * barrier.
 */
final class AlternatingCollectingBarriers extends AbstractAlternatingAlignedBarrierHandlerState {

    AlternatingCollectingBarriers(ChannelState context) {
        super(context);
    }

    @Override
    public BarrierHandlerState alignmentTimeout(
            Controller controller, CheckpointBarrier checkpointBarrier)
            throws IOException, CheckpointException {
        state.prioritizeAllAnnouncements();
        CheckpointBarrier unalignedBarrier = checkpointBarrier.asUnaligned();
        controller.initInputsCheckpoint(unalignedBarrier);
        for (CheckpointableInput input : state.getInputs()) {
            input.checkpointStarted(unalignedBarrier);
        }
        controller.triggerGlobalCheckpoint(unalignedBarrier);
        return new AlternatingCollectingBarriersUnaligned(true, state);
    }

    @Override
    public BarrierHandlerState endOfPartitionReceived(
            Controller controller, InputChannelInfo channelInfo)
            throws IOException, CheckpointException {
        state.channelFinished(channelInfo);

        CheckpointBarrier pendingCheckpointBarrier = controller.getPendingCheckpointBarrier();
        checkState(
                pendingCheckpointBarrier != null,
                "At least one barrier received in collecting barrier state.");
        checkState(
                !pendingCheckpointBarrier.getCheckpointOptions().isUnalignedCheckpoint(),
                "Pending checkpoint barrier should be aligned in "
                        + "collecting aligned barrier state");

        if (controller.allBarriersReceived()) {
            controller.triggerGlobalCheckpoint(pendingCheckpointBarrier);
            return finishCheckpoint();
        } else if (controller.isTimedOut(pendingCheckpointBarrier)) {
            return alignmentTimeout(controller, pendingCheckpointBarrier)
                    .endOfPartitionReceived(controller, channelInfo);
        }

        return this;
    }

    @Override
    protected BarrierHandlerState transitionAfterBarrierReceived(ChannelState state) {
        return this;
    }
}
