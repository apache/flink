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

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/** We are performing aligned checkpoints. We have seen at least a single aligned * barrier. */
final class CollectingBarriers extends AbstractAlignedBarrierHandlerState {

    CollectingBarriers(ChannelState context) {
        super(context);
    }

    @Override
    protected BarrierHandlerState convertAfterBarrierReceived(ChannelState state) {
        return this;
    }

    @Override
    public BarrierHandlerState endOfPartitionReceived(
            Controller controller, InputChannelInfo channelInfo) throws IOException {
        state.channelFinished(channelInfo);
        if (controller.allBarriersReceived()) {
            checkState(
                    controller.getPendingCheckpointBarrier() != null,
                    "At least one barrier received in collecting barrier state.");
            return triggerGlobalCheckpoint(controller, controller.getPendingCheckpointBarrier());
        }

        return this;
    }
}
