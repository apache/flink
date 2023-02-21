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
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;

import java.io.IOException;

/** We are performing aligned checkpoints. We have not seen any barriers yet. */
final class WaitingForFirstBarrier extends AbstractAlignedBarrierHandlerState {

    WaitingForFirstBarrier(CheckpointableInput[] inputs) {
        super(new ChannelState(inputs));
    }

    @Override
    protected BarrierHandlerState convertAfterBarrierReceived(ChannelState state) {
        return new CollectingBarriers(state);
    }

    @Override
    public BarrierHandlerState endOfPartitionReceived(
            Controller controller, InputChannelInfo channelInfo) throws IOException {
        state.channelFinished(channelInfo);

        // Do nothing since we have no pending checkpoint.
        return this;
    }
}
