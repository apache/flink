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

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Represents a state in a state machine of processing a checkpoint. There are 4 base states:
 *
 * <ul>
 *   <li>Waiting for an aligned barrier
 *   <li>Collecting aligned barriers
 *   <li>Waiting for an unaligned barrier
 *   <li>Collecting unaligned barriers
 * </ul>
 *
 * <p>Additionally depending on the configuration we can switch between aligned and unaligned
 * actions.
 */
interface BarrierHandlerState {
    BarrierHandlerState alignedCheckpointTimeout(
            Controller controller, CheckpointBarrier checkpointBarrier)
            throws IOException, CheckpointException;

    BarrierHandlerState announcementReceived(
            Controller controller, InputChannelInfo channelInfo, int sequenceNumber)
            throws IOException;

    BarrierHandlerState barrierReceived(
            Controller controller,
            InputChannelInfo channelInfo,
            CheckpointBarrier checkpointBarrier,
            boolean markChannelBlocked)
            throws IOException, CheckpointException;

    BarrierHandlerState abort(long cancelledId) throws IOException;

    BarrierHandlerState endOfPartitionReceived(Controller controller, InputChannelInfo channelInfo)
            throws IOException, CheckpointException;

    /**
     * An entry point for communication between {@link BarrierHandlerState} and {@link
     * SingleCheckpointBarrierHandler}.
     */
    interface Controller {
        boolean allBarriersReceived();

        @Nullable
        CheckpointBarrier getPendingCheckpointBarrier();

        void triggerGlobalCheckpoint(CheckpointBarrier checkpointBarrier) throws IOException;

        void initInputsCheckpoint(CheckpointBarrier checkpointBarrier) throws CheckpointException;

        boolean isTimedOut(CheckpointBarrier barrier);
    }
}
