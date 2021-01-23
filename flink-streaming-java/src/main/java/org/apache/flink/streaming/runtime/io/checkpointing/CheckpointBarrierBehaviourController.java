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

import java.io.IOException;
import java.util.Optional;

/** Controls when the checkpoint should be actually triggered. */
@Internal
public interface CheckpointBarrierBehaviourController {

    /** Invoked before first {@link CheckpointBarrier} or it's announcement. */
    void preProcessFirstBarrierOrAnnouncement(CheckpointBarrier barrier);

    /** Invoked per every {@link CheckpointBarrier} announcement. */
    void barrierAnnouncement(
            InputChannelInfo channelInfo, CheckpointBarrier announcedBarrier, int sequenceNumber)
            throws IOException;

    /** Invoked per every received {@link CheckpointBarrier}. */
    Optional<CheckpointBarrier> barrierReceived(
            InputChannelInfo channelInfo, CheckpointBarrier barrier)
            throws IOException, CheckpointException;

    /**
     * Invoked once per checkpoint, before the first invocation of {@link
     * #barrierReceived(InputChannelInfo, CheckpointBarrier)} for that given checkpoint.
     *
     * @return {@code true} if checkpoint should be triggered.
     */
    Optional<CheckpointBarrier> preProcessFirstBarrier(
            InputChannelInfo channelInfo, CheckpointBarrier barrier)
            throws IOException, CheckpointException;

    /**
     * Invoked once per checkpoint, after the last invocation of {@link
     * #barrierReceived(InputChannelInfo, CheckpointBarrier)} for that given checkpoint.
     *
     * @return {@code true} if checkpoint should be triggered.
     */
    Optional<CheckpointBarrier> postProcessLastBarrier(
            InputChannelInfo channelInfo, CheckpointBarrier barrier)
            throws IOException, CheckpointException;

    void abortPendingCheckpoint(long cancelledId, CheckpointException exception) throws IOException;

    void obsoleteBarrierReceived(InputChannelInfo channelInfo, CheckpointBarrier barrier)
            throws IOException;
}
