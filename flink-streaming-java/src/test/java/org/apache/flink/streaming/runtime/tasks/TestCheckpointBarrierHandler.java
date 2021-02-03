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

package org.apache.flink.streaming.runtime.tasks;

import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.streaming.runtime.io.checkpointing.CheckpointBarrierHandler;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A test {@link TestCheckpointBarrierHandler} that records the history of checkpoint triggering.
 */
public class TestCheckpointBarrierHandler extends CheckpointBarrierHandler {
    private final List<CheckpointBarrier> triggeredCheckpoints = new ArrayList<>();

    public TestCheckpointBarrierHandler(AbstractInvokable toNotifyOnCheckpoint) {
        super(toNotifyOnCheckpoint);
    }

    @Override
    public boolean triggerCheckpoint(
            CheckpointMetaData checkpointMetaData, CheckpointOptions checkpointOptions)
            throws IOException {
        triggeredCheckpoints.add(
                new CheckpointBarrier(
                        checkpointMetaData.getCheckpointId(),
                        checkpointMetaData.getTimestamp(),
                        checkpointOptions));
        return true;
    }

    public List<CheckpointBarrier> getTriggeredCheckpoints() {
        return triggeredCheckpoints;
    }

    @Override
    public void processBarrier(CheckpointBarrier receivedBarrier, InputChannelInfo channelInfo)
            throws IOException {}

    @Override
    public void processBarrierAnnouncement(
            CheckpointBarrier announcedBarrier, int sequenceNumber, InputChannelInfo channelInfo)
            throws IOException {}

    @Override
    public void processCancellationBarrier(CancelCheckpointMarker cancelBarrier)
            throws IOException {}

    @Override
    public void processEndOfPartition() throws IOException {}

    @Override
    public long getLatestCheckpointId() {
        return 0;
    }

    @Override
    protected boolean isCheckpointPending() {
        return false;
    }
}
