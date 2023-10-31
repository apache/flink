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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An input channel reads recovered state from previous unaligned checkpoint snapshots and then
 * converts into {@link RemoteInputChannel} finally.
 */
public class RemoteRecoveredInputChannel extends RecoveredInputChannel {
    private final ConnectionID connectionId;
    private final ConnectionManager connectionManager;
    private final int partitionRequestListenerTimeout;

    RemoteRecoveredInputChannel(
            SingleInputGate inputGate,
            int channelIndex,
            ResultPartitionID partitionId,
            int consumedSubpartitionIndex,
            ConnectionID connectionId,
            ConnectionManager connectionManager,
            int initialBackOff,
            int maxBackoff,
            int partitionRequestListenerTimeout,
            int networkBuffersPerChannel,
            InputChannelMetrics metrics) {
        super(
                inputGate,
                channelIndex,
                partitionId,
                consumedSubpartitionIndex,
                initialBackOff,
                maxBackoff,
                metrics.getNumBytesInRemoteCounter(),
                metrics.getNumBuffersInRemoteCounter(),
                networkBuffersPerChannel);

        this.connectionId = checkNotNull(connectionId);
        this.connectionManager = checkNotNull(connectionManager);
        this.partitionRequestListenerTimeout = partitionRequestListenerTimeout;
    }

    @Override
    protected InputChannel toInputChannelInternal() throws IOException {
        RemoteInputChannel remoteInputChannel =
                new RemoteInputChannel(
                        inputGate,
                        getChannelIndex(),
                        partitionId,
                        consumedSubpartitionIndex,
                        connectionId,
                        connectionManager,
                        initialBackoff,
                        maxBackoff,
                        partitionRequestListenerTimeout,
                        networkBuffersPerChannel,
                        numBytesIn,
                        numBuffersIn,
                        channelStateWriter);
        remoteInputChannel.setup();
        return remoteInputChannel;
    }
}
