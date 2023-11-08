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

package org.apache.flink.streaming.runtime.io.benchmark;

import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.network.ConnectionID;
import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.metrics.InputChannelMetrics;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.LocalInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;

import java.io.IOException;

/**
 * A benchmark-specific input gate factory which overrides the respective methods of creating {@link
 * RemoteInputChannel} and {@link LocalInputChannel} for requesting specific subpartitions.
 */
public class SingleInputGateBenchmarkFactory extends SingleInputGateFactory {

    public SingleInputGateBenchmarkFactory(
            ResourceID taskExecutorResourceId,
            NettyShuffleEnvironmentConfiguration networkConfig,
            ConnectionManager connectionManager,
            ResultPartitionManager partitionManager,
            TaskEventPublisher taskEventPublisher,
            NetworkBufferPool networkBufferPool) {
        super(
                taskExecutorResourceId,
                networkConfig,
                connectionManager,
                partitionManager,
                taskEventPublisher,
                networkBufferPool,
                null,
                null);
    }

    @Override
    protected InputChannel createKnownInputChannel(
            SingleInputGate inputGate,
            int index,
            int buffersPerChannel,
            NettyShuffleDescriptor inputChannelDescriptor,
            int consumedSubpartitionIndex,
            SingleInputGateFactory.ChannelStatistics channelStatistics,
            InputChannelMetrics metrics) {
        ResultPartitionID partitionId = inputChannelDescriptor.getResultPartitionID();
        if (inputChannelDescriptor.isLocalTo(taskExecutorResourceId)) {
            return new TestLocalInputChannel(
                    inputGate,
                    index,
                    partitionId,
                    index,
                    partitionManager,
                    taskEventPublisher,
                    partitionRequestInitialBackoff,
                    partitionRequestMaxBackoff,
                    metrics);
        } else {
            return new TestRemoteInputChannel(
                    inputGate,
                    index,
                    partitionId,
                    index,
                    inputChannelDescriptor.getConnectionId(),
                    connectionManager,
                    partitionRequestInitialBackoff,
                    partitionRequestMaxBackoff,
                    partitionRequestListenerTimeout,
                    configuredNetworkBuffersPerChannel,
                    metrics);
        }
    }

    /**
     * A {@link LocalInputChannel} which ignores the given subpartition index and uses channel index
     * instead when requesting subpartition.
     */
    static class TestLocalInputChannel extends LocalInputChannel {

        private final ResultPartitionID newPartitionID = new ResultPartitionID();

        public TestLocalInputChannel(
                SingleInputGate inputGate,
                int channelIndex,
                ResultPartitionID partitionId,
                int consumedSubpartitionIndex,
                ResultPartitionManager partitionManager,
                TaskEventPublisher taskEventPublisher,
                int initialBackoff,
                int maxBackoff,
                InputChannelMetrics metrics) {
            super(
                    inputGate,
                    channelIndex,
                    partitionId,
                    consumedSubpartitionIndex,
                    partitionManager,
                    taskEventPublisher,
                    initialBackoff,
                    maxBackoff,
                    metrics.getNumBytesInLocalCounter(),
                    metrics.getNumBuffersInLocalCounter(),
                    ChannelStateWriter.NO_OP);
        }

        @Override
        public void requestSubpartition() throws IOException {
            super.requestSubpartition();
        }

        @Override
        public ResultPartitionID getPartitionId() {
            // the SingleInputGate assumes that all InputChannels are consuming different
            // ResultPartition
            // so can be distinguished by ResultPartitionID. However, the micro benchmark breaks
            // this and
            // all InputChannels in a SingleInputGate consume data from the same ResultPartition. To
            // make
            // it transparent to SingleInputGate, a new and unique ResultPartitionID is returned
            // here
            return newPartitionID;
        }
    }

    /**
     * A {@link RemoteInputChannel} which ignores the given subpartition index and uses channel
     * index instead when requesting subpartition.
     */
    static class TestRemoteInputChannel extends RemoteInputChannel {

        private final ResultPartitionID newPartitionID = new ResultPartitionID();

        public TestRemoteInputChannel(
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
                    connectionId,
                    connectionManager,
                    initialBackOff,
                    maxBackoff,
                    partitionRequestListenerTimeout,
                    networkBuffersPerChannel,
                    metrics.getNumBytesInRemoteCounter(),
                    metrics.getNumBuffersInRemoteCounter(),
                    ChannelStateWriter.NO_OP);
        }

        @Override
        public void requestSubpartition() throws IOException, InterruptedException {
            super.requestSubpartition();
        }

        @Override
        public ResultPartitionID getPartitionId() {
            // the SingleInputGate assumes that all InputChannels are consuming different
            // ResultPartition
            // so can be distinguished by ResultPartitionID. However, the micro benchmark breaks
            // this and
            // all InputChannels in a SingleInputGate consume data from the same ResultPartition. To
            // make
            // it transparent to SingleInputGate, a new and unique ResultPartitionID is returned
            // here
            return newPartitionID;
        }
    }
}
