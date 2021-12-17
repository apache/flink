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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.LocalExecutionPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.NetworkPartitionConnectionInfo;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor.PartitionConnectionInfo;
import org.apache.flink.runtime.util.ConfigurationParserUtils;

import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default {@link ShuffleMaster} for netty and local file based shuffle implementation. */
public class NettyShuffleMaster implements ShuffleMaster<NettyShuffleDescriptor> {

    private final int buffersPerInputChannel;

    private final int buffersPerInputGate;

    private final int sortShuffleMinParallelism;

    private final int sortShuffleMinBuffers;

    private final int networkBufferSize;

    public NettyShuffleMaster(Configuration conf) {
        checkNotNull(conf);
        buffersPerInputChannel =
                conf.getInteger(NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL);
        buffersPerInputGate =
                conf.getInteger(NettyShuffleEnvironmentOptions.NETWORK_EXTRA_BUFFERS_PER_GATE);
        sortShuffleMinParallelism =
                conf.getInteger(
                        NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_PARALLELISM);
        sortShuffleMinBuffers =
                conf.getInteger(NettyShuffleEnvironmentOptions.NETWORK_SORT_SHUFFLE_MIN_BUFFERS);
        networkBufferSize = ConfigurationParserUtils.getPageSize(conf);
    }

    @Override
    public CompletableFuture<NettyShuffleDescriptor> registerPartitionWithProducer(
            JobID jobID,
            PartitionDescriptor partitionDescriptor,
            ProducerDescriptor producerDescriptor) {

        ResultPartitionID resultPartitionID =
                new ResultPartitionID(
                        partitionDescriptor.getPartitionId(),
                        producerDescriptor.getProducerExecutionId());

        NettyShuffleDescriptor shuffleDeploymentDescriptor =
                new NettyShuffleDescriptor(
                        producerDescriptor.getProducerLocation(),
                        createConnectionInfo(
                                producerDescriptor, partitionDescriptor.getConnectionIndex()),
                        resultPartitionID);

        return CompletableFuture.completedFuture(shuffleDeploymentDescriptor);
    }

    @Override
    public void releasePartitionExternally(ShuffleDescriptor shuffleDescriptor) {}

    private static PartitionConnectionInfo createConnectionInfo(
            ProducerDescriptor producerDescriptor, int connectionIndex) {
        return producerDescriptor.getDataPort() >= 0
                ? NetworkPartitionConnectionInfo.fromProducerDescriptor(
                        producerDescriptor, connectionIndex)
                : LocalExecutionPartitionConnectionInfo.INSTANCE;
    }

    /**
     * JM announces network memory requirement from the calculating result of this method. Please
     * note that the calculating algorithm depends on both I/O details of a vertex and network
     * configuration, e.g. {@link NettyShuffleEnvironmentOptions#NETWORK_BUFFERS_PER_CHANNEL} and
     * {@link NettyShuffleEnvironmentOptions#NETWORK_EXTRA_BUFFERS_PER_GATE}, which means we should
     * always keep the consistency of configurations between JM, RM and TM in fine-grained resource
     * management, thus to guarantee that the processes of memory announcing and allocating respect
     * each other.
     */
    @Override
    public MemorySize computeShuffleMemorySizeForTask(TaskInputsOutputsDescriptor desc) {
        checkNotNull(desc);

        int numTotalInputChannels =
                desc.getInputChannelNums().values().stream().mapToInt(Integer::intValue).sum();
        int numTotalInputGates = desc.getInputChannelNums().size();

        int numRequiredNetworkBuffers =
                NettyShuffleUtils.computeNetworkBuffersForAnnouncing(
                        buffersPerInputChannel,
                        buffersPerInputGate,
                        sortShuffleMinParallelism,
                        sortShuffleMinBuffers,
                        numTotalInputChannels,
                        numTotalInputGates,
                        desc.getSubpartitionNums(),
                        desc.getPartitionTypes());

        return new MemorySize((long) networkBufferSize * numRequiredNetworkBuffers);
    }
}
