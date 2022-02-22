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

package org.apache.flink.runtime.io.network;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionFactory;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateFactory;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.ShuffleServiceFactory;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.io.network.metrics.NettyShuffleMetricFactory.registerShuffleMetrics;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Netty based shuffle service implementation. */
public class NettyShuffleServiceFactory
        implements ShuffleServiceFactory<NettyShuffleDescriptor, ResultPartition, SingleInputGate> {

    private static final Logger LOG = LoggerFactory.getLogger(NettyShuffleServiceFactory.class);
    private static final String DIR_NAME_PREFIX = "netty-shuffle";

    @Override
    public NettyShuffleMaster createShuffleMaster(ShuffleMasterContext shuffleMasterContext) {
        return new NettyShuffleMaster(shuffleMasterContext.getConfiguration());
    }

    @Override
    public NettyShuffleEnvironment createShuffleEnvironment(
            ShuffleEnvironmentContext shuffleEnvironmentContext) {
        checkNotNull(shuffleEnvironmentContext);
        NettyShuffleEnvironmentConfiguration networkConfig =
                NettyShuffleEnvironmentConfiguration.fromConfiguration(
                        shuffleEnvironmentContext.getConfiguration(),
                        shuffleEnvironmentContext.getNetworkMemorySize(),
                        shuffleEnvironmentContext.isLocalCommunicationOnly(),
                        shuffleEnvironmentContext.getHostAddress());
        return createNettyShuffleEnvironment(
                networkConfig,
                shuffleEnvironmentContext.getTaskExecutorResourceId(),
                shuffleEnvironmentContext.getEventPublisher(),
                shuffleEnvironmentContext.getParentMetricGroup(),
                shuffleEnvironmentContext.getIoExecutor());
    }

    @VisibleForTesting
    static NettyShuffleEnvironment createNettyShuffleEnvironment(
            NettyShuffleEnvironmentConfiguration config,
            ResourceID taskExecutorResourceId,
            TaskEventPublisher taskEventPublisher,
            MetricGroup metricGroup,
            Executor ioExecutor) {
        return createNettyShuffleEnvironment(
                config,
                taskExecutorResourceId,
                taskEventPublisher,
                new ResultPartitionManager(),
                metricGroup,
                ioExecutor);
    }

    @VisibleForTesting
    static NettyShuffleEnvironment createNettyShuffleEnvironment(
            NettyShuffleEnvironmentConfiguration config,
            ResourceID taskExecutorResourceId,
            TaskEventPublisher taskEventPublisher,
            ResultPartitionManager resultPartitionManager,
            MetricGroup metricGroup,
            Executor ioExecutor) {
        NettyConfig nettyConfig = config.nettyConfig();
        ConnectionManager connectionManager =
                nettyConfig != null
                        ? new NettyConnectionManager(
                                resultPartitionManager, taskEventPublisher, nettyConfig)
                        : new LocalConnectionManager();
        return createNettyShuffleEnvironment(
                config,
                taskExecutorResourceId,
                taskEventPublisher,
                resultPartitionManager,
                connectionManager,
                metricGroup,
                ioExecutor);
    }

    @VisibleForTesting
    public static NettyShuffleEnvironment createNettyShuffleEnvironment(
            NettyShuffleEnvironmentConfiguration config,
            ResourceID taskExecutorResourceId,
            TaskEventPublisher taskEventPublisher,
            ResultPartitionManager resultPartitionManager,
            ConnectionManager connectionManager,
            MetricGroup metricGroup,
            Executor ioExecutor) {
        checkNotNull(config);
        checkNotNull(taskExecutorResourceId);
        checkNotNull(taskEventPublisher);
        checkNotNull(resultPartitionManager);
        checkNotNull(metricGroup);
        checkNotNull(connectionManager);

        FileChannelManager fileChannelManager =
                new FileChannelManagerImpl(config.getTempDirs(), DIR_NAME_PREFIX);
        if (LOG.isInfoEnabled()) {
            LOG.info(
                    "Created a new {} for storing result partitions of BLOCKING shuffles. Used directories:\n\t{}",
                    FileChannelManager.class.getSimpleName(),
                    Arrays.stream(fileChannelManager.getPaths())
                            .map(File::getAbsolutePath)
                            .collect(Collectors.joining("\n\t")));
        }

        NetworkBufferPool networkBufferPool =
                new NetworkBufferPool(
                        config.numNetworkBuffers(),
                        config.networkBufferSize(),
                        config.getRequestSegmentsTimeout());

        // we create a separated buffer pool here for batch shuffle instead of reusing the network
        // buffer pool directly to avoid potential side effects of memory contention, for example,
        // dead lock or "insufficient network buffer" error
        BatchShuffleReadBufferPool batchShuffleReadBufferPool =
                new BatchShuffleReadBufferPool(
                        config.batchShuffleReadMemoryBytes(), config.networkBufferSize());

        // we create a separated IO executor pool here for batch shuffle instead of reusing the
        // TaskManager IO executor pool directly to avoid the potential side effects of execution
        // contention, for example, too long IO or waiting time leading to starvation or timeout
        ExecutorService batchShuffleReadIOExecutor =
                Executors.newFixedThreadPool(
                        Math.max(
                                1,
                                Math.min(
                                        batchShuffleReadBufferPool.getMaxConcurrentRequests(),
                                        4 * Hardware.getNumberCPUCores())),
                        new ExecutorThreadFactory("blocking-shuffle-io"));

        registerShuffleMetrics(metricGroup, networkBufferPool);

        ResultPartitionFactory resultPartitionFactory =
                new ResultPartitionFactory(
                        resultPartitionManager,
                        fileChannelManager,
                        networkBufferPool,
                        batchShuffleReadBufferPool,
                        batchShuffleReadIOExecutor,
                        config.getBlockingSubpartitionType(),
                        config.networkBuffersPerChannel(),
                        config.floatingNetworkBuffersPerGate(),
                        config.networkBufferSize(),
                        config.isBlockingShuffleCompressionEnabled(),
                        config.getCompressionCodec(),
                        config.getMaxBuffersPerChannel(),
                        config.sortShuffleMinBuffers(),
                        config.sortShuffleMinParallelism(),
                        config.isSSLEnabled());

        SingleInputGateFactory singleInputGateFactory =
                new SingleInputGateFactory(
                        taskExecutorResourceId,
                        config,
                        connectionManager,
                        resultPartitionManager,
                        taskEventPublisher,
                        networkBufferPool);

        return new NettyShuffleEnvironment(
                taskExecutorResourceId,
                config,
                networkBufferPool,
                connectionManager,
                resultPartitionManager,
                fileChannelManager,
                resultPartitionFactory,
                singleInputGateFactory,
                ioExecutor,
                batchShuffleReadBufferPool,
                batchShuffleReadIOExecutor);
    }
}
