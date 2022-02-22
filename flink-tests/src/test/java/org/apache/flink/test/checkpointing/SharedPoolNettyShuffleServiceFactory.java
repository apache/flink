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

package org.apache.flink.test.checkpointing;

import org.apache.flink.runtime.io.network.ConnectionManager;
import org.apache.flink.runtime.io.network.LocalConnectionManager;
import org.apache.flink.runtime.io.network.NettyShuffleServiceFactory;
import org.apache.flink.runtime.io.network.TaskEventPublisher;
import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyConnectionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartition;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.shuffle.NettyShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleEnvironment;
import org.apache.flink.runtime.shuffle.ShuffleEnvironmentContext;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMasterContext;
import org.apache.flink.runtime.shuffle.ShuffleServiceFactory;
import org.apache.flink.runtime.taskmanager.NettyShuffleEnvironmentConfiguration;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A variation on {@link NettyShuffleServiceFactory} that uses a single {@link NettyBufferPool} for
 * all created {@link ShuffleEnvironment environments}.
 *
 * <p>Used in {@link UnalignedCheckpointTestBase}.
 */
public final class SharedPoolNettyShuffleServiceFactory
        implements ShuffleServiceFactory<NettyShuffleDescriptor, ResultPartition, SingleInputGate> {

    private final NettyShuffleServiceFactory nettyShuffleServiceFactory =
            new NettyShuffleServiceFactory();

    private static NettyBufferPool bufferPool;

    public static void resetBufferPool(int numberOfArenas) {
        bufferPool = new NettyBufferPool(numberOfArenas);
    }

    public static void clearBufferPool() {
        bufferPool = null;
    }

    @Override
    public ShuffleMaster<NettyShuffleDescriptor> createShuffleMaster(
            ShuffleMasterContext shuffleMasterContext) {
        return nettyShuffleServiceFactory.createShuffleMaster(shuffleMasterContext);
    }

    @Override
    public ShuffleEnvironment<ResultPartition, SingleInputGate> createShuffleEnvironment(
            ShuffleEnvironmentContext shuffleEnvironmentContext) {

        checkNotNull(shuffleEnvironmentContext);
        NettyShuffleEnvironmentConfiguration networkConfig =
                NettyShuffleEnvironmentConfiguration.fromConfiguration(
                        shuffleEnvironmentContext.getConfiguration(),
                        shuffleEnvironmentContext.getNetworkMemorySize(),
                        shuffleEnvironmentContext.isLocalCommunicationOnly(),
                        shuffleEnvironmentContext.getHostAddress());

        final NettyConfig nettyConfig = networkConfig.nettyConfig();
        final TaskEventPublisher taskEventPublisher = shuffleEnvironmentContext.getEventPublisher();
        final ResultPartitionManager resultPartitionManager = new ResultPartitionManager();
        final ConnectionManager connectionManager =
                nettyConfig != null
                        ? new NettyConnectionManager(
                                bufferPool, resultPartitionManager, taskEventPublisher, nettyConfig)
                        : new LocalConnectionManager();

        return NettyShuffleServiceFactory.createNettyShuffleEnvironment(
                networkConfig,
                shuffleEnvironmentContext.getTaskExecutorResourceId(),
                taskEventPublisher,
                resultPartitionManager,
                connectionManager,
                shuffleEnvironmentContext.getParentMetricGroup(),
                shuffleEnvironmentContext.getIoExecutor());
    }
}
