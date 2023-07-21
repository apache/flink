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

package org.apache.flink.runtime.io.network.partition.hybrid.tiered.shuffle;

import org.apache.flink.runtime.executiongraph.IOMetrics;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.BufferCompressor;
import org.apache.flink.runtime.io.network.buffer.BufferPool;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.NoOpBufferAvailablityListener;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionManager;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingBufferAccumulator;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingTierProducerAgent;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.TestingTieredStorageMemoryManager;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.netty.TieredStorageNettyServiceImpl;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageProducerClient;
import org.apache.flink.runtime.io.network.partition.hybrid.tiered.storage.TieredStorageResourceRegistry;
import org.apache.flink.runtime.metrics.groups.TaskIOMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;
import org.apache.flink.util.concurrent.IgnoreShutdownRejectedExecutionHandler;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;

import static org.apache.flink.runtime.shuffle.NettyShuffleUtils.getMinMaxNetworkBuffersPerResultPartition;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link TieredResultPartition}. */
class TieredResultPartitionTest {

    private static final int NUM_THREADS = 4;

    private static final int NETWORK_BUFFER_SIZE = 1024;

    private static final int NUM_TOTAL_BUFFERS = 1000;

    private static final int NUM_TOTAL_BYTES_IN_READ_POOL = 32 * 1024 * 1024;

    private FileChannelManager fileChannelManager;

    private NetworkBufferPool globalPool;

    private BatchShuffleReadBufferPool readBufferPool;

    private ScheduledExecutorService readIOExecutor;

    private TaskIOMetricGroup taskIOMetricGroup;

    @TempDir public java.nio.file.Path tempDataPath;

    @BeforeEach
    void before() {
        fileChannelManager =
                new FileChannelManagerImpl(new String[] {tempDataPath.toString()}, "testing");
        globalPool = new NetworkBufferPool(NUM_TOTAL_BUFFERS, NETWORK_BUFFER_SIZE);
        readBufferPool =
                new BatchShuffleReadBufferPool(NUM_TOTAL_BYTES_IN_READ_POOL, NETWORK_BUFFER_SIZE);
        readIOExecutor =
                new ScheduledThreadPoolExecutor(
                        NUM_THREADS,
                        new ExecutorThreadFactory("test-io-scheduler-thread"),
                        new IgnoreShutdownRejectedExecutionHandler());
    }

    @AfterEach
    void after() throws Exception {
        fileChannelManager.close();
        globalPool.destroy();
        readBufferPool.destroy();
        readIOExecutor.shutdown();
    }

    @Test
    void testClose() throws Exception {
        final int numBuffers = 1;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredResultPartition partition = createTieredStoreResultPartition(1, bufferPool, false);

        partition.close();
        assertThat(bufferPool.isDestroyed()).isTrue();
    }

    @Test
    void testRelease() throws Exception {
        final int numSubpartitions = 2;
        final int numBuffers = 10;

        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredResultPartition partition =
                createTieredStoreResultPartition(numSubpartitions, bufferPool, false);

        partition.emitRecord(ByteBuffer.allocate(NETWORK_BUFFER_SIZE * 5), 1);
        partition.close();
        assertThat(bufferPool.isDestroyed()).isTrue();

        partition.release();

        while (checkNotNull(fileChannelManager.getPaths()[0].listFiles()).length != 0) {
            Thread.sleep(10);
        }

        assertThat(NUM_TOTAL_BUFFERS).isEqualTo(globalPool.getNumberOfAvailableMemorySegments());
    }

    @Test
    void testMinMaxNetworkBuffersTieredResultPartition() {
        int numSubpartitions = 105;
        int tieredStorageTotalExclusiveBufferNum = 103;
        Pair<Integer, Integer> minMaxNetworkBuffers =
                getMinMaxNetworkBuffersPerResultPartition(
                        100,
                        5,
                        100,
                        10,
                        numSubpartitions,
                        true,
                        tieredStorageTotalExclusiveBufferNum,
                        ResultPartitionType.HYBRID_SELECTIVE);
        assertThat(minMaxNetworkBuffers.getLeft()).isEqualTo(tieredStorageTotalExclusiveBufferNum);
        assertThat(minMaxNetworkBuffers.getRight()).isEqualTo(Integer.MAX_VALUE);
    }

    @Test
    void testCreateSubpartitionViewAfterRelease() throws Exception {
        final int numBuffers = 10;
        BufferPool bufferPool = globalPool.createBufferPool(numBuffers, numBuffers);
        TieredResultPartition resultPartition =
                createTieredStoreResultPartition(2, bufferPool, false);
        resultPartition.release();
        assertThatThrownBy(
                        () ->
                                resultPartition.createSubpartitionView(
                                        0, new NoOpBufferAvailablityListener()))
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testEmitRecords() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(3, 3);
        int bufferSize = NETWORK_BUFFER_SIZE;
        try (TieredResultPartition partition =
                createTieredStoreResultPartition(2, bufferPool, false)) {
            partition.emitRecord(ByteBuffer.allocate(bufferSize), 0);
            partition.broadcastRecord(ByteBuffer.allocate(bufferSize));
            IOMetrics ioMetrics = taskIOMetricGroup.createSnapshot();
            assertThat(ioMetrics.getResultPartitionBytes()).hasSize(1);
            ResultPartitionBytes partitionBytes =
                    ioMetrics.getResultPartitionBytes().values().iterator().next();
            assertThat(partitionBytes.getSubpartitionBytes())
                    .containsExactly((long) 2 * bufferSize, bufferSize);
        }
    }

    @Test
    void testMetricsUpdateForBroadcastOnlyResultPartition() throws Exception {
        BufferPool bufferPool = globalPool.createBufferPool(3, 3);
        int bufferSize = NETWORK_BUFFER_SIZE;
        try (TieredResultPartition partition =
                createTieredStoreResultPartition(2, bufferPool, true)) {
            partition.broadcastRecord(ByteBuffer.allocate(bufferSize));
            IOMetrics ioMetrics = taskIOMetricGroup.createSnapshot();
            assertThat(ioMetrics.getResultPartitionBytes()).hasSize(1);
            ResultPartitionBytes partitionBytes =
                    ioMetrics.getResultPartitionBytes().values().iterator().next();
            assertThat(partitionBytes.getSubpartitionBytes())
                    .containsExactly(bufferSize, bufferSize);
        }
    }

    private TieredResultPartition createTieredStoreResultPartition(
            int numSubpartitions, BufferPool bufferPool, boolean isBroadcastOnly)
            throws IOException {
        TestingTierProducerAgent tierProducerAgent = new TestingTierProducerAgent.Builder().build();
        TieredStorageResourceRegistry tieredStorageResourceRegistry =
                new TieredStorageResourceRegistry();
        TieredResultPartition tieredResultPartition =
                new TieredResultPartition(
                        "TieredStoreResultPartitionTest",
                        0,
                        new ResultPartitionID(),
                        ResultPartitionType.HYBRID_SELECTIVE,
                        numSubpartitions,
                        numSubpartitions,
                        new ResultPartitionManager(),
                        new BufferCompressor(NETWORK_BUFFER_SIZE, "LZ4"),
                        () -> bufferPool,
                        new TieredStorageProducerClient(
                                numSubpartitions,
                                isBroadcastOnly,
                                new TestingBufferAccumulator(),
                                null,
                                Collections.singletonList(tierProducerAgent)),
                        tieredStorageResourceRegistry,
                        new TieredStorageNettyServiceImpl(tieredStorageResourceRegistry),
                        Collections.emptyList(),
                        new TestingTieredStorageMemoryManager.Builder().build());
        taskIOMetricGroup =
                UnregisteredMetricGroups.createUnregisteredTaskMetricGroup().getIOMetricGroup();
        tieredResultPartition.setup();
        tieredResultPartition.setMetricGroup(taskIOMetricGroup);
        return tieredResultPartition;
    }
}
