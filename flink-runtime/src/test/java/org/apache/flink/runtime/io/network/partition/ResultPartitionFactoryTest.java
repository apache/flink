/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition;

import org.apache.flink.runtime.deployment.ResultPartitionDeploymentDescriptor;
import org.apache.flink.runtime.io.disk.BatchShuffleReadBufferPool;
import org.apache.flink.runtime.io.disk.FileChannelManager;
import org.apache.flink.runtime.io.disk.FileChannelManagerImpl;
import org.apache.flink.runtime.io.network.buffer.NetworkBufferPool;
import org.apache.flink.runtime.io.network.partition.hybrid.HsResultPartition;
import org.apache.flink.runtime.shuffle.PartitionDescriptorBuilder;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.NettyShuffleDescriptorBuilder;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ResultPartitionFactory}. */
@SuppressWarnings("StaticVariableUsedBeforeInitialization")
@ExtendWith(TestLoggerExtension.class)
class ResultPartitionFactoryTest {

    private static final String tempDir = EnvironmentInformation.getTemporaryFileDirectory();
    private static final int SEGMENT_SIZE = 64;

    private static FileChannelManager fileChannelManager;

    @BeforeAll
    static void setUp() {
        fileChannelManager = new FileChannelManagerImpl(new String[] {tempDir}, "testing");
    }

    @AfterAll
    static void shutdown() throws Exception {
        fileChannelManager.close();
    }

    @Test
    void testBoundedBlockingSubpartitionsCreated() {
        final BoundedBlockingResultPartition resultPartition =
                (BoundedBlockingResultPartition)
                        createResultPartition(ResultPartitionType.BLOCKING);
        assertThat(resultPartition.subpartitions)
                .allSatisfy((sp) -> assertThat(sp).isInstanceOf(BoundedBlockingSubpartition.class));
    }

    @Test
    void testPipelinedSubpartitionsCreated() {
        final PipelinedResultPartition resultPartition =
                (PipelinedResultPartition) createResultPartition(ResultPartitionType.PIPELINED);
        assertThat(resultPartition.subpartitions)
                .allSatisfy((sp) -> assertThat(sp).isInstanceOf(PipelinedSubpartition.class));
    }

    @Test
    void testSortMergePartitionCreated() {
        ResultPartition resultPartition = createResultPartition(ResultPartitionType.BLOCKING, 1);
        assertThat(resultPartition).isInstanceOf(SortMergeResultPartition.class);
    }

    @Test
    void testHybridFullResultPartitionCreated() {
        ResultPartition resultPartition = createResultPartition(ResultPartitionType.HYBRID_FULL);
        assertThat(resultPartition).isInstanceOf(HsResultPartition.class);
    }

    @Test
    void testHybridSelectiveResultPartitionCreated() {
        ResultPartition resultPartition =
                createResultPartition(ResultPartitionType.HYBRID_SELECTIVE);
        assertThat(resultPartition).isInstanceOf(HsResultPartition.class);
    }

    @Test
    void testNoReleaseOnConsumptionForBoundedBlockingPartition() {
        final ResultPartition resultPartition = createResultPartition(ResultPartitionType.BLOCKING);

        resultPartition.onConsumedSubpartition(0);

        assertThat(resultPartition.isReleased()).isFalse();
    }

    @Test
    void testNoReleaseOnConsumptionForSortMergePartition() {
        final ResultPartition resultPartition =
                createResultPartition(ResultPartitionType.BLOCKING, 1);

        resultPartition.onConsumedSubpartition(0);

        assertThat(resultPartition.isReleased()).isFalse();
    }

    @Test
    void testNoReleaseOnConsumptionForHybridFullPartition() {
        final ResultPartition resultPartition =
                createResultPartition(ResultPartitionType.HYBRID_FULL);

        resultPartition.onConsumedSubpartition(0);

        assertThat(resultPartition.isReleased()).isFalse();
    }

    @Test
    void testNoReleaseOnConsumptionForHybridSelectivePartition() {
        final ResultPartition resultPartition =
                createResultPartition(ResultPartitionType.HYBRID_SELECTIVE);

        resultPartition.onConsumedSubpartition(0);

        assertThat(resultPartition.isReleased()).isFalse();
    }

    @Test
    public void testHybridBroadcastEdgeAlwaysUseFullResultPartition() {
        ResultPartition resultPartition =
                createResultPartition(
                        ResultPartitionType.HYBRID_SELECTIVE, Integer.MAX_VALUE, true);
        assertThat(resultPartition.partitionType).isEqualTo(ResultPartitionType.HYBRID_FULL);

        resultPartition =
                createResultPartition(ResultPartitionType.HYBRID_FULL, Integer.MAX_VALUE, true);
        assertThat(resultPartition.partitionType).isEqualTo(ResultPartitionType.HYBRID_FULL);
    }

    private static ResultPartition createResultPartition(ResultPartitionType partitionType) {
        return createResultPartition(partitionType, Integer.MAX_VALUE);
    }

    private static ResultPartition createResultPartition(
            ResultPartitionType partitionType, int sortShuffleMinParallelism) {
        return createResultPartition(partitionType, sortShuffleMinParallelism, false);
    }

    private static ResultPartition createResultPartition(
            ResultPartitionType partitionType, int sortShuffleMinParallelism, boolean isBroadcast) {
        final ResultPartitionManager manager = new ResultPartitionManager();

        final ResultPartitionFactory factory =
                new ResultPartitionFactory(
                        manager,
                        fileChannelManager,
                        new NetworkBufferPool(1, SEGMENT_SIZE),
                        new BatchShuffleReadBufferPool(10 * SEGMENT_SIZE, SEGMENT_SIZE),
                        Executors.newSingleThreadScheduledExecutor(),
                        BoundedBlockingSubpartitionType.AUTO,
                        1,
                        1,
                        SEGMENT_SIZE,
                        false,
                        "LZ4",
                        Integer.MAX_VALUE,
                        10,
                        sortShuffleMinParallelism,
                        false,
                        0);

        final ResultPartitionDeploymentDescriptor descriptor =
                new ResultPartitionDeploymentDescriptor(
                        PartitionDescriptorBuilder.newBuilder()
                                .setPartitionType(partitionType)
                                .setIsBroadcast(isBroadcast)
                                .build(),
                        NettyShuffleDescriptorBuilder.newBuilder().buildLocal(),
                        1);

        // guard our test assumptions
        assertThat(descriptor.getNumberOfSubpartitions()).isEqualTo(1);

        final ResultPartition partition = factory.create("test", 0, descriptor);
        manager.registerResultPartition(partition);

        return partition;
    }
}
