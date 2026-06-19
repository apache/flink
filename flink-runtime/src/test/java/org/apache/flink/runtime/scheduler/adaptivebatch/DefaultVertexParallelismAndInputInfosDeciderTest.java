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

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.ParallelismAndInputInfos;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.apache.flink.shaded.guava33.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link DefaultVertexParallelismAndInputInfosDecider}. */
class DefaultVertexParallelismAndInputInfosDeciderTest {

    private static final long BYTE_256_MB = 256 * 1024 * 1024L;
    private static final long BYTE_512_MB = 512 * 1024 * 1024L;
    private static final long BYTE_1_GB = 1024 * 1024 * 1024L;
    private static final long BYTE_8_GB = 8 * 1024 * 1024 * 1024L;
    private static final long BYTE_1_TB = 1024 * 1024 * 1024 * 1024L;

    private static final int MAX_PARALLELISM = 100;
    private static final int MIN_PARALLELISM = 3;
    private static final int VERTEX_MAX_PARALLELISM = 256;
    private static final int DEFAULT_SOURCE_PARALLELISM = 10;
    private static final long DATA_VOLUME_PER_TASK = 1024 * 1024 * 1024L;

    @Test
    void testDecideParallelism() {
        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_256_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_256_MB + BYTE_8_GB);

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(9);
    }

    @Test
    void testInitiallyNormalizedParallelismIsLargerThanMaxParallelism() {
        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_256_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_8_GB + BYTE_1_TB);

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(MAX_PARALLELISM);
    }

    @Test
    void testInitiallyNormalizedParallelismIsSmallerThanMinParallelism() {
        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_256_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_512_MB);

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(MIN_PARALLELISM);
    }

    @Test
    void testNonBroadcastBytesCanNotDividedEvenly() {
        BlockingResultInfo resultInfo1 = createFromBroadcastResult(BYTE_512_MB);
        BlockingResultInfo resultInfo2 = createFromNonBroadcastResult(BYTE_256_MB + BYTE_8_GB);

        int parallelism =
                createDeciderAndDecideParallelism(Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelism).isEqualTo(9);
    }

    @Test
    void testAllEdgesAllToAll() {
        AllToAllBlockingResultInfo resultInfo1 =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L});
        AllToAllBlockingResultInfo resultInfo2 =
                createAllToAllBlockingResultInfo(
                        new long[] {8L, 12L, 21L, 9L, 13L, 7L, 19L, 13L, 14L, 5L});
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        1, 10, 60L, Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(5);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(2);

        List<IndexRange> subpartitionRanges =
                Arrays.asList(
                        new IndexRange(0, 1),
                        new IndexRange(2, 3),
                        new IndexRange(4, 6),
                        new IndexRange(7, 8),
                        new IndexRange(9, 9));
        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo1.getResultId()),
                subpartitionRanges);
        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo2.getResultId()),
                subpartitionRanges);
    }

    @Test
    void testAllEdgesAllToAllAndDecidedParallelismIsMaxParallelism() {
        AllToAllBlockingResultInfo resultInfo =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L});
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        1, 2, 10L, Collections.singletonList(resultInfo));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(2);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(1);
        checkAllToAllJobVertexInputInfo(
                Iterables.getOnlyElement(
                        parallelismAndInputInfos.getJobVertexInputInfos().values()),
                Arrays.asList(new IndexRange(0, 5), new IndexRange(6, 9)));
    }

    @Test
    void testAllEdgesAllToAllAndDecidedParallelismIsMinParallelism() {
        AllToAllBlockingResultInfo resultInfo =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L});
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        4, 10, 1000L, Collections.singletonList(resultInfo));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(4);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(1);
        checkAllToAllJobVertexInputInfo(
                Iterables.getOnlyElement(
                        parallelismAndInputInfos.getJobVertexInputInfos().values()),
                Arrays.asList(
                        new IndexRange(0, 1),
                        new IndexRange(2, 5),
                        new IndexRange(6, 7),
                        new IndexRange(8, 9)));
    }

    @Test
    void testFallBackToEvenlyDistributeSubpartitions() {
        AllToAllBlockingResultInfo resultInfo =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 1L, 10L, 1L, 10L, 1L, 10L, 1L, 10L, 1L});
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        8, 8, 10L, Collections.singletonList(resultInfo));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(8);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(1);
        checkAllToAllJobVertexInputInfo(
                Iterables.getOnlyElement(
                        parallelismAndInputInfos.getJobVertexInputInfos().values()),
                Arrays.asList(
                        new IndexRange(0, 0),
                        new IndexRange(1, 1),
                        new IndexRange(2, 2),
                        new IndexRange(3, 4),
                        new IndexRange(5, 5),
                        new IndexRange(6, 6),
                        new IndexRange(7, 7),
                        new IndexRange(8, 9)));
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAllEdgesAllToAllAndOneIsBroadcast(boolean singleSubpartitionContainsAllData) {
        AllToAllBlockingResultInfo resultInfo1 =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L}, false, false);
        AllToAllBlockingResultInfo resultInfo2 =
                createAllToAllBlockingResultInfo(
                        singleSubpartitionContainsAllData
                                ? new long[] {10L}
                                : new long[] {1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L, 1L},
                        true,
                        singleSubpartitionContainsAllData);

        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        1, 10, 60L, Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(3);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(2);

        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo1.getResultId()),
                Arrays.asList(new IndexRange(0, 4), new IndexRange(5, 8), new IndexRange(9, 9)));
        if (singleSubpartitionContainsAllData) {
            checkAllToAllJobVertexInputInfo(
                    parallelismAndInputInfos
                            .getJobVertexInputInfos()
                            .get(resultInfo2.getResultId()),
                    Arrays.asList(
                            new IndexRange(0, 0), new IndexRange(0, 0), new IndexRange(0, 0)));
        } else {
            checkAllToAllJobVertexInputInfo(
                    parallelismAndInputInfos
                            .getJobVertexInputInfos()
                            .get(resultInfo2.getResultId()),
                    Arrays.asList(
                            new IndexRange(0, 9), new IndexRange(0, 9), new IndexRange(0, 9)));
        }
    }

    @Test
    void testAllEdgesBroadcast() {
        AllToAllBlockingResultInfo resultInfo1;
        AllToAllBlockingResultInfo resultInfo2;
        resultInfo1 = createAllToAllBlockingResultInfo(new long[] {10L}, true, false);
        resultInfo2 = createAllToAllBlockingResultInfo(new long[] {10L}, true, false);

        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        1, 10, 60L, Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelismAndInputInfos.getParallelism()).isOne();
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(2);

        List<IndexRange> expectedSubpartitionRanges =
                Collections.singletonList(new IndexRange(0, 0));

        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo1.getResultId()),
                expectedSubpartitionRanges);
        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo2.getResultId()),
                expectedSubpartitionRanges);
    }

    @Test
    void testHavePointwiseEdges() {
        AllToAllBlockingResultInfo resultInfo1 =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L});
        PointwiseBlockingResultInfo resultInfo2 =
                createPointwiseBlockingResultInfo(
                        new long[] {8L, 12L, 21L, 9L, 13L}, new long[] {7L, 19L, 13L, 14L, 5L});
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        1, 10, 60L, Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(4);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(2);

        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo1.getResultId()),
                Arrays.asList(
                        new IndexRange(0, 1),
                        new IndexRange(2, 5),
                        new IndexRange(6, 7),
                        new IndexRange(8, 9)));
        checkJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo2.getResultId()),
                Arrays.asList(
                        Map.of(new IndexRange(0, 0), new IndexRange(0, 1)),
                        Map.of(new IndexRange(0, 0), new IndexRange(2, 3)),
                        Map.of(
                                new IndexRange(0, 0),
                                new IndexRange(4, 4),
                                new IndexRange(1, 1),
                                new IndexRange(0, 1)),
                        Map.of(new IndexRange(1, 1), new IndexRange(2, 4))));
    }

    @Test
    void testHavePointwiseAndBroadcastEdge() {
        AllToAllBlockingResultInfo resultInfo1 =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L}, true, false);
        PointwiseBlockingResultInfo resultInfo2 =
                createPointwiseBlockingResultInfo(
                        new long[] {8L, 12L, 21L, 9L, 13L}, new long[] {7L, 19L, 13L, 14L, 5L});
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        1, 10, 60L, Arrays.asList(resultInfo1, resultInfo2));

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(6);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(2);

        checkAllToAllJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo1.getResultId()),
                Arrays.asList(
                        new IndexRange(0, 9),
                        new IndexRange(0, 9),
                        new IndexRange(0, 9),
                        new IndexRange(0, 9),
                        new IndexRange(0, 9),
                        new IndexRange(0, 9)));
        checkJobVertexInputInfo(
                parallelismAndInputInfos.getJobVertexInputInfos().get(resultInfo2.getResultId()),
                Arrays.asList(
                        Map.of(new IndexRange(0, 0), new IndexRange(0, 1)),
                        Map.of(new IndexRange(0, 0), new IndexRange(2, 3)),
                        Map.of(
                                new IndexRange(0, 0),
                                new IndexRange(4, 4),
                                new IndexRange(1, 1),
                                new IndexRange(0, 0)),
                        Map.of(new IndexRange(1, 1), new IndexRange(1, 1)),
                        Map.of(new IndexRange(1, 1), new IndexRange(2, 3)),
                        Map.of(new IndexRange(1, 1), new IndexRange(4, 4))));
    }

    @Test
    void testSourceJobVertex() {
        ParallelismAndInputInfos parallelismAndInputInfos =
                createDeciderAndDecideParallelismAndInputInfos(
                        MIN_PARALLELISM,
                        MAX_PARALLELISM,
                        DATA_VOLUME_PER_TASK,
                        Collections.emptyList());

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(DEFAULT_SOURCE_PARALLELISM);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).isEmpty();
    }

    @Test
    void testDynamicSourceParallelismWithUpstreamInputs() {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDecider(MIN_PARALLELISM, MAX_PARALLELISM, DATA_VOLUME_PER_TASK);

        AllToAllBlockingResultInfo allToAllBlockingResultInfo =
                createAllToAllBlockingResultInfo(
                        new long[] {10L, 15L, 13L, 12L, 1L, 10L, 8L, 20L, 12L, 17L});
        int dynamicSourceParallelism = 4;
        ParallelismAndInputInfos parallelismAndInputInfos =
                decider.decideParallelismAndInputInfosForVertex(
                        new JobVertexID(),
                        Collections.singletonList(
                                toBlockingInputInfoView(allToAllBlockingResultInfo)),
                        -1,
                        dynamicSourceParallelism,
                        MAX_PARALLELISM);

        assertThat(parallelismAndInputInfos.getParallelism()).isEqualTo(4);
        assertThat(parallelismAndInputInfos.getJobVertexInputInfos()).hasSize(1);

        checkAllToAllJobVertexInputInfo(
                Iterables.getOnlyElement(
                        parallelismAndInputInfos.getJobVertexInputInfos().values()),
                Arrays.asList(
                        new IndexRange(0, 1),
                        new IndexRange(2, 5),
                        new IndexRange(6, 7),
                        new IndexRange(8, 9)));
    }

    @Test
    void testComputeSourceParallelismUpperBound() {
        Configuration configuration = new Configuration();
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM,
                DEFAULT_SOURCE_PARALLELISM);
        VertexParallelismAndInputInfosDecider vertexParallelismAndInputInfosDecider =
                createDefaultVertexParallelismAndInputInfosDecider(MAX_PARALLELISM, configuration);
        assertThat(
                        vertexParallelismAndInputInfosDecider.computeSourceParallelismUpperBound(
                                new JobVertexID(), VERTEX_MAX_PARALLELISM))
                .isEqualTo(DEFAULT_SOURCE_PARALLELISM);
    }

    @Test
    void testComputeSourceParallelismUpperBoundFallback() {
        Configuration configuration = new Configuration();
        VertexParallelismAndInputInfosDecider vertexParallelismAndInputInfosDecider =
                createDefaultVertexParallelismAndInputInfosDecider(MAX_PARALLELISM, configuration);
        assertThat(
                        vertexParallelismAndInputInfosDecider.computeSourceParallelismUpperBound(
                                new JobVertexID(), VERTEX_MAX_PARALLELISM))
                .isEqualTo(MAX_PARALLELISM);
    }

    @Test
    void testComputeSourceParallelismUpperBoundNotExceedMaxParallelism() {
        Configuration configuration = new Configuration();
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM,
                VERTEX_MAX_PARALLELISM * 2);
        VertexParallelismAndInputInfosDecider vertexParallelismAndInputInfosDecider =
                createDefaultVertexParallelismAndInputInfosDecider(MAX_PARALLELISM, configuration);
        assertThat(
                        vertexParallelismAndInputInfosDecider.computeSourceParallelismUpperBound(
                                new JobVertexID(), VERTEX_MAX_PARALLELISM))
                .isEqualTo(VERTEX_MAX_PARALLELISM);
    }

    private static void checkAllToAllJobVertexInputInfo(
            JobVertexInputInfo jobVertexInputInfo, List<IndexRange> subpartitionRanges) {
        checkAllToAllJobVertexInputInfo(
                jobVertexInputInfo, new IndexRange(0, 0), subpartitionRanges);
    }

    private static void checkAllToAllJobVertexInputInfo(
            JobVertexInputInfo jobVertexInputInfo,
            IndexRange indexRange,
            List<IndexRange> subpartitionRanges) {
        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < subpartitionRanges.size(); ++i) {
            executionVertexInputInfos.add(
                    new ExecutionVertexInputInfo(i, indexRange, subpartitionRanges.get(i)));
        }
        assertThat(jobVertexInputInfo.getExecutionVertexInputInfos())
                .containsExactlyInAnyOrderElementsOf(executionVertexInputInfos);
    }

    private static void checkJobVertexInputInfo(
            JobVertexInputInfo jobVertexInputInfo,
            List<Map<IndexRange, IndexRange>> consumedSubpartitionGroups) {
        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < consumedSubpartitionGroups.size(); ++i) {
            executionVertexInputInfos.add(
                    new ExecutionVertexInputInfo(i, consumedSubpartitionGroups.get(i)));
        }
        assertThat(jobVertexInputInfo.getExecutionVertexInputInfos())
                .containsExactlyInAnyOrderElementsOf(executionVertexInputInfos);
    }

    static DefaultVertexParallelismAndInputInfosDecider createDecider(
            int minParallelism, int maxParallelism, long dataVolumePerTask) {
        return createDecider(
                minParallelism, maxParallelism, dataVolumePerTask, DEFAULT_SOURCE_PARALLELISM);
    }

    static DefaultVertexParallelismAndInputInfosDecider createDecider(
            int minParallelism,
            int maxParallelism,
            long dataVolumePerTask,
            int defaultSourceParallelism) {
        Configuration configuration = new Configuration();

        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM, minParallelism);
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK,
                new MemorySize(dataVolumePerTask));
        configuration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM,
                defaultSourceParallelism);

        return createDefaultVertexParallelismAndInputInfosDecider(maxParallelism, configuration);
    }

    static DefaultVertexParallelismAndInputInfosDecider
            createDefaultVertexParallelismAndInputInfosDecider(
                    int maxParallelism, Configuration configuration) {
        return DefaultVertexParallelismAndInputInfosDecider.from(
                maxParallelism,
                BatchExecutionOptionsInternal.ADAPTIVE_SKEWED_OPTIMIZATION_SKEWED_FACTOR
                        .defaultValue(),
                BatchExecutionOptionsInternal.ADAPTIVE_SKEWED_OPTIMIZATION_SKEWED_THRESHOLD
                        .defaultValue()
                        .getBytes(),
                configuration);
    }

    private static int createDeciderAndDecideParallelism(List<BlockingResultInfo> consumedResults) {
        return createDeciderAndDecideParallelism(
                MIN_PARALLELISM, MAX_PARALLELISM, DATA_VOLUME_PER_TASK, consumedResults);
    }

    private static int createDeciderAndDecideParallelism(
            int minParallelism,
            int maxParallelism,
            long dataVolumePerTask,
            List<BlockingResultInfo> consumedResults) {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDecider(minParallelism, maxParallelism, dataVolumePerTask);
        return decider.decideParallelism(
                new JobVertexID(),
                toBlockingInputInfoViews(consumedResults),
                minParallelism,
                maxParallelism);
    }

    private static ParallelismAndInputInfos createDeciderAndDecideParallelismAndInputInfos(
            int minParallelism,
            int maxParallelism,
            long dataVolumePerTask,
            List<BlockingResultInfo> consumedResults) {
        final DefaultVertexParallelismAndInputInfosDecider decider =
                createDecider(minParallelism, maxParallelism, dataVolumePerTask);
        return decider.decideParallelismAndInputInfosForVertex(
                new JobVertexID(),
                toBlockingInputInfoViews(consumedResults),
                -1,
                minParallelism,
                maxParallelism);
    }

    private AllToAllBlockingResultInfo createAllToAllBlockingResultInfo(
            long[] aggregatedSubpartitionBytes) {
        return createAllToAllBlockingResultInfo(aggregatedSubpartitionBytes, false, false);
    }

    private AllToAllBlockingResultInfo createAllToAllBlockingResultInfo(
            long[] aggregatedSubpartitionBytes,
            boolean isBroadcast,
            boolean isSingleSubpartitionContainsAllData) {
        // For simplicity, we configure only one partition here, so the aggregatedSubpartitionBytes
        // is equivalent to the subpartition bytes of partition0
        AllToAllBlockingResultInfo resultInfo =
                new AllToAllBlockingResultInfo(
                        new IntermediateDataSetID(),
                        1,
                        aggregatedSubpartitionBytes.length,
                        isBroadcast,
                        isSingleSubpartitionContainsAllData);
        resultInfo.recordPartitionInfo(0, new ResultPartitionBytes(aggregatedSubpartitionBytes));
        return resultInfo;
    }

    private PointwiseBlockingResultInfo createPointwiseBlockingResultInfo(
            long[]... subpartitionBytesByPartition) {

        final Set<Integer> subpartitionNumSet =
                Arrays.stream(subpartitionBytesByPartition)
                        .map(array -> array.length)
                        .collect(Collectors.toSet());
        // all partitions have the same subpartition num
        checkState(subpartitionNumSet.size() == 1);
        int numSubpartitions = subpartitionNumSet.iterator().next();
        int numPartitions = subpartitionBytesByPartition.length;

        PointwiseBlockingResultInfo resultInfo =
                new PointwiseBlockingResultInfo(
                        new IntermediateDataSetID(), numPartitions, numSubpartitions);

        int partitionIndex = 0;
        for (long[] subpartitionBytes : subpartitionBytesByPartition) {
            resultInfo.recordPartitionInfo(
                    partitionIndex++, new ResultPartitionBytes(subpartitionBytes));
        }

        return resultInfo;
    }

    private static class TestingBlockingResultInfo implements BlockingResultInfo {

        private final boolean isBroadcast;
        private final boolean singleSubpartitionContainsAllData;
        private final long producedBytes;
        private final int numPartitions;
        private final int numSubpartitions;

        private TestingBlockingResultInfo(
                boolean isBroadcast,
                boolean singleSubpartitionContainsAllData,
                long producedBytes) {
            this(
                    isBroadcast,
                    singleSubpartitionContainsAllData,
                    producedBytes,
                    MAX_PARALLELISM,
                    MAX_PARALLELISM);
        }

        private TestingBlockingResultInfo(
                boolean isBroadcast,
                boolean singleSubpartitionContainsAllData,
                long producedBytes,
                int numPartitions,
                int numSubpartitions) {
            this.isBroadcast = isBroadcast;
            this.singleSubpartitionContainsAllData = singleSubpartitionContainsAllData;
            this.producedBytes = producedBytes;
            this.numPartitions = numPartitions;
            this.numSubpartitions = numSubpartitions;
        }

        @Override
        public IntermediateDataSetID getResultId() {
            return new IntermediateDataSetID();
        }

        @Override
        public boolean isBroadcast() {
            return isBroadcast;
        }

        @Override
        public boolean isSingleSubpartitionContainsAllData() {
            return singleSubpartitionContainsAllData;
        }

        @Override
        public boolean isPointwise() {
            return false;
        }

        @Override
        public int getNumPartitions() {
            return numPartitions;
        }

        @Override
        public int getNumSubpartitions(int partitionIndex) {
            return numSubpartitions;
        }

        @Override
        public long getNumBytesProduced() {
            return producedBytes;
        }

        @Override
        public long getNumBytesProduced(
                IndexRange partitionIndexRange, IndexRange subpartitionIndexRange) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void recordPartitionInfo(int partitionIndex, ResultPartitionBytes partitionBytes) {}

        @Override
        public void resetPartitionInfo(int partitionIndex) {}

        @Override
        public Map<Integer, long[]> getSubpartitionBytesByPartitionIndex() {
            return Map.of();
        }
    }

    private static BlockingResultInfo createFromBroadcastResult(long producedBytes) {
        return new TestingBlockingResultInfo(true, true, producedBytes);
    }

    private static BlockingResultInfo createFromNonBroadcastResult(long producedBytes) {
        return new TestingBlockingResultInfo(false, false, producedBytes);
    }

    public static BlockingInputInfo toBlockingInputInfoView(BlockingResultInfo blockingResultInfo) {
        boolean existIntraInputKeyCorrelation =
                blockingResultInfo instanceof AllToAllBlockingResultInfo;
        boolean existInterInputsKeyCorrelation =
                blockingResultInfo instanceof AllToAllBlockingResultInfo;
        return new BlockingInputInfo(
                blockingResultInfo,
                0,
                existInterInputsKeyCorrelation,
                existIntraInputKeyCorrelation);
    }

    public static List<BlockingInputInfo> toBlockingInputInfoViews(
            List<BlockingResultInfo> blockingResultInfos) {
        List<BlockingInputInfo> blockingInputInfos = new ArrayList<>();
        for (BlockingResultInfo blockingResultInfo : blockingResultInfos) {
            blockingInputInfos.add(toBlockingInputInfoView(blockingResultInfo));
        }
        return blockingInputInfos;
    }
}
