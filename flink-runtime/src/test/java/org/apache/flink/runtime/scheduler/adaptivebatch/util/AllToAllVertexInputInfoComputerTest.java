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

package org.apache.flink.runtime.scheduler.adaptivebatch.util;

import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;
import org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil.checkConsumedDataVolumePerSubtask;
import static org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil.checkConsumedSubpartitionGroups;
import static org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil.checkCorrectnessForCorrelatedInputs;
import static org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil.checkCorrectnessForNonCorrelatedInput;

/** Tests for {@link AllToAllVertexInputInfoComputer}. */
class AllToAllVertexInputInfoComputerTest {
    @Test
    void testComputeInputsWithIntraInputKeyCorrelation() {
        testComputeInputsWithIntraInputKeyCorrelation(1);
        testComputeInputsWithIntraInputKeyCorrelation(10);
    }

    void testComputeInputsWithIntraInputKeyCorrelation(int numInputInfos) {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos =
                createBlockingInputInfos(1, numInputInfos, 10, true, true, List.of());
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, numInputInfos, 10, true, true, List.of());
        inputInfos.addAll(leftInputInfos);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 10, 1, 10, 10);

        checkCorrectnessForCorrelatedInputs(vertexInputs, inputInfos, 3, 3);

        List<Map<IndexRange, IndexRange>> targetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(1, 1)),
                        Map.of(new IndexRange(0, 9), new IndexRange(2, 2)));
        checkConsumedSubpartitionGroups(targetConsumedSubpartitionGroups, inputInfos, vertexInputs);

        checkConsumedDataVolumePerSubtask(
                new long[] {10L * numInputInfos, 10L * numInputInfos, 10L * numInputInfos},
                leftInputInfos,
                vertexInputs);

        checkConsumedDataVolumePerSubtask(
                new long[] {10L * numInputInfos, 10L * numInputInfos, 10L * numInputInfos},
                rightInputInfos,
                vertexInputs);
    }

    @Test
    void testInputsUnionWithDifferentNumPartitions() {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos1 =
                createBlockingInputInfos(1, 1, 2, true, true, List.of());
        List<BlockingInputInfo> leftInputInfos2 =
                createBlockingInputInfos(1, 1, 3, true, true, List.of());
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, 1, 2, true, true, List.of());
        inputInfos.addAll(leftInputInfos1);
        inputInfos.addAll(leftInputInfos2);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 2, 1, 2, 10);

        checkCorrectnessForCorrelatedInputs(vertexInputs, inputInfos, 2, 3);

        List<Map<IndexRange, IndexRange>> left1TargetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 1), new IndexRange(0, 1)),
                        Map.of(new IndexRange(0, 1), new IndexRange(2, 2)));

        List<Map<IndexRange, IndexRange>> left2TargetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 2), new IndexRange(0, 1)),
                        Map.of(new IndexRange(0, 2), new IndexRange(2, 2)));

        List<Map<IndexRange, IndexRange>> rightTargetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 1), new IndexRange(0, 1)),
                        Map.of(new IndexRange(0, 1), new IndexRange(2, 2)));

        checkConsumedSubpartitionGroups(
                left1TargetConsumedSubpartitionGroups, leftInputInfos1, vertexInputs);
        checkConsumedSubpartitionGroups(
                left2TargetConsumedSubpartitionGroups, leftInputInfos2, vertexInputs);
        checkConsumedSubpartitionGroups(
                rightTargetConsumedSubpartitionGroups, rightInputInfos, vertexInputs);
    }

    @Test
    void testComputeSkewedInputWithIntraInputKeyCorrelation() {
        testComputeSkewedInputWithIntraInputKeyCorrelation(1);
        testComputeSkewedInputWithIntraInputKeyCorrelation(10);
    }

    void testComputeSkewedInputWithIntraInputKeyCorrelation(int numInputInfos) {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos =
                createBlockingInputInfos(1, numInputInfos, 10, true, true, List.of(0));
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, numInputInfos, 10, true, true, List.of());
        inputInfos.addAll(leftInputInfos);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 10, 1, 10, 10);

        checkCorrectnessForCorrelatedInputs(vertexInputs, inputInfos, 3, 3);

        List<Map<IndexRange, IndexRange>> targetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(1, 1)),
                        Map.of(new IndexRange(0, 9), new IndexRange(2, 2)));
        checkConsumedSubpartitionGroups(targetConsumedSubpartitionGroups, inputInfos, vertexInputs);

        checkConsumedDataVolumePerSubtask(
                new long[] {100L * numInputInfos, 10L * numInputInfos, 10L * numInputInfos},
                leftInputInfos,
                vertexInputs);

        checkConsumedDataVolumePerSubtask(
                new long[] {10L * numInputInfos, 10L * numInputInfos, 10L * numInputInfos},
                rightInputInfos,
                vertexInputs);
    }

    @Test
    void testComputeSkewedInputWithoutIntraInputKeyCorrelation() {
        testComputeSkewedInputWithoutIntraInputKeyCorrelation(1);
        testComputeSkewedInputWithoutIntraInputKeyCorrelation(10);
    }

    void testComputeSkewedInputWithoutIntraInputKeyCorrelation(int numInputInfos) {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos =
                createBlockingInputInfos(1, numInputInfos, 10, false, true, List.of(0));
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, numInputInfos, 10, true, true, List.of());
        inputInfos.addAll(leftInputInfos);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 10, 1, 10, 10);

        checkCorrectnessForCorrelatedInputs(vertexInputs, inputInfos, 7, 3);
        checkConsumedDataVolumePerSubtask(
                new long[] {
                    20L * numInputInfos,
                    20L * numInputInfos,
                    20L * numInputInfos,
                    20L * numInputInfos,
                    20L * numInputInfos,
                    10L * numInputInfos,
                    10L * numInputInfos
                },
                leftInputInfos,
                vertexInputs);
        checkConsumedDataVolumePerSubtask(
                new long[] {
                    10L * numInputInfos,
                    10L * numInputInfos,
                    10L * numInputInfos,
                    10L * numInputInfos,
                    10L * numInputInfos,
                    10L * numInputInfos,
                    10L * numInputInfos
                },
                rightInputInfos,
                vertexInputs);
    }

    @Test
    void testComputeMultipleSkewedInputsWithoutIntraInputKeyCorrelation() {
        testComputeMultipleSkewedInputsWithoutIntraInputKeyCorrelation(1);
        testComputeMultipleSkewedInputsWithoutIntraInputKeyCorrelation(10);
    }

    void testComputeMultipleSkewedInputsWithoutIntraInputKeyCorrelation(int numInputInfos) {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos =
                createBlockingInputInfos(1, numInputInfos, 2, false, true, List.of(1));
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, numInputInfos, 2, false, true, List.of(1));
        inputInfos.addAll(leftInputInfos);
        inputInfos.addAll(rightInputInfos);

        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 1, 1, 2, 10);
        checkCorrectnessForCorrelatedInputs(vertexInputs, inputInfos, 2, 3);
        checkConsumedDataVolumePerSubtask(
                new long[] {12L * numInputInfos, 12L * numInputInfos},
                leftInputInfos,
                vertexInputs);
        checkConsumedDataVolumePerSubtask(
                new long[] {22L * numInputInfos, 22L * numInputInfos},
                rightInputInfos,
                vertexInputs);

        // with smaller max parallelism
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs2 =
                computer.compute(new JobVertexID(), inputInfos, 1, 1, 1, 10);
        checkCorrectnessForCorrelatedInputs(vertexInputs2, inputInfos, 1, 3);
        checkConsumedDataVolumePerSubtask(
                new long[] {24L * numInputInfos}, leftInputInfos, vertexInputs2);
        checkConsumedDataVolumePerSubtask(
                new long[] {24L * numInputInfos}, rightInputInfos, vertexInputs2);

        // with bigger max parallelism
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs4 =
                computer.compute(new JobVertexID(), inputInfos, 1, 1, 5, 10);
        checkCorrectnessForCorrelatedInputs(vertexInputs4, inputInfos, 4, 3);

        checkConsumedDataVolumePerSubtask(
                new long[] {
                    12L * numInputInfos,
                    10L * numInputInfos,
                    10L * numInputInfos,
                    12L * numInputInfos
                },
                leftInputInfos,
                vertexInputs4);
        checkConsumedDataVolumePerSubtask(
                new long[] {
                    12L * numInputInfos,
                    10L * numInputInfos,
                    10L * numInputInfos,
                    12L * numInputInfos
                },
                rightInputInfos,
                vertexInputs4);

        // with bigger min parallelism
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs5 =
                computer.compute(new JobVertexID(), inputInfos, 5, 5, 5, 10);
        checkCorrectnessForCorrelatedInputs(vertexInputs5, inputInfos, 5, 3);

        checkConsumedDataVolumePerSubtask(
                new long[] {
                    2L * numInputInfos,
                    10L * numInputInfos,
                    10L * numInputInfos,
                    10L * numInputInfos,
                    12L * numInputInfos
                },
                leftInputInfos,
                vertexInputs5);
        checkConsumedDataVolumePerSubtask(
                new long[] {
                    2L * numInputInfos,
                    10L * numInputInfos,
                    10L * numInputInfos,
                    10L * numInputInfos,
                    12L * numInputInfos
                },
                rightInputInfos,
                vertexInputs5);
    }

    @Test
    void testComputeSkewedInputsWithDifferentNumPartitions() {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos = new ArrayList<>();
        leftInputInfos.addAll(createBlockingInputInfos(1, 1, 2, false, true, List.of(1)));
        leftInputInfos.addAll(createBlockingInputInfos(1, 1, 3, false, true, List.of(1)));
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, 1, 2, false, true, List.of(1));
        inputInfos.addAll(leftInputInfos);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 2, 1, 2, 10);

        checkCorrectnessForCorrelatedInputs(vertexInputs, inputInfos, 2, 3);
        checkConsumedDataVolumePerSubtask(new long[] {55L, 55L}, leftInputInfos, vertexInputs);
        checkConsumedDataVolumePerSubtask(new long[] {12L, 12L}, rightInputInfos, vertexInputs);
    }

    @Test
    void testComputeRebalancedWithAndWithoutCorrelations() {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();

        List<BlockingInputInfo> inputInfoWithCorrelations =
                createBlockingInputInfos(1, 1, 10, true, false, List.of());
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs1 =
                computer.compute(new JobVertexID(), inputInfoWithCorrelations, 2, 1, 5, 10);
        checkCorrectnessForNonCorrelatedInput(vertexInputs1, inputInfoWithCorrelations.get(0), 2);
        checkConsumedDataVolumePerSubtask(
                new long[] {20L, 10L}, inputInfoWithCorrelations, vertexInputs1);

        List<BlockingInputInfo> inputInfoWithoutCorrelations =
                createBlockingInputInfos(1, 1, 10, false, false, List.of());
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs2 =
                computer.compute(new JobVertexID(), inputInfoWithoutCorrelations, 2, 1, 5, 1);
        checkCorrectnessForNonCorrelatedInput(
                vertexInputs2, inputInfoWithoutCorrelations.get(0), 2);
        checkConsumedDataVolumePerSubtask(
                new long[] {15L, 15L}, inputInfoWithoutCorrelations, vertexInputs2);

        // with different parallelism
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs3 =
                computer.compute(new JobVertexID(), inputInfoWithoutCorrelations, 3, 1, 5, 10);
        checkCorrectnessForNonCorrelatedInput(
                vertexInputs3, inputInfoWithoutCorrelations.get(0), 3);
        checkConsumedDataVolumePerSubtask(
                new long[] {10L, 10L, 10L}, inputInfoWithoutCorrelations, vertexInputs3);
    }

    @Test
    void testComputeInputsWithDifferentCorrelations() {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> rebalancedInputInfos =
                createBlockingInputInfos(1, 1, 10, false, false, List.of());
        List<BlockingInputInfo> normalInputInfos =
                createBlockingInputInfos(1, 1, 10, true, true, List.of());
        inputInfos.addAll(rebalancedInputInfos);
        inputInfos.addAll(normalInputInfos);

        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 10, 1, 10, 10);
        // although normalInputInfos exist inter inputs key correlation, it is not essentially
        // correlated with other inputs because it is a single input, so we can use this test method
        checkCorrectnessForNonCorrelatedInput(vertexInputs, normalInputInfos.get(0), 3);
        checkConsumedDataVolumePerSubtask(
                new long[] {10L, 10L, 10L}, normalInputInfos, vertexInputs);

        checkCorrectnessForNonCorrelatedInput(vertexInputs, rebalancedInputInfos.get(0), 3);
        checkConsumedDataVolumePerSubtask(
                new long[] {10L, 10L, 10L}, rebalancedInputInfos, vertexInputs);
    }

    @Test
    void testComputeWithLargeDataVolumePerTask() {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos =
                createBlockingInputInfos(1, 1, 10, true, true, List.of());

        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 10, 1, 10, 100);
        checkCorrectnessForNonCorrelatedInput(vertexInputs, inputInfos.get(0), 1);
        checkConsumedDataVolumePerSubtask(new long[] {30L}, inputInfos, vertexInputs);
    }

    public static List<BlockingInputInfo> createBlockingInputInfos(
            int typeNumber,
            int numInputInfos,
            int numPartitions,
            boolean isIntraInputKeyCorrelated,
            boolean areInterInputsKeysCorrelated,
            List<Integer> skewedSubpartitionIndex) {
        return VertexInputInfoComputerTestUtil.createBlockingInputInfos(
                typeNumber,
                numInputInfos,
                numPartitions,
                3,
                isIntraInputKeyCorrelated,
                areInterInputsKeysCorrelated,
                1,
                10,
                List.of(),
                skewedSubpartitionIndex,
                false);
    }

    private static AllToAllVertexInputInfoComputer createAllToAllVertexInputInfoComputer() {
        return new AllToAllVertexInputInfoComputer(4, 10);
    }
}
