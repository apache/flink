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
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;
import org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil.checkConsumedDataVolumePerSubtask;
import static org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil.checkConsumedSubpartitionGroups;
import static org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil.checkCorrectnessForNonCorrelatedInput;

/** Tests for {@link PointwiseVertexInputInfoComputer}. */
class PointwiseVertexInputInfoComputerTest {

    @Test
    void testComputeNormalInput() {
        PointwiseVertexInputInfoComputer computer = createPointwiseVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = createBlockingInputInfos(2, List.of(), false);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(inputInfos, 2, 10);
        checkCorrectnessForNonCorrelatedInput(vertexInputs, inputInfos.get(0), 2);
        checkConsumedDataVolumePerSubtask(new long[] {3L, 3L}, inputInfos, vertexInputs);

        // with different parallelism
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs2 =
                computer.compute(inputInfos, 3, 10);
        checkCorrectnessForNonCorrelatedInput(vertexInputs2, inputInfos.get(0), 3);
        checkConsumedDataVolumePerSubtask(new long[] {2L, 2L, 2L}, inputInfos, vertexInputs2);
    }

    @Test
    void testComputeSkewedInputsWithDifferentSkewedPartitions() {
        PointwiseVertexInputInfoComputer computer = createPointwiseVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfosWithDifferentSkewedPartitions = new ArrayList<>();
        BlockingInputInfo inputInfo1 = createBlockingInputInfo(3, 3, List.of(0), false);
        BlockingInputInfo inputInfo2 = createBlockingInputInfo(3, 3, List.of(1), false);
        inputInfosWithDifferentSkewedPartitions.add(inputInfo1);
        inputInfosWithDifferentSkewedPartitions.add(inputInfo2);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(inputInfosWithDifferentSkewedPartitions, 3, 10);
        checkCorrectnessForNonCorrelatedInput(vertexInputs, inputInfo1, 3);
        checkConsumedDataVolumePerSubtask(
                new long[] {10L, 10L, 16L}, List.of(inputInfo1), vertexInputs);

        checkCorrectnessForNonCorrelatedInput(vertexInputs, inputInfo2, 3);
        checkConsumedDataVolumePerSubtask(
                new long[] {13L, 10L, 13L}, List.of(inputInfo2), vertexInputs);
    }

    @Test
    void testComputeSkewedInputsWithDifferentNumPartitions() {
        PointwiseVertexInputInfoComputer computer = createPointwiseVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfosWithDifferentNumPartitions = new ArrayList<>();
        BlockingInputInfo inputInfo1 = createBlockingInputInfo(3, 3, List.of(1), false);
        BlockingInputInfo inputInfo2 = createBlockingInputInfo(2, 3, List.of(1), false);
        inputInfosWithDifferentNumPartitions.add(inputInfo1);
        inputInfosWithDifferentNumPartitions.add(inputInfo2);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(inputInfosWithDifferentNumPartitions, 3, 10);
        checkCorrectnessForNonCorrelatedInput(vertexInputs, inputInfo1, 3);
        checkConsumedDataVolumePerSubtask(
                new long[] {13L, 10L, 13L}, List.of(inputInfo1), vertexInputs);

        checkCorrectnessForNonCorrelatedInput(vertexInputs, inputInfo2, 3);
        checkConsumedDataVolumePerSubtask(
                new long[] {13L, 10L, 10L}, List.of(inputInfo2), vertexInputs);
    }

    @Test
    void testComputeSkewedInputsWithDifferentNumSubpartitions() {
        PointwiseVertexInputInfoComputer computer = createPointwiseVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfosWithDifferentNumSubpartitions = new ArrayList<>();
        BlockingInputInfo inputInfo1 = createBlockingInputInfo(3, 3, List.of(1), false);
        BlockingInputInfo inputInfo2 = createBlockingInputInfo(3, 5, List.of(1), false);
        inputInfosWithDifferentNumSubpartitions.add(inputInfo1);
        inputInfosWithDifferentNumSubpartitions.add(inputInfo2);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(inputInfosWithDifferentNumSubpartitions, 3, 10);
        checkCorrectnessForNonCorrelatedInput(vertexInputs, inputInfo1, 3);
        checkConsumedDataVolumePerSubtask(
                new long[] {13L, 10L, 13L}, List.of(inputInfo1), vertexInputs);

        checkCorrectnessForNonCorrelatedInput(vertexInputs, inputInfo2, 3);
        checkConsumedDataVolumePerSubtask(
                new long[] {25L, 20L, 15L}, List.of(inputInfo2), vertexInputs);
    }

    @Test
    void testComputeInputWithIntraCorrelation() {
        PointwiseVertexInputInfoComputer computer = createPointwiseVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = createBlockingInputInfos(3, List.of(), true);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(inputInfos, 3, 10);
        checkCorrectnessForNonCorrelatedInput(vertexInputs, inputInfos.get(0), 3);
        checkConsumedSubpartitionGroups(
                List.of(
                        Map.of(new IndexRange(0, 0), new IndexRange(0, 2)),
                        Map.of(new IndexRange(1, 1), new IndexRange(0, 2)),
                        Map.of(new IndexRange(2, 2), new IndexRange(0, 2))),
                inputInfos,
                vertexInputs);

        // with different parallelism
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs2 =
                computer.compute(inputInfos, 2, 10);
        checkCorrectnessForNonCorrelatedInput(vertexInputs2, inputInfos.get(0), 2);
        checkConsumedSubpartitionGroups(
                List.of(
                        Map.of(new IndexRange(0, 1), new IndexRange(0, 2)),
                        Map.of(new IndexRange(2, 2), new IndexRange(0, 2))),
                inputInfos,
                vertexInputs2);
    }

    private static List<BlockingInputInfo> createBlockingInputInfos(
            int numPartitions,
            List<Integer> skewedPartitionIndex,
            boolean existIntraInputKeyCorrelation) {
        return List.of(
                createBlockingInputInfo(
                        numPartitions, 3, skewedPartitionIndex, existIntraInputKeyCorrelation));
    }

    private static BlockingInputInfo createBlockingInputInfo(
            int numPartitions,
            int numSubpartitions,
            List<Integer> skewedPartitionIndex,
            boolean existIntraInputKeyCorrelation) {
        return VertexInputInfoComputerTestUtil.createBlockingInputInfos(
                        1,
                        1,
                        numPartitions,
                        numSubpartitions,
                        existIntraInputKeyCorrelation,
                        false,
                        1,
                        10,
                        skewedPartitionIndex,
                        List.of(),
                        true)
                .get(0);
    }

    private static PointwiseVertexInputInfoComputer createPointwiseVertexInputInfoComputer() {
        return new PointwiseVertexInputInfoComputer();
    }
}
