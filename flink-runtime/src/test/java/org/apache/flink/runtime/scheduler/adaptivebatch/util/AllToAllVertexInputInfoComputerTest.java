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

import static org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil.checkJobVertexInputInfo;

/** Tests for {@link AllToAllVertexInputInfoComputer}. */
class AllToAllVertexInputInfoComputerTest {

    @Test
    void testComputeAllInputExistIntraInputKeyCorrelation() {
        testComputeAllInputExistIntraInputKeyCorrelation(1);
        testComputeAllInputExistIntraInputKeyCorrelation(10);
    }

    void testComputeAllInputExistIntraInputKeyCorrelation(int numInputInfos) {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos =
                createBlockingInputInfos(1, numInputInfos, 10, true, List.of(0));
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, numInputInfos, 10, true, List.of());
        inputInfos.addAll(leftInputInfos);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 10, 1, 10);
        List<Map<IndexRange, IndexRange>> targetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(1, 1)),
                        Map.of(new IndexRange(0, 9), new IndexRange(2, 2)));
        checkJobVertexInputInfo(3, inputInfos, targetConsumedSubpartitionGroups, vertexInputs);
    }

    @Test
    void testComputeOneSkewedInputNotExistIntraInputKeyCorrelation() {
        testComputeOneSkewedInputNotExistIntraInputKeyCorrelation(1);
        testComputeOneSkewedInputNotExistIntraInputKeyCorrelation(10);
    }

    void testComputeOneSkewedInputNotExistIntraInputKeyCorrelation(int numInputInfos) {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos =
                createBlockingInputInfos(1, numInputInfos, 10, false, List.of(0));
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, numInputInfos, 10, true, List.of());
        inputInfos.addAll(leftInputInfos);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 10, 1, 10);
        List<Map<IndexRange, IndexRange>> leftTargetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 1), new IndexRange(0, 0)),
                        Map.of(new IndexRange(2, 3), new IndexRange(0, 0)),
                        Map.of(new IndexRange(4, 5), new IndexRange(0, 0)),
                        Map.of(new IndexRange(6, 7), new IndexRange(0, 0)),
                        Map.of(new IndexRange(8, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(1, 1)),
                        Map.of(new IndexRange(0, 9), new IndexRange(2, 2)));
        checkJobVertexInputInfo(
                7, leftInputInfos, leftTargetConsumedSubpartitionGroups, vertexInputs);
        List<Map<IndexRange, IndexRange>> rightTargetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 9), new IndexRange(1, 1)),
                        Map.of(new IndexRange(0, 9), new IndexRange(2, 2)));
        checkJobVertexInputInfo(
                7, rightInputInfos, rightTargetConsumedSubpartitionGroups, vertexInputs);
    }

    @Test
    void testComputeAllSkewedInputNotExistIntraInputKeyCorrelation() {
        testComputeAllSkewedInputNotExistIntraInputKeyCorrelation(1);
        testComputeAllSkewedInputNotExistIntraInputKeyCorrelation(10);
    }

    void testComputeAllSkewedInputNotExistIntraInputKeyCorrelation(int numInputInfos) {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos =
                createBlockingInputInfos(1, numInputInfos, 2, false, List.of(1));
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, numInputInfos, 2, false, List.of(1));
        inputInfos.addAll(leftInputInfos);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 2, 1, 2);

        List<Map<IndexRange, IndexRange>> leftTargetConsumedSubpartitionGroups =
                List.of(
                        Map.of(
                                new IndexRange(0, 0),
                                new IndexRange(1, 1),
                                new IndexRange(0, 1),
                                new IndexRange(0, 0)),
                        Map.of(
                                new IndexRange(1, 1),
                                new IndexRange(1, 1),
                                new IndexRange(0, 1),
                                new IndexRange(2, 2)));

        List<Map<IndexRange, IndexRange>> rightTargetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 1), new IndexRange(0, 1)),
                        Map.of(new IndexRange(0, 1), new IndexRange(1, 2)));

        checkJobVertexInputInfo(
                2, leftInputInfos, leftTargetConsumedSubpartitionGroups, vertexInputs);
        checkJobVertexInputInfo(
                2, rightInputInfos, rightTargetConsumedSubpartitionGroups, vertexInputs);
    }

    @Test
    void testComputeAggAllToAllWithDifferentNumPartitions() {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos1 =
                createBlockingInputInfos(1, 1, 2, true, List.of());
        List<BlockingInputInfo> leftInputInfos2 =
                createBlockingInputInfos(1, 1, 3, true, List.of());
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, 1, 2, true, List.of());
        inputInfos.addAll(leftInputInfos1);
        inputInfos.addAll(leftInputInfos2);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 2, 1, 2);

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

        checkJobVertexInputInfo(
                2, leftInputInfos1, left1TargetConsumedSubpartitionGroups, vertexInputs);
        checkJobVertexInputInfo(
                2, leftInputInfos2, left2TargetConsumedSubpartitionGroups, vertexInputs);
        checkJobVertexInputInfo(
                2, rightInputInfos, rightTargetConsumedSubpartitionGroups, vertexInputs);
    }

    @Test
    void testComputeAggAllToAllWithDifferentNumPartitionsAndDataSkewed() {
        AllToAllVertexInputInfoComputer computer = createAllToAllVertexInputInfoComputer();
        List<BlockingInputInfo> inputInfos = new ArrayList<>();
        List<BlockingInputInfo> leftInputInfos1 =
                createBlockingInputInfos(1, 1, 2, false, List.of(1));
        List<BlockingInputInfo> leftInputInfos2 =
                createBlockingInputInfos(1, 1, 3, false, List.of(1));
        List<BlockingInputInfo> rightInputInfos =
                createBlockingInputInfos(2, 1, 2, false, List.of(1));
        inputInfos.addAll(leftInputInfos1);
        inputInfos.addAll(leftInputInfos2);
        inputInfos.addAll(rightInputInfos);
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs =
                computer.compute(new JobVertexID(), inputInfos, 2, 1, 2);

        List<Map<IndexRange, IndexRange>> left1TargetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 1), new IndexRange(0, 1)),
                        Map.of(new IndexRange(0, 1), new IndexRange(1, 2)));

        List<Map<IndexRange, IndexRange>> left2TargetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 2), new IndexRange(0, 1)),
                        Map.of(new IndexRange(0, 2), new IndexRange(1, 2)));

        List<Map<IndexRange, IndexRange>> rightTargetConsumedSubpartitionGroups =
                List.of(
                        Map.of(
                                new IndexRange(0, 0),
                                new IndexRange(1, 1),
                                new IndexRange(0, 1),
                                new IndexRange(0, 0)),
                        Map.of(
                                new IndexRange(1, 1),
                                new IndexRange(1, 1),
                                new IndexRange(0, 1),
                                new IndexRange(2, 2)));

        checkJobVertexInputInfo(
                2, leftInputInfos1, left1TargetConsumedSubpartitionGroups, vertexInputs);
        checkJobVertexInputInfo(
                2, leftInputInfos2, left2TargetConsumedSubpartitionGroups, vertexInputs);
        checkJobVertexInputInfo(
                2, rightInputInfos, rightTargetConsumedSubpartitionGroups, vertexInputs);
    }

    private static List<BlockingInputInfo> createBlockingInputInfos(
            int typeNumber,
            int numInputInfos,
            int numPartitions,
            boolean existIntraInputKeyCorrelation,
            List<Integer> skewedSubpartitionIndex) {
        return VertexInputInfoComputerTestUtil.createBlockingInputInfos(
                typeNumber,
                numInputInfos,
                numPartitions,
                3,
                existIntraInputKeyCorrelation,
                true,
                1,
                10,
                List.of(),
                skewedSubpartitionIndex,
                false);
    }

    private static AllToAllVertexInputInfoComputer createAllToAllVertexInputInfoComputer() {
        return new AllToAllVertexInputInfoComputer(10, 4, 10);
    }
}
