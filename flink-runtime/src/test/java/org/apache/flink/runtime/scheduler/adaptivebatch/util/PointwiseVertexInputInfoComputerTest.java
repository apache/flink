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
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;
import org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.scheduler.adaptivebatch.VertexInputInfoComputerTestUtil.checkJobVertexInputInfo;

/** Tests for {@link PointwiseVertexInputInfoComputer}. */
class PointwiseVertexInputInfoComputerTest {

    @Test
    void testComputePointwiseInputWithoutSkewed() {
        PointwiseVertexInputInfoComputer computer = createPointwiseVertexInputInfoComputer();
        BlockingInputInfo inputInfo = createBlockingInputInfos(2, List.of());
        JobVertexInputInfo vertexInputs = computer.compute(inputInfo, 2);
        List<Map<IndexRange, IndexRange>> targetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 0), new IndexRange(0, 2)),
                        Map.of(new IndexRange(1, 1), new IndexRange(0, 2)));
        checkJobVertexInputInfo(
                2,
                List.of(inputInfo),
                targetConsumedSubpartitionGroups,
                Map.of(inputInfo.getResultId(), vertexInputs));

        JobVertexInputInfo vertexInputs2 = computer.compute(inputInfo, 3);
        List<Map<IndexRange, IndexRange>> targetConsumedSubpartitionGroups2 =
                List.of(
                        Map.of(new IndexRange(0, 0), new IndexRange(0, 1)),
                        Map.of(
                                new IndexRange(0, 0),
                                new IndexRange(2, 2),
                                new IndexRange(1, 1),
                                new IndexRange(0, 0)),
                        Map.of(new IndexRange(1, 1), new IndexRange(1, 2)));
        checkJobVertexInputInfo(
                3,
                List.of(inputInfo),
                targetConsumedSubpartitionGroups2,
                Map.of(inputInfo.getResultId(), vertexInputs2));
    }

    @Test
    void testComputePointwiseInputWithSkewed() {
        PointwiseVertexInputInfoComputer computer = createPointwiseVertexInputInfoComputer();
        BlockingInputInfo inputInfo = createBlockingInputInfos(3, List.of(0));
        JobVertexInputInfo vertexInputs = computer.compute(inputInfo, 3);
        List<Map<IndexRange, IndexRange>> targetConsumedSubpartitionGroups =
                List.of(
                        Map.of(new IndexRange(0, 0), new IndexRange(0, 0)),
                        Map.of(new IndexRange(0, 0), new IndexRange(1, 1)),
                        Map.of(
                                new IndexRange(0, 0),
                                new IndexRange(2, 2),
                                new IndexRange(1, 2),
                                new IndexRange(0, 2)));
        checkJobVertexInputInfo(
                3,
                List.of(inputInfo),
                targetConsumedSubpartitionGroups,
                Map.of(inputInfo.getResultId(), vertexInputs));

        BlockingInputInfo inputInfo2 = createBlockingInputInfos(3, List.of(1));
        JobVertexInputInfo vertexInputs2 = computer.compute(inputInfo2, 3);
        List<Map<IndexRange, IndexRange>> targetConsumedSubpartitionGroups2 =
                List.of(
                        Map.of(
                                new IndexRange(0, 0),
                                new IndexRange(0, 2),
                                new IndexRange(1, 1),
                                new IndexRange(0, 0)),
                        Map.of(new IndexRange(1, 1), new IndexRange(1, 1)),
                        Map.of(
                                new IndexRange(1, 1),
                                new IndexRange(2, 2),
                                new IndexRange(2, 2),
                                new IndexRange(0, 2)));
        checkJobVertexInputInfo(
                3,
                List.of(inputInfo2),
                targetConsumedSubpartitionGroups2,
                Map.of(inputInfo2.getResultId(), vertexInputs2));
    }

    private static BlockingInputInfo createBlockingInputInfos(
            int numPartitions, List<Integer> skewedPartitionIndex) {
        return VertexInputInfoComputerTestUtil.createBlockingInputInfos(
                        1,
                        1,
                        numPartitions,
                        3,
                        false,
                        false,
                        1,
                        10,
                        skewedPartitionIndex,
                        List.of(),
                        true)
                .get(0);
    }

    private static PointwiseVertexInputInfoComputer createPointwiseVertexInputInfoComputer() {
        return new PointwiseVertexInputInfoComputer(10);
    }
}
