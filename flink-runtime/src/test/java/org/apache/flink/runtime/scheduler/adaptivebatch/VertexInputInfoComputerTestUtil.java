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

import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Utils for vertex input info computer test. */
public class VertexInputInfoComputerTestUtil {
    public static List<BlockingInputInfo> createBlockingInputInfos(
            int typeNumber,
            int numInputInfos,
            int numPartitions,
            int numSubpartitions,
            boolean existIntraInputKeyCorrelation,
            boolean existInterInputsKeyCorrelation,
            int defaultSize,
            double skewedFactor,
            List<Integer> skewedPartitionIndex,
            List<Integer> skewedSubpartitionIndex,
            boolean isPointwise) {
        List<BlockingInputInfo> blockingInputInfos = new ArrayList<>();
        for (int i = 0; i < numInputInfos; i++) {
            Map<Integer, long[]> subpartitionBytesByPartitionIndex = new HashMap<>();
            for (int j = 0; j < numPartitions; j++) {
                long[] subpartitionBytes = new long[numSubpartitions];
                for (int k = 0; k < numSubpartitions; k++) {
                    if (skewedSubpartitionIndex.contains(k) || skewedPartitionIndex.contains(j)) {
                        subpartitionBytes[k] = (long) (defaultSize * skewedFactor);
                    } else {
                        subpartitionBytes[k] = defaultSize;
                    }
                }
                subpartitionBytesByPartitionIndex.put(j, subpartitionBytes);
            }
            BlockingResultInfo resultInfo;
            if (isPointwise) {
                resultInfo =
                        new PointwiseBlockingResultInfo(
                                new IntermediateDataSetID(),
                                numPartitions,
                                numSubpartitions,
                                subpartitionBytesByPartitionIndex);
            } else {
                resultInfo =
                        new AllToAllBlockingResultInfo(
                                new IntermediateDataSetID(),
                                numPartitions,
                                numSubpartitions,
                                false,
                                subpartitionBytesByPartitionIndex);
            }
            blockingInputInfos.add(
                    new BlockingInputInfo(
                            resultInfo,
                            typeNumber,
                            existInterInputsKeyCorrelation,
                            existIntraInputKeyCorrelation));
        }
        return blockingInputInfos;
    }

    public static void checkJobVertexInputInfo(
            int targetParallelism,
            List<BlockingInputInfo> inputInfos,
            List<Map<IndexRange, IndexRange>> targetConsumedSubpartitionGroups,
            Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfoMap) {
        JobVertexInputInfo vertexInputInfo =
                checkAndGetJobVertexInputInfo(inputInfos, vertexInputInfoMap);
        List<ExecutionVertexInputInfo> executionVertexInputInfos =
                vertexInputInfo.getExecutionVertexInputInfos();
        assertThat(executionVertexInputInfos.size()).isEqualTo(targetParallelism);
        for (int i = 0; i < targetParallelism; i++) {
            assertThat(executionVertexInputInfos.get(i).getConsumedSubpartitionGroups())
                    .isEqualTo(targetConsumedSubpartitionGroups.get(i));
        }
    }

    public static JobVertexInputInfo checkAndGetJobVertexInputInfo(
            List<BlockingInputInfo> inputInfos,
            Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfoMap) {
        List<JobVertexInputInfo> vertexInputInfos =
                inputInfos.stream()
                        .map(inputInfo -> vertexInputInfoMap.get(inputInfo.getResultId()))
                        .collect(Collectors.toList());
        assertThat(vertexInputInfos.size()).isEqualTo(inputInfos.size());
        JobVertexInputInfo baseVertexInputInfo = vertexInputInfos.get(0);
        for (int i = 1; i < vertexInputInfos.size(); i++) {
            assertThat(vertexInputInfos.get(i)).isEqualTo(baseVertexInputInfo);
        }
        return baseVertexInputInfo;
    }
}
