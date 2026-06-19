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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.executiongraph.IndexRangeUtil.mergeIndexRanges;
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

    private static void checkParallelism(
            int targetParallelism,
            Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfoMap) {
        vertexInputInfoMap
                .values()
                .forEach(
                        info ->
                                assertThat(info.getExecutionVertexInputInfos().size())
                                        .isEqualTo(targetParallelism));
    }

    public static void checkConsumedSubpartitionGroups(
            List<Map<IndexRange, IndexRange>> targetConsumedSubpartitionGroups,
            List<BlockingInputInfo> inputInfos,
            Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfoMap) {
        JobVertexInputInfo vertexInputInfo =
                checkAndGetJobVertexInputInfo(inputInfos, vertexInputInfoMap);
        List<ExecutionVertexInputInfo> executionVertexInputInfos =
                vertexInputInfo.getExecutionVertexInputInfos();
        for (int i = 0; i < executionVertexInputInfos.size(); i++) {
            assertThat(executionVertexInputInfos.get(i).getConsumedSubpartitionGroups())
                    .isEqualTo(targetConsumedSubpartitionGroups.get(i));
        }
    }

    public static void checkConsumedDataVolumePerSubtask(
            long[] targetConsumedDataVolume,
            List<BlockingInputInfo> inputInfos,
            Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputs) {
        long[] consumedDataVolume = new long[targetConsumedDataVolume.length];
        for (BlockingInputInfo inputInfo : inputInfos) {
            JobVertexInputInfo vertexInputInfo = vertexInputs.get(inputInfo.getResultId());
            List<ExecutionVertexInputInfo> executionVertexInputInfos =
                    vertexInputInfo.getExecutionVertexInputInfos();
            for (int i = 0; i < executionVertexInputInfos.size(); ++i) {
                ExecutionVertexInputInfo executionVertexInputInfo =
                        executionVertexInputInfos.get(i);
                consumedDataVolume[i] +=
                        executionVertexInputInfo.getConsumedSubpartitionGroups().entrySet().stream()
                                .mapToLong(
                                        entry ->
                                                inputInfo.getNumBytesProduced(
                                                        entry.getKey(), entry.getValue()))
                                .sum();
            }
        }
        assertThat(consumedDataVolume).isEqualTo(targetConsumedDataVolume);
    }

    private static JobVertexInputInfo checkAndGetJobVertexInputInfo(
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

    public static void checkCorrectnessForNonCorrelatedInput(
            Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfoMap,
            BlockingInputInfo inputInfo,
            int targetParallelism) {
        checkParallelism(targetParallelism, vertexInputInfoMap);
        Map<Integer, List<IndexRange>> consumedPartitionToSubpartitionRanges = new HashMap<>();
        vertexInputInfoMap
                .get(inputInfo.getResultId())
                .getExecutionVertexInputInfos()
                .forEach(
                        info ->
                                info.getConsumedSubpartitionGroups()
                                        .forEach(
                                                (partitionRange, subpartitionRange) -> {
                                                    for (int i = partitionRange.getStartIndex();
                                                            i <= partitionRange.getEndIndex();
                                                            ++i) {
                                                        consumedPartitionToSubpartitionRanges
                                                                .computeIfAbsent(
                                                                        i, k -> new ArrayList<>())
                                                                .add(subpartitionRange);
                                                    }
                                                }));
        Set<Integer> partitionIndex =
                IntStream.rangeClosed(0, inputInfo.getNumPartitions() - 1)
                        .boxed()
                        .collect(Collectors.toSet());
        IndexRange subpartitionRange = new IndexRange(0, inputInfo.getNumSubpartitions(0) - 1);
        assertThat(consumedPartitionToSubpartitionRanges.keySet()).isEqualTo(partitionIndex);
        consumedPartitionToSubpartitionRanges
                .values()
                .forEach(
                        subpartitionRanges -> {
                            List<IndexRange> mergedRange = mergeIndexRanges(subpartitionRanges);
                            assertThat(mergedRange.size()).isEqualTo(1);
                            assertThat(mergedRange.get(0)).isEqualTo(subpartitionRange);
                        });
    }

    public static void checkCorrectnessForCorrelatedInputs(
            Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfoMap,
            List<BlockingInputInfo> inputInfos,
            int targetParallelism,
            int numSubpartitions) {
        checkParallelism(targetParallelism, vertexInputInfoMap);
        Map<Integer, List<BlockingInputInfo>> inputInfosGroupByTypeNumber =
                inputInfos.stream()
                        .collect(Collectors.groupingBy(BlockingInputInfo::getInputTypeNumber));

        Map<Integer, List<JobVertexInputInfo>> vertexInputInfosGroupByTypeNumber =
                inputInfosGroupByTypeNumber.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        e ->
                                                e.getValue().stream()
                                                        .map(
                                                                v ->
                                                                        vertexInputInfoMap.get(
                                                                                v.getResultId()))
                                                        .collect(Collectors.toList())));

        Map<JobVertexInputInfo, Integer> vertexInputInfoToNumPartitionsMap =
                inputInfosGroupByTypeNumber.values().stream()
                        .flatMap(List::stream)
                        .collect(
                                Collectors.toMap(
                                        v -> vertexInputInfoMap.get(v.getResultId()),
                                        BlockingInputInfo::getNumPartitions));
        assertThat(vertexInputInfosGroupByTypeNumber.size()).isEqualTo(2);
        checkCorrectnessForCorrelatedInputs(
                vertexInputInfosGroupByTypeNumber.get(1),
                vertexInputInfosGroupByTypeNumber.get(2),
                vertexInputInfoToNumPartitionsMap,
                numSubpartitions);
    }

    private static void checkCorrectnessForCorrelatedInputs(
            List<JobVertexInputInfo> infosWithTypeNumber1,
            List<JobVertexInputInfo> infosWithTypeNumber2,
            Map<JobVertexInputInfo, Integer> vertexInputInfoToNumPartitionsMap,
            int numSubpartitions) {
        for (JobVertexInputInfo vertexInputInfo : infosWithTypeNumber1) {
            for (JobVertexInputInfo jobVertexInputInfo : infosWithTypeNumber2) {
                checkCorrectnessForConsumedSubpartitionRanges(
                        vertexInputInfo,
                        jobVertexInputInfo,
                        vertexInputInfoToNumPartitionsMap.get(vertexInputInfo),
                        vertexInputInfoToNumPartitionsMap.get(jobVertexInputInfo),
                        numSubpartitions);
            }
        }
    }

    /**
     * This method performs the following checks on inputInfo1 and inputInfo2: 1. Whether they
     * subscribe to all subpartitions. 2. Whether the data in subpartitions with the same index
     * across both inputInfo1 and inputInfo2 is traversed in a Cartesian product manner.
     *
     * @param inputInfo1 the inputInfo1
     * @param inputInfo2 the inputInfo2
     * @param numPartitions1 the number of partitions for inputInfo1
     * @param numPartitions2 the number of partitions for inputInfo2
     * @param numSubpartitions the number of subpartitions for both inputInfo1 and inputInfo2
     */
    private static void checkCorrectnessForConsumedSubpartitionRanges(
            JobVertexInputInfo inputInfo1,
            JobVertexInputInfo inputInfo2,
            int numPartitions1,
            int numPartitions2,
            int numSubpartitions) {
        assertThat(inputInfo1.getExecutionVertexInputInfos().size())
                .isEqualTo(inputInfo2.getExecutionVertexInputInfos().size());
        // subpartition index of input1 -> partition index of input1 -> partition index ranges of
        // input2
        Map<Integer, Map<Integer, Set<IndexRange>>> input1ToInput2 = new HashMap<>();
        for (int i = 0; i < inputInfo1.getExecutionVertexInputInfos().size(); i++) {
            Map<Integer, Set<IndexRange>> subpartitionIndexToPartition1 =
                    getConsumedSubpartitionIndexToPartitionRanges(
                            inputInfo1
                                    .getExecutionVertexInputInfos()
                                    .get(i)
                                    .getConsumedSubpartitionGroups());
            Map<Integer, Set<IndexRange>> subpartitionIndexToPartition2 =
                    getConsumedSubpartitionIndexToPartitionRanges(
                            inputInfo2
                                    .getExecutionVertexInputInfos()
                                    .get(i)
                                    .getConsumedSubpartitionGroups());
            subpartitionIndexToPartition1.forEach(
                    (subpartitionIndex, partitionRanges) -> {
                        assertThat(subpartitionIndexToPartition2.containsKey(subpartitionIndex))
                                .isTrue();
                        partitionRanges.forEach(
                                partitionRange -> {
                                    for (int j = partitionRange.getStartIndex();
                                            j <= partitionRange.getEndIndex();
                                            ++j) {
                                        input1ToInput2
                                                .computeIfAbsent(
                                                        subpartitionIndex, k -> new HashMap<>())
                                                .computeIfAbsent(j, k -> new HashSet<>())
                                                .addAll(
                                                        subpartitionIndexToPartition2.get(
                                                                subpartitionIndex));
                                    }
                                });
                    });
        }
        Set<Integer> partitionIndex =
                IntStream.rangeClosed(0, numPartitions1 - 1).boxed().collect(Collectors.toSet());
        Set<Integer> subpartitionIndexSet =
                IntStream.rangeClosed(0, numSubpartitions - 1).boxed().collect(Collectors.toSet());
        IndexRange partitionRange2 = new IndexRange(0, numPartitions2 - 1);
        assertThat(input1ToInput2.keySet()).isEqualTo(subpartitionIndexSet);
        input1ToInput2.forEach(
                (subpartitionIndex, input1ToInput2PartitionRanges) -> {
                    assertThat(input1ToInput2PartitionRanges.keySet()).isEqualTo(partitionIndex);
                    input1ToInput2PartitionRanges
                            .values()
                            .forEach(
                                    partitionRanges -> {
                                        List<IndexRange> mergedRange =
                                                mergeIndexRanges(partitionRanges);
                                        assertThat(mergedRange.size()).isEqualTo(1);
                                        assertThat(mergedRange.get(0)).isEqualTo(partitionRange2);
                                    });
                });
    }

    private static Map<Integer, Set<IndexRange>> getConsumedSubpartitionIndexToPartitionRanges(
            Map<IndexRange, IndexRange> consumedSubpartitionGroups) {
        Map<Integer, Set<IndexRange>> subpartitionIndexToPartition = new HashMap<>();
        consumedSubpartitionGroups.forEach(
                (partitionRange, subpartitionRange) -> {
                    for (int j = subpartitionRange.getStartIndex();
                            j <= subpartitionRange.getEndIndex();
                            ++j) {
                        subpartitionIndexToPartition
                                .computeIfAbsent(j, key -> new HashSet<>())
                                .add(partitionRange);
                    }
                });
        return subpartitionIndexToPartition;
    }
}
