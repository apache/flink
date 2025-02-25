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
import org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.scheduler.adaptivebatch.util.SubpartitionSlice.createSubpartitionSlice;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.SubpartitionSlice.createSubpartitionSlicesByMultiPartitionRanges;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.calculateDataVolumePerTaskForInput;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.calculateDataVolumePerTaskForInputsGroup;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.cartesianProduct;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetParallelism;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetSubpartitionNum;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetSubpartitionNumForAggregatedInputs;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.createJobVertexInputInfos;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.createdJobVertexInputInfoForBroadcast;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.createdJobVertexInputInfoForNonBroadcast;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.getNonBroadcastInputInfos;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.isLegalParallelism;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.tryComputeSubpartitionSliceRange;
import static org.apache.flink.util.Preconditions.checkState;

/** Helper class that computes VertexInputInfo for all to all like inputs. */
public class AllToAllVertexInputInfoComputer {
    private static final Logger LOG =
            LoggerFactory.getLogger(AllToAllVertexInputInfoComputer.class);

    private final double skewedFactor;
    private final long defaultSkewedThreshold;

    public AllToAllVertexInputInfoComputer(double skewedFactor, long defaultSkewedThreshold) {
        this.skewedFactor = skewedFactor;
        this.defaultSkewedThreshold = defaultSkewedThreshold;
    }

    /**
     * Decide parallelism and input infos, which will make the data be evenly distributed to
     * downstream subtasks for ALL_TO_ALL, such that different downstream subtasks consume roughly
     * the same amount of data.
     *
     * <p>Assume there are two input infos upstream, each with three partitions and two
     * subpartitions, their data bytes information are: input1: 0->[1,1] 1->[2,2] 2->[3,3], input2:
     * 0->[1,1] 1->[1,1] 2->[1,1]. This method processes the data as follows: <br>
     * 1. Create subpartition slices for inputs with same type number, different from pointwise
     * computer, this method creates subpartition slices by following these steps: Firstly,
     * reorganize the data by subpartition index: input1: {0->[1,2,3],1->[1,2,3]}, input2:
     * {0->[1,1,1],1->[1,1,1]}. Secondly, split subpartitions with the same index into relatively
     * balanced n parts (if possible): {0->[1,2][3],1->[1,2][3]}, {0->[1,1,1],1->[1,1,1]}. Then
     * perform a cartesian product operation to ensure data correctness input1:
     * {0->[1,2],0->[3],1->[1,2],1->[3]}, input2: {0->[1,1,1],0->[1,1,1],1->[1,1,1],1->[1,1,1]},
     * Finally, create subpartition slices base on the result of the previous step. i.e., each input
     * has four balanced subpartition slices.<br>
     * 2. Based on the above subpartition slices, calculate the subpartition slice range each task
     * needs to subscribe to, considering data volume and parallelism constraints:
     * [0,0],[1,1],[2,2],[3,3]<br>
     * 3. Convert the calculated subpartition slice range to the form of partition index range ->
     * subpartition index range:<br>
     * task0: input1: {[0,1]->[0]} input2:{[0,2]->[0]}<br>
     * task1: input1: {[2,2]->[0]} input2:{[0,2]->[0]}<br>
     * task2: input1: {[0,1]->[1]} input2:{[0,2]->[1]}<br>
     * task3: input1: {[2,2]->[1]} input2:{[0,2]->[1]}
     *
     * @param jobVertexId The job vertex id
     * @param inputInfos The information of consumed blocking results
     * @param parallelism The parallelism of the job vertex
     * @param minParallelism the min parallelism
     * @param maxParallelism the max parallelism
     * @param dataVolumePerTask proposed data volume per task for this set of inputInfo
     * @return the parallelism and vertex input infos
     */
    public Map<IntermediateDataSetID, JobVertexInputInfo> compute(
            JobVertexID jobVertexId,
            List<BlockingInputInfo> inputInfos,
            int parallelism,
            int minParallelism,
            int maxParallelism,
            long dataVolumePerTask) {
        // For inputs with inter-keys correlation should be process together,  as there is a
        // correlation between them. For inputs without inter-keys, we should handle them
        // separately.
        List<BlockingInputInfo> inputInfosWithoutInterKeysCorrelation = new ArrayList<>();
        List<BlockingInputInfo> inputInfosWithInterKeysCorrelation = new ArrayList<>();
        for (BlockingInputInfo inputInfo : inputInfos) {
            if (inputInfo.areInterInputsKeysCorrelated()) {
                inputInfosWithInterKeysCorrelation.add(inputInfo);
            } else {
                inputInfosWithoutInterKeysCorrelation.add(inputInfo);
            }
        }

        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        if (!inputInfosWithInterKeysCorrelation.isEmpty()) {
            vertexInputInfos.putAll(
                    computeJobVertexInputInfosForInputsWithInterKeysCorrelation(
                            jobVertexId,
                            inputInfosWithInterKeysCorrelation,
                            parallelism,
                            minParallelism,
                            maxParallelism,
                            calculateDataVolumePerTaskForInputsGroup(
                                    dataVolumePerTask,
                                    inputInfosWithInterKeysCorrelation,
                                    inputInfos)));
            // Ensure the parallelism of inputs without inter and intra correlations is
            // consistent with decided parallelism.
            parallelism = checkAndGetParallelism(vertexInputInfos.values());
        }

        if (!inputInfosWithoutInterKeysCorrelation.isEmpty()) {
            vertexInputInfos.putAll(
                    computeJobVertexInputInfosForInputsWithoutInterKeysCorrelation(
                            inputInfosWithoutInterKeysCorrelation,
                            parallelism,
                            calculateDataVolumePerTaskForInputsGroup(
                                    dataVolumePerTask,
                                    inputInfosWithoutInterKeysCorrelation,
                                    inputInfos)));
        }

        return vertexInputInfos;
    }

    private Map<IntermediateDataSetID, JobVertexInputInfo>
            computeJobVertexInputInfosForInputsWithInterKeysCorrelation(
                    JobVertexID jobVertexId,
                    List<BlockingInputInfo> inputInfos,
                    int parallelism,
                    int minParallelism,
                    int maxParallelism,
                    long dataVolumePerTask) {
        List<BlockingInputInfo> nonBroadcastInputInfos = getNonBroadcastInputInfos(inputInfos);
        if (nonBroadcastInputInfos.isEmpty()) {
            LOG.info(
                    "All inputs are broadcast for vertex {}, fallback to compute a parallelism that can evenly distribute num subpartitions.",
                    jobVertexId);
            // This computer is only used in the adaptive batch scenario, where isDynamicGraph
            // should always be true.
            return VertexInputInfoComputationUtils.computeVertexInputInfos(
                    parallelism, inputInfos, true);
        }

        // Divide the data into balanced n parts and describe each part by SubpartitionSlice.
        Map<Integer, List<SubpartitionSlice>> subpartitionSlicesByTypeNumber =
                createSubpartitionSlicesForInputsWithInterKeysCorrelation(
                        nonBroadcastInputInfos, dataVolumePerTask);

        // Distribute the input data evenly among the downstream tasks and record the
        // subpartition slice range for each task.
        Optional<List<IndexRange>> optionalSubpartitionSliceRanges =
                tryComputeSubpartitionSliceRange(
                        minParallelism,
                        maxParallelism,
                        dataVolumePerTask,
                        subpartitionSlicesByTypeNumber);

        if (optionalSubpartitionSliceRanges.isEmpty()) {
            LOG.info(
                    "Cannot find a legal parallelism to evenly distribute data amount for job vertex {}, "
                            + "fallback to compute a parallelism that can evenly distribute num subpartitions.",
                    jobVertexId);
            // This computer is only used in the adaptive batch scenario, where isDynamicGraph
            // should always be true.
            return VertexInputInfoComputationUtils.computeVertexInputInfos(
                    parallelism, inputInfos, true);
        }

        List<IndexRange> subpartitionSliceRanges = optionalSubpartitionSliceRanges.get();

        checkState(
                isLegalParallelism(subpartitionSliceRanges.size(), minParallelism, maxParallelism));

        // Create vertex input info based on the subpartition slice and its range.
        return createJobVertexInputInfos(
                inputInfos,
                subpartitionSlicesByTypeNumber,
                subpartitionSliceRanges,
                index -> inputInfos.get(index).getInputTypeNumber());
    }

    private Map<Integer, List<SubpartitionSlice>>
            createSubpartitionSlicesForInputsWithInterKeysCorrelation(
                    List<BlockingInputInfo> nonBroadcastInputInfos, long dataVolumePerTask) {
        // Aggregate input info with the same type number.
        Map<Integer, AggregatedBlockingInputInfo> aggregatedInputInfoByTypeNumber =
                createAggregatedBlockingInputInfos(nonBroadcastInputInfos, dataVolumePerTask);
        int subPartitionNum =
                checkAndGetSubpartitionNumForAggregatedInputs(
                        aggregatedInputInfoByTypeNumber.values());
        Map<Integer, List<SubpartitionSlice>> subpartitionSliceGroupByTypeNumber = new HashMap<>();
        for (int subpartitionIndex = 0; subpartitionIndex < subPartitionNum; ++subpartitionIndex) {
            // Split the given subpartition group into balanced subpartition slices.
            Map<Integer, List<SubpartitionSlice>> subpartitionSlices =
                    createBalancedSubpartitionSlicesForInputsWithInterKeysCorrelation(
                            subpartitionIndex, aggregatedInputInfoByTypeNumber);

            List<Integer> typeNumberList = new ArrayList<>(subpartitionSlices.keySet());

            List<List<SubpartitionSlice>> originalRangeLists =
                    new ArrayList<>(subpartitionSlices.values());

            // Perform the Cartesian product for inputs with inter-inputs key correlation.
            List<List<SubpartitionSlice>> cartesianProductRangeList =
                    cartesianProduct(originalRangeLists);

            for (List<SubpartitionSlice> subpartitionSlice : cartesianProductRangeList) {
                for (int j = 0; j < subpartitionSlice.size(); ++j) {
                    int typeNumber = typeNumberList.get(j);
                    subpartitionSliceGroupByTypeNumber
                            .computeIfAbsent(typeNumber, ignored -> new ArrayList<>())
                            .add(subpartitionSlice.get(j));
                }
            }
        }

        return subpartitionSliceGroupByTypeNumber;
    }

    private Map<Integer, AggregatedBlockingInputInfo> createAggregatedBlockingInputInfos(
            List<BlockingInputInfo> nonBroadcastInputInfos, long dataVolumePerTask) {
        Map<Integer, List<BlockingInputInfo>> inputsByTypeNumber =
                nonBroadcastInputInfos.stream()
                        .collect(Collectors.groupingBy(BlockingInputInfo::getInputTypeNumber));
        // Inputs with the same type number should be data with the same type, as operators will
        // process them in the same way. Currently, they can be considered to must have the same
        // IntraInputKeyCorrelation
        checkState(hasSameIntraInputKeyCorrelation(inputsByTypeNumber));

        Map<Integer, AggregatedBlockingInputInfo> blockingInputInfoContexts = new HashMap<>();
        for (Map.Entry<Integer, List<BlockingInputInfo>> entry : inputsByTypeNumber.entrySet()) {
            Integer typeNumber = entry.getKey();
            List<BlockingInputInfo> inputInfos = entry.getValue();
            blockingInputInfoContexts.put(
                    typeNumber,
                    AggregatedBlockingInputInfo.createAggregatedBlockingInputInfo(
                            defaultSkewedThreshold, skewedFactor, dataVolumePerTask, inputInfos));
        }

        return blockingInputInfoContexts;
    }

    /**
     * Creates balanced subpartition slices for inputs with inter-key correlations.
     *
     * <p>This method generates a mapping of subpartition indices to lists of subpartition slices,
     * ensuring balanced distribution of input data. When a subpartition is splittable and has data
     * skew, we will split it into n continuous and balanced parts (by split its partition range).
     * If the input is not splittable, this step will be skipped, and subpartitions with the same
     * index will be aggregated into a single SubpartitionSlice.
     *
     * @param subpartitionIndex the index of the subpartition being processed.
     * @param aggregatedInputInfoByTypeNumber a map of aggregated blocking input info, keyed by
     *     input type number.
     * @return a map where the key is the input type number and the value is a list of subpartition
     *     slices for the specified subpartition.
     */
    private static Map<Integer, List<SubpartitionSlice>>
            createBalancedSubpartitionSlicesForInputsWithInterKeysCorrelation(
                    int subpartitionIndex,
                    Map<Integer, AggregatedBlockingInputInfo> aggregatedInputInfoByTypeNumber) {
        Map<Integer, List<SubpartitionSlice>> subpartitionSlices = new HashMap<>();
        IndexRange subpartitionRange = new IndexRange(subpartitionIndex, subpartitionIndex);
        for (Map.Entry<Integer, AggregatedBlockingInputInfo> entry :
                aggregatedInputInfoByTypeNumber.entrySet()) {
            Integer typeNumber = entry.getKey();
            AggregatedBlockingInputInfo aggregatedBlockingInputInfo = entry.getValue();
            if (aggregatedBlockingInputInfo.isSplittable()
                    && aggregatedBlockingInputInfo.isSkewedSubpartition(subpartitionIndex)) {
                List<IndexRange> partitionRanges =
                        computePartitionRangesEvenlyData(
                                subpartitionIndex,
                                aggregatedBlockingInputInfo.getTargetSize(),
                                aggregatedBlockingInputInfo.getSubpartitionBytesByPartition());
                subpartitionSlices.put(
                        typeNumber,
                        createSubpartitionSlicesByMultiPartitionRanges(
                                partitionRanges,
                                subpartitionRange,
                                aggregatedBlockingInputInfo.getSubpartitionBytesByPartition()));
            } else {
                IndexRange partitionRange =
                        new IndexRange(0, aggregatedBlockingInputInfo.getMaxPartitionNum() - 1);
                subpartitionSlices.put(
                        typeNumber,
                        Collections.singletonList(
                                SubpartitionSlice.createSubpartitionSlice(
                                        partitionRange,
                                        subpartitionRange,
                                        aggregatedBlockingInputInfo.getAggregatedSubpartitionBytes(
                                                subpartitionIndex))));
            }
        }
        return subpartitionSlices;
    }

    /**
     * Splits a group of subpartitions with the same subpartition index into balanced slices based
     * on the target size and returns the corresponding partition ranges.
     *
     * @param subPartitionIndex The index of the subpartition to be split.
     * @param targetSize The target size for each slice.
     * @param subPartitionBytesByPartitionIndex The byte size information of subpartitions in each
     *     partition, with the partition index as the key and the byte array as the value.
     * @return A list of {@link IndexRange} objects representing the partition ranges of each slice.
     */
    private static List<IndexRange> computePartitionRangesEvenlyData(
            int subPartitionIndex,
            long targetSize,
            Map<Integer, long[]> subPartitionBytesByPartitionIndex) {
        List<IndexRange> splitPartitionRange = new ArrayList<>();
        int partitionNum = subPartitionBytesByPartitionIndex.size();
        long tmpSum = 0;
        int startIndex = 0;
        for (int i = 0; i < partitionNum; ++i) {
            long[] subPartitionBytes = subPartitionBytesByPartitionIndex.get(i);
            long num = subPartitionBytes[subPartitionIndex];
            if (i == startIndex || tmpSum + num < targetSize) {
                tmpSum += num;
            } else {
                splitPartitionRange.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                tmpSum = num;
            }
        }
        splitPartitionRange.add(new IndexRange(startIndex, partitionNum - 1));
        return splitPartitionRange;
    }

    private Map<IntermediateDataSetID, JobVertexInputInfo>
            computeJobVertexInputInfosForInputsWithoutInterKeysCorrelation(
                    List<BlockingInputInfo> inputInfos, int parallelism, long dataVolumePerTask) {
        long totalDataBytes =
                inputInfos.stream().mapToLong(BlockingInputInfo::getNumBytesProduced).sum();
        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        // For inputs without inter-keys, we should process them one-by-one.
        for (BlockingInputInfo inputInfo : inputInfos) {
            vertexInputInfos.put(
                    inputInfo.getResultId(),
                    computeVertexInputInfoForInputWithoutInterKeysCorrelation(
                            inputInfo,
                            parallelism,
                            calculateDataVolumePerTaskForInput(
                                    dataVolumePerTask,
                                    inputInfo.getNumBytesProduced(),
                                    totalDataBytes)));
        }
        return vertexInputInfos;
    }

    private JobVertexInputInfo computeVertexInputInfoForInputWithoutInterKeysCorrelation(
            BlockingInputInfo inputInfo, int parallelism, long dataVolumePerTask) {
        if (inputInfo.isBroadcast()) {
            return createdJobVertexInputInfoForBroadcast(inputInfo, parallelism);
        }

        List<SubpartitionSlice> subpartitionSlices =
                createSubpartitionSlicesForInputWithoutInterKeysCorrelation(inputInfo);
        // Node: SubpartitionSliceRanges does not represent the real index of the subpartitions, but
        // the location of that subpartition in all subpartitions, as we aggregate all subpartitions
        // into a one-digit array to calculate.
        Optional<List<IndexRange>> optionalSubpartitionSliceRanges =
                tryComputeSubpartitionSliceRange(
                        parallelism,
                        parallelism,
                        dataVolumePerTask,
                        Map.of(inputInfo.getInputTypeNumber(), subpartitionSlices));

        if (optionalSubpartitionSliceRanges.isEmpty()) {
            LOG.info(
                    "Cannot find a legal parallelism to evenly distribute data amount for input {}, "
                            + "fallback to compute a parallelism that can evenly distribute num subpartitions.",
                    inputInfo.getResultId());
            return VertexInputInfoComputationUtils.computeVertexInputInfoForPointwise(
                    inputInfo.getNumPartitions(),
                    parallelism,
                    inputInfo::getNumSubpartitions,
                    true);
        }

        List<IndexRange> subpartitionSliceRanges = optionalSubpartitionSliceRanges.get();

        checkState(isLegalParallelism(subpartitionSliceRanges.size(), parallelism, parallelism));

        // Create vertex input info based on the subpartition slice and ranges.
        return createdJobVertexInputInfoForNonBroadcast(
                inputInfo, subpartitionSliceRanges, subpartitionSlices);
    }

    private List<SubpartitionSlice> createSubpartitionSlicesForInputWithoutInterKeysCorrelation(
            BlockingInputInfo inputInfo) {
        List<SubpartitionSlice> subpartitionSlices = new ArrayList<>();
        if (inputInfo.isIntraInputKeyCorrelated()) {
            // If the input has intra-input correlation, we need to ensure all subpartitions
            // with same index are assigned to the same downstream concurrent task.

            // The number of subpartitions of all partitions for all to all blocking result info
            // should be consistent.
            int numSubpartitions = checkAndGetSubpartitionNum(List.of(inputInfo));
            IndexRange partitionRange = new IndexRange(0, inputInfo.getNumPartitions() - 1);
            for (int i = 0; i < numSubpartitions; ++i) {
                IndexRange subpartitionRange = new IndexRange(i, i);
                subpartitionSlices.add(
                        createSubpartitionSlice(
                                partitionRange,
                                subpartitionRange,
                                inputInfo.getNumBytesProduced(partitionRange, subpartitionRange)));
            }
        } else {
            for (int i = 0; i < inputInfo.getNumPartitions(); ++i) {
                IndexRange partitionRange = new IndexRange(i, i);
                for (int j = 0; j < inputInfo.getNumSubpartitions(i); ++j) {
                    IndexRange subpartitionRange = new IndexRange(j, j);
                    subpartitionSlices.add(
                            createSubpartitionSlice(
                                    partitionRange,
                                    subpartitionRange,
                                    inputInfo.getNumBytesProduced(
                                            partitionRange, subpartitionRange)));
                }
            }
        }
        return subpartitionSlices;
    }

    private static boolean hasSameIntraInputKeyCorrelation(
            Map<Integer, List<BlockingInputInfo>> inputGroups) {
        return inputGroups.values().stream()
                .allMatch(
                        inputs ->
                                inputs.stream()
                                                .map(BlockingInputInfo::isIntraInputKeyCorrelated)
                                                .distinct()
                                                .count()
                                        == 1);
    }
}
