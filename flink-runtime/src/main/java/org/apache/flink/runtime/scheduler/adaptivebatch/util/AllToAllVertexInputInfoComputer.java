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

import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.IndexRangeUtil.mergeIndexRanges;
import static org.apache.flink.runtime.scheduler.adaptivebatch.DefaultVertexParallelismAndInputInfosDecider.MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.adjustToClosestLegalParallelism;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.cartesianProduct;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetSubpartitionNum;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.getBroadcastInputInfos;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.getMaxNumPartitions;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.getNonBroadcastInputInfos;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.isLegalParallelism;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Helper class that computes VertexInputInfo for all to all like inputs. */
public class AllToAllVertexInputInfoComputer {
    private static final Logger LOG =
            LoggerFactory.getLogger(AllToAllVertexInputInfoComputer.class);

    private final long dataVolumePerTask;
    private final double skewedFactor;
    private final long defaultSkewedThreshold;

    public AllToAllVertexInputInfoComputer(
            long dataVolumePerTask, double skewedFactor, long defaultSkewedThreshold) {
        this.dataVolumePerTask = dataVolumePerTask;
        this.skewedFactor = skewedFactor;
        this.defaultSkewedThreshold = defaultSkewedThreshold;
    }

    /**
     * Decide parallelism and input infos, which will make the data be evenly distributed to
     * downstream subtasks for ALL_TO_ALL, such that different downstream subtasks consume roughly
     * the same amount of data.
     *
     * <p>Assume there are two input infos upstream, each with three partitions and two
     * subpartitions: <br>
     * input 1: 0->[1,1] 1->[2,2] 2->[3,3]<br>
     * input 2: 0->[1,1] 1->[1,1] 2->[1,1]<br>
     * This method processes the data as follows: <br>
     * 1. Reorganize the data by subpartition index: <br>
     * input 1: {0->[1,2,3],1->[1,2,3]}, <br>
     * input 2: {0->[1,1,1],1->[1,1,1]}<br>
     * 2. Split subpartitions with the same index into relatively balanced n parts (if possible) and
     * perform a Cartesian product operation to ensure data correctness: {0->[1,2][3],1->[1,2][3]},
     * {0->[1,1,1],1->[1,1,1]} --Cartesian product--> <br>
     * input 1: {0->[1,2],0->[3],1->[1,2],1->[3]}<br>
     * input 2: {0->[1,1,1],0->[1,1,1],1->[1,1,1],1->[1,1,1]}, i.e., each input has four balanced
     * subpartition slices.<br>
     * 3. Based on the above subpartition slices, calculate the subpartition slice range each task
     * needs to subscribe to, considering data volume and parallelism constraints:
     * [0,0],[1,1],[2,2],[3,3]<br>
     * 4. Convert the calculated subpartition slice range to the form of partition index range ->
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
     * @return the parallelism and vertex input infos
     */
    public Map<IntermediateDataSetID, JobVertexInputInfo> compute(
            JobVertexID jobVertexId,
            List<BlockingInputInfo> inputInfos,
            int parallelism,
            int minParallelism,
            int maxParallelism) {
        List<BlockingInputInfo> nonBroadcastInputInfos = getNonBroadcastInputInfos(inputInfos);
        List<BlockingInputInfo> broadcastInputInfos = getBroadcastInputInfos(inputInfos);
        if (nonBroadcastInputInfos.isEmpty()) {
            LOG.info(
                    "All inputs are broadcast for vertex {}, fallback to num based all to all.",
                    jobVertexId);
            return VertexInputInfoComputationUtils.computeVertexInputInfos(
                    parallelism, inputInfos, true);
        }

        Map<Integer, List<SubpartitionSlice>> subpartitionSlicesByTypeNumber =
                divideSubpartitionsEvenlyDistributeData(nonBroadcastInputInfos);

        int subpartitionSlicesSize =
                checkAdnGetSubpartitionSlicesSize(subpartitionSlicesByTypeNumber);
        int maxNumPartitions = getMaxNumPartitions(nonBroadcastInputInfos);
        int maxRangeSize = MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME / maxNumPartitions;

        // divide the data into balanced n parts
        List<IndexRange> subpartitionSliceRanges =
                computeSubpartitionSliceRanges(
                        dataVolumePerTask,
                        maxRangeSize,
                        subpartitionSlicesSize,
                        subpartitionSlicesByTypeNumber);

        // if the parallelism is not legal, try to adjust to a legal parallelism
        if (!isLegalParallelism(subpartitionSliceRanges.size(), minParallelism, maxParallelism)) {
            long minBytesSize = dataVolumePerTask;
            long sumBytesSize = 0;
            for (int i = 0; i < subpartitionSlicesSize; ++i) {
                long currentBytesSize = 0;
                for (List<SubpartitionSlice> subpartitionSlice :
                        subpartitionSlicesByTypeNumber.values()) {
                    currentBytesSize += subpartitionSlice.get(i).getDataBytes();
                }
                minBytesSize = Math.min(minBytesSize, currentBytesSize);
                sumBytesSize += currentBytesSize;
            }
            Optional<List<IndexRange>> adjustedSubpartitionRanges =
                    adjustToClosestLegalParallelism(
                            dataVolumePerTask,
                            subpartitionSliceRanges.size(),
                            minParallelism,
                            maxParallelism,
                            minBytesSize,
                            sumBytesSize,
                            limit ->
                                    computeParallelism(
                                            limit,
                                            maxRangeSize,
                                            subpartitionSlicesSize,
                                            subpartitionSlicesByTypeNumber),
                            limit ->
                                    computeSubpartitionSliceRanges(
                                            limit,
                                            maxRangeSize,
                                            subpartitionSlicesSize,
                                            subpartitionSlicesByTypeNumber));
            if (adjustedSubpartitionRanges.isEmpty()) {
                // can't find any legal parallelism, fall back to evenly distribute subpartitions
                LOG.info(
                        "Cannot find a legal parallelism to evenly distribute skewed data for job vertex {}. "
                                + "Fall back to compute a parallelism that can evenly distribute data.",
                        jobVertexId);
                return VertexInputInfoComputationUtils.computeVertexInputInfos(
                        parallelism, inputInfos, true);
            }
            subpartitionSliceRanges = adjustedSubpartitionRanges.get();
        }

        checkState(
                isLegalParallelism(subpartitionSliceRanges.size(), minParallelism, maxParallelism));

        return createVertexInputInfos(
                subpartitionSlicesByTypeNumber,
                nonBroadcastInputInfos,
                broadcastInputInfos,
                subpartitionSliceRanges);
    }

    private Map<Integer, List<SubpartitionSlice>> divideSubpartitionsEvenlyDistributeData(
            List<BlockingInputInfo> nonBroadcastInputInfos) {

        int subPartitionNum = checkAndGetSubpartitionNum(nonBroadcastInputInfos);

        // Aggregate input info with the same type number.
        Map<Integer, AggregatedBlockingInputInfo> aggregatedInputInfoByTypeNumber =
                computeAggregatedBlockingInputInfos(subPartitionNum, nonBroadcastInputInfos);

        Map<Integer, List<SubpartitionSlice>> subpartitionSliceGroupByTypeNumber = new HashMap<>();

        for (int subpartitionIndex = 0; subpartitionIndex < subPartitionNum; ++subpartitionIndex) {

            // Split the given subpartition group into balanced subpartition slices.
            Map<Integer, List<SubpartitionSlice>> subpartitionSlices =
                    computeSubpartitionSlices(subpartitionIndex, aggregatedInputInfoByTypeNumber);

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

    private Map<Integer, AggregatedBlockingInputInfo> computeAggregatedBlockingInputInfos(
            int subPartitionNum, List<BlockingInputInfo> nonBroadcastInputInfos) {

        Map<Integer, List<BlockingInputInfo>> inputsByTypeNumber =
                nonBroadcastInputInfos.stream()
                        .collect(Collectors.groupingBy(BlockingInputInfo::getInputTypeNumber));

        checkArgument(isLegalInputGroups(inputsByTypeNumber));

        Map<Integer, AggregatedBlockingInputInfo> blockingInputInfoContexts = new HashMap<>();
        for (Map.Entry<Integer, List<BlockingInputInfo>> entry : inputsByTypeNumber.entrySet()) {
            Integer typeNumber = entry.getKey();
            List<BlockingInputInfo> inputInfos = entry.getValue();
            blockingInputInfoContexts.put(
                    typeNumber,
                    AggregatedBlockingInputInfo.createAggregatedBlockingInputInfo(
                            defaultSkewedThreshold,
                            skewedFactor,
                            dataVolumePerTask,
                            subPartitionNum,
                            inputInfos));
        }

        return blockingInputInfoContexts;
    }

    private static Map<Integer, List<SubpartitionSlice>> computeSubpartitionSlices(
            int subpartitionIndex,
            Map<Integer, AggregatedBlockingInputInfo> aggregatedInputInfoByTypeNumber) {
        Map<Integer, List<SubpartitionSlice>> subpartitionSlices = new HashMap<>();
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
                        SubpartitionSlice.createSubpartitionSlices(
                                subpartitionIndex,
                                partitionRanges,
                                aggregatedBlockingInputInfo.getSubpartitionBytesByPartition()));
            } else {
                IndexRange partitionRange =
                        new IndexRange(0, aggregatedBlockingInputInfo.getMaxPartitionNum() - 1);
                subpartitionSlices.put(
                        typeNumber,
                        Collections.singletonList(
                                SubpartitionSlice.createSubpartitionSlice(
                                        subpartitionIndex,
                                        partitionRange,
                                        aggregatedBlockingInputInfo.getAggregatedSubpartitionBytes(
                                                subpartitionIndex))));
            }
        }
        return subpartitionSlices;
    }

    /**
     * Reassembling subpartition slices into balanced n parts and returning the range of index
     * corresponding to each piece of data. Reassembling need to meet the following conditions:<br>
     * 1. The data size of each piece does not exceed the limit.<br>
     * 2. The SubpartitionSlice numbers in each piece does not larger than maxRangeSize.
     *
     * @param limit the limit of data size
     * @param maxRangeSize the max number of SubpartitionSlice in each range
     * @param subpartitionGroupSize the number of SubpartitionSlices
     * @return the range of index corresponding to each piece of data
     */
    private static List<IndexRange> computeSubpartitionSliceRanges(
            long limit,
            int maxRangeSize,
            int subpartitionGroupSize,
            Map<Integer, List<SubpartitionSlice>> subpartitionSlices) {
        List<IndexRange> subpartitionSliceRanges = new ArrayList<>();
        long totalSize = 0;
        int startIndex = 0;
        Map<Integer, Set<SubpartitionSlice>> bucketsByTypeNumber = new HashMap<>();
        for (int i = 0; i < subpartitionGroupSize; ++i) {
            long currentSize = 0L;
            // bytes size after deduplication
            long distinctSize = 0L;
            for (Map.Entry<Integer, List<SubpartitionSlice>> entry :
                    subpartitionSlices.entrySet()) {
                Integer typeNumber = entry.getKey();
                SubpartitionSlice subpartitionSlice = entry.getValue().get(i);
                Set<SubpartitionSlice> bucket =
                        bucketsByTypeNumber.computeIfAbsent(typeNumber, ignored -> new HashSet<>());
                // When the bucket already contains duplicate subpartitionSlices, its size should be
                // ignored.
                if (!bucket.contains(subpartitionSlice)) {
                    distinctSize += subpartitionSlice.getDataBytes();
                }
                currentSize += subpartitionSlice.getDataBytes();
            }
            if (i == startIndex
                    || (totalSize + distinctSize <= limit
                            && (i - startIndex + 1) <= maxRangeSize)) {
                totalSize += distinctSize;
            } else {
                subpartitionSliceRanges.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                totalSize = currentSize;
                bucketsByTypeNumber.clear();
            }
            for (Map.Entry<Integer, List<SubpartitionSlice>> entry :
                    subpartitionSlices.entrySet()) {
                Integer typeNumber = entry.getKey();
                SubpartitionSlice subpartitionSlice = entry.getValue().get(i);
                bucketsByTypeNumber
                        .computeIfAbsent(typeNumber, ignored -> new HashSet<>())
                        .add(subpartitionSlice);
            }
        }
        subpartitionSliceRanges.add(new IndexRange(startIndex, subpartitionGroupSize - 1));
        return subpartitionSliceRanges;
    }

    /**
     * The difference from {@link #computeSubpartitionSliceRanges} is that the calculation here only
     * returns the parallelism after dividing base on the given limits.
     *
     * @param limit the limit of data size
     * @param maxRangeSize the max number of SubpartitionSlice in each range
     * @param subpartitionGroupSize the number of SubpartitionSlices
     * @return the parallelism after dividing
     */
    private static int computeParallelism(
            long limit,
            int maxRangeSize,
            int subpartitionGroupSize,
            Map<Integer, List<SubpartitionSlice>> subpartitionSlices) {
        int count = 1;
        long totalSize = 0;
        int startIndex = 0;
        Map<Integer, Set<SubpartitionSlice>> bucketsByTypeNumber = new HashMap<>();
        for (int i = 0; i < subpartitionGroupSize; ++i) {
            long currentSize = 0L;
            long distinctSize = 0L;
            for (Map.Entry<Integer, List<SubpartitionSlice>> entry :
                    subpartitionSlices.entrySet()) {
                Integer typeNumber = entry.getKey();
                SubpartitionSlice subpartitionSlice = entry.getValue().get(i);
                Set<SubpartitionSlice> bucket =
                        bucketsByTypeNumber.computeIfAbsent(typeNumber, ignored -> new HashSet<>());
                if (!bucket.contains(subpartitionSlice)) {
                    distinctSize += subpartitionSlice.getDataBytes();
                }
                currentSize += subpartitionSlice.getDataBytes();
            }
            if (i == startIndex
                    || (totalSize + distinctSize <= limit
                            && (i - startIndex + 1) <= maxRangeSize)) {
                totalSize += distinctSize;
            } else {
                ++count;
                startIndex = i;
                totalSize = currentSize;
                bucketsByTypeNumber.clear();
            }
            for (Map.Entry<Integer, List<SubpartitionSlice>> entry :
                    subpartitionSlices.entrySet()) {
                Integer typeNumber = entry.getKey();
                SubpartitionSlice subpartitionSlice = entry.getValue().get(i);
                bucketsByTypeNumber
                        .computeIfAbsent(typeNumber, ignored -> new HashSet<>())
                        .add(subpartitionSlice);
            }
        }
        return count;
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

    private static Map<IntermediateDataSetID, JobVertexInputInfo> createVertexInputInfos(
            Map<Integer, List<SubpartitionSlice>> subpartitionSlices,
            List<BlockingInputInfo> nonBroadcastInputInfos,
            List<BlockingInputInfo> broadcastInputInfos,
            List<IndexRange> subpartitionSliceRanges) {
        final Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        for (BlockingInputInfo inputInfo : nonBroadcastInputInfos) {
            List<ExecutionVertexInputInfo> executionVertexInputInfos =
                    createdExecutionVertexInputInfos(
                            inputInfo,
                            subpartitionSliceRanges,
                            subpartitionSlices.get(inputInfo.getInputTypeNumber()));

            vertexInputInfos.put(
                    inputInfo.getResultId(), new JobVertexInputInfo(executionVertexInputInfos));
        }

        for (BlockingInputInfo inputInfo : broadcastInputInfos) {
            List<ExecutionVertexInputInfo> executionVertexInputInfos =
                    createdExecutionVertexInputInfos(
                            inputInfo, subpartitionSliceRanges, Collections.emptyList());
            vertexInputInfos.put(
                    inputInfo.getResultId(), new JobVertexInputInfo(executionVertexInputInfos));
        }

        return vertexInputInfos;
    }

    private static List<ExecutionVertexInputInfo> createdExecutionVertexInputInfos(
            BlockingInputInfo inputInfo,
            List<IndexRange> subpartitionSliceRanges,
            List<SubpartitionSlice> subpartitionSlices) {
        int numPartitions = inputInfo.getNumPartitions();
        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < subpartitionSliceRanges.size(); ++i) {
            ExecutionVertexInputInfo executionVertexInputInfo;
            if (inputInfo.isBroadcast()) {
                if (inputInfo.isSingleSubpartitionContainsAllData()) {
                    executionVertexInputInfo =
                            new ExecutionVertexInputInfo(
                                    i, new IndexRange(0, numPartitions - 1), new IndexRange(0, 0));
                } else {
                    // The partitions of the all-to-all result have the same number of
                    // subpartitions. So we can use the first partition's subpartition
                    // number.
                    executionVertexInputInfo =
                            new ExecutionVertexInputInfo(
                                    i,
                                    new IndexRange(0, numPartitions - 1),
                                    new IndexRange(0, inputInfo.getNumSubpartitions(0) - 1));
                }
            } else {
                IndexRange subpartitionSliceRange = subpartitionSliceRanges.get(i);
                Map<IndexRange, IndexRange> consumedSubpartitionGroups =
                        computeConsumedSubpartitionGroups(
                                numPartitions, subpartitionSliceRange, subpartitionSlices);
                executionVertexInputInfo =
                        new ExecutionVertexInputInfo(i, consumedSubpartitionGroups);
            }
            executionVertexInputInfos.add(executionVertexInputInfo);
        }
        return executionVertexInputInfos;
    }

    /**
     * Merge the subpartition slices of the specified range into an index range map, which the key
     * is the partition index range and the value is the subpartition range.
     *
     * @param numPartitions the number of partitions of input info
     * @param subpartitionSliceRange the range of subpartition slices to be merged
     * @param subpartitionSlices subpartition slices
     * @return a map with no overlaps between its keys
     */
    private static Map<IndexRange, IndexRange> computeConsumedSubpartitionGroups(
            int numPartitions,
            IndexRange subpartitionSliceRange,
            List<SubpartitionSlice> subpartitionSlices) {
        Map<Integer, List<IndexRange>> subPartitionToPartitionRangeMap = new TreeMap<>();
        for (int i = subpartitionSliceRange.getStartIndex();
                i <= subpartitionSliceRange.getEndIndex();
                i++) {
            SubpartitionSlice subpartitionSlice = subpartitionSlices.get(i);
            int subPartitionIdx = subpartitionSlice.getSubpartitionIndex();

            Optional<IndexRange> optionalPartitionRange =
                    subpartitionSlice.getPartitionRange(numPartitions);
            if (optionalPartitionRange.isEmpty()) {
                continue;
            }
            IndexRange partitionRange = optionalPartitionRange.get();

            subPartitionToPartitionRangeMap
                    .computeIfAbsent(subPartitionIdx, k -> new ArrayList<>())
                    .add(partitionRange);
        }

        subPartitionToPartitionRangeMap =
                subPartitionToPartitionRangeMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry -> mergeIndexRanges(entry.getValue())));

        Map<IndexRange, List<IndexRange>> consumedPartitionToSubpartitionRangeMap = new HashMap<>();
        int startSubpartitionIdx =
                subpartitionSlices
                        .get(subpartitionSliceRange.getStartIndex())
                        .getSubpartitionIndex();
        int endSubpartitionIdx =
                subpartitionSlices.get(subpartitionSliceRange.getEndIndex()).getSubpartitionIndex();
        for (int i = startSubpartitionIdx; i <= endSubpartitionIdx; ++i) {
            for (IndexRange partitionRange : subPartitionToPartitionRangeMap.get(i)) {
                consumedPartitionToSubpartitionRangeMap
                        .computeIfAbsent(partitionRange, k -> new ArrayList<>())
                        .add(new IndexRange(i, i));
            }
        }

        return consumedPartitionToSubpartitionRangeMap.entrySet().stream()
                .collect(
                        Collectors.toMap(
                                Map.Entry::getKey,
                                entry -> {
                                    List<IndexRange> mergedRange =
                                            mergeIndexRanges(entry.getValue());
                                    checkState(mergedRange.size() == 1);
                                    return mergedRange.get(0);
                                }));
    }

    private static int checkAdnGetSubpartitionSlicesSize(
            Map<Integer, List<SubpartitionSlice>> subpartitionSlices) {
        Set<Integer> subpartitionSliceSizes =
                subpartitionSlices.values().stream().map(List::size).collect(Collectors.toSet());
        checkArgument(subpartitionSliceSizes.size() == 1);
        return subpartitionSliceSizes.iterator().next();
    }

    private static boolean isLegalInputGroups(Map<Integer, List<BlockingInputInfo>> inputGroups) {
        return inputGroups.values().stream()
                .allMatch(
                        inputs ->
                                inputs.stream()
                                                .map(
                                                        BlockingInputInfo
                                                                ::existIntraInputKeyCorrelation)
                                                .distinct()
                                                .count()
                                        == 1);
    }
}
