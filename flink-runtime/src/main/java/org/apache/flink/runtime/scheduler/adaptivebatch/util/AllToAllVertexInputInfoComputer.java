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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;

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
                    "All inputs are nonBroadcast for vertex {}, fallback to num based all to all.",
                    jobVertexId);
            return VertexInputInfoComputationUtils.computeVertexInputInfos(
                    parallelism, inputInfos, true);
        }

        Map<Integer, List<SubpartitionSlice>> subpartitionSlices =
                divideSubpartitionsEvenlyDistributeData(nonBroadcastInputInfos);

        int subpartitionSlicesSize = checkAdnGetSubpartitionSlicesSize(subpartitionSlices);
        int maxNumPartitions = getMaxNumPartitions(nonBroadcastInputInfos);
        int maxRangeSize = MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME / maxNumPartitions;

        // divide the data into balanced n parts
        List<IndexRange> subpartitionSliceRanges =
                computeSubpartitionSliceRanges(
                        dataVolumePerTask,
                        maxRangeSize,
                        subpartitionSlicesSize,
                        subpartitionSlices);

        // if the parallelism is not legal, try to adjust to a legal parallelism
        if (!isLegalParallelism(subpartitionSliceRanges.size(), minParallelism, maxParallelism)) {
            long minBytesSize = dataVolumePerTask;
            long sumBytesSize = 0;
            for (int i = 0; i < subpartitionSlicesSize; ++i) {
                long currentBytesSize = 0;
                for (List<SubpartitionSlice> subpartitionSlice : subpartitionSlices.values()) {
                    currentBytesSize += subpartitionSlice.get(i).getSize();
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
                                            subpartitionSlices),
                            limit ->
                                    computeSubpartitionSliceRanges(
                                            limit,
                                            maxRangeSize,
                                            subpartitionSlicesSize,
                                            subpartitionSlices));
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
                subpartitionSlices,
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
     * corresponding to each piece of data. Reassembling need to meet the following conditions:
     *
     * <p>1. The data size of each piece does not exceed the limit.
     *
     * <p>2. The SubpartitionSlice numbers in each piece does not larger than maxRangeSize.
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
                    distinctSize += subpartitionSlice.getSize();
                }
                currentSize += subpartitionSlice.getSize();
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
                    distinctSize += subpartitionSlice.getSize();
                }
                currentSize += subpartitionSlice.getSize();
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
                executionVertexInputInfo =
                        new ExecutionVertexInputInfo(
                                i, new IndexRange(0, numPartitions - 1), new IndexRange(0, 0));
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
        int startSubpartitionIdx =
                subpartitionSlices
                        .get(subpartitionSliceRange.getStartIndex())
                        .getSubpartitionIndex();
        int endSubpartitionIdx =
                subpartitionSlices.get(subpartitionSliceRange.getEndIndex()).getSubpartitionIndex();

        Map<Integer, Stack<IndexRange>> subPartitionToPartitionRangeMap = new TreeMap<>();
        for (SubpartitionSlice subpartitionSlice : subpartitionSlices) {
            int subPartitionIdx = subpartitionSlice.getSubpartitionIndex();
            Optional<IndexRange> optionalPartitionRange =
                    subpartitionSlice.getPartitionRange(numPartitions);
            if (optionalPartitionRange.isEmpty()) {
                continue;
            }
            IndexRange partitionRange = optionalPartitionRange.get();
            if (!subPartitionToPartitionRangeMap.containsKey(subPartitionIdx)) {
                Stack<IndexRange> partitionRanges = new Stack<>();
                partitionRanges.add(
                        new IndexRange(
                                partitionRange.getStartIndex(), partitionRange.getEndIndex()));
                subPartitionToPartitionRangeMap.put(subPartitionIdx, partitionRanges);
                continue;
            }
            Stack<IndexRange> partitionRanges =
                    subPartitionToPartitionRangeMap.get(subPartitionIdx);
            IndexRange oldPartitionRange = partitionRanges.pop();
            Optional<IndexRange> mergedPartitionRange =
                    mergeTowRange(oldPartitionRange, partitionRange);
            while (mergedPartitionRange.isPresent() && !partitionRanges.empty()) {
                partitionRange = mergedPartitionRange.get();
                oldPartitionRange = partitionRanges.pop();
                mergedPartitionRange = mergeTowRange(oldPartitionRange, partitionRange);
            }
            if (mergedPartitionRange.isPresent()) {
                partitionRanges.add(mergedPartitionRange.get());
            } else {
                partitionRanges.add(oldPartitionRange);
                partitionRanges.add(partitionRange);
            }
        }

        int startIdx = startSubpartitionIdx;
        Stack<IndexRange> preRangeStack = subPartitionToPartitionRangeMap.get(startIdx);
        Map<IndexRange, IndexRange> consumedSubpartitionGroups = new LinkedHashMap<>();
        for (int i = startSubpartitionIdx + 1; i <= endSubpartitionIdx; ++i) {
            Stack<IndexRange> partitionRangeStack = subPartitionToPartitionRangeMap.get(i);
            if (preRangeStack.equals(partitionRangeStack)) {
                continue;
            }
            checkArgument(preRangeStack.size() == 1);
            consumedSubpartitionGroups.put(preRangeStack.pop(), new IndexRange(startIdx, i - 1));
            preRangeStack = partitionRangeStack;
            startIdx = i;
        }
        while (!preRangeStack.isEmpty()) {
            consumedSubpartitionGroups.put(
                    preRangeStack.pop(), new IndexRange(startIdx, endSubpartitionIdx));
        }
        return remapSubpartitionGroups(consumedSubpartitionGroups);
    }

    /**
     * Remap a set of subpartition group to ensure that there are no overlaps between their
     * PartitionRanges.
     *
     * <p>For example, the original description: {<[0,3],[0,0]>, <[0,1],[1,1]>} will be remapped to:
     * {<[0,1],[0,1]>, <[2,3],[0,0]>}.
     *
     * @param subpartitionGroups the original subpartition groups
     * @return the remapped subpartition groups with no overlaps between their key
     */
    private static Map<IndexRange, IndexRange> remapSubpartitionGroups(
            Map<IndexRange, IndexRange> subpartitionGroups) {
        TreeSet<Integer> pointSet = new TreeSet<>();
        for (IndexRange partitionIndexRange : subpartitionGroups.keySet()) {
            pointSet.add(partitionIndexRange.getStartIndex());
            pointSet.add(partitionIndexRange.getEndIndex() + 1);
        }
        Map<IndexRange, IndexRange> reorganizedPartitionRange = new LinkedHashMap<>();
        Iterator<Integer> iterator = pointSet.iterator();
        int prev = iterator.next();
        while (iterator.hasNext()) {
            int curr = iterator.next() - 1;
            if (prev <= curr) {
                IndexRange newPartitionRange = new IndexRange(prev, curr);
                computeSubpartitionIndexRange(newPartitionRange, subpartitionGroups)
                        .ifPresent(
                                range -> reorganizedPartitionRange.put(newPartitionRange, range));
            }
            prev = curr + 1;
        }
        return reorganizedPartitionRange;
    }

    private static Optional<IndexRange> computeSubpartitionIndexRange(
            IndexRange partitionRange, Map<IndexRange, IndexRange> subpartitionGroups) {
        int subPartitionStartIndex = Integer.MAX_VALUE;
        int subPartitionEndIndex = Integer.MIN_VALUE;
        for (Map.Entry<IndexRange, IndexRange> entry : subpartitionGroups.entrySet()) {
            IndexRange oldPartitionRange = entry.getKey();
            IndexRange oldSubPartitionRange = entry.getValue();
            if (oldPartitionRange.getStartIndex() <= partitionRange.getStartIndex()
                    && oldPartitionRange.getEndIndex() >= partitionRange.getEndIndex()) {
                subPartitionStartIndex =
                        Math.min(oldSubPartitionRange.getStartIndex(), subPartitionStartIndex);
                subPartitionEndIndex =
                        Math.max(oldSubPartitionRange.getEndIndex(), subPartitionEndIndex);
            }
        }
        if (subPartitionStartIndex != Integer.MAX_VALUE
                && subPartitionEndIndex != Integer.MIN_VALUE) {
            return Optional.of(new IndexRange(subPartitionStartIndex, subPartitionEndIndex));
        }
        return Optional.empty();
    }

    private static Optional<IndexRange> mergeTowRange(IndexRange r1, IndexRange r2) {
        if (r1.getStartIndex() > r2.getStartIndex()) {
            IndexRange tmp = r1;
            r1 = r2;
            r2 = tmp;
        }
        if (r1.getEndIndex() + 1 >= r2.getStartIndex()) {
            return Optional.of(
                    new IndexRange(
                            r1.getStartIndex(), Math.max(r1.getEndIndex(), r2.getEndIndex())));
        }
        return Optional.empty();
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
