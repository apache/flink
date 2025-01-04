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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Function;

import static org.apache.flink.runtime.scheduler.adaptivebatch.DefaultVertexParallelismAndInputInfosDecider.MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.adjustToClosestLegalParallelism;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Helper class that computes VertexInputInfo for pointwise input. */
public class PointwiseVertexInputInfoComputer {

    private static final Logger LOG =
            LoggerFactory.getLogger(PointwiseVertexInputInfoComputer.class);

    private final long dataVolumePerTask;

    public PointwiseVertexInputInfoComputer(long dataVolumePerTask) {
        this.dataVolumePerTask = dataVolumePerTask;
    }

    /**
     * Decide parallelism and input infos, which will make the data be evenly distributed to
     * downstream subtasks for POINTWISE, such that different downstream subtasks consume roughly
     * the same amount of data.
     *
     * <p>Assume that `inputInfo` has two partitions, each partition has three subpartitions:
     * {0->[1,2,1], 1->[2,1,2]}, and the expected parallelism is 3. The calculation process is as
     * follows: <br>
     * 1. Merge the subpartition bytes arrays into an 1D array: [1,2,1,2,1,2] <br>
     * 2. Divide the subpartition bytes array into n balanced parts (described by `IndexRange`,
     * named SubpartitionSliceRanges) based on data volume: [0,1],[2,3],[4,5] <br>
     * 3. Reorganize the divided results into a mapping of partition range to subpartition range: {0
     * -> [0,1]}, {0->[2,2],1->[0,0]}, {1->[1,2]}. <br>
     * The final result is the `SubpartitionGroup` that each of the three parallel tasks need to
     * subscribe.
     *
     * @param inputInfo The information of consumed blocking results
     * @param parallelism The parallelism of the job vertex
     * @return the vertex input info
     */
    public JobVertexInputInfo compute(BlockingInputInfo inputInfo, int parallelism) {
        // If the input has intra-input correlation, fall back to the default calculation method to
        // avoid breaking the data distribution.
        if (inputInfo.existIntraInputKeyCorrelation()) {
            return VertexInputInfoComputationUtils.computeVertexInputInfoForPointwise(
                    inputInfo.getNumPartitions(),
                    parallelism,
                    inputInfo::getNumSubpartitions,
                    true);
        }

        int totalNumSubPartitions = 0;
        for (int i = 0; i < inputInfo.getNumPartitions(); i++) {
            totalNumSubPartitions += inputInfo.getNumSubpartitions(i);
        }
        // Node: SubpartitionSliceRanges does not represent the real index of the subpartitions, but
        // the location of that subpartition in all subpartitions, as we aggregate all subpartitions
        // into a one-digit array to calculate.
        Optional<List<IndexRange>> optionalSubpartitionSliceRanges =
                computeSubpartitionSliceRangesForInputNotExistIntraCorrelation(
                        inputInfo.getNumPartitions(),
                        totalNumSubPartitions,
                        inputInfo.getSubpartitionBytesByPartitionIndex(),
                        parallelism);

        if (optionalSubpartitionSliceRanges.isEmpty()) {
            LOG.info(
                    "Filed to decide parallelism in balanced way for input {}, fallback to computePartitionOrSubpartitionRangesEvenlySum",
                    inputInfo.getResultId());
            return VertexInputInfoComputationUtils.computeVertexInputInfoForPointwise(
                    inputInfo.getNumPartitions(),
                    parallelism,
                    inputInfo::getNumSubpartitions,
                    true);
        }
        List<IndexRange> subpartitionSliceRanges = optionalSubpartitionSliceRanges.get();
        // Calculate the partition index and subpartition index of each subpartition in
        // subpartitionSlice.
        Map<Integer, Tuple2<Integer, Integer>> subpartitionToPartitionInfoMap =
                computeSubpartitionToPartitionInfoMap(
                        subpartitionSliceRanges, inputInfo::getNumSubpartitions);

        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < subpartitionSliceRanges.size(); ++i) {
            executionVertexInputInfos.add(
                    new ExecutionVertexInputInfo(
                            i,
                            computeConsumedSubpartitionGroups(
                                    subpartitionSliceRanges.get(i),
                                    subpartitionToPartitionInfoMap)));
        }
        return new JobVertexInputInfo(executionVertexInputInfos);
    }

    /**
     * Computes the subpartition slice ranges for input data that does not have intra-input key
     * correlation.
     *
     * <p>Note: The returned SubpartitionSliceRanges does not represent the real index of the
     * subpartitions, but the location of that subpartition in dataBytes, which aggregates all
     * subpartitions into a one-digit array for calculation.
     *
     * @param numPartitions The number of partitions in the input data.
     * @param totalNumSubPartitions The total number of subpartitions across all partitions.
     * @param subpartitionBytesByPartitionIndex A map from partition index to an array of byte sizes
     *     for each subpartition within that partition.
     * @param parallelism The target parallelism for distributing the subpartitions.
     * @return An optional list of {@link IndexRange} objects representing the ranges of
     *     subpartitions assigned to each task.
     */
    private Optional<List<IndexRange>>
            computeSubpartitionSliceRangesForInputNotExistIntraCorrelation(
                    int numPartitions,
                    int totalNumSubPartitions,
                    Map<Integer, long[]> subpartitionBytesByPartitionIndex,
                    int parallelism) {
        // map subpartitionBytesByPartitionIndex from 2D to 1D
        long[] dataBytes = new long[totalNumSubPartitions];
        long sum = 0L;
        long min = Integer.MAX_VALUE;
        int i = 0;
        for (int j = 0; j < numPartitions; ++j) {
            long[] subpartitionBytes = subpartitionBytesByPartitionIndex.get(j);
            for (long subpartitionByte : subpartitionBytes) {
                dataBytes[i++] = subpartitionByte;
                sum += subpartitionByte;
                min = Math.min(subpartitionByte, min);
            }
        }
        checkState(i == totalNumSubPartitions);
        // compute the amount of data for each subtask base on the parallelism limit
        long bytesLimit =
                computeBytesLimit(
                        dataBytes, sum, min, parallelism, MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME);

        List<IndexRange> subpartitionSliceRanges =
                computeSubpartitionSliceRangeEvenlyData(
                        dataBytes, bytesLimit, MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME);

        if (subpartitionSliceRanges.size() != parallelism) {
            LOG.info(
                    "The parallelism {} is not equal to the expected parallelism {}, fallback to computePartitionOrSubpartitionRangesEvenlySum",
                    subpartitionSliceRanges.size(),
                    parallelism);
            subpartitionSliceRanges =
                    computeSubpartitionSliceRangesEvenlySum(dataBytes.length, parallelism);
        }

        if (subpartitionSliceRanges.size() != parallelism) {
            return adjustToClosestLegalParallelism(
                    dataVolumePerTask,
                    subpartitionSliceRanges.size(),
                    parallelism,
                    parallelism,
                    min,
                    sum,
                    limit ->
                            computeParallelism(
                                    dataBytes, limit, MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME),
                    limit ->
                            computeSubpartitionSliceRangeEvenlyData(
                                    dataBytes, limit, MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME));
        }

        return Optional.of(subpartitionSliceRanges);
    }

    /**
     * Computes the maximum byte limit for each task to ensure that data is evenly distributed
     * across tasks, given a target parallelism and the maximum range size for parallel tasks.
     *
     * <p>This method uses binary search to find an appropriate byte limit value. Specifically: <br>
     * - Initialize the left and right boundaries to the minimum value and the total sum,
     * respectively. <br>
     * - In each iteration, calculate the middle value and call {@link #computeParallelism(long[],
     * long, int)} to determine the parallelism for the current middle value. <br>
     * - If the computed parallelism exceeds the target parallelism, update the left boundary;
     * otherwise, update the right boundary. <br>
     * - Finally, return the smallest byte limit value that satisfies the conditions.
     *
     * @param dataBytes Array representing the data volume in bytes.
     * @param sum Total data volume.
     * @param min Minimum byte limit.
     * @param parallelism Target parallelism.
     * @param maxRangeSize Maximum range size for parallel tasks.
     * @return The smallest byte limit value that meets the conditions.
     */
    private long computeBytesLimit(
            long[] dataBytes, long sum, long min, int parallelism, int maxRangeSize) {
        long left = min;
        long right = sum;
        while (left < right) {
            long mid = left + (right - left) / 2;
            int count = computeParallelism(dataBytes, mid, maxRangeSize);
            if (count > parallelism) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    /**
     * Computes the parallelism required to process data given byte and range size limits.
     *
     * @param dataBytes Array representing the data volume in bytes.
     * @param limit The maximum number of bytes each task can process.
     * @param maxRangeSize The maximum number of subpartitions each task can handle.
     * @return An integer representing the computed parallelism.
     */
    private static int computeParallelism(long[] dataBytes, long limit, int maxRangeSize) {
        long tmpSum = 0;
        int startIndex = 0;
        int count = 1;
        for (int i = 0; i < dataBytes.length; ++i) {
            long num = dataBytes[i];
            if (i == startIndex
                    || (tmpSum + num <= limit && (i - startIndex + 1) <= maxRangeSize)) {
                tmpSum += num;
            } else {
                startIndex = i;
                tmpSum = num;
                count += 1;
            }
        }
        return count;
    }

    /**
     * Computes the subpartition slice ranges for tasks given byte and range size limits.
     *
     * @param dataBytes Array representing the data volume in bytes.
     * @param limit The maximum number of bytes each task can process.
     * @param maxRangeSize The maximum number of subpartitions each task can handle.
     * @return A list of {@link IndexRange} objects, where each object represents the range of
     *     subpartitions in dataBytes array.
     */
    private static List<IndexRange> computeSubpartitionSliceRangeEvenlyData(
            long[] dataBytes, long limit, int maxRangeSize) {
        List<IndexRange> ranges = new ArrayList<>();
        long tmpSum = 0;
        int startIndex = 0;
        for (int i = 0; i < dataBytes.length; ++i) {
            long num = dataBytes[i];
            if (i == startIndex
                    || (tmpSum + num <= limit && (i - startIndex + 1) <= maxRangeSize)) {
                tmpSum += num;
            } else {
                ranges.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                tmpSum = num;
            }
        }
        ranges.add(new IndexRange(startIndex, dataBytes.length - 1));
        return ranges;
    }

    /**
     * Computes the subpartition slice ranges for evenly distributing subpartition nums across
     * tasks.
     *
     * @param totalSubpartitions The total number of subpartitions to be distributed.
     * @param parallelism The number of tasks across which the subpartitions should be distributed.
     * @return A list of {@link IndexRange} objects, where each object represents the range of
     *     subpartitions in totalSubpartitions.
     */
    private static List<IndexRange> computeSubpartitionSliceRangesEvenlySum(
            int totalSubpartitions, int parallelism) {
        List<IndexRange> ranges = new ArrayList<>();
        int baseSize = totalSubpartitions / parallelism;
        int remainder = totalSubpartitions % parallelism;
        int start = 0;
        for (int i = 0; i < parallelism; i++) {
            int end = start + baseSize - 1;
            if (i < remainder) {
                end += 1;
            }
            ranges.add(new IndexRange(start, end));
            start = end + 1;
        }
        checkArgument(start == totalSubpartitions);
        return ranges;
    }

    /**
     * Computes a mapping from each subpartition slice index to its corresponding partition index
     * and real subpartition index.
     *
     * @param subpartitionSliceRange A list of {@link IndexRange} objects representing the ranges of
     *     subpartition slice to be mapped.
     * @param numOfSubpartitionsRetriever A function that takes a partition index and returns the
     *     number of subpartitions in that partition.
     * @return A map where each key is a subpartition slice index, and each value is a tuple
     *     containing the partition index and real subpartition index.
     */
    private static Map<Integer, Tuple2<Integer, Integer>> computeSubpartitionToPartitionInfoMap(
            List<IndexRange> subpartitionSliceRange,
            Function<Integer, Integer> numOfSubpartitionsRetriever) {
        Map<Integer, Tuple2<Integer, Integer>> subpartitionToPartitionIndexMap = new HashMap<>();
        int currentSubpartitionsCount = numOfSubpartitionsRetriever.apply(0);
        int currentPartitionIndex = 0;
        int currentSubpartitionIndex = 0;
        for (IndexRange range : subpartitionSliceRange) {
            for (int i = range.getStartIndex(); i <= range.getEndIndex(); ++i) {
                if (currentSubpartitionIndex >= currentSubpartitionsCount) {
                    ++currentPartitionIndex;
                    currentSubpartitionIndex = 0;
                    currentSubpartitionsCount =
                            numOfSubpartitionsRetriever.apply(currentPartitionIndex);
                }
                subpartitionToPartitionIndexMap.put(
                        i, new Tuple2<>(currentPartitionIndex, currentSubpartitionIndex));
                ++currentSubpartitionIndex;
            }
        }
        return subpartitionToPartitionIndexMap;
    }

    /**
     * Maps the subpartitions in the given subpartition slice range to the mapping of PartitionRange
     * to Subpartition. The result represents the subpartition groups that a single task needs to
     * subscribe to.
     *
     * @param subpartitionSliceRange An {@link IndexRange} representing the slice of subpartitions
     *     to be mapped.
     * @param subpartitionToPartitionInfoMap A map from subpartition slice index to a tuple
     *     containing the partition index and subpartition index.
     * @return A map where each key is an {@link IndexRange} representing a partition range, and
     *     each value is an {@link IndexRange} representing the corresponding subpartition range.
     */
    private static Map<IndexRange, IndexRange> computeConsumedSubpartitionGroups(
            IndexRange subpartitionSliceRange,
            Map<Integer, Tuple2<Integer, Integer>> subpartitionToPartitionInfoMap) {
        // Map subpartition slice range to partitionIdxToSubpartitionRangeMap.
        Map<Integer, IndexRange> partitionIdxToSubpartitionRangeMap = new TreeMap<>();
        int prePartitionIdx =
                subpartitionToPartitionInfoMap.get(subpartitionSliceRange.getStartIndex()).f0;
        int startSubpartitionIdx =
                subpartitionToPartitionInfoMap.get(subpartitionSliceRange.getStartIndex()).f1;
        int endSubpartitionIdx = startSubpartitionIdx;
        for (int i = subpartitionSliceRange.getStartIndex() + 1;
                i <= subpartitionSliceRange.getEndIndex();
                ++i) {
            int partitionIdx =
                    subpartitionToPartitionInfoMap.get(subpartitionSliceRange.getStartIndex()).f0;
            if (partitionIdx == prePartitionIdx) {
                ++endSubpartitionIdx;
            } else {
                checkState(!partitionIdxToSubpartitionRangeMap.containsKey(prePartitionIdx));
                partitionIdxToSubpartitionRangeMap.put(
                        prePartitionIdx, new IndexRange(startSubpartitionIdx, endSubpartitionIdx));
                prePartitionIdx = partitionIdx;
                startSubpartitionIdx = 0;
                endSubpartitionIdx = startSubpartitionIdx;
            }
        }
        checkState(!partitionIdxToSubpartitionRangeMap.containsKey(prePartitionIdx));
        partitionIdxToSubpartitionRangeMap.put(
                prePartitionIdx, new IndexRange(startSubpartitionIdx, endSubpartitionIdx));

        // Merge contiguous partition indexes with the same subpartition range.
        Map<IndexRange, IndexRange> consumedSubpartitionGroups = new HashMap<>();
        Iterator<Integer> partitionIndexIterator =
                partitionIdxToSubpartitionRangeMap.keySet().iterator();
        int startPartitionIdx = partitionIndexIterator.next();
        int endPartitionIdx = startPartitionIdx;
        IndexRange preSubpartitionRange = partitionIdxToSubpartitionRangeMap.get(startPartitionIdx);
        while (partitionIndexIterator.hasNext()) {
            int currentPartitionIndex = partitionIndexIterator.next();
            IndexRange subPartitionRange =
                    partitionIdxToSubpartitionRangeMap.get(currentPartitionIndex);
            if (subPartitionRange.equals(preSubpartitionRange)) {
                checkState(endPartitionIdx + 1 == currentPartitionIndex);
                endPartitionIdx += 1;
            } else {
                consumedSubpartitionGroups.put(
                        new IndexRange(startPartitionIdx, endPartitionIdx), preSubpartitionRange);
                preSubpartitionRange = subPartitionRange;
                startPartitionIdx = currentPartitionIndex;
                endPartitionIdx = startPartitionIdx;
            }
        }
        consumedSubpartitionGroups.put(
                new IndexRange(startPartitionIdx, endPartitionIdx), preSubpartitionRange);
        return consumedSubpartitionGroups;
    }
}
