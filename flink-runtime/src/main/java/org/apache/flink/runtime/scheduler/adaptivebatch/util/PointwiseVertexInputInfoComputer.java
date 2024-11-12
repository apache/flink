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
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.runtime.scheduler.adaptivebatch.DefaultVertexParallelismAndInputInfosDecider.MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.adjustToClosestLegalParallelism;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetSubpartitionNum;
import static org.apache.flink.util.Preconditions.checkArgument;

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
     * @param inputInfo The information of consumed blocking results
     * @param parallelism The parallelism of the job vertex
     * @return the vertex input info
     */
    public JobVertexInputInfo compute(BlockingInputInfo inputInfo, Integer parallelism) {

        Map<Integer, long[]> subpartitionBytesByPartitionIndex =
                inputInfo.getSubpartitionBytesByPartitionIndex();
        int numPartitions = inputInfo.getNumPartitions();
        int numSubPartitions = checkAndGetSubpartitionNum(Collections.singletonList(inputInfo));
        // compute the size of each subpartition
        long[] nums = new long[numPartitions * numSubPartitions];
        long sum = 0L;
        long min = Integer.MAX_VALUE;
        for (int i = 0; i < numPartitions; ++i) {
            long[] subpartitionBytes = subpartitionBytesByPartitionIndex.get(i);
            for (int j = 0; j < numSubPartitions; ++j) {
                int k = i * numSubPartitions + j;
                nums[k] = subpartitionBytes[j];
                sum += nums[k];
                min = Math.min(nums[k], min);
            }
        }

        // compute the amount of data for each subtask base on the parallelism limit
        long bytesLimit =
                computeBytesLimit(
                        nums, sum, min, parallelism, MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME);

        List<IndexRange> subpartitionRanges =
                computeSubpartitionRangesEvenlyData(
                        nums, bytesLimit, MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME);

        if (subpartitionRanges.size() != parallelism) {
            LOG.info(
                    "The parallelism {} is not equal to the expected parallelism {}, fallback to computePartitionOrSubpartitionRangesEvenlySum",
                    subpartitionRanges.size(),
                    parallelism);
            subpartitionRanges = computeSubpartitionRangesEvenlySum(nums.length, parallelism);
        }

        if (subpartitionRanges.size() != parallelism) {
            Optional<List<IndexRange>> adjustedSubpartitionRanges =
                    adjustToClosestLegalParallelism(
                            dataVolumePerTask,
                            subpartitionRanges.size(),
                            parallelism,
                            parallelism,
                            min,
                            sum,
                            limit ->
                                    computeParallelism(
                                            nums, limit, MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME),
                            limit ->
                                    computeSubpartitionRangesEvenlyData(
                                            nums, limit, MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME));
            if (adjustedSubpartitionRanges.isEmpty()) {
                LOG.info(
                        "The parallelism {} is not equal to the expected parallelism {}, fallback to computeVertexInputInfoForPointwise",
                        subpartitionRanges.size(),
                        parallelism);
                return VertexInputInfoComputationUtils.computeVertexInputInfoForPointwise(
                        numPartitions, parallelism, inputInfo::getNumSubpartitions, true);
            }
            subpartitionRanges = adjustedSubpartitionRanges.get();
        }

        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < subpartitionRanges.size(); ++i) {
            ExecutionVertexInputInfo executionVertexInputInfo;
            if (inputInfo.isBroadcast()) {
                executionVertexInputInfo =
                        new ExecutionVertexInputInfo(
                                i, new IndexRange(0, numPartitions - 1), new IndexRange(0, 0));
            } else {
                Map<IndexRange, IndexRange> consumedSubpartitionGroups =
                        computeConsumedSubpartitionGroups(
                                subpartitionRanges.get(i), numSubPartitions);
                executionVertexInputInfo =
                        new ExecutionVertexInputInfo(i, consumedSubpartitionGroups);
            }
            executionVertexInputInfos.add(executionVertexInputInfo);
        }
        return new JobVertexInputInfo(executionVertexInputInfos);
    }

    private long computeBytesLimit(
            long[] nums, long sum, long min, int parallelism, int maxRangeSize) {
        long left = min;
        long right = sum;
        while (left < right) {
            long mid = left + (right - left) / 2;
            int count = computeParallelism(nums, mid, maxRangeSize);
            if (count > parallelism) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    private static List<IndexRange> computeSubpartitionRangesEvenlyData(
            long[] nums, long limit, int maxRangeSize) {
        List<IndexRange> ranges = new ArrayList<>();
        long tmpSum = 0;
        int startIndex = 0;
        for (int i = 0; i < nums.length; ++i) {
            long num = nums[i];
            if (i == startIndex
                    || (tmpSum + num <= limit && (i - startIndex + 1) <= maxRangeSize)) {
                tmpSum += num;
            } else {
                ranges.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                tmpSum = num;
            }
        }
        ranges.add(new IndexRange(startIndex, nums.length - 1));
        return ranges;
    }

    private static int computeParallelism(long[] nums, long limit, int maxRangeSize) {
        long tmpSum = 0;
        int startIndex = 0;
        int count = 1;
        for (int i = 0; i < nums.length; ++i) {
            long num = nums[i];
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

    private static List<IndexRange> computeSubpartitionRangesEvenlySum(
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

    private static Map<IndexRange, IndexRange> computeConsumedSubpartitionGroups(
            IndexRange subpartitionRange, int numSubPartitions) {
        List<IndexRange> subPartitionRangeList = new ArrayList<>();
        int prePartitionIdx = subpartitionRange.getStartIndex() / numSubPartitions;
        int start = subpartitionRange.getStartIndex() % numSubPartitions;
        int end = start;
        for (int i = subpartitionRange.getStartIndex() + 1;
                i <= subpartitionRange.getEndIndex();
                ++i) {
            int partitionIdx = i / numSubPartitions;
            if (partitionIdx == prePartitionIdx) {
                ++end;
            } else {
                subPartitionRangeList.add(new IndexRange(start, end));
                prePartitionIdx = partitionIdx;
                start = 0;
                end = start;
            }
        }
        subPartitionRangeList.add(new IndexRange(start, end));
        Map<IndexRange, IndexRange> consumedSubpartitionGroups = new LinkedHashMap<>();
        int startPartitionIdx = subpartitionRange.getStartIndex() / numSubPartitions;
        int endPartitionIdx = startPartitionIdx;
        IndexRange preSubpartitionRange = subPartitionRangeList.get(0);
        for (int i = 1; i < subPartitionRangeList.size(); ++i) {
            IndexRange subPartitionRange = subPartitionRangeList.get(i);
            if (subPartitionRange.equals(preSubpartitionRange)) {
                ++endPartitionIdx;
            } else {
                consumedSubpartitionGroups.put(
                        new IndexRange(startPartitionIdx, endPartitionIdx), preSubpartitionRange);
                preSubpartitionRange = subPartitionRange;
                startPartitionIdx = endPartitionIdx + 1;
                endPartitionIdx = startPartitionIdx;
            }
        }
        consumedSubpartitionGroups.put(
                new IndexRange(startPartitionIdx, endPartitionIdx), preSubpartitionRange);
        return consumedSubpartitionGroups;
    }
}
