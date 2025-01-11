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
import org.apache.flink.runtime.scheduler.adaptivebatch.BisectionSearchUtils;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.apache.flink.runtime.executiongraph.IndexRangeUtil.mergeIndexRanges;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Utils class for VertexParallelismAndInputInfosDecider. */
public class VertexParallelismAndInputInfosDeciderUtils {
    private static final Logger LOG =
            LoggerFactory.getLogger(VertexParallelismAndInputInfosDeciderUtils.class);

    /**
     * Adjust the parallelism to the closest legal parallelism and return the computed subpartition
     * ranges.
     *
     * @param currentDataVolumeLimit current data volume limit
     * @param currentParallelism current parallelism
     * @param minParallelism the min parallelism
     * @param maxParallelism the max parallelism
     * @param minLimit the minimum data volume limit
     * @param maxLimit the maximum data volume limit
     * @param parallelismComputer a function to compute the parallelism according to the data volume
     *     limit
     * @param subpartitionRangesComputer a function to compute the subpartition ranges according to
     *     the data volume limit
     * @return the computed subpartition ranges or {@link Optional#empty()} if we can't find any
     *     legal parallelism
     */
    public static Optional<List<IndexRange>> adjustToClosestLegalParallelism(
            long currentDataVolumeLimit,
            int currentParallelism,
            int minParallelism,
            int maxParallelism,
            long minLimit,
            long maxLimit,
            Function<Long, Integer> parallelismComputer,
            Function<Long, List<IndexRange>> subpartitionRangesComputer) {
        long adjustedDataVolumeLimit = currentDataVolumeLimit;
        if (currentParallelism < minParallelism) {
            // Current parallelism is smaller than the user-specified lower-limit of parallelism ,
            // we need to adjust it to the closest/minimum possible legal parallelism. That is, we
            // need to find the maximum legal dataVolumeLimit.
            adjustedDataVolumeLimit =
                    BisectionSearchUtils.findMaxLegalValue(
                            value -> parallelismComputer.apply(value) >= minParallelism,
                            minLimit,
                            currentDataVolumeLimit);

            // When we find the minimum possible legal parallelism, the dataVolumeLimit that can
            // lead to this parallelism may be a range, and we need to find the minimum value of
            // this range to make the data distribution as even as possible (the smaller the
            // dataVolumeLimit, the more even the distribution)
            final long minPossibleLegalParallelism =
                    parallelismComputer.apply(adjustedDataVolumeLimit);
            adjustedDataVolumeLimit =
                    BisectionSearchUtils.findMinLegalValue(
                            value ->
                                    parallelismComputer.apply(value) == minPossibleLegalParallelism,
                            minLimit,
                            adjustedDataVolumeLimit);

        } else if (currentParallelism > maxParallelism) {
            // Current parallelism is larger than the user-specified upper-limit of parallelism ,
            // we need to adjust it to the closest/maximum possible legal parallelism. That is, we
            // need to find the minimum legal dataVolumeLimit.
            adjustedDataVolumeLimit =
                    BisectionSearchUtils.findMinLegalValue(
                            value -> parallelismComputer.apply(value) <= maxParallelism,
                            currentDataVolumeLimit,
                            maxLimit);
        }

        int adjustedParallelism = parallelismComputer.apply(adjustedDataVolumeLimit);
        if (isLegalParallelism(adjustedParallelism, minParallelism, maxParallelism)) {
            return Optional.of(subpartitionRangesComputer.apply(adjustedDataVolumeLimit));
        } else {
            return Optional.empty();
        }
    }

    /**
     * Computes the Cartesian product of a list of lists.
     *
     * <p>The Cartesian product is a set of all possible combinations formed by picking one element
     * from each list. For example, given input lists [[1, 2], [3, 4]], the result will be [[1, 3],
     * [1, 4], [2, 3], [2, 4]].
     *
     * <p>Note: If the input list is empty or contains an empty list, the result will be an empty
     * list.
     *
     * @param <T> the type of elements in the lists
     * @param lists a list of lists for which the Cartesian product is to be computed
     * @return a list of lists representing the Cartesian product, where each inner list is a
     *     combination
     */
    public static <T> List<List<T>> cartesianProduct(List<List<T>> lists) {
        List<List<T>> resultLists = new ArrayList<>();
        if (lists.isEmpty()) {
            resultLists.add(new ArrayList<>());
            return resultLists;
        } else {
            List<T> firstList = lists.get(0);
            List<List<T>> remainingLists = cartesianProduct(lists.subList(1, lists.size()));
            for (T condition : firstList) {
                for (List<T> remainingList : remainingLists) {
                    ArrayList<T> resultList = new ArrayList<>();
                    resultList.add(condition);
                    resultList.addAll(remainingList);
                    resultLists.add(resultList);
                }
            }
        }
        return resultLists;
    }

    /**
     * Calculates the median of a given array of long integers. If the calculated median is less
     * than 1, it returns 1 instead.
     *
     * @param nums an array of long integers for which to calculate the median.
     * @return the median value, which will be at least 1.
     */
    public static long median(long[] nums) {
        int len = nums.length;
        long[] sortedNums = LongStream.of(nums).sorted().toArray();
        if (len % 2 == 0) {
            return Math.max((sortedNums[len / 2] + sortedNums[len / 2 - 1]) / 2, 1L);
        } else {
            return Math.max(sortedNums[len / 2], 1L);
        }
    }

    /**
     * Computes the skew threshold based on the given media size and skewed factor.
     *
     * <p>The skew threshold is calculated as the product of the media size and the skewed factor.
     * To ensure that the computed threshold does not fall below a specified default value, the
     * method uses {@link Math#max} to return the largest of the calculated threshold and the
     * default threshold.
     *
     * @param medianSize the size of the median
     * @param skewedFactor a factor indicating the degree of skewness
     * @param defaultSkewedThreshold the default threshold to be used if the calculated threshold is
     *     less than this value
     * @return the computed skew threshold, which is guaranteed to be at least the default skewed
     *     threshold.
     */
    public static long computeSkewThreshold(
            long medianSize, double skewedFactor, long defaultSkewedThreshold) {
        return (long) Math.max(medianSize * skewedFactor, defaultSkewedThreshold);
    }

    /**
     * Computes the target data size for each task based on the sizes of non-skewed subpartitions.
     *
     * <p>The target size is determined as the average size of non-skewed subpartitions and ensures
     * that the target size is at least equal to the specified data volume per task.
     *
     * @param subpartitionBytes an array representing the data size of each subpartition
     * @param skewedThreshold skewed threshold in bytes
     * @param dataVolumePerTask the amount of data that should be allocated per task
     * @return the computed target size for each task, which is the maximum between the average size
     *     of non-skewed subpartitions and data volume per task.
     */
    public static long computeTargetSize(
            long[] subpartitionBytes, long skewedThreshold, long dataVolumePerTask) {
        long[] nonSkewPartitions =
                LongStream.of(subpartitionBytes).filter(v -> v <= skewedThreshold).toArray();
        if (nonSkewPartitions.length == 0) {
            return dataVolumePerTask;
        } else {
            return Math.max(
                    dataVolumePerTask,
                    LongStream.of(nonSkewPartitions).sum() / nonSkewPartitions.length);
        }
    }

    public static List<BlockingInputInfo> getNonBroadcastInputInfos(
            List<BlockingInputInfo> consumedResults) {
        return consumedResults.stream()
                .filter(resultInfo -> !resultInfo.isBroadcast())
                .collect(Collectors.toList());
    }

    public static boolean hasSameNumPartitions(List<BlockingInputInfo> inputInfos) {
        Set<Integer> partitionNums =
                inputInfos.stream()
                        .map(BlockingInputInfo::getNumPartitions)
                        .collect(Collectors.toSet());
        return partitionNums.size() == 1;
    }

    public static int getMaxNumPartitions(List<BlockingInputInfo> consumedResults) {
        checkArgument(!consumedResults.isEmpty());
        return consumedResults.stream()
                .mapToInt(BlockingInputInfo::getNumPartitions)
                .max()
                .getAsInt();
    }

    public static int checkAndGetSubpartitionNum(List<BlockingInputInfo> consumedResults) {
        final Set<Integer> subpartitionNumSet =
                consumedResults.stream()
                        .flatMap(
                                resultInfo ->
                                        IntStream.range(0, resultInfo.getNumPartitions())
                                                .boxed()
                                                .map(resultInfo::getNumSubpartitions))
                        .collect(Collectors.toSet());
        // all partitions have the same subpartition num
        checkState(subpartitionNumSet.size() == 1);
        return subpartitionNumSet.iterator().next();
    }

    public static int checkAndGetSubpartitionNumForAggregatedInputs(
            Collection<AggregatedBlockingInputInfo> inputInfos) {
        final Set<Integer> subpartitionNumSet =
                inputInfos.stream()
                        .map(AggregatedBlockingInputInfo::getNumSubpartitions)
                        .collect(Collectors.toSet());
        // all partitions have the same subpartition num
        checkState(subpartitionNumSet.size() == 1);
        return subpartitionNumSet.iterator().next();
    }

    public static boolean isLegalParallelism(
            int parallelism, int minParallelism, int maxParallelism) {
        return parallelism >= minParallelism && parallelism <= maxParallelism;
    }

    public static boolean checkAndGetIntraCorrelation(List<BlockingInputInfo> inputInfos) {
        Set<Boolean> intraCorrelationSet =
                inputInfos.stream()
                        .map(BlockingInputInfo::isIntraInputKeyCorrelated)
                        .collect(Collectors.toSet());
        checkArgument(intraCorrelationSet.size() == 1);
        return intraCorrelationSet.iterator().next();
    }

    public static int checkAndGetParallelism(Collection<JobVertexInputInfo> vertexInputInfos) {
        final Set<Integer> parallelismSet =
                vertexInputInfos.stream()
                        .map(
                                vertexInputInfo ->
                                        vertexInputInfo.getExecutionVertexInputInfos().size())
                        .collect(Collectors.toSet());
        checkState(parallelismSet.size() == 1);
        return parallelismSet.iterator().next();
    }

    /**
     * Attempts to compute the subpartition slice ranges to ensure even distribution of data across
     * downstream tasks.
     *
     * <p>This method first tries to compute the subpartition slice ranges by evenly distributing
     * the data volume. If that fails, it attempts to compute the ranges by evenly distributing the
     * number of subpartition slices.
     *
     * @param minParallelism The minimum parallelism.
     * @param maxParallelism The maximum parallelism.
     * @param maxDataVolumePerTask The maximum data volume per task.
     * @param subpartitionSlicesByTypeNumber A map of lists of subpartition slices grouped by type
     *     number.
     * @return An {@code Optional} containing a list of index ranges representing the subpartition
     *     slice ranges. Returns an empty {@code Optional} if no suitable ranges can be computed.
     */
    public static Optional<List<IndexRange>> tryComputeSubpartitionSliceRange(
            int minParallelism,
            int maxParallelism,
            long maxDataVolumePerTask,
            Map<Integer, List<SubpartitionSlice>> subpartitionSlicesByTypeNumber) {
        Optional<List<IndexRange>> subpartitionSliceRanges =
                tryComputeSubpartitionSliceRangeEvenlyDistributedData(
                        minParallelism,
                        maxParallelism,
                        maxDataVolumePerTask,
                        subpartitionSlicesByTypeNumber);
        if (subpartitionSliceRanges.isEmpty()) {
            LOG.info(
                    "Failed to compute a legal subpartition slice range that can evenly distribute data amount, "
                            + "fallback to compute it that can evenly distribute the number of subpartition slices.");
            subpartitionSliceRanges =
                    tryComputeSubpartitionSliceRangeEvenlyDistributedSubpartitionSlices(
                            minParallelism, maxParallelism, subpartitionSlicesByTypeNumber);
        }
        return subpartitionSliceRanges;
    }

    public static JobVertexInputInfo createdJobVertexInputInfoForBroadcast(
            BlockingInputInfo inputInfo, int parallelism) {
        checkArgument(inputInfo.isBroadcast());
        int numPartitions = inputInfo.getNumPartitions();
        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < parallelism; ++i) {
            ExecutionVertexInputInfo executionVertexInputInfo;
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
            executionVertexInputInfos.add(executionVertexInputInfo);
        }
        return new JobVertexInputInfo(executionVertexInputInfos);
    }

    public static JobVertexInputInfo createdJobVertexInputInfoForNonBroadcast(
            BlockingInputInfo inputInfo,
            List<IndexRange> subpartitionSliceRanges,
            List<SubpartitionSlice> subpartitionSlices) {
        checkArgument(!inputInfo.isBroadcast());
        int numPartitions = inputInfo.getNumPartitions();
        List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
        for (int i = 0; i < subpartitionSliceRanges.size(); ++i) {
            IndexRange subpartitionSliceRange = subpartitionSliceRanges.get(i);
            // Convert subpartitionSlices to partition range to subpartition range
            Map<IndexRange, IndexRange> consumedSubpartitionGroups =
                    computeConsumedSubpartitionGroups(
                            subpartitionSliceRange,
                            subpartitionSlices,
                            numPartitions,
                            inputInfo.isPointwise());
            executionVertexInputInfos.add(
                    new ExecutionVertexInputInfo(i, consumedSubpartitionGroups));
        }
        return new JobVertexInputInfo(executionVertexInputInfos);
    }

    private static Optional<List<IndexRange>> tryComputeSubpartitionSliceRangeEvenlyDistributedData(
            int minParallelism,
            int maxParallelism,
            long maxDataVolumePerTask,
            Map<Integer, List<SubpartitionSlice>> subpartitionSlicesByTypeNumber) {
        int subpartitionSlicesSize =
                checkAndGetSubpartitionSlicesSize(subpartitionSlicesByTypeNumber);
        // Distribute the input data evenly among the downstream tasks and record the
        // subpartition slice range for each task.
        List<IndexRange> subpartitionSliceRanges =
                computeSubpartitionSliceRanges(
                        maxDataVolumePerTask,
                        subpartitionSlicesSize,
                        subpartitionSlicesByTypeNumber);
        // if the parallelism is not legal, try to adjust to a legal parallelism
        if (!isLegalParallelism(subpartitionSliceRanges.size(), minParallelism, maxParallelism)) {
            long minBytesSize = maxDataVolumePerTask;
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
            return adjustToClosestLegalParallelism(
                    maxDataVolumePerTask,
                    subpartitionSliceRanges.size(),
                    minParallelism,
                    maxParallelism,
                    minBytesSize,
                    sumBytesSize,
                    limit ->
                            computeParallelism(
                                    limit, subpartitionSlicesSize, subpartitionSlicesByTypeNumber),
                    limit ->
                            computeSubpartitionSliceRanges(
                                    limit, subpartitionSlicesSize, subpartitionSlicesByTypeNumber));
        }
        return Optional.of(subpartitionSliceRanges);
    }

    private static Optional<List<IndexRange>>
            tryComputeSubpartitionSliceRangeEvenlyDistributedSubpartitionSlices(
                    int minParallelism,
                    int maxParallelism,
                    Map<Integer, List<SubpartitionSlice>> subpartitionSlicesByTypeNumber) {
        int subpartitionSlicesSize =
                checkAndGetSubpartitionSlicesSize(subpartitionSlicesByTypeNumber);
        if (subpartitionSlicesSize < minParallelism) {
            return Optional.empty();
        }
        int parallelism = Math.min(subpartitionSlicesSize, maxParallelism);
        List<IndexRange> subpartitionSliceRanges = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            int start = i * subpartitionSlicesSize / parallelism;
            int nextStart = (i + 1) * subpartitionSlicesSize / parallelism;
            subpartitionSliceRanges.add(new IndexRange(start, nextStart - 1));
        }
        checkState(subpartitionSliceRanges.size() == parallelism);
        return Optional.of(subpartitionSliceRanges);
    }

    /**
     * Merge the subpartition slices of the specified range into an index range map, which the key
     * is the partition index range and the value is the subpartition range.
     *
     * <p>Note: In existing algorithms, the consumed subpartition groups for POINTWISE always ensure
     * that there is no overlap in the partition ranges, while for ALL_TO_ALL, the consumed
     * subpartition groups always ensure that there is no overlap in the subpartition ranges. For
     * example, if a task needs to subscribe to {[0,0]->[0,1] ,[1,1]->[0]} (partition range to
     * subpartition range), for POINT WISE it will be: {[0,0]->[0,1], [1,1]->[0,0]}, for ALL_TO-ALL
     * it will be: {[0,1]->[0,0], [0,0]->[1,1]}.The result of this method will also follow this
     * convention.
     *
     * @param subpartitionSliceRange the range of subpartition slices to be merged
     * @param subpartitionSlices subpartition slices
     * @param numPartitions the real number of partitions of input info, use to correct the
     *     partition range
     * @param isPointwise whether the input info is pointwise
     * @return a map indicating the ranges that task needs to consume, the key is partition range
     *     and the value is subpartition range.
     */
    private static Map<IndexRange, IndexRange> computeConsumedSubpartitionGroups(
            IndexRange subpartitionSliceRange,
            List<SubpartitionSlice> subpartitionSlices,
            int numPartitions,
            boolean isPointwise) {
        Map<IndexRange, List<IndexRange>> rangeMap =
                new TreeMap<>(Comparator.comparingInt(IndexRange::getStartIndex));
        for (int i = subpartitionSliceRange.getStartIndex();
                i <= subpartitionSliceRange.getEndIndex();
                ++i) {
            SubpartitionSlice subpartitionSlice = subpartitionSlices.get(i);
            IndexRange keyRange, valueRange;
            if (isPointwise) {
                keyRange = subpartitionSlice.getPartitionRange(numPartitions);
                valueRange = subpartitionSlice.getSubpartitionRange();
            } else {
                keyRange = subpartitionSlice.getSubpartitionRange();
                valueRange = subpartitionSlice.getPartitionRange(numPartitions);
            }
            rangeMap.computeIfAbsent(keyRange, k -> new ArrayList<>()).add(valueRange);
        }

        rangeMap =
                rangeMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry -> mergeIndexRanges(entry.getValue())));

        // reversed the map to merge keys associated with the same value
        Map<IndexRange, List<IndexRange>> reversedRangeMap = new HashMap<>();
        for (Map.Entry<IndexRange, List<IndexRange>> entry : rangeMap.entrySet()) {
            IndexRange valueRange = entry.getKey();
            for (IndexRange keyRange : entry.getValue()) {
                reversedRangeMap.computeIfAbsent(keyRange, k -> new ArrayList<>()).add(valueRange);
            }
        }

        Map<IndexRange, IndexRange> mergedReversedRangeMap =
                reversedRangeMap.entrySet().stream()
                        .collect(
                                Collectors.toMap(
                                        Map.Entry::getKey,
                                        entry -> {
                                            List<IndexRange> mergedRange =
                                                    mergeIndexRanges(entry.getValue());
                                            checkState(mergedRange.size() == 1);
                                            return mergedRange.get(0);
                                        }));

        if (isPointwise) {
            return reverseIndexRangeMap(mergedReversedRangeMap);
        }

        return mergedReversedRangeMap;
    }

    /**
     * Reassembling subpartition slices into balanced n parts and returning the range of index
     * corresponding to each piece of data. Reassembling need to meet the following conditions:<br>
     * 1. The data size of each piece does not exceed the limit.<br>
     * 2. The SubpartitionSlice number in each piece is not larger than maxRangeSize.
     *
     * @param limit the limit of data size
     * @param subpartitionGroupSize the number of SubpartitionSlices
     * @param subpartitionSlices the subpartition slices to be processed
     * @return the range of index corresponding to each piece of data
     */
    private static List<IndexRange> computeSubpartitionSliceRanges(
            long limit,
            int subpartitionGroupSize,
            Map<Integer, List<SubpartitionSlice>> subpartitionSlices) {
        List<IndexRange> subpartitionSliceRanges = new ArrayList<>();
        long accumulatedSize = 0;
        int startIndex = 0;
        Map<Integer, Set<SubpartitionSlice>> bucketsByTypeNumber = new HashMap<>();
        for (int i = 0; i < subpartitionGroupSize; ++i) {
            long currentGroupSize = 0L;
            // bytes size after deduplication
            long currentGroupSizeDeduplicated = 0L;
            for (Map.Entry<Integer, List<SubpartitionSlice>> entry :
                    subpartitionSlices.entrySet()) {
                Integer typeNumber = entry.getKey();
                SubpartitionSlice subpartitionSlice = entry.getValue().get(i);
                Set<SubpartitionSlice> bucket =
                        bucketsByTypeNumber.computeIfAbsent(typeNumber, ignored -> new HashSet<>());
                // When the bucket already contains duplicate subpartitionSlices, its size should be
                // ignored.
                if (!bucket.contains(subpartitionSlice)) {
                    currentGroupSizeDeduplicated += subpartitionSlice.getDataBytes();
                }
                currentGroupSize += subpartitionSlice.getDataBytes();
            }
            if (i == startIndex || accumulatedSize + currentGroupSizeDeduplicated <= limit) {
                accumulatedSize += currentGroupSizeDeduplicated;
            } else {
                subpartitionSliceRanges.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                accumulatedSize = currentGroupSize;
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
     * @param subpartitionSlicesSize the number of SubpartitionSlices
     * @param subpartitionSlices the subpartition slices to be processed
     * @return the parallelism after dividing
     */
    private static int computeParallelism(
            long limit,
            int subpartitionSlicesSize,
            Map<Integer, List<SubpartitionSlice>> subpartitionSlices) {
        int count = 1;
        long accumulatedSize = 0;
        int startIndex = 0;
        Map<Integer, Set<SubpartitionSlice>> bucketsByTypeNumber = new HashMap<>();
        for (int i = 0; i < subpartitionSlicesSize; ++i) {
            long currentGroupSize = 0L;
            long currentGroupSizeDeduplicated = 0L;
            for (Map.Entry<Integer, List<SubpartitionSlice>> entry :
                    subpartitionSlices.entrySet()) {
                Integer typeNumber = entry.getKey();
                SubpartitionSlice subpartitionSlice = entry.getValue().get(i);
                Set<SubpartitionSlice> bucket =
                        bucketsByTypeNumber.computeIfAbsent(typeNumber, ignored -> new HashSet<>());
                if (!bucket.contains(subpartitionSlice)) {
                    currentGroupSizeDeduplicated += subpartitionSlice.getDataBytes();
                }
                currentGroupSize += subpartitionSlice.getDataBytes();
            }
            if (i == startIndex || accumulatedSize + currentGroupSizeDeduplicated <= limit) {
                accumulatedSize += currentGroupSizeDeduplicated;
            } else {
                ++count;
                startIndex = i;
                accumulatedSize = currentGroupSize;
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

    private static int checkAndGetSubpartitionSlicesSize(
            Map<Integer, List<SubpartitionSlice>> subpartitionSlices) {
        Set<Integer> subpartitionSliceSizes =
                subpartitionSlices.values().stream().map(List::size).collect(Collectors.toSet());
        checkArgument(subpartitionSliceSizes.size() == 1);
        return subpartitionSliceSizes.iterator().next();
    }

    private static Map<IndexRange, IndexRange> reverseIndexRangeMap(
            Map<IndexRange, IndexRange> indexRangeMap) {
        Map<IndexRange, IndexRange> reversedRangeMap = new HashMap<>();
        for (Map.Entry<IndexRange, IndexRange> entry : indexRangeMap.entrySet()) {
            checkState(!reversedRangeMap.containsKey(entry.getValue()));
            reversedRangeMap.put(entry.getValue(), entry.getKey());
        }
        return reversedRangeMap;
    }

    public static long calculateDataVolumePerTaskForInputsGroup(
            long globalDataVolumePerTask,
            List<BlockingInputInfo> inputsGroup,
            List<BlockingInputInfo> allInputs) {
        return calculateDataVolumePerTaskForInput(
                globalDataVolumePerTask,
                inputsGroup.stream().mapToLong(BlockingInputInfo::getNumBytesProduced).sum(),
                allInputs.stream().mapToLong(BlockingInputInfo::getNumBytesProduced).sum());
    }

    public static long calculateDataVolumePerTaskForInput(
            long globalDataVolumePerTask, long inputsGroupBytes, long totalDataBytes) {
        return (long) ((double) inputsGroupBytes / totalDataBytes * globalDataVolumePerTask);
    }
}
