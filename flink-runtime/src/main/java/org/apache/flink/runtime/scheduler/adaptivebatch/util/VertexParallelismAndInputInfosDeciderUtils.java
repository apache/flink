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
import org.apache.flink.runtime.scheduler.adaptivebatch.BisectionSearchUtils;
import org.apache.flink.runtime.scheduler.adaptivebatch.BlockingInputInfo;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkState;

/** Utils class for VertexParallelismAndInputInfosDecider. */
public class VertexParallelismAndInputInfosDeciderUtils {
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

    public static List<BlockingInputInfo> getBroadcastInputInfos(
            List<BlockingInputInfo> consumedResults) {
        return consumedResults.stream()
                .filter(BlockingInputInfo::isBroadcast)
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

    public static int getMaxNumSubpartitions(List<BlockingInputInfo> consumedResults) {
        checkArgument(!consumedResults.isEmpty());
        return consumedResults.stream()
                .mapToInt(
                        resultInfo ->
                                IntStream.range(0, resultInfo.getNumPartitions())
                                        .boxed()
                                        .mapToInt(resultInfo::getNumSubpartitions)
                                        .sum())
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

    public static boolean isLegalParallelism(
            int parallelism, int minParallelism, int maxParallelism) {
        return parallelism >= minParallelism && parallelism <= maxParallelism;
    }

    public static boolean checkAndGetIntraCorrelation(List<BlockingInputInfo> inputInfos) {
        Set<Boolean> intraCorrelationSet =
                inputInfos.stream()
                        .map(BlockingInputInfo::existIntraInputKeyCorrelation)
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
}
