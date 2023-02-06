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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.runtime.executiongraph.ExecutionVertexInputInfo;
import org.apache.flink.runtime.executiongraph.IndexRange;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.ParallelismAndInputInfos;
import org.apache.flink.runtime.executiongraph.VertexInputInfoComputationUtils;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Default implementation of {@link VertexParallelismAndInputInfosDecider}. This implementation will
 * decide parallelism and {@link JobVertexInputInfo}s as follows:
 *
 * <p>1. For job vertices whose inputs are all ALL_TO_ALL edges, evenly distribute data to
 * downstream subtasks, make different downstream subtasks consume roughly the same amount of data.
 *
 * <p>2. For other cases, evenly distribute subpartitions to downstream subtasks, make different
 * downstream subtasks consume roughly the same number of subpartitions.
 */
public class DefaultVertexParallelismAndInputInfosDecider
        implements VertexParallelismAndInputInfosDecider {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultVertexParallelismAndInputInfosDecider.class);

    /**
     * The maximum number of subpartitions belonging to the same result that each task can consume.
     * We currently need this limitation to avoid too many channels in a downstream task leading to
     * poor performance.
     *
     * <p>TODO: Once we support one channel to consume multiple upstream subpartitions in the
     * future, we can remove this limitation
     */
    private static final int MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME = 32768;

    private final int globalMaxParallelism;
    private final int globalMinParallelism;
    private final long dataVolumePerTask;
    private final int globalDefaultSourceParallelism;

    private DefaultVertexParallelismAndInputInfosDecider(
            int globalMaxParallelism,
            int globalMinParallelism,
            MemorySize dataVolumePerTask,
            int globalDefaultSourceParallelism) {

        checkArgument(globalMinParallelism > 0, "The minimum parallelism must be larger than 0.");
        checkArgument(
                globalMaxParallelism >= globalMinParallelism,
                "Maximum parallelism should be greater than or equal to the minimum parallelism.");
        checkArgument(
                globalDefaultSourceParallelism > 0,
                "The default source parallelism must be larger than 0.");
        checkNotNull(dataVolumePerTask);

        this.globalMaxParallelism = globalMaxParallelism;
        this.globalMinParallelism = globalMinParallelism;
        this.dataVolumePerTask = dataVolumePerTask.getBytes();
        this.globalDefaultSourceParallelism = globalDefaultSourceParallelism;
    }

    @Override
    public ParallelismAndInputInfos decideParallelismAndInputInfosForVertex(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int vertexInitialParallelism,
            int vertexMaxParallelism) {
        checkArgument(
                vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                        || vertexInitialParallelism > 0);
        checkArgument(vertexMaxParallelism > 0 && vertexMaxParallelism >= vertexInitialParallelism);

        if (consumedResults.isEmpty()) {
            // source job vertex
            int parallelism =
                    vertexInitialParallelism > 0
                            ? vertexInitialParallelism
                            : computeSourceParallelism(jobVertexId, vertexMaxParallelism);
            return new ParallelismAndInputInfos(parallelism, Collections.emptyMap());
        } else {
            int minParallelism = globalMinParallelism;
            int maxParallelism = globalMaxParallelism;

            if (vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                    && vertexMaxParallelism < minParallelism) {
                LOG.info(
                        "The vertex maximum parallelism {} is smaller than the global minimum parallelism {}. "
                                + "Use {} as the lower bound to decide parallelism of job vertex {}.",
                        vertexMaxParallelism,
                        minParallelism,
                        vertexMaxParallelism,
                        jobVertexId);
                minParallelism = vertexMaxParallelism;
            }
            if (vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                    && vertexMaxParallelism < maxParallelism) {
                LOG.info(
                        "The vertex maximum parallelism {} is smaller than the global maximum parallelism {}. "
                                + "Use {} as the upper bound to decide parallelism of job vertex {}.",
                        vertexMaxParallelism,
                        maxParallelism,
                        vertexMaxParallelism,
                        jobVertexId);
                maxParallelism = vertexMaxParallelism;
            }
            checkState(maxParallelism >= minParallelism);

            if (vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                    && areAllInputsAllToAll(consumedResults)
                    && !areAllInputsBroadcast(consumedResults)) {
                return decideParallelismAndEvenlyDistributeData(
                        jobVertexId,
                        consumedResults,
                        vertexInitialParallelism,
                        minParallelism,
                        maxParallelism);
            } else {
                return decideParallelismAndEvenlyDistributeSubpartitions(
                        jobVertexId,
                        consumedResults,
                        vertexInitialParallelism,
                        minParallelism,
                        maxParallelism);
            }
        }
    }

    private int computeSourceParallelism(JobVertexID jobVertexId, int maxParallelism) {
        if (globalDefaultSourceParallelism > maxParallelism) {
            LOG.info(
                    "The global default source parallelism {} is larger than the maximum parallelism {}. "
                            + "Use {} as the parallelism of source job vertex {}.",
                    globalDefaultSourceParallelism,
                    maxParallelism,
                    maxParallelism,
                    jobVertexId);
            return maxParallelism;
        } else {
            return globalDefaultSourceParallelism;
        }
    }

    private static boolean areAllInputsAllToAll(List<BlockingResultInfo> consumedResults) {
        return consumedResults.stream().noneMatch(BlockingResultInfo::isPointwise);
    }

    private static boolean areAllInputsBroadcast(List<BlockingResultInfo> consumedResults) {
        return consumedResults.stream().allMatch(BlockingResultInfo::isBroadcast);
    }

    /**
     * Decide parallelism and input infos, which will make the subpartitions be evenly distributed
     * to downstream subtasks, such that different downstream subtasks consume roughly the same
     * number of subpartitions.
     *
     * @param jobVertexId The job vertex id
     * @param consumedResults The information of consumed blocking results
     * @param initialParallelism The initial parallelism of the job vertex
     * @param minParallelism the min parallelism
     * @param maxParallelism the max parallelism
     * @return the parallelism and vertex input infos
     */
    private ParallelismAndInputInfos decideParallelismAndEvenlyDistributeSubpartitions(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int initialParallelism,
            int minParallelism,
            int maxParallelism) {
        checkArgument(!consumedResults.isEmpty());
        int parallelism =
                initialParallelism > 0
                        ? initialParallelism
                        : decideParallelism(
                                jobVertexId, consumedResults, minParallelism, maxParallelism);
        return new ParallelismAndInputInfos(
                parallelism,
                VertexInputInfoComputationUtils.computeVertexInputInfos(
                        parallelism, consumedResults, true));
    }

    int decideParallelism(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int minParallelism,
            int maxParallelism) {
        checkArgument(!consumedResults.isEmpty());

        // Considering that the sizes of broadcast results are usually very small, we compute the
        // parallelism only based on sizes of non-broadcast results
        final List<BlockingResultInfo> nonBroadcastResults =
                getNonBroadcastResultInfos(consumedResults);
        if (nonBroadcastResults.isEmpty()) {
            return minParallelism;
        }

        long totalBytes =
                nonBroadcastResults.stream()
                        .mapToLong(BlockingResultInfo::getNumBytesProduced)
                        .sum();
        int parallelism = (int) Math.ceil((double) totalBytes / dataVolumePerTask);
        int minParallelismLimitedByMaxSubpartitions =
                (int)
                        Math.ceil(
                                (double) getMaxNumSubpartitions(nonBroadcastResults)
                                        / MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME);
        parallelism = Math.max(parallelism, minParallelismLimitedByMaxSubpartitions);

        LOG.debug(
                "The total size of non-broadcast data is {}, the initially decided parallelism of job vertex {} is {}.",
                new MemorySize(totalBytes),
                jobVertexId,
                parallelism);

        if (parallelism < minParallelism) {
            LOG.info(
                    "The initially decided parallelism {} is smaller than the minimum parallelism {}. "
                            + "Use {} as the finally decided parallelism of job vertex {}.",
                    parallelism,
                    minParallelism,
                    minParallelism,
                    jobVertexId);
            parallelism = minParallelism;
        } else if (parallelism > maxParallelism) {
            LOG.info(
                    "The initially decided parallelism {} is larger than the maximum parallelism {}. "
                            + "Use {} as the finally decided parallelism of job vertex {}.",
                    parallelism,
                    maxParallelism,
                    maxParallelism,
                    jobVertexId);
            parallelism = maxParallelism;
        }

        return parallelism;
    }

    /**
     * Decide parallelism and input infos, which will make the data be evenly distributed to
     * downstream subtasks, such that different downstream subtasks consume roughly the same amount
     * of data.
     *
     * @param jobVertexId The job vertex id
     * @param consumedResults The information of consumed blocking results
     * @param initialParallelism The initial parallelism of the job vertex
     * @param minParallelism the min parallelism
     * @param maxParallelism the max parallelism
     * @return the parallelism and vertex input infos
     */
    private ParallelismAndInputInfos decideParallelismAndEvenlyDistributeData(
            JobVertexID jobVertexId,
            List<BlockingResultInfo> consumedResults,
            int initialParallelism,
            int minParallelism,
            int maxParallelism) {
        checkArgument(initialParallelism == ExecutionConfig.PARALLELISM_DEFAULT);
        checkArgument(!consumedResults.isEmpty());
        consumedResults.forEach(resultInfo -> checkState(!resultInfo.isPointwise()));

        // Considering that the sizes of broadcast results are usually very small, we compute the
        // parallelism and input infos only based on sizes of non-broadcast results
        final List<BlockingResultInfo> nonBroadcastResults =
                getNonBroadcastResultInfos(consumedResults);
        int subpartitionNum = checkAndGetSubpartitionNum(nonBroadcastResults);

        long[] bytesBySubpartition = new long[subpartitionNum];
        Arrays.fill(bytesBySubpartition, 0L);
        for (BlockingResultInfo resultInfo : nonBroadcastResults) {
            List<Long> subpartitionBytes =
                    ((AllToAllBlockingResultInfo) resultInfo).getAggregatedSubpartitionBytes();
            for (int i = 0; i < subpartitionNum; ++i) {
                bytesBySubpartition[i] += subpartitionBytes.get(i);
            }
        }

        int maxNumPartitions = getMaxNumPartitions(nonBroadcastResults);
        int maxRangeSize = MAX_NUM_SUBPARTITIONS_PER_TASK_CONSUME / maxNumPartitions;
        // compute subpartition ranges
        List<IndexRange> subpartitionRanges =
                computeSubpartitionRanges(bytesBySubpartition, dataVolumePerTask, maxRangeSize);

        // if the parallelism is not legal, adjust to a legal parallelism
        if (!isLegalParallelism(subpartitionRanges.size(), minParallelism, maxParallelism)) {
            Optional<List<IndexRange>> adjustedSubpartitionRanges =
                    adjustToClosestLegalParallelism(
                            dataVolumePerTask,
                            subpartitionRanges.size(),
                            minParallelism,
                            maxParallelism,
                            Arrays.stream(bytesBySubpartition).min().getAsLong(),
                            Arrays.stream(bytesBySubpartition).sum(),
                            limit -> computeParallelism(bytesBySubpartition, limit, maxRangeSize),
                            limit ->
                                    computeSubpartitionRanges(
                                            bytesBySubpartition, limit, maxRangeSize));
            if (!adjustedSubpartitionRanges.isPresent()) {
                // can't find any legal parallelism, fall back to evenly distribute subpartitions
                LOG.info(
                        "Cannot find a legal parallelism to evenly distribute data for job vertex {}. "
                                + "Fall back to compute a parallelism that can evenly distribute subpartitions.",
                        jobVertexId);
                return decideParallelismAndEvenlyDistributeSubpartitions(
                        jobVertexId,
                        consumedResults,
                        initialParallelism,
                        minParallelism,
                        maxParallelism);
            }
            subpartitionRanges = adjustedSubpartitionRanges.get();
        }

        checkState(isLegalParallelism(subpartitionRanges.size(), minParallelism, maxParallelism));
        return createParallelismAndInputInfos(consumedResults, subpartitionRanges);
    }

    private static boolean isLegalParallelism(
            int parallelism, int minParallelism, int maxParallelism) {
        return parallelism >= minParallelism && parallelism <= maxParallelism;
    }

    private static int checkAndGetSubpartitionNum(List<BlockingResultInfo> consumedResults) {
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
    private static Optional<List<IndexRange>> adjustToClosestLegalParallelism(
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

    private static ParallelismAndInputInfos createParallelismAndInputInfos(
            List<BlockingResultInfo> consumedResults, List<IndexRange> subpartitionRanges) {

        final Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();
        consumedResults.forEach(
                resultInfo -> {
                    int sourceParallelism = resultInfo.getNumPartitions();
                    IndexRange partitionRange = new IndexRange(0, sourceParallelism - 1);

                    List<ExecutionVertexInputInfo> executionVertexInputInfos = new ArrayList<>();
                    for (int i = 0; i < subpartitionRanges.size(); ++i) {
                        IndexRange subpartitionRange;
                        if (resultInfo.isBroadcast()) {
                            subpartitionRange = new IndexRange(0, 0);
                        } else {
                            subpartitionRange = subpartitionRanges.get(i);
                        }
                        ExecutionVertexInputInfo executionVertexInputInfo =
                                new ExecutionVertexInputInfo(i, partitionRange, subpartitionRange);
                        executionVertexInputInfos.add(executionVertexInputInfo);
                    }

                    vertexInputInfos.put(
                            resultInfo.getResultId(),
                            new JobVertexInputInfo(executionVertexInputInfos));
                });
        return new ParallelismAndInputInfos(subpartitionRanges.size(), vertexInputInfos);
    }

    private static List<IndexRange> computeSubpartitionRanges(
            long[] nums, long limit, int maxRangeSize) {
        List<IndexRange> subpartitionRanges = new ArrayList<>();
        long tmpSum = 0;
        int startIndex = 0;
        for (int i = 0; i < nums.length; ++i) {
            long num = nums[i];
            if (i == startIndex
                    || (tmpSum + num <= limit && (i - startIndex + 1) <= maxRangeSize)) {
                tmpSum += num;
            } else {
                subpartitionRanges.add(new IndexRange(startIndex, i - 1));
                startIndex = i;
                tmpSum = num;
            }
        }
        subpartitionRanges.add(new IndexRange(startIndex, nums.length - 1));
        return subpartitionRanges;
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

    private static int getMaxNumPartitions(List<BlockingResultInfo> consumedResults) {
        checkArgument(!consumedResults.isEmpty());
        return consumedResults.stream()
                .mapToInt(BlockingResultInfo::getNumPartitions)
                .max()
                .getAsInt();
    }

    private static int getMaxNumSubpartitions(List<BlockingResultInfo> consumedResults) {
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

    private static List<BlockingResultInfo> getNonBroadcastResultInfos(
            List<BlockingResultInfo> consumedResults) {
        return consumedResults.stream()
                .filter(resultInfo -> !resultInfo.isBroadcast())
                .collect(Collectors.toList());
    }

    static DefaultVertexParallelismAndInputInfosDecider from(
            int maxParallelism, Configuration configuration) {
        return new DefaultVertexParallelismAndInputInfosDecider(
                maxParallelism,
                configuration.getInteger(
                        BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM),
                configuration.get(
                        BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK),
                configuration.get(
                        BatchExecutionOptions
                                .ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM));
    }
}
