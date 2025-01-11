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
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.ParallelismAndInputInfos;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.adaptivebatch.util.AllToAllVertexInputInfoComputer;
import org.apache.flink.runtime.scheduler.adaptivebatch.util.PointwiseVertexInputInfoComputer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.calculateDataVolumePerTaskForInputsGroup;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.checkAndGetParallelism;
import static org.apache.flink.runtime.scheduler.adaptivebatch.util.VertexParallelismAndInputInfosDeciderUtils.getNonBroadcastInputInfos;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Default implementation of {@link VertexParallelismAndInputInfosDecider}. This implementation will
 * decide parallelism and {@link JobVertexInputInfo}s as follows:
 *
 * <p>1. We will first attempt to: evenly distribute data to downstream subtasks, make different
 * downstream subtasks consume roughly the same amount of data.
 *
 * <p>2. If step 1 fails or is not applicable, we will proceed to: evenly distribute subpartitions
 * to downstream subtasks, make different downstream subtasks consume roughly the same number of
 * subpartitions.
 */
public class DefaultVertexParallelismAndInputInfosDecider
        implements VertexParallelismAndInputInfosDecider {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultVertexParallelismAndInputInfosDecider.class);

    private final int globalMaxParallelism;
    private final int globalMinParallelism;
    private final long dataVolumePerTask;
    private final int globalDefaultSourceParallelism;
    private final AllToAllVertexInputInfoComputer allToAllVertexInputInfoComputer;
    private final PointwiseVertexInputInfoComputer pointwiseVertexInputInfoComputer;

    private DefaultVertexParallelismAndInputInfosDecider(
            int globalMaxParallelism,
            int globalMinParallelism,
            MemorySize dataVolumePerTask,
            int globalDefaultSourceParallelism,
            double skewedFactor,
            long skewedThreshold) {

        checkArgument(globalMinParallelism > 0, "The minimum parallelism must be larger than 0.");
        checkArgument(
                globalMaxParallelism >= globalMinParallelism,
                "Maximum parallelism should be greater than or equal to the minimum parallelism.");
        checkArgument(
                globalDefaultSourceParallelism > 0,
                "The default source parallelism must be larger than 0.");
        checkNotNull(dataVolumePerTask);
        checkArgument(
                skewedFactor > 0, "The default skewed partition factor must be larger than 0.");
        checkArgument(skewedThreshold > 0, "The default skewed threshold must be larger than 0.");

        this.globalMaxParallelism = globalMaxParallelism;
        this.globalMinParallelism = globalMinParallelism;
        this.dataVolumePerTask = dataVolumePerTask.getBytes();
        this.globalDefaultSourceParallelism = globalDefaultSourceParallelism;
        this.allToAllVertexInputInfoComputer =
                new AllToAllVertexInputInfoComputer(skewedFactor, skewedThreshold);
        this.pointwiseVertexInputInfoComputer = new PointwiseVertexInputInfoComputer();
    }

    @Override
    public ParallelismAndInputInfos decideParallelismAndInputInfosForVertex(
            JobVertexID jobVertexId,
            List<BlockingInputInfo> consumedResults,
            int vertexInitialParallelism,
            int vertexMinParallelism,
            int vertexMaxParallelism) {
        checkArgument(
                vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                        || vertexInitialParallelism > 0);
        checkArgument(
                vertexMinParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                        || vertexMinParallelism > 0);
        checkArgument(
                vertexMaxParallelism > 0
                        && vertexMaxParallelism >= vertexInitialParallelism
                        && vertexMaxParallelism >= vertexMinParallelism);

        if (consumedResults.isEmpty()) {
            // source job vertex
            int parallelism =
                    vertexInitialParallelism > 0
                            ? vertexInitialParallelism
                            : computeSourceParallelismUpperBound(jobVertexId, vertexMaxParallelism);
            return new ParallelismAndInputInfos(parallelism, Collections.emptyMap());
        }

        int minParallelism = Math.max(globalMinParallelism, vertexMinParallelism);
        int maxParallelism = globalMaxParallelism;

        if (vertexInitialParallelism == ExecutionConfig.PARALLELISM_DEFAULT
                && vertexMaxParallelism < minParallelism) {
            LOG.info(
                    "The vertex maximum parallelism {} is smaller than the minimum parallelism {}. "
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

        return decideParallelismAndInputInfosForNonSource(
                jobVertexId,
                consumedResults,
                vertexInitialParallelism,
                minParallelism,
                maxParallelism);
    }

    @Override
    public int computeSourceParallelismUpperBound(JobVertexID jobVertexId, int maxParallelism) {
        if (globalDefaultSourceParallelism > maxParallelism) {
            LOG.info(
                    "The global default source parallelism {} is larger than the maximum parallelism {}. "
                            + "Use {} as the upper bound parallelism of source job vertex {}.",
                    globalDefaultSourceParallelism,
                    maxParallelism,
                    maxParallelism,
                    jobVertexId);
            return maxParallelism;
        } else {
            return globalDefaultSourceParallelism;
        }
    }

    @Override
    public long getDataVolumePerTask() {
        return dataVolumePerTask;
    }

    private ParallelismAndInputInfos decideParallelismAndInputInfosForNonSource(
            JobVertexID jobVertexId,
            List<BlockingInputInfo> consumedResults,
            int vertexInitialParallelism,
            int minParallelism,
            int maxParallelism) {
        int parallelism =
                vertexInitialParallelism > 0
                        ? vertexInitialParallelism
                        : decideParallelism(
                                jobVertexId, consumedResults, minParallelism, maxParallelism);

        List<BlockingInputInfo> pointwiseInputs = new ArrayList<>();

        List<BlockingInputInfo> allToAllInputs = new ArrayList<>();

        consumedResults.forEach(
                inputInfo -> {
                    if (inputInfo.isPointwise()) {
                        pointwiseInputs.add(inputInfo);
                    } else {
                        allToAllInputs.add(inputInfo);
                    }
                });

        // For AllToAll like inputs, we derive parallelism as a whole, while for Pointwise inputs,
        // we derive parallelism separately for each input, and our goal is ensured that the final
        // parallelisms of those inputs are consistent and meet expectations.
        // Since AllToAll supports deriving parallelism within a flexible range, this might
        // interfere with the target parallelism. Therefore, in the following cases, we need to
        // reset the minimum and maximum parallelism to limit the flexibility of parallelism
        // derivation to achieve the goal:
        // 1.  Vertex has a specified parallelism, we should follow it.
        // 2.  There are pointwise inputs, which means that there may be inputs whose parallelism is
        // derived one-by-one, we need to reset the min and max parallelism.
        if (vertexInitialParallelism > 0 || !pointwiseInputs.isEmpty()) {
            minParallelism = parallelism;
            maxParallelism = parallelism;
        }

        Map<IntermediateDataSetID, JobVertexInputInfo> vertexInputInfos = new HashMap<>();

        if (!allToAllInputs.isEmpty()) {
            vertexInputInfos.putAll(
                    allToAllVertexInputInfoComputer.compute(
                            jobVertexId,
                            allToAllInputs,
                            parallelism,
                            minParallelism,
                            maxParallelism,
                            calculateDataVolumePerTaskForInputsGroup(
                                    dataVolumePerTask, allToAllInputs, consumedResults)));
        }

        if (!pointwiseInputs.isEmpty()) {
            vertexInputInfos.putAll(
                    pointwiseVertexInputInfoComputer.compute(
                            pointwiseInputs,
                            parallelism,
                            calculateDataVolumePerTaskForInputsGroup(
                                    dataVolumePerTask, pointwiseInputs, consumedResults)));
        }

        return new ParallelismAndInputInfos(
                checkAndGetParallelism(vertexInputInfos.values()), vertexInputInfos);
    }

    int decideParallelism(
            JobVertexID jobVertexId,
            List<BlockingInputInfo> consumedResults,
            int minParallelism,
            int maxParallelism) {
        checkArgument(!consumedResults.isEmpty());

        // Considering that the sizes of broadcast results are usually very small, we compute the
        // parallelism only based on sizes of non-broadcast results
        final List<BlockingInputInfo> nonBroadcastResults =
                getNonBroadcastInputInfos(consumedResults);
        if (nonBroadcastResults.isEmpty()) {
            return minParallelism;
        }

        long totalBytes =
                nonBroadcastResults.stream()
                        .mapToLong(BlockingInputInfo::getNumBytesProduced)
                        .sum();
        int parallelism = (int) Math.ceil((double) totalBytes / dataVolumePerTask);

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

    static DefaultVertexParallelismAndInputInfosDecider from(
            int maxParallelism,
            double skewedFactor,
            long skewedThreshold,
            Configuration configuration) {
        return new DefaultVertexParallelismAndInputInfosDecider(
                maxParallelism,
                configuration.get(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MIN_PARALLELISM),
                configuration.get(
                        BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK),
                configuration.get(
                        BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM,
                        maxParallelism),
                skewedFactor,
                skewedThreshold);
    }
}
