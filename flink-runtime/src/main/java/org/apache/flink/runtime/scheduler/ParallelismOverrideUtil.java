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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.graph.ExecutionPlan;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility for applying {@link PipelineOptions#PARALLELISM_OVERRIDES} to a {@link JobGraph}.
 *
 * <p>Parallelism overrides let operators change the parallelism of specific job vertices without
 * modifying the job code. Each entry maps a {@link JobVertex#getID() JobVertexID} hex string to the
 * desired parallelism.
 *
 * <p>Overrides can be configured in two places, merged in this order of precedence:
 *
 * <ol>
 *   <li>{@link JobGraph#getJobConfiguration() Job configuration} (higher precedence — set by the
 *       job author or the client)
 *   <li>Job master configuration (lower precedence — set on the cluster side, e.g. {@code
 *       flink-conf.yaml})
 * </ol>
 *
 * <p>This utility is called by scheduler factories ({@link
 * org.apache.flink.runtime.scheduler.DefaultSchedulerFactory}, {@link
 * org.apache.flink.runtime.scheduler.adaptive.AdaptiveSchedulerFactory}, and {@link
 * org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchSchedulerFactory}) after {@link
 * org.apache.flink.streaming.api.graph.StreamGraph StreamGraph} has been converted to a {@link
 * JobGraph}. This ensures overrides apply uniformly in both Session Mode (JobGraph submission) and
 * Application Mode (StreamGraph submission).
 *
 * <p>The adaptive-batch StreamGraph path is handled separately by {@link
 * org.apache.flink.runtime.scheduler.adaptivebatch.DefaultAdaptiveExecutionHandler}, which checks
 * overrides per-vertex in {@code getInitialParallelism} as the JobGraph is built incrementally.
 */
@Internal
public class ParallelismOverrideUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelismOverrideUtil.class);

    /**
     * Applies parallelism overrides to the given execution plan if it is a {@link JobGraph}.
     *
     * <p>This overload lets scheduler factories that accept either a {@link JobGraph} or a {@link
     * org.apache.flink.streaming.api.graph.StreamGraph} call the utility unconditionally, keeping
     * the call site structurally consistent with factories that always work on a {@code JobGraph}.
     *
     * <p>For a {@link JobGraph} input, this delegates to {@link
     * #applyParallelismOverrides(JobGraph, Configuration)}.
     *
     * <p>For a {@link org.apache.flink.streaming.api.graph.StreamGraph} input, this is a no-op: the
     * adaptive-batch StreamGraph path builds the JobGraph incrementally, so overrides cannot be
     * applied upfront. They are applied per-vertex by {@link
     * org.apache.flink.runtime.scheduler.adaptivebatch.DefaultAdaptiveExecutionHandler} in {@code
     * getInitialParallelism} as each vertex becomes ready.
     *
     * @param executionPlan the execution plan; overrides are applied only when this is a {@link
     *     JobGraph}
     * @param jobMasterConfiguration the job master configuration containing potential overrides
     * @throws IllegalArgumentException if an override value is not a positive integer
     */
    public static void applyParallelismOverridesIfApplicable(
            ExecutionPlan executionPlan, Configuration jobMasterConfiguration) {
        if (executionPlan instanceof JobGraph) {
            applyParallelismOverrides((JobGraph) executionPlan, jobMasterConfiguration);
        }
    }

    /**
     * Applies parallelism overrides from configuration to the JobGraph.
     *
     * <p>Overrides are taken from two sources (in order of precedence):
     *
     * <ol>
     *   <li>JobGraph configuration (higher precedence)
     *   <li>Job master configuration (lower precedence)
     * </ol>
     *
     * @param jobGraph the JobGraph to modify
     * @param jobMasterConfiguration the job master configuration containing potential overrides
     * @throws IllegalArgumentException if an override value is not a positive integer
     */
    public static void applyParallelismOverrides(
            JobGraph jobGraph, Configuration jobMasterConfiguration) {
        Map<String, String> overrides = new HashMap<>();

        // Add overrides from job master configuration
        Map<String, String> masterConfigOverrides =
                jobMasterConfiguration.get(PipelineOptions.PARALLELISM_OVERRIDES);
        overrides.putAll(masterConfigOverrides);

        // Add overrides from job configuration (these take precedence)
        Map<String, String> jobConfigOverrides =
                jobGraph.getJobConfiguration().get(PipelineOptions.PARALLELISM_OVERRIDES);
        overrides.putAll(jobConfigOverrides);

        // Apply overrides to each vertex
        for (JobVertex vertex : jobGraph.getVertices()) {
            String vertexIdHex = vertex.getID().toHexString();
            String override = overrides.get(vertexIdHex);
            if (override != null) {
                int currentParallelism = vertex.getParallelism();
                int overrideParallelism = parseParallelism(vertexIdHex, override);
                LOG.info(
                        "Applying parallelism override for job vertex {} ({}): {} -> {}",
                        vertex.getName(),
                        vertex.getID(),
                        currentParallelism,
                        overrideParallelism);
                vertex.setParallelism(overrideParallelism);
            }
        }
    }

    private static int parseParallelism(String vertexIdHex, String override) {
        final int parallelism;
        try {
            parallelism = Integer.parseInt(override);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid parallelism override for job vertex %s: '%s' is not a valid integer.",
                            vertexIdHex, override),
                    e);
        }
        if (parallelism <= 0) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid parallelism override for job vertex %s: parallelism must be positive, got %d.",
                            vertexIdHex, parallelism));
        }
        return parallelism;
    }

    private ParallelismOverrideUtil() {
        // Utility class, not meant to be instantiated
    }
}
