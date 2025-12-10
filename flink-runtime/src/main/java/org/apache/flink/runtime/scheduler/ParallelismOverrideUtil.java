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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for applying parallelism overrides from configuration to JobGraph vertices.
 *
 * <p>This utility must be called after converting StreamGraph to JobGraph in all SchedulerNGFactory
 * implementations to ensure parallelism overrides are respected in Application Mode (where
 * StreamGraph is submitted directly).
 */
@Internal
public class ParallelismOverrideUtil {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelismOverrideUtil.class);

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
     */
    public static void applyParallelismOverrides(
            JobGraph jobGraph, Configuration jobMasterConfiguration) {
        Map<String, String> overrides = new HashMap<>();

        // Add overrides from job master configuration
        overrides.putAll(jobMasterConfiguration.get(PipelineOptions.PARALLELISM_OVERRIDES));

        // Add overrides from job configuration (these take precedence)
        overrides.putAll(jobGraph.getJobConfiguration().get(PipelineOptions.PARALLELISM_OVERRIDES));

        // Apply overrides to each vertex
        for (JobVertex vertex : jobGraph.getVertices()) {
            String override = overrides.get(vertex.getID().toHexString());
            if (override != null) {
                int currentParallelism = vertex.getParallelism();
                int overrideParallelism = Integer.parseInt(override);
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

    private ParallelismOverrideUtil() {
        // Utility class, not meant to be instantiated
    }
}
