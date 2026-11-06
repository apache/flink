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

package org.apache.flink.runtime.jobgraph;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Resolution and application of {@link PipelineOptions#PARALLELISM_OVERRIDES}.
 *
 * <p>The map is resolved once at submission, job configuration over cluster configuration, and
 * applied after the job vertex ids are computed: by the Dispatcher for a submitted {@link
 * JobGraph}, by the JobGraph generator for a {@link
 * org.apache.flink.streaming.api.graph.StreamGraph}, whose job configuration carries the resolved
 * map. The persisted plan therefore replays the same overrides on recovery, and an override never
 * changes a job vertex id, an operator id or the chaining.
 *
 * <p>A value that is not a positive integer fails only the job that contains the vertex, so a bad
 * entry in the cluster configuration does not block other jobs.
 */
@Internal
public final class JobVertexParallelismOverrides {

    private static final Logger LOG = LoggerFactory.getLogger(JobVertexParallelismOverrides.class);

    private static final Pattern JOB_VERTEX_ID = Pattern.compile("[0-9a-f]{32}");

    private JobVertexParallelismOverrides() {}

    /** Merges the cluster and the job map; the entries of the job map win. */
    public static Map<String, String> merge(
            ReadableConfig clusterConfiguration, ReadableConfig jobConfiguration) {
        final Map<String, String> overrides =
                new HashMap<>(clusterConfiguration.get(PipelineOptions.PARALLELISM_OVERRIDES));
        overrides.putAll(jobConfiguration.get(PipelineOptions.PARALLELISM_OVERRIDES));
        return overrides;
    }

    /**
     * Keys the map by job vertex id. Entries whose key is not the hex form of a job vertex id are
     * dropped with a warning; values are kept as they are and checked when they are applied.
     */
    public static Map<JobVertexID, String> byJobVertexId(Map<String, String> overrides) {
        final Map<JobVertexID, String> keyed = new HashMap<>(overrides.size());
        for (Map.Entry<String, String> override : overrides.entrySet()) {
            final String key = override.getKey();
            if (JOB_VERTEX_ID.matcher(key).matches()) {
                keyed.put(JobVertexID.fromHexString(key), override.getValue());
            } else {
                LOG.warn(
                        "Ignoring the entry '{}' of {}: a job vertex id has 32 lowercase hex characters.",
                        key,
                        PipelineOptions.PARALLELISM_OVERRIDES.key());
            }
        }
        return keyed;
    }

    /** The inverse of {@link #byJobVertexId(Map)}, in the format of the configuration option. */
    public static Map<String, String> toConfigurationValue(Map<JobVertexID, String> overrides) {
        final Map<String, String> value = new HashMap<>(overrides.size());
        for (Map.Entry<JobVertexID, String> override : overrides.entrySet()) {
            value.put(override.getKey().toHexString(), override.getValue());
        }
        return value;
    }

    /**
     * Sets the overridden parallelism on the vertex if the map contains one for it.
     *
     * @return whether the vertex was changed
     * @throws IllegalArgumentException if the override for the vertex is not a positive integer
     */
    public static boolean apply(JobVertex vertex, Map<JobVertexID, String> overrides) {
        final String override = overrides.get(vertex.getID());
        if (override == null) {
            return false;
        }
        final int parallelism = parseParallelism(vertex.getID(), override);
        LOG.info(
                "Changing job vertex {} parallelism from {} to {}",
                vertex.getID(),
                vertex.getParallelism(),
                parallelism);
        vertex.setParallelism(parallelism);
        return true;
    }

    private static int parseParallelism(JobVertexID jobVertexId, String override) {
        final int parallelism;
        try {
            parallelism = Integer.parseInt(override);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(
                            "The parallelism override '%s' for job vertex %s in %s is not an integer.",
                            override, jobVertexId, PipelineOptions.PARALLELISM_OVERRIDES.key()),
                    e);
        }
        if (parallelism < 1 && parallelism != ExecutionConfig.PARALLELISM_DEFAULT) {
            throw new IllegalArgumentException(
                    String.format(
                            "The parallelism override %d for job vertex %s in %s must be at least 1,"
                                    + " or -1 for the default parallelism.",
                            parallelism, jobVertexId, PipelineOptions.PARALLELISM_OVERRIDES.key()));
        }
        return parallelism;
    }
}
