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

package org.apache.flink.test.scheduling;

import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;

import java.time.Duration;
import java.util.Map;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.NO_RESTART_STRATEGY;

/** Shared pieces of the {@code pipeline.jobvertex-parallelism-overrides} tests. */
public final class ParallelismOverridesTestUtils {

    private ParallelismOverridesTestUtils() {}

    /** A chain head with a uid gets the job vertex id {@code murmur3_128(uid)}. */
    public static JobVertexID vertexIdOfChainHead(String uid) {
        return new JobVertexID(StreamingJobGraphGenerator.generateOperatorID(uid).getBytes());
    }

    /**
     * One TaskManager with 8 slots, no restarts, and the given overrides in the cluster
     * configuration. With {@code boundAdaptiveBatchParallelism} the adaptive batch scheduler sizes
     * results for a max parallelism of 8 and starts sources at 4, so the small override values of
     * the tests stay inside what the upstream results allow.
     */
    public static MiniCluster startMiniCluster(
            SchedulerType scheduler,
            Map<String, String> overrides,
            boolean boundAdaptiveBatchParallelism)
            throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, scheduler);
        configuration.set(RestOptions.BIND_PORT, "0");
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING, Duration.ZERO);
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ZERO);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY, NO_RESTART_STRATEGY.getMainValue());
        if (boundAdaptiveBatchParallelism) {
            configuration.set(BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM, 8);
            configuration.set(
                    BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_DEFAULT_SOURCE_PARALLELISM, 4);
        }
        if (!overrides.isEmpty()) {
            configuration.set(PipelineOptions.PARALLELISM_OVERRIDES, overrides);
        }
        final MiniCluster miniCluster =
                new MiniCluster(
                        new MiniClusterConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumTaskManagers(1)
                                .setNumSlotsPerTaskManager(8)
                                .build());
        miniCluster.start();
        return miniCluster;
    }
}
