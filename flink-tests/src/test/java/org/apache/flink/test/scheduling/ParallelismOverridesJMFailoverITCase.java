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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.AccessExecutionVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServicesWithLeadershipControl;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

/**
 * A job master failover creates the scheduler again from the plan the Dispatcher holds, so a {@link
 * StreamGraph} is translated a second time. That translation must honour the {@link
 * PipelineOptions#PARALLELISM_OVERRIDES} the Dispatcher attached at submission (FLINK-38770).
 */
@ExtendWith(ParameterizedTestExtension.class)
class ParallelismOverridesJMFailoverITCase {

    private static final String MAP_UID = "failover-map";
    private static final JobVertexID MAP_VERTEX_ID =
            ParallelismOverridesTestUtils.vertexIdOfChainHead(MAP_UID);
    private static final int OVERRIDE = 3;
    private static final long WAIT_RETRY_INTERVAL_MILLIS = 100L;
    private static final int WAIT_RETRY_ATTEMPTS = 1200;

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @TempDir Path haStoragePath;

    @Parameter public SchedulerType scheduler;

    @Parameters(name = "scheduler={0}")
    private static Collection<SchedulerType> schedulers() {
        return Arrays.asList(SchedulerType.Default, SchedulerType.Adaptive);
    }

    @TestTemplate
    void testOverrideSurvivesJobMasterFailover() throws Exception {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, scheduler);
        configuration.set(RestOptions.BIND_PORT, "0");
        configuration.set(HighAvailabilityOptions.HA_STORAGE_PATH, haStoragePath.toString());
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING, Duration.ZERO);
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ZERO);
        configuration.set(
                PipelineOptions.PARALLELISM_OVERRIDES,
                Collections.singletonMap(MAP_VERTEX_ID.toHexString(), String.valueOf(OVERRIDE)));

        final EmbeddedHaServicesWithLeadershipControl haServices =
                new EmbeddedHaServicesWithLeadershipControl(EXECUTOR_EXTENSION.getExecutor());
        try (TestingMiniCluster miniCluster =
                TestingMiniCluster.newBuilder(
                                TestingMiniClusterConfiguration.newBuilder()
                                        .setConfiguration(configuration)
                                        .setNumTaskManagers(1)
                                        .setNumSlotsPerTaskManager(8)
                                        .build())
                        .setHighAvailabilityServicesSupplier(() -> haServices)
                        .build()) {
            miniCluster.start();

            final JobID jobId = miniCluster.submitJob(unboundedStreamGraph()).get().getJobID();
            final ArchivedExecutionGraph beforeFailover =
                    waitUntilMapRunsWith(miniCluster, jobId, OVERRIDE, 0L);

            haServices.revokeJobMasterLeadership(jobId).get();
            haServices.grantJobMasterLeadership(jobId);

            waitUntilMapRunsWith(
                    miniCluster,
                    jobId,
                    OVERRIDE,
                    beforeFailover.getStatusTimestamp(JobStatus.CREATED));
            miniCluster.cancelJob(jobId).get();
        }
    }

    /**
     * Waits until the map vertex of an execution graph created after {@code createdAfter} runs with
     * the given parallelism and returns that graph.
     */
    private static ArchivedExecutionGraph waitUntilMapRunsWith(
            TestingMiniCluster miniCluster, JobID jobId, int parallelism, long createdAfter)
            throws Exception {
        final ArchivedExecutionGraph[] latest = new ArchivedExecutionGraph[1];
        // bounded: on code that ignores the override this condition never becomes true
        CommonTestUtils.waitUntilCondition(
                () -> {
                    final ArchivedExecutionGraph graph;
                    try {
                        graph = miniCluster.getArchivedExecutionGraph(jobId).get();
                    } catch (Exception e) {
                        // the job master is not reachable while leadership moves
                        return false;
                    }
                    if (graph.getStatusTimestamp(JobStatus.CREATED) <= createdAfter
                            || graph.getState() != JobStatus.RUNNING) {
                        return false;
                    }
                    final AccessExecutionJobVertex map = graph.getAllVertices().get(MAP_VERTEX_ID);
                    if (map == null || map.getParallelism() != parallelism) {
                        return false;
                    }
                    for (AccessExecutionVertex task : map.getTaskVertices()) {
                        if (task.getExecutionState() != ExecutionState.RUNNING) {
                            return false;
                        }
                    }
                    latest[0] = graph;
                    return true;
                },
                WAIT_RETRY_INTERVAL_MILLIS,
                WAIT_RETRY_ATTEMPTS);
        return latest[0];
    }

    private static StreamGraph unboundedStreamGraph() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.fromSequence(0, Long.MAX_VALUE)
                .name("source")
                .map(v -> v + 1)
                .uid(MAP_UID)
                .name("map")
                .sinkTo(new DiscardingSink<>())
                .name("sink");
        final StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobId(JobID.generate());
        return streamGraph;
    }
}
