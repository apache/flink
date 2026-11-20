/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.recovery;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.core.testutils.AllCallbackWrapper;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperExtension;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.test.scheduling.ParallelismOverridesTestUtils;
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
import java.util.UUID;

import static org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType.FIXED_DELAY;

/**
 * The operator rescales a running job in place through the resource requirements of the adaptive
 * scheduler, which the Dispatcher persists with the plan. A JobManager failover afterwards must
 * keep that parallelism, not go back to the {@link PipelineOptions#PARALLELISM_OVERRIDES} the job
 * was submitted with.
 */
@ExtendWith(ParameterizedTestExtension.class)
class ParallelismOverridesRescaleRecoveryITCase {

    private static final String SOURCE_UID = "rescale-source";
    private static final String MAP_UID = "rescale-map";
    private static final String SINK_UID = "rescale-sink";
    private static final JobVertexID SOURCE_VERTEX_ID =
            ParallelismOverridesTestUtils.vertexIdOfChainHead(SOURCE_UID);
    private static final JobVertexID MAP_VERTEX_ID =
            ParallelismOverridesTestUtils.vertexIdOfChainHead(MAP_UID);
    private static final JobVertexID SINK_VERTEX_ID =
            ParallelismOverridesTestUtils.vertexIdOfChainHead(SINK_UID);
    private static final int OVERRIDE = 3;
    private static final int RESCALED = 2;

    @RegisterExtension
    private static final AllCallbackWrapper<ZooKeeperExtension> ZOOKEEPER_EXTENSION =
            new AllCallbackWrapper<>(new ZooKeeperExtension());

    @TempDir Path haStoragePath;

    @Parameter public boolean submitStreamGraph;

    @Parameters(name = "streamGraph={0}")
    private static Collection<Boolean> planTypes() {
        return Arrays.asList(true, false);
    }

    @TestTemplate
    void testRescaleSurvivesJobManagerFailover() throws Exception {
        final MiniClusterConfiguration miniClusterConfiguration =
                new MiniClusterConfiguration.Builder()
                        .setConfiguration(createConfiguration())
                        .setNumSlotsPerTaskManager(8)
                        .build();
        final StreamGraph streamGraph = unboundedStreamGraph();
        final ExecutionPlan plan = submitStreamGraph ? streamGraph : streamGraph.getJobGraph();
        final JobID jobId = plan.getJobID();

        final MiniCluster miniCluster = new MiniCluster(miniClusterConfiguration);
        try {
            miniCluster.start();
            miniCluster.submitJob(plan).get();
            waitUntilMapRunsWith(miniCluster, jobId, OVERRIDE);

            miniCluster
                    .updateJobResourceRequirements(
                            jobId,
                            JobResourceRequirements.newBuilder()
                                    .setParallelismForJobVertex(SOURCE_VERTEX_ID, 1, 1)
                                    .setParallelismForJobVertex(MAP_VERTEX_ID, 1, RESCALED)
                                    .setParallelismForJobVertex(SINK_VERTEX_ID, 1, 1)
                                    .build())
                    .get();
            waitUntilMapRunsWith(miniCluster, jobId, RESCALED);
        } finally {
            miniCluster.closeAsyncWithoutCleaningHighAvailabilityData().get();
        }

        try (MiniCluster recovered = new MiniCluster(miniClusterConfiguration)) {
            recovered.start();
            waitUntilMapRunsWith(recovered, jobId, RESCALED);
            recovered.cancelJob(jobId).get();
        }
    }

    private Configuration createConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
        configuration.set(RestOptions.BIND_PORT, "0");
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING, Duration.ZERO);
        configuration.set(
                JobManagerOptions.SCHEDULER_EXECUTING_RESOURCE_STABILIZATION_TIMEOUT,
                Duration.ZERO);
        // keeps the job alive while the first cluster shuts down
        configuration.set(RestartStrategyOptions.RESTART_STRATEGY, FIXED_DELAY.getMainValue());
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, Integer.MAX_VALUE);
        configuration.set(
                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMillis(100));
        configuration.set(HighAvailabilityOptions.HA_MODE, "zookeeper");
        configuration.set(
                HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM,
                ZOOKEEPER_EXTENSION.getCustomExtension().getConnectString());
        configuration.set(HighAvailabilityOptions.HA_STORAGE_PATH, haStoragePath.toString());
        // a test run that fails must not leave its job for the next run to recover
        configuration.set(HighAvailabilityOptions.HA_CLUSTER_ID, UUID.randomUUID().toString());
        configuration.set(
                PipelineOptions.PARALLELISM_OVERRIDES,
                Collections.singletonMap(MAP_VERTEX_ID.toHexString(), String.valueOf(OVERRIDE)));
        return configuration;
    }

    private static void waitUntilMapRunsWith(MiniCluster miniCluster, JobID jobId, int parallelism)
            throws Exception {
        final int[] last = {-1};
        try {
            CommonTestUtils.waitUntilCondition(
                    () -> {
                        final ArchivedExecutionGraph graph;
                        try {
                            graph = miniCluster.getArchivedExecutionGraph(jobId).get();
                        } catch (Exception e) {
                            return false;
                        }
                        final AccessExecutionJobVertex map =
                                graph.getAllVertices().get(MAP_VERTEX_ID);
                        if (map != null) {
                            last[0] = map.getParallelism();
                        }
                        return graph.getState() == JobStatus.RUNNING
                                && map != null
                                && map.getParallelism() == parallelism;
                    },
                    100L,
                    600);
        } catch (Exception e) {
            throw new AssertionError(
                    String.format(
                            "map did not run with parallelism %d, last seen %d",
                            parallelism, last[0]),
                    e);
        }
    }

    private static StreamGraph unboundedStreamGraph() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.fromSequence(0, Long.MAX_VALUE)
                .uid(SOURCE_UID)
                .name("source")
                .map(v -> v + 1)
                .uid(MAP_UID)
                .name("map")
                .sinkTo(new DiscardingSink<>())
                .uid(SINK_UID)
                .name("sink");
        final StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setJobId(JobID.generate());
        return streamGraph;
    }
}
