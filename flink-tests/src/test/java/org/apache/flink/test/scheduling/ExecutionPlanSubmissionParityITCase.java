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
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions.SchedulerType;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameter;
import org.apache.flink.testutils.junit.extensions.parameterized.ParameterizedTestExtension;
import org.apache.flink.testutils.junit.extensions.parameterized.Parameters;

import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assumptions.assumeThat;

/**
 * Submits the same program once as a {@link StreamGraph} (what every client does since FLINK-36065)
 * and once as the {@link org.apache.flink.runtime.jobgraph.JobGraph} the client used to send, and
 * checks that the scheduler ends up with the same vertices at the same parallelism.
 *
 * <p>With {@link PipelineOptions#PARALLELISM_OVERRIDES} in the cluster configuration this is the
 * regression test for FLINK-38770: the overrides were only applied to submitted JobGraphs.
 */
@ExtendWith(ParameterizedTestExtension.class)
class ExecutionPlanSubmissionParityITCase {

    private static final String SOURCE_UID = "parity-source";
    private static final String MAP_UID = "parity-map";
    private static final String SINK_UID = "parity-sink";

    private static final JobVertexID MAP_VERTEX_ID =
            ParallelismOverridesTestUtils.vertexIdOfChainHead(MAP_UID);

    @Parameter public SchedulerType scheduler;

    @Parameters(name = "scheduler={0}")
    private static Collection<SchedulerType> schedulers() {
        return Arrays.asList(
                SchedulerType.Default, SchedulerType.Adaptive, SchedulerType.AdaptiveBatch);
    }

    @TestTemplate
    void testOverridesApplyEquallyToBothPlanTypes() throws Exception {
        final Map<String, String> overrides =
                Collections.singletonMap(MAP_VERTEX_ID.toHexString(), "3");

        try (MiniCluster miniCluster =
                ParallelismOverridesTestUtils.startMiniCluster(scheduler, overrides, true)) {
            final ArchivedExecutionGraph viaStreamGraph =
                    runToCompletion(miniCluster, streamGraph());
            final ArchivedExecutionGraph viaJobGraph =
                    runToCompletion(miniCluster, streamGraph().getJobGraph());

            assertSameVertices(viaStreamGraph, viaJobGraph);
            assertThat(viaJobGraph.getAllVertices().get(MAP_VERTEX_ID).getParallelism())
                    .as("override applied on the JobGraph path")
                    .isEqualTo(3);
            assertThat(viaStreamGraph.getAllVertices().get(MAP_VERTEX_ID).getParallelism())
                    .as("override applied on the StreamGraph path")
                    .isEqualTo(3);
        }
    }

    @TestTemplate
    void testBothPlanTypesAgreeWithoutOverrides() throws Exception {
        try (MiniCluster miniCluster =
                ParallelismOverridesTestUtils.startMiniCluster(
                        scheduler, Collections.emptyMap(), true)) {
            assertSameVertices(
                    runToCompletion(miniCluster, streamGraph()),
                    runToCompletion(miniCluster, streamGraph().getJobGraph()));
        }
    }

    /**
     * Known gap of the incrementally generated JobGraph: the upstream result partitions are sized
     * before the overridden vertex exists, from its max parallelism, which here derives from the
     * job's default parallelism of 1. The JobGraph path sizes them from the overridden vertex.
     */
    @TestTemplate
    void testOverrideAboveUpstreamSizingIsAKnownGapOfTheIncrementalGenerator() throws Exception {
        assumeThat(scheduler).isEqualTo(SchedulerType.AdaptiveBatch);
        final Map<String, String> overrides =
                Collections.singletonMap(MAP_VERTEX_ID.toHexString(), "3");

        try (MiniCluster miniCluster =
                ParallelismOverridesTestUtils.startMiniCluster(scheduler, overrides, false)) {
            assertThat(
                            runToCompletion(miniCluster, streamGraph().getJobGraph())
                                    .getAllVertices()
                                    .get(MAP_VERTEX_ID)
                                    .getParallelism())
                    .as("JobGraph path")
                    .isEqualTo(3);
            assertThatThrownBy(() -> runToCompletion(miniCluster, streamGraph()))
                    .as("StreamGraph path")
                    .rootCause()
                    .hasMessageContaining("subpartitions cannot be consumed by 3 consumers");
        }
    }

    private static void assertSameVertices(
            ArchivedExecutionGraph viaStreamGraph, ArchivedExecutionGraph viaJobGraph) {
        final Map<JobVertexID, ? extends AccessExecutionJobVertex> streamVertices =
                viaStreamGraph.getAllVertices();
        final Map<JobVertexID, ? extends AccessExecutionJobVertex> jobVertices =
                viaJobGraph.getAllVertices();
        assertThat(streamVertices.keySet())
                .as("job vertex ids")
                .containsExactlyInAnyOrderElementsOf(jobVertices.keySet());
        for (JobVertexID id : jobVertices.keySet()) {
            final AccessExecutionJobVertex fromStreamGraph = streamVertices.get(id);
            final AccessExecutionJobVertex fromJobGraph = jobVertices.get(id);
            assertThat(fromStreamGraph.getParallelism())
                    .as("parallelism of %s", fromJobGraph.getName())
                    .isEqualTo(fromJobGraph.getParallelism());
            assertThat(fromStreamGraph.getMaxParallelism())
                    .as("max parallelism of %s", fromJobGraph.getName())
                    .isEqualTo(fromJobGraph.getMaxParallelism());
        }
    }

    /** {@code source -> map -> sink}, chaining disabled so every operator is a chain head. */
    private StreamGraph streamGraph() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());
        env.setRuntimeMode(
                scheduler == SchedulerType.AdaptiveBatch
                        ? RuntimeExecutionMode.BATCH
                        : RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);
        env.disableOperatorChaining();
        env.fromSequence(0, 999)
                .setParallelism(1)
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

    private static ArchivedExecutionGraph runToCompletion(
            MiniCluster miniCluster, ExecutionPlan plan) throws Exception {
        final JobID jobId = miniCluster.submitJob(plan).get().getJobID();
        final JobResult result = miniCluster.requestJobResult(jobId).get();
        if (result.getSerializedThrowable().isPresent()) {
            throw new AssertionError(
                    String.format(
                            "job %s submitted as %s failed",
                            jobId, plan.getClass().getSimpleName()),
                    result.getSerializedThrowable()
                            .get()
                            .deserializeError(
                                    ExecutionPlanSubmissionParityITCase.class.getClassLoader()));
        }
        return miniCluster.getArchivedExecutionGraph(jobId).get();
    }
}
