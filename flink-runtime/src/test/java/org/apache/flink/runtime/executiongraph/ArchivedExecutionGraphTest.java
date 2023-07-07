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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.ArchivedExecutionConfig;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.runtime.checkpoint.CheckpointRetentionPolicy;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStatsSummarySnapshot;
import org.apache.flink.runtime.checkpoint.StatsSummarySnapshot;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismInfo;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismStore;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link ArchivedExecutionGraph}. */
public class ArchivedExecutionGraphTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private static ExecutionGraph runtimeGraph;

    @BeforeAll
    static void setupExecutionGraph() throws Exception {
        // -------------------------------------------------------------------------------------------------------------
        // Setup
        // -------------------------------------------------------------------------------------------------------------

        JobVertexID v1ID = new JobVertexID();
        JobVertexID v2ID = new JobVertexID();

        JobVertex v1 = new JobVertex("v1", v1ID);
        JobVertex v2 = new JobVertex("v2", v2ID);

        v1.setParallelism(1);
        v2.setParallelism(2);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);

        ExecutionConfig config = new ExecutionConfig();

        config.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());
        config.setParallelism(4);
        config.enableObjectReuse();
        config.setGlobalJobParameters(new TestJobParameters());

        CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration(
                        100,
                        100,
                        100,
                        1,
                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION,
                        true,
                        false,
                        0,
                        0,
                        0);
        JobCheckpointingSettings checkpointingSettings =
                new JobCheckpointingSettings(chkConfig, null);

        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertices(asList(v1, v2))
                        .setJobCheckpointingSettings(checkpointingSettings)
                        .setExecutionConfig(config)
                        .build();

        SchedulerBase scheduler =
                SchedulerTestingUtils.createScheduler(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        EXECUTOR_RESOURCE.getExecutor());

        runtimeGraph = scheduler.getExecutionGraph();

        scheduler.startScheduling();
        scheduler.updateTaskExecutionState(
                new TaskExecutionState(
                        runtimeGraph
                                .getAllExecutionVertices()
                                .iterator()
                                .next()
                                .getCurrentExecutionAttempt()
                                .getAttemptId(),
                        ExecutionState.FAILED,
                        new RuntimeException("Local failure")));
    }

    @Test
    void testArchive() throws IOException, ClassNotFoundException {
        ArchivedExecutionGraph archivedGraph = ArchivedExecutionGraph.createFrom(runtimeGraph);

        compareExecutionGraph(runtimeGraph, archivedGraph);
    }

    @Test
    void testSerialization() throws IOException, ClassNotFoundException {
        ArchivedExecutionGraph archivedGraph = ArchivedExecutionGraph.createFrom(runtimeGraph);

        verifySerializability(archivedGraph);
    }

    @Test
    void testCreateFromInitializingJobForSuspendedJob() {
        final ArchivedExecutionGraph suspendedExecutionGraph =
                ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                        new JobID(),
                        "TestJob",
                        JobStatus.SUSPENDED,
                        new Exception("Test suspension exception"),
                        null,
                        System.currentTimeMillis());

        assertThat(suspendedExecutionGraph.getState()).isEqualTo(JobStatus.SUSPENDED);
        assertThat(suspendedExecutionGraph.getFailureInfo()).isNotNull();
    }

    @Test
    void testCheckpointSettingsArchiving() {
        final CheckpointCoordinatorConfiguration checkpointCoordinatorConfiguration =
                CheckpointCoordinatorConfiguration.builder().build();

        final ArchivedExecutionGraph archivedGraph =
                ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                        new JobID(),
                        "TestJob",
                        JobStatus.INITIALIZING,
                        null,
                        new JobCheckpointingSettings(checkpointCoordinatorConfiguration, null),
                        System.currentTimeMillis());

        assertContainsCheckpointSettings(archivedGraph);
    }

    public static void assertContainsCheckpointSettings(ArchivedExecutionGraph archivedGraph) {
        assertThat(archivedGraph.getCheckpointCoordinatorConfiguration()).isNotNull();
        assertThat(archivedGraph.getCheckpointStatsSnapshot()).isNotNull();
        assertThat(archivedGraph.getCheckpointStorageName().get()).isEqualTo("Unknown");
        assertThat(archivedGraph.getStateBackendName().get()).isEqualTo("Unknown");
    }

    @Test
    void testArchiveSparseWithVertices() {
        final JobVertex jobVertex = new JobVertex("op");
        jobVertex.setParallelism(1);

        final int storedParallelism = 4;
        final int storedMaxParallelism = 8;
        final DefaultVertexParallelismStore initialParallelismStore =
                new DefaultVertexParallelismStore();
        initialParallelismStore.setParallelismInfo(
                jobVertex.getID(),
                new DefaultVertexParallelismInfo(
                        storedParallelism, storedMaxParallelism, ignored -> Optional.empty()));

        final ArchivedExecutionGraph archivedGraph =
                ArchivedExecutionGraph.createSparseArchivedExecutionGraphWithJobVertices(
                        new JobID(),
                        "TestJob",
                        JobStatus.INITIALIZING,
                        null,
                        null,
                        System.currentTimeMillis(),
                        Arrays.asList(jobVertex),
                        initialParallelismStore);

        // make sure both vertex retrieval APIs work and are equivalent
        final ArchivedExecutionJobVertex archivedVertex =
                archivedGraph.getJobVertex(jobVertex.getID());
        assertThat(archivedVertex).isNotNull();
        assertThat(archivedGraph.getVerticesTopologically()).containsExactly(archivedVertex);

        // parallelism store is queried for (max)parallelism
        assertThat(archivedVertex.getParallelism()).isEqualTo(storedParallelism);
        assertThat(archivedVertex.getMaxParallelism()).isEqualTo(storedMaxParallelism);

        assertThat(archivedVertex.getName()).isEqualTo(jobVertex.getName());

        // everything related to subtasks returns sane defaults
        assertThat(archivedVertex.getAggregatedUserAccumulatorsStringified()).hasSize(0);
        assertThat(archivedVertex.getTaskVertices()).hasSize(0);
        assertThat(archivedVertex.getAggregateState()).isSameAs(ExecutionState.CREATED);
    }

    @Test
    void testArchiveWithStatusOverride() throws IOException, ClassNotFoundException {
        ArchivedExecutionGraph archivedGraph =
                ArchivedExecutionGraph.createFrom(runtimeGraph, JobStatus.RESTARTING);

        assertThat(archivedGraph.getState()).isEqualTo(JobStatus.RESTARTING);
        assertThat(archivedGraph.getStatusTimestamp(JobStatus.FAILED)).isEqualTo(0L);
    }

    private static void compareExecutionGraph(
            AccessExecutionGraph runtimeGraph, AccessExecutionGraph archivedGraph)
            throws IOException, ClassNotFoundException {
        // -------------------------------------------------------------------------------------------------------------
        // ExecutionGraph
        // -------------------------------------------------------------------------------------------------------------
        assertThat(runtimeGraph.getJsonPlan()).isEqualTo(archivedGraph.getJsonPlan());
        assertThat(runtimeGraph.getJobID()).isEqualTo(archivedGraph.getJobID());
        assertThat(runtimeGraph.getJobName()).isEqualTo(archivedGraph.getJobName());
        assertThat(runtimeGraph.getState()).isEqualTo(archivedGraph.getState());
        assertThat(runtimeGraph.getFailureInfo().getExceptionAsString())
                .isEqualTo(archivedGraph.getFailureInfo().getExceptionAsString());
        assertThat(runtimeGraph.getStatusTimestamp(JobStatus.CREATED))
                .isEqualTo(archivedGraph.getStatusTimestamp(JobStatus.CREATED));
        assertThat(runtimeGraph.getStatusTimestamp(JobStatus.RUNNING))
                .isEqualTo(archivedGraph.getStatusTimestamp(JobStatus.RUNNING));
        assertThat(runtimeGraph.getStatusTimestamp(JobStatus.FAILING))
                .isEqualTo(archivedGraph.getStatusTimestamp(JobStatus.FAILING));
        assertThat(runtimeGraph.getStatusTimestamp(JobStatus.FAILED))
                .isEqualTo(archivedGraph.getStatusTimestamp(JobStatus.FAILED));
        assertThat(runtimeGraph.getStatusTimestamp(JobStatus.CANCELLING))
                .isEqualTo(archivedGraph.getStatusTimestamp(JobStatus.CANCELLING));
        assertThat(runtimeGraph.getStatusTimestamp(JobStatus.CANCELED))
                .isEqualTo(archivedGraph.getStatusTimestamp(JobStatus.CANCELED));
        assertThat(runtimeGraph.getStatusTimestamp(JobStatus.FINISHED))
                .isEqualTo(archivedGraph.getStatusTimestamp(JobStatus.FINISHED));
        assertThat(runtimeGraph.getStatusTimestamp(JobStatus.RESTARTING))
                .isEqualTo(archivedGraph.getStatusTimestamp(JobStatus.RESTARTING));
        assertThat(runtimeGraph.getStatusTimestamp(JobStatus.SUSPENDED))
                .isEqualTo(archivedGraph.getStatusTimestamp(JobStatus.SUSPENDED));
        assertThat(runtimeGraph.isStoppable()).isEqualTo(archivedGraph.isStoppable());

        // -------------------------------------------------------------------------------------------------------------
        // CheckpointStats
        // -------------------------------------------------------------------------------------------------------------
        CheckpointStatsSnapshot runtimeSnapshot = runtimeGraph.getCheckpointStatsSnapshot();
        CheckpointStatsSnapshot archivedSnapshot = archivedGraph.getCheckpointStatsSnapshot();

        List<Function<CompletedCheckpointStatsSummarySnapshot, StatsSummarySnapshot>> meters =
                asList(
                        CompletedCheckpointStatsSummarySnapshot::getEndToEndDurationStats,
                        CompletedCheckpointStatsSummarySnapshot::getPersistedDataStats,
                        CompletedCheckpointStatsSummarySnapshot::getProcessedDataStats,
                        CompletedCheckpointStatsSummarySnapshot::getStateSizeStats);

        List<Function<StatsSummarySnapshot, Object>> aggs =
                asList(
                        StatsSummarySnapshot::getAverage,
                        StatsSummarySnapshot::getMinimum,
                        StatsSummarySnapshot::getMaximum,
                        StatsSummarySnapshot::getSum,
                        StatsSummarySnapshot::getCount,
                        s -> s.getQuantile(0.5d),
                        s -> s.getQuantile(0.9d),
                        s -> s.getQuantile(0.95d),
                        s -> s.getQuantile(0.99d),
                        s -> s.getQuantile(0.999d));
        for (Function<CompletedCheckpointStatsSummarySnapshot, StatsSummarySnapshot> meter :
                meters) {
            StatsSummarySnapshot runtime = meter.apply(runtimeSnapshot.getSummaryStats());
            StatsSummarySnapshot archived = meter.apply(runtimeSnapshot.getSummaryStats());
            for (Function<StatsSummarySnapshot, Object> agg : aggs) {
                assertThat(agg.apply(runtime)).isEqualTo(agg.apply(archived));
            }
        }

        assertThat(runtimeSnapshot.getCounts().getTotalNumberOfCheckpoints())
                .isEqualTo(archivedSnapshot.getCounts().getTotalNumberOfCheckpoints());
        assertThat(runtimeSnapshot.getCounts().getNumberOfCompletedCheckpoints())
                .isEqualTo(archivedSnapshot.getCounts().getNumberOfCompletedCheckpoints());
        assertThat(runtimeSnapshot.getCounts().getNumberOfInProgressCheckpoints())
                .isEqualTo(archivedSnapshot.getCounts().getNumberOfInProgressCheckpoints());

        // -------------------------------------------------------------------------------------------------------------
        // ArchivedExecutionConfig
        // -------------------------------------------------------------------------------------------------------------
        ArchivedExecutionConfig runtimeConfig = runtimeGraph.getArchivedExecutionConfig();
        ArchivedExecutionConfig archivedConfig = archivedGraph.getArchivedExecutionConfig();

        assertThat(runtimeConfig.getExecutionMode()).isEqualTo(archivedConfig.getExecutionMode());
        assertThat(runtimeConfig.getParallelism()).isEqualTo(archivedConfig.getParallelism());
        assertThat(runtimeConfig.getObjectReuseEnabled())
                .isEqualTo(archivedConfig.getObjectReuseEnabled());
        assertThat(runtimeConfig.getRestartStrategyDescription())
                .isEqualTo(archivedConfig.getRestartStrategyDescription());
        assertThat(archivedConfig.getGlobalJobParameters().get("hello")).isNotNull();
        assertThat(runtimeConfig.getGlobalJobParameters().get("hello"))
                .isEqualTo(archivedConfig.getGlobalJobParameters().get("hello"));

        // -------------------------------------------------------------------------------------------------------------
        // StringifiedAccumulators
        // -------------------------------------------------------------------------------------------------------------
        ArchivedExecutionGraphTestUtils.compareStringifiedAccumulators(
                runtimeGraph.getAccumulatorResultsStringified(),
                archivedGraph.getAccumulatorResultsStringified());
        ArchivedExecutionGraphTestUtils.compareSerializedAccumulators(
                runtimeGraph.getAccumulatorsSerialized(),
                archivedGraph.getAccumulatorsSerialized());

        // -------------------------------------------------------------------------------------------------------------
        // JobVertices
        // -------------------------------------------------------------------------------------------------------------
        Map<JobVertexID, ? extends AccessExecutionJobVertex> runtimeVertices =
                runtimeGraph.getAllVertices();
        Map<JobVertexID, ? extends AccessExecutionJobVertex> archivedVertices =
                archivedGraph.getAllVertices();

        for (Map.Entry<JobVertexID, ? extends AccessExecutionJobVertex> vertex :
                runtimeVertices.entrySet()) {
            compareExecutionJobVertex(vertex.getValue(), archivedVertices.get(vertex.getKey()));
        }

        Iterator<? extends AccessExecutionJobVertex> runtimeTopologicalVertices =
                runtimeGraph.getVerticesTopologically().iterator();
        Iterator<? extends AccessExecutionJobVertex> archiveTopologicaldVertices =
                archivedGraph.getVerticesTopologically().iterator();

        while (runtimeTopologicalVertices.hasNext()) {
            assertThat(archiveTopologicaldVertices.hasNext()).isTrue();
            compareExecutionJobVertex(
                    runtimeTopologicalVertices.next(), archiveTopologicaldVertices.next());
        }

        // -------------------------------------------------------------------------------------------------------------
        // ExecutionVertices
        // -------------------------------------------------------------------------------------------------------------
        Iterator<? extends AccessExecutionVertex> runtimeExecutionVertices =
                runtimeGraph.getAllExecutionVertices().iterator();
        Iterator<? extends AccessExecutionVertex> archivedExecutionVertices =
                archivedGraph.getAllExecutionVertices().iterator();

        while (runtimeExecutionVertices.hasNext()) {
            assertThat(archivedExecutionVertices.hasNext()).isTrue();
            ArchivedExecutionGraphTestUtils.compareExecutionVertex(
                    runtimeExecutionVertices.next(), archivedExecutionVertices.next());
        }
    }

    private static void compareExecutionJobVertex(
            AccessExecutionJobVertex runtimeJobVertex, AccessExecutionJobVertex archivedJobVertex) {
        assertThat(runtimeJobVertex.getName()).isEqualTo(archivedJobVertex.getName());
        assertThat(runtimeJobVertex.getParallelism()).isEqualTo(archivedJobVertex.getParallelism());
        assertThat(runtimeJobVertex.getMaxParallelism())
                .isEqualTo(archivedJobVertex.getMaxParallelism());
        assertThat(runtimeJobVertex.getJobVertexId()).isEqualTo(archivedJobVertex.getJobVertexId());
        assertThat(runtimeJobVertex.getAggregateState())
                .isEqualTo(archivedJobVertex.getAggregateState());

        ArchivedExecutionGraphTestUtils.compareStringifiedAccumulators(
                runtimeJobVertex.getAggregatedUserAccumulatorsStringified(),
                archivedJobVertex.getAggregatedUserAccumulatorsStringified());

        AccessExecutionVertex[] runtimeExecutionVertices = runtimeJobVertex.getTaskVertices();
        AccessExecutionVertex[] archivedExecutionVertices = archivedJobVertex.getTaskVertices();
        assertThat(runtimeExecutionVertices.length).isEqualTo(archivedExecutionVertices.length);
        for (int x = 0; x < runtimeExecutionVertices.length; x++) {
            ArchivedExecutionGraphTestUtils.compareExecutionVertex(
                    runtimeExecutionVertices[x], archivedExecutionVertices[x]);
        }
    }

    private static void verifySerializability(ArchivedExecutionGraph graph)
            throws IOException, ClassNotFoundException {
        ArchivedExecutionGraph copy = CommonTestUtils.createCopySerializable(graph);
        compareExecutionGraph(graph, copy);
    }

    private static class TestJobParameters extends ExecutionConfig.GlobalJobParameters {
        private static final long serialVersionUID = -8118611781035212808L;
        private Map<String, String> parameters;

        private TestJobParameters() {
            this.parameters = new HashMap<>();
            this.parameters.put("hello", "world");
        }

        @Override
        public Map<String, String> toMap() {
            return parameters;
        }
    }
}
