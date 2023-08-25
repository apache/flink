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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the interaction between the {@link DefaultScheduler}, {@link ExecutionGraph} and the
 * {@link CheckpointCoordinator}.
 */
class DefaultSchedulerCheckpointCoordinatorTest {

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    /** Tests that the checkpoint coordinator is shut down if the execution graph is failed. */
    @Test
    void testClosingSchedulerShutsDownCheckpointCoordinatorOnFailedExecutionGraph()
            throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter =
                TestingCheckpointIDCounter.createStoreWithShutdownCheckAndNoStartAction(
                        counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store =
                TestingCompletedCheckpointStore
                        .createStoreWithShutdownCheckAndNoCompletedCheckpoints(storeShutdownFuture);

        final SchedulerBase scheduler = createSchedulerAndEnableCheckpointing(counter, store);
        final ExecutionGraph graph = scheduler.getExecutionGraph();
        final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

        assertThat(checkpointCoordinator).isNotNull();
        assertThat(checkpointCoordinator.isShutdown()).isFalse();

        graph.failJob(new Exception("Test Exception"), System.currentTimeMillis());

        scheduler.closeAsync().get();

        assertThat(checkpointCoordinator.isShutdown()).isTrue();
        assertThat(counterShutdownFuture).isCompletedWithValue(JobStatus.FAILED);
        assertThat(storeShutdownFuture).isCompletedWithValue(JobStatus.FAILED);
    }

    /** Tests that the checkpoint coordinator is shut down if the execution graph is suspended. */
    @Test
    void testClosingSchedulerShutsDownCheckpointCoordinatorOnSuspendedExecutionGraph()
            throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter =
                TestingCheckpointIDCounter.createStoreWithShutdownCheckAndNoStartAction(
                        counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store =
                TestingCompletedCheckpointStore
                        .createStoreWithShutdownCheckAndNoCompletedCheckpoints(storeShutdownFuture);

        final SchedulerBase scheduler = createSchedulerAndEnableCheckpointing(counter, store);
        final ExecutionGraph graph = scheduler.getExecutionGraph();
        final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

        assertThat(checkpointCoordinator).isNotNull();
        assertThat(checkpointCoordinator.isShutdown()).isFalse();

        graph.suspend(new Exception("Test Exception"));

        scheduler.closeAsync().get();

        assertThat(checkpointCoordinator.isShutdown()).isTrue();
        assertThat(counterShutdownFuture).isCompletedWithValue(JobStatus.SUSPENDED);
        assertThat(storeShutdownFuture).isCompletedWithValue(JobStatus.SUSPENDED);
    }

    /** Tests that the checkpoint coordinator is shut down if the execution graph is finished. */
    @Test
    void testClosingSchedulerShutsDownCheckpointCoordinatorOnFinishedExecutionGraph()
            throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter =
                TestingCheckpointIDCounter.createStoreWithShutdownCheckAndNoStartAction(
                        counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store =
                TestingCompletedCheckpointStore
                        .createStoreWithShutdownCheckAndNoCompletedCheckpoints(storeShutdownFuture);

        final SchedulerBase scheduler = createSchedulerAndEnableCheckpointing(counter, store);
        final ExecutionGraph graph = scheduler.getExecutionGraph();
        final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

        assertThat(checkpointCoordinator).isNotNull();
        assertThat(checkpointCoordinator.isShutdown()).isFalse();

        scheduler.startScheduling();

        for (ExecutionVertex executionVertex : graph.getAllExecutionVertices()) {
            final Execution currentExecutionAttempt = executionVertex.getCurrentExecutionAttempt();
            scheduler.updateTaskExecutionState(
                    new TaskExecutionState(
                            currentExecutionAttempt.getAttemptId(), ExecutionState.FINISHED));
        }

        assertThat(graph.getTerminationFuture()).isCompletedWithValue(JobStatus.FINISHED);

        scheduler.closeAsync().get();

        assertThat(checkpointCoordinator.isShutdown()).isTrue();
        assertThat(counterShutdownFuture).isCompletedWithValue(JobStatus.FINISHED);
        assertThat(storeShutdownFuture).isCompletedWithValue(JobStatus.FINISHED);
    }

    /** Tests that the checkpoint coordinator is shut down if the execution graph is suspended. */
    @Test
    void testClosingSchedulerSuspendsExecutionGraphAndShutsDownCheckpointCoordinator()
            throws Exception {
        final CompletableFuture<JobStatus> counterShutdownFuture = new CompletableFuture<>();
        CheckpointIDCounter counter =
                TestingCheckpointIDCounter.createStoreWithShutdownCheckAndNoStartAction(
                        counterShutdownFuture);

        final CompletableFuture<JobStatus> storeShutdownFuture = new CompletableFuture<>();
        CompletedCheckpointStore store =
                TestingCompletedCheckpointStore
                        .createStoreWithShutdownCheckAndNoCompletedCheckpoints(storeShutdownFuture);

        final SchedulerBase scheduler = createSchedulerAndEnableCheckpointing(counter, store);
        final ExecutionGraph graph = scheduler.getExecutionGraph();
        final CheckpointCoordinator checkpointCoordinator = graph.getCheckpointCoordinator();

        assertThat(checkpointCoordinator).isNotNull();
        assertThat(checkpointCoordinator.isShutdown()).isFalse();

        scheduler.closeAsync().get();

        assertThat(graph.getState()).isEqualTo(JobStatus.SUSPENDED);
        assertThat(checkpointCoordinator.isShutdown()).isTrue();
        assertThat(counterShutdownFuture).isCompletedWithValue(JobStatus.SUSPENDED);
        assertThat(storeShutdownFuture).isCompletedWithValue(JobStatus.SUSPENDED);
    }

    private DefaultScheduler createSchedulerAndEnableCheckpointing(
            CheckpointIDCounter counter, CompletedCheckpointStore store) throws Exception {
        final Time timeout = Time.days(1L);

        final JobVertex jobVertex = new JobVertex("MockVertex");
        jobVertex.setInvokableClass(AbstractInvokable.class);

        final CheckpointCoordinatorConfiguration chkConfig =
                CheckpointCoordinatorConfiguration.builder()
                        .setCheckpointInterval(100L)
                        .setCheckpointTimeout(100L)
                        .build();
        final JobCheckpointingSettings checkpointingSettings =
                new JobCheckpointingSettings(chkConfig, null);

        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .addJobVertex(jobVertex)
                        .setJobCheckpointingSettings(checkpointingSettings)
                        .build();

        return new DefaultSchedulerBuilder(
                        jobGraph,
                        ComponentMainThreadExecutorServiceAdapter.forMainThread(),
                        EXECUTOR_EXTENSION.getExecutor())
                .setCheckpointRecoveryFactory(new TestingCheckpointRecoveryFactory(store, counter))
                .setRpcTimeout(timeout)
                .build();
    }
}
