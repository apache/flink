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

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for deferred abort behavior in regional checkpoint mode. */
class RegionalCheckpointDeferredAbortTest {

    private static final String TASK_MANAGER_LOCATION_INFO = "Unknown location";

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @TempDir Path tmpFolder;

    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @BeforeEach
    void setUp() {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    @Test
    void testRegionalEnabledSingleDeclineDoesNotImmediatelyAbort() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();

        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .setRegionalCheckpointEnabled(true)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        // Trigger checkpoint
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();

        long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        PendingCheckpoint checkpoint =
                checkpointCoordinator.getPendingCheckpoints().get(checkpointId);

        // Decline from task 1 — should NOT immediately abort
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId,
                        new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED)),
                TASK_MANAGER_LOCATION_INFO);

        // Checkpoint should still be pending (not disposed) because task 2 hasn't responded
        assertThat(checkpoint.isDisposed()).isFalse();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();
        assertThat(checkpoint.hasDeclines()).isTrue();

        checkpointCoordinator.shutdown();
    }

    @Test
    void testRegionalEnabledAllTasksRespondTriggersEvaluation() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
        ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();

        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .setRegionalCheckpointEnabled(true)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        // Trigger checkpoint
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        PendingCheckpoint checkpoint =
                checkpointCoordinator.getPendingCheckpoints().get(checkpointId);

        // Decline from task 1
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId,
                        new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED)),
                TASK_MANAGER_LOCATION_INFO);

        // Still pending
        assertThat(checkpoint.isDisposed()).isFalse();

        // Acknowledge from task 2 — now all tasks have responded
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointId),
                TASK_MANAGER_LOCATION_INFO);

        // After all tasks responded, tryCompleteRegionalCheckpoint should be triggered.
        // The stub implementation aborts, so the checkpoint should now be disposed.
        assertThat(checkpoint.isDisposed()).isTrue();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();

        checkpointCoordinator.shutdown();
    }

    @Test
    void testRegionalDisabledDeclineImmediatelyAborts() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();

        // Regional checkpoint DISABLED (default)
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .setRegionalCheckpointEnabled(false)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        // Trigger checkpoint
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        PendingCheckpoint checkpoint =
                checkpointCoordinator.getPendingCheckpoints().get(checkpointId);

        // Decline from task 1 — should immediately abort
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId,
                        new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED)),
                TASK_MANAGER_LOCATION_INFO);

        assertThat(checkpoint.isDisposed()).isTrue();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();

        checkpointCoordinator.shutdown();
    }

    @Test
    void testSavepointDeclineImmediatelyAbortsRegardlessOfRegionalSetting() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();

        // Regional checkpoint ENABLED
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .setRegionalCheckpointEnabled(true)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        // Trigger a savepoint (not a regular checkpoint)
        final CompletableFuture<CompletedCheckpoint> savepointFuture =
                checkpointCoordinator.triggerSavepoint(
                        tmpFolder.toString(), SavepointFormatType.CANONICAL);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();

        long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        PendingCheckpoint checkpoint =
                checkpointCoordinator.getPendingCheckpoints().get(checkpointId);

        // Verify it's a savepoint
        assertThat(checkpoint.getProps().isSavepoint()).isTrue();

        // Decline from task 1 — should immediately abort even with regional enabled
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId,
                        new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED)),
                TASK_MANAGER_LOCATION_INFO);

        assertThat(checkpoint.isDisposed()).isTrue();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();

        checkpointCoordinator.shutdown();
    }

    @Test
    void testRegionalEnabledDeclineThenDeclineTriggersEvaluation() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
        ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();

        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .setRegionalCheckpointEnabled(true)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        // Trigger checkpoint
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        PendingCheckpoint checkpoint =
                checkpointCoordinator.getPendingCheckpoints().get(checkpointId);

        // Decline from task 1
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId,
                        new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED)),
                TASK_MANAGER_LOCATION_INFO);

        assertThat(checkpoint.isDisposed()).isFalse();

        // Decline from task 2 — now all tasks responded (both declined)
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        attemptID2,
                        checkpointId,
                        new CheckpointException(CheckpointFailureReason.CHECKPOINT_DECLINED)),
                TASK_MANAGER_LOCATION_INFO);

        // After all tasks responded, the stub triggers abort
        assertThat(checkpoint.isDisposed()).isTrue();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();

        checkpointCoordinator.shutdown();
    }
}
