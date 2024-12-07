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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.execution.RecoveryClaimMode;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.testutils.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.core.testutils.ScheduledTask;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.exceptions.RpcException;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.DiscardRecordedStateObject;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.PhysicalStateHandleID;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.SharedStateRegistryImpl;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestingStreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorageAccess;
import org.apache.flink.runtime.state.memory.NonPersistentMetadataCheckpointStorageLocation;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TernaryBoolean;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.util.function.TriFunctionWithException;

import org.apache.flink.shaded.guava32.com.google.common.collect.Iterables;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.verification.VerificationMode;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;

import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_ASYNC_EXCEPTION;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_EXPIRED;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.FINALIZE_CHECKPOINT_FAILURE;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.IO_EXCEPTION;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN;
import static org.apache.flink.runtime.checkpoint.CheckpointStoreUtil.INVALID_CHECKPOINT_ID;
import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createExecutionAttemptId;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the checkpoint coordinator. */
class CheckpointCoordinatorTest {

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testSharedStateNotDiscaredOnAbort() throws Exception {
        JobVertexID v1 = new JobVertexID(), v2 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(v1)
                        .addJobVertex(v2)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);
        coordinator.startCheckpointScheduler();

        CompletableFuture<CompletedCheckpoint> cpFuture = coordinator.triggerCheckpoint(true);
        manuallyTriggeredScheduledExecutor.triggerAll();
        cpFuture.getNow(null);

        TestingStreamStateHandle metaState = handle();
        TestingStreamStateHandle privateState = handle();
        TestingStreamStateHandle sharedState = handle();

        ackCheckpoint(1L, coordinator, v1, graph, metaState, privateState, sharedState);
        declineCheckpoint(1L, coordinator, v2, graph);

        assertThat(privateState.isDisposed()).isTrue();
        assertThat(metaState.isDisposed()).isTrue();
        assertThat(sharedState.isDisposed()).isFalse();

        cpFuture = coordinator.triggerCheckpoint(true);
        manuallyTriggeredScheduledExecutor.triggerAll();
        cpFuture.getNow(null);

        ackCheckpoint(2L, coordinator, v1, graph, handle(), handle(), handle());
        ackCheckpoint(2L, coordinator, v2, graph, handle(), handle(), handle());

        cpFuture.get();
        assertThat(sharedState.isDisposed()).isTrue();
    }

    @Test
    void testAbortedCheckpointStatsUpdatedAfterFailure() throws Exception {
        testReportStatsAfterFailure(
                1L,
                (coordinator, execution, metrics) -> {
                    coordinator.reportCheckpointMetrics(1L, execution.getAttemptId(), metrics);
                    return null;
                });
    }

    @Test
    void testCheckpointStatsUpdatedAfterFailure() throws Exception {
        testReportStatsAfterFailure(
                1L,
                (coordinator, execution, metrics) ->
                        coordinator.receiveAcknowledgeMessage(
                                new AcknowledgeCheckpoint(
                                        execution.getVertex().getJobId(),
                                        execution.getAttemptId(),
                                        1L,
                                        metrics,
                                        new TaskStateSnapshot()),
                                TASK_MANAGER_LOCATION_INFO));
    }

    private void testReportStatsAfterFailure(
            long checkpointId,
            TriFunctionWithException<
                            CheckpointCoordinator,
                            Execution,
                            CheckpointMetrics,
                            ?,
                            CheckpointException>
                    reportFn)
            throws Exception {

        JobVertexID decliningVertexID = new JobVertexID();
        JobVertexID lateReportVertexID = new JobVertexID();

        ExecutionGraph executionGraph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(decliningVertexID)
                        .addJobVertex(lateReportVertexID)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex decliningVertex =
                executionGraph.getJobVertex(decliningVertexID).getTaskVertices()[0];
        ExecutionVertex lateReportVertex =
                executionGraph.getJobVertex(lateReportVertexID).getTaskVertices()[0];
        CheckpointStatsTracker statsTracker =
                new DefaultCheckpointStatsTracker(
                        Integer.MAX_VALUE,
                        UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCheckpointStatsTracker(statsTracker)
                        .build(executionGraph);

        CompletableFuture<CompletedCheckpoint> result = coordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        checkState(
                coordinator.getNumberOfPendingCheckpoints() == 1,
                "wrong number of pending checkpoints: %s",
                coordinator.getNumberOfPendingCheckpoints());
        if (result.isDone()) {
            result.get();
        }

        coordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        executionGraph.getJobID(),
                        decliningVertex.getCurrentExecutionAttempt().getAttemptId(),
                        checkpointId,
                        new CheckpointException(CHECKPOINT_DECLINED)),
                "test");

        CheckpointMetrics lateReportedMetrics =
                new CheckpointMetricsBuilder()
                        .setTotalBytesPersisted(18)
                        .setBytesPersistedOfThisCheckpoint(18)
                        .setBytesProcessedDuringAlignment(19)
                        .setAsyncDurationMillis(20)
                        .setAlignmentDurationNanos(123 * 1_000_000)
                        .setCheckpointStartDelayNanos(567 * 1_000_000)
                        .build();

        reportFn.apply(
                coordinator, lateReportVertex.getCurrentExecutionAttempt(), lateReportedMetrics);

        assertStatsEqual(
                checkpointId,
                lateReportVertex.getJobvertexId(),
                0,
                lateReportedMetrics,
                statsTracker.createSnapshot().getHistory().getCheckpointById(checkpointId));
    }

    private boolean hasNoSubState(OperatorState s) {
        return s.getNumberCollectedStates() == 0;
    }

    private void assertStatsEqual(
            long checkpointId,
            JobVertexID jobVertexID,
            int subtasIdx,
            CheckpointMetrics expected,
            AbstractCheckpointStats actual) {
        assertThat(actual.getCheckpointId()).isEqualTo(checkpointId);
        assertThat(actual.getStatus()).isEqualTo(CheckpointStatsStatus.FAILED);
        assertThat(actual.getNumberOfAcknowledgedSubtasks()).isZero();
        assertStatsMetrics(jobVertexID, subtasIdx, expected, actual);
    }

    public static void assertStatsMetrics(
            JobVertexID jobVertexID,
            int subtasIdx,
            CheckpointMetrics expected,
            AbstractCheckpointStats actual) {
        assertThat(actual.getStateSize()).isEqualTo(expected.getTotalBytesPersisted());
        SubtaskStateStats taskStats =
                actual.getAllTaskStateStats().stream()
                        .filter(s -> s.getJobVertexId().equals(jobVertexID))
                        .findAny()
                        .get()
                        .getSubtaskStats()[subtasIdx];
        assertThat(taskStats.getAlignmentDuration())
                .isEqualTo(expected.getAlignmentDurationNanos() / 1_000_000);
        assertThat(taskStats.getUnalignedCheckpoint()).isEqualTo(expected.getUnalignedCheckpoint());
        assertThat(taskStats.getAsyncCheckpointDuration())
                .isEqualTo(expected.getAsyncDurationMillis());
        assertThat(taskStats.getAlignmentDuration())
                .isEqualTo(expected.getAlignmentDurationNanos() / 1_000_000);
        assertThat(taskStats.getCheckpointStartDelay())
                .isEqualTo(expected.getCheckpointStartDelayNanos() / 1_000_000);
    }

    private static final String TASK_MANAGER_LOCATION_INFO = "Unknown location";

    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @TempDir private java.nio.file.Path tmpFolder;

    @BeforeEach
    void setUp() {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    @Test
    void testScheduleTriggerRequestDuringShutdown() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        CheckpointCoordinator coordinator =
                getCheckpointCoordinator(new ScheduledExecutorServiceAdapter(executor));
        coordinator.shutdown();
        executor.shutdownNow();
        coordinator.scheduleTriggerRequest(); // shouldn't fail
    }

    @Test
    void testMinCheckpointPause() throws Exception {
        // will use a different thread to allow checkpoint triggering before exiting from
        // receiveAcknowledgeMessage
        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        CheckpointCoordinator coordinator = null;
        try {
            int pause = 1000;
            JobVertexID jobVertexId = new JobVertexID();
            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexId)
                            .setMainThreadExecutor(
                                    ComponentMainThreadExecutorServiceAdapter
                                            .forSingleThreadExecutor(
                                                    new DirectScheduledExecutorService()))
                            .build(EXECUTOR_RESOURCE.getExecutor());

            ExecutionVertex vertex = graph.getJobVertex(jobVertexId).getTaskVertices()[0];
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();

            coordinator =
                    new CheckpointCoordinatorBuilder()
                            .setTimer(new ScheduledExecutorServiceAdapter(executorService))
                            .setCheckpointCoordinatorConfiguration(
                                    CheckpointCoordinatorConfiguration.builder()
                                            .setCheckpointInterval(pause)
                                            .setCheckpointTimeout(Long.MAX_VALUE)
                                            .setMaxConcurrentCheckpoints(1)
                                            .setMinPauseBetweenCheckpoints(pause)
                                            .build())
                            .build(graph);
            coordinator.startCheckpointScheduler();

            coordinator.triggerCheckpoint(
                    true); // trigger, execute, and later complete by receiveAcknowledgeMessage
            coordinator.triggerCheckpoint(
                    true); // enqueue and later see if it gets executed in the middle of
            // receiveAcknowledgeMessage
            while (coordinator.getPendingCheckpoints().values().stream()
                    .noneMatch(pc -> pc.getCheckpointStorageLocation() != null)) {
                // wait for at least 1 request to be fully processed
                // explicitly check for the checkpoint storage location to make sure
                // that the checkpoint was actually triggered
                // if the checkpoint wasn't triggered yet the acknowledge below
                // will fail with an error
                Thread.sleep(10);
            }
            coordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptId, 1L),
                    TASK_MANAGER_LOCATION_INFO);
            Thread.sleep(pause / 2);
            assertThat(coordinator.getNumberOfPendingCheckpoints()).isZero();
            // make sure that the 2nd request is eventually processed
            while (coordinator.getNumberOfPendingCheckpoints() == 0) {
                Thread.sleep(1);
            }
        } finally {
            if (coordinator != null) {
                coordinator.shutdown();
            }
            executorService.shutdownNow();
        }
    }

    @Test
    void testCheckpointAbortsIfTriggerTasksAreNotExecuted() throws Exception {
        // set up the coordinator and validate the initial state
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .addJobVertex(new JobVertexID(), false)
                        .setTransitToRunning(false)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(graph);

        // nothing should be happening
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        // trigger the first checkpoint. this should not succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointFuture).isCompletedExceptionally();

        // still, nothing should be happening
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        checkpointCoordinator.shutdown();
    }

    @Test
    void testCheckpointAbortsIfTriggerTasksAreFinished() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2, false)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(graph);
        Arrays.stream(graph.getJobVertex(jobVertexID1).getTaskVertices())
                .forEach(task -> task.getCurrentExecutionAttempt().markFinished());

        // nothing should be happening
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        // trigger the first checkpoint. this should not succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointFuture).isCompletedExceptionally();

        // still, nothing should be happening
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        checkpointCoordinator.shutdown();
    }

    @Test
    void testCheckpointTriggeredAfterSomeTasksFinishedIfAllowed() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1, 3, 256)
                        .addJobVertex(jobVertexID2, 3, 256)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionJobVertex jobVertex1 = graph.getJobVertex(jobVertexID1);
        ExecutionJobVertex jobVertex2 = graph.getJobVertex(jobVertexID2);

        jobVertex1.getTaskVertices()[0].getCurrentExecutionAttempt().markFinished();
        jobVertex1.getTaskVertices()[1].getCurrentExecutionAttempt().markFinished();
        jobVertex2.getTaskVertices()[1].getCurrentExecutionAttempt().markFinished();

        CheckpointStatsTracker statsTracker =
                new DefaultCheckpointStatsTracker(
                        Integer.MAX_VALUE,
                        UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setAllowCheckpointsAfterTasksFinished(true)
                        .setCheckpointStatsTracker(statsTracker)
                        .build(graph);

        // nothing should be happening
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        // trigger the first checkpoint. this will not fail because we allow checkpointing even with
        // finished tasks
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointFuture.isDone()).isFalse();
        assertThat(checkpointFuture.isCompletedExceptionally()).isFalse();

        // Triggering should succeed
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();
        PendingCheckpoint pendingCheckpoint =
                checkpointCoordinator.getPendingCheckpoints().values().iterator().next();
        AbstractCheckpointStats checkpointStats =
                statsTracker
                        .createSnapshot()
                        .getHistory()
                        .getCheckpointById(pendingCheckpoint.getCheckpointID());
        assertThat(checkpointStats.getNumberOfAcknowledgedSubtasks()).isEqualTo(3);
        for (ExecutionVertex task :
                Arrays.asList(
                        jobVertex1.getTaskVertices()[0],
                        jobVertex1.getTaskVertices()[1],
                        jobVertex2.getTaskVertices()[1])) {

            // those tasks that are already finished are automatically marked as acknowledged
            assertThat(
                            checkpointStats.getTaskStateStats(task.getJobvertexId())
                                    .getSubtaskStats()[task.getParallelSubtaskIndex()])
                    .isNotNull();
        }
    }

    @Test
    void testTasksFinishDuringTriggering() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .setTransitToRunning(false)
                        .addJobVertex(jobVertexID1, 1, 256)
                        .addJobVertex(jobVertexID2, 1, 256)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionJobVertex jobVertex1 = graph.getJobVertex(jobVertexID1);
        ExecutionVertex taskVertex = jobVertex1.getTaskVertices()[0];
        ExecutionJobVertex jobVertex2 = graph.getJobVertex(jobVertexID2);
        ExecutionVertex taskVertex2 = jobVertex2.getTaskVertices()[0];

        AtomicBoolean checkpointAborted = new AtomicBoolean(false);
        LogicalSlot slot1 =
                new TestingLogicalSlotBuilder()
                        .setTaskManagerGateway(
                                new SimpleAckingTaskManagerGateway() {
                                    @Override
                                    public CompletableFuture<Acknowledge> triggerCheckpoint(
                                            ExecutionAttemptID executionAttemptID,
                                            JobID jobId,
                                            long checkpointId,
                                            long timestamp,
                                            CheckpointOptions checkpointOptions) {
                                        taskVertex.getCurrentExecutionAttempt().markFinished();
                                        return FutureUtils.completedExceptionally(
                                                new RpcException(""));
                                    }
                                })
                        .createTestingLogicalSlot();
        LogicalSlot slot2 =
                new TestingLogicalSlotBuilder()
                        .setTaskManagerGateway(
                                new SimpleAckingTaskManagerGateway() {
                                    @Override
                                    public void notifyCheckpointAborted(
                                            ExecutionAttemptID executionAttemptID,
                                            JobID jobId,
                                            long checkpointId,
                                            long latestCompletedCheckpointId,
                                            long timestamp) {
                                        checkpointAborted.set(true);
                                    }
                                })
                        .createTestingLogicalSlot();
        ExecutionGraphTestUtils.setVertexResource(taskVertex, slot1);
        taskVertex.getCurrentExecutionAttempt().transitionState(ExecutionState.RUNNING);
        ExecutionGraphTestUtils.setVertexResource(taskVertex2, slot2);
        taskVertex2.getCurrentExecutionAttempt().transitionState(ExecutionState.RUNNING);

        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setAllowCheckpointsAfterTasksFinished(true)
                        .build(graph);

        // nothing should be happening
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        // trigger the first checkpoint. this will not fail because we allow checkpointing even with
        // finished tasks
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointFuture).isCompletedExceptionally();
        assertThat(checkpointAborted.get()).isTrue();
    }

    @Test
    void testTriggerAndDeclineCheckpointThenFailureManagerThrowsException() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];
        final ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
        final ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();

        final String errorMsg = "Exceeded checkpoint failure tolerance number!";

        CheckpointFailureManager checkpointFailureManager = getCheckpointFailureManager(errorMsg);

        // set up the coordinator
        CheckpointCoordinator checkpointCoordinator =
                getCheckpointCoordinator(graph, checkpointFailureManager);

        try {
            // trigger the checkpoint. this should succeed
            final CompletableFuture<CompletedCheckpoint> checkPointFuture =
                    checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            FutureUtils.throwIfCompletedExceptionally(checkPointFuture);

            long checkpointId =
                    checkpointCoordinator
                            .getPendingCheckpoints()
                            .entrySet()
                            .iterator()
                            .next()
                            .getKey();
            PendingCheckpoint checkpoint =
                    checkpointCoordinator.getPendingCheckpoints().get(checkpointId);

            // acknowledge from one of the tasks
            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointId),
                    TASK_MANAGER_LOCATION_INFO);
            assertThat(checkpoint.isDisposed()).isFalse();
            assertThat(checkpoint.areTasksFullyAcknowledged()).isFalse();

            // decline checkpoint from the other task
            checkpointCoordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            graph.getJobID(),
                            attemptID1,
                            checkpointId,
                            new CheckpointException(CHECKPOINT_DECLINED)),
                    TASK_MANAGER_LOCATION_INFO);

            fail("Test failed.");
        } catch (Exception e) {
            ExceptionUtils.assertThrowableWithMessage(e, errorMsg);
        } finally {
            checkpointCoordinator.shutdown();
        }
    }

    @Test
    void testIOExceptionCheckpointExceedsTolerableFailureNumber() throws Exception {
        // create some mock Execution vertices that receive the checkpoint trigger messages
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .addJobVertex(new JobVertexID())
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final String expectedErrorMessage = "Expected Error Message";
        CheckpointFailureManager checkpointFailureManager =
                getCheckpointFailureManager(expectedErrorMessage);
        CheckpointCoordinator checkpointCoordinator =
                getCheckpointCoordinator(graph, checkpointFailureManager);

        try {
            checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();

            checkpointCoordinator.abortPendingCheckpoints(new CheckpointException(IO_EXCEPTION));

            fail("Test failed.");
        } catch (Exception e) {
            ExceptionUtils.assertThrowableWithMessage(e, expectedErrorMessage);
        } finally {
            checkpointCoordinator.shutdown();
        }
    }

    @Test
    void testIOExceptionForPeriodicSchedulingWithInactiveTasks() throws Exception {
        CheckpointCoordinator checkpointCoordinator =
                setupCheckpointCoordinatorWithInactiveTasks(new IOExceptionCheckpointStorage());

        final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                checkpointCoordinator.triggerCheckpoint(
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        null,
                        true);
        manuallyTriggeredScheduledExecutor.triggerAll();

        try {
            onCompletionPromise.get();
            fail("should not trigger periodic checkpoint after IOException occurred.");
        } catch (Exception e) {
            final Optional<CheckpointException> checkpointExceptionOptional =
                    ExceptionUtils.findThrowable(e, CheckpointException.class);
            if (!checkpointExceptionOptional.isPresent()
                    || checkpointExceptionOptional.get().getCheckpointFailureReason()
                            != IO_EXCEPTION) {
                throw e;
            }
        }
    }

    /** Tests that do not trigger checkpoint when IOException occurred. */
    @Test
    void testTriggerCheckpointAfterCheckpointStorageIOException() throws Exception {
        // given: Checkpoint coordinator which fails on initializeCheckpointLocation.
        TestFailJobCallback failureCallback = new TestFailJobCallback();
        CheckpointStatsTracker statsTracker =
                new DefaultCheckpointStatsTracker(
                        Integer.MAX_VALUE,
                        UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointStatsTracker(statsTracker)
                        .setFailureManager(new CheckpointFailureManager(0, failureCallback))
                        .setCheckpointStorage(new IOExceptionCheckpointStorage())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        // when: The checkpoint is triggered.
        testTriggerCheckpoint(checkpointCoordinator, IO_EXCEPTION);

        // then: Failure manager should fail the job.
        assertThat(failureCallback.getInvokeCounter()).isOne();

        // then: Should created PendingCheckpoint
        assertThat(statsTracker.getPendingCheckpointStats(1)).isNotNull();
    }

    @Test
    void testCheckpointAbortsIfTriggerTasksAreFinishedAndIOException() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2, false)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        // set up the coordinator
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointStorage(new IOExceptionCheckpointStorage())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        Arrays.stream(graph.getJobVertex(jobVertexID1).getTaskVertices())
                .forEach(task -> task.getCurrentExecutionAttempt().markFinished());

        // nothing should be happening
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        checkpointCoordinator.startCheckpointScheduler();
        // trigger the first checkpoint. this should not succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertThat(checkpointFuture).isCompletedExceptionally();

        // still, nothing should be happening
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        checkpointCoordinator.shutdown();
    }

    @Test
    void testExpiredCheckpointExceedsTolerableFailureNumber() throws Exception {
        // create some mock Execution vertices that receive the checkpoint trigger messages
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .addJobVertex(new JobVertexID())
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final String errorMsg = "Exceeded checkpoint failure tolerance number!";
        CheckpointFailureManager checkpointFailureManager = getCheckpointFailureManager(errorMsg);
        CheckpointCoordinator checkpointCoordinator =
                getCheckpointCoordinator(graph, checkpointFailureManager);

        try {
            checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();

            checkpointCoordinator.abortPendingCheckpoints(
                    new CheckpointException(CHECKPOINT_EXPIRED));

            fail("Test failed.");
        } catch (Exception e) {
            ExceptionUtils.assertThrowableWithMessage(e, errorMsg);
        } finally {
            checkpointCoordinator.shutdown();
        }
    }

    @Test
    void testTriggerAndDeclineSyncCheckpointFailureSimple() throws Exception {
        testTriggerAndDeclineCheckpointSimple(CHECKPOINT_DECLINED);
    }

    @Test
    void testTriggerAndDeclineAsyncCheckpointFailureSimple() throws Exception {
        testTriggerAndDeclineCheckpointSimple(CHECKPOINT_ASYNC_EXCEPTION);
    }

    /**
     * This test triggers a checkpoint and then sends a decline checkpoint message from one of the
     * tasks. The expected behaviour is that said checkpoint is discarded and a new checkpoint is
     * triggered.
     */
    private void testTriggerAndDeclineCheckpointSimple(
            CheckpointFailureReason checkpointFailureReason) throws Exception {
        final CheckpointException checkpointException =
                new CheckpointException(checkpointFailureReason);

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

        TestFailJobCallback failJobCallback = new TestFailJobCallback();
        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setAlignedCheckpointTimeout(Long.MAX_VALUE)
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCheckpointFailureManager(
                                new CheckpointFailureManager(0, failJobCallback))
                        .build(graph);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        // trigger the first checkpoint. this should succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        // validate that we have a pending checkpoint
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        // we have one task scheduled that will cancel after timeout
        assertThat(manuallyTriggeredScheduledExecutor.getActiveScheduledTasks()).hasSize(1);

        long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        PendingCheckpoint checkpoint =
                checkpointCoordinator.getPendingCheckpoints().get(checkpointId);

        assertThat(checkpoint).isNotNull();
        assertThat(checkpoint.getCheckpointID()).isEqualTo(checkpointId);
        assertThat(checkpoint.getJobId()).isEqualTo(graph.getJobID());
        assertThat(checkpoint.getNumberOfNonAcknowledgedTasks()).isEqualTo(2);
        assertThat(checkpoint.getNumberOfAcknowledgedTasks()).isZero();
        assertThat(checkpoint.getOperatorStates().size()).isZero();
        assertThat(checkpoint.isDisposed()).isFalse();
        assertThat(checkpoint.areTasksFullyAcknowledged()).isFalse();

        // check that the vertices received the trigger checkpoint message
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            CheckpointCoordinatorTestingUtils.TriggeredCheckpoint triggeredCheckpoint =
                    gateway.getOnlyTriggeredCheckpoint(
                            vertex.getCurrentExecutionAttempt().getAttemptId());
            assertThat(triggeredCheckpoint.checkpointId).isEqualTo(checkpointId);
            assertThat(triggeredCheckpoint.timestamp)
                    .isEqualTo(checkpoint.getCheckpointTimestamp());
            assertThat(triggeredCheckpoint.checkpointOptions)
                    .isEqualTo(CheckpointOptions.forCheckpointWithDefaultLocation());
        }

        // acknowledge from one of the tasks
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointId),
                "Unknown location");
        assertThat(checkpoint.getNumberOfAcknowledgedTasks()).isOne();
        assertThat(checkpoint.getNumberOfNonAcknowledgedTasks()).isOne();
        assertThat(checkpoint.isDisposed()).isFalse();
        assertThat(checkpoint.areTasksFullyAcknowledged()).isFalse();

        // acknowledge the same task again (should not matter)
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointId),
                "Unknown location");
        assertThat(checkpoint.isDisposed()).isFalse();
        assertThat(checkpoint.areTasksFullyAcknowledged()).isFalse();

        // decline checkpoint from the other task, this should cancel the checkpoint
        // and trigger a new one
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(), attemptID1, checkpointId, checkpointException),
                TASK_MANAGER_LOCATION_INFO);
        assertThat(checkpoint.isDisposed()).isTrue();

        // the canceler is also removed
        assertThat(manuallyTriggeredScheduledExecutor.getActiveScheduledTasks().size()).isZero();

        // validate that we have no new pending checkpoint
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        // decline again, nothing should happen
        // decline from the other task, nothing should happen
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(), attemptID1, checkpointId, checkpointException),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(), attemptID2, checkpointId, checkpointException),
                TASK_MANAGER_LOCATION_INFO);
        assertThat(checkpoint.isDisposed()).isTrue();
        assertThat(failJobCallback.getInvokeCounter()).isOne();

        checkpointCoordinator.shutdown();
    }

    /**
     * This test triggers two checkpoints and then sends a decline message from one of the tasks for
     * the first checkpoint. This should discard the first checkpoint while not triggering a new
     * checkpoint because a later checkpoint is already in progress.
     */
    @Test
    void testTriggerAndDeclineCheckpointComplex() throws Exception {
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
        CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(graph);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();
        assertThat(manuallyTriggeredScheduledExecutor.getActiveScheduledTasks()).isEmpty();

        // trigger the first checkpoint. this should succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture1 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture1);

        // trigger second checkpoint, should also succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture2 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture2);

        // validate that we have a pending checkpoint
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isEqualTo(2);
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();
        assertThat(manuallyTriggeredScheduledExecutor.getActiveScheduledTasks()).hasSize(2);

        Iterator<Map.Entry<Long, PendingCheckpoint>> it =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator();
        long checkpoint1Id = it.next().getKey();
        long checkpoint2Id = it.next().getKey();
        PendingCheckpoint checkpoint1 =
                checkpointCoordinator.getPendingCheckpoints().get(checkpoint1Id);
        PendingCheckpoint checkpoint2 =
                checkpointCoordinator.getPendingCheckpoints().get(checkpoint2Id);

        assertThat(checkpoint1).isNotNull();
        assertThat(checkpoint1.getCheckpointID()).isEqualTo(checkpoint1Id);
        assertThat(checkpoint1.getJobId()).isEqualTo(graph.getJobID());
        assertThat(checkpoint1.getNumberOfNonAcknowledgedTasks()).isEqualTo(2);
        assertThat(checkpoint1.getNumberOfAcknowledgedTasks()).isZero();
        assertThat(checkpoint1.getOperatorStates()).isEmpty();
        assertThat(checkpoint1.isDisposed()).isFalse();
        assertThat(checkpoint1.areTasksFullyAcknowledged()).isFalse();

        assertThat(checkpoint2).isNotNull();
        assertThat(checkpoint2.getCheckpointID()).isEqualTo(checkpoint2Id);
        assertThat(checkpoint2.getJobId()).isEqualTo(graph.getJobID());
        assertThat(checkpoint2.getNumberOfNonAcknowledgedTasks()).isEqualTo(2);
        assertThat(checkpoint2.getNumberOfAcknowledgedTasks()).isZero();
        assertThat(checkpoint2.getOperatorStates()).isEmpty();
        assertThat(checkpoint2.isDisposed()).isFalse();
        assertThat(checkpoint2.areTasksFullyAcknowledged()).isFalse();

        // check that the vertices received the trigger checkpoint message
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            List<CheckpointCoordinatorTestingUtils.TriggeredCheckpoint> triggeredCheckpoints =
                    gateway.getTriggeredCheckpoints(
                            vertex.getCurrentExecutionAttempt().getAttemptId());
            assertThat(triggeredCheckpoints).hasSize(2);
            assertThat(triggeredCheckpoints.get(0).checkpointId).isEqualTo(checkpoint1Id);
            assertThat(triggeredCheckpoints.get(1).checkpointId).isEqualTo(checkpoint2Id);
        }

        // decline checkpoint from one of the tasks, this should cancel the checkpoint
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpoint1Id,
                        new CheckpointException(CHECKPOINT_DECLINED)),
                TASK_MANAGER_LOCATION_INFO);
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            assertThat(
                            gateway.getOnlyNotifiedAbortedCheckpoint(
                                            vertex.getCurrentExecutionAttempt().getAttemptId())
                                    .checkpointId)
                    .isEqualTo(checkpoint1Id);
        }

        assertThat(checkpoint1.isDisposed()).isTrue();

        // validate that we have only one pending checkpoint left
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();
        assertThat(manuallyTriggeredScheduledExecutor.getActiveScheduledTasks()).hasSize(1);

        // validate that it is the same second checkpoint from earlier
        long checkpointIdNew =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        PendingCheckpoint checkpointNew =
                checkpointCoordinator.getPendingCheckpoints().get(checkpointIdNew);
        assertThat(checkpointIdNew).isEqualTo(checkpoint2Id);

        assertThat(checkpointNew).isNotNull();
        assertThat(checkpointNew.getCheckpointID()).isEqualTo(checkpointIdNew);
        assertThat(checkpointNew.getJobId()).isEqualTo(graph.getJobID());
        assertThat(checkpointNew.getNumberOfNonAcknowledgedTasks()).isEqualTo(2);
        assertThat(checkpointNew.getNumberOfAcknowledgedTasks()).isZero();
        assertThat(checkpointNew.getOperatorStates()).isEmpty();
        assertThat(checkpointNew.isDisposed()).isFalse();
        assertThat(checkpointNew.areTasksFullyAcknowledged()).isFalse();
        assertThat(checkpointNew.getCheckpointID()).isNotEqualTo(checkpoint1.getCheckpointID());

        // decline again, nothing should happen
        // decline from the other task, nothing should happen
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpoint1Id,
                        new CheckpointException(CHECKPOINT_DECLINED)),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        attemptID2,
                        checkpoint1Id,
                        new CheckpointException(CHECKPOINT_DECLINED)),
                TASK_MANAGER_LOCATION_INFO);
        assertThat(checkpoint1.isDisposed()).isTrue();

        // will not notify abort message again
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            assertThat(
                            gateway.getNotifiedAbortedCheckpoints(
                                    vertex.getCurrentExecutionAttempt().getAttemptId()))
                    .hasSize(1);
        }

        checkpointCoordinator.shutdown();
    }

    @Test
    void testTriggerAndConfirmSimpleCheckpoint() throws Exception {
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
        CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(graph);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();
        assertThat(manuallyTriggeredScheduledExecutor.getActiveScheduledTasks()).isEmpty();

        // trigger the first checkpoint. this should succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        // validate that we have a pending checkpoint
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();
        assertThat(manuallyTriggeredScheduledExecutor.getActiveScheduledTasks()).hasSize(1);

        long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        PendingCheckpoint checkpoint =
                checkpointCoordinator.getPendingCheckpoints().get(checkpointId);

        assertThat(checkpoint).isNotNull();
        assertThat(checkpoint.getCheckpointID()).isEqualTo(checkpointId);
        assertThat(checkpoint.getJobId()).isEqualTo(graph.getJobID());
        assertThat(checkpoint.getNumberOfNonAcknowledgedTasks()).isEqualTo(2);
        assertThat(checkpoint.getNumberOfAcknowledgedTasks()).isZero();
        assertThat(checkpoint.getOperatorStates()).isEmpty();
        assertThat(checkpoint.isDisposed()).isFalse();
        assertThat(checkpoint.areTasksFullyAcknowledged()).isFalse();

        // check that the vertices received the trigger checkpoint message
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointId);
        }

        OperatorID opID1 = vertex1.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();
        OperatorID opID2 = vertex2.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();
        OperatorSubtaskState subtaskState1 = mock(OperatorSubtaskState.class);
        OperatorSubtaskState subtaskState2 = mock(OperatorSubtaskState.class);
        TaskStateSnapshot taskOperatorSubtaskStates1 =
                new TaskStateSnapshot(singletonMap(opID1, subtaskState1));
        TaskStateSnapshot taskOperatorSubtaskStates2 =
                new TaskStateSnapshot(singletonMap(opID2, subtaskState2));

        // acknowledge from one of the tasks
        AcknowledgeCheckpoint acknowledgeCheckpoint1 =
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID2,
                        checkpointId,
                        new CheckpointMetrics(),
                        taskOperatorSubtaskStates2);
        checkpointCoordinator.receiveAcknowledgeMessage(
                acknowledgeCheckpoint1, TASK_MANAGER_LOCATION_INFO);
        assertThat(checkpoint.getNumberOfAcknowledgedTasks()).isOne();
        assertThat(checkpoint.getNumberOfNonAcknowledgedTasks()).isOne();
        assertThat(checkpoint.isDisposed()).isFalse();
        assertThat(checkpoint.areTasksFullyAcknowledged()).isFalse();
        verify(subtaskState2, times(1))
                .registerSharedStates(any(SharedStateRegistry.class), eq(checkpointId));

        // acknowledge the same task again (should not matter)
        checkpointCoordinator.receiveAcknowledgeMessage(
                acknowledgeCheckpoint1, TASK_MANAGER_LOCATION_INFO);
        assertThat(checkpoint.isDisposed()).isFalse();
        assertThat(checkpoint.areTasksFullyAcknowledged()).isFalse();
        verify(subtaskState2, times(2))
                .registerSharedStates(any(SharedStateRegistry.class), eq(checkpointId));

        // acknowledge the other task.
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId,
                        new CheckpointMetrics(),
                        taskOperatorSubtaskStates1),
                TASK_MANAGER_LOCATION_INFO);

        // the checkpoint is internally converted to a successful checkpoint and the
        // pending checkpoint object is disposed
        assertThat(checkpoint.isDisposed()).isTrue();

        // the now we should have a completed checkpoint
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isOne();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();

        // the canceler should be removed now
        assertThat(manuallyTriggeredScheduledExecutor.getActiveScheduledTasks()).isEmpty();

        // validate that the subtasks states have registered their shared states.
        {
            verify(subtaskState1, times(1))
                    .registerSharedStates(any(SharedStateRegistry.class), eq(checkpointId));
            verify(subtaskState2, times(2))
                    .registerSharedStates(any(SharedStateRegistry.class), eq(checkpointId));
        }

        // validate that the relevant tasks got a confirmation message
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getOnlyNotifiedCompletedCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointId);
        }

        CompletedCheckpoint success = checkpointCoordinator.getSuccessfulCheckpoints().get(0);
        assertThat(success.getJobId()).isEqualTo(graph.getJobID());
        assertThat(success.getCheckpointID()).isEqualTo(checkpoint.getCheckpointID());
        assertThat(success.getOperatorStates()).hasSize(2);

        // ---------------
        // trigger another checkpoint and see that this one replaces the other checkpoint
        // ---------------
        gateway.resetCount();
        checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        long checkpointIdNew =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, checkpointIdNew),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointIdNew),
                TASK_MANAGER_LOCATION_INFO);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isOne();
        assertThat(manuallyTriggeredScheduledExecutor.getActiveScheduledTasks()).isEmpty();

        CompletedCheckpoint successNew = checkpointCoordinator.getSuccessfulCheckpoints().get(0);
        assertThat(successNew.getJobId()).isEqualTo(graph.getJobID());
        assertThat(successNew.getCheckpointID()).isEqualTo(checkpointIdNew);
        assertThat(successNew.getOperatorStates()).hasSize(2);
        assertThat(successNew.getOperatorStates().values().stream().allMatch(this::hasNoSubState))
                .isTrue();

        // validate that the relevant tasks got a confirmation message
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointIdNew);
            assertThat(gateway.getOnlyNotifiedCompletedCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointIdNew);
        }

        checkpointCoordinator.shutdown();
    }

    @Test
    void testMultipleConcurrentCheckpoints() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();
        JobVertexID jobVertexID3 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .addJobVertex(jobVertexID3, false)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];
        ExecutionVertex vertex3 = graph.getJobVertex(jobVertexID3).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
        ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();
        ExecutionAttemptID attemptID3 = vertex3.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .build())
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        // trigger the first checkpoint. this should succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture1 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture1);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        PendingCheckpoint pending1 =
                checkpointCoordinator.getPendingCheckpoints().values().iterator().next();
        long checkpointId1 = pending1.getCheckpointID();

        // trigger messages should have been sent
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointId1);
        }

        // acknowledge one of the three tasks
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointId1),
                TASK_MANAGER_LOCATION_INFO);

        // start the second checkpoint
        // trigger the first checkpoint. this should succeed
        gateway.resetCount();
        final CompletableFuture<CompletedCheckpoint> checkpointFuture2 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture2);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isEqualTo(2);
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        PendingCheckpoint pending2;
        {
            Iterator<PendingCheckpoint> all =
                    checkpointCoordinator.getPendingCheckpoints().values().iterator();
            PendingCheckpoint cc1 = all.next();
            PendingCheckpoint cc2 = all.next();
            pending2 = pending1 == cc1 ? cc2 : cc1;
        }
        long checkpointId2 = pending2.getCheckpointID();

        // trigger messages should have been sent
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointId2);
        }

        // we acknowledge the remaining two tasks from the first
        // checkpoint and two tasks from the second checkpoint
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID3, checkpointId1),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, checkpointId2),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, checkpointId1),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointId2),
                TASK_MANAGER_LOCATION_INFO);

        // now, the first checkpoint should be confirmed
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isOne();
        assertThat(pending1.isDisposed()).isTrue();

        // the first confirm message should be out
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2, vertex3)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getOnlyNotifiedCompletedCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointId1);
        }

        // send the last remaining ack for the second checkpoint
        gateway.resetCount();
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID3, checkpointId2),
                TASK_MANAGER_LOCATION_INFO);

        // now, the second checkpoint should be confirmed
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isEqualTo(2);
        assertThat(pending2.isDisposed()).isTrue();

        // the second commit message should be out
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2, vertex3)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getOnlyNotifiedCompletedCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointId2);
        }

        // validate the committed checkpoints
        List<CompletedCheckpoint> scs = checkpointCoordinator.getSuccessfulCheckpoints();

        CompletedCheckpoint sc1 = scs.get(0);
        assertThat(sc1.getCheckpointID()).isEqualTo(checkpointId1);
        assertThat(sc1.getJobId()).isEqualTo(graph.getJobID());
        assertThat(sc1.getOperatorStates()).hasSize(3);
        assertThat(sc1.getOperatorStates().values()).allMatch(this::hasNoSubState);

        CompletedCheckpoint sc2 = scs.get(1);
        assertThat(sc2.getCheckpointID()).isEqualTo(checkpointId2);
        assertThat(sc2.getJobId()).isEqualTo(graph.getJobID());
        assertThat(sc2.getOperatorStates()).hasSize(3);
        assertThat(sc2.getOperatorStates().values()).allMatch(this::hasNoSubState);

        checkpointCoordinator.shutdown();
    }

    @Test
    void testSuccessfulCheckpointSubsumesUnsuccessful() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();
        JobVertexID jobVertexID3 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .addJobVertex(jobVertexID3, false)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];
        ExecutionVertex vertex3 = graph.getJobVertex(jobVertexID3).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
        ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();
        ExecutionAttemptID attemptID3 = vertex3.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        final StandaloneCompletedCheckpointStore completedCheckpointStore =
                new StandaloneCompletedCheckpointStore(10);
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .build())
                        .setCompletedCheckpointStore(completedCheckpointStore)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        // trigger the first checkpoint. this should succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture1 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture1);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        PendingCheckpoint pending1 =
                checkpointCoordinator.getPendingCheckpoints().values().iterator().next();
        long checkpointId1 = pending1.getCheckpointID();

        // trigger messages should have been sent
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointId1);
        }

        OperatorID opID1 = vertex1.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();
        OperatorID opID2 = vertex2.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();
        OperatorID opID3 = vertex3.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();

        TaskStateSnapshot taskOperatorSubtaskStates11 = spy(new TaskStateSnapshot());
        TaskStateSnapshot taskOperatorSubtaskStates12 = spy(new TaskStateSnapshot());
        TaskStateSnapshot taskOperatorSubtaskStates13 = spy(new TaskStateSnapshot());

        OperatorSubtaskStateMock subtaskState11mock = new OperatorSubtaskStateMock();
        OperatorSubtaskStateMock subtaskState12mock = new OperatorSubtaskStateMock();
        OperatorSubtaskStateMock subtaskState13mock = new OperatorSubtaskStateMock();
        OperatorSubtaskState subtaskState11 = subtaskState11mock.getSubtaskState();
        OperatorSubtaskState subtaskState12 = subtaskState12mock.getSubtaskState();
        OperatorSubtaskState subtaskState13 = subtaskState13mock.getSubtaskState();
        taskOperatorSubtaskStates11.putSubtaskStateByOperatorID(opID1, subtaskState11);
        taskOperatorSubtaskStates12.putSubtaskStateByOperatorID(opID2, subtaskState12);
        taskOperatorSubtaskStates13.putSubtaskStateByOperatorID(opID3, subtaskState13);

        // acknowledge one of the three tasks
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID2,
                        checkpointId1,
                        new CheckpointMetrics(),
                        taskOperatorSubtaskStates12),
                TASK_MANAGER_LOCATION_INFO);

        // start the second checkpoint
        gateway.resetCount();
        final CompletableFuture<CompletedCheckpoint> checkpointFuture2 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture2);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isEqualTo(2);
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        PendingCheckpoint pending2;
        {
            Iterator<PendingCheckpoint> all =
                    checkpointCoordinator.getPendingCheckpoints().values().iterator();
            PendingCheckpoint cc1 = all.next();
            PendingCheckpoint cc2 = all.next();
            pending2 = pending1 == cc1 ? cc2 : cc1;
        }
        long checkpointId2 = pending2.getCheckpointID();

        TaskStateSnapshot taskOperatorSubtaskStates21 = spy(new TaskStateSnapshot());
        TaskStateSnapshot taskOperatorSubtaskStates22 = spy(new TaskStateSnapshot());
        TaskStateSnapshot taskOperatorSubtaskStates23 = spy(new TaskStateSnapshot());

        OperatorSubtaskStateMock subtaskState21mock = new OperatorSubtaskStateMock();
        OperatorSubtaskStateMock subtaskState22mock = new OperatorSubtaskStateMock();
        OperatorSubtaskStateMock subtaskState23mock = new OperatorSubtaskStateMock();
        OperatorSubtaskState subtaskState21 = subtaskState21mock.getSubtaskState();
        OperatorSubtaskState subtaskState22 = subtaskState22mock.getSubtaskState();
        OperatorSubtaskState subtaskState23 = subtaskState23mock.getSubtaskState();

        taskOperatorSubtaskStates21.putSubtaskStateByOperatorID(opID1, subtaskState21);
        taskOperatorSubtaskStates22.putSubtaskStateByOperatorID(opID2, subtaskState22);
        taskOperatorSubtaskStates23.putSubtaskStateByOperatorID(opID3, subtaskState23);

        // trigger messages should have been sent
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointId2);
        }

        // we acknowledge one more task from the first checkpoint and the second
        // checkpoint completely. The second checkpoint should then subsume the first checkpoint

        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID3,
                        checkpointId2,
                        new CheckpointMetrics(),
                        taskOperatorSubtaskStates23),
                TASK_MANAGER_LOCATION_INFO);

        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId2,
                        new CheckpointMetrics(),
                        taskOperatorSubtaskStates21),
                TASK_MANAGER_LOCATION_INFO);

        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId1,
                        new CheckpointMetrics(),
                        taskOperatorSubtaskStates11),
                TASK_MANAGER_LOCATION_INFO);

        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID2,
                        checkpointId2,
                        new CheckpointMetrics(),
                        taskOperatorSubtaskStates22),
                TASK_MANAGER_LOCATION_INFO);

        // now, the second checkpoint should be confirmed, and the first discarded
        // actually both pending checkpoints are discarded, and the second has been transformed
        // into a successful checkpoint
        assertThat(pending1.isDisposed()).isTrue();
        assertThat(pending2.isDisposed()).isTrue();

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isOne();

        // validate that all received subtask states in the first checkpoint have been discarded
        subtaskState11mock.verifyDiscard();
        subtaskState12mock.verifyDiscard();

        // validate that all subtask states in the second checkpoint are not discarded
        subtaskState21mock.verifyNotDiscard();
        subtaskState22mock.verifyNotDiscard();
        subtaskState23mock.verifyNotDiscard();

        // validate the committed checkpoints
        List<CompletedCheckpoint> scs = checkpointCoordinator.getSuccessfulCheckpoints();
        CompletedCheckpoint success = scs.get(0);
        assertThat(success.getCheckpointID()).isEqualTo(checkpointId2);
        assertThat(success.getJobId()).isEqualTo(graph.getJobID());
        assertThat(success.getOperatorStates()).hasSize(3);

        // the first confirm message should be out
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2, vertex3)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getOnlyNotifiedCompletedCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointId2);
        }

        // send the last remaining ack for the first checkpoint. This should not do anything
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID3,
                        checkpointId1,
                        new CheckpointMetrics(),
                        taskOperatorSubtaskStates13),
                TASK_MANAGER_LOCATION_INFO);
        subtaskState13mock.verifyDiscard();

        checkpointCoordinator.shutdown();
        completedCheckpointStore.shutdown(JobStatus.FINISHED, new CheckpointsCleaner());

        // validate that the states in the second checkpoint have been discarded
        subtaskState21mock.verifyDiscard();
        subtaskState22mock.verifyDiscard();
        subtaskState23mock.verifyDiscard();
    }

    @Test
    void testCheckpointTimeoutIsolated() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2, false)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        // trigger a checkpoint, partially acknowledged
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();

        PendingCheckpoint checkpoint =
                checkpointCoordinator.getPendingCheckpoints().values().iterator().next();
        assertThat(checkpoint.isDisposed()).isFalse();

        OperatorID opID1 = vertex1.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();

        TaskStateSnapshot taskOperatorSubtaskStates1 = spy(new TaskStateSnapshot());
        OperatorSubtaskStateMock operatorSubtaskStateMock = new OperatorSubtaskStateMock();
        OperatorSubtaskState subtaskState1 = operatorSubtaskStateMock.getSubtaskState();
        taskOperatorSubtaskStates1.putSubtaskStateByOperatorID(opID1, subtaskState1);

        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpoint.getCheckpointID(),
                        new CheckpointMetrics(),
                        taskOperatorSubtaskStates1),
                TASK_MANAGER_LOCATION_INFO);

        // triggers cancelling
        manuallyTriggeredScheduledExecutor.triggerScheduledTasks();
        assertThat(checkpoint.isDisposed())
                .as("Checkpoint was not canceled by the timeout")
                .isTrue();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();
        operatorSubtaskStateMock.verifyDiscard();

        // no confirm message must have been sent
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getNotifiedCompletedCheckpoints(attemptId)).isEmpty();
        }

        checkpointCoordinator.shutdown();
    }

    @Test
    void testHandleMessagesForNonExistingCheckpoints() throws Exception {
        // create some mock execution vertices and trigger some checkpoint
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2, false)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();

        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().keySet().iterator().next();

        // send some messages that do not belong to either the job or the any
        // of the vertices that need to be acknowledged.
        // non of the messages should throw an exception

        // wrong job id
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(new JobID(), attemptID1, checkpointId),
                TASK_MANAGER_LOCATION_INFO);

        // unknown checkpoint
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, 1L),
                TASK_MANAGER_LOCATION_INFO);

        // unknown ack vertex
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(), createExecutionAttemptId(), checkpointId),
                TASK_MANAGER_LOCATION_INFO);

        checkpointCoordinator.shutdown();
    }

    /**
     * Tests that late acknowledge checkpoint messages are properly cleaned up. Furthermore it tests
     * that unknown checkpoint messages for the same job a are cleaned up as well. In contrast
     * checkpointing messages from other jobs should not be touched. A late acknowledge message is
     * an acknowledge message which arrives after the checkpoint has been declined.
     *
     * @throws Exception
     */
    @Test
    void testStateCleanupForLateOrUnknownMessages() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2, false)
                        .setTaskManagerGateway(gateway)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
        ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();

        CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setMaxConcurrentCheckpoints(1)
                        .build();
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(chkConfig)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();

        PendingCheckpoint pendingCheckpoint =
                checkpointCoordinator.getPendingCheckpoints().values().iterator().next();

        long checkpointId = pendingCheckpoint.getCheckpointID();

        OperatorID opIDtrigger =
                vertex1.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();

        TaskStateSnapshot taskOperatorSubtaskStatesTrigger = spy(new TaskStateSnapshot());
        OperatorSubtaskStateMock subtaskStateMock = new OperatorSubtaskStateMock();
        OperatorSubtaskState subtaskStateTrigger = subtaskStateMock.getSubtaskState();
        taskOperatorSubtaskStatesTrigger.putSubtaskStateByOperatorID(
                opIDtrigger, subtaskStateTrigger);

        // acknowledge the first trigger vertex
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId,
                        new CheckpointMetrics(),
                        taskOperatorSubtaskStatesTrigger),
                TASK_MANAGER_LOCATION_INFO);

        // verify that the subtask state has not been discarded
        subtaskStateMock.verifyNotDiscard();

        TaskStateSnapshot unknownSubtaskState = mock(TaskStateSnapshot.class);

        // receive an acknowledge message for an unknown vertex
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        createExecutionAttemptId(),
                        checkpointId,
                        new CheckpointMetrics(),
                        unknownSubtaskState),
                TASK_MANAGER_LOCATION_INFO);

        // we should discard acknowledge messages from an unknown vertex belonging to our job
        verify(unknownSubtaskState, times(1)).discardState();

        TaskStateSnapshot differentJobSubtaskState = mock(TaskStateSnapshot.class);

        // receive an acknowledge message from an unknown job
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        new JobID(),
                        createExecutionAttemptId(),
                        checkpointId,
                        new CheckpointMetrics(),
                        differentJobSubtaskState),
                TASK_MANAGER_LOCATION_INFO);

        // we should not interfere with different jobs
        verify(differentJobSubtaskState, never()).discardState();

        // duplicate acknowledge message for the trigger vertex
        TaskStateSnapshot triggerSubtaskState = mock(TaskStateSnapshot.class);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId,
                        new CheckpointMetrics(),
                        triggerSubtaskState),
                TASK_MANAGER_LOCATION_INFO);

        // duplicate acknowledge messages for a known vertex should not trigger discarding the state
        verify(triggerSubtaskState, never()).discardState();

        // let the checkpoint fail at the first ack vertex
        subtaskStateMock.reset();
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId,
                        new CheckpointException(CHECKPOINT_DECLINED)),
                TASK_MANAGER_LOCATION_INFO);

        assertThat(pendingCheckpoint.isDisposed()).isTrue();

        // check that we've cleaned up the already acknowledged state
        subtaskStateMock.verifyDiscard();

        TaskStateSnapshot ackSubtaskState = mock(TaskStateSnapshot.class);

        // late acknowledge message from the second ack vertex
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID2,
                        checkpointId,
                        new CheckpointMetrics(),
                        ackSubtaskState),
                TASK_MANAGER_LOCATION_INFO);

        // check that we also cleaned up this state
        verify(ackSubtaskState, times(1)).discardState();

        // receive an acknowledge message from an unknown job
        reset(differentJobSubtaskState);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        new JobID(),
                        createExecutionAttemptId(),
                        checkpointId,
                        new CheckpointMetrics(),
                        differentJobSubtaskState),
                TASK_MANAGER_LOCATION_INFO);

        // we should not interfere with different jobs
        verify(differentJobSubtaskState, never()).discardState();

        TaskStateSnapshot unknownSubtaskState2 = mock(TaskStateSnapshot.class);

        // receive an acknowledge message for an unknown vertex
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        createExecutionAttemptId(),
                        checkpointId,
                        new CheckpointMetrics(),
                        unknownSubtaskState2),
                TASK_MANAGER_LOCATION_INFO);

        // we should discard acknowledge messages from an unknown vertex belonging to our job
        verify(unknownSubtaskState2, times(1)).discardState();
    }

    @Test
    void testMaxConcurrentAttempts1() {
        testMaxConcurrentAttempts(1);
    }

    @Test
    void testMaxConcurrentAttempts2() {
        testMaxConcurrentAttempts(2);
    }

    @Test
    void testMaxConcurrentAttempts5() {
        testMaxConcurrentAttempts(5);
    }

    @Test
    void testTriggerAndConfirmSimpleSavepoint() throws Exception {
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
        CheckpointStatsTracker statsTracker =
                new DefaultCheckpointStatsTracker(
                        Integer.MAX_VALUE,
                        UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setAlignedCheckpointTimeout(Long.MAX_VALUE)
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCheckpointStatsTracker(statsTracker)
                        .build(graph);

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();

        // trigger the first checkpoint. this should succeed
        String savepointDir = TempDirUtils.newFolder(tmpFolder).getAbsolutePath();
        CompletableFuture<CompletedCheckpoint> savepointFuture =
                checkpointCoordinator.triggerSavepoint(savepointDir, SavepointFormatType.CANONICAL);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(savepointFuture).isNotDone();

        // validate that we have a pending savepoint
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();

        long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        PendingCheckpoint pending = checkpointCoordinator.getPendingCheckpoints().get(checkpointId);

        assertThat(pending).isNotNull();
        assertThat(pending.getCheckpointID()).isEqualTo(checkpointId);
        assertThat(pending.getJobId()).isEqualTo(graph.getJobID());
        assertThat(pending.getNumberOfNonAcknowledgedTasks()).isEqualTo(2);
        assertThat(pending.getNumberOfAcknowledgedTasks()).isZero();
        assertThat(pending.getOperatorStates()).isEmpty();
        assertThat(pending.isDisposed()).isFalse();
        assertThat(pending.areTasksFullyAcknowledged()).isFalse();
        assertThat(pending.canBeSubsumed()).isFalse();

        OperatorID opID1 = OperatorID.fromJobVertexID(vertex1.getJobvertexId());
        OperatorID opID2 = OperatorID.fromJobVertexID(vertex2.getJobvertexId());
        OperatorSubtaskState subtaskState1 = mock(OperatorSubtaskState.class);
        OperatorSubtaskState subtaskState2 = mock(OperatorSubtaskState.class);
        TaskStateSnapshot taskOperatorSubtaskStates1 =
                new TaskStateSnapshot(singletonMap(opID1, subtaskState1));
        TaskStateSnapshot taskOperatorSubtaskStates2 =
                new TaskStateSnapshot(singletonMap(opID2, subtaskState2));

        // acknowledge from one of the tasks
        AcknowledgeCheckpoint acknowledgeCheckpoint2 =
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID2,
                        checkpointId,
                        new CheckpointMetrics(),
                        taskOperatorSubtaskStates2);
        checkpointCoordinator.receiveAcknowledgeMessage(
                acknowledgeCheckpoint2, TASK_MANAGER_LOCATION_INFO);
        assertThat(pending.getNumberOfAcknowledgedTasks()).isOne();
        assertThat(pending.getNumberOfNonAcknowledgedTasks()).isOne();
        assertThat(pending.isDisposed()).isFalse();
        assertThat(pending.areTasksFullyAcknowledged()).isFalse();
        assertThat(savepointFuture.isDone()).isFalse();

        // acknowledge the same task again (should not matter)
        checkpointCoordinator.receiveAcknowledgeMessage(
                acknowledgeCheckpoint2, TASK_MANAGER_LOCATION_INFO);
        assertThat(pending.isDisposed()).isFalse();
        assertThat(pending.areTasksFullyAcknowledged()).isFalse();
        assertThat(savepointFuture).isNotDone();

        // acknowledge the other task.
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId,
                        new CheckpointMetrics(),
                        taskOperatorSubtaskStates1),
                TASK_MANAGER_LOCATION_INFO);

        // the checkpoint is internally converted to a successful checkpoint and the
        // pending checkpoint object is disposed
        assertThat(pending.isDisposed()).isTrue();
        assertThat(savepointFuture.get()).isNotNull();

        // the now we should have a completed checkpoint
        // savepoints should not registered as retained checkpoints
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();

        // validate that the relevant tasks got a confirmation message
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointId);
            assertThat(gateway.getNotifiedCompletedCheckpoints(attemptId)).isEmpty();
        }

        CompletedCheckpoint success = savepointFuture.get();
        assertThat(success.getJobId()).isEqualTo(graph.getJobID());
        assertThat(success.getCheckpointID()).isEqualTo(pending.getCheckpointID());
        assertThat(success.getOperatorStates()).hasSize(2);

        AbstractCheckpointStats actualStats =
                statsTracker.createSnapshot().getHistory().getCheckpointById(checkpointId);

        assertThat(actualStats.getCheckpointId()).isEqualTo(checkpointId);
        assertThat(actualStats.getStatus()).isEqualTo(CheckpointStatsStatus.COMPLETED);

        checkpointCoordinator.shutdown();
    }

    /**
     * Triggers a savepoint and two checkpoints. The second checkpoint completes and subsumes the
     * first checkpoint, but not the first savepoint. Then we trigger another checkpoint and
     * savepoint. The 2nd savepoint completes and subsumes the last checkpoint, but not the first
     * savepoint.
     */
    @Test
    void testSavepointsAreNotSubsumed() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
        ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();

        StandaloneCheckpointIDCounter counter = new StandaloneCheckpointIDCounter();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                spy(
                        new CheckpointCoordinatorBuilder()
                                .setCheckpointCoordinatorConfiguration(
                                        CheckpointCoordinatorConfiguration.builder()
                                                .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                                .build())
                                .setCheckpointIDCounter(counter)
                                .setCompletedCheckpointStore(
                                        new StandaloneCompletedCheckpointStore(1))
                                .setTimer(manuallyTriggeredScheduledExecutor)
                                .build(graph));

        String savepointDir = TempDirUtils.newFolder(tmpFolder).getAbsolutePath();

        // Trigger savepoint and checkpoint
        CompletableFuture<CompletedCheckpoint> savepointFuture1 =
                checkpointCoordinator.triggerSavepoint(savepointDir, SavepointFormatType.CANONICAL);

        manuallyTriggeredScheduledExecutor.triggerAll();
        long savepointId1 = counter.getLast();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();

        CompletableFuture<CompletedCheckpoint> checkpointFuture1 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isEqualTo(2);
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture1);

        CompletableFuture<CompletedCheckpoint> checkpointFuture2 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture2);
        long checkpointId2 = counter.getLast();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isEqualTo(3);

        // 2nd checkpoint should subsume the 1st checkpoint, but not the savepoint
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, checkpointId2),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointId2),
                TASK_MANAGER_LOCATION_INFO);
        // no completed checkpoint before checkpointId2.
        verify(checkpointCoordinator, times(1))
                .sendAcknowledgeMessages(
                        anyList(), eq(checkpointId2), anyLong(), eq(INVALID_CHECKPOINT_ID));

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isOne();

        assertThat(checkpointCoordinator.getPendingCheckpoints().get(savepointId1).isDisposed())
                .isFalse();
        assertThat(savepointFuture1).isNotDone();

        CompletableFuture<CompletedCheckpoint> checkpointFuture3 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture3);
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isEqualTo(2);

        CompletableFuture<CompletedCheckpoint> savepointFuture2 =
                checkpointCoordinator.triggerSavepoint(savepointDir, SavepointFormatType.CANONICAL);
        manuallyTriggeredScheduledExecutor.triggerAll();
        long savepointId2 = counter.getLast();
        FutureUtils.throwIfCompletedExceptionally(savepointFuture2);
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isEqualTo(3);

        // savepoints should not subsume checkpoints
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, savepointId2),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, savepointId2),
                TASK_MANAGER_LOCATION_INFO);

        // we do not send notify checkpoint complete for savepoints
        verify(checkpointCoordinator, times(0))
                .sendAcknowledgeMessages(anyList(), eq(savepointId2), anyLong(), anyLong());

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isEqualTo(2);
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isOne();

        assertThat(checkpointCoordinator.getPendingCheckpoints().get(savepointId1).isDisposed())
                .isFalse();

        assertThat(savepointFuture1).isNotDone();
        assertThat(savepointFuture2).isCompletedWithValueMatching(Objects::nonNull);

        // Ack first savepoint
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, savepointId1),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, savepointId1),
                TASK_MANAGER_LOCATION_INFO);

        // we do not send notify checkpoint complete for savepoints
        verify(checkpointCoordinator, times(0))
                .sendAcknowledgeMessages(anyList(), eq(savepointId1), anyLong(), anyLong());

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isOne();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isOne();
        assertThat(savepointFuture1).isCompletedWithValueMatching(Objects::nonNull);

        CompletableFuture<CompletedCheckpoint> checkpointFuture4 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture4);
        long checkpointId4 = counter.getLast();

        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, checkpointId4),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointId4),
                TASK_MANAGER_LOCATION_INFO);

        // checkpoint2 would be subsumed.
        verify(checkpointCoordinator, times(1))
                .sendAcknowledgeMessages(
                        anyList(), eq(checkpointId4), anyLong(), eq(checkpointId2));
    }

    private void testMaxConcurrentAttempts(int maxConcurrentAttempts) {
        try {
            JobVertexID jobVertexID1 = new JobVertexID();

            CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                    new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexID1)
                            .setTaskManagerGateway(gateway)
                            .build(EXECUTOR_RESOURCE.getExecutor());

            ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];

            ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();

            CheckpointCoordinatorConfiguration chkConfig =
                    new CheckpointCoordinatorConfiguration
                                    .CheckpointCoordinatorConfigurationBuilder()
                            .setCheckpointInterval(10) // periodic interval is 10 ms
                            .setCheckpointTimeout(200000) // timeout is very long (200 s)
                            .setMinPauseBetweenCheckpoints(0L) // no extra delay
                            .setMaxConcurrentCheckpoints(maxConcurrentAttempts)
                            .build();
            CheckpointCoordinator checkpointCoordinator =
                    new CheckpointCoordinatorBuilder()
                            .setCheckpointCoordinatorConfiguration(chkConfig)
                            .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build(graph);

            checkpointCoordinator.startCheckpointScheduler();

            for (int i = 0; i < maxConcurrentAttempts; i++) {
                manuallyTriggeredScheduledExecutor.triggerNonPeriodicScheduledTasks(
                        CheckpointCoordinator.ScheduledTrigger.class);
                manuallyTriggeredScheduledExecutor.triggerAll();
            }

            assertThat(gateway.getTriggeredCheckpoints(attemptID1).size())
                    .isEqualTo(maxConcurrentAttempts);
            assertThat(gateway.getNotifiedCompletedCheckpoints(attemptID1).size()).isZero();

            // now, once we acknowledge one checkpoint, it should trigger the next one
            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, 1L),
                    TASK_MANAGER_LOCATION_INFO);

            Predicate<ScheduledFuture<?>> checkpointTriggerPredicate =
                    scheduledFuture -> {
                        ScheduledTask<?> scheduledTask = (ScheduledTask<?>) scheduledFuture;
                        return scheduledTask.getCallable()
                                        instanceof
                                        ManuallyTriggeredScheduledExecutorService.RunnableCaller
                                && ((ManuallyTriggeredScheduledExecutorService.RunnableCaller<?>)
                                                scheduledTask.getCallable())
                                        .command
                                        .getClass()
                                        .equals(CheckpointCoordinator.ScheduledTrigger.class);
                    };

            final long numCheckpointTriggerTasks =
                    manuallyTriggeredScheduledExecutor.getActiveNonPeriodicScheduledTask().stream()
                            .filter(checkpointTriggerPredicate)
                            .count();
            assertThat(numCheckpointTriggerTasks).isOne();

            manuallyTriggeredScheduledExecutor.triggerNonPeriodicScheduledTasks(
                    CheckpointCoordinator.ScheduledTrigger.class);
            manuallyTriggeredScheduledExecutor.triggerAll();

            assertThat(gateway.getTriggeredCheckpoints(attemptID1))
                    .hasSize(maxConcurrentAttempts + 1);

            // no further checkpoints should happen
            manuallyTriggeredScheduledExecutor.triggerNonPeriodicScheduledTasks(
                    CheckpointCoordinator.ScheduledTrigger.class);
            manuallyTriggeredScheduledExecutor.triggerAll();

            assertThat(gateway.getTriggeredCheckpoints(attemptID1))
                    .hasSize(maxConcurrentAttempts + 1);

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    void testMaxConcurrentAttemptsWithSubsumption() throws Exception {
        final int maxConcurrentAttempts = 2;
        JobVertexID jobVertexID1 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();

        CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(10) // periodic interval is 10 ms
                        .setCheckpointTimeout(200000) // timeout is very long (200 s)
                        .setMinPauseBetweenCheckpoints(0L) // no extra delay
                        .setMaxConcurrentCheckpoints(maxConcurrentAttempts)
                        .build();
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(chkConfig)
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        checkpointCoordinator.startCheckpointScheduler();

        do {
            manuallyTriggeredScheduledExecutor.triggerNonPeriodicScheduledTasks(
                    CheckpointCoordinator.ScheduledTrigger.class);
            manuallyTriggeredScheduledExecutor.triggerAll();
        } while (checkpointCoordinator.getNumberOfPendingCheckpoints() < maxConcurrentAttempts);

        // validate that the pending checkpoints are there
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints())
                .isEqualTo(maxConcurrentAttempts);
        assertThat(checkpointCoordinator.getPendingCheckpoints()).containsKey(1L);
        assertThat(checkpointCoordinator.getPendingCheckpoints()).containsKey(2L);

        // now we acknowledge the second checkpoint, which should subsume the first checkpoint
        // and allow two more checkpoints to be triggered
        // now, once we acknowledge one checkpoint, it should trigger the next one
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, 2L),
                TASK_MANAGER_LOCATION_INFO);

        // after a while, there should be the new checkpoints
        do {
            manuallyTriggeredScheduledExecutor.triggerNonPeriodicScheduledTasks(
                    CheckpointCoordinator.ScheduledTrigger.class);
            manuallyTriggeredScheduledExecutor.triggerAll();
        } while (checkpointCoordinator.getNumberOfPendingCheckpoints() < maxConcurrentAttempts);

        // do the final check
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints())
                .isEqualTo(maxConcurrentAttempts);
        assertThat(checkpointCoordinator.getPendingCheckpoints()).containsKey(3L);
        assertThat(checkpointCoordinator.getPendingCheckpoints()).containsKey(4L);

        checkpointCoordinator.shutdown();
    }

    @Test
    void testPeriodicSchedulingWithInactiveTasks() throws Exception {
        CheckpointCoordinator checkpointCoordinator =
                setupCheckpointCoordinatorWithInactiveTasks(new JobManagerCheckpointStorage());

        // the coordinator should start checkpointing now
        manuallyTriggeredScheduledExecutor.triggerNonPeriodicScheduledTasks(
                CheckpointCoordinator.ScheduledTrigger.class);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isGreaterThan(0);
    }

    private CheckpointCoordinator setupCheckpointCoordinatorWithInactiveTasks(
            CheckpointStorage checkpointStorage) throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .setTransitToRunning(false)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];

        CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointInterval(10) // periodic interval is 10 ms
                        .setCheckpointTimeout(200000) // timeout is very long (200 s)
                        .setMinPauseBetweenCheckpoints(0) // no extra delay
                        .setMaxConcurrentCheckpoints(2) // max two concurrent checkpoints
                        .build();
        CheckpointIDCounterWithOwner checkpointIDCounter = new CheckpointIDCounterWithOwner();
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(chkConfig)
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                        .setCheckpointStorage(checkpointStorage)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCheckpointIDCounter(checkpointIDCounter)
                        .build(graph);
        checkpointIDCounter.setOwner(checkpointCoordinator);

        checkpointCoordinator.startCheckpointScheduler();

        manuallyTriggeredScheduledExecutor.triggerNonPeriodicScheduledTasks(
                CheckpointCoordinator.ScheduledTrigger.class);
        manuallyTriggeredScheduledExecutor.triggerAll();
        // no checkpoint should have started so far
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();

        // now move the state to RUNNING
        vertex1.getCurrentExecutionAttempt().transitionState(ExecutionState.RUNNING);

        // the coordinator should start checkpointing now
        manuallyTriggeredScheduledExecutor.triggerNonPeriodicScheduledTasks(
                CheckpointCoordinator.ScheduledTrigger.class);
        manuallyTriggeredScheduledExecutor.triggerAll();

        return checkpointCoordinator;
    }

    /** Tests that the savepoints can be triggered concurrently. */
    @Test
    void testConcurrentSavepoints() throws Exception {
        int numSavepoints = 5;

        JobVertexID jobVertexID1 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();

        StandaloneCheckpointIDCounter checkpointIDCounter = new StandaloneCheckpointIDCounter();

        CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setMaxConcurrentCheckpoints(
                                1) // max one checkpoint at a time => should not affect savepoints
                        .build();
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(chkConfig)
                        .setCheckpointIDCounter(checkpointIDCounter)
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        List<CompletableFuture<CompletedCheckpoint>> savepointFutures = new ArrayList<>();

        String savepointDir = TempDirUtils.newFolder(tmpFolder).getAbsolutePath();

        // Trigger savepoints
        for (int i = 0; i < numSavepoints; i++) {
            savepointFutures.add(
                    checkpointCoordinator.triggerSavepoint(
                            savepointDir, SavepointFormatType.CANONICAL));
        }

        // After triggering multiple savepoints, all should in progress
        for (CompletableFuture<CompletedCheckpoint> savepointFuture : savepointFutures) {
            assertThat(savepointFuture).isNotDone();
        }

        manuallyTriggeredScheduledExecutor.triggerAll();

        // ACK all savepoints
        long checkpointId = checkpointIDCounter.getLast();
        for (int i = 0; i < numSavepoints; i++, checkpointId--) {
            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, checkpointId),
                    TASK_MANAGER_LOCATION_INFO);
        }

        // After ACKs, all should be completed
        for (CompletableFuture<CompletedCheckpoint> savepointFuture : savepointFutures) {
            assertThat(savepointFuture).isCompletedWithValueMatching(Objects::nonNull);
        }
    }

    /** Tests that no minimum delay between savepoints is enforced. */
    @Test
    void testMinDelayBetweenSavepoints() throws Exception {
        CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setMinPauseBetweenCheckpoints(
                                100000000L) // very long min delay => should not affect savepoints
                        .setMaxConcurrentCheckpoints(1)
                        .build();
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(chkConfig)
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        String savepointDir = TempDirUtils.newFolder(tmpFolder).getAbsolutePath();

        CompletableFuture<CompletedCheckpoint> savepoint0 =
                checkpointCoordinator.triggerSavepoint(savepointDir, SavepointFormatType.CANONICAL);
        assertThat(savepoint0).as("Did not trigger savepoint").isNotDone();

        CompletableFuture<CompletedCheckpoint> savepoint1 =
                checkpointCoordinator.triggerSavepoint(savepointDir, SavepointFormatType.CANONICAL);
        assertThat(savepoint1).as("Did not trigger savepoint").isNotDone();
    }

    /** Tests that the externalized checkpoint configuration is respected. */
    @Test
    void testExternalizedCheckpoints() throws Exception {
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .build(EXECUTOR_RESOURCE.getExecutor());

        // set up the coordinator and validate the initial state
        CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setCheckpointRetentionPolicy(CheckpointRetentionPolicy.RETAIN_ON_FAILURE)
                        .build();
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(chkConfig)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);

        CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        for (PendingCheckpoint checkpoint :
                checkpointCoordinator.getPendingCheckpoints().values()) {
            CheckpointProperties props = checkpoint.getProps();
            CheckpointProperties expected =
                    CheckpointProperties.forCheckpoint(CheckpointRetentionPolicy.RETAIN_ON_FAILURE);

            assertThat(props).isEqualTo(expected);
        }

        // the now we should have a completed checkpoint
        checkpointCoordinator.shutdown();
    }

    @Test
    void testCreateKeyGroupPartitions() {
        testCreateKeyGroupPartitions(1, 1);
        testCreateKeyGroupPartitions(13, 1);
        testCreateKeyGroupPartitions(13, 2);
        testCreateKeyGroupPartitions(Short.MAX_VALUE, 1);
        testCreateKeyGroupPartitions(Short.MAX_VALUE, 13);
        testCreateKeyGroupPartitions(Short.MAX_VALUE, Short.MAX_VALUE);

        Random r = new Random(1234);
        for (int k = 0; k < 1000; ++k) {
            int maxParallelism = 1 + r.nextInt(Short.MAX_VALUE - 1);
            int parallelism = 1 + r.nextInt(maxParallelism);
            testCreateKeyGroupPartitions(maxParallelism, parallelism);
        }
    }

    private void testCreateKeyGroupPartitions(int maxParallelism, int parallelism) {
        List<KeyGroupRange> ranges =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism, parallelism);
        for (int i = 0; i < maxParallelism; ++i) {
            KeyGroupRange range =
                    ranges.get(
                            KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                    maxParallelism, parallelism, i));
            if (!range.contains(i)) {
                fail("Could not find expected key-group " + i + " in range " + range);
            }
        }
    }

    @Test
    void testPartitionableStateRepartitioning() {
        Random r = new Random(42);

        for (int run = 0; run < 10000; ++run) {
            int oldParallelism = 1 + r.nextInt(9);
            int newParallelism = 1 + r.nextInt(9);

            int numNamedStates = 1 + r.nextInt(9);
            int maxPartitionsPerState = 1 + r.nextInt(9);

            doTestPartitionableStateRepartitioning(
                    r, oldParallelism, newParallelism, numNamedStates, maxPartitionsPerState);
        }
    }

    private void doTestPartitionableStateRepartitioning(
            Random r,
            int oldParallelism,
            int newParallelism,
            int numNamedStates,
            int maxPartitionsPerState) {

        List<List<OperatorStateHandle>> previousParallelOpInstanceStates =
                new ArrayList<>(oldParallelism);

        for (int i = 0; i < oldParallelism; ++i) {
            Path fakePath = new Path("/fake-" + i);
            Map<String, OperatorStateHandle.StateMetaInfo> namedStatesToOffsets = new HashMap<>();
            int off = 0;
            for (int s = 0; s < numNamedStates - 1; ++s) {
                long[] offs = new long[1 + r.nextInt(maxPartitionsPerState)];

                for (int o = 0; o < offs.length; ++o) {
                    offs[o] = off;
                    ++off;
                }

                OperatorStateHandle.Mode mode =
                        r.nextInt(10) == 0
                                ? OperatorStateHandle.Mode.UNION
                                : OperatorStateHandle.Mode.SPLIT_DISTRIBUTE;
                namedStatesToOffsets.put(
                        "State-" + s, new OperatorStateHandle.StateMetaInfo(offs, mode));
            }

            if (numNamedStates % 2 == 0) {
                // finally add a broadcast state
                long[] offs = {off + 1, off + 2, off + 3, off + 4};

                namedStatesToOffsets.put(
                        "State-" + (numNamedStates - 1),
                        new OperatorStateHandle.StateMetaInfo(
                                offs, OperatorStateHandle.Mode.BROADCAST));
            }

            previousParallelOpInstanceStates.add(
                    Collections.singletonList(
                            new OperatorStreamStateHandle(
                                    namedStatesToOffsets, new FileStateHandle(fakePath, -1))));
        }

        Map<StreamStateHandle, Map<String, List<Long>>> expected = new HashMap<>();

        int taskIndex = 0;
        int expectedTotalPartitions = 0;
        for (List<OperatorStateHandle> previousParallelOpInstanceState :
                previousParallelOpInstanceStates) {
            assertThat(previousParallelOpInstanceState.size()).isOne();

            for (OperatorStateHandle psh : previousParallelOpInstanceState) {
                Map<String, OperatorStateHandle.StateMetaInfo> offsMap =
                        psh.getStateNameToPartitionOffsets();
                Map<String, List<Long>> offsMapWithList = new HashMap<>(offsMap.size());
                for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> e : offsMap.entrySet()) {

                    long[] offs = e.getValue().getOffsets();
                    int replication;
                    switch (e.getValue().getDistributionMode()) {
                        case UNION:
                            replication = newParallelism;
                            break;
                        case BROADCAST:
                            int extra = taskIndex < (newParallelism % oldParallelism) ? 1 : 0;
                            replication = newParallelism / oldParallelism + extra;
                            break;
                        case SPLIT_DISTRIBUTE:
                            replication = 1;
                            break;
                        default:
                            throw new RuntimeException(
                                    "Unknown distribution mode "
                                            + e.getValue().getDistributionMode());
                    }

                    if (replication > 0) {
                        expectedTotalPartitions += replication * offs.length;
                        List<Long> offsList = new ArrayList<>(offs.length);

                        for (long off : offs) {
                            for (int p = 0; p < replication; ++p) {
                                offsList.add(off);
                            }
                        }
                        offsMapWithList.put(e.getKey(), offsList);
                    }
                }

                if (!offsMapWithList.isEmpty()) {
                    expected.put(psh.getDelegateStateHandle(), offsMapWithList);
                }
                taskIndex++;
            }
        }

        OperatorStateRepartitioner<OperatorStateHandle> repartitioner =
                RoundRobinOperatorStateRepartitioner.INSTANCE;

        List<List<OperatorStateHandle>> pshs =
                repartitioner.repartitionState(
                        previousParallelOpInstanceStates, oldParallelism, newParallelism);

        Map<StreamStateHandle, Map<String, List<Long>>> actual = new HashMap<>();

        int minCount = Integer.MAX_VALUE;
        int maxCount = 0;
        int actualTotalPartitions = 0;
        for (int p = 0; p < newParallelism; ++p) {
            int partitionCount = 0;

            Collection<OperatorStateHandle> pshc = pshs.get(p);
            for (OperatorStateHandle sh : pshc) {
                for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> namedState :
                        sh.getStateNameToPartitionOffsets().entrySet()) {

                    Map<String, List<Long>> stateToOffsets =
                            actual.computeIfAbsent(
                                    sh.getDelegateStateHandle(), k -> new HashMap<>());

                    List<Long> actualOffs =
                            stateToOffsets.computeIfAbsent(
                                    namedState.getKey(), k -> new ArrayList<>());
                    long[] add = namedState.getValue().getOffsets();
                    for (long l : add) {
                        actualOffs.add(l);
                    }

                    partitionCount += namedState.getValue().getOffsets().length;
                }
            }

            minCount = Math.min(minCount, partitionCount);
            maxCount = Math.max(maxCount, partitionCount);
            actualTotalPartitions += partitionCount;
        }

        for (Map<String, List<Long>> v : actual.values()) {
            for (List<Long> l : v.values()) {
                Collections.sort(l);
            }
        }

        // if newParallelism equals to oldParallelism, we would only redistribute UNION state if
        // possible.
        if (oldParallelism != newParallelism) {
            int maxLoadDiff = maxCount - minCount;
            assertThat(maxLoadDiff <= 1)
                    .as("Difference in partition load is > 1 : " + maxLoadDiff)
                    .isTrue();
        }
        assertThat(actualTotalPartitions).isEqualTo(expectedTotalPartitions);
        assertThat(actual).isEqualTo(expected);
    }

    /** Tests that the pending checkpoint stats callbacks are created. */
    @Test
    void testCheckpointStatsTrackerPendingCheckpointCallback() throws Exception {
        // set up the coordinator and validate the initial state
        CheckpointStatsTracker tracker = mock(CheckpointStatsTracker.class);
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCheckpointStatsTracker(tracker)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        when(tracker.reportPendingCheckpoint(
                        anyLong(), anyLong(), any(CheckpointProperties.class), any(Map.class)))
                .thenReturn(mock(PendingCheckpointStats.class));

        // Trigger a checkpoint and verify callback
        CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        verify(tracker, times(1))
                .reportPendingCheckpoint(
                        eq(1L),
                        any(Long.class),
                        eq(
                                CheckpointProperties.forCheckpoint(
                                        CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION)),
                        any());
    }

    /** Tests that the restore callbacks are called if registered. */
    @Test
    void testCheckpointStatsTrackerRestoreCallback() throws Exception {
        StandaloneCompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(1);

        // set up the coordinator and validate the initial state
        CheckpointStatsTracker tracker =
                new DefaultCheckpointStatsTracker(
                        10, UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCompletedCheckpointStore(store)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCheckpointStatsTracker(tracker)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        store.addCheckpointAndSubsumeOldestOne(
                new CompletedCheckpoint(
                        new JobID(),
                        42,
                        0,
                        0,
                        Collections.<OperatorID, OperatorState>emptyMap(),
                        Collections.<MasterState>emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation(),
                        null),
                new CheckpointsCleaner(),
                () -> {});

        assertThat(
                        checkpointCoordinator.restoreLatestCheckpointedStateToAll(
                                Collections.emptySet(), true))
                .isTrue();

        assertThat(tracker.createSnapshot().getLatestRestoredCheckpoint().getCheckpointId())
                .isEqualTo(42);
    }

    @Test
    void testSharedStateRegistrationOnRestore() throws Exception {
        for (RecoveryClaimMode recoveryClaimMode : RecoveryClaimMode.values()) {
            JobVertexID jobVertexID1 = new JobVertexID();

            int parallelism1 = 2;
            int maxParallelism1 = 4;

            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexID1, parallelism1, maxParallelism1)
                            .build(EXECUTOR_RESOURCE.getExecutor());

            ExecutionJobVertex jobVertex1 = graph.getJobVertex(jobVertexID1);

            List<CompletedCheckpoint> checkpoints = Collections.emptyList();
            SharedStateRegistry firstInstance =
                    SharedStateRegistry.DEFAULT_FACTORY.create(
                            org.apache.flink.util.concurrent.Executors.directExecutor(),
                            checkpoints,
                            recoveryClaimMode);
            final EmbeddedCompletedCheckpointStore store =
                    new EmbeddedCompletedCheckpointStore(10, checkpoints, firstInstance);

            // set up the coordinator and validate the initial state
            final CheckpointCoordinatorBuilder coordinatorBuilder =
                    new CheckpointCoordinatorBuilder().setTimer(manuallyTriggeredScheduledExecutor);
            final CheckpointCoordinator coordinator =
                    coordinatorBuilder.setCompletedCheckpointStore(store).build(graph);

            final int numCheckpoints = 3;

            List<KeyGroupRange> keyGroupPartitions1 =
                    StateAssignmentOperation.createKeyGroupPartitions(
                            maxParallelism1, parallelism1);

            for (int i = 0; i < numCheckpoints; ++i) {
                performIncrementalCheckpoint(
                        graph.getJobID(), coordinator, jobVertex1, keyGroupPartitions1, i);
            }

            List<CompletedCheckpoint> completedCheckpoints = coordinator.getSuccessfulCheckpoints();
            assertThat(completedCheckpoints.size()).isEqualTo(numCheckpoints);

            int sharedHandleCount = 0;

            List<List<HandleAndLocalPath>> sharedHandlesByCheckpoint =
                    new ArrayList<>(numCheckpoints);

            for (int i = 0; i < numCheckpoints; ++i) {
                sharedHandlesByCheckpoint.add(new ArrayList<>(2));
            }

            int cp = 0;
            for (CompletedCheckpoint completedCheckpoint : completedCheckpoints) {
                for (OperatorState taskState : completedCheckpoint.getOperatorStates().values()) {
                    for (OperatorSubtaskState subtaskState : taskState.getStates()) {
                        for (KeyedStateHandle keyedStateHandle :
                                subtaskState.getManagedKeyedState()) {
                            // test we are once registered with the current registry
                            verify(keyedStateHandle, times(1))
                                    .registerSharedStates(
                                            firstInstance, completedCheckpoint.getCheckpointID());
                            IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle =
                                    (IncrementalRemoteKeyedStateHandle) keyedStateHandle;

                            sharedHandlesByCheckpoint
                                    .get(cp)
                                    .addAll(incrementalKeyedStateHandle.getSharedState());

                            for (HandleAndLocalPath handleAndLocalPath :
                                    incrementalKeyedStateHandle.getSharedState()) {
                                StreamStateHandle streamStateHandle =
                                        handleAndLocalPath.getHandle();
                                assertThat(
                                                streamStateHandle
                                                        instanceof PlaceholderStreamStateHandle)
                                        .isFalse();
                                DiscardRecordedStateObject.verifyDiscard(
                                        streamStateHandle, TernaryBoolean.FALSE);
                                ++sharedHandleCount;
                            }

                            for (HandleAndLocalPath handleAndLocalPath :
                                    incrementalKeyedStateHandle.getPrivateState()) {
                                DiscardRecordedStateObject.verifyDiscard(
                                        handleAndLocalPath.getHandle(), TernaryBoolean.FALSE);
                            }

                            verify(incrementalKeyedStateHandle.getMetaDataStateHandle(), never())
                                    .discardState();
                        }

                        verify(subtaskState, never()).discardState();
                    }
                }
                ++cp;
            }

            // 2 (parallelism) x (1 (CP0) + 2 (CP1) + 2 (CP2)) = 10
            assertThat(sharedHandleCount).isEqualTo(10);

            // discard CP0
            store.removeOldestCheckpoint();

            // we expect no shared state was discarded because the state of CP0 is still referenced
            // by CP1
            for (List<HandleAndLocalPath> cpList : sharedHandlesByCheckpoint) {
                for (HandleAndLocalPath handleAndLocalPath : cpList) {
                    DiscardRecordedStateObject.verifyDiscard(
                            handleAndLocalPath.getHandle(), TernaryBoolean.FALSE);
                }
            }

            // shutdown the store
            store.shutdown(JobStatus.SUSPENDED, new CheckpointsCleaner());

            // restore the store
            Set<ExecutionJobVertex> tasks = new HashSet<>();
            tasks.add(jobVertex1);

            assertThat(store.getShutdownStatus().orElse(null)).isEqualTo(JobStatus.SUSPENDED);
            SharedStateRegistry secondInstance =
                    SharedStateRegistry.DEFAULT_FACTORY.create(
                            org.apache.flink.util.concurrent.Executors.directExecutor(),
                            store.getAllCheckpoints(),
                            recoveryClaimMode);
            final EmbeddedCompletedCheckpointStore secondStore =
                    new EmbeddedCompletedCheckpointStore(
                            10, store.getAllCheckpoints(), secondInstance);
            final CheckpointCoordinator secondCoordinator =
                    coordinatorBuilder.setCompletedCheckpointStore(secondStore).build(graph);
            assertThat(secondCoordinator.restoreLatestCheckpointedStateToAll(tasks, false))
                    .isTrue();

            // validate that all shared states are registered again after the recovery.
            cp = 0;
            for (CompletedCheckpoint completedCheckpoint : completedCheckpoints) {
                for (OperatorState taskState : completedCheckpoint.getOperatorStates().values()) {
                    for (OperatorSubtaskState subtaskState : taskState.getStates()) {
                        for (KeyedStateHandle keyedStateHandle :
                                subtaskState.getManagedKeyedState()) {
                            VerificationMode verificationMode;
                            // test we are once registered with the new registry
                            if (cp > 0) {
                                verificationMode = times(1);
                            } else {
                                verificationMode = never();
                            }

                            // check that all are registered with the new registry
                            verify(keyedStateHandle, verificationMode)
                                    .registerSharedStates(
                                            secondInstance, completedCheckpoint.getCheckpointID());
                        }
                    }
                }
                ++cp;
            }

            // discard CP1
            secondStore.removeOldestCheckpoint();

            // we expect that all shared state from CP0 is no longer referenced and discarded. CP2
            // is
            // still live and also
            // references the state from CP1, so we expect they are not discarded.
            verifyDiscard(
                    sharedHandlesByCheckpoint,
                    cpId ->
                            recoveryClaimMode == RecoveryClaimMode.CLAIM && cpId == 0
                                    ? TernaryBoolean.TRUE
                                    : TernaryBoolean.FALSE);

            // discard CP2
            secondStore.removeOldestCheckpoint();

            // still expect shared state not to be discarded because it may be used in later
            // checkpoints
            verifyDiscard(
                    sharedHandlesByCheckpoint,
                    cpId -> cpId == 1 ? TernaryBoolean.FALSE : TernaryBoolean.UNDEFINED);
        }
    }

    @Test
    void jobFailsIfInFlightSynchronousSavepointIsDiscarded() throws Exception {
        final Tuple2<Integer, Throwable> invocationCounterAndException = Tuple2.of(0, null);
        final Throwable expectedRootCause = new IOException("Custom-Exception");

        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
        ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        final CheckpointCoordinator coordinator =
                getCheckpointCoordinator(
                        graph,
                        new CheckpointFailureManager(
                                0,
                                new CheckpointFailureManager.FailJobCallback() {
                                    @Override
                                    public void failJob(Throwable cause) {
                                        invocationCounterAndException.f0 += 1;
                                        invocationCounterAndException.f1 = cause;
                                    }

                                    @Override
                                    public void failJobDueToTaskFailure(
                                            Throwable cause, ExecutionAttemptID failingTask) {
                                        throw new AssertionError(
                                                "This method should not be called for the test.");
                                    }
                                }));

        final CompletableFuture<CompletedCheckpoint> savepointFuture =
                coordinator.triggerSynchronousSavepoint(
                        false, "test-dir", SavepointFormatType.CANONICAL);

        manuallyTriggeredScheduledExecutor.triggerAll();
        final PendingCheckpoint syncSavepoint =
                declineSynchronousSavepoint(
                        graph.getJobID(), coordinator, attemptID1, expectedRootCause);

        assertThat(syncSavepoint.isDisposed()).isTrue();
        String expectedRootCauseMessage =
                String.format(
                        "%s: %s",
                        expectedRootCause.getClass().getName(), expectedRootCause.getMessage());
        try {
            savepointFuture.get();
            fail("Expected Exception not found.");
        } catch (ExecutionException e) {
            final Throwable cause = ExceptionUtils.stripExecutionException(e);
            assertThat(cause instanceof CheckpointException).isTrue();
            assertThat(cause.getCause().getCause().getMessage())
                    .isEqualTo(expectedRootCauseMessage);
        }

        assertThat(invocationCounterAndException.f0.intValue()).isOne();
        assertThat(
                        invocationCounterAndException.f1 instanceof CheckpointException
                                && invocationCounterAndException
                                        .f1
                                        .getCause()
                                        .getCause()
                                        .getMessage()
                                        .equals(expectedRootCauseMessage))
                .isTrue();

        coordinator.shutdown();
    }

    /** Tests that do not trigger checkpoint when stop the coordinator after the eager pre-check. */
    @Test
    void testTriggerCheckpointAfterStopping() throws Exception {
        StoppingCheckpointIDCounter testingCounter = new StoppingCheckpointIDCounter();
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointIDCounter(testingCounter)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        testingCounter.setOwner(checkpointCoordinator);

        testTriggerCheckpoint(checkpointCoordinator, PERIODIC_SCHEDULER_SHUTDOWN);
    }

    /** Tests that do not trigger checkpoint when CheckpointIDCounter IOException occurred. */
    @Test
    void testTriggerCheckpointWithCounterIOException() throws Exception {
        // given: Checkpoint coordinator which fails on getCheckpointId.
        IOExceptionCheckpointIDCounter testingCounter = new IOExceptionCheckpointIDCounter();
        TestFailJobCallback failureCallback = new TestFailJobCallback();

        CheckpointStatsTracker statsTracker =
                new DefaultCheckpointStatsTracker(
                        Integer.MAX_VALUE,
                        UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());

        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointIDCounter(testingCounter)
                        .setFailureManager(new CheckpointFailureManager(0, failureCallback))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCheckpointStatsTracker(statsTracker)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        testingCounter.setOwner(checkpointCoordinator);

        // when: The checkpoint is triggered.
        testTriggerCheckpoint(checkpointCoordinator, IO_EXCEPTION);

        // then: Failure manager should fail the job.
        assertThat(failureCallback.getInvokeCounter()).isOne();

        // then: The NumberOfFailedCheckpoints and TotalNumberOfCheckpoints should be 1.
        CheckpointStatsCounts counts = statsTracker.createSnapshot().getCounts();
        assertThat(counts.getNumberOfRestoredCheckpoints()).isZero();
        assertThat(counts.getTotalNumberOfCheckpoints()).isOne();
        assertThat(counts.getNumberOfInProgressCheckpoints()).isZero();
        assertThat(counts.getNumberOfCompletedCheckpoints()).isZero();
        assertThat(counts.getNumberOfFailedCheckpoints()).isOne();

        // then: The PendingCheckpoint shouldn't be created.
        assertThat(statsTracker.getPendingCheckpointStats(1)).isNull();
    }

    /**
     * This test checks that an exception that occurs while trying to store a {@link
     * CompletedCheckpoint} in the {@link CompletedCheckpointStore} is properly reported to the
     * {@link CheckpointFailureManager}, see FLINK-32347.
     */
    @Test
    void testExceptionInStoringCompletedCheckpointIsReportedToFailureManager() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTransitToRunning(false)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex task = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        task.getCurrentExecutionAttempt().transitionState(ExecutionState.RUNNING);

        CheckpointIDCounterWithOwner testingCounter = new CheckpointIDCounterWithOwner();
        TestFailJobCallback failureCallback = new TestFailJobCallback();

        CheckpointStatsTracker statsTracker =
                new DefaultCheckpointStatsTracker(
                        Integer.MAX_VALUE,
                        UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());

        final String exceptionMsg = "Test store exception.";
        try (SharedStateRegistry sharedStateRegistry = new SharedStateRegistryImpl()) {
            // Prepare a store that fails when the coordinator stores a checkpoint.
            TestingCompletedCheckpointStore testingCompletedCheckpointStore =
                    TestingCompletedCheckpointStore.builder()
                            .withGetSharedStateRegistrySupplier(() -> sharedStateRegistry)
                            .withAddCheckpointAndSubsumeOldestOneFunction(
                                    (cp, cleaner, runnable) -> {
                                        throw new RuntimeException(exceptionMsg);
                                    })
                            .build();

            CheckpointCoordinator checkpointCoordinator =
                    new CheckpointCoordinatorBuilder()
                            .setCheckpointIDCounter(testingCounter)
                            .setFailureManager(new CheckpointFailureManager(0, failureCallback))
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .setCheckpointStatsTracker(statsTracker)
                            .setCompletedCheckpointStore(testingCompletedCheckpointStore)
                            .build(graph);
            testingCounter.setOwner(checkpointCoordinator);

            checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();

            PendingCheckpoint pendingCheckpoint =
                    checkpointCoordinator
                            .getPendingCheckpoints()
                            .entrySet()
                            .iterator()
                            .next()
                            .getValue();

            try {
                checkpointCoordinator.receiveAcknowledgeMessage(
                        new AcknowledgeCheckpoint(
                                pendingCheckpoint.getJobId(),
                                task.getCurrentExecutionAttempt().getAttemptId(),
                                pendingCheckpoint.getCheckpointID(),
                                new CheckpointMetrics(),
                                new TaskStateSnapshot()),
                        "localhost");

                fail("Exception is expected here");
            } catch (CheckpointException cpex) {
                assertThat(cpex.getCheckpointFailureReason())
                        .isEqualTo(FINALIZE_CHECKPOINT_FAILURE);
                assertThat(cpex.getCause().getMessage()).isEqualTo(exceptionMsg);
            }

            // then: Failure manager should fail the job.
            assertThat(failureCallback.getInvokeCounter()).isOne();

            // then: The NumberOfFailedCheckpoints and TotalNumberOfCheckpoints should be 1.
            CheckpointStatsCounts counts = statsTracker.createSnapshot().getCounts();
            assertThat(counts.getNumberOfRestoredCheckpoints()).isZero();
            assertThat(counts.getTotalNumberOfCheckpoints()).isOne();
            assertThat(counts.getNumberOfInProgressCheckpoints()).isZero();
            assertThat(counts.getNumberOfCompletedCheckpoints()).isZero();
            assertThat(counts.getNumberOfFailedCheckpoints()).isOne();

            // then: The PendingCheckpoint should already exist.
            assertThat(statsTracker.getPendingCheckpointStats(1)).isNotNull();
        }
    }

    private void testTriggerCheckpoint(
            CheckpointCoordinator checkpointCoordinator,
            CheckpointFailureReason expectedFailureReason)
            throws Exception {
        try {
            // start the coordinator
            checkpointCoordinator.startCheckpointScheduler();
            final CompletableFuture<CompletedCheckpoint> onCompletionPromise =
                    checkpointCoordinator.triggerCheckpoint(
                            CheckpointProperties.forCheckpoint(
                                    CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                            null,
                            true);
            manuallyTriggeredScheduledExecutor.triggerAll();
            try {
                onCompletionPromise.get();
                fail("should not trigger periodic checkpoint");
            } catch (ExecutionException e) {
                final Optional<CheckpointException> checkpointExceptionOptional =
                        ExceptionUtils.findThrowable(e, CheckpointException.class);
                if (!checkpointExceptionOptional.isPresent()
                        || checkpointExceptionOptional.get().getCheckpointFailureReason()
                                != expectedFailureReason) {
                    throw e;
                }
            }
        } finally {
            checkpointCoordinator.shutdown();
        }
    }

    @Test
    void testSavepointScheduledInUnalignedMode() throws Exception {
        int maxConcurrentCheckpoints = 1;
        int checkpointRequestsToSend = 10;
        int activeRequests = 0;

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .build(EXECUTOR_RESOURCE.getExecutor());
        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setUnalignedCheckpointsEnabled(true)
                                        .setMaxConcurrentCheckpoints(maxConcurrentCheckpoints)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(graph);
        try {
            List<Future<?>> checkpointFutures = new ArrayList<>(checkpointRequestsToSend);
            coordinator.startCheckpointScheduler();
            while (activeRequests < checkpointRequestsToSend) {
                checkpointFutures.add(coordinator.triggerCheckpoint(true));
                activeRequests++;
            }
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertThat(coordinator.getNumQueuedRequests())
                    .isEqualTo(activeRequests - maxConcurrentCheckpoints);

            Future<?> savepointFuture =
                    coordinator.triggerSavepoint("/tmp", SavepointFormatType.CANONICAL);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertThat(coordinator.getNumQueuedRequests())
                    .isEqualTo(++activeRequests - maxConcurrentCheckpoints);

            coordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            graph.getJobID(),
                            createExecutionAttemptId(),
                            1L,
                            new CheckpointException(CHECKPOINT_DECLINED)),
                    "none");
            manuallyTriggeredScheduledExecutor.triggerAll();

            activeRequests--; // savepoint triggered
            assertThat(coordinator.getNumQueuedRequests())
                    .isEqualTo(activeRequests - maxConcurrentCheckpoints);
            assertThat(checkpointFutures.stream().filter(Future::isDone).count()).isOne();

            assertThat(savepointFuture.isDone()).isFalse();
            assertThat(coordinator.getNumberOfPendingCheckpoints())
                    .isEqualTo(maxConcurrentCheckpoints);
            CheckpointProperties props =
                    coordinator.getPendingCheckpoints().values().iterator().next().getProps();
            assertThat(props.isSavepoint()).isTrue();
            assertThat(props.forceCheckpoint()).isFalse();
        } finally {
            coordinator.shutdown();
        }
    }

    /**
     * Test that the checkpoint still behave correctly when the task checkpoint is triggered by the
     * master hooks and finished before the master checkpoint. Also make sure that the operator
     * coordinators are checkpointed before starting the task checkpoint.
     */
    @Test
    void testExternallyInducedSourceWithOperatorCoordinator() throws Exception {
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

        OperatorID opID1 = vertex1.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();
        OperatorID opID2 = vertex2.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();
        TaskStateSnapshot taskOperatorSubtaskStates1 = new TaskStateSnapshot();
        TaskStateSnapshot taskOperatorSubtaskStates2 = new TaskStateSnapshot();
        OperatorSubtaskState subtaskState1 = OperatorSubtaskState.builder().build();
        OperatorSubtaskState subtaskState2 = OperatorSubtaskState.builder().build();
        taskOperatorSubtaskStates1.putSubtaskStateByOperatorID(opID1, subtaskState1);
        taskOperatorSubtaskStates1.putSubtaskStateByOperatorID(opID2, subtaskState2);

        // Create a mock OperatorCoordinatorCheckpointContext which completes the checkpoint
        // immediately.
        AtomicBoolean coordCheckpointDone = new AtomicBoolean(false);
        OperatorCoordinatorCheckpointContext coordinatorCheckpointContext =
                new CheckpointCoordinatorTestingUtils
                                .MockOperatorCheckpointCoordinatorContextBuilder()
                        .setOnCallingCheckpointCoordinator(
                                (checkpointId, result) -> {
                                    coordCheckpointDone.set(true);
                                    result.complete(new byte[0]);
                                })
                        .setOperatorID(opID1)
                        .build();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCoordinatorsToCheckpoint(
                                Collections.singleton(coordinatorCheckpointContext))
                        .build(graph);
        AtomicReference<Long> checkpointIdRef = new AtomicReference<>();

        // Add a master hook which triggers and acks the task checkpoint immediately.
        // In this case the task checkpoints would complete before the job master checkpoint
        // completes.
        checkpointCoordinator.addMasterHook(
                new MasterTriggerRestoreHook<Integer>() {
                    @Override
                    public String getIdentifier() {
                        return "anything";
                    }

                    @Override
                    @Nullable
                    public CompletableFuture<Integer> triggerCheckpoint(
                            long checkpointId, long timestamp, Executor executor) throws Exception {
                        assertThat(coordCheckpointDone.get())
                                .as("The coordinator checkpoint should have finished.")
                                .isTrue();
                        // Acknowledge the checkpoint in the master hooks so the task snapshots
                        // complete before
                        // the master state snapshot completes.
                        checkpointIdRef.set(checkpointId);
                        AcknowledgeCheckpoint acknowledgeCheckpoint1 =
                                new AcknowledgeCheckpoint(
                                        graph.getJobID(),
                                        attemptID1,
                                        checkpointId,
                                        new CheckpointMetrics(),
                                        taskOperatorSubtaskStates1);
                        AcknowledgeCheckpoint acknowledgeCheckpoint2 =
                                new AcknowledgeCheckpoint(
                                        graph.getJobID(),
                                        attemptID2,
                                        checkpointId,
                                        new CheckpointMetrics(),
                                        taskOperatorSubtaskStates2);
                        checkpointCoordinator.receiveAcknowledgeMessage(
                                acknowledgeCheckpoint1, TASK_MANAGER_LOCATION_INFO);
                        checkpointCoordinator.receiveAcknowledgeMessage(
                                acknowledgeCheckpoint2, TASK_MANAGER_LOCATION_INFO);
                        return null;
                    }

                    @Override
                    public void restoreCheckpoint(long checkpointId, Integer checkpointData) {}

                    @Override
                    public SimpleVersionedSerializer<Integer> createCheckpointDataSerializer() {
                        return new SimpleVersionedSerializer<Integer>() {
                            @Override
                            public int getVersion() {
                                return 0;
                            }

                            @Override
                            public byte[] serialize(Integer obj) {
                                return new byte[0];
                            }

                            @Override
                            public Integer deserialize(int version, byte[] serialized) {
                                return 1;
                            }
                        };
                    }
                });

        // Verify initial state.
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isZero();
        assertThat(manuallyTriggeredScheduledExecutor.getActiveScheduledTasks().size()).isZero();

        // trigger the first checkpoint. this should succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        // now we should have a completed checkpoint
        assertThat(checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints()).isOne();
        assertThat(checkpointCoordinator.getNumberOfPendingCheckpoints()).isZero();

        // the canceler should be removed now
        assertThat(manuallyTriggeredScheduledExecutor.getActiveScheduledTasks()).isEmpty();

        // validate that the relevant tasks got a confirmation message
        long checkpointId = checkpointIdRef.get();
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertThat(gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId)
                    .isEqualTo(checkpointId);
        }

        CompletedCheckpoint success = checkpointCoordinator.getSuccessfulCheckpoints().get(0);
        assertThat(success.getJobId()).isEqualTo(graph.getJobID());
        assertThat(success.getOperatorStates().size()).isEqualTo(2);

        checkpointCoordinator.shutdown();
    }

    @Test
    void testCompleteCheckpointFailureWithExternallyInducedSource() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
        ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();

        OperatorID opID1 = vertex1.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();
        OperatorID opID2 = vertex2.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();
        TaskStateSnapshot taskOperatorSubtaskStates1 = new TaskStateSnapshot();
        TaskStateSnapshot taskOperatorSubtaskStates2 = new TaskStateSnapshot();
        OperatorSubtaskState subtaskState1 = OperatorSubtaskState.builder().build();
        OperatorSubtaskState subtaskState2 = OperatorSubtaskState.builder().build();
        taskOperatorSubtaskStates1.putSubtaskStateByOperatorID(opID1, subtaskState1);
        taskOperatorSubtaskStates2.putSubtaskStateByOperatorID(opID2, subtaskState2);

        // Create a mock OperatorCoordinatorCheckpointContext which completes the checkpoint
        // immediately.
        AtomicBoolean coordCheckpointDone = new AtomicBoolean(false);
        OperatorCoordinatorCheckpointContext coordinatorCheckpointContext =
                new CheckpointCoordinatorTestingUtils
                                .MockOperatorCheckpointCoordinatorContextBuilder()
                        .setOnCallingCheckpointCoordinator(
                                (checkpointId, result) -> {
                                    coordCheckpointDone.set(true);
                                    result.complete(new byte[0]);
                                })
                        .setOperatorID(opID1)
                        .build();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCoordinatorsToCheckpoint(
                                Collections.singleton(coordinatorCheckpointContext))
                        .setCheckpointStorage(
                                new JobManagerCheckpointStorage() {
                                    private static final long serialVersionUID =
                                            8134582566514272546L;

                                    // Throw exception when finalizing the checkpoint.
                                    @Override
                                    public CheckpointStorageAccess createCheckpointStorage(
                                            JobID jobId) throws IOException {
                                        return new MemoryBackendCheckpointStorageAccess(
                                                jobId, null, null, true, 100) {
                                            @Override
                                            public CheckpointStorageLocation
                                                    initializeLocationForCheckpoint(
                                                            long checkpointId) throws IOException {
                                                return new NonPersistentMetadataCheckpointStorageLocation(
                                                        1000) {
                                                    @Override
                                                    public CheckpointMetadataOutputStream
                                                            createMetadataOutputStream()
                                                                    throws IOException {
                                                        throw new IOException(
                                                                "Artificial Exception");
                                                    }
                                                };
                                            }
                                        };
                                    }
                                })
                        .build(graph);
        AtomicReference<Long> checkpointIdRef = new AtomicReference<>();

        // Add a master hook which triggers and acks the task checkpoint immediately.
        // In this case the task checkpoints would complete before the job master checkpoint
        // completes.
        checkpointCoordinator.addMasterHook(
                new MasterTriggerRestoreHook<Integer>() {
                    @Override
                    public String getIdentifier() {
                        return "anything";
                    }

                    @Override
                    @Nullable
                    public CompletableFuture<Integer> triggerCheckpoint(
                            long checkpointId, long timestamp, Executor executor) throws Exception {
                        assertThat(coordCheckpointDone.get())
                                .as("The coordinator checkpoint should have finished.")
                                .isTrue();
                        // Acknowledge the checkpoint in the master hooks so the task snapshots
                        // complete before
                        // the master state snapshot completes.
                        checkpointIdRef.set(checkpointId);
                        AcknowledgeCheckpoint acknowledgeCheckpoint1 =
                                new AcknowledgeCheckpoint(
                                        graph.getJobID(),
                                        attemptID1,
                                        checkpointId,
                                        new CheckpointMetrics(),
                                        taskOperatorSubtaskStates1);
                        AcknowledgeCheckpoint acknowledgeCheckpoint2 =
                                new AcknowledgeCheckpoint(
                                        graph.getJobID(),
                                        attemptID2,
                                        checkpointId,
                                        new CheckpointMetrics(),
                                        taskOperatorSubtaskStates2);
                        checkpointCoordinator.receiveAcknowledgeMessage(
                                acknowledgeCheckpoint1, TASK_MANAGER_LOCATION_INFO);
                        checkpointCoordinator.receiveAcknowledgeMessage(
                                acknowledgeCheckpoint2, TASK_MANAGER_LOCATION_INFO);
                        return null;
                    }

                    @Override
                    public void restoreCheckpoint(long checkpointId, Integer checkpointData)
                            throws Exception {}

                    @Override
                    public SimpleVersionedSerializer<Integer> createCheckpointDataSerializer() {
                        return new SimpleVersionedSerializer<Integer>() {
                            @Override
                            public int getVersion() {
                                return 0;
                            }

                            @Override
                            public byte[] serialize(Integer obj) {
                                return new byte[0];
                            }

                            @Override
                            public Integer deserialize(int version, byte[] serialized) {
                                return 1;
                            }
                        };
                    }
                });

        // trigger the first checkpoint. this should succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertThat(checkpointFuture).isCompletedExceptionally();
        assertThat(checkpointCoordinator.getSuccessfulCheckpoints()).isEmpty();
    }

    @Test
    void testResetCalledInRegionRecovery() throws Exception {
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        TestResetHook hook = new TestResetHook("id");

        checkpointCoordinator.addMasterHook(hook);
        assertThat(hook.resetCalled).isFalse();
        checkpointCoordinator.restoreLatestCheckpointedStateToSubtasks(Collections.emptySet());
        assertThat(hook.resetCalled).isTrue();
    }

    @Test
    void testNotifyCheckpointAbortionInOperatorCoordinator() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex executionVertex = graph.getJobVertex(jobVertexID).getTaskVertices()[0];
        ExecutionAttemptID attemptID = executionVertex.getCurrentExecutionAttempt().getAttemptId();

        CheckpointCoordinatorTestingUtils.MockOperatorCoordinatorCheckpointContext context =
                new CheckpointCoordinatorTestingUtils
                                .MockOperatorCheckpointCoordinatorContextBuilder()
                        .setOperatorID(new OperatorID())
                        .setOnCallingCheckpointCoordinator(
                                (ignored, future) -> future.complete(new byte[0]))
                        .build();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCoordinatorsToCheckpoint(Collections.singleton(context))
                        .build(graph);
        try {
            // Trigger checkpoint 1.
            checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            long checkpointId1 =
                    Collections.max(checkpointCoordinator.getPendingCheckpoints().keySet());
            // Trigger checkpoint 2.
            checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();

            // Acknowledge checkpoint 2. This should abort checkpoint 1.
            long checkpointId2 =
                    Collections.max(checkpointCoordinator.getPendingCheckpoints().keySet());
            AcknowledgeCheckpoint acknowledgeCheckpoint1 =
                    new AcknowledgeCheckpoint(
                            graph.getJobID(),
                            attemptID,
                            checkpointId2,
                            new CheckpointMetrics(),
                            null);
            checkpointCoordinator.receiveAcknowledgeMessage(acknowledgeCheckpoint1, "");

            // OperatorCoordinator should have been notified of the abortion of checkpoint 1.
            assertThat(context.getAbortedCheckpoints())
                    .isEqualTo(Collections.singletonList(checkpointId1));
            assertThat(context.getCompletedCheckpoints())
                    .isEqualTo(Collections.singletonList(checkpointId2));
        } finally {
            checkpointCoordinator.shutdown();
        }
    }

    @Test
    void testTimeoutWhileCheckpointOperatorCoordinatorNotFinishing() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        CheckpointCoordinatorTestingUtils.MockOperatorCoordinatorCheckpointContext context =
                new CheckpointCoordinatorTestingUtils
                                .MockOperatorCheckpointCoordinatorContextBuilder()
                        .setOperatorID(new OperatorID())
                        .setOnCallingCheckpointCoordinator(
                                (ignored, future) -> {
                                    // Never complete
                                })
                        .build();

        ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setCheckpointTimeout(10)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCoordinatorsToCheckpoint(Collections.singleton(context))
                        .build(graph);
        try {
            checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertThat(checkpointCoordinator.isTriggering()).isTrue();

            manuallyTriggeredScheduledExecutor.triggerNonPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();

            assertThat(checkpointCoordinator.isTriggering()).isFalse();
        } finally {
            checkpointCoordinator.shutdown();
            executorService.shutdownNow();
        }
    }

    @Test
    void testAbortingBeforeTriggeringCheckpointOperatorCoordinator() throws Exception {
        // Warn: The case is fragile since a specific order of executing the tasks is required to
        // reproduce the issue.
        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        String trigger = "Trigger";
        String abort = "Abort";
        final List<String> notificationSequence = new ArrayList<>();
        CheckpointCoordinatorTestingUtils.MockOperatorCoordinatorCheckpointContext context =
                new CheckpointCoordinatorTestingUtils
                                .MockOperatorCheckpointCoordinatorContextBuilder()
                        .setOperatorID(new OperatorID())
                        .setOnCallingCheckpointCoordinator(
                                (id, future) -> {
                                    notificationSequence.add(trigger + id);
                                    future.complete(new byte[0]);
                                })
                        .setOnCallingAbortCurrentTriggering(() -> notificationSequence.add(abort))
                        .build();

        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setCheckpointTimeout(10)
                                        .build())
                        .setIoExecutor(manuallyTriggeredScheduledExecutor)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCoordinatorsToCheckpoint(Collections.singleton(context))
                        .build(graph);
        try {
            checkpointCoordinator.triggerCheckpoint(false);
            // trigger three times to trigger checkpoint, to get checkpoint id and create pending
            // checkpoint
            manuallyTriggeredScheduledExecutor.trigger();
            manuallyTriggeredScheduledExecutor.trigger();
            manuallyTriggeredScheduledExecutor.trigger();

            // declineCheckpoint should be called after pending checkpoint is created but before the
            // following steps
            declineCheckpoint(1L, checkpointCoordinator, jobVertexID, graph);
            // then trigger all tasks. the order is 1.initialize checkpoint location, 2.handle
            // checkpoint abortion, 3.trigger coordinator checkpointing for the aborted checkpoint.
            // The disordering of abortion and triggering was causing an error
            manuallyTriggeredScheduledExecutor.triggerAll();

            // trigger the next checkpoint
            checkState(!checkpointCoordinator.isTriggering());
            checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();

            assertThat(
                            !notificationSequence.contains(trigger + "1")
                                    || notificationSequence.indexOf(trigger + "1")
                                            < notificationSequence.indexOf(abort))
                    .isTrue();
        } finally {
            checkpointCoordinator.shutdown();
        }
    }

    @Test
    void testReportLatestCompletedCheckpointIdWithAbort() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTransitToRunning(false)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        ExecutionVertex task = graph.getJobVertex(jobVertexID).getTaskVertices()[0];

        AtomicLong reportedCheckpointId = new AtomicLong(-1);
        LogicalSlot slot =
                new TestingLogicalSlotBuilder()
                        .setTaskManagerGateway(
                                new SimpleAckingTaskManagerGateway() {
                                    @Override
                                    public void notifyCheckpointAborted(
                                            ExecutionAttemptID executionAttemptID,
                                            JobID jobId,
                                            long checkpointId,
                                            long latestCompletedCheckpointId,
                                            long timestamp) {
                                        reportedCheckpointId.set(latestCompletedCheckpointId);
                                    }
                                })
                        .createTestingLogicalSlot();
        ExecutionGraphTestUtils.setVertexResource(task, slot);
        task.getCurrentExecutionAttempt().transitionState(ExecutionState.RUNNING);

        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setAllowCheckpointsAfterTasksFinished(true)
                        .build(graph);

        // Trigger a successful checkpoint
        CompletableFuture<CompletedCheckpoint> result =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        long completedCheckpointId =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        task.getCurrentExecutionAttempt().getAttemptId(),
                        completedCheckpointId,
                        new CheckpointMetrics(),
                        new TaskStateSnapshot()),
                "localhost");
        assertThat(result).isDone();
        assertThat(result).isNotCompletedExceptionally();

        result = checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        long abortedCheckpointId =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        task.getCurrentExecutionAttempt().getAttemptId(),
                        abortedCheckpointId,
                        new CheckpointException(CHECKPOINT_EXPIRED)),
                "localhost");
        assertThat(result).isCompletedExceptionally();

        assertThat(reportedCheckpointId).hasValue(completedCheckpointId);
    }

    @Test
    void testBaseLocationsNotInitialized() throws Exception {
        File checkpointDir = TempDirUtils.newFolder(tmpFolder);
        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .setTransitToRunning(false)
                        .build(EXECUTOR_RESOURCE.getExecutor());
        new CheckpointCoordinatorBuilder()
                .setCheckpointCoordinatorConfiguration(
                        CheckpointCoordinatorConfiguration.builder()
                                .setCheckpointInterval(Long.MAX_VALUE)
                                .build())
                .setCheckpointStorage(new FileSystemCheckpointStorage(checkpointDir.toURI()))
                .build(graph);
        Path jobCheckpointPath =
                new Path(checkpointDir.getAbsolutePath(), graph.getJobID().toString());
        FileSystem fs = FileSystem.get(checkpointDir.toURI());

        // directory will not be created if checkpointing is disabled
        assertThat(fs.exists(jobCheckpointPath)).isFalse();
    }

    private CheckpointCoordinator getCheckpointCoordinator(ExecutionGraph graph) throws Exception {
        return new CheckpointCoordinatorBuilder()
                .setCheckpointCoordinatorConfiguration(
                        CheckpointCoordinatorConfiguration.builder()
                                .setAlignedCheckpointTimeout(Long.MAX_VALUE)
                                .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                .build())
                .setTimer(manuallyTriggeredScheduledExecutor)
                .build(graph);
    }

    private CheckpointCoordinator getCheckpointCoordinator(
            ExecutionGraph graph, CheckpointFailureManager failureManager) throws Exception {

        return new CheckpointCoordinatorBuilder()
                .setTimer(manuallyTriggeredScheduledExecutor)
                .setFailureManager(failureManager)
                .build(graph);
    }

    private CheckpointCoordinator getCheckpointCoordinator(ScheduledExecutor timer)
            throws Exception {
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .addJobVertex(new JobVertexID())
                        .build(EXECUTOR_RESOURCE.getExecutor());

        // set up the coordinator and validate the initial state
        return new CheckpointCoordinatorBuilder().setTimer(timer).build(graph);
    }

    private CheckpointFailureManager getCheckpointFailureManager(String errorMsg) {
        return new CheckpointFailureManager(
                0,
                new CheckpointFailureManager.FailJobCallback() {
                    @Override
                    public void failJob(Throwable cause) {
                        throw new RuntimeException(errorMsg);
                    }

                    @Override
                    public void failJobDueToTaskFailure(
                            Throwable cause, ExecutionAttemptID failingTask) {
                        throw new RuntimeException(errorMsg);
                    }
                });
    }

    private PendingCheckpoint declineSynchronousSavepoint(
            final JobID jobId,
            final CheckpointCoordinator coordinator,
            final ExecutionAttemptID attemptID,
            final Throwable reason) {

        final long checkpointId =
                coordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        final PendingCheckpoint checkpoint = coordinator.getPendingCheckpoints().get(checkpointId);
        coordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        jobId,
                        attemptID,
                        checkpointId,
                        new CheckpointException(CHECKPOINT_DECLINED, reason)),
                TASK_MANAGER_LOCATION_INFO);
        return checkpoint;
    }

    private void performIncrementalCheckpoint(
            JobID jobId,
            CheckpointCoordinator checkpointCoordinator,
            ExecutionJobVertex jobVertex1,
            List<KeyGroupRange> keyGroupPartitions1,
            int cpSequenceNumber)
            throws Exception {

        // trigger the checkpoint
        checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertThat(checkpointCoordinator.getPendingCheckpoints()).hasSize(1);
        long checkpointId =
                Iterables.getOnlyElement(checkpointCoordinator.getPendingCheckpoints().keySet());

        for (int index = 0; index < jobVertex1.getParallelism(); index++) {

            KeyGroupRange keyGroupRange = keyGroupPartitions1.get(index);

            List<HandleAndLocalPath> privateState = new ArrayList<>();
            privateState.add(
                    HandleAndLocalPath.of(
                            new TestingStreamStateHandle("private-1", new byte[] {'p'}),
                            "private-1"));

            List<HandleAndLocalPath> sharedState = new ArrayList<>();

            // let all but the first CP overlap by one shared state.
            if (cpSequenceNumber > 0) {
                sharedState.add(
                        HandleAndLocalPath.of(
                                new TestingStreamStateHandle(
                                        "shared-" + (cpSequenceNumber - 1) + "-" + keyGroupRange,
                                        new byte[] {'s'}),
                                "shared-" + (cpSequenceNumber - 1)));
            }

            sharedState.add(
                    HandleAndLocalPath.of(
                            new TestingStreamStateHandle(
                                    "shared-" + cpSequenceNumber + "-" + keyGroupRange,
                                    new byte[] {'s'}),
                            "shared-" + cpSequenceNumber));

            IncrementalRemoteKeyedStateHandle managedState =
                    spy(
                            new IncrementalRemoteKeyedStateHandle(
                                    new UUID(42L, 42L),
                                    keyGroupRange,
                                    checkpointId,
                                    sharedState,
                                    privateState,
                                    spy(new ByteStreamStateHandle("meta", new byte[] {'m'}))));

            OperatorSubtaskState operatorSubtaskState =
                    spy(OperatorSubtaskState.builder().setManagedKeyedState(managedState).build());

            Map<OperatorID, OperatorSubtaskState> opStates = new HashMap<>();

            opStates.put(
                    jobVertex1.getOperatorIDs().get(0).getGeneratedOperatorID(),
                    operatorSubtaskState);

            TaskStateSnapshot taskStateSnapshot = new TaskStateSnapshot(opStates);

            AcknowledgeCheckpoint acknowledgeCheckpoint =
                    new AcknowledgeCheckpoint(
                            jobId,
                            jobVertex1
                                    .getTaskVertices()[index]
                                    .getCurrentExecutionAttempt()
                                    .getAttemptId(),
                            checkpointId,
                            new CheckpointMetrics(),
                            taskStateSnapshot);

            checkpointCoordinator.receiveAcknowledgeMessage(
                    acknowledgeCheckpoint, TASK_MANAGER_LOCATION_INFO);
        }
    }

    private static class IOExceptionCheckpointIDCounter extends CheckpointIDCounterWithOwner {
        @Override
        public long getAndIncrement() throws Exception {
            checkNotNull(owner);
            throw new IOException("disk is error!");
        }
    }

    private static class IOExceptionCheckpointStorage extends JobManagerCheckpointStorage {
        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
            return new MemoryBackendCheckpointStorageAccess(jobId, null, null, true, 100) {
                @Override
                public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId)
                        throws IOException {
                    throw new IOException("disk is error!");
                }
            };
        }
    }

    private static class StoppingCheckpointIDCounter extends CheckpointIDCounterWithOwner {
        @Override
        public long getAndIncrement() throws Exception {
            checkNotNull(owner);
            owner.stopCheckpointScheduler();
            return super.getAndIncrement();
        }
    }

    private static class CheckpointIDCounterWithOwner extends StandaloneCheckpointIDCounter {
        protected CheckpointCoordinator owner;

        void setOwner(CheckpointCoordinator coordinator) {
            this.owner = checkNotNull(coordinator);
        }
    }

    private static class TestFailJobCallback implements CheckpointFailureManager.FailJobCallback {

        private int invokeCounter = 0;

        @Override
        public void failJob(Throwable cause) {
            invokeCounter++;
        }

        @Override
        public void failJobDueToTaskFailure(
                final Throwable cause, final ExecutionAttemptID executionAttemptID) {
            invokeCounter++;
        }

        public int getInvokeCounter() {
            return invokeCounter;
        }
    }

    private static class TestResetHook implements MasterTriggerRestoreHook<String> {

        private final String id;
        boolean resetCalled;

        TestResetHook(String id) {
            this.id = id;
            this.resetCalled = false;
        }

        @Override
        public String getIdentifier() {
            return id;
        }

        @Override
        public void reset() throws Exception {
            resetCalled = true;
        }

        @Override
        public CompletableFuture<String> triggerCheckpoint(
                long checkpointId, long timestamp, Executor executor) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void restoreCheckpoint(long checkpointId, @Nullable String checkpointData)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public SimpleVersionedSerializer<String> createCheckpointDataSerializer() {
            throw new UnsupportedOperationException();
        }
    }

    private static void verifyDiscard(
            List<List<HandleAndLocalPath>> sharedHandles,
            Function<Integer, TernaryBoolean> checkpointVerify) {
        for (List<HandleAndLocalPath> cpList : sharedHandles) {
            for (HandleAndLocalPath handleAndLocalPath : cpList) {
                String key = handleAndLocalPath.getLocalPath();
                int checkpointID = Integer.parseInt(String.valueOf(key.charAt(key.length() - 1)));
                DiscardRecordedStateObject.verifyDiscard(
                        handleAndLocalPath.getHandle(), checkpointVerify.apply(checkpointID));
            }
        }
    }

    private TestingStreamStateHandle handle() {
        return new TestingStreamStateHandle();
    }

    private void declineCheckpoint(
            long checkpointId,
            CheckpointCoordinator coordinator,
            JobVertexID nackVertexID,
            ExecutionGraph graph) {
        coordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        graph.getJobVertex(nackVertexID)
                                .getTaskVertices()[0]
                                .getCurrentExecutionAttempt()
                                .getAttemptId(),
                        checkpointId,
                        new CheckpointException(CHECKPOINT_DECLINED)),
                "test");
    }

    private void ackCheckpoint(
            long checkpointId,
            CheckpointCoordinator coordinator,
            JobVertexID ackVertexID,
            ExecutionGraph graph,
            TestingStreamStateHandle metaState,
            TestingStreamStateHandle privateState,
            TestingStreamStateHandle sharedState)
            throws CheckpointException {
        List<HandleAndLocalPath> sharedStateList =
                new ArrayList<>(
                        singletonList(HandleAndLocalPath.of(sharedState, "shared-state-key")));
        List<HandleAndLocalPath> privateStateList =
                new ArrayList<>(
                        singletonList(HandleAndLocalPath.of(privateState, "private-state-key")));

        ExecutionJobVertex jobVertex = graph.getJobVertex(ackVertexID);
        OperatorID operatorID = jobVertex.getOperatorIDs().get(0).getGeneratedOperatorID();
        coordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        jobVertex.getTaskVertices()[0].getCurrentExecutionAttempt().getAttemptId(),
                        checkpointId,
                        new CheckpointMetrics(),
                        new TaskStateSnapshot(
                                singletonMap(
                                        operatorID,
                                        OperatorSubtaskState.builder()
                                                .setManagedKeyedState(
                                                        new IncrementalRemoteKeyedStateHandle(
                                                                UUID.randomUUID(),
                                                                KeyGroupRange.of(0, 9),
                                                                checkpointId,
                                                                sharedStateList,
                                                                privateStateList,
                                                                metaState))
                                                .build()))),
                "test");
    }

    static class OperatorSubtaskStateMock {
        OperatorSubtaskState subtaskState;
        TestingOperatorStateHandle managedOpHandle;
        TestingOperatorStateHandle rawOpHandle;

        OperatorSubtaskStateMock() {
            this.managedOpHandle = new TestingOperatorStateHandle();
            this.rawOpHandle = new TestingOperatorStateHandle();
            this.subtaskState =
                    OperatorSubtaskState.builder()
                            .setManagedOperatorState(managedOpHandle)
                            .setRawOperatorState(rawOpHandle)
                            .build();
        }

        public OperatorSubtaskState getSubtaskState() {
            return this.subtaskState;
        }

        public void reset() {
            managedOpHandle.reset();
            rawOpHandle.reset();
        }

        public void verifyDiscard() {
            assert (managedOpHandle.isDiscarded() && rawOpHandle.discarded);
        }

        public void verifyNotDiscard() {
            assert (!managedOpHandle.isDiscarded() && !rawOpHandle.isDiscarded());
        }

        private static class TestingOperatorStateHandle implements OperatorStateHandle {

            private static final long serialVersionUID = 983594934287613083L;

            boolean discarded;

            @Override
            public Map<String, StateMetaInfo> getStateNameToPartitionOffsets() {
                return Collections.emptyMap();
            }

            @Override
            public FSDataInputStream openInputStream() throws IOException {
                throw new IOException("Cannot open input streams in testing implementation.");
            }

            @Override
            public PhysicalStateHandleID getStreamStateHandleID() {
                throw new RuntimeException("Cannot return ID in testing implementation.");
            }

            @Override
            public Optional<byte[]> asBytesIfInMemory() {
                return Optional.empty();
            }

            @Override
            public StreamStateHandle getDelegateStateHandle() {
                return null;
            }

            @Override
            public void discardState() throws Exception {
                assertThat(discarded).isFalse();
                discarded = true;
            }

            @Override
            public long getStateSize() {
                return 0L;
            }

            public void reset() {
                discarded = false;
            }

            public boolean isDiscarded() {
                return discarded;
            }
        }
    }
}
