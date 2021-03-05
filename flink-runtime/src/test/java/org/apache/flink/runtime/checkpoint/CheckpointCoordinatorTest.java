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
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.concurrent.ScheduledExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;
import org.apache.flink.runtime.messages.checkpoint.AcknowledgeCheckpoint;
import org.apache.flink.runtime.messages.checkpoint.DeclineCheckpoint;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SharedStateRegistry;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemoryBackendCheckpointStorageAccess;
import org.apache.flink.runtime.state.memory.NonPersistentMetadataCheckpointStorageLocation;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.testutils.TestCompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testutils.DirectScheduledExecutorService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.TriFunctionWithException;

import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import com.sun.istack.Nullable;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.mockito.verification.VerificationMode;

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
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_ASYNC_EXCEPTION;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_DECLINED;
import static org.apache.flink.runtime.checkpoint.CheckpointFailureReason.CHECKPOINT_EXPIRED;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for the checkpoint coordinator. */
public class CheckpointCoordinatorTest extends TestLogger {

    @Test
    public void testAbortedCheckpointStatsUpdatedAfterFailure() throws Exception {
        testReportStatsAfterFailure(
                1L,
                (coordinator, execution, metrics) -> {
                    coordinator.reportStats(1L, execution.getAttemptId(), metrics);
                    return null;
                });
    }

    @Test
    public void testCheckpointStatsUpdatedAfterFailure() throws Exception {
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
                        .build();

        ExecutionVertex decliningVertex =
                executionGraph.getJobVertex(decliningVertexID).getTaskVertices()[0];
        ExecutionVertex lateReportVertex =
                executionGraph.getJobVertex(lateReportVertexID).getTaskVertices()[0];
        CheckpointStatsTracker statsTracker =
                new CheckpointStatsTracker(
                        Integer.MAX_VALUE,
                        CheckpointCoordinatorConfiguration.builder().build(),
                        new UnregisteredMetricsGroup());
        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(executionGraph)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();
        coordinator.setCheckpointStatsTracker(statsTracker);

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

    private void assertStatsEqual(
            long checkpointId,
            JobVertexID jobVertexID,
            int subtasIdx,
            CheckpointMetrics expected,
            AbstractCheckpointStats actual) {
        assertEquals(checkpointId, actual.getCheckpointId());
        assertEquals(CheckpointStatsStatus.FAILED, actual.getStatus());
        assertEquals(expected.getTotalBytesPersisted(), actual.getStateSize());
        assertEquals(0, actual.getNumberOfAcknowledgedSubtasks());
        SubtaskStateStats taskStats =
                actual.getAllTaskStateStats().stream()
                        .filter(s -> s.getJobVertexId().equals(jobVertexID))
                        .findAny()
                        .get()
                        .getSubtaskStats()[subtasIdx];
        assertEquals(
                expected.getAlignmentDurationNanos() / 1_000_000, taskStats.getAlignmentDuration());
        assertEquals(expected.getUnalignedCheckpoint(), taskStats.getUnalignedCheckpoint());
        assertEquals(expected.getAsyncDurationMillis(), taskStats.getAsyncCheckpointDuration());
        assertEquals(
                expected.getAlignmentDurationNanos() / 1_000_000, taskStats.getAlignmentDuration());
        assertEquals(
                expected.getCheckpointStartDelayNanos() / 1_000_000,
                taskStats.getCheckpointStartDelay());
    }

    private static final String TASK_MANAGER_LOCATION_INFO = "Unknown location";

    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @Rule public TemporaryFolder tmpFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    @Test
    public void testScheduleTriggerRequestDuringShutdown() throws Exception {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        CheckpointCoordinator coordinator =
                getCheckpointCoordinator(new ScheduledExecutorServiceAdapter(executor));
        coordinator.shutdown();
        executor.shutdownNow();
        coordinator.scheduleTriggerRequest(); // shouldn't fail
    }

    @Test
    public void testMinCheckpointPause() throws Exception {
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
                            .build();

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
                            .setExecutionGraph(graph)
                            .build();
            coordinator.startCheckpointScheduler();

            coordinator.triggerCheckpoint(
                    true); // trigger, execute, and later complete by receiveAcknowledgeMessage
            coordinator.triggerCheckpoint(
                    true); // enqueue and later see if it gets executed in the middle of
            // receiveAcknowledgeMessage
            while (coordinator.getNumberOfPendingCheckpoints()
                    == 0) { // wait for at least 1 request to be fully processed
                Thread.sleep(10);
            }
            coordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptId, 1L),
                    TASK_MANAGER_LOCATION_INFO);
            Thread.sleep(pause / 2);
            assertEquals(0, coordinator.getNumberOfPendingCheckpoints());
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
    public void testCheckpointAbortsIfTriggerTasksAreNotExecuted() {
        try {
            // set up the coordinator and validate the initial state
            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(new JobVertexID())
                            .addJobVertex(new JobVertexID(), false)
                            .setTransitToRunning(false)
                            .build();

            CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(graph);

            // nothing should be happening
            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            // trigger the first checkpoint. this should not succeed
            final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                    checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertTrue(checkpointFuture.isCompletedExceptionally());

            // still, nothing should be happening
            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCheckpointAbortsIfTriggerTasksAreFinished() {
        try {
            JobVertexID jobVertexID1 = new JobVertexID();
            JobVertexID jobVertexID2 = new JobVertexID();

            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexID1)
                            .addJobVertex(jobVertexID2, false)
                            .build();

            CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(graph);
            Arrays.stream(graph.getJobVertex(jobVertexID1).getTaskVertices())
                    .forEach(task -> task.getCurrentExecutionAttempt().markFinished());

            // nothing should be happening
            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            // trigger the first checkpoint. this should not succeed
            final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                    checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertTrue(checkpointFuture.isCompletedExceptionally());

            // still, nothing should be happening
            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCheckpointTriggeredAfterSomeTasksFinishedIfAllowed() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1, 3, 256)
                        .addJobVertex(jobVertexID2, 3, 256)
                        .build();
        ExecutionJobVertex jobVertex1 = graph.getJobVertex(jobVertexID1);
        ExecutionJobVertex jobVertex2 = graph.getJobVertex(jobVertexID2);

        jobVertex1.getTaskVertices()[0].getCurrentExecutionAttempt().markFinished();
        jobVertex1.getTaskVertices()[1].getCurrentExecutionAttempt().markFinished();
        jobVertex2.getTaskVertices()[1].getCurrentExecutionAttempt().markFinished();

        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setAllowCheckpointsAfterTasksFinished(true)
                        .build();

        CheckpointStatsTracker statsTracker =
                new CheckpointStatsTracker(
                        Integer.MAX_VALUE,
                        CheckpointCoordinatorConfiguration.builder().build(),
                        new UnregisteredMetricsGroup());
        checkpointCoordinator.setCheckpointStatsTracker(statsTracker);

        // nothing should be happening
        assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
        assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

        // trigger the first checkpoint. this will not fail because we allow checkpointing even with
        // finished tasks
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertFalse(checkpointFuture.isDone());
        assertFalse(checkpointFuture.isCompletedExceptionally());

        // Triggering should succeed
        assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());
        PendingCheckpoint pendingCheckpoint =
                checkpointCoordinator.getPendingCheckpoints().values().iterator().next();
        AbstractCheckpointStats checkpointStats =
                statsTracker
                        .createSnapshot()
                        .getHistory()
                        .getCheckpointById(pendingCheckpoint.getCheckpointID());
        assertEquals(3, checkpointStats.getNumberOfAcknowledgedSubtasks());
        for (ExecutionVertex task :
                Arrays.asList(
                        jobVertex1.getTaskVertices()[0],
                        jobVertex1.getTaskVertices()[1],
                        jobVertex2.getTaskVertices()[1])) {

            // those tasks that are already finished are automatically marked as acknowledged
            assertNotNull(
                    checkpointStats.getTaskStateStats(task.getJobvertexId())
                            .getSubtaskStats()[task.getParallelSubtaskIndex()]);
        }
    }

    @Test
    public void testTriggerAndDeclineCheckpointThenFailureManagerThrowsException()
            throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .build();
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
            assertFalse(checkpoint.isDisposed());
            assertFalse(checkpoint.areTasksFullyAcknowledged());

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
            // expected
            assertTrue(e instanceof RuntimeException);
            assertEquals(errorMsg, e.getMessage());
        } finally {
            try {
                checkpointCoordinator.shutdown();
            } catch (Exception e) {
                e.printStackTrace();
                fail(e.getMessage());
            }
        }
    }

    @Test
    public void testExpiredCheckpointExceedsTolerableFailureNumber() throws Exception {
        // create some mock Execution vertices that receive the checkpoint trigger messages
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .addJobVertex(new JobVertexID())
                        .build();

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
            // expected
            assertTrue(e instanceof RuntimeException);
            assertEquals(errorMsg, e.getMessage());
        } finally {
            checkpointCoordinator.shutdown();
        }
    }

    @Test
    public void testTriggerAndDeclineSyncCheckpointFailureSimple() {
        testTriggerAndDeclineCheckpointSimple(CHECKPOINT_DECLINED);
    }

    @Test
    public void testTriggerAndDeclineAsyncCheckpointFailureSimple() {
        testTriggerAndDeclineCheckpointSimple(CHECKPOINT_ASYNC_EXCEPTION);
    }

    /**
     * This test triggers a checkpoint and then sends a decline checkpoint message from one of the
     * tasks. The expected behaviour is that said checkpoint is discarded and a new checkpoint is
     * triggered.
     */
    private void testTriggerAndDeclineCheckpointSimple(
            CheckpointFailureReason checkpointFailureReason) {
        try {
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
                            .build();

            ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
            ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

            ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
            ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();

            TestFailJobCallback failJobCallback = new TestFailJobCallback();
            // set up the coordinator and validate the initial state
            CheckpointCoordinator checkpointCoordinator =
                    new CheckpointCoordinatorBuilder()
                            .setExecutionGraph(graph)
                            .setCheckpointCoordinatorConfiguration(
                                    CheckpointCoordinatorConfiguration.builder()
                                            .setAlignmentTimeout(Long.MAX_VALUE)
                                            .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                            .build())
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .setCheckpointFailureManager(
                                    new CheckpointFailureManager(0, failJobCallback))
                            .build();

            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            // trigger the first checkpoint. this should succeed
            final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                    checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

            // validate that we have a pending checkpoint
            assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            // we have one task scheduled that will cancel after timeout
            assertEquals(1, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

            long checkpointId =
                    checkpointCoordinator
                            .getPendingCheckpoints()
                            .entrySet()
                            .iterator()
                            .next()
                            .getKey();
            PendingCheckpoint checkpoint =
                    checkpointCoordinator.getPendingCheckpoints().get(checkpointId);

            assertNotNull(checkpoint);
            assertEquals(checkpointId, checkpoint.getCheckpointId());
            assertEquals(graph.getJobID(), checkpoint.getJobId());
            assertEquals(2, checkpoint.getNumberOfNonAcknowledgedTasks());
            assertEquals(0, checkpoint.getNumberOfAcknowledgedTasks());
            assertEquals(0, checkpoint.getOperatorStates().size());
            assertFalse(checkpoint.isDisposed());
            assertFalse(checkpoint.areTasksFullyAcknowledged());

            // check that the vertices received the trigger checkpoint message
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
                CheckpointCoordinatorTestingUtils.TriggeredCheckpoint triggeredCheckpoint =
                        gateway.getOnlyTriggeredCheckpoint(
                                vertex.getCurrentExecutionAttempt().getAttemptId());
                assertEquals(checkpointId, triggeredCheckpoint.checkpointId);
                assertEquals(checkpoint.getCheckpointTimestamp(), triggeredCheckpoint.timestamp);
                assertEquals(
                        CheckpointOptions.forCheckpointWithDefaultLocation(),
                        triggeredCheckpoint.checkpointOptions);
            }

            // acknowledge from one of the tasks
            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointId),
                    "Unknown location");
            assertEquals(1, checkpoint.getNumberOfAcknowledgedTasks());
            assertEquals(1, checkpoint.getNumberOfNonAcknowledgedTasks());
            assertFalse(checkpoint.isDisposed());
            assertFalse(checkpoint.areTasksFullyAcknowledged());

            // acknowledge the same task again (should not matter)
            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointId),
                    "Unknown location");
            assertFalse(checkpoint.isDisposed());
            assertFalse(checkpoint.areTasksFullyAcknowledged());

            // decline checkpoint from the other task, this should cancel the checkpoint
            // and trigger a new one
            checkpointCoordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            graph.getJobID(), attemptID1, checkpointId, checkpointException),
                    TASK_MANAGER_LOCATION_INFO);
            assertTrue(checkpoint.isDisposed());

            // the canceler is also removed
            assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

            // validate that we have no new pending checkpoint
            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

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
            assertTrue(checkpoint.isDisposed());
            assertEquals(1, failJobCallback.getInvokeCounter());

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /**
     * This test triggers two checkpoints and then sends a decline message from one of the tasks for
     * the first checkpoint. This should discard the first checkpoint while not triggering a new
     * checkpoint because a later checkpoint is already in progress.
     */
    @Test
    public void testTriggerAndDeclineCheckpointComplex() {
        try {
            JobVertexID jobVertexID1 = new JobVertexID();
            JobVertexID jobVertexID2 = new JobVertexID();

            CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                    new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexID1)
                            .addJobVertex(jobVertexID2)
                            .setTaskManagerGateway(gateway)
                            .build();

            ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
            ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

            ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
            ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();
            CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(graph);

            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
            assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

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
            assertEquals(2, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
            assertEquals(2, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

            Iterator<Map.Entry<Long, PendingCheckpoint>> it =
                    checkpointCoordinator.getPendingCheckpoints().entrySet().iterator();
            long checkpoint1Id = it.next().getKey();
            long checkpoint2Id = it.next().getKey();
            PendingCheckpoint checkpoint1 =
                    checkpointCoordinator.getPendingCheckpoints().get(checkpoint1Id);
            PendingCheckpoint checkpoint2 =
                    checkpointCoordinator.getPendingCheckpoints().get(checkpoint2Id);

            assertNotNull(checkpoint1);
            assertEquals(checkpoint1Id, checkpoint1.getCheckpointId());
            assertEquals(graph.getJobID(), checkpoint1.getJobId());
            assertEquals(2, checkpoint1.getNumberOfNonAcknowledgedTasks());
            assertEquals(0, checkpoint1.getNumberOfAcknowledgedTasks());
            assertEquals(0, checkpoint1.getOperatorStates().size());
            assertFalse(checkpoint1.isDisposed());
            assertFalse(checkpoint1.areTasksFullyAcknowledged());

            assertNotNull(checkpoint2);
            assertEquals(checkpoint2Id, checkpoint2.getCheckpointId());
            assertEquals(graph.getJobID(), checkpoint2.getJobId());
            assertEquals(2, checkpoint2.getNumberOfNonAcknowledgedTasks());
            assertEquals(0, checkpoint2.getNumberOfAcknowledgedTasks());
            assertEquals(0, checkpoint2.getOperatorStates().size());
            assertFalse(checkpoint2.isDisposed());
            assertFalse(checkpoint2.areTasksFullyAcknowledged());

            // check that the vertices received the trigger checkpoint message
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
                List<CheckpointCoordinatorTestingUtils.TriggeredCheckpoint> triggeredCheckpoints =
                        gateway.getTriggeredCheckpoints(
                                vertex.getCurrentExecutionAttempt().getAttemptId());
                assertEquals(2, triggeredCheckpoints.size());
                assertEquals(checkpoint1Id, triggeredCheckpoints.get(0).checkpointId);
                assertEquals(checkpoint2Id, triggeredCheckpoints.get(1).checkpointId);
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
                assertEquals(
                        checkpoint1Id,
                        gateway.getOnlyNotifiedAbortedCheckpoint(
                                        vertex.getCurrentExecutionAttempt().getAttemptId())
                                .checkpointId);
            }

            assertTrue(checkpoint1.isDisposed());

            // validate that we have only one pending checkpoint left
            assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
            assertEquals(1, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

            // validate that it is the same second checkpoint from earlier
            long checkpointIdNew =
                    checkpointCoordinator
                            .getPendingCheckpoints()
                            .entrySet()
                            .iterator()
                            .next()
                            .getKey();
            PendingCheckpoint checkpointNew =
                    checkpointCoordinator.getPendingCheckpoints().get(checkpointIdNew);
            assertEquals(checkpoint2Id, checkpointIdNew);

            assertNotNull(checkpointNew);
            assertEquals(checkpointIdNew, checkpointNew.getCheckpointId());
            assertEquals(graph.getJobID(), checkpointNew.getJobId());
            assertEquals(2, checkpointNew.getNumberOfNonAcknowledgedTasks());
            assertEquals(0, checkpointNew.getNumberOfAcknowledgedTasks());
            assertEquals(0, checkpointNew.getOperatorStates().size());
            assertFalse(checkpointNew.isDisposed());
            assertFalse(checkpointNew.areTasksFullyAcknowledged());
            assertNotEquals(checkpoint1.getCheckpointId(), checkpointNew.getCheckpointId());

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
            assertTrue(checkpoint1.isDisposed());

            // will not notify abort message again
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
                assertEquals(
                        1,
                        gateway.getNotifiedAbortedCheckpoints(
                                        vertex.getCurrentExecutionAttempt().getAttemptId())
                                .size());
            }

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testTriggerAndConfirmSimpleCheckpoint() {
        try {
            JobVertexID jobVertexID1 = new JobVertexID();
            JobVertexID jobVertexID2 = new JobVertexID();

            CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                    new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexID1)
                            .addJobVertex(jobVertexID2)
                            .setTaskManagerGateway(gateway)
                            .build();

            ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
            ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

            ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
            ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();
            CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(graph);

            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
            assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

            // trigger the first checkpoint. this should succeed
            final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                    checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

            // validate that we have a pending checkpoint
            assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
            assertEquals(1, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

            long checkpointId =
                    checkpointCoordinator
                            .getPendingCheckpoints()
                            .entrySet()
                            .iterator()
                            .next()
                            .getKey();
            PendingCheckpoint checkpoint =
                    checkpointCoordinator.getPendingCheckpoints().get(checkpointId);

            assertNotNull(checkpoint);
            assertEquals(checkpointId, checkpoint.getCheckpointId());
            assertEquals(graph.getJobID(), checkpoint.getJobId());
            assertEquals(2, checkpoint.getNumberOfNonAcknowledgedTasks());
            assertEquals(0, checkpoint.getNumberOfAcknowledgedTasks());
            assertEquals(0, checkpoint.getOperatorStates().size());
            assertFalse(checkpoint.isDisposed());
            assertFalse(checkpoint.areTasksFullyAcknowledged());

            // check that the vertices received the trigger checkpoint message
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
                ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
                assertEquals(
                        checkpointId, gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId);
            }

            OperatorID opID1 =
                    vertex1.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();
            OperatorID opID2 =
                    vertex2.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();
            TaskStateSnapshot taskOperatorSubtaskStates1 = mock(TaskStateSnapshot.class);
            TaskStateSnapshot taskOperatorSubtaskStates2 = mock(TaskStateSnapshot.class);
            OperatorSubtaskState subtaskState1 = mock(OperatorSubtaskState.class);
            OperatorSubtaskState subtaskState2 = mock(OperatorSubtaskState.class);
            when(taskOperatorSubtaskStates1.getSubtaskStateByOperatorID(opID1))
                    .thenReturn(subtaskState1);
            when(taskOperatorSubtaskStates2.getSubtaskStateByOperatorID(opID2))
                    .thenReturn(subtaskState2);

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
            assertEquals(1, checkpoint.getNumberOfAcknowledgedTasks());
            assertEquals(1, checkpoint.getNumberOfNonAcknowledgedTasks());
            assertFalse(checkpoint.isDisposed());
            assertFalse(checkpoint.areTasksFullyAcknowledged());
            verify(taskOperatorSubtaskStates2, never())
                    .registerSharedStates(any(SharedStateRegistry.class));

            // acknowledge the same task again (should not matter)
            checkpointCoordinator.receiveAcknowledgeMessage(
                    acknowledgeCheckpoint1, TASK_MANAGER_LOCATION_INFO);
            assertFalse(checkpoint.isDisposed());
            assertFalse(checkpoint.areTasksFullyAcknowledged());
            verify(subtaskState2, never()).registerSharedStates(any(SharedStateRegistry.class));

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
            assertTrue(checkpoint.isDisposed());

            // the now we should have a completed checkpoint
            assertEquals(1, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());

            // the canceler should be removed now
            assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

            // validate that the subtasks states have registered their shared states.
            {
                verify(subtaskState1, times(1))
                        .registerSharedStates(any(SharedStateRegistry.class));
                verify(subtaskState2, times(1))
                        .registerSharedStates(any(SharedStateRegistry.class));
            }

            // validate that the relevant tasks got a confirmation message
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
                ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
                assertEquals(
                        checkpointId,
                        gateway.getOnlyNotifiedCompletedCheckpoint(attemptId).checkpointId);
            }

            CompletedCheckpoint success = checkpointCoordinator.getSuccessfulCheckpoints().get(0);
            assertEquals(graph.getJobID(), success.getJobId());
            assertEquals(checkpoint.getCheckpointId(), success.getCheckpointID());
            assertEquals(2, success.getOperatorStates().size());

            // ---------------
            // trigger another checkpoint and see that this one replaces the other checkpoint
            // ---------------
            gateway.resetCount();
            checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();

            long checkpointIdNew =
                    checkpointCoordinator
                            .getPendingCheckpoints()
                            .entrySet()
                            .iterator()
                            .next()
                            .getKey();
            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, checkpointIdNew),
                    TASK_MANAGER_LOCATION_INFO);
            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointIdNew),
                    TASK_MANAGER_LOCATION_INFO);

            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(1, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
            assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

            CompletedCheckpoint successNew =
                    checkpointCoordinator.getSuccessfulCheckpoints().get(0);
            assertEquals(graph.getJobID(), successNew.getJobId());
            assertEquals(checkpointIdNew, successNew.getCheckpointID());
            assertTrue(successNew.getOperatorStates().isEmpty());

            // validate that the relevant tasks got a confirmation message
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
                ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
                assertEquals(
                        checkpointIdNew,
                        gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId);
                assertEquals(
                        checkpointIdNew,
                        gateway.getOnlyNotifiedCompletedCheckpoint(attemptId).checkpointId);
            }

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testMultipleConcurrentCheckpoints() {
        try {
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
                            .build();

            ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
            ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];
            ExecutionVertex vertex3 = graph.getJobVertex(jobVertexID3).getTaskVertices()[0];

            ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
            ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();
            ExecutionAttemptID attemptID3 = vertex3.getCurrentExecutionAttempt().getAttemptId();

            // set up the coordinator and validate the initial state
            CheckpointCoordinator checkpointCoordinator =
                    new CheckpointCoordinatorBuilder()
                            .setExecutionGraph(graph)
                            .setCheckpointCoordinatorConfiguration(
                                    CheckpointCoordinatorConfiguration.builder()
                                            .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                            .build())
                            .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build();

            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            // trigger the first checkpoint. this should succeed
            final CompletableFuture<CompletedCheckpoint> checkpointFuture1 =
                    checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            FutureUtils.throwIfCompletedExceptionally(checkpointFuture1);

            assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            PendingCheckpoint pending1 =
                    checkpointCoordinator.getPendingCheckpoints().values().iterator().next();
            long checkpointId1 = pending1.getCheckpointId();

            // trigger messages should have been sent
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
                ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
                assertEquals(
                        checkpointId1, gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId);
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

            assertEquals(2, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            PendingCheckpoint pending2;
            {
                Iterator<PendingCheckpoint> all =
                        checkpointCoordinator.getPendingCheckpoints().values().iterator();
                PendingCheckpoint cc1 = all.next();
                PendingCheckpoint cc2 = all.next();
                pending2 = pending1 == cc1 ? cc2 : cc1;
            }
            long checkpointId2 = pending2.getCheckpointId();

            // trigger messages should have been sent
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
                ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
                assertEquals(
                        checkpointId2, gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId);
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
            assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(1, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
            assertTrue(pending1.isDisposed());

            // the first confirm message should be out
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2, vertex3)) {
                ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
                assertEquals(
                        checkpointId1,
                        gateway.getOnlyNotifiedCompletedCheckpoint(attemptId).checkpointId);
            }

            // send the last remaining ack for the second checkpoint
            gateway.resetCount();
            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptID3, checkpointId2),
                    TASK_MANAGER_LOCATION_INFO);

            // now, the second checkpoint should be confirmed
            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(2, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
            assertTrue(pending2.isDisposed());

            // the second commit message should be out
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2, vertex3)) {
                ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
                assertEquals(
                        checkpointId2,
                        gateway.getOnlyNotifiedCompletedCheckpoint(attemptId).checkpointId);
            }

            // validate the committed checkpoints
            List<CompletedCheckpoint> scs = checkpointCoordinator.getSuccessfulCheckpoints();

            CompletedCheckpoint sc1 = scs.get(0);
            assertEquals(checkpointId1, sc1.getCheckpointID());
            assertEquals(graph.getJobID(), sc1.getJobId());
            assertTrue(sc1.getOperatorStates().isEmpty());

            CompletedCheckpoint sc2 = scs.get(1);
            assertEquals(checkpointId2, sc2.getCheckpointID());
            assertEquals(graph.getJobID(), sc2.getJobId());
            assertTrue(sc2.getOperatorStates().isEmpty());

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testSuccessfulCheckpointSubsumesUnsuccessful() {
        try {
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
                            .build();

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
                            .setExecutionGraph(graph)
                            .setCheckpointCoordinatorConfiguration(
                                    CheckpointCoordinatorConfiguration.builder()
                                            .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                            .build())
                            .setCompletedCheckpointStore(completedCheckpointStore)
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build();

            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            // trigger the first checkpoint. this should succeed
            final CompletableFuture<CompletedCheckpoint> checkpointFuture1 =
                    checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            FutureUtils.throwIfCompletedExceptionally(checkpointFuture1);

            assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            PendingCheckpoint pending1 =
                    checkpointCoordinator.getPendingCheckpoints().values().iterator().next();
            long checkpointId1 = pending1.getCheckpointId();

            // trigger messages should have been sent
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
                ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
                assertEquals(
                        checkpointId1, gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId);
            }

            OperatorID opID1 =
                    vertex1.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();
            OperatorID opID2 =
                    vertex2.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();
            OperatorID opID3 =
                    vertex3.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();

            TaskStateSnapshot taskOperatorSubtaskStates11 = spy(new TaskStateSnapshot());
            TaskStateSnapshot taskOperatorSubtaskStates12 = spy(new TaskStateSnapshot());
            TaskStateSnapshot taskOperatorSubtaskStates13 = spy(new TaskStateSnapshot());

            OperatorSubtaskState subtaskState11 = mock(OperatorSubtaskState.class);
            OperatorSubtaskState subtaskState12 = mock(OperatorSubtaskState.class);
            OperatorSubtaskState subtaskState13 = mock(OperatorSubtaskState.class);
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

            assertEquals(2, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            PendingCheckpoint pending2;
            {
                Iterator<PendingCheckpoint> all =
                        checkpointCoordinator.getPendingCheckpoints().values().iterator();
                PendingCheckpoint cc1 = all.next();
                PendingCheckpoint cc2 = all.next();
                pending2 = pending1 == cc1 ? cc2 : cc1;
            }
            long checkpointId2 = pending2.getCheckpointId();

            TaskStateSnapshot taskOperatorSubtaskStates21 = spy(new TaskStateSnapshot());
            TaskStateSnapshot taskOperatorSubtaskStates22 = spy(new TaskStateSnapshot());
            TaskStateSnapshot taskOperatorSubtaskStates23 = spy(new TaskStateSnapshot());

            OperatorSubtaskState subtaskState21 = mock(OperatorSubtaskState.class);
            OperatorSubtaskState subtaskState22 = mock(OperatorSubtaskState.class);
            OperatorSubtaskState subtaskState23 = mock(OperatorSubtaskState.class);

            taskOperatorSubtaskStates21.putSubtaskStateByOperatorID(opID1, subtaskState21);
            taskOperatorSubtaskStates22.putSubtaskStateByOperatorID(opID2, subtaskState22);
            taskOperatorSubtaskStates23.putSubtaskStateByOperatorID(opID3, subtaskState23);

            // trigger messages should have been sent
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
                ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
                assertEquals(
                        checkpointId2, gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId);
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
            assertTrue(pending1.isDisposed());
            assertTrue(pending2.isDisposed());

            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(1, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            // validate that all received subtask states in the first checkpoint have been discarded
            verify(subtaskState11, times(1)).discardState();
            verify(subtaskState12, times(1)).discardState();

            // validate that all subtask states in the second checkpoint are not discarded
            verify(subtaskState21, never()).discardState();
            verify(subtaskState22, never()).discardState();
            verify(subtaskState23, never()).discardState();

            // validate the committed checkpoints
            List<CompletedCheckpoint> scs = checkpointCoordinator.getSuccessfulCheckpoints();
            CompletedCheckpoint success = scs.get(0);
            assertEquals(checkpointId2, success.getCheckpointID());
            assertEquals(graph.getJobID(), success.getJobId());
            assertEquals(3, success.getOperatorStates().size());

            // the first confirm message should be out
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2, vertex3)) {
                ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
                assertEquals(
                        checkpointId2,
                        gateway.getOnlyNotifiedCompletedCheckpoint(attemptId).checkpointId);
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
            verify(subtaskState13, times(1)).discardState();

            checkpointCoordinator.shutdown();
            completedCheckpointStore.shutdown(JobStatus.FINISHED, new CheckpointsCleaner());

            // validate that the states in the second checkpoint have been discarded
            verify(subtaskState21, times(1)).discardState();
            verify(subtaskState22, times(1)).discardState();
            verify(subtaskState23, times(1)).discardState();

        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCheckpointTimeoutIsolated() {
        try {
            JobVertexID jobVertexID1 = new JobVertexID();
            JobVertexID jobVertexID2 = new JobVertexID();

            CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                    new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexID1)
                            .addJobVertex(jobVertexID2, false)
                            .setTaskManagerGateway(gateway)
                            .build();

            ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
            ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

            ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();

            // set up the coordinator
            CheckpointCoordinator checkpointCoordinator =
                    new CheckpointCoordinatorBuilder()
                            .setExecutionGraph(graph)
                            .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build();

            // trigger a checkpoint, partially acknowledged
            final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                    checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            FutureUtils.throwIfCompletedExceptionally(checkpointFuture);
            assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());

            PendingCheckpoint checkpoint =
                    checkpointCoordinator.getPendingCheckpoints().values().iterator().next();
            assertFalse(checkpoint.isDisposed());

            OperatorID opID1 =
                    vertex1.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();

            TaskStateSnapshot taskOperatorSubtaskStates1 = spy(new TaskStateSnapshot());
            OperatorSubtaskState subtaskState1 = mock(OperatorSubtaskState.class);
            taskOperatorSubtaskStates1.putSubtaskStateByOperatorID(opID1, subtaskState1);

            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(
                            graph.getJobID(),
                            attemptID1,
                            checkpoint.getCheckpointId(),
                            new CheckpointMetrics(),
                            taskOperatorSubtaskStates1),
                    TASK_MANAGER_LOCATION_INFO);

            // triggers cancelling
            manuallyTriggeredScheduledExecutor.triggerScheduledTasks();
            assertTrue("Checkpoint was not canceled by the timeout", checkpoint.isDisposed());
            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

            // validate that the received states have been discarded
            verify(subtaskState1, times(1)).discardState();

            // no confirm message must have been sent
            for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
                ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
                assertEquals(0, gateway.getNotifiedCompletedCheckpoints(attemptId).size());
            }

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testHandleMessagesForNonExistingCheckpoints() {
        try {
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
                            .build();

            ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];

            ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();

            CheckpointCoordinator checkpointCoordinator =
                    new CheckpointCoordinatorBuilder()
                            .setExecutionGraph(graph)
                            .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build();

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
                            graph.getJobID(), new ExecutionAttemptID(), checkpointId),
                    TASK_MANAGER_LOCATION_INFO);

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
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
    public void testStateCleanupForLateOrUnknownMessages() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2, false)
                        .setTaskManagerGateway(gateway)
                        .build();

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
                        .setExecutionGraph(graph)
                        .setCheckpointCoordinatorConfiguration(chkConfig)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());

        PendingCheckpoint pendingCheckpoint =
                checkpointCoordinator.getPendingCheckpoints().values().iterator().next();

        long checkpointId = pendingCheckpoint.getCheckpointId();

        OperatorID opIDtrigger =
                vertex1.getJobVertex().getOperatorIDs().get(0).getGeneratedOperatorID();

        TaskStateSnapshot taskOperatorSubtaskStatesTrigger = spy(new TaskStateSnapshot());
        OperatorSubtaskState subtaskStateTrigger = mock(OperatorSubtaskState.class);
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
        verify(subtaskStateTrigger, never()).discardState();

        TaskStateSnapshot unknownSubtaskState = mock(TaskStateSnapshot.class);

        // receive an acknowledge message for an unknown vertex
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(
                        graph.getJobID(),
                        new ExecutionAttemptID(),
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
                        new ExecutionAttemptID(),
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
        reset(subtaskStateTrigger);
        checkpointCoordinator.receiveDeclineMessage(
                new DeclineCheckpoint(
                        graph.getJobID(),
                        attemptID1,
                        checkpointId,
                        new CheckpointException(CHECKPOINT_DECLINED)),
                TASK_MANAGER_LOCATION_INFO);

        assertTrue(pendingCheckpoint.isDisposed());

        // check that we've cleaned up the already acknowledged state
        verify(subtaskStateTrigger, times(1)).discardState();

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
                        new ExecutionAttemptID(),
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
                        new ExecutionAttemptID(),
                        checkpointId,
                        new CheckpointMetrics(),
                        unknownSubtaskState2),
                TASK_MANAGER_LOCATION_INFO);

        // we should discard acknowledge messages from an unknown vertex belonging to our job
        verify(unknownSubtaskState2, times(1)).discardState();
    }

    @Test
    public void testMaxConcurrentAttempts1() {
        testMaxConcurrentAttempts(1);
    }

    @Test
    public void testMaxConcurrentAttempts2() {
        testMaxConcurrentAttempts(2);
    }

    @Test
    public void testMaxConcurrentAttempts5() {
        testMaxConcurrentAttempts(5);
    }

    @Test
    public void testTriggerAndConfirmSimpleSavepoint() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .setTaskManagerGateway(gateway)
                        .build();

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
        ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator = getCheckpointCoordinator(graph);

        assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
        assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

        // trigger the first checkpoint. this should succeed
        String savepointDir = tmpFolder.newFolder().getAbsolutePath();
        CompletableFuture<CompletedCheckpoint> savepointFuture =
                checkpointCoordinator.triggerSavepoint(savepointDir);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertFalse(savepointFuture.isDone());

        // validate that we have a pending savepoint
        assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());

        long checkpointId =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        PendingCheckpoint pending = checkpointCoordinator.getPendingCheckpoints().get(checkpointId);

        assertNotNull(pending);
        assertEquals(checkpointId, pending.getCheckpointId());
        assertEquals(graph.getJobID(), pending.getJobId());
        assertEquals(2, pending.getNumberOfNonAcknowledgedTasks());
        assertEquals(0, pending.getNumberOfAcknowledgedTasks());
        assertEquals(0, pending.getOperatorStates().size());
        assertFalse(pending.isDisposed());
        assertFalse(pending.areTasksFullyAcknowledged());
        assertFalse(pending.canBeSubsumed());

        OperatorID opID1 = OperatorID.fromJobVertexID(vertex1.getJobvertexId());
        OperatorID opID2 = OperatorID.fromJobVertexID(vertex2.getJobvertexId());
        TaskStateSnapshot taskOperatorSubtaskStates1 = mock(TaskStateSnapshot.class);
        TaskStateSnapshot taskOperatorSubtaskStates2 = mock(TaskStateSnapshot.class);
        OperatorSubtaskState subtaskState1 = mock(OperatorSubtaskState.class);
        OperatorSubtaskState subtaskState2 = mock(OperatorSubtaskState.class);
        when(taskOperatorSubtaskStates1.getSubtaskStateByOperatorID(opID1))
                .thenReturn(subtaskState1);
        when(taskOperatorSubtaskStates2.getSubtaskStateByOperatorID(opID2))
                .thenReturn(subtaskState2);

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
        assertEquals(1, pending.getNumberOfAcknowledgedTasks());
        assertEquals(1, pending.getNumberOfNonAcknowledgedTasks());
        assertFalse(pending.isDisposed());
        assertFalse(pending.areTasksFullyAcknowledged());
        assertFalse(savepointFuture.isDone());

        // acknowledge the same task again (should not matter)
        checkpointCoordinator.receiveAcknowledgeMessage(
                acknowledgeCheckpoint2, TASK_MANAGER_LOCATION_INFO);
        assertFalse(pending.isDisposed());
        assertFalse(pending.areTasksFullyAcknowledged());
        assertFalse(savepointFuture.isDone());

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
        assertTrue(pending.isDisposed());
        assertNotNull(savepointFuture.get());

        // the now we should have a completed checkpoint
        assertEquals(1, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
        assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());

        // validate that the relevant tasks got a confirmation message
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertEquals(checkpointId, gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId);
        }

        // validate that the shared states are registered
        {
            verify(subtaskState1, times(1)).registerSharedStates(any(SharedStateRegistry.class));
            verify(subtaskState2, times(1)).registerSharedStates(any(SharedStateRegistry.class));
        }

        CompletedCheckpoint success = checkpointCoordinator.getSuccessfulCheckpoints().get(0);
        assertEquals(graph.getJobID(), success.getJobId());
        assertEquals(pending.getCheckpointId(), success.getCheckpointID());
        assertEquals(2, success.getOperatorStates().size());

        // ---------------
        // trigger another checkpoint and see that this one replaces the other checkpoint
        // ---------------
        gateway.resetCount();
        savepointFuture = checkpointCoordinator.triggerSavepoint(savepointDir);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertFalse(savepointFuture.isDone());

        long checkpointIdNew =
                checkpointCoordinator.getPendingCheckpoints().entrySet().iterator().next().getKey();
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, checkpointIdNew),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointIdNew),
                TASK_MANAGER_LOCATION_INFO);

        assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
        assertEquals(1, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

        CompletedCheckpoint successNew = checkpointCoordinator.getSuccessfulCheckpoints().get(0);
        assertEquals(graph.getJobID(), successNew.getJobId());
        assertEquals(checkpointIdNew, successNew.getCheckpointID());
        assertTrue(successNew.getOperatorStates().isEmpty());
        assertNotNull(savepointFuture.get());

        // validate that the first savepoint does not discard its private states.
        verify(subtaskState1, never()).discardState();
        verify(subtaskState2, never()).discardState();

        // validate that the relevant tasks got a confirmation message
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertEquals(
                    checkpointIdNew, gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId);
            assertEquals(
                    checkpointIdNew,
                    gateway.getOnlyNotifiedCompletedCheckpoint(attemptId).checkpointId);
        }

        checkpointCoordinator.shutdown();
    }

    /**
     * Triggers a savepoint and two checkpoints. The second checkpoint completes and subsumes the
     * first checkpoint, but not the first savepoint. Then we trigger another checkpoint and
     * savepoint. The 2nd savepoint completes and subsumes the last checkpoint, but not the first
     * savepoint.
     */
    @Test
    public void testSavepointsAreNotSubsumed() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .build();

        ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];
        ExecutionVertex vertex2 = graph.getJobVertex(jobVertexID2).getTaskVertices()[0];

        ExecutionAttemptID attemptID1 = vertex1.getCurrentExecutionAttempt().getAttemptId();
        ExecutionAttemptID attemptID2 = vertex2.getCurrentExecutionAttempt().getAttemptId();

        StandaloneCheckpointIDCounter counter = new StandaloneCheckpointIDCounter();

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .build())
                        .setCheckpointIDCounter(counter)
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(10))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        String savepointDir = tmpFolder.newFolder().getAbsolutePath();

        // Trigger savepoint and checkpoint
        CompletableFuture<CompletedCheckpoint> savepointFuture1 =
                checkpointCoordinator.triggerSavepoint(savepointDir);

        manuallyTriggeredScheduledExecutor.triggerAll();
        long savepointId1 = counter.getLast();
        assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());

        CompletableFuture<CompletedCheckpoint> checkpointFuture1 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        assertEquals(2, checkpointCoordinator.getNumberOfPendingCheckpoints());
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture1);

        CompletableFuture<CompletedCheckpoint> checkpointFuture2 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture2);
        long checkpointId2 = counter.getLast();
        assertEquals(3, checkpointCoordinator.getNumberOfPendingCheckpoints());

        // 2nd checkpoint should subsume the 1st checkpoint, but not the savepoint
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, checkpointId2),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, checkpointId2),
                TASK_MANAGER_LOCATION_INFO);

        assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());
        assertEquals(1, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

        assertFalse(checkpointCoordinator.getPendingCheckpoints().get(savepointId1).isDisposed());
        assertFalse(savepointFuture1.isDone());

        CompletableFuture<CompletedCheckpoint> checkpointFuture3 =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture3);
        assertEquals(2, checkpointCoordinator.getNumberOfPendingCheckpoints());

        CompletableFuture<CompletedCheckpoint> savepointFuture2 =
                checkpointCoordinator.triggerSavepoint(savepointDir);
        manuallyTriggeredScheduledExecutor.triggerAll();
        long savepointId2 = counter.getLast();
        FutureUtils.throwIfCompletedExceptionally(savepointFuture2);
        assertEquals(3, checkpointCoordinator.getNumberOfPendingCheckpoints());

        // 2nd savepoint should subsume the last checkpoint, but not the 1st savepoint
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, savepointId2),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, savepointId2),
                TASK_MANAGER_LOCATION_INFO);

        assertEquals(1, checkpointCoordinator.getNumberOfPendingCheckpoints());
        assertEquals(2, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());

        assertFalse(checkpointCoordinator.getPendingCheckpoints().get(savepointId1).isDisposed());

        assertFalse(savepointFuture1.isDone());
        assertNotNull(savepointFuture2.get());

        // Ack first savepoint
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, savepointId1),
                TASK_MANAGER_LOCATION_INFO);
        checkpointCoordinator.receiveAcknowledgeMessage(
                new AcknowledgeCheckpoint(graph.getJobID(), attemptID2, savepointId1),
                TASK_MANAGER_LOCATION_INFO);

        assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
        assertEquals(3, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
        assertNotNull(savepointFuture1.get());
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
                            .build();

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
                            .setExecutionGraph(graph)
                            .setCheckpointCoordinatorConfiguration(chkConfig)
                            .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build();

            checkpointCoordinator.startCheckpointScheduler();

            for (int i = 0; i < maxConcurrentAttempts; i++) {
                manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
                manuallyTriggeredScheduledExecutor.triggerAll();
            }

            assertEquals(maxConcurrentAttempts, gateway.getTriggeredCheckpoints(attemptID1).size());
            assertEquals(0, gateway.getNotifiedCompletedCheckpoints(attemptID1).size());

            // now, once we acknowledge one checkpoint, it should trigger the next one
            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, 1L),
                    TASK_MANAGER_LOCATION_INFO);

            final Collection<ScheduledFuture<?>> periodicScheduledTasks =
                    manuallyTriggeredScheduledExecutor.getPeriodicScheduledTask();
            assertEquals(1, periodicScheduledTasks.size());

            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();

            assertEquals(
                    maxConcurrentAttempts + 1, gateway.getTriggeredCheckpoints(attemptID1).size());

            // no further checkpoints should happen
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();

            assertEquals(
                    maxConcurrentAttempts + 1, gateway.getTriggeredCheckpoints(attemptID1).size());

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testMaxConcurrentAttempsWithSubsumption() {
        try {
            final int maxConcurrentAttempts = 2;
            JobVertexID jobVertexID1 = new JobVertexID();

            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexID1)
                            .build();

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
                            .setExecutionGraph(graph)
                            .setCheckpointCoordinatorConfiguration(chkConfig)
                            .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build();

            checkpointCoordinator.startCheckpointScheduler();

            do {
                manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
                manuallyTriggeredScheduledExecutor.triggerAll();
            } while (checkpointCoordinator.getNumberOfPendingCheckpoints() < maxConcurrentAttempts);

            // validate that the pending checkpoints are there
            assertEquals(
                    maxConcurrentAttempts, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertNotNull(checkpointCoordinator.getPendingCheckpoints().get(1L));
            assertNotNull(checkpointCoordinator.getPendingCheckpoints().get(2L));

            // now we acknowledge the second checkpoint, which should subsume the first checkpoint
            // and allow two more checkpoints to be triggered
            // now, once we acknowledge one checkpoint, it should trigger the next one
            checkpointCoordinator.receiveAcknowledgeMessage(
                    new AcknowledgeCheckpoint(graph.getJobID(), attemptID1, 2L),
                    TASK_MANAGER_LOCATION_INFO);

            // after a while, there should be the new checkpoints
            do {
                manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
                manuallyTriggeredScheduledExecutor.triggerAll();
            } while (checkpointCoordinator.getNumberOfPendingCheckpoints() < maxConcurrentAttempts);

            // do the final check
            assertEquals(
                    maxConcurrentAttempts, checkpointCoordinator.getNumberOfPendingCheckpoints());
            assertNotNull(checkpointCoordinator.getPendingCheckpoints().get(3L));
            assertNotNull(checkpointCoordinator.getPendingCheckpoints().get(4L));

            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testPeriodicSchedulingWithInactiveTasks() {
        try {
            JobVertexID jobVertexID1 = new JobVertexID();

            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(jobVertexID1)
                            .setTransitToRunning(false)
                            .build();

            ExecutionVertex vertex1 = graph.getJobVertex(jobVertexID1).getTaskVertices()[0];

            CheckpointCoordinatorConfiguration chkConfig =
                    new CheckpointCoordinatorConfiguration
                                    .CheckpointCoordinatorConfigurationBuilder()
                            .setCheckpointInterval(10) // periodic interval is 10 ms
                            .setCheckpointTimeout(200000) // timeout is very long (200 s)
                            .setMinPauseBetweenCheckpoints(0) // no extra delay
                            .setMaxConcurrentCheckpoints(2) // max two concurrent checkpoints
                            .build();
            CheckpointCoordinator checkpointCoordinator =
                    new CheckpointCoordinatorBuilder()
                            .setExecutionGraph(graph)
                            .setCheckpointCoordinatorConfiguration(chkConfig)
                            .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build();

            checkpointCoordinator.startCheckpointScheduler();

            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();
            // no checkpoint should have started so far
            assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());

            // now move the state to RUNNING
            vertex1.getCurrentExecutionAttempt().transitionState(ExecutionState.RUNNING);

            // the coordinator should start checkpointing now
            manuallyTriggeredScheduledExecutor.triggerPeriodicScheduledTasks();
            manuallyTriggeredScheduledExecutor.triggerAll();

            assertTrue(checkpointCoordinator.getNumberOfPendingCheckpoints() > 0);
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    /** Tests that the savepoints can be triggered concurrently. */
    @Test
    public void testConcurrentSavepoints() throws Exception {
        int numSavepoints = 5;

        JobVertexID jobVertexID1 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .build();

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
                        .setExecutionGraph(graph)
                        .setCheckpointCoordinatorConfiguration(chkConfig)
                        .setCheckpointIDCounter(checkpointIDCounter)
                        .setCompletedCheckpointStore(new StandaloneCompletedCheckpointStore(2))
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        List<CompletableFuture<CompletedCheckpoint>> savepointFutures = new ArrayList<>();

        String savepointDir = tmpFolder.newFolder().getAbsolutePath();

        // Trigger savepoints
        for (int i = 0; i < numSavepoints; i++) {
            savepointFutures.add(checkpointCoordinator.triggerSavepoint(savepointDir));
        }

        // After triggering multiple savepoints, all should in progress
        for (CompletableFuture<CompletedCheckpoint> savepointFuture : savepointFutures) {
            assertFalse(savepointFuture.isDone());
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
            assertNotNull(savepointFuture.get());
        }
    }

    /** Tests that no minimum delay between savepoints is enforced. */
    @Test
    public void testMinDelayBetweenSavepoints() throws Exception {
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
                        .build();

        String savepointDir = tmpFolder.newFolder().getAbsolutePath();

        CompletableFuture<CompletedCheckpoint> savepoint0 =
                checkpointCoordinator.triggerSavepoint(savepointDir);
        assertFalse("Did not trigger savepoint", savepoint0.isDone());

        CompletableFuture<CompletedCheckpoint> savepoint1 =
                checkpointCoordinator.triggerSavepoint(savepointDir);
        assertFalse("Did not trigger savepoint", savepoint1.isDone());
    }

    /** Tests that the externalized checkpoint configuration is respected. */
    @Test
    public void testExternalizedCheckpoints() throws Exception {
        try {

            ExecutionGraph graph =
                    new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                            .addJobVertex(new JobVertexID())
                            .build();

            // set up the coordinator and validate the initial state
            CheckpointCoordinatorConfiguration chkConfig =
                    new CheckpointCoordinatorConfiguration
                                    .CheckpointCoordinatorConfigurationBuilder()
                            .setCheckpointRetentionPolicy(
                                    CheckpointRetentionPolicy.RETAIN_ON_FAILURE)
                            .build();
            CheckpointCoordinator checkpointCoordinator =
                    new CheckpointCoordinatorBuilder()
                            .setExecutionGraph(graph)
                            .setCheckpointCoordinatorConfiguration(chkConfig)
                            .setTimer(manuallyTriggeredScheduledExecutor)
                            .build();

            CompletableFuture<CompletedCheckpoint> checkpointFuture =
                    checkpointCoordinator.triggerCheckpoint(false);
            manuallyTriggeredScheduledExecutor.triggerAll();
            FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

            for (PendingCheckpoint checkpoint :
                    checkpointCoordinator.getPendingCheckpoints().values()) {
                CheckpointProperties props = checkpoint.getProps();
                CheckpointProperties expected =
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.RETAIN_ON_FAILURE);

                assertEquals(expected, props);
            }

            // the now we should have a completed checkpoint
            checkpointCoordinator.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
            fail(e.getMessage());
        }
    }

    @Test
    public void testCreateKeyGroupPartitions() {
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
                Assert.fail("Could not find expected key-group " + i + " in range " + range);
            }
        }
    }

    @Test
    public void testPartitionableStateRepartitioning() {
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
            Assert.assertEquals(1, previousParallelOpInstanceState.size());

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

        OperatorStateRepartitioner repartitioner = RoundRobinOperatorStateRepartitioner.INSTANCE;

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
                            actual.get(sh.getDelegateStateHandle());
                    if (stateToOffsets == null) {
                        stateToOffsets = new HashMap<>();
                        actual.put(sh.getDelegateStateHandle(), stateToOffsets);
                    }

                    List<Long> actualOffs = stateToOffsets.get(namedState.getKey());
                    if (actualOffs == null) {
                        actualOffs = new ArrayList<>();
                        stateToOffsets.put(namedState.getKey(), actualOffs);
                    }
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
            Assert.assertTrue(
                    "Difference in partition load is > 1 : " + maxLoadDiff, maxLoadDiff <= 1);
        }
        Assert.assertEquals(expectedTotalPartitions, actualTotalPartitions);
        Assert.assertEquals(expected, actual);
    }

    /** Tests that the pending checkpoint stats callbacks are created. */
    @Test
    public void testCheckpointStatsTrackerPendingCheckpointCallback() throws Exception {
        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        CheckpointStatsTracker tracker = mock(CheckpointStatsTracker.class);
        checkpointCoordinator.setCheckpointStatsTracker(tracker);

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
    public void testCheckpointStatsTrackerRestoreCallback() throws Exception {
        StandaloneCompletedCheckpointStore store = new StandaloneCompletedCheckpointStore(1);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCompletedCheckpointStore(store)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        store.addCheckpoint(
                new CompletedCheckpoint(
                        new JobID(),
                        0,
                        0,
                        0,
                        Collections.<OperatorID, OperatorState>emptyMap(),
                        Collections.<MasterState>emptyList(),
                        CheckpointProperties.forCheckpoint(
                                CheckpointRetentionPolicy.NEVER_RETAIN_AFTER_TERMINATION),
                        new TestCompletedCheckpointStorageLocation()),
                new CheckpointsCleaner(),
                () -> {});

        CheckpointStatsTracker tracker = mock(CheckpointStatsTracker.class);
        checkpointCoordinator.setCheckpointStatsTracker(tracker);

        assertTrue(
                checkpointCoordinator.restoreLatestCheckpointedStateToAll(
                        Collections.emptySet(), true));

        verify(tracker, times(1)).reportRestoredCheckpoint(any(RestoredCheckpointStats.class));
    }

    @Test
    public void testSharedStateRegistrationOnRestore() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();

        int parallelism1 = 2;
        int maxParallelism1 = 4;

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1, parallelism1, maxParallelism1)
                        .build();

        ExecutionJobVertex jobVertex1 = graph.getJobVertex(jobVertexID1);

        EmbeddedCompletedCheckpointStore store = new EmbeddedCompletedCheckpointStore(10);

        final List<SharedStateRegistry> createdSharedStateRegistries = new ArrayList<>(2);

        // set up the coordinator and validate the initial state
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setCompletedCheckpointStore(store)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setSharedStateRegistryFactory(
                                deleteExecutor -> {
                                    SharedStateRegistry instance =
                                            new SharedStateRegistry(deleteExecutor);
                                    createdSharedStateRegistries.add(instance);
                                    return instance;
                                })
                        .build();

        final int numCheckpoints = 3;

        List<KeyGroupRange> keyGroupPartitions1 =
                StateAssignmentOperation.createKeyGroupPartitions(maxParallelism1, parallelism1);

        for (int i = 0; i < numCheckpoints; ++i) {
            performIncrementalCheckpoint(
                    graph.getJobID(), checkpointCoordinator, jobVertex1, keyGroupPartitions1, i);
        }

        List<CompletedCheckpoint> completedCheckpoints =
                checkpointCoordinator.getSuccessfulCheckpoints();
        assertEquals(numCheckpoints, completedCheckpoints.size());

        int sharedHandleCount = 0;

        List<Map<StateHandleID, StreamStateHandle>> sharedHandlesByCheckpoint =
                new ArrayList<>(numCheckpoints);

        for (int i = 0; i < numCheckpoints; ++i) {
            sharedHandlesByCheckpoint.add(new HashMap<>(2));
        }

        int cp = 0;
        for (CompletedCheckpoint completedCheckpoint : completedCheckpoints) {
            for (OperatorState taskState : completedCheckpoint.getOperatorStates().values()) {
                for (OperatorSubtaskState subtaskState : taskState.getStates()) {
                    for (KeyedStateHandle keyedStateHandle : subtaskState.getManagedKeyedState()) {
                        // test we are once registered with the current registry
                        verify(keyedStateHandle, times(1))
                                .registerSharedStates(createdSharedStateRegistries.get(0));
                        IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle =
                                (IncrementalRemoteKeyedStateHandle) keyedStateHandle;

                        sharedHandlesByCheckpoint
                                .get(cp)
                                .putAll(incrementalKeyedStateHandle.getSharedState());

                        for (StreamStateHandle streamStateHandle :
                                incrementalKeyedStateHandle.getSharedState().values()) {
                            assertTrue(
                                    !(streamStateHandle instanceof PlaceholderStreamStateHandle));
                            verify(streamStateHandle, never()).discardState();
                            ++sharedHandleCount;
                        }

                        for (StreamStateHandle streamStateHandle :
                                incrementalKeyedStateHandle.getPrivateState().values()) {
                            verify(streamStateHandle, never()).discardState();
                        }

                        verify(incrementalKeyedStateHandle.getMetaStateHandle(), never())
                                .discardState();
                    }

                    verify(subtaskState, never()).discardState();
                }
            }
            ++cp;
        }

        // 2 (parallelism) x (1 (CP0) + 2 (CP1) + 2 (CP2)) = 10
        assertEquals(10, sharedHandleCount);

        // discard CP0
        store.removeOldestCheckpoint();

        // we expect no shared state was discarded because the state of CP0 is still referenced by
        // CP1
        for (Map<StateHandleID, StreamStateHandle> cpList : sharedHandlesByCheckpoint) {
            for (StreamStateHandle streamStateHandle : cpList.values()) {
                verify(streamStateHandle, never()).discardState();
            }
        }

        // shutdown the store
        store.shutdown(JobStatus.SUSPENDED, new CheckpointsCleaner());

        // restore the store
        Set<ExecutionJobVertex> tasks = new HashSet<>();
        tasks.add(jobVertex1);
        assertTrue(checkpointCoordinator.restoreLatestCheckpointedStateToAll(tasks, false));

        // validate that all shared states are registered again after the recovery.
        cp = 0;
        for (CompletedCheckpoint completedCheckpoint : completedCheckpoints) {
            for (OperatorState taskState : completedCheckpoint.getOperatorStates().values()) {
                for (OperatorSubtaskState subtaskState : taskState.getStates()) {
                    for (KeyedStateHandle keyedStateHandle : subtaskState.getManagedKeyedState()) {
                        VerificationMode verificationMode;
                        // test we are once registered with the new registry
                        if (cp > 0) {
                            verificationMode = times(1);
                        } else {
                            verificationMode = never();
                        }

                        // check that all are registered with the new registry
                        verify(keyedStateHandle, verificationMode)
                                .registerSharedStates(createdSharedStateRegistries.get(1));
                    }
                }
            }
            ++cp;
        }

        // discard CP1
        store.removeOldestCheckpoint();

        // we expect that all shared state from CP0 is no longer referenced and discarded. CP2 is
        // still live and also
        // references the state from CP1, so we expect they are not discarded.
        for (Map<StateHandleID, StreamStateHandle> cpList : sharedHandlesByCheckpoint) {
            for (Map.Entry<StateHandleID, StreamStateHandle> entry : cpList.entrySet()) {
                String key = entry.getKey().getKeyString();
                int belongToCP = Integer.parseInt(String.valueOf(key.charAt(key.length() - 1)));
                if (belongToCP == 0) {
                    verify(entry.getValue(), times(1)).discardState();
                } else {
                    verify(entry.getValue(), never()).discardState();
                }
            }
        }

        // discard CP2
        store.removeOldestCheckpoint();

        // we expect all shared state was discarded now, because all CPs are
        for (Map<StateHandleID, StreamStateHandle> cpList : sharedHandlesByCheckpoint) {
            for (StreamStateHandle streamStateHandle : cpList.values()) {
                verify(streamStateHandle, times(1)).discardState();
            }
        }
    }

    @Test
    public void jobFailsIfInFlightSynchronousSavepointIsDiscarded() throws Exception {
        final Tuple2<Integer, Throwable> invocationCounterAndException = Tuple2.of(0, null);
        final Throwable expectedRootCause = new IOException("Custom-Exception");

        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .build();

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
                coordinator.triggerSynchronousSavepoint(false, "test-dir");

        manuallyTriggeredScheduledExecutor.triggerAll();
        final PendingCheckpoint syncSavepoint =
                declineSynchronousSavepoint(
                        graph.getJobID(), coordinator, attemptID1, expectedRootCause);

        assertTrue(syncSavepoint.isDisposed());

        try {
            savepointFuture.get();
            fail("Expected Exception not found.");
        } catch (ExecutionException e) {
            final Throwable cause = ExceptionUtils.stripExecutionException(e);
            assertTrue(cause instanceof CheckpointException);
            assertEquals(expectedRootCause.getMessage(), cause.getCause().getCause().getMessage());
        }

        assertEquals(1L, invocationCounterAndException.f0.intValue());
        assertTrue(
                invocationCounterAndException.f1 instanceof CheckpointException
                        && invocationCounterAndException
                                .f1
                                .getCause()
                                .getCause()
                                .getMessage()
                                .equals(expectedRootCause.getMessage()));

        coordinator.shutdown();
    }

    /** Tests that do not trigger checkpoint when stop the coordinator after the eager pre-check. */
    @Test
    public void testTriggerCheckpointAfterCancel() throws Exception {
        // set up the coordinator
        TestingCheckpointIDCounter idCounter = new TestingCheckpointIDCounter();
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointIDCounter(idCounter)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();
        idCounter.setOwner(checkpointCoordinator);

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
                fail("should not trigger periodic checkpoint after stop the coordinator.");
            } catch (ExecutionException e) {
                final Optional<CheckpointException> checkpointExceptionOptional =
                        ExceptionUtils.findThrowable(e, CheckpointException.class);
                assertTrue(checkpointExceptionOptional.isPresent());
                assertEquals(
                        CheckpointFailureReason.PERIODIC_SCHEDULER_SHUTDOWN,
                        checkpointExceptionOptional.get().getCheckpointFailureReason());
            }
        } finally {
            checkpointCoordinator.shutdown();
        }
    }

    @Test
    public void testSavepointScheduledInUnalignedMode() throws Exception {
        int maxConcurrentCheckpoints = 1;
        int checkpointRequestsToSend = 10;
        int activeRequests = 0;

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .build();
        CheckpointCoordinator coordinator =
                new CheckpointCoordinatorBuilder()
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setUnalignedCheckpointsEnabled(true)
                                        .setMaxConcurrentCheckpoints(maxConcurrentCheckpoints)
                                        .build())
                        .setExecutionGraph(graph)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();
        try {
            List<Future<?>> checkpointFutures = new ArrayList<>(checkpointRequestsToSend);
            coordinator.startCheckpointScheduler();
            while (activeRequests < checkpointRequestsToSend) {
                checkpointFutures.add(coordinator.triggerCheckpoint(true));
                activeRequests++;
            }
            assertEquals(
                    activeRequests - maxConcurrentCheckpoints, coordinator.getNumQueuedRequests());

            Future<?> savepointFuture = coordinator.triggerSavepoint("/tmp");
            manuallyTriggeredScheduledExecutor.triggerAll();
            assertEquals(
                    ++activeRequests - maxConcurrentCheckpoints,
                    coordinator.getNumQueuedRequests());

            coordinator.receiveDeclineMessage(
                    new DeclineCheckpoint(
                            graph.getJobID(),
                            new ExecutionAttemptID(),
                            1L,
                            new CheckpointException(CHECKPOINT_DECLINED)),
                    "none");
            manuallyTriggeredScheduledExecutor.triggerAll();

            activeRequests--; // savepoint triggered
            assertEquals(
                    activeRequests - maxConcurrentCheckpoints, coordinator.getNumQueuedRequests());
            assertEquals(1, checkpointFutures.stream().filter(Future::isDone).count());

            assertFalse(savepointFuture.isDone());
            assertEquals(maxConcurrentCheckpoints, coordinator.getNumberOfPendingCheckpoints());
            CheckpointProperties props =
                    coordinator.getPendingCheckpoints().values().iterator().next().getProps();
            assertTrue(props.isSavepoint());
            assertFalse(props.forceCheckpoint());
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
    public void testExternallyInducedSourceWithOperatorCoordinator() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway =
                new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .setTaskManagerGateway(gateway)
                        .build();

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
                        .setExecutionGraph(graph)
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCoordinatorsToCheckpoint(
                                Collections.singleton(coordinatorCheckpointContext))
                        .build();
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
                        assertTrue(
                                "The coordinator checkpoint should have finished.",
                                coordCheckpointDone.get());
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
                            public byte[] serialize(Integer obj) throws IOException {
                                return new byte[0];
                            }

                            @Override
                            public Integer deserialize(int version, byte[] serialized)
                                    throws IOException {
                                return 1;
                            }
                        };
                    }
                });

        // Verify initial state.
        assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());
        assertEquals(0, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
        assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

        // trigger the first checkpoint. this should succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();
        FutureUtils.throwIfCompletedExceptionally(checkpointFuture);

        // now we should have a completed checkpoint
        assertEquals(1, checkpointCoordinator.getNumberOfRetainedSuccessfulCheckpoints());
        assertEquals(0, checkpointCoordinator.getNumberOfPendingCheckpoints());

        // the canceler should be removed now
        assertEquals(0, manuallyTriggeredScheduledExecutor.getScheduledTasks().size());

        // validate that the relevant tasks got a confirmation message
        long checkpointId = checkpointIdRef.get();
        for (ExecutionVertex vertex : Arrays.asList(vertex1, vertex2)) {
            ExecutionAttemptID attemptId = vertex.getCurrentExecutionAttempt().getAttemptId();
            assertEquals(checkpointId, gateway.getOnlyTriggeredCheckpoint(attemptId).checkpointId);
        }

        CompletedCheckpoint success = checkpointCoordinator.getSuccessfulCheckpoints().get(0);
        assertEquals(graph.getJobID(), success.getJobId());
        assertEquals(2, success.getOperatorStates().size());

        checkpointCoordinator.shutdown();
    }

    @Test
    public void testCompleteCheckpointFailureWithExternallyInducedSource() throws Exception {
        JobVertexID jobVertexID1 = new JobVertexID();
        JobVertexID jobVertexID2 = new JobVertexID();

        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID1)
                        .addJobVertex(jobVertexID2)
                        .build();

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
                        .setExecutionGraph(graph)
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
                                                jobId, null, null, 100) {
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
                        .build();
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
                        assertTrue(
                                "The coordinator checkpoint should have finished.",
                                coordCheckpointDone.get());
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
                            public byte[] serialize(Integer obj) throws IOException {
                                return new byte[0];
                            }

                            @Override
                            public Integer deserialize(int version, byte[] serialized)
                                    throws IOException {
                                return 1;
                            }
                        };
                    }
                });

        // trigger the first checkpoint. this should succeed
        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                checkpointCoordinator.triggerCheckpoint(false);
        manuallyTriggeredScheduledExecutor.triggerAll();

        assertTrue(checkpointFuture.isCompletedExceptionally());
        assertTrue(checkpointCoordinator.getSuccessfulCheckpoints().isEmpty());
    }

    @Test
    public void testResetCalledInRegionRecovery() throws Exception {
        CheckpointCoordinator checkpointCoordinator =
                new CheckpointCoordinatorBuilder()
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .build();

        TestResetHook hook = new TestResetHook("id");

        checkpointCoordinator.addMasterHook(hook);
        assertFalse(hook.resetCalled);
        checkpointCoordinator.restoreLatestCheckpointedStateToSubtasks(Collections.emptySet());
        assertTrue(hook.resetCalled);
    }

    @Test
    public void testNotifyCheckpointAbortionInOperatorCoordinator() throws Exception {
        JobVertexID jobVertexID = new JobVertexID();
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(jobVertexID)
                        .build();

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
                        .setExecutionGraph(graph)
                        .setCheckpointCoordinatorConfiguration(
                                CheckpointCoordinatorConfiguration.builder()
                                        .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                        .build())
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCoordinatorsToCheckpoint(Collections.singleton(context))
                        .build();
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
            assertEquals(Collections.singletonList(1L), context.getAbortedCheckpoints());
            assertEquals(Collections.singletonList(2L), context.getCompletedCheckpoints());
        } finally {
            checkpointCoordinator.shutdown();
        }
    }

    private CheckpointCoordinator getCheckpointCoordinator(ExecutionGraph graph) throws Exception {
        return new CheckpointCoordinatorBuilder()
                .setExecutionGraph(graph)
                .setCheckpointCoordinatorConfiguration(
                        CheckpointCoordinatorConfiguration.builder()
                                .setAlignmentTimeout(Long.MAX_VALUE)
                                .setMaxConcurrentCheckpoints(Integer.MAX_VALUE)
                                .build())
                .setTimer(manuallyTriggeredScheduledExecutor)
                .build();
    }

    private CheckpointCoordinator getCheckpointCoordinator(
            ExecutionGraph graph, CheckpointFailureManager failureManager) throws Exception {

        return new CheckpointCoordinatorBuilder()
                .setExecutionGraph(graph)
                .setTimer(manuallyTriggeredScheduledExecutor)
                .setFailureManager(failureManager)
                .build();
    }

    private CheckpointCoordinator getCheckpointCoordinator(ScheduledExecutor timer)
            throws Exception {
        ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .addJobVertex(new JobVertexID())
                        .addJobVertex(new JobVertexID())
                        .build();

        // set up the coordinator and validate the initial state
        return new CheckpointCoordinatorBuilder().setExecutionGraph(graph).setTimer(timer).build();
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

        assertEquals(1, checkpointCoordinator.getPendingCheckpoints().size());
        long checkpointId =
                Iterables.getOnlyElement(checkpointCoordinator.getPendingCheckpoints().keySet());

        for (int index = 0; index < jobVertex1.getParallelism(); index++) {

            KeyGroupRange keyGroupRange = keyGroupPartitions1.get(index);

            Map<StateHandleID, StreamStateHandle> privateState = new HashMap<>();
            privateState.put(
                    new StateHandleID("private-1"),
                    spy(new ByteStreamStateHandle("private-1", new byte[] {'p'})));

            Map<StateHandleID, StreamStateHandle> sharedState = new HashMap<>();

            // let all but the first CP overlap by one shared state.
            if (cpSequenceNumber > 0) {
                sharedState.put(
                        new StateHandleID("shared-" + (cpSequenceNumber - 1)),
                        spy(new PlaceholderStreamStateHandle()));
            }

            sharedState.put(
                    new StateHandleID("shared-" + cpSequenceNumber),
                    spy(
                            new ByteStreamStateHandle(
                                    "shared-" + cpSequenceNumber + "-" + keyGroupRange,
                                    new byte[] {'s'})));

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

    private static class TestingCheckpointIDCounter extends StandaloneCheckpointIDCounter {
        private CheckpointCoordinator owner;

        @Override
        public long getAndIncrement() throws Exception {
            checkNotNull(owner);
            owner.stopCheckpointScheduler();
            return super.getAndIncrement();
        }

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
}
