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

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.testutils.CommonTestUtils;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartAllFailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.TestRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.utils.SimpleAckingTaskManagerGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.scheduler.DefaultScheduler;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.scheduler.TestExecutionSlotAllocatorFactory;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.TestingCheckpointStorageAccessCoordinatorView;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskexecutor.TaskExecutorOperatorEventGateway;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.setExecutionToState;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.fail;

/**
 * Tests for the integration of the {@link OperatorCoordinator} with the scheduler, to ensure the
 * relevant actions are leading to the right method invocations on the coordinator.
 */
class OperatorCoordinatorSchedulerTest {

    private final JobVertexID testVertexId = new JobVertexID();
    private final OperatorID testOperatorId = new OperatorID();

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private final ManuallyTriggeredScheduledExecutorService executor =
            new ManuallyTriggeredScheduledExecutorService();

    private DefaultScheduler createdScheduler;

    @AfterEach
    void shutdownScheduler() throws Exception {
        if (createdScheduler != null) {
            closeScheduler(createdScheduler);
        }
    }

    // ------------------------------------------------------------------------
    //  tests for scheduling
    // ------------------------------------------------------------------------

    @Test
    void testCoordinatorStartedWhenSchedulerStarts() throws Exception {
        final DefaultScheduler scheduler = createAndStartScheduler();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        assertThat(coordinator.isStarted()).isTrue();
    }

    @Test
    void testCoordinatorDisposedWhenSchedulerStops() throws Exception {
        final DefaultScheduler scheduler = createAndStartScheduler();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        closeScheduler(scheduler);

        assertThat(coordinator.isClosed()).isTrue();
    }

    @Test
    void testFailureToStartPropagatesExceptions() throws Exception {
        final OperatorCoordinator.Provider failingCoordinatorProvider =
                new TestingOperatorCoordinator.Provider(
                        testOperatorId, CoordinatorThatFailsInStart::new);
        final DefaultScheduler scheduler = createScheduler(failingCoordinatorProvider);

        try {
            scheduler.startScheduling();
            fail("expected an exception");
        } catch (Exception ignored) {
            // expected
        }
    }

    @Test
    void testFailureToStartClosesCoordinator() throws Exception {
        final OperatorCoordinator.Provider failingCoordinatorProvider =
                new TestingOperatorCoordinator.Provider(
                        testOperatorId, CoordinatorThatFailsInStart::new);
        final DefaultScheduler scheduler = createScheduler(failingCoordinatorProvider);
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        try {
            scheduler.startScheduling();
        } catch (Exception ignored) {
        }

        assertThat(coordinator.isClosed()).isTrue();
    }

    @Test
    void deployingTaskFailureNotifiesCoordinator() throws Exception {
        final DefaultScheduler scheduler = createAndStartScheduler();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        failTask(scheduler, 1);

        assertThat(coordinator.getFailedTasks()).hasSize(1).containsExactly(1);
    }

    @Test
    void runningTaskFailureNotifiesCoordinator() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        failTask(scheduler, 1);

        assertThat(coordinator.getFailedTasks()).hasSize(1).containsExactly(1);
    }

    @Test
    void cancellationAsPartOfFailoverNotifiesCoordinator() throws Exception {
        final DefaultScheduler scheduler = createSchedulerWithAllRestartOnFailureAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        failTask(scheduler, 1);

        assertThat(coordinator.getFailedTasks()).hasSize(2).containsExactlyInAnyOrder(0, 1);
    }

    @Test
    void taskRepeatedFailureNotifyCoordinator() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        failAndRestartTask(scheduler, 0);
        failAndRestartTask(scheduler, 0);

        assertThat(coordinator.getFailedTasks()).hasSize(2).containsExactly(0, 0);
    }

    @Test
    void taskGatewayNotSetBeforeTasksRunning() throws Exception {
        final DefaultScheduler scheduler = createAndStartScheduler();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);
        final OperatorCoordinator.SubtaskGateway gateway = coordinator.getSubtaskGateway(0);

        assertThat(gateway).isNull();
    }

    @Test
    void taskGatewayAvailableWhenTasksRunning() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);
        final OperatorCoordinator.SubtaskGateway gateway = coordinator.getSubtaskGateway(0);

        assertThat(gateway).isNotNull();
    }

    @Test
    void taskTaskManagerFailuresAreReportedBack() throws Exception {
        final DefaultScheduler scheduler =
                createSchedulerAndDeployTasks(new FailingTaskExecutorOperatorEventGateway());

        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);
        final OperatorCoordinator.SubtaskGateway gateway = coordinator.getSubtaskGateway(0);

        final CompletableFuture<?> result = gateway.sendEvent(new TestOperatorEvent());
        executor.triggerAll(); // process event sending

        assertThatThrownBy(result::get).satisfies(anyCauseMatches(TestException.class));
    }

    // THESE TESTS BELOW SHOULD LEGITIMATELY WORK, BUT THE SCHEDULER ITSELF SEEMS TO NOT HANDLE
    // THIS SITUATION AT THE MOMENT
    // WE KEEP THESE TESTS HERE TO ENABLE THEM ONCE THE SCHEDULER'S CONTRACT SUPPORTS THEM

    @Disabled
    @Test
    void deployingTaskCancellationNotifiesCoordinator() throws Exception {
        final DefaultScheduler scheduler = createAndStartScheduler();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        cancelTask(scheduler, 1);

        assertThat(coordinator.getFailedTasks()).hasSize(1).containsExactly(1);
    }

    @Disabled
    @Test
    void runningTaskCancellationNotifiesCoordinator() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        cancelTask(scheduler, 0);

        assertThat(coordinator.getFailedTasks()).hasSize(1).containsExactly(0);
    }

    // ------------------------------------------------------------------------
    //  tests for checkpointing
    // ------------------------------------------------------------------------

    @Test
    void testTakeCheckpoint() throws Exception {
        final byte[] checkpointData = new byte[656];
        new Random().nextBytes(checkpointData);

        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                triggerCheckpoint(scheduler);
        coordinator.getLastTriggeredCheckpoint().complete(checkpointData);
        executor.triggerAll();
        OperatorEvent event =
                new AcknowledgeCheckpointEvent(coordinator.getLastTriggeredCheckpointId());
        OperatorCoordinatorHolder holder = getCoordinatorHolder(scheduler);
        for (int i = 0; i < holder.currentParallelism(); i++) {
            holder.handleEventFromOperator(i, 0, event);
        }
        acknowledgeCurrentCheckpoint(scheduler);

        final OperatorState state = checkpointFuture.get().getOperatorStates().get(testOperatorId);
        assertThat(getStateHandleContents(state.getCoordinatorState()))
                .containsExactly(checkpointData);
    }

    @Test
    void testSnapshotSyncFailureFailsCheckpoint() throws Exception {
        final OperatorCoordinator.Provider failingCoordinatorProvider =
                new TestingOperatorCoordinator.Provider(
                        testOperatorId, CoordinatorThatFailsCheckpointing::new);
        final DefaultScheduler scheduler =
                createSchedulerAndDeployTasks(failingCoordinatorProvider);

        final CompletableFuture<?> checkpointFuture = triggerCheckpoint(scheduler);

        assertThatThrownBy(checkpointFuture::get).satisfies(anyCauseMatches(TestException.class));
    }

    @Test
    void testSnapshotAsyncFailureFailsCheckpoint() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        final CompletableFuture<?> checkpointFuture = triggerCheckpoint(scheduler);
        final CompletableFuture<?> coordinatorStateFuture =
                coordinator.getLastTriggeredCheckpoint();

        coordinatorStateFuture.completeExceptionally(new TestException());
        waitForCompletionToPropagate(checkpointFuture);

        assertThatThrownBy(checkpointFuture::get).satisfies(anyCauseMatches(TestException.class));
    }

    @Test
    void testSavepointRestoresCoordinator() throws Exception {
        final byte[] testCoordinatorState = new byte[123];
        new Random().nextBytes(testCoordinatorState);

        final DefaultScheduler scheduler =
                createSchedulerWithRestoredSavepoint(testCoordinatorState);
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        final byte[] restoredState = coordinator.getLastRestoredCheckpointState();
        assertThat(restoredState).containsExactly(testCoordinatorState);
    }

    @Test
    void testGlobalFailureResetsToCheckpoint() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        final byte[] coordinatorState = new byte[] {7, 11, 3, 5};
        takeCompleteCheckpoint(scheduler, coordinator, coordinatorState);
        failGlobalAndRestart(scheduler, new TestException());

        assertThat(coordinator.getLastRestoredCheckpointState())
                .as("coordinator should have a restored checkpoint")
                .containsExactly(coordinatorState);
    }

    @Test
    void testGlobalFailureBeforeCheckpointResetsToEmptyState() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        failGlobalAndRestart(scheduler, new TestException());

        assertThat(coordinator.getLastRestoredCheckpointState())
                .as("coordinator should have null restored state")
                .isEqualTo(TestingOperatorCoordinator.NULL_RESTORE_VALUE);
        assertThat(coordinator.getLastRestoredCheckpointId())
                .isEqualTo(OperatorCoordinator.NO_CHECKPOINT);
    }

    @Test
    void testGlobalFailureTwiceWillNotResetToCheckpointTwice() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);
        AtomicInteger resetToCheckpointCounter = new AtomicInteger(0);
        coordinator.setResetToCheckpointConsumer(
                (ignore1, ignore2) -> resetToCheckpointCounter.incrementAndGet());

        // fail global twice.
        scheduler.handleGlobalFailure(new TestException());
        failGlobalAndRestart(scheduler, new TestException());

        assertThat(resetToCheckpointCounter).hasValue(1);
        assertThat(coordinator.getLastRestoredCheckpointState())
                .as("coordinator should have null restored state")
                .isEqualTo(TestingOperatorCoordinator.NULL_RESTORE_VALUE);
        assertThat(coordinator.getLastRestoredCheckpointId())
                .isEqualTo(OperatorCoordinator.NO_CHECKPOINT);
    }

    @Test
    void testGlobalFailoverDoesNotNotifyLocalRestore() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        takeCompleteCheckpoint(scheduler, coordinator, new byte[0]);
        failGlobalAndRestart(scheduler, new TestException());

        assertThat(coordinator.getRestoredTasks()).isEmpty();
    }

    @Test
    void testLocalFailoverResetsTask() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        final long checkpointId = takeCompleteCheckpoint(scheduler, coordinator, new byte[0]);
        failAndRestartTask(scheduler, 1);

        assertThat(coordinator.getRestoredTasks()).hasSize(1);
        final TestingOperatorCoordinator.SubtaskAndCheckpoint restoredTask =
                coordinator.getRestoredTasks().get(0);
        assertThat(restoredTask.subtaskIndex).isOne();
        assertThat(restoredTask.checkpointId).isEqualTo(checkpointId);
    }

    @Test
    void testLocalFailoverBeforeCheckpointResetsTask() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        failAndRestartTask(scheduler, 1);

        assertThat(coordinator.getRestoredTasks()).hasSize(1);
        final TestingOperatorCoordinator.SubtaskAndCheckpoint restoredTask =
                coordinator.getRestoredTasks().get(0);
        assertThat(restoredTask.subtaskIndex).isOne();
        assertThat(restoredTask.checkpointId).isEqualTo(OperatorCoordinator.NO_CHECKPOINT);
    }

    @Test
    void testLocalFailoverDoesNotResetToCheckpoint() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        takeCompleteCheckpoint(scheduler, coordinator, new byte[] {37, 11, 83, 4});
        failAndRestartTask(scheduler, 0);

        assertThat(coordinator.getLastRestoredCheckpointState())
                .as("coordinator should not have a restored checkpoint")
                .isNull();
    }

    @Test
    void testConfirmCheckpointComplete() throws Exception {
        final DefaultScheduler scheduler = createSchedulerAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        final long checkpointId =
                takeCompleteCheckpoint(scheduler, coordinator, new byte[] {37, 11, 83, 4});

        assertThat(coordinator.getLastCheckpointComplete())
                .as("coordinator should be notified of completed checkpoint")
                .isEqualTo(checkpointId);
    }

    // ------------------------------------------------------------------------
    //  tests for failover notifications in a batch setup (no checkpoints)
    // ------------------------------------------------------------------------

    @Test
    void testBatchGlobalFailureResetsToEmptyState() throws Exception {
        final DefaultScheduler scheduler = createSchedulerWithoutCheckpointingAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        failGlobalAndRestart(scheduler, new TestException());

        assertThat(coordinator.getLastRestoredCheckpointState())
                .as("coordinator should have null restored state")
                .isEqualTo(TestingOperatorCoordinator.NULL_RESTORE_VALUE);
        assertThat(coordinator.getLastRestoredCheckpointId())
                .isEqualTo(OperatorCoordinator.NO_CHECKPOINT);
    }

    @Test
    void testBatchGlobalFailoverDoesNotNotifyLocalRestore() throws Exception {
        final DefaultScheduler scheduler = createSchedulerWithoutCheckpointingAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        failGlobalAndRestart(scheduler, new TestException());

        assertThat(coordinator.getRestoredTasks()).isEmpty();
    }

    @Test
    void testBatchLocalFailoverResetsTask() throws Exception {
        final DefaultScheduler scheduler = createSchedulerWithoutCheckpointingAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        failAndRestartTask(scheduler, 1);

        assertThat(coordinator.getRestoredTasks()).hasSize(1);
        final TestingOperatorCoordinator.SubtaskAndCheckpoint restoredTask =
                coordinator.getRestoredTasks().get(0);
        assertThat(restoredTask.subtaskIndex).isOne();
        assertThat(restoredTask.checkpointId).isEqualTo(OperatorCoordinator.NO_CHECKPOINT);
    }

    @Test
    void testBatchLocalFailoverDoesNotResetToCheckpoint() throws Exception {
        final DefaultScheduler scheduler = createSchedulerWithoutCheckpointingAndDeployTasks();
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        failAndRestartTask(scheduler, 0);

        assertThat(coordinator.getLastRestoredCheckpointState())
                .as("coordinator should not have a restored checkpoint")
                .isNull();
    }

    // ------------------------------------------------------------------------
    //  tests for REST request delivery
    // ------------------------------------------------------------------------

    @Test
    @SuppressWarnings("unchecked")
    void testDeliveringClientRequestToRequestHandler() throws Exception {
        final OperatorCoordinator.Provider provider =
                new TestingCoordinationRequestHandler.Provider(testOperatorId);
        final DefaultScheduler scheduler = createScheduler(provider);

        final String payload = "testing payload";
        final TestingCoordinationRequestHandler.Request<String> request =
                new TestingCoordinationRequestHandler.Request<>(payload);
        final TestingCoordinationRequestHandler.Response<String> response =
                (TestingCoordinationRequestHandler.Response<String>)
                        scheduler
                                .deliverCoordinationRequestToCoordinator(testOperatorId, request)
                                .get();

        assertThat(response.getPayload()).isEqualTo(payload);
    }

    @Test
    void testDeliveringClientRequestToNonRequestHandler() throws Exception {
        final OperatorCoordinator.Provider provider =
                new TestingOperatorCoordinator.Provider(testOperatorId);
        final DefaultScheduler scheduler = createScheduler(provider);

        final String payload = "testing payload";
        final TestingCoordinationRequestHandler.Request<String> request =
                new TestingCoordinationRequestHandler.Request<>(payload);

        CommonTestUtils.assertThrows(
                "cannot handle client event",
                FlinkException.class,
                () -> scheduler.deliverCoordinationRequestToCoordinator(testOperatorId, request));
    }

    @Test
    void testDeliveringClientRequestToNonExistingCoordinator() throws Exception {
        final OperatorCoordinator.Provider provider =
                new TestingOperatorCoordinator.Provider(testOperatorId);
        final DefaultScheduler scheduler = createScheduler(provider);

        final String payload = "testing payload";
        final TestingCoordinationRequestHandler.Request<String> request =
                new TestingCoordinationRequestHandler.Request<>(payload);

        CommonTestUtils.assertThrows(
                "does not exist",
                FlinkException.class,
                () -> scheduler.deliverCoordinationRequestToCoordinator(new OperatorID(), request));
    }

    // ------------------------------------------------------------------------
    //  test setups
    // ------------------------------------------------------------------------

    private DefaultScheduler createScheduler(OperatorCoordinator.Provider provider)
            throws Exception {
        return setupTestJobAndScheduler(provider);
    }

    private DefaultScheduler createAndStartScheduler() throws Exception {
        final DefaultScheduler scheduler =
                setupTestJobAndScheduler(new TestingOperatorCoordinator.Provider(testOperatorId));
        scheduler.startScheduling();
        executor.triggerAll();

        // guard test assumptions: this brings tasks into DEPLOYING state
        assertThat(SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, 0))
                .isEqualTo(ExecutionState.DEPLOYING);

        return scheduler;
    }

    private DefaultScheduler createSchedulerAndDeployTasks() throws Exception {
        return createSchedulerAndDeployTasks(
                new TestingOperatorCoordinator.Provider(testOperatorId));
    }

    private DefaultScheduler createSchedulerWithAllRestartOnFailureAndDeployTasks()
            throws Exception {
        final DefaultScheduler scheduler =
                setupTestJobAndScheduler(
                        new TestingOperatorCoordinator.Provider(testOperatorId), null, null, true);
        scheduleAllTasksToRunning(scheduler);
        return scheduler;
    }

    private DefaultScheduler createSchedulerWithoutCheckpointingAndDeployTasks() throws Exception {
        final Consumer<JobGraph> noCheckpoints = (jobGraph) -> jobGraph.setSnapshotSettings(null);
        final DefaultScheduler scheduler =
                setupTestJobAndScheduler(
                        new TestingOperatorCoordinator.Provider(testOperatorId),
                        null,
                        noCheckpoints,
                        false);

        // guard test assumptions: this must set up a scheduler without checkpoints
        assertThat(scheduler.getExecutionGraph().getCheckpointCoordinator()).isNull();

        scheduleAllTasksToRunning(scheduler);
        return scheduler;
    }

    private DefaultScheduler createSchedulerAndDeployTasks(OperatorCoordinator.Provider provider)
            throws Exception {
        final DefaultScheduler scheduler = setupTestJobAndScheduler(provider);
        scheduleAllTasksToRunning(scheduler);
        return scheduler;
    }

    private DefaultScheduler createSchedulerAndDeployTasks(TaskExecutorOperatorEventGateway gateway)
            throws Exception {
        final DefaultScheduler scheduler =
                setupTestJobAndScheduler(
                        new TestingOperatorCoordinator.Provider(testOperatorId),
                        gateway,
                        null,
                        false);
        scheduleAllTasksToRunning(scheduler);
        return scheduler;
    }

    private DefaultScheduler createSchedulerWithRestoredSavepoint(byte[] coordinatorState)
            throws Exception {
        final byte[] savepointMetadata =
                serializeAsCheckpointMetadata(testOperatorId, coordinatorState);
        final String savepointPointer = "testingSavepointPointer";

        final TestingCheckpointStorageAccessCoordinatorView storage =
                new TestingCheckpointStorageAccessCoordinatorView();
        storage.registerSavepoint(savepointPointer, savepointMetadata);

        final Consumer<JobGraph> savepointConfigurer =
                (jobGraph) -> {
                    SchedulerTestingUtils.enableCheckpointing(
                            jobGraph, new ModernStateBackend(), storage.asCheckpointStorage());
                    jobGraph.setSavepointRestoreSettings(
                            SavepointRestoreSettings.forPath(savepointPointer));
                };

        final DefaultScheduler scheduler =
                setupTestJobAndScheduler(
                        new TestingOperatorCoordinator.Provider(testOperatorId),
                        null,
                        savepointConfigurer,
                        false);

        scheduler.startScheduling();
        return scheduler;
    }

    private DefaultScheduler setupTestJobAndScheduler(OperatorCoordinator.Provider provider)
            throws Exception {
        return setupTestJobAndScheduler(provider, null, null, false);
    }

    private DefaultScheduler setupTestJobAndScheduler(
            OperatorCoordinator.Provider provider,
            @Nullable TaskExecutorOperatorEventGateway taskExecutorOperatorEventGateway,
            @Nullable Consumer<JobGraph> jobGraphPreProcessing,
            boolean restartAllOnFailover)
            throws Exception {

        final OperatorIDPair opIds = OperatorIDPair.of(new OperatorID(), provider.getOperatorId());
        final JobVertex vertex =
                new JobVertex(
                        "Vertex with OperatorCoordinator",
                        testVertexId,
                        Collections.singletonList(opIds));
        vertex.setInvokableClass(NoOpInvokable.class);
        vertex.addOperatorCoordinator(new SerializedValue<>(provider));
        vertex.setParallelism(2);

        final JobGraph jobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder().addJobVertex(vertex).build();

        SchedulerTestingUtils.enableCheckpointing(jobGraph);
        if (jobGraphPreProcessing != null) {
            jobGraphPreProcessing.accept(jobGraph);
        }

        final ComponentMainThreadExecutor mainThreadExecutor =
                new ComponentMainThreadExecutorServiceAdapter(
                        (ScheduledExecutorService) executor, Thread.currentThread());

        final DefaultSchedulerBuilder schedulerBuilder =
                taskExecutorOperatorEventGateway == null
                        ? createSchedulerBuilder(
                                jobGraph, mainThreadExecutor, EXECUTOR_EXTENSION.getExecutor())
                        : createSchedulerBuilder(
                                jobGraph,
                                mainThreadExecutor,
                                taskExecutorOperatorEventGateway,
                                EXECUTOR_EXTENSION.getExecutor());
        if (restartAllOnFailover) {
            schedulerBuilder.setFailoverStrategyFactory(new RestartAllFailoverStrategy.Factory());
        }

        final DefaultScheduler scheduler =
                schedulerBuilder.setFutureExecutor(executor).setDelayExecutor(executor).build();

        this.createdScheduler = scheduler;
        return scheduler;
    }

    private static DefaultSchedulerBuilder createSchedulerBuilder(
            JobGraph jobGraph,
            ComponentMainThreadExecutor mainThreadExecutor,
            ScheduledExecutorService scheduledExecutorService) {

        return createSchedulerBuilder(
                jobGraph,
                mainThreadExecutor,
                new SimpleAckingTaskManagerGateway(),
                scheduledExecutorService);
    }

    private static DefaultSchedulerBuilder createSchedulerBuilder(
            JobGraph jobGraph,
            ComponentMainThreadExecutor mainThreadExecutor,
            TaskExecutorOperatorEventGateway operatorEventGateway,
            ScheduledExecutorService scheduledExecutorService) {

        final TaskManagerGateway gateway =
                operatorEventGateway instanceof TaskManagerGateway
                        ? (TaskManagerGateway) operatorEventGateway
                        : new TaskExecutorOperatorEventGatewayAdapter(operatorEventGateway);

        return createSchedulerBuilder(
                jobGraph, mainThreadExecutor, gateway, scheduledExecutorService);
    }

    private static DefaultSchedulerBuilder createSchedulerBuilder(
            JobGraph jobGraph,
            ComponentMainThreadExecutor mainThreadExecutor,
            TaskManagerGateway taskManagerGateway,
            ScheduledExecutorService executorService) {

        return new DefaultSchedulerBuilder(jobGraph, mainThreadExecutor, executorService)
                .setSchedulingStrategyFactory(new PipelinedRegionSchedulingStrategy.Factory())
                .setRestartBackoffTimeStrategy(new TestRestartBackoffTimeStrategy(true, 0))
                .setExecutionSlotAllocatorFactory(
                        new TestExecutionSlotAllocatorFactory(taskManagerGateway));
    }

    private void scheduleAllTasksToRunning(DefaultScheduler scheduler) {
        scheduler.startScheduling();
        executor.triggerAll();
        executor.triggerScheduledTasks();
        SchedulerTestingUtils.setAllExecutionsToRunning(scheduler);

        // guard test assumptions: this brings tasks into RUNNING state
        assertThat(SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, 0))
                .isEqualTo(ExecutionState.RUNNING);

        // trigger actions depending on the switch to running, like the notifications
        // that the task is reads and the task gateway setup
        executor.triggerAll();
    }

    private TestingOperatorCoordinator getCoordinator(DefaultScheduler scheduler) {
        OperatorCoordinatorHolder holder = getCoordinatorHolder(scheduler);

        final OperatorCoordinator coordinator = holder.coordinator();
        assertThat(coordinator).isInstanceOf(TestingOperatorCoordinator.class);

        return (TestingOperatorCoordinator) coordinator;
    }

    private OperatorCoordinatorHolder getCoordinatorHolder(DefaultScheduler scheduler) {
        final ExecutionJobVertex vertexWithCoordinator = getJobVertex(scheduler, testVertexId);
        assertThat(vertexWithCoordinator).as("vertex for coordinator not found").isNotNull();

        final Optional<OperatorCoordinatorHolder> coordinatorOptional =
                vertexWithCoordinator.getOperatorCoordinators().stream()
                        .filter((holder) -> holder.operatorId().equals(testOperatorId))
                        .findFirst();
        assertThat(coordinatorOptional).as("vertex does not contain coordinator").isPresent();

        return coordinatorOptional.get();
    }

    // ------------------------------------------------------------------------
    //  test actions
    // ------------------------------------------------------------------------

    private void failTask(DefaultScheduler scheduler, int subtask) {
        SchedulerTestingUtils.failExecution(scheduler, testVertexId, subtask);
        executor.triggerAll();

        // guard the test assumptions: This must not lead to a restart, but must keep the task in
        // FAILED state
        assertThat(SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, subtask))
                .isEqualTo(ExecutionState.FAILED);
    }

    private void failAndRedeployTask(DefaultScheduler scheduler, int subtask) {
        failTask(scheduler, subtask);

        executor.triggerAll();
        executor.triggerScheduledTasks();
        executor.triggerAll();

        // guard the test assumptions: This must lead to a restarting and redeploying
        assertThat(SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, subtask))
                .isEqualTo(ExecutionState.DEPLOYING);
    }

    private void failAndRestartTask(DefaultScheduler scheduler, int subtask) {
        failAndRedeployTask(scheduler, subtask);
        setExecutionToState(ExecutionState.INITIALIZING, scheduler, testVertexId, subtask);
        setExecutionToState(ExecutionState.RUNNING, scheduler, testVertexId, subtask);

        // guard the test assumptions: This must bring the task back to RUNNING
        assertThat(SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, subtask))
                .isEqualTo(ExecutionState.RUNNING);
    }

    private void failGlobalAndRestart(DefaultScheduler scheduler, Throwable reason)
            throws InterruptedException {
        scheduler.handleGlobalFailure(reason);
        SchedulerTestingUtils.setAllExecutionsToCancelled(scheduler);

        // make sure the checkpoint is no longer triggering (this means that the subtask
        // gateway has been closed)
        final CheckpointCoordinator checkpointCoordinator =
                scheduler.getExecutionGraph().getCheckpointCoordinator();
        while (checkpointCoordinator != null && checkpointCoordinator.isTriggering()) {
            Thread.sleep(1);
        }

        // make sure we propagate all asynchronous and delayed actions
        executor.triggerAll();
        executor.triggerScheduledTasks();
        executor.triggerAll();

        SchedulerTestingUtils.setAllExecutionsToRunning(scheduler);
        executor.triggerAll();

        // guard the test assumptions: This must bring the tasks back to RUNNING
        assertThat(SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, 0))
                .isEqualTo(ExecutionState.RUNNING);
    }

    private void cancelTask(DefaultScheduler scheduler, int subtask) {
        SchedulerTestingUtils.canceledExecution(scheduler, testVertexId, subtask);
        executor.triggerAll();

        // guard the test assumptions: This must not lead to a restart, but must keep the task in
        // FAILED state
        assertThat(SchedulerTestingUtils.getExecutionState(scheduler, testVertexId, subtask))
                .isEqualTo(ExecutionState.CANCELED);
    }

    private CompletableFuture<CompletedCheckpoint> triggerCheckpoint(DefaultScheduler scheduler)
            throws Exception {
        final CompletableFuture<CompletedCheckpoint> future =
                SchedulerTestingUtils.triggerCheckpoint(scheduler);
        final TestingOperatorCoordinator coordinator = getCoordinator(scheduler);

        // the Checkpoint Coordinator executes parts of the logic in its timer thread, and delegates
        // some calls
        // to the scheduler executor. so we need to do a mix of waiting for the timer thread and
        // working off
        // tasks in the scheduler executor.
        // we can drop this here once the CheckpointCoordinator also runs in a 'main thread
        // executor'.
        while (!(coordinator.hasTriggeredCheckpoint() || future.isDone())) {
            executor.triggerAll();
            Thread.sleep(1);
        }

        return future;
    }

    private void waitForCompletionToPropagate(CompletableFuture<?> checkpointFuture) {
        // this part is necessary because the user/application-code-driven coordinator
        // forwards the checkpoint to the scheduler thread, which in turn needs to finish
        // work
        while (!checkpointFuture.isDone()) {
            executor.triggerAll();
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                throw new Error(e);
            }
        }
    }

    private void acknowledgeCurrentCheckpoint(DefaultScheduler scheduler) {
        executor.triggerAll();
        SchedulerTestingUtils.acknowledgeCurrentCheckpoint(scheduler);
        executor.triggerAll();
    }

    private long takeCompleteCheckpoint(
            DefaultScheduler scheduler,
            TestingOperatorCoordinator testingOperatorCoordinator,
            byte[] coordinatorState)
            throws Exception {

        final CompletableFuture<CompletedCheckpoint> checkpointFuture =
                triggerCheckpoint(scheduler);

        testingOperatorCoordinator.getLastTriggeredCheckpoint().complete(coordinatorState);
        executor.triggerAll();
        OperatorEvent event =
                new AcknowledgeCheckpointEvent(
                        testingOperatorCoordinator.getLastTriggeredCheckpointId());
        OperatorCoordinatorHolder holder = getCoordinatorHolder(scheduler);
        for (int i = 0; i < holder.currentParallelism(); i++) {
            holder.handleEventFromOperator(i, 0, event);
        }
        acknowledgeCurrentCheckpoint(scheduler);

        // wait until checkpoint has completed
        final long checkpointId = checkpointFuture.get().getCheckpointID();

        // now wait until it has been acknowledged
        while (!testingOperatorCoordinator.hasCompleteCheckpoint()) {
            executor.triggerAll();
            Thread.sleep(1);
        }

        return checkpointId;
    }

    private void closeScheduler(DefaultScheduler scheduler) throws Exception {
        final CompletableFuture<Void> closeFuture = scheduler.closeAsync();
        executor.triggerAll();
        closeFuture.get();
    }

    // ------------------------------------------------------------------------
    //  miscellaneous utilities
    // ------------------------------------------------------------------------

    private static ExecutionJobVertex getJobVertex(
            DefaultScheduler scheduler, JobVertexID jobVertexId) {
        final ExecutionVertexID id = new ExecutionVertexID(jobVertexId, 0);
        return scheduler.getExecutionVertex(id).getJobVertex();
    }

    private static OperatorState createOperatorState(OperatorID id, byte[] coordinatorState) {
        final OperatorState state = new OperatorState(id, 10, 16384);
        state.setCoordinatorState(new ByteStreamStateHandle("name", coordinatorState));
        return state;
    }

    private static byte[] serializeAsCheckpointMetadata(OperatorID id, byte[] coordinatorState)
            throws IOException {
        final OperatorState state = createOperatorState(id, coordinatorState);
        final CheckpointMetadata metadata =
                new CheckpointMetadata(
                        1337L, Collections.singletonList(state), Collections.emptyList());

        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        Checkpoints.storeCheckpointMetadata(metadata, out);
        return out.toByteArray();
    }

    private static byte[] getStateHandleContents(StreamStateHandle stateHandle) {
        if (stateHandle instanceof ByteStreamStateHandle) {
            return ((ByteStreamStateHandle) stateHandle).getData();
        }
        fail("other state handles not implemented");
        return null;
    }

    // ------------------------------------------------------------------------
    //  test mocks
    // ------------------------------------------------------------------------

    private static final class TestOperatorEvent implements OperatorEvent {}

    private static final class TestException extends Exception {}

    private static final class CoordinatorThatFailsInStart extends TestingOperatorCoordinator {

        public CoordinatorThatFailsInStart(Context context) {
            super(context);
        }

        @Override
        public void start() throws Exception {
            throw new Exception("test failure");
        }
    }

    private static final class CoordinatorThatFailsCheckpointing
            extends TestingOperatorCoordinator {

        public CoordinatorThatFailsCheckpointing(Context context) {
            super(context);
        }

        @Override
        public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result) {
            throw new Error(new TestException());
        }
    }

    private static final class FailingTaskExecutorOperatorEventGateway
            implements TaskExecutorOperatorEventGateway {

        @Override
        public CompletableFuture<Acknowledge> sendOperatorEventToTask(
                ExecutionAttemptID task, OperatorID operator, SerializedValue<OperatorEvent> evt) {
            return FutureUtils.completedExceptionally(new TestException());
        }
    }

    private static class ModernStateBackend implements StateBackend {

        @Override
        public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
                Environment env,
                JobID jobID,
                String operatorIdentifier,
                TypeSerializer<K> keySerializer,
                int numberOfKeyGroups,
                KeyGroupRange keyGroupRange,
                TaskKvStateRegistry kvStateRegistry,
                TtlTimeProvider ttlTimeProvider,
                MetricGroup metricGroup,
                @Nonnull Collection<KeyedStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            throw new UnsupportedOperationException();
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                Environment env,
                String operatorIdentifier,
                @Nonnull Collection<OperatorStateHandle> stateHandles,
                CloseableRegistry cancelStreamRegistry)
                throws Exception {
            throw new UnsupportedOperationException();
        }
    }

    private static final class TaskExecutorOperatorEventGatewayAdapter
            extends SimpleAckingTaskManagerGateway {

        private final TaskExecutorOperatorEventGateway operatorGateway;

        private TaskExecutorOperatorEventGatewayAdapter(
                TaskExecutorOperatorEventGateway operatorGateway) {
            this.operatorGateway = operatorGateway;
        }

        @Override
        public CompletableFuture<Acknowledge> sendOperatorEventToTask(
                ExecutionAttemptID task, OperatorID operator, SerializedValue<OperatorEvent> evt) {
            return operatorGateway.sendOperatorEventToTask(task, operator, evt);
        }
    }
}
