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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.DefaultSubtaskAttemptNumberStore;
import org.apache.flink.runtime.executiongraph.EdgeManager;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphID;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.executiongraph.JobVertexInputInfo;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.failover.partitionrelease.PartitionGroupReleaseStrategy;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.operators.coordination.CoordinatorStoreImpl;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismInfo;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.TestingAccessExecution;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.ShuffleDescriptor;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AdaptiveScheduler AdaptiveScheduler's} {@link Executing} state. */
class ExecutingTest {

    private static final Logger log = LoggerFactory.getLogger(ExecutingTest.class);

    @RegisterExtension
    private static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @Test
    void testExecutionGraphDeploymentOnEnter() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            MockExecutionJobVertex mockExecutionJobVertex =
                    new MockExecutionJobVertex(MockExecutionVertex::new);
            MockExecutionVertex mockExecutionVertex =
                    (MockExecutionVertex) mockExecutionJobVertex.getMockExecutionVertex();
            mockExecutionVertex.setMockedExecutionState(ExecutionState.CREATED);
            ExecutionGraph executionGraph =
                    new MockExecutionGraph(() -> Collections.singletonList(mockExecutionJobVertex));
            Executing exec =
                    new ExecutingStateBuilder().setExecutionGraph(executionGraph).build(ctx);

            assertThat(mockExecutionVertex.isDeployCalled()).isTrue();
            assertThat(executionGraph.getState()).isEqualTo(JobStatus.RUNNING);
        }
    }

    @Test
    void testNoDeploymentCallOnEnterWhenVertexRunning() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            MockExecutionJobVertex mockExecutionJobVertex =
                    new MockExecutionJobVertex(MockExecutionVertex::new);
            ExecutionGraph executionGraph =
                    new MockExecutionGraph(() -> Collections.singletonList(mockExecutionJobVertex));
            executionGraph.transitionToRunning();
            final MockExecutionVertex mockExecutionVertex =
                    (MockExecutionVertex) mockExecutionJobVertex.getMockExecutionVertex();
            mockExecutionVertex.setMockedExecutionState(ExecutionState.RUNNING);

            new Executing(
                    executionGraph,
                    getExecutionGraphHandler(executionGraph, ctx.getMainThreadExecutor()),
                    new TestingOperatorCoordinatorHandler(),
                    log,
                    ctx,
                    ClassLoader.getSystemClassLoader(),
                    new ArrayList<>(),
                    (context) -> TestingStateTransitionManager.withNoOp(),
                    1);
            assertThat(mockExecutionVertex.isDeployCalled()).isFalse();
        }
    }

    @Test
    void testIllegalStateExceptionOnNotRunningExecutionGraph() {
        assertThatThrownBy(
                        () -> {
                            try (MockExecutingContext ctx = new MockExecutingContext()) {
                                ExecutionGraph notRunningExecutionGraph =
                                        new StateTrackingMockExecutionGraph();
                                assertThat(notRunningExecutionGraph.getState())
                                        .isNotEqualTo(JobStatus.RUNNING);

                                new Executing(
                                        notRunningExecutionGraph,
                                        getExecutionGraphHandler(
                                                notRunningExecutionGraph,
                                                ctx.getMainThreadExecutor()),
                                        new TestingOperatorCoordinatorHandler(),
                                        log,
                                        ctx,
                                        ClassLoader.getSystemClassLoader(),
                                        new ArrayList<>(),
                                        context -> TestingStateTransitionManager.withNoOp(),
                                        1);
                            }
                        })
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void testTriggerRescaleOnCompletedCheckpoint() throws Exception {
        final AtomicBoolean rescaleTriggered = new AtomicBoolean();
        final Function<StateTransitionManager.Context, StateTransitionManager>
                stateTransitionManagerFactory =
                        context ->
                                TestingStateTransitionManager.withOnTriggerEventOnly(
                                        () -> rescaleTriggered.set(true));

        try (MockExecutingContext ctx = new MockExecutingContext()) {
            final Executing testInstance =
                    new ExecutingStateBuilder()
                            .setStateTransitionManagerFactory(stateTransitionManagerFactory)
                            .build(ctx);

            assertThat(rescaleTriggered).isFalse();
            testInstance.onCompletedCheckpoint();
            assertThat(rescaleTriggered).isTrue();
        }
    }

    @Test
    public void testTriggerRescaleOnFailedCheckpoint() throws Exception {
        final AtomicInteger rescaleTriggerCount = new AtomicInteger();
        final Function<StateTransitionManager.Context, StateTransitionManager>
                stateTransitionManagerFactory =
                        context ->
                                TestingStateTransitionManager.withOnTriggerEventOnly(
                                        rescaleTriggerCount::incrementAndGet);

        final int rescaleOnFailedCheckpointsCount = 3;
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            final Executing testInstance =
                    new ExecutingStateBuilder()
                            .setStateTransitionManagerFactory(stateTransitionManagerFactory)
                            .setRescaleOnFailedCheckpointCount(rescaleOnFailedCheckpointsCount)
                            .build(ctx);

            // do multiple rescale iterations to verify that subsequent failed checkpoints after a
            // rescale result in the expected behavior
            for (int rescaleIteration = 1; rescaleIteration <= 3; rescaleIteration++) {

                // trigger an initial failed checkpoint event to show that the counting only starts
                // with the subsequent change event
                testInstance.onFailedCheckpoint();

                // trigger change
                testInstance.onNewResourceRequirements();

                for (int i = 0; i < rescaleOnFailedCheckpointsCount; i++) {
                    assertThat(rescaleTriggerCount)
                            .as(
                                    "No rescale operation should have been triggered for iteration #%d, yet.",
                                    rescaleIteration)
                            .hasValue(rescaleIteration - 1);
                    testInstance.onFailedCheckpoint();
                }

                assertThat(rescaleTriggerCount)
                        .as(
                                "The rescale operation for iteration #%d should have been properly triggered.",
                                rescaleIteration)
                        .hasValue(rescaleIteration);
            }
        }
    }

    @Test
    public void testOnCompletedCheckpointResetsFailedCheckpointCount() throws Exception {
        final AtomicInteger rescaleTriggeredCount = new AtomicInteger();
        final Function<StateTransitionManager.Context, StateTransitionManager>
                stateTransitionManagerFactory =
                        context ->
                                TestingStateTransitionManager.withOnTriggerEventOnly(
                                        rescaleTriggeredCount::incrementAndGet);

        final int rescaleOnFailedCheckpointsCount = 3;
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            final Executing testInstance =
                    new ExecutingStateBuilder()
                            .setStateTransitionManagerFactory(stateTransitionManagerFactory)
                            .setRescaleOnFailedCheckpointCount(rescaleOnFailedCheckpointsCount)
                            .build(ctx);

            // trigger an initial failed checkpoint event to show that the counting only starts with
            // the subsequent change event
            testInstance.onFailedCheckpoint();

            // trigger change
            testInstance.onNewResourcesAvailable();

            IntStream.range(0, rescaleOnFailedCheckpointsCount - 1)
                    .forEach(ignored -> testInstance.onFailedCheckpoint());

            assertThat(rescaleTriggeredCount)
                    .as("No rescaling should have been trigger, yet.")
                    .hasValue(0);

            testInstance.onCompletedCheckpoint();

            // trigger change
            testInstance.onNewResourceRequirements();

            assertThat(rescaleTriggeredCount)
                    .as("The completed checkpoint should have triggered a rescale.")
                    .hasValue(1);

            IntStream.range(0, rescaleOnFailedCheckpointsCount - 1)
                    .forEach(ignored -> testInstance.onFailedCheckpoint());

            assertThat(rescaleTriggeredCount)
                    .as(
                            "No additional rescaling should have been trigger by any subsequent failed checkpoint, yet.")
                    .hasValue(1);

            testInstance.onFailedCheckpoint();

            assertThat(rescaleTriggeredCount)
                    .as("The previous failed checkpoint should have triggered the rescale.")
                    .hasValue(2);
        }
    }

    @Test
    void testDisposalOfOperatorCoordinatorsOnLeaveOfStateWithExecutionGraph() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            TestingOperatorCoordinatorHandler operatorCoordinator =
                    new TestingOperatorCoordinatorHandler();
            Executing exec =
                    new ExecutingStateBuilder()
                            .setOperatorCoordinatorHandler(operatorCoordinator)
                            .build(ctx);
            exec.onLeave(MockState.class);

            assertThat(operatorCoordinator.isDisposed()).isTrue();
        }
    }

    @Test
    void testUnrecoverableGlobalFailureTransitionsToFailingState() throws Exception {
        final String failureMsg = "test exception";
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            ctx.setExpectFailing(
                    failingArguments -> {
                        assertThat(failingArguments.getExecutionGraph()).isNotNull();
                        assertThat(failingArguments.getFailureCause().getMessage())
                                .isEqualTo(failureMsg);
                    });
            ctx.setHowToHandleFailure(FailureResult::canNotRestart);
            exec.handleGlobalFailure(
                    new RuntimeException(failureMsg), FailureEnricherUtils.EMPTY_FAILURE_LABELS);
        }
    }

    @Test
    void testRecoverableGlobalFailureTransitionsToRestarting() throws Exception {
        final Duration duration = Duration.ZERO;
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            ctx.setExpectRestarting(
                    restartingArguments -> {
                        assertThat(restartingArguments.getBackoffTime()).isEqualTo(duration);
                        assertThat(restartingArguments.isForcedRestart()).isFalse();
                    });
            ctx.setHowToHandleFailure(f -> FailureResult.canRestart(f, duration));
            exec.handleGlobalFailure(
                    new RuntimeException("Recoverable error"),
                    FailureEnricherUtils.EMPTY_FAILURE_LABELS);
        }
    }

    @Test
    void testCancelTransitionsToCancellingState() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            ctx.setExpectCancelling(assertNonNull());
            exec.cancel();
        }
    }

    @Test
    void testTransitionToFinishedOnFailedExecutionGraph() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState())
                                    .isEqualTo(JobStatus.FAILED));

            // transition EG into terminal state, which will notify the Executing state about the
            // failure (async via the supplied executor)
            exec.getExecutionGraph()
                    .failJob(new RuntimeException("test failure"), System.currentTimeMillis());
        }
    }

    @Test
    void testTransitionToFinishedOnSuspend() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState())
                                    .isEqualTo(JobStatus.SUSPENDED));
            exec.suspend(new RuntimeException("suspend"));
        }
    }

    @Test
    void testFailureReportedViaUpdateTaskExecutionStateCausesFailingOnNoRestart() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            StateTrackingMockExecutionGraph returnsFailedStateExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(returnsFailedStateExecutionGraph)
                            .build(ctx);

            ctx.setHowToHandleFailure(FailureResult::canNotRestart);
            ctx.setExpectFailing(assertNonNull());

            Exception exception = new RuntimeException();
            TestingAccessExecution execution =
                    TestingAccessExecution.newBuilder()
                            .withExecutionState(ExecutionState.FAILED)
                            .withErrorInfo(new ErrorInfo(exception, System.currentTimeMillis()))
                            .build();
            returnsFailedStateExecutionGraph.registerExecution(execution);
            TaskExecutionStateTransition taskExecutionStateTransition =
                    createFailingStateTransition(execution.getAttemptId(), exception);
            exec.updateTaskExecutionState(
                    taskExecutionStateTransition, FailureEnricherUtils.EMPTY_FAILURE_LABELS);
        }
    }

    @Test
    void testFailureReportedViaUpdateTaskExecutionStateCausesRestart() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            StateTrackingMockExecutionGraph returnsFailedStateExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(returnsFailedStateExecutionGraph)
                            .build(ctx);
            ctx.setHowToHandleFailure(failure -> FailureResult.canRestart(failure, Duration.ZERO));
            ctx.setExpectRestarting(
                    restartingArguments -> {
                        assertThat(restartingArguments).isNotNull();
                        assertThat(restartingArguments.isForcedRestart()).isFalse();
                    });

            Exception exception = new RuntimeException();
            TestingAccessExecution execution =
                    TestingAccessExecution.newBuilder()
                            .withExecutionState(ExecutionState.FAILED)
                            .withErrorInfo(new ErrorInfo(exception, System.currentTimeMillis()))
                            .build();
            returnsFailedStateExecutionGraph.registerExecution(execution);
            TaskExecutionStateTransition taskExecutionStateTransition =
                    createFailingStateTransition(execution.getAttemptId(), exception);
            exec.updateTaskExecutionState(
                    taskExecutionStateTransition, FailureEnricherUtils.EMPTY_FAILURE_LABELS);
        }
    }

    @Test
    void testFalseReportsViaUpdateTaskExecutionStateAreIgnored() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            MockExecutionGraph returnsFailedStateExecutionGraph =
                    new MockExecutionGraph(false, Collections::emptyList);
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(returnsFailedStateExecutionGraph)
                            .build(ctx);

            Exception exception = new RuntimeException();
            TestingAccessExecution execution =
                    TestingAccessExecution.newBuilder()
                            .withExecutionState(ExecutionState.FAILED)
                            .withErrorInfo(new ErrorInfo(exception, System.currentTimeMillis()))
                            .build();
            returnsFailedStateExecutionGraph.registerExecution(execution);
            TaskExecutionStateTransition taskExecutionStateTransition =
                    createFailingStateTransition(execution.getAttemptId(), exception);
            exec.updateTaskExecutionState(
                    taskExecutionStateTransition, FailureEnricherUtils.EMPTY_FAILURE_LABELS);

            ctx.assertNoStateTransition();
        }
    }

    @Test
    void testExecutionVertexMarkedAsFailedOnDeploymentFailure() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            MockExecutionJobVertex mejv =
                    new MockExecutionJobVertex(FailOnDeployMockExecutionVertex::new);
            ExecutionGraph executionGraph =
                    new MockExecutionGraph(() -> Collections.singletonList(mejv));
            Executing exec =
                    new ExecutingStateBuilder().setExecutionGraph(executionGraph).build(ctx);

            assertThat(
                            ((FailOnDeployMockExecutionVertex) mejv.getMockExecutionVertex())
                                    .getMarkedFailure())
                    .isInstanceOf(JobException.class);
        }
    }

    @Test
    void testTransitionToStopWithSavepointState() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            CheckpointCoordinator coordinator =
                    new CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder()
                            .build(EXECUTOR_EXTENSION.getExecutor());
            StateTrackingMockExecutionGraph mockedExecutionGraphWithCheckpointCoordinator =
                    new StateTrackingMockExecutionGraph() {
                        @Nullable
                        @Override
                        public CheckpointCoordinator getCheckpointCoordinator() {
                            return coordinator;
                        }
                    };
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(mockedExecutionGraphWithCheckpointCoordinator)
                            .build(ctx);

            ctx.setExpectStopWithSavepoint(assertNonNull());
            exec.stopWithSavepoint("file:///tmp/target", true, SavepointFormatType.CANONICAL);
        }
    }

    @Test
    void testCheckpointSchedulerIsStoppedOnStopWithSavepoint() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            CheckpointCoordinator coordinator =
                    new CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder()
                            .build(EXECUTOR_EXTENSION.getExecutor());
            StateTrackingMockExecutionGraph mockedExecutionGraphWithCheckpointCoordinator =
                    new StateTrackingMockExecutionGraph() {
                        @Nullable
                        @Override
                        public CheckpointCoordinator getCheckpointCoordinator() {
                            return coordinator;
                        }
                    };
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(mockedExecutionGraphWithCheckpointCoordinator)
                            .build(ctx);

            coordinator.startCheckpointScheduler();

            // we assume checkpointing to be enabled
            assertThat(coordinator.isPeriodicCheckpointingStarted()).isTrue();

            ctx.setExpectStopWithSavepoint(assertNonNull());
            exec.stopWithSavepoint("file:///tmp/target", true, SavepointFormatType.CANONICAL);

            assertThat(coordinator.isPeriodicCheckpointingStarted()).isFalse();
        }
    }

    @Test
    void testJobInformationMethods() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            final JobID jobId = exec.getExecutionGraph().getJobID();
            assertThat(exec.getJob()).isInstanceOf(ArchivedExecutionGraph.class);
            assertThat(exec.getJob().getJobID()).isEqualTo(jobId);
            assertThat(exec.getJobStatus()).isEqualTo(JobStatus.RUNNING);
        }
    }

    @Test
    void testStateDoesNotExposeGloballyTerminalExecutionGraph() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            final FinishingMockExecutionGraph finishingMockExecutionGraph =
                    new FinishingMockExecutionGraph();
            Executing executing =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(finishingMockExecutionGraph)
                            .build(ctx);

            // ideally we'd delay the async call to #onGloballyTerminalState instead, but the
            // context does not support that
            ctx.setExpectFinished(eg -> {});

            finishingMockExecutionGraph.completeTerminationFuture(JobStatus.FINISHED);

            // this is just a sanity check for the test
            assertThat(executing.getExecutionGraph().getState()).isEqualTo(JobStatus.FINISHED);

            assertThat(executing.getJobStatus()).isEqualTo(JobStatus.RUNNING);
            assertThat(executing.getJob().getState()).isEqualTo(JobStatus.RUNNING);
            assertThat(executing.getJob().getStatusTimestamp(JobStatus.FINISHED)).isZero();
        }
    }

    @Test
    void testExecutingChecksForNewResourcesWhenBeingCreated() throws Exception {
        final String onChangeEventLabel = "onChange";
        final String onTriggerEventLabel = "onTrigger";
        final Queue<String> actualEvents = new ArrayDeque<>();
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            new ExecutingStateBuilder()
                    .setStateTransitionManagerFactory(
                            context ->
                                    new TestingStateTransitionManager(
                                            () -> actualEvents.add(onChangeEventLabel),
                                            () -> actualEvents.add(onTriggerEventLabel)))
                    .build(ctx);

            ctx.triggerExecutors();

            assertThat(actualEvents.poll()).isEqualTo(onChangeEventLabel);
            assertThat(actualEvents.poll()).isEqualTo(onTriggerEventLabel);
            assertThat(actualEvents.isEmpty()).isTrue();
        }
    }

    @Test
    public void testOmitsWaitingForResourcesStateWhenRestarting() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            final Executing testInstance = new ExecutingStateBuilder().build(ctx);
            ctx.setExpectRestarting(
                    restartingArguments ->
                            assertThat(restartingArguments.isForcedRestart()).isTrue());
            testInstance.transitionToSubsequentState();
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testInternalParallelismChangeBehavior(boolean parallelismChanged) throws Exception {
        try (MockExecutingContext adaptiveSchedulerCtx = new MockExecutingContext()) {
            final AtomicBoolean onChangeCalled = new AtomicBoolean();
            final Function<StateTransitionManager.Context, StateTransitionManager>
                    stateTransitionManagerFactory =
                            transitionCtx ->
                                    TestingStateTransitionManager.withOnChangeEventOnly(
                                            () -> {
                                                assertThat(transitionCtx.hasDesiredResources())
                                                        .isEqualTo(parallelismChanged);
                                                assertThat(transitionCtx.hasSufficientResources())
                                                        .isEqualTo(parallelismChanged);
                                                onChangeCalled.set(true);
                                            });

            final MockExecutionJobVertex mockExecutionJobVertex =
                    new MockExecutionJobVertex(MockExecutionVertex::new);

            final ExecutionGraph executionGraph =
                    new MockExecutionGraph(() -> Collections.singletonList(mockExecutionJobVertex));

            adaptiveSchedulerCtx.setHasDesiredResources(() -> true);
            adaptiveSchedulerCtx.setHasSufficientResources(() -> true);
            adaptiveSchedulerCtx.setVertexParallelism(
                    new VertexParallelism(
                            executionGraph.getAllVertices().values().stream()
                                    .collect(
                                            Collectors.toMap(
                                                    AccessExecutionJobVertex::getJobVertexId,
                                                    v ->
                                                            parallelismChanged
                                                                    ? 1 + v.getParallelism()
                                                                    : v.getParallelism()))));

            final Executing exec =
                    new ExecutingStateBuilder()
                            .setStateTransitionManagerFactory(stateTransitionManagerFactory)
                            .setExecutionGraph(executionGraph)
                            .build(adaptiveSchedulerCtx);

            exec.onNewResourcesAvailable();
            assertThat(onChangeCalled.get()).isTrue();
        }
    }

    public static TaskExecutionStateTransition createFailingStateTransition(
            ExecutionAttemptID attemptId, Exception exception) throws JobException {
        return new TaskExecutionStateTransition(
                new TaskExecutionState(attemptId, ExecutionState.FAILED, exception));
    }

    private final class ExecutingStateBuilder {
        private ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .build(EXECUTOR_EXTENSION.getExecutor());
        private OperatorCoordinatorHandler operatorCoordinatorHandler;
        private Function<StateTransitionManager.Context, StateTransitionManager>
                stateTransitionManagerFactory = context -> TestingStateTransitionManager.withNoOp();
        private int rescaleOnFailedCheckpointCount = 1;

        private ExecutingStateBuilder() throws JobException, JobExecutionException {
            operatorCoordinatorHandler = new TestingOperatorCoordinatorHandler();
        }

        public ExecutingStateBuilder setExecutionGraph(ExecutionGraph executionGraph) {
            this.executionGraph = executionGraph;
            return this;
        }

        public ExecutingStateBuilder setOperatorCoordinatorHandler(
                OperatorCoordinatorHandler operatorCoordinatorHandler) {
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
            return this;
        }

        public ExecutingStateBuilder setStateTransitionManagerFactory(
                Function<StateTransitionManager.Context, StateTransitionManager>
                        stateTransitionManagerFactory) {
            this.stateTransitionManagerFactory = stateTransitionManagerFactory;
            return this;
        }

        public ExecutingStateBuilder setRescaleOnFailedCheckpointCount(
                int rescaleOnFailedCheckpointCount) {
            this.rescaleOnFailedCheckpointCount = rescaleOnFailedCheckpointCount;
            return this;
        }

        private Executing build(MockExecutingContext ctx) {
            executionGraph.transitionToRunning();

            try {
                return new Executing(
                        executionGraph,
                        getExecutionGraphHandler(executionGraph, ctx.getMainThreadExecutor()),
                        operatorCoordinatorHandler,
                        log,
                        ctx,
                        ClassLoader.getSystemClassLoader(),
                        new ArrayList<>(),
                        stateTransitionManagerFactory::apply,
                        rescaleOnFailedCheckpointCount);
            } finally {
                Preconditions.checkState(
                        !ctx.hadStateTransition,
                        "State construction is an on-going state transition, during which no further transitions are allowed.");
            }
        }
    }

    private ExecutionGraphHandler getExecutionGraphHandler(
            ExecutionGraph executionGraph, ComponentMainThreadExecutor mainThreadExecutor) {
        return new ExecutionGraphHandler(
                executionGraph, log, mainThreadExecutor, mainThreadExecutor);
    }

    private static class MockExecutingContext extends MockStateWithExecutionGraphContext
            implements Executing.Context {

        private final StateValidator<FailingArguments> failingStateValidator =
                new StateValidator<>("failing");
        private final StateValidator<RestartingArguments> restartingStateValidator =
                new StateValidator<>("restarting");
        private final StateValidator<CancellingArguments> cancellingStateValidator =
                new StateValidator<>("cancelling");

        private Function<Throwable, FailureResult> howToHandleFailure;
        private StateValidator<StopWithSavepointArguments> stopWithSavepointValidator =
                new StateValidator<>("stopWithSavepoint");
        private CompletableFuture<String> mockedStopWithSavepointOperationFuture =
                new CompletableFuture<>();

        private VertexParallelism vertexParallelism = new VertexParallelism(Collections.emptyMap());
        private Supplier<Boolean> hasDesiredResourcesSupplier = () -> false;
        private Supplier<Boolean> hasSufficientResourcesSupplier = () -> false;

        public void setExpectFailing(Consumer<FailingArguments> asserter) {
            failingStateValidator.expectInput(asserter);
        }

        public void setExpectRestarting(Consumer<RestartingArguments> asserter) {
            restartingStateValidator.expectInput(asserter);
        }

        public void setExpectCancelling(Consumer<CancellingArguments> asserter) {
            cancellingStateValidator.expectInput(asserter);
        }

        public void setExpectStopWithSavepoint(Consumer<StopWithSavepointArguments> asserter) {
            stopWithSavepointValidator.expectInput(asserter);
        }

        public void setHowToHandleFailure(Function<Throwable, FailureResult> function) {
            this.howToHandleFailure = function;
        }

        public void setVertexParallelism(VertexParallelism vertexParallelism) {
            this.vertexParallelism = vertexParallelism;
        }

        public void setHasDesiredResources(Supplier<Boolean> sup) {
            hasDesiredResourcesSupplier = sup;
        }

        public void setHasSufficientResources(Supplier<Boolean> sup) {
            hasSufficientResourcesSupplier = sup;
        }

        // --------- Interface Implementations ------- //

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                List<ExceptionHistoryEntry> failureCollection) {
            cancellingStateValidator.validateInput(
                    new CancellingArguments(
                            executionGraph, executionGraphHandler, operatorCoordinatorHandler));
            hadStateTransition = true;
        }

        @Override
        public FailureResult howToHandleFailure(
                Throwable failure, CompletableFuture<Map<String, String>> failureLabels) {
            return howToHandleFailure.apply(failure);
        }

        @Override
        public Optional<VertexParallelism> getAvailableVertexParallelism() {
            return Optional.ofNullable(vertexParallelism);
        }

        @Override
        public void goToRestarting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime,
                boolean forcedRestart,
                List<ExceptionHistoryEntry> failureCollection) {
            restartingStateValidator.validateInput(
                    new RestartingArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            backoffTime,
                            forcedRestart));
            hadStateTransition = true;
        }

        @Override
        public void goToFailing(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause,
                List<ExceptionHistoryEntry> failureCollection) {
            failingStateValidator.validateInput(
                    new FailingArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            failureCause));
            hadStateTransition = true;
        }

        @Override
        public CompletableFuture<String> goToStopWithSavepoint(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                CheckpointScheduling checkpointScheduling,
                CompletableFuture<String> savepointFuture,
                List<ExceptionHistoryEntry> failureCollection) {
            stopWithSavepointValidator.validateInput(
                    new StopWithSavepointArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            checkpointScheduling,
                            savepointFuture));
            hadStateTransition = true;
            return mockedStopWithSavepointOperationFuture;
        }

        @Override
        public ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay) {
            return getMainThreadExecutor()
                    .schedule(
                            () -> runIfState(expectedState, action),
                            delay.toMillis(),
                            TimeUnit.MILLISECONDS);
        }

        @Override
        public boolean hasDesiredResources() {
            return hasDesiredResourcesSupplier.get();
        }

        @Override
        public boolean hasSufficientResources() {
            return hasSufficientResourcesSupplier.get();
        }

        @Override
        public void close() throws Exception {
            super.close();
            failingStateValidator.close();
            restartingStateValidator.close();
            cancellingStateValidator.close();
            stopWithSavepointValidator.close();
        }

        @Override
        public void archiveFailure(RootExceptionHistoryEntry failure) {}
    }

    static class CancellingArguments {
        private final ExecutionGraph executionGraph;
        private final ExecutionGraphHandler executionGraphHandler;
        private final OperatorCoordinatorHandler operatorCoordinatorHandle;

        public CancellingArguments(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandle) {
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandle = operatorCoordinatorHandle;
        }

        public ExecutionGraph getExecutionGraph() {
            return executionGraph;
        }

        public ExecutionGraphHandler getExecutionGraphHandler() {
            return executionGraphHandler;
        }

        public OperatorCoordinatorHandler getOperatorCoordinatorHandle() {
            return operatorCoordinatorHandle;
        }
    }

    static class StopWithSavepointArguments extends CancellingArguments {
        private final CheckpointScheduling checkpointScheduling;
        private final CompletableFuture<String> savepointFuture;

        public StopWithSavepointArguments(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandle,
                CheckpointScheduling checkpointScheduling,
                CompletableFuture<String> savepointFuture) {
            super(executionGraph, executionGraphHandler, operatorCoordinatorHandle);
            this.checkpointScheduling = checkpointScheduling;
            this.savepointFuture = savepointFuture;
        }
    }

    static class RestartingArguments extends CancellingArguments {
        private final Duration backoffTime;
        private final boolean forcedRestart;

        public RestartingArguments(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime,
                boolean forcedRestart) {
            super(executionGraph, executionGraphHandler, operatorCoordinatorHandler);
            this.backoffTime = backoffTime;
            this.forcedRestart = forcedRestart;
        }

        public Duration getBackoffTime() {
            return backoffTime;
        }

        public boolean isForcedRestart() {
            return forcedRestart;
        }
    }

    static class FailingArguments extends CancellingArguments {
        private final Throwable failureCause;

        public FailingArguments(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause) {
            super(executionGraph, executionGraphHandler, operatorCoordinatorHandler);
            this.failureCause = failureCause;
        }

        public Throwable getFailureCause() {
            return failureCause;
        }
    }

    static class MockExecutionGraph extends StateTrackingMockExecutionGraph {
        private final boolean updateStateReturnValue;
        private final Supplier<Iterable<ExecutionJobVertex>> getVerticesTopologicallySupplier;

        MockExecutionGraph(
                Supplier<Iterable<ExecutionJobVertex>> getVerticesTopologicallySupplier) {
            this(false, getVerticesTopologicallySupplier);
        }

        private MockExecutionGraph(
                boolean updateStateReturnValue,
                Supplier<Iterable<ExecutionJobVertex>> getVerticesTopologicallySupplier) {
            this.updateStateReturnValue = updateStateReturnValue;
            this.getVerticesTopologicallySupplier = getVerticesTopologicallySupplier;
        }

        @Override
        public boolean updateState(TaskExecutionStateTransition state) {
            return updateStateReturnValue;
        }

        @Override
        public Iterable<ExecutionJobVertex> getVerticesTopologically() {
            return getVerticesTopologicallySupplier.get();
        }
    }

    private static class FinishingMockExecutionGraph extends StateTrackingMockExecutionGraph {
        @Override
        public long getStatusTimestamp(JobStatus status) {
            switch (status) {
                case INITIALIZING:
                    return 1;
                case CREATED:
                    return 2;
                case RUNNING:
                    return 3;
                case FINISHED:
                    return 4;
            }
            return 0;
        }
    }

    private static class MockState implements State {
        @Override
        public void cancel() {}

        @Override
        public void suspend(Throwable cause) {}

        @Override
        public JobID getJobId() {
            return null;
        }

        @Override
        public JobStatus getJobStatus() {
            return null;
        }

        @Override
        public ArchivedExecutionGraph getJob() {
            return null;
        }

        @Override
        public void handleGlobalFailure(
                Throwable cause, CompletableFuture<Map<String, String>> failureLabels) {}

        @Override
        public Logger getLogger() {
            return null;
        }
    }

    static class MockExecutionJobVertex extends ExecutionJobVertex {
        private final ExecutionVertex mockExecutionVertex;

        MockExecutionJobVertex(
                Function<ExecutionJobVertex, ExecutionVertex> executionVertexSupplier)
                throws JobException {
            super(
                    new MockInternalExecutionGraphAccessor(),
                    new JobVertex("test"),
                    new DefaultVertexParallelismInfo(1, 1, max -> Optional.empty()),
                    new CoordinatorStoreImpl(),
                    UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());

            initialize(
                    1,
                    Duration.ofMillis(1L),
                    1L,
                    new DefaultSubtaskAttemptNumberStore(Collections.emptyList()));
            mockExecutionVertex = executionVertexSupplier.apply(this);
        }

        @Override
        public ExecutionVertex[] getTaskVertices() {
            return new ExecutionVertex[] {mockExecutionVertex};
        }

        public ExecutionVertex getMockExecutionVertex() {
            return mockExecutionVertex;
        }
    }

    static class FailOnDeployMockExecutionVertex extends ExecutionVertex {

        @Nullable private Throwable markFailed = null;

        public FailOnDeployMockExecutionVertex(ExecutionJobVertex jobVertex) {
            super(jobVertex, 1, new IntermediateResult[] {}, Duration.ofMillis(1L), 1L, 1, 0);
        }

        @Override
        public void deploy() throws JobException {
            throw new JobException("Intentional Test exception");
        }

        @Override
        public void markFailed(Throwable t) {
            markFailed = t;
        }

        @Nullable
        public Throwable getMarkedFailure() {
            return markFailed;
        }
    }

    static class MockExecutionVertex extends ExecutionVertex {
        private boolean deployCalled = false;
        private ExecutionState mockedExecutionState = ExecutionState.RUNNING;

        MockExecutionVertex(ExecutionJobVertex jobVertex) {
            super(jobVertex, 1, new IntermediateResult[] {}, Duration.ofMillis(1L), 1L, 1, 0);
        }

        @Override
        public void deploy() throws JobException {
            deployCalled = true;
        }

        public boolean isDeployCalled() {
            return deployCalled;
        }

        @Override
        public ExecutionState getExecutionState() {
            return mockedExecutionState;
        }

        public void setMockedExecutionState(ExecutionState mockedExecutionState) {
            this.mockedExecutionState = mockedExecutionState;
        }
    }

    private static class MockInternalExecutionGraphAccessor
            implements InternalExecutionGraphAccessor {

        @Override
        public Executor getFutureExecutor() {
            return ForkJoinPool.commonPool();
        }

        // --- mocked methods

        @Override
        public ClassLoader getUserClassLoader() {
            return null;
        }

        @Override
        public JobID getJobID() {
            return null;
        }

        @Override
        public BlobWriter getBlobWriter() {
            return null;
        }

        @Nonnull
        @Override
        public ComponentMainThreadExecutor getJobMasterMainThreadExecutor() {
            return null;
        }

        @Override
        public ShuffleMaster<?> getShuffleMaster() {
            return null;
        }

        @Override
        public JobMasterPartitionTracker getPartitionTracker() {
            return null;
        }

        @Override
        public void registerExecution(Execution exec) {}

        @Override
        public void deregisterExecution(Execution exec) {}

        @Override
        public PartitionGroupReleaseStrategy getPartitionGroupReleaseStrategy() {
            return null;
        }

        @Override
        public void failGlobal(Throwable t) {}

        @Override
        public void notifySchedulerNgAboutInternalTaskFailure(
                ExecutionAttemptID attemptId,
                Throwable t,
                boolean cancelTask,
                boolean releasePartitions) {}

        @Override
        public void jobVertexFinished() {}

        @Override
        public void jobVertexUnFinished() {}

        @Override
        public ExecutionDeploymentListener getExecutionDeploymentListener() {
            return null;
        }

        @Override
        public void notifyExecutionChange(
                Execution execution,
                ExecutionState previousState,
                ExecutionState newExecutionState) {}

        @Override
        public EdgeManager getEdgeManager() {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }

        @Override
        public ExecutionVertex getExecutionVertexOrThrow(ExecutionVertexID id) {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }

        @Override
        public IntermediateResultPartition getResultPartitionOrThrow(
                IntermediateResultPartitionID id) {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }

        @Override
        public void deleteBlobs(List<PermanentBlobKey> blobKeys) {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }

        @Override
        public ExecutionJobVertex getJobVertex(JobVertexID id) {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }

        @Override
        public boolean isDynamic() {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }

        @Override
        public MarkPartitionFinishedStrategy getMarkPartitionFinishedStrategy() {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }

        @Override
        public ExecutionGraphID getExecutionGraphID() {
            return new ExecutionGraphID();
        }

        @Override
        public List<ShuffleDescriptor> getClusterPartitionShuffleDescriptors(
                IntermediateDataSetID intermediateResultPartition) {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }

        @Override
        public JobVertexInputInfo getJobVertexInputInfo(
                JobVertexID jobVertexId, IntermediateDataSetID resultId) {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }

        @Override
        public TaskDeploymentDescriptorFactory getTaskDeploymentDescriptorFactory() {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }
    }
}
