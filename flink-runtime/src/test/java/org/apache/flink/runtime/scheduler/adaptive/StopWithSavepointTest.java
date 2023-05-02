/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.TestingAccessExecution;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointStoppingException;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Tests for the {@link StopWithSavepoint} state. */
@ExtendWith(TestLoggerExtension.class)
class StopWithSavepointTest {
    private static final Logger LOG = LoggerFactory.getLogger(StopWithSavepointTest.class);

    private static final String SAVEPOINT_PATH = "test://savepoint/path";

    @Test
    void testFinishedOnSuccessfulStopWithSavepoint() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            CompletableFuture<String> savepointFuture = new CompletableFuture<>();

            StopWithSavepoint sws =
                    createStopWithSavepoint(ctx, mockExecutionGraph, savepointFuture);
            ctx.setStopWithSavepoint(sws);

            sws.onGloballyTerminalState(JobStatus.FINISHED);
            // this is a sanity check that we haven't scheduled a state transition
            ctx.triggerExecutors();

            ctx.setExpectFinished(assertNonNull());
            savepointFuture.complete(SAVEPOINT_PATH);
            ctx.triggerExecutors();

            assertThat(sws.getOperationFuture().get()).isEqualTo(SAVEPOINT_PATH);
        }
    }

    @Test
    void testJobFailedAndSavepointOperationSucceeds() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            final CompletableFuture<String> savepointFuture = new CompletableFuture<>();
            StopWithSavepoint sws =
                    createStopWithSavepoint(ctx, mockExecutionGraph, savepointFuture);
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(FailureResult::canNotRestart);

            // fail job:
            mockExecutionGraph.completeTerminationFuture(JobStatus.FAILED);
            // this is a sanity check that we haven't scheduled a state transition
            ctx.triggerExecutors();

            ctx.setExpectFailing(
                    failingArguments -> {
                        assertThat(failingArguments.getExecutionGraph().getState())
                                .isEqualTo(JobStatus.FAILED);
                        assertThat(failingArguments.getFailureCause())
                                .satisfies(
                                        FlinkAssertions.anyCauseMatches(
                                                StopWithSavepointStoppingException.class));
                    });

            savepointFuture.complete(SAVEPOINT_PATH);
            ctx.triggerExecutors();

            assertThat(sws.getOperationFuture()).isCompletedExceptionally();
        }
    }

    @Test
    void testJobFailedAndSavepointOperationFails() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            CompletableFuture<String> savepointFuture = new CompletableFuture<>();
            StopWithSavepoint sws =
                    createStopWithSavepoint(ctx, mockExecutionGraph, savepointFuture);
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(FailureResult::canNotRestart);

            ctx.setExpectFailing(
                    failingArguments -> {
                        assertThat(failingArguments.getExecutionGraph().getState())
                                .isEqualTo(JobStatus.FAILED);
                        assertThat(failingArguments.getFailureCause())
                                .satisfies(FlinkAssertions.anyCauseMatches(FlinkException.class));
                    });

            // fail job:
            mockExecutionGraph.completeTerminationFuture(JobStatus.FAILED);
            savepointFuture.completeExceptionally(new RuntimeException());
            ctx.triggerExecutors();

            assertThat(sws.getOperationFuture()).isCompletedExceptionally();
        }
    }

    @Test
    void testJobFinishedBeforeSavepointFuture() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            CompletableFuture<String> savepointFuture = new CompletableFuture<>();

            StopWithSavepoint sws =
                    createStopWithSavepoint(ctx, mockExecutionGraph, savepointFuture);
            ctx.setStopWithSavepoint(sws);
            ctx.setExpectFinished(assertNonNull());

            mockExecutionGraph.completeTerminationFuture(JobStatus.FINISHED);

            savepointFuture.complete(SAVEPOINT_PATH);
            ctx.triggerExecutors();

            assertThat(sws.getOperationFuture().get()).isEqualTo(SAVEPOINT_PATH);
        }
    }

    @Test
    void testTransitionToCancellingOnCancel() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StopWithSavepoint sws = createStopWithSavepoint(ctx);
            ctx.setStopWithSavepoint(sws);
            ctx.setExpectCancelling(assertNonNull());

            sws.cancel();
        }
    }

    @Test
    void testTransitionToFinishedOnSuspend() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StopWithSavepoint sws = createStopWithSavepoint(ctx);
            ctx.setExpectFinished(
                    archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState())
                                .isEqualTo(JobStatus.SUSPENDED);
                    });

            sws.suspend(new RuntimeException());
        }
    }

    @Test
    void testRestartOnGlobalFailureIfRestartConfigured() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            final CompletableFuture<String> savepointFuture = CompletableFuture.completedFuture("");
            StopWithSavepoint sws = createStopWithSavepoint(ctx, savepointFuture);
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(failure -> FailureResult.canRestart(failure, Duration.ZERO));

            ctx.setExpectRestarting(assertNonNull());

            sws.handleGlobalFailure(
                    new RuntimeException(), FailureEnricherUtils.EMPTY_FAILURE_LABELS);
        }
    }

    @Test
    void testFailingOnGlobalFailureIfNoRestartConfigured() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {

            final CompletableFuture<String> savepointFuture = CompletableFuture.completedFuture("");
            StopWithSavepoint sws = createStopWithSavepoint(ctx, savepointFuture);
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(FailureResult::canNotRestart);

            ctx.setExpectFailing(
                    failingArguments -> {
                        assertThat(failingArguments.getFailureCause())
                                .satisfies(FlinkAssertions.anyCauseMatches(RuntimeException.class));
                    });

            sws.handleGlobalFailure(
                    new RuntimeException(), FailureEnricherUtils.EMPTY_FAILURE_LABELS);
        }
    }

    @Test
    void testFailingOnUpdateTaskExecutionStateWithNoRestart() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StateTrackingMockExecutionGraph executionGraph = new StateTrackingMockExecutionGraph();
            final CompletableFuture<String> savepointFuture = CompletableFuture.completedFuture("");
            StopWithSavepoint sws = createStopWithSavepoint(ctx, executionGraph, savepointFuture);
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(FailureResult::canNotRestart);

            ctx.setExpectFailing(
                    failingArguments -> {
                        assertThat(failingArguments.getFailureCause())
                                .satisfies(FlinkAssertions.anyCauseMatches(RuntimeException.class));
                    });

            Exception exception = new RuntimeException();
            TestingAccessExecution execution =
                    TestingAccessExecution.newBuilder()
                            .withExecutionState(ExecutionState.FAILED)
                            .withErrorInfo(new ErrorInfo(exception, System.currentTimeMillis()))
                            .build();
            executionGraph.registerExecution(execution);
            TaskExecutionStateTransition taskExecutionStateTransition =
                    ExecutingTest.createFailingStateTransition(execution.getAttemptId(), exception);
            assertThat(
                            sws.updateTaskExecutionState(
                                    taskExecutionStateTransition,
                                    FailureEnricherUtils.EMPTY_FAILURE_LABELS))
                    .isTrue();
        }
    }

    @Test
    void testRestartingOnUpdateTaskExecutionStateWithRestart() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StateTrackingMockExecutionGraph executionGraph = new StateTrackingMockExecutionGraph();
            final CompletableFuture<String> savepointFuture = CompletableFuture.completedFuture("");
            StopWithSavepoint sws = createStopWithSavepoint(ctx, executionGraph, savepointFuture);
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(failure -> FailureResult.canRestart(failure, Duration.ZERO));

            ctx.setExpectRestarting(assertNonNull());

            Exception exception = new RuntimeException();
            TestingAccessExecution execution =
                    TestingAccessExecution.newBuilder()
                            .withExecutionState(ExecutionState.FAILED)
                            .withErrorInfo(new ErrorInfo(exception, System.currentTimeMillis()))
                            .build();
            executionGraph.registerExecution(execution);
            TaskExecutionStateTransition taskExecutionStateTransition =
                    ExecutingTest.createFailingStateTransition(execution.getAttemptId(), exception);
            assertThat(
                            sws.updateTaskExecutionState(
                                    taskExecutionStateTransition,
                                    FailureEnricherUtils.EMPTY_FAILURE_LABELS))
                    .isTrue();
        }
    }

    @Test
    void testExceptionalOperationFutureCompletionOnLeaveWhileWaitingOnSavepointCompletion()
            throws Exception {
        final StopWithSavepoint sws;
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            sws = createStopWithSavepoint(ctx);
            ctx.setStopWithSavepoint(sws);

            sws.onLeave(Canceling.class);
        }
        assertThat(sws.getOperationFuture()).isCompletedExceptionally();
    }

    @Test
    void testExceptionalSavepointCompletionLeadsToExceptionalOperationFutureCompletion()
            throws Exception {
        final StopWithSavepoint sws;
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            CheckpointScheduling mockStopWithSavepointOperations = new MockCheckpointScheduling();
            CompletableFuture<String> savepointFuture = new CompletableFuture<>();
            sws = createStopWithSavepoint(ctx, mockStopWithSavepointOperations, savepointFuture);
            ctx.setStopWithSavepoint(sws);
            ctx.setExpectExecuting(assertNonNull());

            savepointFuture.completeExceptionally(new RuntimeException("Test error"));
        }
        assertThat(sws.getOperationFuture()).isCompletedExceptionally();
    }

    @Test
    void testErrorCreatingSavepointLeadsToTransitionToExecutingState() throws Exception {
        final StopWithSavepoint sws;
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            CheckpointScheduling mockStopWithSavepointOperations = new MockCheckpointScheduling();
            CompletableFuture<String> savepointFuture = new CompletableFuture<>();
            sws = createStopWithSavepoint(ctx, mockStopWithSavepointOperations, savepointFuture);
            ctx.setStopWithSavepoint(sws);
            ctx.setExpectExecuting(
                    executingArguments ->
                            assertThat(executingArguments.getExecutionGraph().getState())
                                    .isEqualTo(JobStatus.RUNNING));

            savepointFuture.completeExceptionally(new RuntimeException("Test error"));
        }
        assertThat(sws.getOperationFuture()).isCompletedExceptionally();
    }

    @Test
    void testRestartOnTaskFailureAfterSavepointCompletion() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            CheckpointScheduling mockStopWithSavepointOperations = new MockCheckpointScheduling();
            CompletableFuture<String> savepointFuture = new CompletableFuture<>();
            StateTrackingMockExecutionGraph executionGraph = new StateTrackingMockExecutionGraph();
            StopWithSavepoint sws =
                    createStopWithSavepoint(
                            ctx, mockStopWithSavepointOperations, executionGraph, savepointFuture);
            ctx.setStopWithSavepoint(sws);

            ctx.setHowToHandleFailure(failure -> FailureResult.canRestart(failure, Duration.ZERO));

            ctx.setExpectRestarting(assertNonNull());

            // 1. complete savepoint future
            savepointFuture.complete(SAVEPOINT_PATH);
            ctx.triggerExecutors();

            // 2. fail task
            Exception exception = new RuntimeException();
            TestingAccessExecution execution =
                    TestingAccessExecution.newBuilder()
                            .withExecutionState(ExecutionState.FAILED)
                            .withErrorInfo(new ErrorInfo(exception, System.currentTimeMillis()))
                            .build();
            executionGraph.registerExecution(execution);
            TaskExecutionStateTransition taskExecutionStateTransition =
                    ExecutingTest.createFailingStateTransition(execution.getAttemptId(), exception);
            assertThat(
                            sws.updateTaskExecutionState(
                                    taskExecutionStateTransition,
                                    FailureEnricherUtils.EMPTY_FAILURE_LABELS))
                    .isTrue();
        }
    }

    @Test
    void testOnFailureWaitsForSavepointCompletion() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            CheckpointScheduling mockStopWithSavepointOperations = new MockCheckpointScheduling();
            CompletableFuture<String> savepointFuture = new CompletableFuture<>();
            StateTrackingMockExecutionGraph executionGraph = new StateTrackingMockExecutionGraph();
            StopWithSavepoint sws =
                    createStopWithSavepoint(
                            ctx, mockStopWithSavepointOperations, executionGraph, savepointFuture);
            ctx.setStopWithSavepoint(sws);

            ctx.setHowToHandleFailure(failure -> FailureResult.canRestart(failure, Duration.ZERO));

            sws.onFailure(new Exception("task failure"));
            // this is a sanity check that we haven't scheduled a state transition
            ctx.triggerExecutors();

            ctx.setExpectRestarting(assertNonNull());
            savepointFuture.complete(SAVEPOINT_PATH);
            ctx.triggerExecutors();
        }
    }

    @Test
    void testConcurrentSavepointFailureAndGloballyTerminalStateCauseRestart() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            CheckpointScheduling mockStopWithSavepointOperations = new MockCheckpointScheduling();
            CompletableFuture<String> savepointFuture = new CompletableFuture<>();
            StateTrackingMockExecutionGraph executionGraph = new StateTrackingMockExecutionGraph();
            StopWithSavepoint sws =
                    createStopWithSavepoint(
                            ctx, mockStopWithSavepointOperations, executionGraph, savepointFuture);
            ctx.setStopWithSavepoint(sws);

            ctx.setHowToHandleFailure(failure -> FailureResult.canRestart(failure, Duration.ZERO));

            sws.onFailure(new Exception("task failure"));
            // this is a sanity check that we haven't scheduled a state transition
            ctx.triggerExecutors();

            ctx.setExpectRestarting(assertNonNull());
            savepointFuture.completeExceptionally(new Exception("savepoint failure"));
            ctx.triggerExecutors();
        }
    }

    @Test
    void testEnsureCheckpointSchedulerIsStartedAgain() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            MockCheckpointScheduling mockStopWithSavepointOperations =
                    new MockCheckpointScheduling();

            assertThat(mockStopWithSavepointOperations.isCheckpointSchedulerStarted()).isFalse();

            CompletableFuture<String> savepointFuture = new CompletableFuture<>();
            StopWithSavepoint sws =
                    createStopWithSavepoint(ctx, mockStopWithSavepointOperations, savepointFuture);
            ctx.setStopWithSavepoint(sws);
            ctx.setExpectExecuting(assertNonNull()); // avoid "Unexpected state transition" failure

            // a failure should start the scheduler again
            savepointFuture.completeExceptionally(new RuntimeException("Test error"));
            ctx.triggerExecutors();
            assertThat(mockStopWithSavepointOperations.isCheckpointSchedulerStarted()).isTrue();
        }
    }

    private static StopWithSavepoint createStopWithSavepoint(MockStopWithSavepointContext ctx) {
        return createStopWithSavepoint(
                ctx,
                new MockCheckpointScheduling(),
                new StateTrackingMockExecutionGraph(),
                new CompletableFuture<>());
    }

    private static StopWithSavepoint createStopWithSavepoint(
            MockStopWithSavepointContext ctx, CompletableFuture<String> savepointFuture) {
        return createStopWithSavepoint(
                ctx,
                new MockCheckpointScheduling(),
                new StateTrackingMockExecutionGraph(),
                savepointFuture);
    }

    private static StopWithSavepoint createStopWithSavepoint(
            MockStopWithSavepointContext ctx,
            ExecutionGraph executionGraph,
            CompletableFuture<String> savepointFuture) {
        return createStopWithSavepoint(
                ctx, new MockCheckpointScheduling(), executionGraph, savepointFuture);
    }

    private static StopWithSavepoint createStopWithSavepoint(
            MockStopWithSavepointContext ctx,
            CheckpointScheduling checkpointScheduling,
            CompletableFuture<String> savepointFuture) {
        return createStopWithSavepoint(
                ctx, checkpointScheduling, new StateTrackingMockExecutionGraph(), savepointFuture);
    }

    private static StopWithSavepoint createStopWithSavepoint(
            MockStopWithSavepointContext ctx,
            CheckpointScheduling checkpointScheduling,
            ExecutionGraph executionGraph,
            CompletableFuture<String> savepointFuture) {
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        LOG,
                        ctx.getMainThreadExecutor(),
                        ctx.getMainThreadExecutor());
        OperatorCoordinatorHandler operatorCoordinatorHandler =
                new TestingOperatorCoordinatorHandler();

        executionGraph.transitionToRunning();

        return new StopWithSavepoint(
                ctx,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                checkpointScheduling,
                LOG,
                ClassLoader.getSystemClassLoader(),
                savepointFuture,
                new ArrayList<>());
    }

    private static class MockStopWithSavepointContext extends MockStateWithExecutionGraphContext
            implements StopWithSavepoint.Context {

        private Function<Throwable, FailureResult> howToHandleFailure;

        private final StateValidator<ExecutingTest.FailingArguments> failingStateValidator =
                new StateValidator<>("failing");
        private final StateValidator<ExecutingTest.RestartingArguments> restartingStateValidator =
                new StateValidator<>("restarting");
        private final StateValidator<ExecutingTest.CancellingArguments> cancellingStateValidator =
                new StateValidator<>("cancelling");

        private final StateValidator<ExecutingTest.CancellingArguments> executingStateTransition =
                new StateValidator<>("executing");

        private StopWithSavepoint state;

        public void setStopWithSavepoint(StopWithSavepoint sws) {
            this.state = sws;
        }

        public void setExpectFailing(Consumer<ExecutingTest.FailingArguments> asserter) {
            failingStateValidator.expectInput(asserter);
        }

        public void setExpectRestarting(Consumer<ExecutingTest.RestartingArguments> asserter) {
            restartingStateValidator.expectInput(asserter);
        }

        public void setExpectCancelling(Consumer<ExecutingTest.CancellingArguments> asserter) {
            cancellingStateValidator.expectInput(asserter);
        }

        public void setExpectExecuting(Consumer<ExecutingTest.CancellingArguments> asserter) {
            executingStateTransition.expectInput(asserter);
        }

        public void setHowToHandleFailure(Function<Throwable, FailureResult> function) {
            this.howToHandleFailure = function;
        }

        @Override
        public FailureResult howToHandleFailure(Throwable failure) {
            return howToHandleFailure.apply(failure);
        }

        private void simulateTransitionToState(Class<? extends State> target) {
            checkNotNull(
                    state,
                    "StopWithSavepoint state must be set via setStopWithSavepoint() to call onLeave() on leaving the state");
            state.onLeave(target);
        }

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                List<ExceptionHistoryEntry> failureCollection) {
            if (hadStateTransition) {
                throw new IllegalStateException("Only one state transition is allowed.");
            }
            simulateTransitionToState(Canceling.class);

            cancellingStateValidator.validateInput(
                    new ExecutingTest.CancellingArguments(
                            executionGraph, executionGraphHandler, operatorCoordinatorHandler));
            hadStateTransition = true;
        }

        @Override
        public void goToRestarting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime,
                List<ExceptionHistoryEntry> failureCollection) {
            if (hadStateTransition) {
                throw new IllegalStateException("Only one state transition is allowed.");
            }
            simulateTransitionToState(Restarting.class);
            restartingStateValidator.validateInput(
                    new ExecutingTest.RestartingArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            backoffTime));
            hadStateTransition = true;
        }

        @Override
        public void goToFailing(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause,
                List<ExceptionHistoryEntry> failureCollection) {
            if (hadStateTransition) {
                throw new IllegalStateException("Only one state transition is allowed.");
            }
            simulateTransitionToState(Failing.class);
            failingStateValidator.validateInput(
                    new ExecutingTest.FailingArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            failureCause));
            hadStateTransition = true;
        }

        @Override
        public void goToExecuting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                List<ExceptionHistoryEntry> failureCollection) {
            if (hadStateTransition) {
                throw new IllegalStateException("Only one state transition is allowed.");
            }
            simulateTransitionToState(Executing.class);
            executingStateTransition.validateInput(
                    new ExecutingTest.CancellingArguments(
                            executionGraph, executionGraphHandler, operatorCoordinatorHandler));
            hadStateTransition = true;
        }

        @Override
        public boolean isState(State expectedState) {
            return !hadStateTransition;
        }

        @Override
        public ScheduledFuture<?> runIfState(State state, Runnable runnable, Duration delay) {
            if (!delay.isZero()) {
                throw new UnsupportedOperationException(
                        "Currently only immediate execution is supported");
            }

            return getMainThreadExecutor()
                    .schedule(
                            () -> {
                                if (isState(state)) {
                                    runnable.run();
                                }
                            },
                            delay.toMillis(),
                            TimeUnit.MILLISECONDS);
        }

        @Override
        public void close() throws Exception {
            super.close();
            failingStateValidator.close();
            restartingStateValidator.close();
            cancellingStateValidator.close();
            executingStateTransition.close();
        }

        @Override
        public void handleGlobalFailure(Throwable cause) {
            state.handleGlobalFailure(cause, FailureEnricherUtils.EMPTY_FAILURE_LABELS);
        }
    }

    private static class MockCheckpointScheduling implements CheckpointScheduling {
        private boolean checkpointSchedulerStarted = false;

        @Override
        public void startCheckpointScheduler() {
            checkpointSchedulerStarted = true;
        }

        @Override
        public void stopCheckpointScheduler() {
            checkpointSchedulerStarted = false;
        }

        boolean isCheckpointSchedulerStarted() {
            return checkpointSchedulerStarted;
        }
    }
}
