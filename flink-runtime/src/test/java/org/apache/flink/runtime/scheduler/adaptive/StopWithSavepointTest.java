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
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.runtime.scheduler.adaptive.ExecutingTest.createFailingStateTransition;
import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link StopWithSavepoint} state. */
public class StopWithSavepointTest extends TestLogger {
    private static final String SAVEPOINT_PATH = "test://savepoint/path";

    @Test
    public void testFinishedOnSuccessfulStopWithSavepoint() throws Exception {
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

            assertThat(sws.getOperationFuture().get(), is(SAVEPOINT_PATH));
        }
    }

    @Test
    public void testJobFailed() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            StopWithSavepoint sws = createStopWithSavepoint(ctx, mockExecutionGraph);
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);

            ctx.setExpectFailing(
                    failingArguments -> {
                        assertThat(
                                failingArguments.getExecutionGraph().getState(),
                                is(JobStatus.FAILED));
                        assertThat(
                                failingArguments.getFailureCause(),
                                containsCause(FlinkException.class));
                    });

            // fail job:
            mockExecutionGraph.completeTerminationFuture(JobStatus.FAILED);
            ctx.triggerExecutors();

            assertThat(sws.getOperationFuture().isCompletedExceptionally(), is(true));
        }
    }

    @Test
    public void testJobFailedAndSavepointOperationFails() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            CompletableFuture<String> savepointFuture = new CompletableFuture<>();
            StopWithSavepoint sws =
                    createStopWithSavepoint(ctx, mockExecutionGraph, savepointFuture);
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);

            ctx.setExpectFailing(
                    failingArguments -> {
                        assertThat(
                                failingArguments.getExecutionGraph().getState(),
                                is(JobStatus.FAILED));
                        assertThat(
                                failingArguments.getFailureCause(),
                                containsCause(FlinkException.class));
                    });

            // fail job:
            mockExecutionGraph.completeTerminationFuture(JobStatus.FAILED);
            savepointFuture.completeExceptionally(new RuntimeException());
            ctx.triggerExecutors();

            assertThat(sws.getOperationFuture().isCompletedExceptionally(), is(true));
        }
    }

    @Test
    public void testJobFinishedBeforeSavepointFuture() throws Exception {
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

            assertThat(sws.getOperationFuture().get(), is(SAVEPOINT_PATH));
        }
    }

    @Test
    public void testTransitionToCancellingOnCancel() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StopWithSavepoint sws = createStopWithSavepoint(ctx);
            ctx.setStopWithSavepoint(sws);
            ctx.setExpectCancelling(assertNonNull());

            sws.cancel();
        }
    }

    @Test
    public void testTransitionToFinishedOnSuspend() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StopWithSavepoint sws = createStopWithSavepoint(ctx);
            ctx.setExpectFinished(
                    archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED));
                    });

            sws.suspend(new RuntimeException());
        }
    }

    @Test
    public void testRestartOnGlobalFailureIfRestartConfigured() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            StopWithSavepoint sws = createStopWithSavepoint(ctx);
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(
                    (throwable) -> Executing.FailureResult.canRestart(throwable, Duration.ZERO));

            ctx.setExpectRestarting(assertNonNull());

            sws.handleGlobalFailure(new RuntimeException());
        }
    }

    @Test
    public void testFailingOnGlobalFailureIfNoRestartConfigured() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {

            StopWithSavepoint sws = createStopWithSavepoint(ctx);
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);

            ctx.setExpectFailing(
                    failingArguments -> {
                        assertThat(
                                failingArguments.getFailureCause(),
                                containsCause(RuntimeException.class));
                    });

            sws.handleGlobalFailure(new RuntimeException());
        }
    }

    @Test
    public void testFailingOnUpdateTaskExecutionStateWithNoRestart() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {

            StopWithSavepoint sws =
                    createStopWithSavepoint(ctx, new StateTrackingMockExecutionGraph());
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);

            ctx.setExpectFailing(
                    failingArguments -> {
                        assertThat(
                                failingArguments.getFailureCause(),
                                containsCause(RuntimeException.class));
                    });

            assertThat(sws.updateTaskExecutionState(createFailingStateTransition()), is(true));
        }
    }

    @Test
    public void testRestartingOnUpdateTaskExecutionStateWithRestart() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {

            StopWithSavepoint sws =
                    createStopWithSavepoint(ctx, new StateTrackingMockExecutionGraph());
            ctx.setStopWithSavepoint(sws);
            ctx.setHowToHandleFailure(
                    (throwable) -> Executing.FailureResult.canRestart(throwable, Duration.ZERO));

            ctx.setExpectRestarting(assertNonNull());

            assertThat(sws.updateTaskExecutionState(createFailingStateTransition()), is(true));
        }
    }

    @Test
    public void testExceptionalOperationFutureCompletionOnLeaveWhileWaitingOnSavepointCompletion()
            throws Exception {
        MockStopWithSavepointContext ctx = new MockStopWithSavepointContext();
        StopWithSavepoint sws = createStopWithSavepoint(ctx);
        ctx.setStopWithSavepoint(sws);

        sws.onLeave(Canceling.class);

        ctx.close();
        assertThat(sws.getOperationFuture().isCompletedExceptionally(), is(true));
    }

    @Test
    public void testExceptionalSavepointCompletionLeadsToExceptionalOperationFutureCompletion()
            throws Exception {
        MockStopWithSavepointContext ctx = new MockStopWithSavepointContext();
        CheckpointScheduling mockStopWithSavepointOperations = new MockCheckpointScheduling();
        CompletableFuture<String> savepointFuture = new CompletableFuture<>();
        StopWithSavepoint sws =
                createStopWithSavepoint(ctx, mockStopWithSavepointOperations, savepointFuture);
        ctx.setStopWithSavepoint(sws);
        ctx.setExpectExecuting(assertNonNull());

        savepointFuture.completeExceptionally(new RuntimeException("Test error"));

        ctx.close();
        assertThat(sws.getOperationFuture().isCompletedExceptionally(), is(true));
    }

    @Test
    public void testErrorCreatingSavepointLeadsToTransitionToExecutingState() throws Exception {
        MockStopWithSavepointContext ctx = new MockStopWithSavepointContext();
        CheckpointScheduling mockStopWithSavepointOperations = new MockCheckpointScheduling();
        CompletableFuture<String> savepointFuture = new CompletableFuture<>();
        StopWithSavepoint sws =
                createStopWithSavepoint(ctx, mockStopWithSavepointOperations, savepointFuture);
        ctx.setStopWithSavepoint(sws);
        ctx.setExpectExecuting(
                executingArguments ->
                        assertThat(
                                executingArguments.getExecutionGraph().getState(),
                                is(JobStatus.RUNNING)));

        savepointFuture.completeExceptionally(new RuntimeException("Test error"));

        ctx.close();
        assertThat(sws.getOperationFuture().isCompletedExceptionally(), is(true));
    }

    @Test
    public void testRestartOnTaskFailureAfterSavepointCompletion() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            CheckpointScheduling mockStopWithSavepointOperations = new MockCheckpointScheduling();
            CompletableFuture<String> savepointFuture = new CompletableFuture<>();
            StopWithSavepoint sws =
                    createStopWithSavepoint(ctx, mockStopWithSavepointOperations, savepointFuture);
            ctx.setStopWithSavepoint(sws);

            ctx.setHowToHandleFailure(
                    (throwable) -> Executing.FailureResult.canRestart(throwable, Duration.ZERO));

            ctx.setExpectRestarting(assertNonNull());

            // 1. complete savepoint future
            savepointFuture.complete(SAVEPOINT_PATH);
            ctx.triggerExecutors();

            // 2. fail task
            assertThat(sws.updateTaskExecutionState(createFailingStateTransition()), is(true));
        }
    }

    @Test
    public void testEnsureCheckpointSchedulerIsStartedAgain() throws Exception {
        try (MockStopWithSavepointContext ctx = new MockStopWithSavepointContext()) {
            MockCheckpointScheduling mockStopWithSavepointOperations =
                    new MockCheckpointScheduling();

            assertThat(mockStopWithSavepointOperations.isCheckpointSchedulerStarted(), is(false));

            CompletableFuture<String> savepointFuture = new CompletableFuture<>();
            StopWithSavepoint sws =
                    createStopWithSavepoint(ctx, mockStopWithSavepointOperations, savepointFuture);
            ctx.setStopWithSavepoint(sws);
            ctx.setExpectExecuting(assertNonNull()); // avoid "Unexpected state transition" failure

            // a failure should start the scheduler again
            savepointFuture.completeExceptionally(new RuntimeException("Test error"));
            ctx.triggerExecutors();
            assertThat(mockStopWithSavepointOperations.isCheckpointSchedulerStarted(), is(true));
        }
    }

    private StopWithSavepoint createStopWithSavepoint(MockStopWithSavepointContext ctx) {
        return createStopWithSavepoint(
                ctx,
                new MockCheckpointScheduling(),
                new StateTrackingMockExecutionGraph(),
                new CompletableFuture<>());
    }

    private StopWithSavepoint createStopWithSavepoint(
            MockStopWithSavepointContext ctx,
            ExecutionGraph executionGraph,
            CompletableFuture<String> savepointFuture) {
        return createStopWithSavepoint(
                ctx, new MockCheckpointScheduling(), executionGraph, savepointFuture);
    }

    private StopWithSavepoint createStopWithSavepoint(
            MockStopWithSavepointContext ctx, ExecutionGraph executionGraph) {
        return createStopWithSavepoint(ctx, executionGraph, new CompletableFuture<>());
    }

    private StopWithSavepoint createStopWithSavepoint(
            MockStopWithSavepointContext ctx,
            CheckpointScheduling checkpointScheduling,
            CompletableFuture<String> savepointFuture) {
        return createStopWithSavepoint(
                ctx, checkpointScheduling, new StateTrackingMockExecutionGraph(), savepointFuture);
    }

    private StopWithSavepoint createStopWithSavepoint(
            MockStopWithSavepointContext ctx,
            CheckpointScheduling checkpointScheduling,
            ExecutionGraph executionGraph,
            CompletableFuture<String> savepointFuture) {
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        log,
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
                log,
                ClassLoader.getSystemClassLoader(),
                savepointFuture);
    }

    private static class MockStopWithSavepointContext extends MockStateWithExecutionGraphContext
            implements StopWithSavepoint.Context {

        private Function<Throwable, Executing.FailureResult> howToHandleFailure;

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

        public void setHowToHandleFailure(Function<Throwable, Executing.FailureResult> function) {
            this.howToHandleFailure = function;
        }

        @Override
        public Executing.FailureResult howToHandleFailure(Throwable failure) {
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
                OperatorCoordinatorHandler operatorCoordinatorHandler) {
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
                Duration backoffTime) {
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
                Throwable failureCause) {
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
                OperatorCoordinatorHandler operatorCoordinatorHandler) {
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
