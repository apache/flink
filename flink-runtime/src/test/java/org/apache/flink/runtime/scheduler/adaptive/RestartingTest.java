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

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.core.testutils.CompletedScheduledFuture;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link Restarting} state of the {@link AdaptiveScheduler}. */
class RestartingTest {
    private static final Logger log = LoggerFactory.getLogger(RestartingTest.class);

    @Test
    void testExecutionGraphCancellationOnEnter() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            createRestartingState(ctx, mockExecutionGraph);

            assertThat(mockExecutionGraph.getState()).isEqualTo(JobStatus.CANCELLING);
        }
    }

    @Test
    void testTransitionToWaitingForResourcesWhenCancellationComplete() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            Restarting restarting = createRestartingState(ctx);
            ctx.setExpectWaitingForResources();
            restarting.onGloballyTerminalState(JobStatus.CANCELED);
        }
    }

    @Test
    void testCancel() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            Restarting restarting = createRestartingState(ctx);
            ctx.setExpectCancelling(assertNonNull());
            restarting.cancel();
        }
    }

    @Test
    void testSuspendWithJobInCancellingState() throws Exception {
        testSuspend(false);
    }

    @Test
    void testSuspendWithJobInCancelledState() throws Exception {
        testSuspend(true);
    }

    private void testSuspend(boolean cancellationCompleted) throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            final StateTrackingMockExecutionGraph executionGraph =
                    new StateTrackingMockExecutionGraph();
            final Restarting restarting = createRestartingState(ctx, executionGraph);

            if (cancellationCompleted) {
                executionGraph.completeTerminationFuture(JobStatus.CANCELED);
            }

            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState())
                                    .isEqualTo(JobStatus.SUSPENDED));
            final Throwable cause = new RuntimeException("suspend");
            restarting.suspend(cause);
        }
    }

    @Test
    void testGlobalFailuresAreIgnored() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            Restarting restarting = createRestartingState(ctx);
            restarting.handleGlobalFailure(
                    new RuntimeException(), FailureEnricherUtils.EMPTY_FAILURE_LABELS);
            ctx.assertNoStateTransition();
        }
    }

    @Test
    void testStateDoesNotExposeGloballyTerminalExecutionGraph() throws Exception {
        try (MockRestartingContext ctx = new MockRestartingContext()) {
            StateTrackingMockExecutionGraph mockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            Restarting restarting = createRestartingState(ctx, mockExecutionGraph);

            // ideally we'd just delay the state transitions, but the context does not support that
            ctx.setExpectWaitingForResources();
            mockExecutionGraph.completeTerminationFuture(JobStatus.CANCELED);

            // this is just a sanity check for the test
            assertThat(restarting.getExecutionGraph().getState()).isEqualTo(JobStatus.CANCELED);

            assertThat(restarting.getJobStatus()).isEqualTo(JobStatus.RESTARTING);
            assertThat(restarting.getJob().getState()).isEqualTo(JobStatus.RESTARTING);
            assertThat(restarting.getJob().getStatusTimestamp(JobStatus.CANCELED)).isZero();
        }
    }

    public Restarting createRestartingState(
            MockRestartingContext ctx, ExecutionGraph executionGraph) {
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        log,
                        ctx.getMainThreadExecutor(),
                        ctx.getMainThreadExecutor());
        final OperatorCoordinatorHandler operatorCoordinatorHandler =
                new TestingOperatorCoordinatorHandler();
        executionGraph.transitionToRunning();
        return new Restarting(
                ctx,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                log,
                Duration.ZERO,
                ClassLoader.getSystemClassLoader(),
                new ArrayList<>());
    }

    public Restarting createRestartingState(MockRestartingContext ctx)
            throws JobException, JobExecutionException {
        return createRestartingState(ctx, new StateTrackingMockExecutionGraph());
    }

    private static class MockRestartingContext extends MockStateWithExecutionGraphContext
            implements Restarting.Context {

        private final StateValidator<ExecutingTest.CancellingArguments> cancellingStateValidator =
                new StateValidator<>("Cancelling");

        private final StateValidator<Void> waitingForResourcesStateValidator =
                new StateValidator<>("WaitingForResources");

        public void setExpectCancelling(Consumer<ExecutingTest.CancellingArguments> asserter) {
            cancellingStateValidator.expectInput(asserter);
        }

        public void setExpectWaitingForResources() {
            waitingForResourcesStateValidator.expectInput((none) -> {});
        }

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                List<ExceptionHistoryEntry> failureCollection) {
            cancellingStateValidator.validateInput(
                    new ExecutingTest.CancellingArguments(
                            executionGraph, executionGraphHandler, operatorCoordinatorHandler));
            hadStateTransition = true;
        }

        @Override
        public void archiveFailure(RootExceptionHistoryEntry failure) {}

        @Override
        public void goToWaitingForResources(ExecutionGraph previousExecutionGraph) {
            waitingForResourcesStateValidator.validateInput(null);
            hadStateTransition = true;
        }

        @Override
        public ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay) {
            if (!hadStateTransition) {
                action.run();
            }
            return CompletedScheduledFuture.create(null);
        }

        @Override
        public void close() throws Exception {
            super.close();
            cancellingStateValidator.close();
            waitingForResourcesStateValidator.close();
        }
    }
}
