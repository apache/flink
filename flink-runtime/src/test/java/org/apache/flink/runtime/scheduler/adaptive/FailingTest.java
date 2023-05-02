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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.TestingAccessExecution;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link Failing} state of the {@link AdaptiveScheduler}. */
public class FailingTest extends TestLogger {

    private final Throwable testFailureCause = new RuntimeException();

    @Test
    public void testFailingStateOnEnter() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            StateTrackingMockExecutionGraph meg = new StateTrackingMockExecutionGraph();

            createFailingState(ctx, meg);

            assertThat(meg.getState(), is(JobStatus.FAILING));
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testTransitionToFailedWhenFailingCompletes() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            StateTrackingMockExecutionGraph meg = new StateTrackingMockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED)));
            meg.completeTerminationFuture(JobStatus.FAILED);
        }
    }

    @Test
    public void testTransitionToCancelingOnCancel() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            StateTrackingMockExecutionGraph meg = new StateTrackingMockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);
            ctx.setExpectCanceling(assertNonNull());
            failing.cancel();
        }
    }

    @Test
    public void testTransitionToFinishedOnSuspend() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            StateTrackingMockExecutionGraph meg = new StateTrackingMockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED)));
            failing.suspend(new RuntimeException("suspend"));
        }
    }

    @Test
    public void testIgnoreGlobalFailure() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            StateTrackingMockExecutionGraph meg = new StateTrackingMockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);
            failing.handleGlobalFailure(
                    new RuntimeException(), FailureEnricherUtils.EMPTY_FAILURE_LABELS);
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testTaskFailuresAreIgnored() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            StateTrackingMockExecutionGraph meg = new StateTrackingMockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);
            // register execution at EG
            Exception exception = new RuntimeException();
            TestingAccessExecution execution =
                    TestingAccessExecution.newBuilder()
                            .withExecutionState(ExecutionState.FAILED)
                            .withErrorInfo(new ErrorInfo(exception, System.currentTimeMillis()))
                            .build();
            meg.registerExecution(execution);
            TaskExecutionStateTransition update =
                    ExecutingTest.createFailingStateTransition(execution.getAttemptId(), exception);
            failing.updateTaskExecutionState(update, FailureEnricherUtils.EMPTY_FAILURE_LABELS);
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testStateDoesNotExposeGloballyTerminalExecutionGraph() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            StateTrackingMockExecutionGraph meg = new StateTrackingMockExecutionGraph();
            Failing failing = createFailingState(ctx, meg);

            // ideally we'd delay the async call to #onGloballyTerminalState instead, but the
            // context does not support that
            ctx.setExpectFinished(eg -> {});

            meg.completeTerminationFuture(JobStatus.FAILED);

            // this is just a sanity check for the test
            assertThat(meg.getState(), is(JobStatus.FAILED));

            assertThat(failing.getJobStatus(), is(JobStatus.FAILING));
            assertThat(failing.getJob().getState(), is(JobStatus.FAILING));
            assertThat(failing.getJob().getStatusTimestamp(JobStatus.FAILED), is(0L));
        }
    }

    private Failing createFailingState(MockFailingContext ctx, ExecutionGraph executionGraph) {
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        log,
                        ctx.getMainThreadExecutor(),
                        ctx.getMainThreadExecutor());
        final OperatorCoordinatorHandler operatorCoordinatorHandler =
                new TestingOperatorCoordinatorHandler();
        executionGraph.transitionToRunning();
        return new Failing(
                ctx,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                log,
                testFailureCause,
                ClassLoader.getSystemClassLoader(),
                new ArrayList<>());
    }

    private static class MockFailingContext extends MockStateWithExecutionGraphContext
            implements Failing.Context {

        private final StateValidator<ExecutingTest.CancellingArguments> cancellingStateValidator =
                new StateValidator<>("cancelling");

        public void setExpectCanceling(Consumer<ExecutingTest.CancellingArguments> asserter) {
            cancellingStateValidator.expectInput(asserter);
        }

        @Override
        public void archiveFailure(RootExceptionHistoryEntry failure) {}

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
        public void close() throws Exception {
            super.close();
            cancellingStateValidator.close();
        }
    }
}
