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

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.function.Consumer;

import static org.apache.flink.runtime.scheduler.declarative.WaitingForResourcesTest.assertNonNull;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link Failing} state of the declarative scheduler. */
public class FailingTest extends TestLogger {
    private final Throwable testFailureCause = new RuntimeException();

    @Test
    public void testFailJobOnEnter() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            Failing failing = createFailingState(ctx);
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED)));

            failing.onEnter();
        }
    }

    @Test
    public void testIgnoreGlobalFailure() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            Failing failing = createFailingState(ctx);
            failing.handleGlobalFailure(new RuntimeException());
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testTransitionToCancelOnCancel() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            Failing failing = createFailingState(ctx);
            ctx.setExpectCanceling(assertNonNull());
            failing.cancel();
        }
    }

    @Test
    public void testTransitionToFinishedOnSuspend() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            Failing failing = createFailingState(ctx);
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED)));

            failing.onEnter();
            failing.suspend(new RuntimeException("suspend"));
        }
    }

    @Test
    public void testJobStatusChangeOnEnter() throws Exception {
        try (MockFailingContext ctx = new MockFailingContext()) {
            Failing failing = createFailingState(ctx);
            ctx.setExpectFinished(assertNonNull());

            assertThat(failing.getJobStatus(), is(JobStatus.RUNNING));

            failing.onEnter();

            assertThat(failing.getJobStatus(), is(JobStatus.FAILED));
        }
    }

    private Failing createFailingState(MockFailingContext ctx)
            throws JobException, JobExecutionException {
        ExecutionGraph executionGraph = TestingExecutionGraphBuilder.newBuilder().build();
        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        log,
                        ctx.getMainThreadExecutor(),
                        ctx.getMainThreadExecutor());
        final OperatorCoordinatorHandler operatorCoordinatorHandler =
                new OperatorCoordinatorHandler(
                        executionGraph,
                        (throwable) -> {
                            throw new RuntimeException("Error in test", throwable);
                        });
        executionGraph.transitionToRunning();
        return new Failing(
                ctx,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                log,
                testFailureCause);
    }

    private static class MockFailingContext extends MockStateWithExecutionGraphContext
            implements Failing.Context {

        private final StateValidator<ExecutingTest.CancellingArguments> cancellingStateValidator =
                new StateValidator<>("cancelling");

        public void setExpectCanceling(Consumer<ExecutingTest.CancellingArguments> asserter) {
            cancellingStateValidator.expectInput(asserter);
        }

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler) {
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
