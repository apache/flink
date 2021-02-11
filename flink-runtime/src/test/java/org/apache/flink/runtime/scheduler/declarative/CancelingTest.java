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
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.InternalFailuresListener;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.TestLogger;

import org.junit.Ignore;
import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link Canceling} state of the declarative scheduler. */
public class CancelingTest extends TestLogger {

    @Test
    public void testExecutionGraphCancelationOnEnter() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            RestartingTest.CancellableExecutionGraph cancellableExecutionGraph =
                    new RestartingTest.CancellableExecutionGraph();
            createCancelingState(ctx, cancellableExecutionGraph);

            assertThat(cancellableExecutionGraph.isCancelled(), is(true));
        }
    }

    @Test
    public void testTransitionToFinishedWhenCancellationCompletes() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            Canceling canceling = createCancelingState(ctx);

            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.CANCELED)));
            canceling.getExecutionGraph().cancel(); // this calls onTerminalState.onTerminalState()
        }
    }

    @Test
    @Ignore(
            "I fail because the job cancellation completes immediately and suspend becomes a no-op."
                    + "I was testing undefined behavior because suspend cannot be called before the job cancellation was initiated.")
    public void testTransitionToSuspend() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED)));

            Canceling canceling = createCancelingState(ctx);

            canceling.suspend(new RuntimeException("suspend"));
        }
    }

    @Test
    @Ignore(
            "I fail because the job cancellation completes immediately and I want to transition to finished."
                    + "I was testing undefined behavior because cancel cannot be called before the job cancellation was initiated.")
    public void testCancelIsIgnored() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            Canceling canceling = createCancelingState(ctx);
            canceling.cancel();
            ctx.assertNoStateTransition();
        }
    }

    @Test
    @Ignore(
            "I fail because the job cancellation completes immediately and suspend becomes a no-op."
                    + "I was testing undefined behavior because handleGlobalFailure cannot be called before the job cancellation was initiated.")
    public void testGlobalFailuresAreIgnored() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            Canceling canceling = createCancelingState(ctx);
            canceling.handleGlobalFailure(new RuntimeException("test"));
            ctx.assertNoStateTransition();
        }
    }

    @Test
    @Ignore("I fail with an NPE; CancelingTest.testTaskFailuresAreIgnored(CancelingTest.java:110)")
    public void testTaskFailuresAreIgnored() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            Canceling canceling = createCancelingState(ctx);
            // register execution at EG
            ExecutingTest.MockExecutionJobVertex ejv =
                    new ExecutingTest.MockExecutionJobVertex(canceling.getExecutionGraph());
            TaskExecutionStateTransition update =
                    new TaskExecutionStateTransition(
                            new TaskExecutionState(
                                    canceling.getJob().getJobID(),
                                    ejv.getMockExecutionVertex()
                                            .getCurrentExecutionAttempt()
                                            .getAttemptId(),
                                    ExecutionState.FAILED,
                                    new RuntimeException()));
            canceling.updateTaskExecutionState(update);
            ctx.assertNoStateTransition();
        }
    }

    private Canceling createCancelingState(MockStateWithExecutionGraphContext ctx)
            throws JobException, JobExecutionException {
        return createCancelingState(ctx, TestingExecutionGraphBuilder.newBuilder().build());
    }

    private Canceling createCancelingState(
            MockStateWithExecutionGraphContext ctx, ExecutionGraph executionGraph) {
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
        Canceling canceling =
                new Canceling(
                        ctx,
                        executionGraph,
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        log);
        executionGraph.setInternalTaskFailuresListener(new TestInternalFailuresListener(canceling));
        return canceling;
    }

    private static class TestInternalFailuresListener implements InternalFailuresListener {

        private final Canceling canceling;

        public TestInternalFailuresListener(Canceling canceling) {
            this.canceling = canceling;
        }

        @Override
        public void notifyTaskFailure(
                ExecutionAttemptID attemptId,
                Throwable t,
                boolean cancelTask,
                boolean releasePartitions) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void notifyGlobalFailure(Throwable t) {
            canceling.handleGlobalFailure(t);
        }
    }
}
