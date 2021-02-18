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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

/** Tests for the {@link Canceling} state of the {@link AdaptiveScheduler}. */
public class CancelingTest extends TestLogger {

    @Test
    public void testExecutionGraphCancelationOnEnter() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            StateTrackingMockExecutionGraph stateTrackingMockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            createCancelingState(ctx, stateTrackingMockExecutionGraph);

            assertThat(stateTrackingMockExecutionGraph.getState(), is(JobStatus.CANCELLING));
        }
    }

    @Test
    public void testTransitionToFinishedWhenCancellationCompletes() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            StateTrackingMockExecutionGraph stateTrackingMockExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            Canceling canceling = createCancelingState(ctx, stateTrackingMockExecutionGraph);
            assertThat(stateTrackingMockExecutionGraph.getState(), is(JobStatus.CANCELLING));
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.CANCELED)));
            // this transitions the EG from CANCELLING to CANCELLED.
            stateTrackingMockExecutionGraph.completeTerminationFuture(JobStatus.CANCELED);
        }
    }

    @Test
    public void testTransitionToSuspend() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            Canceling canceling = createCancelingState(ctx, new StateTrackingMockExecutionGraph());
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED)));
            canceling.suspend(new RuntimeException("suspend"));
        }
    }

    @Test
    public void testCancelIsIgnored() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            Canceling canceling = createCancelingState(ctx, new StateTrackingMockExecutionGraph());
            canceling.cancel();
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testGlobalFailuresAreIgnored() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            Canceling canceling = createCancelingState(ctx, new StateTrackingMockExecutionGraph());
            canceling.handleGlobalFailure(new RuntimeException("test"));
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testTaskFailuresAreIgnored() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            StateTrackingMockExecutionGraph meg = new StateTrackingMockExecutionGraph();
            Canceling canceling = createCancelingState(ctx, meg);
            // register execution at EG
            ExecutingTest.MockExecutionJobVertex ejv = new ExecutingTest.MockExecutionJobVertex();
            TaskExecutionStateTransition update =
                    new TaskExecutionStateTransition(
                            new TaskExecutionState(
                                    ejv.getMockExecutionVertex()
                                            .getCurrentExecutionAttempt()
                                            .getAttemptId(),
                                    ExecutionState.FAILED,
                                    new RuntimeException()));
            canceling.updateTaskExecutionState(update);
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testStateDoesNotExposeGloballyTerminalExecutionGraph() throws Exception {
        try (MockStateWithExecutionGraphContext ctx = new MockStateWithExecutionGraphContext()) {
            StateTrackingMockExecutionGraph meg = new StateTrackingMockExecutionGraph();
            Canceling canceling = createCancelingState(ctx, meg);

            // ideally we'd delay the async call to #onGloballyTerminalState instead, but the
            // context does not support that
            ctx.setExpectFinished(eg -> {});

            meg.completeTerminationFuture(JobStatus.CANCELED);

            // this is just a sanity check for the test
            assertThat(meg.getState(), is(JobStatus.CANCELED));

            assertThat(canceling.getJobStatus(), is(JobStatus.CANCELLING));
            assertThat(canceling.getJob().getState(), is(JobStatus.CANCELLING));
            assertThat(canceling.getJob().getStatusTimestamp(JobStatus.CANCELED), is(0L));
        }
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
        return canceling;
    }
}
