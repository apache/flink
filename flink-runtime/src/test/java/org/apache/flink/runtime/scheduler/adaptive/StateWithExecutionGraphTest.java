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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;

import java.util.concurrent.CompletableFuture;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/** Tests for the {@link StateWithExecutionGraph} state. */
public class StateWithExecutionGraphTest extends TestLogger {

    /**
     * Since we execute the {@link StateWithExecutionGraph#onGloballyTerminalState} callback
     * asynchronously, {@link StateWithExecutionGraph#suspend} needs to support that it is called
     * when the ExecutionGraph has already reached a globally terminal state.
     */
    @Test
    public void testSuspendCanBeCalledWhenExecutionGraphHasReachedGloballyTerminalState()
            throws Exception {
        try (MockStateWithExecutionGraphContext context =
                new MockStateWithExecutionGraphContext()) {
            final StateTrackingMockExecutionGraph testingExecutionGraph =
                    new StateTrackingMockExecutionGraph();
            testingExecutionGraph.transitionToRunning();

            final TestingStateWithExecutionGraph stateWithExecutionGraph =
                    createStateWithExecutionGraph(context, testingExecutionGraph);

            context.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED)));

            // transition to FAILED
            testingExecutionGraph.failJob(
                    new FlinkException("Transition job to FAILED state"),
                    System.currentTimeMillis());
            testingExecutionGraph.completeTerminationFuture(JobStatus.FAILED);

            assertThat(testingExecutionGraph.getState(), is(JobStatus.FAILED));

            // As long as we don't execute StateWithExecutionGraph#onGloballyTerminalState
            // immediately when reaching a globally terminal state or if don't immediately leave
            // this state when reaching a globally terminal state, this test is still valid because
            // the suspend call can happen asynchronously.
            assertFalse(stateWithExecutionGraph.getGloballyTerminalStateFuture().isDone());
            stateWithExecutionGraph.suspend(new FlinkException("Test exception"));
        }
    }

    @Test
    public void testOperatorCoordinatorShutdownOnLeave() throws Exception {
        try (MockStateWithExecutionGraphContext context =
                new MockStateWithExecutionGraphContext()) {

            final TestingOperatorCoordinatorHandler testingOperatorCoordinatorHandler =
                    new TestingOperatorCoordinatorHandler();
            final TestingStateWithExecutionGraph stateWithExecutionGraph =
                    createStateWithExecutionGraph(context, testingOperatorCoordinatorHandler);

            stateWithExecutionGraph.onLeave(AdaptiveSchedulerTest.DummyState.class);

            assertThat(testingOperatorCoordinatorHandler.isDisposed(), is(true));
        }
    }

    @Test
    public void testSuspendToFinished() throws Exception {
        try (MockStateWithExecutionGraphContext context =
                new MockStateWithExecutionGraphContext()) {

            final TestingStateWithExecutionGraph stateWithExecutionGraph =
                    createStateWithExecutionGraph(context);

            context.setExpectFinished(aeg -> assertThat(aeg.getState(), is(JobStatus.SUSPENDED)));

            stateWithExecutionGraph.suspend(new RuntimeException());
        }
    }

    @Test
    public void testOnGloballyTerminalStateCalled() throws Exception {
        MockStateWithExecutionGraphContext context = new MockStateWithExecutionGraphContext();

        StateTrackingMockExecutionGraph mockExecutionGraph = new StateTrackingMockExecutionGraph();
        final TestingStateWithExecutionGraph stateWithExecutionGraph =
                createStateWithExecutionGraph(context, mockExecutionGraph);

        mockExecutionGraph.completeTerminationFuture(JobStatus.FINISHED);

        context.close();

        assertThat(
                stateWithExecutionGraph.getGloballyTerminalStateFuture().get(),
                is(JobStatus.FINISHED));
    }

    @Test
    public void testOnGloballyTerminalStateNotCalledOnNonGloballyTerminalState() throws Exception {
        MockStateWithExecutionGraphContext context = new MockStateWithExecutionGraphContext();

        StateTrackingMockExecutionGraph mockExecutionGraph = new StateTrackingMockExecutionGraph();
        final TestingStateWithExecutionGraph stateWithExecutionGraph =
                createStateWithExecutionGraph(context, mockExecutionGraph);

        mockExecutionGraph.completeTerminationFuture(JobStatus.SUSPENDED);

        context.close();

        assertThat(stateWithExecutionGraph.getGloballyTerminalStateFuture().isDone(), is(false));
    }

    private TestingStateWithExecutionGraph createStateWithExecutionGraph(
            MockStateWithExecutionGraphContext context) {
        final ExecutionGraph executionGraph = new StateTrackingMockExecutionGraph();
        return createStateWithExecutionGraph(context, executionGraph);
    }

    private TestingStateWithExecutionGraph createStateWithExecutionGraph(
            MockStateWithExecutionGraphContext context,
            OperatorCoordinatorHandler operatorCoordinatorHandler) {
        final ExecutionGraph executionGraph = new StateTrackingMockExecutionGraph();
        return createStateWithExecutionGraph(context, executionGraph, operatorCoordinatorHandler);
    }

    private TestingStateWithExecutionGraph createStateWithExecutionGraph(
            MockStateWithExecutionGraphContext context, ExecutionGraph executionGraph) {
        final OperatorCoordinatorHandler operatorCoordinatorHandler =
                new TestingOperatorCoordinatorHandler();
        return createStateWithExecutionGraph(context, executionGraph, operatorCoordinatorHandler);
    }

    private TestingStateWithExecutionGraph createStateWithExecutionGraph(
            MockStateWithExecutionGraphContext context,
            ExecutionGraph executionGraph,
            OperatorCoordinatorHandler operatorCoordinatorHandler) {

        final ExecutionGraphHandler executionGraphHandler =
                new ExecutionGraphHandler(
                        executionGraph,
                        log,
                        context.getMainThreadExecutor(),
                        context.getMainThreadExecutor());

        executionGraph.transitionToRunning();

        return new TestingStateWithExecutionGraph(
                context, executionGraph, executionGraphHandler, operatorCoordinatorHandler, log);
    }

    private static final class TestingStateWithExecutionGraph extends StateWithExecutionGraph {

        private final CompletableFuture<JobStatus> globallyTerminalStateFuture =
                new CompletableFuture<>();

        TestingStateWithExecutionGraph(
                Context context,
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Logger logger) {
            super(
                    context,
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    logger);
        }

        public CompletableFuture<JobStatus> getGloballyTerminalStateFuture() {
            return globallyTerminalStateFuture;
        }

        @Override
        public void cancel() {}

        @Override
        public JobStatus getJobStatus() {
            return getExecutionGraph().getState();
        }

        @Override
        public void handleGlobalFailure(Throwable cause) {}

        @Override
        boolean updateTaskExecutionState(
                TaskExecutionStateTransition taskExecutionStateTransition) {
            return false;
        }

        @Override
        void onGloballyTerminalState(JobStatus globallyTerminalState) {
            globallyTerminalStateFuture.complete(globallyTerminalState);
        }
    }
}
