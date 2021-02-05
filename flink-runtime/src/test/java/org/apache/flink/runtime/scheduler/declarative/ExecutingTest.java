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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertexDeploymentTest;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.NoOpExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.slots.TaskManagerGateway;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.runtime.scheduler.declarative.WaitingForResourcesTest.assertNonNull;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.junit.Assert.assertThat;

/** Tests for declarative scheduler's {@link Executing} state. */
public class ExecutingTest extends TestLogger {
    @Test
    public void testTransitionToFailing() throws Exception {
        final String failureMsg = "test exception";
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = getExecutingState(ctx);
            exec.onEnter();
            ctx.setExpectFailing(
                    (failingArguments -> {
                        assertThat(failingArguments.getExecutionGraph(), notNullValue());
                        assertThat(failingArguments.getFailureCause().getMessage(), is(failureMsg));
                    }));
            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);
            exec.handleGlobalFailure(new RuntimeException(failureMsg));
        }
    }

    @Test
    public void testTransitionToRestarting() throws Exception {
        final Duration duration = Duration.ZERO;
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = getExecutingState(ctx);
            exec.onEnter();
            ctx.setExpectRestarting(
                    (restartingArguments ->
                            assertThat(restartingArguments.getBackoffTime(), is(duration))));
            ctx.setHowToHandleFailure((t) -> Executing.FailureResult.canRestart(duration));
            exec.handleGlobalFailure(new RuntimeException("Recoverable error"));
        }
    }

    @Test
    public void testTransitionToCancelling() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = getExecutingState(ctx);
            exec.onEnter();
            ctx.setExpectCancelling(assertNonNull());
            exec.cancel();
        }
    }

    @Test
    public void testTransitionToFinishedOnTerminalState() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = getExecutingState(ctx);
            exec.onEnter();
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED)));

            ctx.setExpectedStateChecker((state) -> state == exec);
            // transition EG into terminal state, which will notify the Executing state about the
            // failure (async via the supplied executor)
            exec.getExecutionGraph().failJob(new RuntimeException("test failure"));
        }
    }

    @Test
    public void testTransitionToFinishedOnSuspend() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = getExecutingState(ctx);
            ctx.setExpectedStateChecker((state) -> state == exec);

            ctx.setExpectFinished(
                    archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED));
                    });
            exec.onEnter();
            exec.suspend(new RuntimeException("suspend"));
        }
    }

    @Test
    public void testScaleUp() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = getExecutingState(ctx);
            exec.onEnter();

            ctx.setExpectRestarting(
                    restartingArguments -> {
                        // expect immediate restart on scale up
                        assertThat(restartingArguments.getBackoffTime(), is(Duration.ZERO));
                    });
            ctx.setCanScaleUp(() -> true);
            exec.notifyNewResourcesAvailable();
        }
    }

    @Test
    public void testNoScaleUp() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = getExecutingState(ctx);
            ctx.setCanScaleUp(() -> false);
            exec.onEnter();
            exec.notifyNewResourcesAvailable();
        }
    }

    @Test
    public void testFailingOnDeploymentFailure() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ctx.setCanScaleUp(() -> false);
            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);
            ctx.setExpectFailing(assertNonNull());

            // create ExecutionGraph with one ExecutionVertex, which fails during deployment.
            JobGraph jobGraph = new JobGraph(new JobVertex("test"));
            Executing exec = getExecutingState(ctx, null, jobGraph);
            TestingLogicalSlotBuilder slotBuilder = new TestingLogicalSlotBuilder();
            TaskManagerGateway taskManagerGateway =
                    new ExecutionVertexDeploymentTest.SubmitFailingSimpleAckingTaskManagerGateway();
            slotBuilder.setTaskManagerGateway(taskManagerGateway);
            LogicalSlot slot = slotBuilder.createTestingLogicalSlot();
            exec.getExecutionGraph()
                    .getAllExecutionVertices()
                    .forEach(executionVertex -> executionVertex.tryAssignResource(slot));

            // trigger deployment
            exec.onEnter();
        }
    }

    @Test
    public void testFailureReportedViaUpdateTaskExecutionState() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ExecutionGraph returnsFailedStateExecutionGraph =
                    new ReturnsTrueOnUpdateExecutionGraph();
            Executing exec = getExecutingState(ctx, returnsFailedStateExecutionGraph, null);
            exec.onEnter();
            ctx.setCanScaleUp(() -> false);
            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);
            ctx.setExpectFailing(assertNonNull());

            TaskExecutionStateTransition stateTransition =
                    new TaskExecutionStateTransition(
                            new TaskExecutionState(
                                    exec.getExecutionGraph().getJobID(),
                                    new ExecutionAttemptID(),
                                    ExecutionState.FAILED,
                                    new RuntimeException()));

            exec.updateTaskExecutionState(stateTransition);
        }
    }

    @Test
    public void testJobInformationMethods() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = getExecutingState(ctx);
            final JobID jobId = exec.getExecutionGraph().getJobID();
            exec.onEnter();
            assertThat(exec.getJob(), instanceOf(ArchivedExecutionGraph.class));
            assertThat(exec.getJob().getJobID(), is(jobId));
            assertThat(exec.getJobStatus(), is(JobStatus.CREATED));
        }
    }

    public Executing getExecutingState(MockExecutingContext ctx)
            throws JobException, JobExecutionException {
        return getExecutingState(ctx, null, null);
    }

    public Executing getExecutingState(
            MockExecutingContext ctx,
            @Nullable ExecutionGraph alternativeEG,
            @Nullable JobGraph jobGraph)
            throws JobException, JobExecutionException {
        ExecutionGraph executionGraph = alternativeEG;
        if (alternativeEG == null) {
            executionGraph = TestingExecutionGraphBuilder.newBuilder().build();
        }
        if (jobGraph != null) {
            executionGraph.attachJobGraph(jobGraph.getVerticesSortedTopologicallyFromSources());
        }
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
        return new Executing(
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                log,
                ctx,
                ClassLoader.getSystemClassLoader());
    }

    private static class MockExecutingContext extends MockStateWithExecutionGraphContext
            implements Executing.Context {

        private final StateValidator<FailingArguments> failingStateValidator =
                new StateValidator<>("failing");
        private final StateValidator<RestartingArguments> restartingStateValidator =
                new StateValidator<>("restarting");
        private final StateValidator<CancellingArguments> cancellingStateValidator =
                new StateValidator<>("cancelling");

        private Function<Throwable, Executing.FailureResult> howToHandleFailure;
        private Supplier<Boolean> canScaleUp;

        public void setExpectFailing(Consumer<FailingArguments> asserter) {
            failingStateValidator.expectInput(asserter);
        }

        public void setExpectRestarting(Consumer<RestartingArguments> asserter) {
            restartingStateValidator.expectInput(asserter);
        }

        public void setExpectCancelling(Consumer<CancellingArguments> asserter) {
            cancellingStateValidator.expectInput(asserter);
        }

        public void setHowToHandleFailure(Function<Throwable, Executing.FailureResult> function) {
            this.howToHandleFailure = function;
        }

        public void setCanScaleUp(Supplier<Boolean> supplier) {
            this.canScaleUp = supplier;
        }

        // --------- Interface Implementations ------- //

        @Override
        public void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler) {
            cancellingStateValidator.validateInput(
                    new CancellingArguments(
                            executionGraph, executionGraphHandler, operatorCoordinatorHandler));
        }

        @Override
        public Executing.FailureResult howToHandleFailure(Throwable failure) {
            return howToHandleFailure.apply(failure);
        }

        @Override
        public boolean canScaleUp(ExecutionGraph executionGraph) {
            return canScaleUp.get();
        }

        @Override
        public void goToRestarting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime) {
            restartingStateValidator.validateInput(
                    new RestartingArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            backoffTime));
        }

        @Override
        public void goToFailing(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause) {
            failingStateValidator.validateInput(
                    new FailingArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            failureCause));
        }

        @Override
        public void close() throws Exception {
            super.close();
            failingStateValidator.close();
            restartingStateValidator.close();
            cancellingStateValidator.close();
        }
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

    static class RestartingArguments extends CancellingArguments {
        private final Duration backoffTime;

        public RestartingArguments(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime) {
            super(executionGraph, executionGraphHandler, operatorCoordinatorHandler);
            this.backoffTime = backoffTime;
        }

        public Duration getBackoffTime() {
            return backoffTime;
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

    private static class ReturnsTrueOnUpdateExecutionGraph extends ExecutionGraph {
        ReturnsTrueOnUpdateExecutionGraph() throws IOException {
            super(
                    new JobInformation(
                            new JobID(),
                            "Test Job",
                            new SerializedValue<>(new ExecutionConfig()),
                            new Configuration(),
                            Collections.emptyList(),
                            Collections.emptyList()),
                    TestingUtils.defaultExecutor(),
                    TestingUtils.defaultExecutor(),
                    AkkaUtils.getDefaultTimeout(),
                    1,
                    ExecutionGraph.class.getClassLoader(),
                    VoidBlobWriter.getInstance(),
                    PartitionReleaseStrategyFactoryLoader.loadPartitionReleaseStrategyFactory(
                            new Configuration()),
                    NettyShuffleMaster.INSTANCE,
                    NoOpJobMasterPartitionTracker.INSTANCE,
                    ScheduleMode.EAGER,
                    NoOpExecutionDeploymentListener.get(),
                    (execution, newState) -> {},
                    0L);
        }

        @Override
        public boolean updateState(TaskExecutionStateTransition state) {
            return true;
        }
    }
}
