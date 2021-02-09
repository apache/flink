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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.NoOpExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.TestingExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionReleaseStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;

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
    public void testExecutionGraphDeploymentOnEnter() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            MockExecutionJobVertex mockExecutionJobVertex = new MockExecutionJobVertex();
            ExecutionGraph executionGraph =
                    new MockExecutionGraph(() -> Collections.singletonList(mockExecutionJobVertex));
            Executing exec =
                    new ExecutingStateBuilder().setExecutionGraph(executionGraph).build(ctx);

            exec.onEnter();
            assertThat(mockExecutionJobVertex.isExecutionDeployed(), is(true));
            assertThat(executionGraph.getState(), is(JobStatus.RUNNING));
        }
    }

    @Test
    public void testDisposalOfOperatorCoordinatorsOnLeaveOfStateWithExecutionGraph()
            throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            MockOperatorCoordinatorHandler operatorCoordinator =
                    new MockOperatorCoordinatorHandler();
            Executing exec =
                    new ExecutingStateBuilder()
                            .setOperatorCoordinatorHandler(operatorCoordinator)
                            .build(ctx);
            exec.onLeave(new MockState());

            assertThat(operatorCoordinator.isDisposed(), is(true));
        }
    }

    @Test
    public void testUnrecoverableGlobalFailureTransitionsToFailingState() throws Exception {
        final String failureMsg = "test exception";
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
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
    public void testRecoverableGlobalFailureTransitionsToRestarting() throws Exception {
        final Duration duration = Duration.ZERO;
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            exec.onEnter();
            ctx.setExpectRestarting(
                    (restartingArguments ->
                            assertThat(restartingArguments.getBackoffTime(), is(duration))));
            ctx.setHowToHandleFailure((t) -> Executing.FailureResult.canRestart(duration));
            exec.handleGlobalFailure(new RuntimeException("Recoverable error"));
        }
    }

    @Test
    public void testCancelTransitionsToCancellingState() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            exec.onEnter();
            ctx.setExpectCancelling(assertNonNull());
            exec.cancel();
        }
    }

    @Test
    public void testTransitionToFinishedOnFailedExecutionGraph() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            exec.onEnter();
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED)));

            // transition EG into terminal state, which will notify the Executing state about the
            // failure (async via the supplied executor)
            exec.getExecutionGraph().failJob(new RuntimeException("test failure"));
        }
    }

    @Test
    public void testTransitionToFinishedOnSuspend() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);

            ctx.setExpectFinished(
                    archivedExecutionGraph -> {
                        assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED));
                    });
            exec.onEnter();
            exec.suspend(new RuntimeException("suspend"));
        }
    }

    @Test
    public void testNotifyNewResourcesAvailableWithCanScaleUpTransitionsToRestarting()
            throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
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
    public void testNotifyNewResourcesAvailableWithNoResourcesAndNoStateChange() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            ctx.setCanScaleUp(() -> false);
            exec.onEnter();
            exec.notifyNewResourcesAvailable();
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testFailureReportedViaUpdateTaskExecutionStateCausesFailingOnNoRestart()
            throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ExecutionGraph returnsFailedStateExecutionGraph = new MockExecutionGraph(true);
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(returnsFailedStateExecutionGraph)
                            .build(ctx);

            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);
            ctx.setExpectFailing(assertNonNull());

            exec.updateTaskExecutionState(
                    createFailingStateTransition(exec.getExecutionGraph().getJobID()));
        }
    }

    @Test
    public void testFailureReportedViaUpdateTaskExecutionStateCausesRestart() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ExecutionGraph returnsFailedStateExecutionGraph = new MockExecutionGraph(true);
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(returnsFailedStateExecutionGraph)
                            .build(ctx);
            ctx.setHowToHandleFailure((ign) -> Executing.FailureResult.canRestart(Duration.ZERO));
            ctx.setExpectRestarting(assertNonNull());

            exec.updateTaskExecutionState(
                    createFailingStateTransition(exec.getExecutionGraph().getJobID()));
        }
    }

    @Test
    public void testFalseReportsViaUpdateTaskExecutionStateAreIgnored() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ExecutionGraph returnsFailedStateExecutionGraph = new MockExecutionGraph(false);
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(returnsFailedStateExecutionGraph)
                            .build(ctx);

            exec.updateTaskExecutionState(
                    createFailingStateTransition(exec.getExecutionGraph().getJobID()));

            ctx.assertNoStateTransition();
        }
    }

    private TaskExecutionStateTransition createFailingStateTransition(JobID jobId) {
        return new TaskExecutionStateTransition(
                new TaskExecutionState(
                        jobId,
                        new ExecutionAttemptID(),
                        ExecutionState.FAILED,
                        new RuntimeException()));
    }

    @Test
    public void testJobInformationMethods() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            final JobID jobId = exec.getExecutionGraph().getJobID();
            exec.onEnter();
            assertThat(exec.getJob(), instanceOf(ArchivedExecutionGraph.class));
            assertThat(exec.getJob().getJobID(), is(jobId));
            assertThat(exec.getJobStatus(), is(JobStatus.RUNNING));
        }
    }

    private final class ExecutingStateBuilder {
        private ExecutionGraph executionGraph = TestingExecutionGraphBuilder.newBuilder().build();
        private OperatorCoordinatorHandler operatorCoordinatorHandler;

        private ExecutingStateBuilder() throws JobException, JobExecutionException {
            operatorCoordinatorHandler =
                    new OperatorCoordinatorHandler(
                            executionGraph,
                            (throwable) -> {
                                throw new RuntimeException("Error in test", throwable);
                            });
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

        private Executing build(MockExecutingContext ctx) {
            executionGraph.transitionToRunning();
            final ExecutionGraphHandler executionGraphHandler =
                    new ExecutionGraphHandler(
                            executionGraph,
                            log,
                            ctx.getMainThreadExecutor(),
                            ctx.getMainThreadExecutor());
            return new Executing(
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    log,
                    ctx,
                    ClassLoader.getSystemClassLoader());
        }
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
            hadStateTransition = true;
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
            hadStateTransition = true;
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
            hadStateTransition = true;
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

    private static class MockExecutionGraph extends ExecutionGraph {
        private final boolean updateStateReturnValue;
        private final Supplier<Iterable<ExecutionJobVertex>> getVerticesTopologicallySupplier;

        MockExecutionGraph(Supplier<Iterable<ExecutionJobVertex>> getVerticesTopologicallySupplier)
                throws IOException {
            this(false, getVerticesTopologicallySupplier);
        }

        MockExecutionGraph(boolean updateStateReturnValue) throws IOException {
            this(updateStateReturnValue, null);
        }

        private MockExecutionGraph(
                boolean updateStateReturnValue,
                Supplier<Iterable<ExecutionJobVertex>> getVerticesTopologicallySupplier)
                throws IOException {
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

    private static class MockState implements State {
        @Override
        public void cancel() {}

        @Override
        public void suspend(Throwable cause) {}

        @Override
        public JobStatus getJobStatus() {
            return null;
        }

        @Override
        public ArchivedExecutionGraph getJob() {
            return null;
        }

        @Override
        public void handleGlobalFailure(Throwable cause) {}

        @Override
        public Logger getLogger() {
            return null;
        }
    }

    private static class MockOperatorCoordinatorHandler extends OperatorCoordinatorHandler {

        private boolean disposed = false;

        public MockOperatorCoordinatorHandler() throws JobException, JobExecutionException {
            super(
                    TestingExecutionGraphBuilder.newBuilder().build(),
                    (throwable) -> {
                        throw new RuntimeException("Error in test", throwable);
                    });
        }

        @Override
        public void disposeAllOperatorCoordinators() {
            disposed = true;
        }

        public boolean isDisposed() {
            return disposed;
        }
    }

    private static class MockExecutionJobVertex extends ExecutionJobVertex {
        private final MockExecutionVertex mockExecutionVertex;

        MockExecutionJobVertex() throws JobException, JobExecutionException {
            super(
                    TestingExecutionGraphBuilder.newBuilder().build(),
                    new JobVertex("test"),
                    1,
                    1,
                    Time.milliseconds(1L),
                    1L);
            mockExecutionVertex = new MockExecutionVertex(this);
        }

        @Override
        public ExecutionVertex[] getTaskVertices() {
            return new ExecutionVertex[] {mockExecutionVertex};
        }

        public boolean isExecutionDeployed() {
            return mockExecutionVertex.isDeployed();
        }
    }

    private static class MockExecutionVertex extends ExecutionVertex {
        private boolean deployed = false;

        MockExecutionVertex(ExecutionJobVertex jobVertex) {
            super(jobVertex, 1, new IntermediateResult[] {}, Time.milliseconds(1L), 1L, 1);
        }

        @Override
        public void deploy() throws JobException {
            deployed = true;
        }

        public boolean isDeployed() {
            return deployed;
        }
    }
}
