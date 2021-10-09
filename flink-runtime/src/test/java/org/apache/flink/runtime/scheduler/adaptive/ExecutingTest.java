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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.CompletedScheduledFuture;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinator;
import org.apache.flink.runtime.checkpoint.CheckpointCoordinatorTestingUtils;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.DefaultSubtaskAttemptNumberStore;
import org.apache.flink.runtime.executiongraph.EdgeManager;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.IntermediateResult;
import org.apache.flink.runtime.executiongraph.IntermediateResultPartition;
import org.apache.flink.runtime.executiongraph.InternalExecutionGraphAccessor;
import org.apache.flink.runtime.executiongraph.JobInformation;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionGroupReleaseStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultVertexParallelismInfo;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.types.Either;
import org.apache.flink.util.SerializedValue;
import org.apache.flink.util.TestLogger;

import org.junit.Test;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/** Tests for {@link AdaptiveScheduler AdaptiveScheduler's} {@link Executing} state. */
public class ExecutingTest extends TestLogger {

    @Test
    public void testExecutionGraphDeploymentOnEnter() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            MockExecutionJobVertex mockExecutionJobVertex =
                    new MockExecutionJobVertex(MockExecutionVertex::new);
            MockExecutionVertex mockExecutionVertex =
                    (MockExecutionVertex) mockExecutionJobVertex.getMockExecutionVertex();
            mockExecutionVertex.setMockedExecutionState(ExecutionState.CREATED);
            ExecutionGraph executionGraph =
                    new MockExecutionGraph(() -> Collections.singletonList(mockExecutionJobVertex));
            Executing exec =
                    new ExecutingStateBuilder().setExecutionGraph(executionGraph).build(ctx);

            assertThat(mockExecutionVertex.isDeployCalled(), is(true));
            assertThat(executionGraph.getState(), is(JobStatus.RUNNING));
        }
    }

    @Test
    public void testNoDeploymentCallOnEnterWhenVertexRunning() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            MockExecutionJobVertex mockExecutionJobVertex =
                    new MockExecutionJobVertex(MockExecutionVertex::new);
            ExecutionGraph executionGraph =
                    new MockExecutionGraph(() -> Collections.singletonList(mockExecutionJobVertex));
            executionGraph.transitionToRunning();
            final MockExecutionVertex mockExecutionVertex =
                    ((MockExecutionVertex) mockExecutionJobVertex.getMockExecutionVertex());
            mockExecutionVertex.setMockedExecutionState(ExecutionState.RUNNING);

            new Executing(
                    executionGraph,
                    getExecutionGraphHandler(executionGraph, ctx.getMainThreadExecutor()),
                    new TestingOperatorCoordinatorHandler(),
                    log,
                    ctx,
                    ClassLoader.getSystemClassLoader());
            assertThat(mockExecutionVertex.isDeployCalled(), is(false));
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testIllegalStateExceptionOnNotRunningExecutionGraph() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ExecutionGraph notRunningExecutionGraph = new StateTrackingMockExecutionGraph();
            assertThat(notRunningExecutionGraph.getState(), is(not(JobStatus.RUNNING)));

            new Executing(
                    notRunningExecutionGraph,
                    getExecutionGraphHandler(notRunningExecutionGraph, ctx.getMainThreadExecutor()),
                    new TestingOperatorCoordinatorHandler(),
                    log,
                    ctx,
                    ClassLoader.getSystemClassLoader());
        }
    }

    @Test
    public void testDisposalOfOperatorCoordinatorsOnLeaveOfStateWithExecutionGraph()
            throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            TestingOperatorCoordinatorHandler operatorCoordinator =
                    new TestingOperatorCoordinatorHandler();
            Executing exec =
                    new ExecutingStateBuilder()
                            .setOperatorCoordinatorHandler(operatorCoordinator)
                            .build(ctx);
            exec.onLeave(MockState.class);

            assertThat(operatorCoordinator.isDisposed(), is(true));
        }
    }

    @Test
    public void testUnrecoverableGlobalFailureTransitionsToFailingState() throws Exception {
        final String failureMsg = "test exception";
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
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
            ctx.setExpectRestarting(
                    (restartingArguments ->
                            assertThat(restartingArguments.getBackoffTime(), is(duration))));
            ctx.setHowToHandleFailure((t) -> Executing.FailureResult.canRestart(t, duration));
            exec.handleGlobalFailure(new RuntimeException("Recoverable error"));
        }
    }

    @Test
    public void testCancelTransitionsToCancellingState() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            ctx.setExpectCancelling(assertNonNull());
            exec.cancel();
        }
    }

    @Test
    public void testTransitionToFinishedOnFailedExecutionGraph() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            ctx.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED)));

            // transition EG into terminal state, which will notify the Executing state about the
            // failure (async via the supplied executor)
            exec.getExecutionGraph()
                    .failJob(new RuntimeException("test failure"), System.currentTimeMillis());
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
            exec.suspend(new RuntimeException("suspend"));
        }
    }

    @Test
    public void testNotifyNewResourcesAvailableWithCanScaleUpTransitionsToRestarting()
            throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);

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
            exec.notifyNewResourcesAvailable();
            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testFailureReportedViaUpdateTaskExecutionStateCausesFailingOnNoRestart()
            throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ExecutionGraph returnsFailedStateExecutionGraph =
                    new MockExecutionGraph(true, Collections::emptyList);
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(returnsFailedStateExecutionGraph)
                            .build(ctx);

            ctx.setHowToHandleFailure(Executing.FailureResult::canNotRestart);
            ctx.setExpectFailing(assertNonNull());

            exec.updateTaskExecutionState(createFailingStateTransition());
        }
    }

    @Test
    public void testFailureReportedViaUpdateTaskExecutionStateCausesRestart() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            ExecutionGraph returnsFailedStateExecutionGraph =
                    new MockExecutionGraph(true, Collections::emptyList);
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(returnsFailedStateExecutionGraph)
                            .build(ctx);
            ctx.setHowToHandleFailure(
                    (throwable) -> Executing.FailureResult.canRestart(throwable, Duration.ZERO));
            ctx.setExpectRestarting(assertNonNull());

            exec.updateTaskExecutionState(createFailingStateTransition());
        }
    }

    @Test
    public void testFalseReportsViaUpdateTaskExecutionStateAreIgnored() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            MockExecutionGraph returnsFailedStateExecutionGraph =
                    new MockExecutionGraph(false, Collections::emptyList);
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(returnsFailedStateExecutionGraph)
                            .build(ctx);

            exec.updateTaskExecutionState(createFailingStateTransition());

            ctx.assertNoStateTransition();
        }
    }

    @Test
    public void testExecutionVertexMarkedAsFailedOnDeploymentFailure() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            MockExecutionJobVertex mejv =
                    new MockExecutionJobVertex(FailOnDeployMockExecutionVertex::new);
            ExecutionGraph executionGraph =
                    new MockExecutionGraph(() -> Collections.singletonList(mejv));
            Executing exec =
                    new ExecutingStateBuilder().setExecutionGraph(executionGraph).build(ctx);

            assertThat(
                    ((FailOnDeployMockExecutionVertex) mejv.getMockExecutionVertex())
                            .getMarkedFailure(),
                    is(instanceOf(JobException.class)));
        }
    }

    @Test
    public void testTransitionToStopWithSavepointState() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            CheckpointCoordinator coordinator =
                    new CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder().build();
            StateTrackingMockExecutionGraph mockedExecutionGraphWithCheckpointCoordinator =
                    new StateTrackingMockExecutionGraph() {
                        @Nullable
                        @Override
                        public CheckpointCoordinator getCheckpointCoordinator() {
                            return coordinator;
                        }
                    };
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(mockedExecutionGraphWithCheckpointCoordinator)
                            .build(ctx);

            ctx.setExpectStopWithSavepoint(assertNonNull());
            exec.stopWithSavepoint("file:///tmp/target", true);
        }
    }

    @Test
    public void testCheckpointSchedulerIsStoppedOnStopWithSavepoint() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            CheckpointCoordinator coordinator =
                    new CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder().build();
            StateTrackingMockExecutionGraph mockedExecutionGraphWithCheckpointCoordinator =
                    new StateTrackingMockExecutionGraph() {
                        @Nullable
                        @Override
                        public CheckpointCoordinator getCheckpointCoordinator() {
                            return coordinator;
                        }
                    };
            Executing exec =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(mockedExecutionGraphWithCheckpointCoordinator)
                            .build(ctx);

            coordinator.startCheckpointScheduler();

            // we assume checkpointing to be enabled
            assertThat(coordinator.isPeriodicCheckpointingStarted(), is(true));

            ctx.setExpectStopWithSavepoint(assertNonNull());
            exec.stopWithSavepoint("file:///tmp/target", true);

            assertThat(coordinator.isPeriodicCheckpointingStarted(), is(false));
        }
    }

    static TaskExecutionStateTransition createFailingStateTransition() {
        return new TaskExecutionStateTransition(
                new TaskExecutionState(
                        new ExecutionAttemptID(), ExecutionState.FAILED, new RuntimeException()));
    }

    @Test
    public void testJobInformationMethods() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            Executing exec = new ExecutingStateBuilder().build(ctx);
            final JobID jobId = exec.getExecutionGraph().getJobID();
            assertThat(exec.getJob(), instanceOf(ArchivedExecutionGraph.class));
            assertThat(exec.getJob().getJobID(), is(jobId));
            assertThat(exec.getJobStatus(), is(JobStatus.RUNNING));
        }
    }

    @Test
    public void testStateDoesNotExposeGloballyTerminalExecutionGraph() throws Exception {
        try (MockExecutingContext ctx = new MockExecutingContext()) {
            final FinishingMockExecutionGraph finishingMockExecutionGraph =
                    new FinishingMockExecutionGraph();
            Executing executing =
                    new ExecutingStateBuilder()
                            .setExecutionGraph(finishingMockExecutionGraph)
                            .build(ctx);

            // ideally we'd delay the async call to #onGloballyTerminalState instead, but the
            // context does not support that
            ctx.setExpectFinished(eg -> {});

            finishingMockExecutionGraph.completeTerminationFuture(JobStatus.FINISHED);

            // this is just a sanity check for the test
            assertThat(executing.getExecutionGraph().getState(), is(JobStatus.FINISHED));

            assertThat(executing.getJobStatus(), is(JobStatus.RUNNING));
            assertThat(executing.getJob().getState(), is(JobStatus.RUNNING));
            assertThat(executing.getJob().getStatusTimestamp(JobStatus.FINISHED), is(0L));
        }
    }

    @Test
    public void testExecutingChecksForNewResourcesWhenBeingCreated() throws Exception {
        try (MockExecutingContext context = new MockExecutingContext()) {
            context.setCanScaleUp(() -> true);
            context.setExpectRestarting(
                    restartingArguments -> {
                        // expect immediate restart on scale up
                        assertThat(restartingArguments.getBackoffTime(), is(Duration.ZERO));
                    });

            final Executing executing = new ExecutingStateBuilder().build(context);
        }
    }

    private final class ExecutingStateBuilder {
        private ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder().build();
        private OperatorCoordinatorHandler operatorCoordinatorHandler;

        private ExecutingStateBuilder() throws JobException, JobExecutionException {
            operatorCoordinatorHandler = new TestingOperatorCoordinatorHandler();
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

            return new Executing(
                    executionGraph,
                    getExecutionGraphHandler(executionGraph, ctx.getMainThreadExecutor()),
                    operatorCoordinatorHandler,
                    log,
                    ctx,
                    ClassLoader.getSystemClassLoader());
        }
    }

    private ExecutionGraphHandler getExecutionGraphHandler(
            ExecutionGraph executionGraph, ComponentMainThreadExecutor mainThreadExecutor) {
        return new ExecutionGraphHandler(
                executionGraph, log, mainThreadExecutor, mainThreadExecutor);
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
        private Supplier<Boolean> canScaleUp = () -> false;
        private StateValidator<StopWithSavepointArguments> stopWithSavepointValidator =
                new StateValidator<>("stopWithSavepoint");
        private CompletableFuture<String> mockedStopWithSavepointOperationFuture =
                new CompletableFuture<>();

        public void setExpectFailing(Consumer<FailingArguments> asserter) {
            failingStateValidator.expectInput(asserter);
        }

        public void setExpectRestarting(Consumer<RestartingArguments> asserter) {
            restartingStateValidator.expectInput(asserter);
        }

        public void setExpectCancelling(Consumer<CancellingArguments> asserter) {
            cancellingStateValidator.expectInput(asserter);
        }

        public void setExpectStopWithSavepoint(Consumer<StopWithSavepointArguments> asserter) {
            stopWithSavepointValidator.expectInput(asserter);
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
        public CompletableFuture<String> goToStopWithSavepoint(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                CheckpointScheduling checkpointScheduling,
                CompletableFuture<String> savepointFuture) {
            stopWithSavepointValidator.validateInput(
                    new StopWithSavepointArguments(
                            executionGraph,
                            executionGraphHandler,
                            operatorCoordinatorHandler,
                            checkpointScheduling,
                            savepointFuture));
            hadStateTransition = true;
            return mockedStopWithSavepointOperationFuture;
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
            failingStateValidator.close();
            restartingStateValidator.close();
            cancellingStateValidator.close();
            stopWithSavepointValidator.close();
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

    static class StopWithSavepointArguments extends CancellingArguments {
        private final CheckpointScheduling checkpointScheduling;
        private final CompletableFuture<String> savepointFuture;

        public StopWithSavepointArguments(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandle,
                CheckpointScheduling checkpointScheduling,
                CompletableFuture<String> savepointFuture) {
            super(executionGraph, executionGraphHandler, operatorCoordinatorHandle);
            this.checkpointScheduling = checkpointScheduling;
            this.savepointFuture = savepointFuture;
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

    static class MockExecutionGraph extends StateTrackingMockExecutionGraph {
        private final boolean updateStateReturnValue;
        private final Supplier<Iterable<ExecutionJobVertex>> getVerticesTopologicallySupplier;

        MockExecutionGraph(
                Supplier<Iterable<ExecutionJobVertex>> getVerticesTopologicallySupplier) {
            this(false, getVerticesTopologicallySupplier);
        }

        private MockExecutionGraph(
                boolean updateStateReturnValue,
                Supplier<Iterable<ExecutionJobVertex>> getVerticesTopologicallySupplier) {
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

    private static class FinishingMockExecutionGraph extends StateTrackingMockExecutionGraph {
        @Override
        public long getStatusTimestamp(JobStatus status) {
            switch (status) {
                case INITIALIZING:
                    return 1;
                case CREATED:
                    return 2;
                case RUNNING:
                    return 3;
                case FINISHED:
                    return 4;
            }
            return 0;
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

    static class MockExecutionJobVertex extends ExecutionJobVertex {
        private final ExecutionVertex mockExecutionVertex;

        MockExecutionJobVertex(
                Function<ExecutionJobVertex, ExecutionVertex> executionVertexSupplier)
                throws JobException {
            super(
                    new MockInternalExecutionGraphAccessor(),
                    new JobVertex("test"),
                    1,
                    Time.milliseconds(1L),
                    1L,
                    new DefaultVertexParallelismInfo(1, 1, max -> Optional.empty()),
                    new DefaultSubtaskAttemptNumberStore(Collections.emptyList()));
            mockExecutionVertex = executionVertexSupplier.apply(this);
        }

        @Override
        public ExecutionVertex[] getTaskVertices() {
            return new ExecutionVertex[] {mockExecutionVertex};
        }

        public ExecutionVertex getMockExecutionVertex() {
            return mockExecutionVertex;
        }
    }

    static class FailOnDeployMockExecutionVertex extends ExecutionVertex {

        @Nullable private Throwable markFailed = null;

        public FailOnDeployMockExecutionVertex(ExecutionJobVertex jobVertex) {
            super(jobVertex, 1, new IntermediateResult[] {}, Time.milliseconds(1L), 1L, 1, 0);
        }

        @Override
        public void deploy() throws JobException {
            throw new JobException("Intentional Test exception");
        }

        @Override
        public void markFailed(Throwable t) {
            markFailed = t;
        }

        @Nullable
        public Throwable getMarkedFailure() {
            return markFailed;
        }
    }

    static class MockExecutionVertex extends ExecutionVertex {
        private boolean deployCalled = false;
        private ExecutionState mockedExecutionState = ExecutionState.RUNNING;

        MockExecutionVertex(ExecutionJobVertex jobVertex) {
            super(jobVertex, 1, new IntermediateResult[] {}, Time.milliseconds(1L), 1L, 1, 0);
        }

        @Override
        public void deploy() throws JobException {
            deployCalled = true;
        }

        public boolean isDeployCalled() {
            return deployCalled;
        }

        @Override
        public ExecutionState getExecutionState() {
            return mockedExecutionState;
        }

        public void setMockedExecutionState(ExecutionState mockedExecutionState) {
            this.mockedExecutionState = mockedExecutionState;
        }
    }

    private static class MockInternalExecutionGraphAccessor
            implements InternalExecutionGraphAccessor {

        @Override
        public Executor getFutureExecutor() {
            return ForkJoinPool.commonPool();
        }

        // --- mocked methods

        @Override
        public ClassLoader getUserClassLoader() {
            return null;
        }

        @Override
        public JobID getJobID() {
            return null;
        }

        @Override
        public BlobWriter getBlobWriter() {
            return null;
        }

        @Override
        public Either<SerializedValue<JobInformation>, PermanentBlobKey>
                getJobInformationOrBlobKey() {
            return null;
        }

        @Override
        public TaskDeploymentDescriptorFactory.PartitionLocationConstraint
                getPartitionLocationConstraint() {
            return null;
        }

        @Nonnull
        @Override
        public ComponentMainThreadExecutor getJobMasterMainThreadExecutor() {
            return null;
        }

        @Override
        public ShuffleMaster<?> getShuffleMaster() {
            return null;
        }

        @Override
        public JobMasterPartitionTracker getPartitionTracker() {
            return null;
        }

        @Override
        public void registerExecution(Execution exec) {}

        @Override
        public void deregisterExecution(Execution exec) {}

        @Override
        public PartitionGroupReleaseStrategy getPartitionGroupReleaseStrategy() {
            return null;
        }

        @Override
        public void failGlobal(Throwable t) {}

        @Override
        public void notifySchedulerNgAboutInternalTaskFailure(
                ExecutionAttemptID attemptId,
                Throwable t,
                boolean cancelTask,
                boolean releasePartitions) {}

        @Override
        public void vertexFinished() {}

        @Override
        public void vertexUnFinished() {}

        @Override
        public ExecutionDeploymentListener getExecutionDeploymentListener() {
            return null;
        }

        @Override
        public void notifyExecutionChange(Execution execution, ExecutionState newExecutionState) {}

        @Override
        public EdgeManager getEdgeManager() {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }

        @Override
        public ExecutionVertex getExecutionVertexOrThrow(ExecutionVertexID id) {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }

        @Override
        public IntermediateResultPartition getResultPartitionOrThrow(
                IntermediateResultPartitionID id) {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }

        @Override
        public void deleteBlobs(List<PermanentBlobKey> blobKeys) {
            throw new UnsupportedOperationException(
                    "This method is not supported by the MockInternalExecutionGraphAccessor.");
        }
    }
}
