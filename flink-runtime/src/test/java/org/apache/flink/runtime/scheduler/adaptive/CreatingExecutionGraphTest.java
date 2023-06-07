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
import org.apache.flink.core.testutils.CompletedScheduledFuture;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.failure.FailureEnricherUtils;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.GlobalFailureHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the {@link CreatingExecutionGraph} state. */
@ExtendWith(TestLoggerExtension.class)
public class CreatingExecutionGraphTest extends TestLogger {

    @Test
    public void testCancelTransitionsToFinished() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CreatingExecutionGraph creatingExecutionGraph =
                    new CreatingExecutionGraph(
                            context,
                            new CompletableFuture<>(),
                            log,
                            CreatingExecutionGraphTest::createTestingOperatorCoordinatorHandler,
                            null);

            context.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState())
                                    .isEqualTo(JobStatus.CANCELED));

            creatingExecutionGraph.cancel();
        }
    }

    @Test
    public void testSuspendTransitionsToFinished() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CreatingExecutionGraph creatingExecutionGraph =
                    new CreatingExecutionGraph(
                            context,
                            new CompletableFuture<>(),
                            log,
                            CreatingExecutionGraphTest::createTestingOperatorCoordinatorHandler,
                            null);

            context.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState())
                                    .isEqualTo(JobStatus.SUSPENDED));

            creatingExecutionGraph.suspend(new FlinkException("Job has been suspended."));
        }
    }

    @Test
    public void testGlobalFailureTransitionsToFinished() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CreatingExecutionGraph creatingExecutionGraph =
                    new CreatingExecutionGraph(
                            context,
                            new CompletableFuture<>(),
                            log,
                            CreatingExecutionGraphTest::createTestingOperatorCoordinatorHandler,
                            null);

            context.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState())
                                    .isEqualTo(JobStatus.FAILED));

            creatingExecutionGraph.handleGlobalFailure(
                    new FlinkException("Test exception"),
                    FailureEnricherUtils.EMPTY_FAILURE_LABELS);
        }
    }

    @Test
    public void testFailedExecutionGraphCreationTransitionsToFinished() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CompletableFuture<CreatingExecutionGraph.ExecutionGraphWithVertexParallelism>
                    executionGraphWithVertexParallelismFuture = new CompletableFuture<>();
            new CreatingExecutionGraph(
                    context,
                    executionGraphWithVertexParallelismFuture,
                    log,
                    CreatingExecutionGraphTest::createTestingOperatorCoordinatorHandler,
                    null);

            context.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState())
                                    .isEqualTo(JobStatus.FAILED));

            executionGraphWithVertexParallelismFuture.completeExceptionally(
                    new FlinkException("Test exception"));
        }
    }

    @Test
    public void testNotPossibleSlotAssignmentTransitionsToWaitingForResources() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CompletableFuture<CreatingExecutionGraph.ExecutionGraphWithVertexParallelism>
                    executionGraphWithVertexParallelismFuture = new CompletableFuture<>();
            new CreatingExecutionGraph(
                    context,
                    executionGraphWithVertexParallelismFuture,
                    log,
                    CreatingExecutionGraphTest::createTestingOperatorCoordinatorHandler,
                    null);

            context.setTryToAssignSlotsFunction(
                    ignored -> CreatingExecutionGraph.AssignmentResult.notPossible());
            context.setExpectWaitingForResources();

            executionGraphWithVertexParallelismFuture.complete(
                    getGraph(new StateTrackingMockExecutionGraph()));
        }
    }

    @Test
    public void testSuccessfulSlotAssignmentTransitionsToExecuting() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CompletableFuture<CreatingExecutionGraph.ExecutionGraphWithVertexParallelism>
                    executionGraphWithVertexParallelismFuture = new CompletableFuture<>();
            new CreatingExecutionGraph(
                    context,
                    executionGraphWithVertexParallelismFuture,
                    log,
                    CreatingExecutionGraphTest::createTestingOperatorCoordinatorHandler,
                    null);

            final StateTrackingMockExecutionGraph executionGraph =
                    new StateTrackingMockExecutionGraph();

            context.setTryToAssignSlotsFunction(CreatingExecutionGraphTest::successfulAssignment);
            context.setExpectedExecuting(
                    actualExecutionGraph ->
                            assertThat(actualExecutionGraph).isEqualTo(executionGraph));

            executionGraphWithVertexParallelismFuture.complete(getGraph(executionGraph));
        }
    }

    @Test
    public void testOperatorCoordinatorUsesFailureHandlerOfTheCurrentState() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CompletableFuture<CreatingExecutionGraph.ExecutionGraphWithVertexParallelism>
                    executionGraphWithVertexParallelismFuture = new CompletableFuture<>();
            final AtomicReference<GlobalFailureHandler> operatorCoordinatorGlobalFailureHandlerRef =
                    new AtomicReference<>();
            new CreatingExecutionGraph(
                    context,
                    executionGraphWithVertexParallelismFuture,
                    log,
                    (executionGraph, errorHandler) -> {
                        operatorCoordinatorGlobalFailureHandlerRef.set(errorHandler);
                        return new TestingOperatorCoordinatorHandler();
                    },
                    null);

            final StateTrackingMockExecutionGraph executionGraph =
                    new StateTrackingMockExecutionGraph();

            context.setTryToAssignSlotsFunction(CreatingExecutionGraphTest::successfulAssignment);
            context.setExpectedExecuting(
                    actualExecutionGraph ->
                            assertThat(actualExecutionGraph).isEqualTo(executionGraph));

            executionGraphWithVertexParallelismFuture.complete(getGraph(executionGraph));

            assertThat(operatorCoordinatorGlobalFailureHandlerRef.get()).isSameAs(context);
        }
    }

    private static CreatingExecutionGraph.AssignmentResult successfulAssignment(
            CreatingExecutionGraph.ExecutionGraphWithVertexParallelism
                    executionGraphWithVertexParallelism) {
        return CreatingExecutionGraph.AssignmentResult.success(
                executionGraphWithVertexParallelism.getExecutionGraph());
    }

    private static OperatorCoordinatorHandler createTestingOperatorCoordinatorHandler(
            ExecutionGraph executionGraph, GlobalFailureHandler globalFailureHandler) {
        return new TestingOperatorCoordinatorHandler();
    }

    static class MockCreatingExecutionGraphContext
            implements CreatingExecutionGraph.Context, AutoCloseable {
        private final StateValidator<ArchivedExecutionGraph> finishedStateValidator =
                new StateValidator<>("Finished");
        private final StateValidator<Void> waitingForResourcesStateValidator =
                new StateValidator<>("WaitingForResources");
        private final StateValidator<ExecutionGraph> executingStateValidator =
                new StateValidator<>("Executing");

        private Function<
                        CreatingExecutionGraph.ExecutionGraphWithVertexParallelism,
                        CreatingExecutionGraph.AssignmentResult>
                tryToAssignSlotsFunction =
                        e -> CreatingExecutionGraph.AssignmentResult.success(e.getExecutionGraph());

        private GlobalFailureHandler globalFailureHandler =
                t -> {
                    // No-op.
                };

        private boolean hadStateTransitionHappened = false;

        public void setExpectFinished(Consumer<ArchivedExecutionGraph> asserter) {
            finishedStateValidator.expectInput(asserter);
        }

        public void setExpectWaitingForResources() {
            waitingForResourcesStateValidator.expectInput((none) -> {});
        }

        public void setExpectedExecuting(Consumer<ExecutionGraph> asserter) {
            executingStateValidator.expectInput(asserter);
        }

        public void setTryToAssignSlotsFunction(
                Function<
                                CreatingExecutionGraph.ExecutionGraphWithVertexParallelism,
                                CreatingExecutionGraph.AssignmentResult>
                        tryToAssignSlotsFunction) {
            this.tryToAssignSlotsFunction = tryToAssignSlotsFunction;
        }

        public void setGlobalFailureHandler(GlobalFailureHandler globalFailureHandler) {
            this.globalFailureHandler = globalFailureHandler;
        }

        @Override
        public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            finishedStateValidator.validateInput(archivedExecutionGraph);
            hadStateTransitionHappened = true;
        }

        @Override
        public void goToExecuting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                List<ExceptionHistoryEntry> failureCollection) {
            executingStateValidator.validateInput(executionGraph);
            hadStateTransitionHappened = true;
        }

        @Override
        public ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause) {
            return ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                    new JobID(), "testJob", jobStatus, cause, null, 0L);
        }

        @Override
        public ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay) {
            if (!hadStateTransitionHappened) {
                action.run();
            }

            return CompletedScheduledFuture.create(null);
        }

        @Override
        public void handleGlobalFailure(Throwable cause) {
            globalFailureHandler.handleGlobalFailure(cause);
        }

        @Override
        public CreatingExecutionGraph.AssignmentResult tryToAssignSlots(
                CreatingExecutionGraph.ExecutionGraphWithVertexParallelism
                        executionGraphWithVertexParallelism) {
            return tryToAssignSlotsFunction.apply(executionGraphWithVertexParallelism);
        }

        @Override
        public void goToWaitingForResources(@Nullable ExecutionGraph previousExecutionGraph) {
            waitingForResourcesStateValidator.validateInput(null);
            hadStateTransitionHappened = true;
        }

        @Override
        public Executor getIOExecutor() {
            return Executors.directExecutor();
        }

        @Override
        public ComponentMainThreadExecutor getMainThreadExecutor() {
            return ComponentMainThreadExecutorServiceAdapter.forMainThread();
        }

        @Override
        public JobManagerJobMetricGroup getMetricGroup() {
            return UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup();
        }

        @Override
        public void close() throws Exception {
            finishedStateValidator.close();
            waitingForResourcesStateValidator.close();
            executingStateValidator.close();
        }
    }

    private static CreatingExecutionGraph.ExecutionGraphWithVertexParallelism getGraph(
            StateTrackingMockExecutionGraph executionGraph) {
        return CreatingExecutionGraph.ExecutionGraphWithVertexParallelism.create(
                executionGraph, JobSchedulingPlan.empty());
    }
}
