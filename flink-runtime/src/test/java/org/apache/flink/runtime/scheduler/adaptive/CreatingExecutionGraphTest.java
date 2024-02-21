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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.GlobalFailureHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.concurrent.Executors;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
class CreatingExecutionGraphTest {

    private static final Logger LOG = LoggerFactory.getLogger(CreatingExecutionGraphTest.class);

    @RegisterExtension
    MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext();

    @Test
    void testFailedExecutionGraphCreationTransitionsToFinished() {
        final CompletableFuture<CreatingExecutionGraph.ExecutionGraphWithVertexParallelism>
                executionGraphWithVertexParallelismFuture = new CompletableFuture<>();
        new CreatingExecutionGraph(
                context,
                executionGraphWithVertexParallelismFuture,
                LOG,
                CreatingExecutionGraphTest::createTestingOperatorCoordinatorHandler,
                null);

        context.setExpectFinished(
                archivedExecutionGraph ->
                        assertThat(archivedExecutionGraph.getState()).isEqualTo(JobStatus.FAILED));

        executionGraphWithVertexParallelismFuture.completeExceptionally(
                new FlinkException("Test exception"));
    }

    @Test
    void testNotPossibleSlotAssignmentTransitionsToWaitingForResources() {
        final CompletableFuture<CreatingExecutionGraph.ExecutionGraphWithVertexParallelism>
                executionGraphWithVertexParallelismFuture = new CompletableFuture<>();
        new CreatingExecutionGraph(
                context,
                executionGraphWithVertexParallelismFuture,
                LOG,
                CreatingExecutionGraphTest::createTestingOperatorCoordinatorHandler,
                null);

        context.setTryToAssignSlotsFunction(
                ignored -> CreatingExecutionGraph.AssignmentResult.notPossible());
        context.setExpectWaitingForResources();

        executionGraphWithVertexParallelismFuture.complete(
                getGraph(new StateTrackingMockExecutionGraph()));
    }

    @Test
    void testSuccessfulSlotAssignmentTransitionsToExecuting() {
        final CompletableFuture<CreatingExecutionGraph.ExecutionGraphWithVertexParallelism>
                executionGraphWithVertexParallelismFuture = new CompletableFuture<>();
        new CreatingExecutionGraph(
                context,
                executionGraphWithVertexParallelismFuture,
                LOG,
                CreatingExecutionGraphTest::createTestingOperatorCoordinatorHandler,
                null);

        final StateTrackingMockExecutionGraph executionGraph =
                new StateTrackingMockExecutionGraph();

        context.setTryToAssignSlotsFunction(CreatingExecutionGraphTest::successfulAssignment);
        context.setExpectedExecuting(
                actualExecutionGraph -> assertThat(actualExecutionGraph).isEqualTo(executionGraph));

        executionGraphWithVertexParallelismFuture.complete(getGraph(executionGraph));
    }

    @Test
    void testOperatorCoordinatorUsesFailureHandlerOfTheCurrentState() {
        final CompletableFuture<CreatingExecutionGraph.ExecutionGraphWithVertexParallelism>
                executionGraphWithVertexParallelismFuture = new CompletableFuture<>();
        final AtomicReference<GlobalFailureHandler> operatorCoordinatorGlobalFailureHandlerRef =
                new AtomicReference<>();
        new CreatingExecutionGraph(
                context,
                executionGraphWithVertexParallelismFuture,
                LOG,
                (executionGraph, errorHandler) -> {
                    operatorCoordinatorGlobalFailureHandlerRef.set(errorHandler);
                    return new TestingOperatorCoordinatorHandler();
                },
                null);

        final StateTrackingMockExecutionGraph executionGraph =
                new StateTrackingMockExecutionGraph();

        context.setTryToAssignSlotsFunction(CreatingExecutionGraphTest::successfulAssignment);
        context.setExpectedExecuting(
                actualExecutionGraph -> assertThat(actualExecutionGraph).isEqualTo(executionGraph));

        executionGraphWithVertexParallelismFuture.complete(getGraph(executionGraph));

        assertThat(operatorCoordinatorGlobalFailureHandlerRef.get()).isSameAs(context);
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

    static class MockCreatingExecutionGraphContext extends MockStateWithoutExecutionGraphContext
            implements CreatingExecutionGraph.Context {
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
        public void goToExecuting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                List<ExceptionHistoryEntry> failureCollection) {
            executingStateValidator.validateInput(executionGraph);
            registerStateTransition();
        }

        @Override
        public ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay) {
            if (!hasStateTransition()) {
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
            registerStateTransition();
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
        public void afterEach(ExtensionContext extensionContext) throws Exception {
            super.afterEach(extensionContext);
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
