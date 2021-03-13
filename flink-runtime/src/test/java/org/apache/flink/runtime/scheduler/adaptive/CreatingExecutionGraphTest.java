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
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;

/** Tests for the {@link CreatingExecutionGraph} state. */
public class CreatingExecutionGraphTest extends TestLogger {

    @Test
    public void testCancelTransitionsToFinished() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CreatingExecutionGraph creatingExecutionGraph =
                    new CreatingExecutionGraph(context, new CompletableFuture<>(), log);

            context.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.CANCELED)));

            creatingExecutionGraph.cancel();
        }
    }

    @Test
    public void testSuspendTransitionsToFinished() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CreatingExecutionGraph creatingExecutionGraph =
                    new CreatingExecutionGraph(context, new CompletableFuture<>(), log);

            context.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.SUSPENDED)));

            creatingExecutionGraph.suspend(new FlinkException("Job has been suspended."));
        }
    }

    @Test
    public void testGlobalFailureTransitionsToFinished() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CreatingExecutionGraph creatingExecutionGraph =
                    new CreatingExecutionGraph(context, new CompletableFuture<>(), log);

            context.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED)));

            creatingExecutionGraph.handleGlobalFailure(new FlinkException("Test exception"));
        }
    }

    @Test
    public void testFailedExecutionGraphCreationTransitionsToFinished() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CompletableFuture<ExecutionGraph> executionGraphFuture =
                    new CompletableFuture<>();
            final CreatingExecutionGraph creatingExecutionGraph =
                    new CreatingExecutionGraph(context, executionGraphFuture, log);

            context.setExpectFinished(
                    archivedExecutionGraph ->
                            assertThat(archivedExecutionGraph.getState(), is(JobStatus.FAILED)));

            executionGraphFuture.completeExceptionally(new FlinkException("Test exception"));
        }
    }

    @Test
    public void testNotPossibleSlotAssignmentTransitionsToWaitingForResources() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CompletableFuture<ExecutionGraph> executionGraphFuture =
                    new CompletableFuture<>();
            final CreatingExecutionGraph creatingExecutionGraph =
                    new CreatingExecutionGraph(context, executionGraphFuture, log);

            context.setTryToAssignSlotsFunction(
                    ignored -> CreatingExecutionGraph.AssignmentResult.notPossible());
            context.setExpectWaitingForResources();

            executionGraphFuture.complete(new StateTrackingMockExecutionGraph());
        }
    }

    @Test
    public void testSuccessfulSlotAssignmentTransitionsToExecuting() throws Exception {
        try (MockCreatingExecutionGraphContext context = new MockCreatingExecutionGraphContext()) {
            final CompletableFuture<ExecutionGraph> executionGraphFuture =
                    new CompletableFuture<>();
            final CreatingExecutionGraph creatingExecutionGraph =
                    new CreatingExecutionGraph(context, executionGraphFuture, log);

            final StateTrackingMockExecutionGraph executionGraph =
                    new StateTrackingMockExecutionGraph();

            context.setTryToAssignSlotsFunction(CreatingExecutionGraph.AssignmentResult::success);
            context.setExpectedExecuting(
                    actualExecutionGraph ->
                            assertThat(actualExecutionGraph, sameInstance(executionGraph)));

            executionGraphFuture.complete(executionGraph);
        }
    }

    static class MockCreatingExecutionGraphContext
            implements CreatingExecutionGraph.Context, AutoCloseable {
        private final StateValidator<ArchivedExecutionGraph> finishedStateValidator =
                new StateValidator<>("Finished");
        private final StateValidator<Void> waitingForResourcesStateValidator =
                new StateValidator<>("WaitingForResources");
        private final StateValidator<ExecutionGraph> executingStateValidator =
                new StateValidator<>("Executing");

        private Function<ExecutionGraph, CreatingExecutionGraph.AssignmentResult>
                tryToAssignSlotsFunction = CreatingExecutionGraph.AssignmentResult::success;

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
                Function<ExecutionGraph, CreatingExecutionGraph.AssignmentResult>
                        tryToAssignSlotsFunction) {
            this.tryToAssignSlotsFunction = tryToAssignSlotsFunction;
        }

        @Override
        public void goToFinished(ArchivedExecutionGraph archivedExecutionGraph) {
            finishedStateValidator.validateInput(archivedExecutionGraph);
            hadStateTransitionHappened = true;
        }

        @Override
        public void goToExecuting(ExecutionGraph executionGraph) {
            executingStateValidator.validateInput(executionGraph);
            hadStateTransitionHappened = true;
        }

        @Override
        public ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause) {
            return ArchivedExecutionGraph.createFromInitializingJob(
                    new JobID(), "testJob", jobStatus, cause, 0L);
        }

        @Override
        public void runIfState(State expectedState, Runnable action, Duration delay) {
            if (!hadStateTransitionHappened) {
                action.run();
            }
        }

        @Override
        public CreatingExecutionGraph.AssignmentResult tryToAssignSlots(
                ExecutionGraph executionGraph) {
            return tryToAssignSlotsFunction.apply(executionGraph);
        }

        @Override
        public void goToWaitingForResources() {
            waitingForResourcesStateValidator.validateInput(null);
            hadStateTransitionHappened = true;
        }

        @Override
        public void close() throws Exception {
            finishedStateValidator.close();
            waitingForResourcesStateValidator.close();
            executingStateValidator.close();
        }
    }
}
