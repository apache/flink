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

import org.apache.flink.core.testutils.CompletedScheduledFuture;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Consumer;

import static org.apache.flink.runtime.scheduler.adaptive.WaitingForResourcesTest.assertNonNull;

/** Mock the {@link StateWithExecutionGraph.Context} for restarting state. */
class MockRestartingContext extends MockStateWithExecutionGraphContext
        implements Restarting.Context {

    private final StateValidator<ExecutingTest.CancellingArguments> cancellingStateValidator =
            new StateValidator<>("Cancelling");

    private final StateValidator<ExecutionGraph> waitingForResourcesStateValidator =
            new StateValidator<>("WaitingForResources");

    private final StateValidator<ExecutionGraph> creatingExecutionGraphStateValidator =
            new StateValidator<>("CreatingExecutionGraph");

    @Nullable private VertexParallelism availableVertexParallelism;

    public void setExpectCancelling(Consumer<ExecutingTest.CancellingArguments> asserter) {
        cancellingStateValidator.expectInput(asserter);
    }

    public void setExpectWaitingForResources() {
        waitingForResourcesStateValidator.expectInput(assertNonNull());
    }

    public void setExpectCreatingExecutionGraph() {
        creatingExecutionGraphStateValidator.expectInput(assertNonNull());
    }

    public void setAvailableVertexParallelism(
            @Nullable VertexParallelism availableVertexParallelism) {
        this.availableVertexParallelism = availableVertexParallelism;
    }

    @Override
    public void goToCanceling(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            List<ExceptionHistoryEntry> failureCollection) {
        cancellingStateValidator.validateInput(
                new ExecutingTest.CancellingArguments(
                        executionGraph, executionGraphHandler, operatorCoordinatorHandler));
        hadStateTransition = true;
    }

    @Override
    public void archiveFailure(RootExceptionHistoryEntry failure) {}

    @Override
    public void goToWaitingForResources(@Nullable ExecutionGraph previousExecutionGraph) {
        waitingForResourcesStateValidator.validateInput(previousExecutionGraph);
        hadStateTransition = true;
    }

    @Override
    public void goToCreatingExecutionGraph(@Nullable ExecutionGraph previousExecutionGraph) {
        creatingExecutionGraphStateValidator.validateInput(previousExecutionGraph);
        hadStateTransition = true;
    }

    @Override
    public ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay) {
        if (!hadStateTransition) {
            action.run();
        }
        return CompletedScheduledFuture.create(null);
    }

    @Override
    public Optional<VertexParallelism> getAvailableVertexParallelism() {
        return Optional.ofNullable(availableVertexParallelism);
    }

    @Override
    public void close() throws Exception {
        super.close();
        cancellingStateValidator.close();
        waitingForResourcesStateValidator.close();
        creatingExecutionGraphStateValidator.close();
    }
}
