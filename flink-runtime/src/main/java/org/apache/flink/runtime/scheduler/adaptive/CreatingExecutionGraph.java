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
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

/**
 * State which waits for the creation of the {@link ExecutionGraph}. If the creation fails, then the
 * state transitions to {@link Finished}. If the creation succeeds, then the system tries to assign
 * the required slots. If the set of available slots has changed so that the created {@link
 * ExecutionGraph} cannot be executed, the state transitions back into {@link WaitingForResources}.
 * If there are enough slots for the {@link ExecutionGraph} to run, the state transitions to {@link
 * Executing}.
 */
public class CreatingExecutionGraph implements State {

    private final Context context;

    private final Logger log;

    public CreatingExecutionGraph(
            Context context,
            CompletableFuture<ExecutionGraphWithVertexParallelism>
                    executionGraphWithParallelismFuture,
            Logger log) {
        this.context = context;
        this.log = log;

        FutureUtils.assertNoException(
                executionGraphWithParallelismFuture.handle(
                        (executionGraphWithVertexParallelism, throwable) -> {
                            context.runIfState(
                                    this,
                                    () ->
                                            handleExecutionGraphCreation(
                                                    executionGraphWithVertexParallelism, throwable),
                                    Duration.ZERO);
                            return null;
                        }));
    }

    private void handleExecutionGraphCreation(
            @Nullable ExecutionGraphWithVertexParallelism executionGraphWithVertexParallelism,
            @Nullable Throwable throwable) {
        if (throwable != null) {
            log.info(
                    "Failed to go from {} to {} because the ExecutionGraph creation failed.",
                    CreatingExecutionGraph.class.getSimpleName(),
                    Executing.class.getSimpleName(),
                    throwable);
            context.goToFinished(context.getArchivedExecutionGraph(JobStatus.FAILED, throwable));
        } else {
            final AssignmentResult result =
                    context.tryToAssignSlots(executionGraphWithVertexParallelism);

            if (result.isSuccess()) {
                log.debug(
                        "Successfully reserved and assigned the required slots for the ExecutionGraph.");
                context.goToExecuting(result.getExecutionGraph());
            } else {
                log.debug(
                        "Failed to reserve and assign the required slots. Waiting for new resources.");
                context.goToWaitingForResources();
            }
        }
    }

    @Override
    public void cancel() {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.CANCELED, null));
    }

    @Override
    public void suspend(Throwable cause) {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.SUSPENDED, cause));
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.INITIALIZING;
    }

    @Override
    public ArchivedExecutionGraph getJob() {
        return context.getArchivedExecutionGraph(getJobStatus(), null);
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        context.goToFinished(context.getArchivedExecutionGraph(JobStatus.FAILED, cause));
    }

    @Override
    public Logger getLogger() {
        return log;
    }

    /** Context for the {@link CreatingExecutionGraph} state. */
    interface Context {

        /**
         * Transitions into the {@link Finished} state.
         *
         * @param archivedExecutionGraph archivedExecutionGraph representing the final job state
         */
        void goToFinished(ArchivedExecutionGraph archivedExecutionGraph);

        /**
         * Transitions into the {@link Executing} state.
         *
         * @param executionGraph executionGraph which is passed to the {@link Executing} state
         */
        void goToExecuting(ExecutionGraph executionGraph);

        /** Transitions into the {@link WaitingForResources} state. */
        void goToWaitingForResources();

        /**
         * Creates the {@link ArchivedExecutionGraph} for the given job status and cause. Cause can
         * be null if there is no failure.
         *
         * @param jobStatus jobStatus to initialize the {@link ArchivedExecutionGraph} with
         * @param cause cause describing a failure cause; {@code null} if there is none
         * @return the created {@link ArchivedExecutionGraph}
         */
        ArchivedExecutionGraph getArchivedExecutionGraph(
                JobStatus jobStatus, @Nullable Throwable cause);

        /**
         * Runs the given action after a delay if the state at this time equals the expected state.
         *
         * @param expectedState expectedState describes the required state at the time of running
         *     the action
         * @param action action to run if the expected state equals the actual state
         * @param delay delay after which to run the action
         * @return a ScheduledFuture representing pending completion of the task
         */
        ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay);

        /**
         * Try to assign slots to the created {@link ExecutionGraph}. If it is possible, then this
         * method returns a successful {@link AssignmentResult} which contains the assigned {@link
         * ExecutionGraph}. If not, then the assignment result is a failure.
         *
         * @param executionGraphWithVertexParallelism executionGraphWithVertexParallelism to assign
         *     slots to resources
         * @return {@link AssignmentResult} representing the result of the assignment
         */
        AssignmentResult tryToAssignSlots(
                ExecutionGraphWithVertexParallelism executionGraphWithVertexParallelism);
    }

    /**
     * Class representing the assignment result of the slots to the {@link ExecutionGraph}. The
     * assignment is either successful or not possible. If it is successful, the assignment also
     * contains the assigned {@link ExecutionGraph}.
     */
    static final class AssignmentResult {

        private static final AssignmentResult NOT_POSSIBLE = new AssignmentResult(null);

        @Nullable private final ExecutionGraph executionGraph;

        private AssignmentResult(@Nullable ExecutionGraph executionGraph) {
            this.executionGraph = executionGraph;
        }

        boolean isSuccess() {
            return executionGraph != null;
        }

        ExecutionGraph getExecutionGraph() {
            Preconditions.checkState(
                    isSuccess(), "Can only return the ExecutionGraph if it is a success.");
            return executionGraph;
        }

        static AssignmentResult success(ExecutionGraph executionGraph) {
            return new AssignmentResult(
                    Preconditions.checkNotNull(
                            executionGraph,
                            "AssignmentResult.success expects a non-null ExecutionGraph."));
        }

        static AssignmentResult notPossible() {
            return NOT_POSSIBLE;
        }
    }

    /** Factory for the {@link CreatingExecutionGraph} state. */
    static class Factory implements StateFactory<CreatingExecutionGraph> {

        private final Context context;

        private final CompletableFuture<ExecutionGraphWithVertexParallelism>
                executionGraphWithParallelismFuture;

        private final Logger log;

        Factory(
                Context context,
                CompletableFuture<ExecutionGraphWithVertexParallelism>
                        executionGraphWithParallelismFuture,
                Logger log) {
            this.context = context;
            this.executionGraphWithParallelismFuture = executionGraphWithParallelismFuture;
            this.log = log;
        }

        @Override
        public Class<CreatingExecutionGraph> getStateClass() {
            return CreatingExecutionGraph.class;
        }

        @Override
        public CreatingExecutionGraph getState() {
            return new CreatingExecutionGraph(context, executionGraphWithParallelismFuture, log);
        }
    }

    static class ExecutionGraphWithVertexParallelism {
        private final ExecutionGraph executionGraph;

        private final VertexParallelism vertexParallelism;

        private ExecutionGraphWithVertexParallelism(
                ExecutionGraph executionGraph, VertexParallelism vertexParallelism) {
            this.executionGraph = executionGraph;
            this.vertexParallelism = vertexParallelism;
        }

        public static ExecutionGraphWithVertexParallelism create(
                ExecutionGraph executionGraph, VertexParallelism vertexParallelism) {
            return new ExecutionGraphWithVertexParallelism(executionGraph, vertexParallelism);
        }

        public ExecutionGraph getExecutionGraph() {
            return executionGraph;
        }

        public VertexParallelism getVertexParallelism() {
            return vertexParallelism;
        }
    }
}
