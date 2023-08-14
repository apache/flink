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
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.jsonplan.JsonPlanGenerator;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.scheduler.DefaultOperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.GlobalFailureHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.adaptive.allocator.VertexParallelism;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;

/**
 * State which waits for the creation of the {@link ExecutionGraph}. If the creation fails, then the
 * state transitions to {@link Finished}. If the creation succeeds, then the system tries to assign
 * the required slots. If the set of available slots has changed so that the created {@link
 * ExecutionGraph} cannot be executed, the state transitions back into {@link WaitingForResources}.
 * If there are enough slots for the {@link ExecutionGraph} to run, the state transitions to {@link
 * Executing}.
 */
public class CreatingExecutionGraph extends StateWithoutExecutionGraph {

    private final Context context;
    private final OperatorCoordinatorHandlerFactory operatorCoordinatorHandlerFactory;

    private final @Nullable ExecutionGraph previousExecutionGraph;

    public CreatingExecutionGraph(
            Context context,
            CompletableFuture<ExecutionGraphWithVertexParallelism>
                    executionGraphWithParallelismFuture,
            Logger logger,
            OperatorCoordinatorHandlerFactory operatorCoordinatorFactory,
            ExecutionGraph previousExecutionGraph1) {
        super(context, logger);
        this.context = context;
        this.operatorCoordinatorHandlerFactory = operatorCoordinatorFactory;

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
        previousExecutionGraph = previousExecutionGraph1;
    }

    private void handleExecutionGraphCreation(
            @Nullable ExecutionGraphWithVertexParallelism executionGraphWithVertexParallelism,
            @Nullable Throwable throwable) {
        if (throwable != null) {
            getLogger()
                    .info(
                            "Failed to go from {} to {} because the ExecutionGraph creation failed.",
                            CreatingExecutionGraph.class.getSimpleName(),
                            Executing.class.getSimpleName(),
                            throwable);
            context.goToFinished(context.getArchivedExecutionGraph(JobStatus.FAILED, throwable));
        } else {
            for (ExecutionVertex vertex :
                    executionGraphWithVertexParallelism.executionGraph.getAllExecutionVertices()) {
                vertex.getCurrentExecutionAttempt().transitionState(ExecutionState.SCHEDULED);
            }

            final AssignmentResult result =
                    context.tryToAssignSlots(executionGraphWithVertexParallelism);

            if (result.isSuccess()) {
                getLogger()
                        .debug(
                                "Successfully reserved and assigned the required slots for the ExecutionGraph.");
                final ExecutionGraph executionGraph = result.getExecutionGraph();
                final ExecutionGraphHandler executionGraphHandler =
                        new ExecutionGraphHandler(
                                executionGraph,
                                getLogger(),
                                context.getIOExecutor(),
                                context.getMainThreadExecutor());
                // Operator coordinator outlives the current state, so we need to use context as a
                // global failure handler.
                final OperatorCoordinatorHandler operatorCoordinatorHandler =
                        operatorCoordinatorHandlerFactory.create(executionGraph, context);
                operatorCoordinatorHandler.initializeOperatorCoordinators(
                        context.getMainThreadExecutor());
                operatorCoordinatorHandler.startAllOperatorCoordinators();
                final String updatedPlan =
                        JsonPlanGenerator.generatePlan(
                                executionGraph.getJobID(),
                                executionGraph.getJobName(),
                                JobType.STREAMING, // Adaptive scheduler works only with STREAMING
                                // jobs
                                () ->
                                        IterableUtils.toStream(
                                                        executionGraph.getVerticesTopologically())
                                                .map(ExecutionJobVertex::getJobVertex)
                                                .iterator(),
                                executionGraphWithVertexParallelism.getVertexParallelism());
                executionGraph.setJsonPlan(updatedPlan);
                context.goToExecuting(
                        result.getExecutionGraph(),
                        executionGraphHandler,
                        operatorCoordinatorHandler,
                        Collections.emptyList());
            } else {
                getLogger()
                        .debug(
                                "Failed to reserve and assign the required slots. Waiting for new resources.");
                context.goToWaitingForResources(previousExecutionGraph);
            }
        }
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.CREATED;
    }

    /** Context for the {@link CreatingExecutionGraph} state. */
    interface Context
            extends StateWithoutExecutionGraph.Context,
                    GlobalFailureHandler,
                    StateTransitions.ToExecuting,
                    StateTransitions.ToWaitingForResources {

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

        /**
         * Gets the I/O executor.
         *
         * @return the I/O executor
         */
        Executor getIOExecutor();

        /**
         * Gets the main thread executor.
         *
         * @return the main thread executor
         */
        ComponentMainThreadExecutor getMainThreadExecutor();

        /**
         * Gets the {@link JobManagerJobMetricGroup}.
         *
         * @return the metric group
         */
        JobManagerJobMetricGroup getMetricGroup();
    }

    @FunctionalInterface
    interface OperatorCoordinatorHandlerFactory {

        /**
         * Creates a new {@link OperatorCoordinatorHandler}. This interface is primarily intended
         * for easier testing.
         *
         * @param executionGraph Current execution graph, that contains operator coordinators that
         *     we want to start.
         * @param globalFailureHandler Global failure handler.
         * @return An {@link OperatorCoordinatorHandler} instance.
         */
        OperatorCoordinatorHandler create(
                ExecutionGraph executionGraph, GlobalFailureHandler globalFailureHandler);
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

        private final @Nullable ExecutionGraph previousExecutionGraph;

        private final Logger log;

        Factory(
                Context context,
                CompletableFuture<ExecutionGraphWithVertexParallelism>
                        executionGraphWithParallelismFuture,
                Logger log,
                @Nullable ExecutionGraph previousExecutionGraph) {
            this.context = context;
            this.executionGraphWithParallelismFuture = executionGraphWithParallelismFuture;
            this.log = log;
            this.previousExecutionGraph = previousExecutionGraph;
        }

        @Override
        public Class<CreatingExecutionGraph> getStateClass() {
            return CreatingExecutionGraph.class;
        }

        @Override
        public CreatingExecutionGraph getState() {
            return new CreatingExecutionGraph(
                    context,
                    executionGraphWithParallelismFuture,
                    log,
                    DefaultOperatorCoordinatorHandler::new,
                    previousExecutionGraph);
        }
    }

    static class ExecutionGraphWithVertexParallelism {
        private final ExecutionGraph executionGraph;

        private final JobSchedulingPlan jobSchedulingPlan;

        private ExecutionGraphWithVertexParallelism(
                ExecutionGraph executionGraph, JobSchedulingPlan jobSchedulingPlan) {
            this.executionGraph = executionGraph;
            this.jobSchedulingPlan = jobSchedulingPlan;
        }

        public static ExecutionGraphWithVertexParallelism create(
                ExecutionGraph executionGraph, JobSchedulingPlan vertexParallelism) {
            return new ExecutionGraphWithVertexParallelism(executionGraph, vertexParallelism);
        }

        public ExecutionGraph getExecutionGraph() {
            return executionGraph;
        }

        public VertexParallelism getVertexParallelism() {
            return jobSchedulingPlan.getVertexParallelism();
        }

        public JobSchedulingPlan getJobSchedulingPlan() {
            return jobSchedulingPlan;
        }
    }
}
