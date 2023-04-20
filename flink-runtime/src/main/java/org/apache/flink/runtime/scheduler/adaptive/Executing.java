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
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.checkpoint.CheckpointScheduling;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.exceptionhistory.ExceptionHistoryEntry;
import org.apache.flink.runtime.scheduler.stopwithsavepoint.StopWithSavepointTerminationManager;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

/** State which represents a running job with an {@link ExecutionGraph} and assigned slots. */
class Executing extends StateWithExecutionGraph implements ResourceListener {

    private final Context context;

    Executing(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Logger logger,
            Context context,
            ClassLoader userCodeClassLoader,
            List<ExceptionHistoryEntry> failureCollection) {
        super(
                context,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                logger,
                userCodeClassLoader,
                failureCollection);
        this.context = context;
        Preconditions.checkState(
                executionGraph.getState() == JobStatus.RUNNING, "Assuming running execution graph");

        deploy();

        // check if new resources have come available in the meantime
        context.runIfState(this, this::maybeRescale, Duration.ZERO);
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.RUNNING;
    }

    @Override
    public void cancel() {
        context.goToCanceling(
                getExecutionGraph(),
                getExecutionGraphHandler(),
                getOperatorCoordinatorHandler(),
                getFailures());
    }

    @Override
    void onFailure(Throwable cause) {
        FailureResultUtil.restartOrFail(context.howToHandleFailure(cause), context, this);
    }

    @Override
    void onGloballyTerminalState(JobStatus globallyTerminalState) {
        context.goToFinished(ArchivedExecutionGraph.createFrom(getExecutionGraph()));
    }

    private void deploy() {
        for (ExecutionJobVertex executionJobVertex :
                getExecutionGraph().getVerticesTopologically()) {
            for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
                if (executionVertex.getExecutionState() == ExecutionState.CREATED
                        || executionVertex.getExecutionState() == ExecutionState.SCHEDULED) {
                    deploySafely(executionVertex);
                }
            }
        }
    }

    private void deploySafely(ExecutionVertex executionVertex) {
        try {
            executionVertex.deploy();
        } catch (JobException e) {
            handleDeploymentFailure(executionVertex, e);
        }
    }

    private void handleDeploymentFailure(ExecutionVertex executionVertex, JobException e) {
        executionVertex.markFailed(e);
    }

    @Override
    public void onNewResourcesAvailable() {
        maybeRescale();
    }

    @Override
    public void onNewResourceRequirements() {
        maybeRescale();
    }

    private void maybeRescale() {
        if (context.shouldRescale(getExecutionGraph())) {
            getLogger().info("Can change the parallelism of job. Restarting job.");
            context.goToRestarting(
                    getExecutionGraph(),
                    getExecutionGraphHandler(),
                    getOperatorCoordinatorHandler(),
                    Duration.ofMillis(0L),
                    getFailures());
        }
    }

    CompletableFuture<String> stopWithSavepoint(
            @Nullable final String targetDirectory,
            boolean terminate,
            SavepointFormatType formatType) {
        final ExecutionGraph executionGraph = getExecutionGraph();

        StopWithSavepointTerminationManager.checkSavepointActionPreconditions(
                executionGraph.getCheckpointCoordinator(),
                targetDirectory,
                executionGraph.getJobID(),
                getLogger());

        getLogger().info("Triggering stop-with-savepoint for job {}.", executionGraph.getJobID());

        CheckpointScheduling schedulingProvider = new CheckpointSchedulingProvider(executionGraph);

        schedulingProvider.stopCheckpointScheduler();

        final CompletableFuture<String> savepointFuture =
                executionGraph
                        .getCheckpointCoordinator()
                        .triggerSynchronousSavepoint(terminate, targetDirectory, formatType)
                        .thenApply(CompletedCheckpoint::getExternalPointer);
        return context.goToStopWithSavepoint(
                executionGraph,
                getExecutionGraphHandler(),
                getOperatorCoordinatorHandler(),
                schedulingProvider,
                savepointFuture,
                getFailures());
    }

    /** Context of the {@link Executing} state. */
    interface Context
            extends StateWithExecutionGraph.Context,
                    StateTransitions.ToCancelling,
                    StateTransitions.ToFailing,
                    StateTransitions.ToRestarting,
                    StateTransitions.ToStopWithSavepoint {

        /**
         * Asks how to handle the failure.
         *
         * @param failure failure describing the failure cause
         * @return {@link FailureResult} which describes how to handle the failure
         */
        FailureResult howToHandleFailure(Throwable failure);

        /**
         * Asks if we should rescale the currently executing job.
         *
         * @param executionGraph executionGraph for making the scaling decision.
         * @return true, if we should rescale
         */
        boolean shouldRescale(ExecutionGraph executionGraph);

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
    }

    static class Factory implements StateFactory<Executing> {

        private final Context context;
        private final Logger log;
        private final ExecutionGraph executionGraph;
        private final ExecutionGraphHandler executionGraphHandler;
        private final OperatorCoordinatorHandler operatorCoordinatorHandler;
        private final ClassLoader userCodeClassLoader;
        private final List<ExceptionHistoryEntry> failureCollection;

        Factory(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Logger log,
                Context context,
                ClassLoader userCodeClassLoader,
                List<ExceptionHistoryEntry> failureCollection) {
            this.context = context;
            this.log = log;
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
            this.userCodeClassLoader = userCodeClassLoader;
            this.failureCollection = failureCollection;
        }

        public Class<Executing> getStateClass() {
            return Executing.class;
        }

        public Executing getState() {
            return new Executing(
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    log,
                    context,
                    userCodeClassLoader,
                    failureCollection);
        }
    }
}
