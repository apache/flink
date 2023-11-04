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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
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
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;

/** State which represents a running job with an {@link ExecutionGraph} and assigned slots. */
class Executing extends StateWithExecutionGraph implements ResourceListener {

    private final Context context;
    private final Instant lastRescale;
    // only one schedule at the time
    private boolean rescaleScheduled = false;
    private final Duration scalingIntervalMin;
    @Nullable private final Duration scalingIntervalMax;

    Executing(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Logger logger,
            Context context,
            ClassLoader userCodeClassLoader,
            List<ExceptionHistoryEntry> failureCollection,
            Duration scalingIntervalMin,
            @Nullable Duration scalingIntervalMax,
            Instant lastRescale) {
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
        this.scalingIntervalMin = scalingIntervalMin;
        this.scalingIntervalMax = scalingIntervalMax;
        // Executing is recreated with each restart (when we rescale)
        // we consider the first execution of the pipeline as a rescale event
        this.lastRescale = lastRescale;
        Preconditions.checkState(
                !scalingIntervalMin.isNegative(),
                "%s must be positive integer or 0",
                JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MIN.key());
        if (scalingIntervalMax != null) {
            Preconditions.checkState(
                    scalingIntervalMax.compareTo(scalingIntervalMin) > 0,
                    "%s(%d) must be greater than %s(%d)",
                    JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MAX.key(),
                    scalingIntervalMax,
                    JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MIN.key(),
                    scalingIntervalMin);
        }

        deploy();

        // check if new resources have come available in the meantime
        rescaleWhenCooldownPeriodIsOver();
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
        rescaleWhenCooldownPeriodIsOver();
    }

    @Override
    public void onNewResourceRequirements() {
        rescaleWhenCooldownPeriodIsOver();
    }

    /** Force rescaling as long as the target parallelism is different from the current one. */
    private void forceRescale() {
        if (context.shouldRescale(getExecutionGraph(), true)) {
            getLogger()
                    .info(
                            "Added resources are still there after {} time({}), force a rescale.",
                            JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MAX.key(),
                            scalingIntervalMax);
            context.goToRestarting(
                    getExecutionGraph(),
                    getExecutionGraphHandler(),
                    getOperatorCoordinatorHandler(),
                    Duration.ofMillis(0L),
                    getFailures());
        }
    }

    /**
     * Rescale the job if {@link Context#shouldRescale} is true. Otherwise, force a rescale using
     * {@link Executing#forceRescale()} after {@link
     * JobManagerOptions#SCHEDULER_SCALING_INTERVAL_MAX}.
     */
    private void maybeRescale() {
        rescaleScheduled = false;
        if (context.shouldRescale(getExecutionGraph(), false)) {
            getLogger().info("Can change the parallelism of the job. Restarting the job.");
            context.goToRestarting(
                    getExecutionGraph(),
                    getExecutionGraphHandler(),
                    getOperatorCoordinatorHandler(),
                    Duration.ofMillis(0L),
                    getFailures());
        } else if (scalingIntervalMax != null) {
            getLogger()
                    .info(
                            "The longer the pipeline runs, the more the (small) resource gain is worth the restarting time. "
                                    + "Last resource added does not meet {}, force a rescale after {} time({}) if the resource is still there.",
                            JobManagerOptions.MIN_PARALLELISM_INCREASE,
                            JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MAX.key(),
                            scalingIntervalMax);
            if (timeSinceLastRescale().compareTo(scalingIntervalMax) > 0) {
                forceRescale();
            } else {
                // schedule a force rescale in JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MAX time
                context.runIfState(this, this::forceRescale, scalingIntervalMax);
            }
        }
    }

    private Duration timeSinceLastRescale() {
        return Duration.between(lastRescale, Instant.now());
    }

    private void rescaleWhenCooldownPeriodIsOver() {
        if (timeSinceLastRescale().compareTo(scalingIntervalMin) > 0) {
            maybeRescale();
        } else if (!rescaleScheduled) {
            rescaleScheduled = true;
            // schedule maybeRescale resetting the cooldown period
            context.runIfState(this, this::maybeRescale, scalingIntervalMin);
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
         * @param forceRescale should we force rescaling
         * @return true, if we should rescale
         */
        boolean shouldRescale(ExecutionGraph executionGraph, boolean forceRescale);

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
            final Configuration jobConfiguration = executionGraph.getJobConfiguration();
            return new Executing(
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    log,
                    context,
                    userCodeClassLoader,
                    failureCollection,
                    jobConfiguration.get(JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MIN),
                    jobConfiguration.get(JobManagerOptions.SCHEDULER_SCALING_INTERVAL_MAX),
                    Instant.now());
        }
    }
}
