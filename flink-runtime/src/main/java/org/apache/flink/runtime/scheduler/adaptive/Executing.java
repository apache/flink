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
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;

/** State which represents a running job with an {@link ExecutionGraph} and assigned slots. */
class Executing extends StateWithExecutionGraph implements ResourceConsumer {

    private final Context context;

    private final ClassLoader userCodeClassLoader;

    Executing(
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Logger logger,
            Context context,
            ClassLoader userCodeClassLoader) {
        super(context, executionGraph, executionGraphHandler, operatorCoordinatorHandler, logger);
        this.context = context;
        this.userCodeClassLoader = userCodeClassLoader;

        deploy();

        // check if new resources have come available in the meantime
        context.runIfState(this, this::notifyNewResourcesAvailable, Duration.ZERO);
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.RUNNING;
    }

    @Override
    public void cancel() {
        context.goToCanceling(
                getExecutionGraph(), getExecutionGraphHandler(), getOperatorCoordinatorHandler());
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        handleAnyFailure(cause);
    }

    private void handleAnyFailure(Throwable cause) {
        final FailureResult failureResult = context.howToHandleFailure(cause);

        if (failureResult.canRestart()) {
            context.goToRestarting(
                    getExecutionGraph(),
                    getExecutionGraphHandler(),
                    getOperatorCoordinatorHandler(),
                    failureResult.getBackoffTime());
        } else {
            context.goToFailing(
                    getExecutionGraph(),
                    getExecutionGraphHandler(),
                    getOperatorCoordinatorHandler(),
                    failureResult.getFailureCause());
        }
    }

    @Override
    boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionState) {
        final boolean successfulUpdate = getExecutionGraph().updateState(taskExecutionState);

        if (successfulUpdate) {
            if (taskExecutionState.getExecutionState() == ExecutionState.FAILED) {
                Throwable cause = taskExecutionState.getError(userCodeClassLoader);
                handleAnyFailure(cause);
            }
        }

        return successfulUpdate;
    }

    @Override
    void onGloballyTerminalState(JobStatus globallyTerminalState) {
        context.goToFinished(ArchivedExecutionGraph.createFrom(getExecutionGraph()));
    }

    private void deploy() {
        for (ExecutionJobVertex executionJobVertex :
                getExecutionGraph().getVerticesTopologically()) {
            for (ExecutionVertex executionVertex : executionJobVertex.getTaskVertices()) {
                deploySafely(executionVertex);
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
    public void notifyNewResourcesAvailable() {
        if (context.canScaleUp(getExecutionGraph())) {
            getLogger().info("New resources are available. Restarting job to scale up.");
            context.goToRestarting(
                    getExecutionGraph(),
                    getExecutionGraphHandler(),
                    getOperatorCoordinatorHandler(),
                    Duration.ofMillis(0L));
        }
    }

    /** Context of the {@link Executing} state. */
    interface Context extends StateWithExecutionGraph.Context {

        /**
         * Transitions into the {@link Canceling} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Canceling} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Canceling} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Canceling} state
         */
        void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler);

        /**
         * Asks how to handle the failure.
         *
         * @param failure failure describing the failure cause
         * @return {@link FailureResult} which describes how to handle the failure
         */
        FailureResult howToHandleFailure(Throwable failure);

        /**
         * Asks if we can scale up the currently executing job.
         *
         * @param executionGraph executionGraph for making the scaling decision.
         * @return true, if we can scale up
         */
        boolean canScaleUp(ExecutionGraph executionGraph);

        /**
         * Transitions into the {@link Restarting} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Restarting} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Restarting}
         *     state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pas to the {@link
         *     Restarting} state
         * @param backoffTime backoffTime to wait before transitioning to the {@link Restarting}
         *     state
         */
        void goToRestarting(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Duration backoffTime);

        /**
         * Transitions into the {@link Failing} state.
         *
         * @param executionGraph executionGraph to pass to the {@link Failing} state
         * @param executionGraphHandler executionGraphHandler to pass to the {@link Failing} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler to pass to the {@link
         *     Failing} state
         * @param failureCause failureCause describing why the job execution failed
         */
        void goToFailing(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Throwable failureCause);

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

    /**
     * The {@link FailureResult} describes how a failure shall be handled. Currently, there are two
     * alternatives: Either restarting the job or failing it.
     */
    static final class FailureResult {
        @Nullable private final Duration backoffTime;

        @Nullable private final Throwable failureCause;

        private FailureResult(@Nullable Duration backoffTime, @Nullable Throwable failureCause) {
            this.backoffTime = backoffTime;
            this.failureCause = failureCause;
        }

        boolean canRestart() {
            return backoffTime != null;
        }

        Duration getBackoffTime() {
            Preconditions.checkState(
                    canRestart(), "Failure result must be restartable to return a backoff time.");
            return backoffTime;
        }

        Throwable getFailureCause() {
            Preconditions.checkState(
                    failureCause != null,
                    "Failure result must not be restartable to return a failure cause.");
            return failureCause;
        }

        /**
         * Creates a FailureResult which allows to restart the job.
         *
         * @param backoffTime backoffTime to wait before restarting the job
         * @return FailureResult which allows to restart the job
         */
        static FailureResult canRestart(Duration backoffTime) {
            return new FailureResult(backoffTime, null);
        }

        /**
         * Creates FailureResult which does not allow to restart the job.
         *
         * @param failureCause failureCause describes the reason why the job cannot be restarted
         * @return FailureResult which does not allow to restart the job
         */
        static FailureResult canNotRestart(Throwable failureCause) {
            return new FailureResult(null, failureCause);
        }
    }

    static class Factory implements StateFactory<Executing> {

        private final Context context;
        private final Logger log;
        private final ExecutionGraph executionGraph;
        private final ExecutionGraphHandler executionGraphHandler;
        private final OperatorCoordinatorHandler operatorCoordinatorHandler;
        private final ClassLoader userCodeClassLoader;

        public Factory(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Logger log,
                Context context,
                ClassLoader userCodeClassLoader) {
            this.context = context;
            this.log = log;
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
            this.userCodeClassLoader = userCodeClassLoader;
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
                    userCodeClassLoader);
        }
    }
}
