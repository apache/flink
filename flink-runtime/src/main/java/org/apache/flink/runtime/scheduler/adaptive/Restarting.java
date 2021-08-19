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
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.TaskExecutionStateTransition;
import org.apache.flink.runtime.scheduler.ExecutionGraphHandler;
import org.apache.flink.runtime.scheduler.OperatorCoordinatorHandler;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;

/** State which describes a job which is currently being restarted. */
class Restarting extends StateWithExecutionGraph {

    private final Context context;

    private final Duration backoffTime;

    @Nullable private ScheduledFuture<?> goToWaitingForResourcesFuture;

    Restarting(
            Context context,
            ExecutionGraph executionGraph,
            ExecutionGraphHandler executionGraphHandler,
            OperatorCoordinatorHandler operatorCoordinatorHandler,
            Logger logger,
            Duration backoffTime,
            ClassLoader userCodeClassLoader) {
        super(
                context,
                executionGraph,
                executionGraphHandler,
                operatorCoordinatorHandler,
                logger,
                userCodeClassLoader);
        this.context = context;
        this.backoffTime = backoffTime;

        getExecutionGraph().cancel();
    }

    @Override
    public void onLeave(Class<? extends State> newState) {
        if (goToWaitingForResourcesFuture != null) {
            goToWaitingForResourcesFuture.cancel(false);
        }

        super.onLeave(newState);
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.RESTARTING;
    }

    @Override
    public void cancel() {
        context.goToCanceling(
                getExecutionGraph(), getExecutionGraphHandler(), getOperatorCoordinatorHandler());
    }

    @Override
    public void handleGlobalFailure(Throwable cause) {
        getLogger()
                .debug(
                        "Ignored global failure because we are already restarting the job {}.",
                        getJobId(),
                        cause);
    }

    @Override
    boolean updateTaskExecutionState(TaskExecutionStateTransition taskExecutionStateTransition) {
        ExecutionVertexID executionVertexId =
                getExecutionVertexId(taskExecutionStateTransition.getID());
        Throwable cause = taskExecutionStateTransition.getError(userCodeClassLoader);
        archiveExecutionFailure(
                executionVertexId,
                cause == null
                        ? new FlinkException(
                                "Unknown failure cause. Probably related to FLINK-21376.")
                        : cause);
        return getExecutionGraph().updateState(taskExecutionStateTransition);
    }

    @Override
    void onGloballyTerminalState(JobStatus globallyTerminalState) {
        Preconditions.checkArgument(globallyTerminalState == JobStatus.CANCELED);
        goToWaitingForResourcesFuture =
                context.runIfState(this, context::goToWaitingForResources, backoffTime);
    }

    /** Context of the {@link Restarting} state. */
    interface Context extends StateWithExecutionGraph.Context {

        /**
         * Transitions into the {@link Canceling} state.
         *
         * @param executionGraph executionGraph which is passed to the {@link Canceling} state
         * @param executionGraphHandler executionGraphHandler which is passed to the {@link
         *     Canceling} state
         * @param operatorCoordinatorHandler operatorCoordinatorHandler which is passed to the
         *     {@link Canceling} state
         */
        void goToCanceling(
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler);

        /** Transitions into the {@link WaitingForResources} state. */
        void goToWaitingForResources();

        /**
         * Runs the given action after the specified delay if the state is the expected state at
         * this time.
         *
         * @param expectedState expectedState describes the required state to run the action after
         *     the delay
         * @param action action to run if the state equals the expected state
         * @param delay delay after which the action should be executed
         * @return a ScheduledFuture representing pending completion of the task
         */
        ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay);
    }

    static class Factory implements StateFactory<Restarting> {

        private final Context context;
        private final Logger log;
        private final ExecutionGraph executionGraph;
        private final ExecutionGraphHandler executionGraphHandler;
        private final OperatorCoordinatorHandler operatorCoordinatorHandler;
        private final Duration backoffTime;
        private final ClassLoader userCodeClassLoader;

        public Factory(
                Context context,
                ExecutionGraph executionGraph,
                ExecutionGraphHandler executionGraphHandler,
                OperatorCoordinatorHandler operatorCoordinatorHandler,
                Logger log,
                Duration backoffTime,
                ClassLoader userCodeClassLoader) {
            this.context = context;
            this.log = log;
            this.executionGraph = executionGraph;
            this.executionGraphHandler = executionGraphHandler;
            this.operatorCoordinatorHandler = operatorCoordinatorHandler;
            this.backoffTime = backoffTime;
            this.userCodeClassLoader = userCodeClassLoader;
        }

        public Class<Restarting> getStateClass() {
            return Restarting.class;
        }

        public Restarting getState() {
            return new Restarting(
                    context,
                    executionGraph,
                    executionGraphHandler,
                    operatorCoordinatorHandler,
                    log,
                    backoffTime,
                    userCodeClassLoader);
        }
    }
}
