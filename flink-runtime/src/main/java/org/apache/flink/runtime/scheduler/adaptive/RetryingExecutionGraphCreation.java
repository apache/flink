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

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;

/**
 * State entered when {@link CreatingExecutionGraph} fails to create the {@link ExecutionGraph} with
 * a recoverable error.
 */
class RetryingExecutionGraphCreation extends StateWithoutExecutionGraph {

    private final Context context;

    private final Duration backoffTime;

    @Nullable private final ExecutionGraph previousExecutionGraph;

    @Nullable private ScheduledFuture<?> goToSubsequentStateFuture;

    RetryingExecutionGraphCreation(
            Context context,
            Logger logger,
            @Nullable ExecutionGraph previousExecutionGraph,
            Duration backoffTime) {
        super(context, logger);
        this.context = context;
        this.previousExecutionGraph = previousExecutionGraph;
        this.backoffTime = backoffTime;

        goToSubsequentStateFuture =
                context.runIfState(this, this::goToSubsequentState, backoffTime);
    }

    @Override
    public void onLeave(Class<? extends State> newState) {
        if (goToSubsequentStateFuture != null) {
            goToSubsequentStateFuture.cancel(false);
        }
        super.onLeave(newState);
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.CREATED;
    }

    private void goToSubsequentState() {
        context.goToWaitingForResources(previousExecutionGraph);
    }

    /** Context of the {@link RetryingExecutionGraphCreation} state. */
    interface Context
            extends StateWithoutExecutionGraph.Context, StateTransitions.ToWaitingForResources {

        /**
         * Runs the given action after the specified delay if the state is the expected state at
         * this time.
         *
         * @param expectedState expectedState describes the required state to run the action after
         *     the delay
         * @param action action to run if the state equals the expected state
         * @param delay delay after which to run the action
         * @return a ScheduledFuture representing pending completion of the task
         */
        ScheduledFuture<?> runIfState(State expectedState, Runnable action, Duration delay);
    }

    static class Factory implements StateFactory<RetryingExecutionGraphCreation> {

        private final Context context;
        private final Logger log;
        @Nullable private final ExecutionGraph previousExecutionGraph;
        private final Duration backoffTime;

        public Factory(
                Context context,
                Logger log,
                @Nullable ExecutionGraph previousExecutionGraph,
                Duration backoffTime) {
            this.context = context;
            this.log = log;
            this.previousExecutionGraph = previousExecutionGraph;
            this.backoffTime = backoffTime;
        }

        @Override
        public Class<RetryingExecutionGraphCreation> getStateClass() {
            return RetryingExecutionGraphCreation.class;
        }

        @Override
        public RetryingExecutionGraphCreation getState() {
            return new RetryingExecutionGraphCreation(
                    context, log, previousExecutionGraph, backoffTime);
        }
    }
}
