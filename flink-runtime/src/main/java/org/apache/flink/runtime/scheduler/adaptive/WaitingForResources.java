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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Function;

/**
 * State which describes that the scheduler is waiting for resources in order to execute the job.
 */
class WaitingForResources extends StateWithoutExecutionGraph
        implements ResourceListener, StateTransitionManager.Context {

    private final Context context;

    @Nullable private ScheduledFuture<?> resourceTimeoutFuture;

    @Nullable private final ExecutionGraph previousExecutionGraph;

    private final StateTransitionManager stateTransitionManager;

    @VisibleForTesting
    WaitingForResources(
            Context context,
            Logger log,
            Duration submissionResourceWaitTimeout,
            Function<StateTransitionManager.Context, StateTransitionManager>
                    stateTransitionManagerFactory) {
        this(context, log, submissionResourceWaitTimeout, null, stateTransitionManagerFactory);
    }

    WaitingForResources(
            Context context,
            Logger log,
            Duration submissionResourceWaitTimeout,
            @Nullable ExecutionGraph previousExecutionGraph,
            Function<StateTransitionManager.Context, StateTransitionManager>
                    stateTransitionManagerFactory) {
        super(context, log);
        this.context = Preconditions.checkNotNull(context);
        Preconditions.checkNotNull(submissionResourceWaitTimeout);
        this.stateTransitionManager = stateTransitionManagerFactory.apply(this);

        // since state transitions are not allowed in state constructors, schedule calls for later.
        if (!submissionResourceWaitTimeout.isNegative()) {
            resourceTimeoutFuture =
                    context.runIfState(this, this::resourceTimeout, submissionResourceWaitTimeout);
        }
        this.previousExecutionGraph = previousExecutionGraph;
        context.runIfState(this, this::checkPotentialStateTransition, Duration.ZERO);
    }

    @Override
    public void onLeave(Class<? extends State> newState) {
        if (resourceTimeoutFuture != null) {
            resourceTimeoutFuture.cancel(false);
        }
        stateTransitionManager.close();
        super.onLeave(newState);
    }

    @Override
    public JobStatus getJobStatus() {
        return JobStatus.CREATED;
    }

    @Override
    public void onNewResourcesAvailable() {
        checkPotentialStateTransition();
    }

    @Override
    public void onNewResourceRequirements() {
        checkPotentialStateTransition();
    }

    private void checkPotentialStateTransition() {
        stateTransitionManager.onChange();
        stateTransitionManager.onTrigger();
    }

    private void resourceTimeout() {
        getLogger()
                .debug(
                        "Initial resource allocation timeout triggered: Creating ExecutionGraph with available resources.");
        transitionToSubsequentState();
    }

    @Override
    public boolean hasSufficientResources() {
        return context.hasSufficientResources();
    }

    @Override
    public boolean hasDesiredResources() {
        return context.hasDesiredResources();
    }

    @Override
    public void transitionToSubsequentState() {
        context.goToCreatingExecutionGraph(previousExecutionGraph);
    }

    @Override
    public ScheduledFuture<?> scheduleOperation(Runnable callback, Duration delay) {
        return context.runIfState(this, callback, delay);
    }

    /** Context of the {@link WaitingForResources} state. */
    interface Context
            extends StateWithoutExecutionGraph.Context, StateTransitions.ToCreatingExecutionGraph {

        /**
         * Checks whether we have the desired resources.
         *
         * @return {@code true} if we have enough resources; otherwise {@code false}
         */
        boolean hasDesiredResources();

        /**
         * Checks if we currently have sufficient resources for executing the job.
         *
         * @return {@code true} if we have sufficient resources; otherwise {@code false}
         */
        boolean hasSufficientResources();

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

    static class Factory implements StateFactory<WaitingForResources> {

        private final Context context;
        private final Logger log;
        private final Duration submissionResourceWaitTimeout;
        @Nullable private final ExecutionGraph previousExecutionGraph;
        private final Function<StateTransitionManager.Context, StateTransitionManager>
                stateTransitionManagerFactory;

        public Factory(
                Context context,
                Logger log,
                Duration submissionResourceWaitTimeout,
                Function<StateTransitionManager.Context, StateTransitionManager>
                        stateTransitionManagerFactory,
                @Nullable ExecutionGraph previousExecutionGraph) {
            this.context = context;
            this.log = log;
            this.submissionResourceWaitTimeout = submissionResourceWaitTimeout;
            this.previousExecutionGraph = previousExecutionGraph;
            this.stateTransitionManagerFactory = stateTransitionManagerFactory;
        }

        public Class<WaitingForResources> getStateClass() {
            return WaitingForResources.class;
        }

        public WaitingForResources getState() {
            return new WaitingForResources(
                    context,
                    log,
                    submissionResourceWaitTimeout,
                    previousExecutionGraph,
                    stateTransitionManagerFactory);
        }
    }
}
