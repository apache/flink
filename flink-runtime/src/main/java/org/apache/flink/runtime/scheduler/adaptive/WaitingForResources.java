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
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.ScheduledFuture;

/**
 * State which describes that the scheduler is waiting for resources in order to execute the job.
 */
class WaitingForResources implements State, ResourceConsumer {

    private final Context context;

    private final Logger log;

    private final ResourceCounter desiredResources;

    private final ScheduledFuture<?> resourceTimeoutFuture;

    WaitingForResources(
            Context context,
            Logger log,
            ResourceCounter desiredResources,
            Duration resourceTimeout) {
        this.context = Preconditions.checkNotNull(context);
        this.log = Preconditions.checkNotNull(log);
        this.desiredResources = Preconditions.checkNotNull(desiredResources);
        Preconditions.checkArgument(
                !desiredResources.isEmpty(), "Desired resources must not be empty");

        // since state transitions are not allowed in state constructors, schedule calls for later.
        resourceTimeoutFuture =
                context.runIfState(
                        this, this::resourceTimeout, Preconditions.checkNotNull(resourceTimeout));
        context.runIfState(this, this::notifyNewResourcesAvailable, Duration.ZERO);
    }

    @Override
    public void onLeave(Class<? extends State> newState) {
        resourceTimeoutFuture.cancel(false);
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

    @Override
    public void notifyNewResourcesAvailable() {
        if (context.hasEnoughResources(desiredResources)) {
            createExecutionGraphWithAvailableResources();
        }
    }

    private void resourceTimeout() {
        log.debug("Resource timeout triggered: Creating ExecutionGraph with available resources.");
        createExecutionGraphWithAvailableResources();
    }

    private void createExecutionGraphWithAvailableResources() {
        context.goToCreatingExecutionGraph();
    }

    /** Context of the {@link WaitingForResources} state. */
    interface Context {

        /**
         * Transitions into the {@link Finished} state.
         *
         * @param archivedExecutionGraph archivedExecutionGraph representing the final job state
         */
        void goToFinished(ArchivedExecutionGraph archivedExecutionGraph);

        /** Transitions into the {@link CreatingExecutionGraph} state. */
        void goToCreatingExecutionGraph();

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
         * Checks whether we have enough resources to fulfill the desired resources.
         *
         * @param desiredResources desiredResources describing the desired resources
         * @return {@code true} if we have enough resources; otherwise {@code false}
         */
        boolean hasEnoughResources(ResourceCounter desiredResources);

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
        private final ResourceCounter desiredResources;
        private final Duration resourceTimeout;

        public Factory(
                Context context,
                Logger log,
                ResourceCounter desiredResources,
                Duration resourceTimeout) {
            this.context = context;
            this.log = log;
            this.desiredResources = desiredResources;
            this.resourceTimeout = resourceTimeout;
        }

        public Class<WaitingForResources> getStateClass() {
            return WaitingForResources.class;
        }

        public WaitingForResources getState() {
            return new WaitingForResources(context, log, desiredResources, resourceTimeout);
        }
    }
}
