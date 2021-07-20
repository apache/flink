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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

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
    private final Clock clock;

    /** If set, there's an ongoing deadline waiting for a resource stabilization. */
    @Nullable private Deadline resourceStabilizationDeadline;

    private final Duration resourceStabilizationTimeout;

    @Nullable private ScheduledFuture<?> resourceTimeoutFuture;

    WaitingForResources(
            Context context,
            Logger log,
            ResourceCounter desiredResources,
            Duration initialResourceAllocationTimeout,
            Duration resourceStabilizationTimeout) {
        this(
                context,
                log,
                desiredResources,
                initialResourceAllocationTimeout,
                resourceStabilizationTimeout,
                SystemClock.getInstance());
    }

    @VisibleForTesting
    WaitingForResources(
            Context context,
            Logger log,
            ResourceCounter desiredResources,
            Duration initialResourceAllocationTimeout,
            Duration resourceStabilizationTimeout,
            Clock clock) {
        this.context = Preconditions.checkNotNull(context);
        this.log = Preconditions.checkNotNull(log);
        this.desiredResources = Preconditions.checkNotNull(desiredResources);
        this.resourceStabilizationTimeout =
                Preconditions.checkNotNull(resourceStabilizationTimeout);
        this.clock = clock;
        Preconditions.checkNotNull(initialResourceAllocationTimeout);

        Preconditions.checkArgument(
                !desiredResources.isEmpty(), "Desired resources must not be empty");

        Preconditions.checkArgument(
                !resourceStabilizationTimeout.isNegative(),
                "Resource stabilization timeout must not be negative");

        // since state transitions are not allowed in state constructors, schedule calls for later.
        if (!initialResourceAllocationTimeout.isNegative()) {
            resourceTimeoutFuture =
                    context.runIfState(
                            this, this::resourceTimeout, initialResourceAllocationTimeout);
        }
        context.runIfState(this, this::notifyNewResourcesAvailable, Duration.ZERO);
    }

    @Override
    public void onLeave(Class<? extends State> newState) {
        if (resourceTimeoutFuture != null) {
            resourceTimeoutFuture.cancel(false);
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

    @Override
    public void notifyNewResourcesAvailable() {
        checkDesiredOrSufficientResourcesAvailable();
    }

    private void checkDesiredOrSufficientResourcesAvailable() {
        if (context.hasDesiredResources(desiredResources)) {
            createExecutionGraphWithAvailableResources();
            return;
        }

        if (context.hasSufficientResources()) {
            if (resourceStabilizationDeadline == null) {
                resourceStabilizationDeadline =
                        Deadline.fromNowWithClock(resourceStabilizationTimeout, clock);
            }
            if (resourceStabilizationDeadline.isOverdue()) {
                createExecutionGraphWithAvailableResources();
            } else {
                // schedule next resource check
                context.runIfState(
                        this,
                        this::checkDesiredOrSufficientResourcesAvailable,
                        resourceStabilizationDeadline.timeLeft());
            }
        } else {
            // clear deadline due to insufficient resources
            resourceStabilizationDeadline = null;
        }
    }

    private void resourceTimeout() {
        log.debug(
                "Initial resource allocation timeout triggered: Creating ExecutionGraph with available resources.");
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
         * Checks whether we have the desired resources.
         *
         * @param desiredResources desiredResources describing the desired resources
         * @return {@code true} if we have enough resources; otherwise {@code false}
         */
        boolean hasDesiredResources(ResourceCounter desiredResources);

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
        private final ResourceCounter desiredResources;
        private final Duration initialResourceAllocationTimeout;
        private final Duration resourceStabilizationTimeout;

        public Factory(
                Context context,
                Logger log,
                ResourceCounter desiredResources,
                Duration initialResourceAllocationTimeout,
                Duration resourceStabilizationTimeout) {
            this.context = context;
            this.log = log;
            this.desiredResources = desiredResources;
            this.initialResourceAllocationTimeout = initialResourceAllocationTimeout;
            this.resourceStabilizationTimeout = resourceStabilizationTimeout;
        }

        public Class<WaitingForResources> getStateClass() {
            return WaitingForResources.class;
        }

        public WaitingForResources getState() {
            return new WaitingForResources(
                    context,
                    log,
                    desiredResources,
                    initialResourceAllocationTimeout,
                    resourceStabilizationTimeout);
        }
    }
}
