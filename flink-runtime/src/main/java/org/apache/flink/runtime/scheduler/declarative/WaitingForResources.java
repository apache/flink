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

package org.apache.flink.runtime.scheduler.declarative;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobmaster.slotpool.ResourceCounter;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;

/**
 * State which describes that the scheduler is waiting for resources in order to execute the job.
 */
class WaitingForResources implements State, ResourceConsumer {

    private final Context context;

    private final Logger logger;

    private final ResourceCounter desiredResources;

    WaitingForResources(Context context, Logger logger, ResourceCounter desiredResources) {
        this.context = context;
        this.logger = logger;
        this.desiredResources = desiredResources;
    }

    @Override
    public void onEnter() {
        context.runIfState(this, this::resourceTimeout, Duration.ofSeconds(10L));
        notifyNewResourcesAvailable();
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
        return logger;
    }

    @Override
    public void notifyNewResourcesAvailable() {
        if (context.hasEnoughResources(desiredResources)) {
            createExecutionGraphWithAvailableResources();
        }
    }

    private void resourceTimeout() {
        createExecutionGraphWithAvailableResources();
    }

    private void createExecutionGraphWithAvailableResources() {
        try {
            final ExecutionGraph executionGraph =
                    context.createExecutionGraphWithAvailableResources();

            context.goToExecuting(executionGraph);
        } catch (Exception exception) {
            logger.error("handling initialization failure", exception);
            context.goToFinished(context.getArchivedExecutionGraph(JobStatus.FAILED, exception));
        }
    }

    /** Context of the {@link WaitingForResources} state. */
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
         * Creates an {@link ExecutionGraph} with the available resources.
         *
         * @return the created {@link ExecutionGraph}
         * @throws Exception if the creation of the {@link ExecutionGraph} fails
         */
        ExecutionGraph createExecutionGraphWithAvailableResources() throws Exception;

        /**
         * Runs the given action after a delay if the state at this time equals the expected state.
         *
         * @param expectedState expectedState describes the required state at the time of running
         *     the action
         * @param action action to run if the expected state equals the actual state
         * @param delay delay after which to run the action
         */
        void runIfState(State expectedState, Runnable action, Duration delay);
    }
}
