/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.application;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.dispatcher.Dispatcher;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

/** Base class for all applications. */
public abstract class AbstractApplication implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractApplication.class);

    private static final long serialVersionUID = 1L;

    private final ApplicationID applicationId;

    private ApplicationState applicationState;

    /**
     * Timestamps (in milliseconds as returned by {@code System.currentTimeMillis()}) when the
     * application transitioned into a certain status. The index into this array is the ordinal of
     * the enum value, i.e. the timestamp when the application went into state "RUNNING" is at
     * {@code timestamps[RUNNING.ordinal()]}.
     */
    private final long[] statusTimestamps;

    private final Set<JobID> jobs = new HashSet<>();

    public AbstractApplication(ApplicationID applicationId) {
        this.applicationId = applicationId;
        this.statusTimestamps = new long[ApplicationState.values().length];
        this.applicationState = ApplicationState.CREATED;
        this.statusTimestamps[ApplicationState.CREATED.ordinal()] = System.currentTimeMillis();
    }

    /**
     * Entry method to run the application asynchronously.
     *
     * <p>The returned CompletableFuture indicates that the execution request has been accepted and
     * the application transitions to RUNNING state.
     *
     * <p><b>Note:</b> This method must be called in the main thread of the {@link Dispatcher}.
     *
     * @param dispatcherGateway the dispatcher of the cluster to run the application.
     * @param scheduledExecutor the executor to run the user logic.
     * @param mainThreadExecutor the executor bound to the main thread.
     * @param errorHandler the handler for fatal errors.
     * @return a future indicating that the execution request has been accepted.
     */
    public abstract CompletableFuture<Acknowledge> execute(
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final Executor mainThreadExecutor,
            final FatalErrorHandler errorHandler);

    /**
     * Cancels the application execution.
     *
     * <p>This method is responsible for initiating the cancellation process and handling the
     * appropriate state transitions of the application.
     *
     * <p><b>Note:</b> This method must be called in the main thread of the {@link Dispatcher}.
     */
    public abstract void cancel();

    /**
     * Cleans up execution associated with the application.
     *
     * <p>This method is typically invoked when the cluster is shutting down.
     */
    public abstract void dispose();

    public abstract String getName();

    public ApplicationID getApplicationId() {
        return applicationId;
    }

    public Set<JobID> getJobs() {
        return Collections.unmodifiableSet(jobs);
    }

    /**
     * Adds a job ID to the jobs set.
     *
     * <p><b>Note:</b>This method must be called in the main thread of the {@link Dispatcher}.
     */
    public boolean addJob(JobID jobId) {
        return jobs.add(jobId);
    }

    public ApplicationState getApplicationStatus() {
        return applicationState;
    }

    // ------------------------------------------------------------------------
    //  State Transitions
    // ------------------------------------------------------------------------

    private static final Map<ApplicationState, Set<ApplicationState>> ALLOWED_TRANSITIONS;

    static {
        ALLOWED_TRANSITIONS = new EnumMap<>(ApplicationState.class);
        ALLOWED_TRANSITIONS.put(
                ApplicationState.CREATED,
                new HashSet<>(Arrays.asList(ApplicationState.RUNNING, ApplicationState.CANCELING)));
        ALLOWED_TRANSITIONS.put(
                ApplicationState.RUNNING,
                new HashSet<>(
                        Arrays.asList(
                                ApplicationState.FINISHED,
                                ApplicationState.FAILING,
                                ApplicationState.CANCELING)));
        ALLOWED_TRANSITIONS.put(
                ApplicationState.FAILING,
                new HashSet<>(Collections.singletonList(ApplicationState.FAILED)));
        ALLOWED_TRANSITIONS.put(
                ApplicationState.CANCELING,
                new HashSet<>(Collections.singletonList(ApplicationState.CANCELED)));
    }

    /** All state transition methods must be called in the main thread. */
    public void transitionToRunning() {
        transitionState(ApplicationState.RUNNING);
    }

    /** All state transition methods must be called in the main thread. */
    public void transitionToCanceling() {
        transitionState(ApplicationState.CANCELING);
    }

    /** All state transition methods must be called in the main thread. */
    public void transitionToFailing() {
        transitionState(ApplicationState.FAILING);
    }

    /** All state transition methods must be called in the main thread. */
    public void transitionToFailed() {
        transitionState(ApplicationState.FAILED);
    }

    /** All state transition methods must be called in the main thread. */
    public void transitionToFinished() {
        transitionState(ApplicationState.FINISHED);
    }

    /** All state transition methods must be called in the main thread. */
    public void transitionToCanceled() {
        transitionState(ApplicationState.CANCELED);
    }

    void transitionState(ApplicationState targetState) {
        validateTransition(targetState);
        LOG.info(
                "Application {} ({}) switched from state {} to {}.",
                getName(),
                getApplicationId(),
                applicationState,
                targetState);
        this.statusTimestamps[targetState.ordinal()] = System.currentTimeMillis();
        this.applicationState = targetState;
    }

    private void validateTransition(ApplicationState targetState) {
        Set<ApplicationState> allowedTransitions = ALLOWED_TRANSITIONS.get(applicationState);
        if (allowedTransitions == null || !allowedTransitions.contains(targetState)) {
            throw new IllegalStateException(
                    String.format(
                            "Invalid transition from %s to %s", applicationState, targetState));
        }
    }

    public long getStatusTimestamp(ApplicationState status) {
        return this.statusTimestamps[status.ordinal()];
    }
}
