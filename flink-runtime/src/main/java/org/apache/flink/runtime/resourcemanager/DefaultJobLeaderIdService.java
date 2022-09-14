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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalListener;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Service which retrieves for a registered job the current job leader id (the leader id of the job
 * manager responsible for the job). The leader id will be exposed as a future via the {@link
 * #getLeaderId(JobID)}. The future will only be completed with an exception in case the service
 * will be stopped.
 */
public class DefaultJobLeaderIdService implements JobLeaderIdService {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultJobLeaderIdService.class);

    /** High availability services to use by this service. */
    private final HighAvailabilityServices highAvailabilityServices;

    private final ScheduledExecutor scheduledExecutor;

    private final Time jobTimeout;

    /** Map of currently monitored jobs. */
    private final Map<JobID, JobLeaderIdListener> jobLeaderIdListeners;

    /** Actions to call when the job leader changes. */
    private JobLeaderIdActions jobLeaderIdActions;

    public DefaultJobLeaderIdService(
            HighAvailabilityServices highAvailabilityServices,
            ScheduledExecutor scheduledExecutor,
            Time jobTimeout) {
        this.highAvailabilityServices =
                Preconditions.checkNotNull(highAvailabilityServices, "highAvailabilityServices");
        this.scheduledExecutor = Preconditions.checkNotNull(scheduledExecutor, "scheduledExecutor");
        this.jobTimeout = Preconditions.checkNotNull(jobTimeout, "jobTimeout");

        jobLeaderIdListeners = new HashMap<>(4);

        jobLeaderIdActions = null;
    }

    @Override
    public void start(JobLeaderIdActions initialJobLeaderIdActions) throws Exception {
        if (isStarted()) {
            clear();
        }

        this.jobLeaderIdActions = Preconditions.checkNotNull(initialJobLeaderIdActions);
    }

    @Override
    public void stop() throws Exception {
        clear();

        this.jobLeaderIdActions = null;
    }

    /**
     * Checks whether the service has been started.
     *
     * @return True if the service has been started; otherwise false
     */
    public boolean isStarted() {
        return jobLeaderIdActions != null;
    }

    @Override
    public void clear() throws Exception {
        Exception exception = null;

        for (JobLeaderIdListener listener : jobLeaderIdListeners.values()) {
            try {
                listener.stop();
            } catch (Exception e) {
                exception = ExceptionUtils.firstOrSuppressed(e, exception);
            }
        }

        if (exception != null) {
            ExceptionUtils.rethrowException(
                    exception,
                    "Could not properly stop the "
                            + DefaultJobLeaderIdService.class.getSimpleName()
                            + '.');
        }

        jobLeaderIdListeners.clear();
    }

    @Override
    public void addJob(JobID jobId) throws Exception {
        Preconditions.checkNotNull(jobLeaderIdActions);

        LOG.debug("Add job {} to job leader id monitoring.", jobId);

        if (!jobLeaderIdListeners.containsKey(jobId)) {
            LeaderRetrievalService leaderRetrievalService =
                    highAvailabilityServices.getJobManagerLeaderRetriever(jobId);

            JobLeaderIdListener jobIdListener =
                    new JobLeaderIdListener(jobId, jobLeaderIdActions, leaderRetrievalService);
            jobLeaderIdListeners.put(jobId, jobIdListener);
        }
    }

    @Override
    public void removeJob(JobID jobId) throws Exception {
        LOG.debug("Remove job {} from job leader id monitoring.", jobId);

        JobLeaderIdListener listener = jobLeaderIdListeners.remove(jobId);

        if (listener != null) {
            listener.stop();
        }
    }

    @Override
    public boolean containsJob(JobID jobId) {
        return jobLeaderIdListeners.containsKey(jobId);
    }

    @Override
    public CompletableFuture<JobMasterId> getLeaderId(JobID jobId) throws Exception {
        if (!jobLeaderIdListeners.containsKey(jobId)) {
            addJob(jobId);
        }

        JobLeaderIdListener listener = jobLeaderIdListeners.get(jobId);

        return listener.getLeaderIdFuture().thenApply(JobMasterId::fromUuidOrNull);
    }

    @Override
    public boolean isValidTimeout(JobID jobId, UUID timeoutId) {
        JobLeaderIdListener jobLeaderIdListener = jobLeaderIdListeners.get(jobId);

        if (null != jobLeaderIdListener) {
            return Objects.equals(timeoutId, jobLeaderIdListener.getTimeoutId());
        } else {
            return false;
        }
    }

    // --------------------------------------------------------------------------------
    // Static utility classes
    // --------------------------------------------------------------------------------

    /**
     * Listener which stores the current leader id and exposes them as a future value when
     * requested. The returned future will always be completed properly except when stopping the
     * listener.
     */
    private final class JobLeaderIdListener implements LeaderRetrievalListener {
        private final Object timeoutLock = new Object();
        private final JobID jobId;
        private final JobLeaderIdActions listenerJobLeaderIdActions;
        private final LeaderRetrievalService leaderRetrievalService;

        private volatile CompletableFuture<UUID> leaderIdFuture;
        private volatile boolean running = true;

        /** Null if no timeout has been scheduled; otherwise non null. */
        @Nullable private volatile ScheduledFuture<?> timeoutFuture;

        /** Null if no timeout has been scheduled; otherwise non null. */
        @Nullable private volatile UUID timeoutId;

        private JobLeaderIdListener(
                JobID jobId,
                JobLeaderIdActions listenerJobLeaderIdActions,
                LeaderRetrievalService leaderRetrievalService)
                throws Exception {
            this.jobId = Preconditions.checkNotNull(jobId);
            this.listenerJobLeaderIdActions =
                    Preconditions.checkNotNull(listenerJobLeaderIdActions);
            this.leaderRetrievalService = Preconditions.checkNotNull(leaderRetrievalService);

            leaderIdFuture = new CompletableFuture<>();

            activateTimeout();

            // start the leader service we're listening to
            leaderRetrievalService.start(this);
        }

        public CompletableFuture<UUID> getLeaderIdFuture() {
            return leaderIdFuture;
        }

        @Nullable
        public UUID getTimeoutId() {
            return timeoutId;
        }

        public void stop() throws Exception {
            running = false;
            leaderRetrievalService.stop();
            cancelTimeout();
            leaderIdFuture.completeExceptionally(
                    new Exception("Job leader id service has been stopped."));
        }

        @Override
        public void notifyLeaderAddress(
                @Nullable String leaderAddress, @Nullable UUID leaderSessionId) {
            if (running) {
                UUID previousJobLeaderId = null;

                if (leaderIdFuture.isDone()) {
                    try {
                        previousJobLeaderId = leaderIdFuture.getNow(null);
                    } catch (CompletionException e) {
                        // this should never happen since we complete this future always properly
                        handleError(e);
                    }

                    if (leaderSessionId == null) {
                        // there was a leader, but we no longer have one
                        LOG.debug("Job {} no longer has a job leader.", jobId);
                        leaderIdFuture = new CompletableFuture<>();
                    } else {
                        // there was an active leader, but we now have a new leader
                        LOG.debug(
                                "Job {} has a new job leader {}@{}.",
                                jobId,
                                leaderSessionId,
                                leaderAddress);
                        leaderIdFuture = CompletableFuture.completedFuture(leaderSessionId);
                    }
                } else {
                    if (leaderSessionId != null) {
                        // there was no active leader, but we now have a new leader
                        LOG.debug(
                                "Job {} has a new job leader {}@{}.",
                                jobId,
                                leaderSessionId,
                                leaderAddress);
                        leaderIdFuture.complete(leaderSessionId);
                    }
                }

                if (previousJobLeaderId != null && !previousJobLeaderId.equals(leaderSessionId)) {
                    // we had a previous job leader, so notify about his lost leadership
                    listenerJobLeaderIdActions.jobLeaderLostLeadership(
                            jobId, new JobMasterId(previousJobLeaderId));

                    if (null == leaderSessionId) {
                        // No current leader active ==> Set a timeout for the job
                        activateTimeout();

                        // check if we got stopped asynchronously
                        if (!running) {
                            cancelTimeout();
                        }
                    }
                } else if (null != leaderSessionId) {
                    // Cancel timeout because we've found an active leader for it
                    cancelTimeout();
                }
            } else {
                LOG.debug(
                        "A leader id change {}@{} has been detected after the listener has been stopped.",
                        leaderSessionId,
                        leaderAddress);
            }
        }

        @Override
        public void handleError(Exception exception) {
            if (running) {
                listenerJobLeaderIdActions.handleError(exception);
            } else {
                LOG.debug(
                        "An error occurred in the {} after the listener has been stopped.",
                        JobLeaderIdListener.class.getSimpleName(),
                        exception);
            }
        }

        private void activateTimeout() {
            synchronized (timeoutLock) {
                cancelTimeout();

                final UUID newTimeoutId = UUID.randomUUID();

                timeoutId = newTimeoutId;
                timeoutFuture =
                        scheduledExecutor.schedule(
                                new Runnable() {
                                    @Override
                                    public void run() {
                                        listenerJobLeaderIdActions.notifyJobTimeout(
                                                jobId, newTimeoutId);
                                    }
                                },
                                jobTimeout.toMilliseconds(),
                                TimeUnit.MILLISECONDS);
            }
        }

        private void cancelTimeout() {
            synchronized (timeoutLock) {
                if (timeoutFuture != null) {
                    timeoutFuture.cancel(true);
                }

                timeoutFuture = null;
                timeoutId = null;
            }
        }
    }
}
