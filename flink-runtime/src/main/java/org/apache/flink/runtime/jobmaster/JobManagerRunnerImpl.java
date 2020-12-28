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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.client.JobInitializationException;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * The runner for the job manager. It deals with job level leader election and make underlying job
 * manager properly reacted.
 */
public class JobManagerRunnerImpl
        implements LeaderContender, OnCompletionActions, JobManagerRunner {

    private static final Logger log = LoggerFactory.getLogger(JobManagerRunnerImpl.class);

    // ------------------------------------------------------------------------

    /**
     * Lock to ensure that this runner can deal with leader election event and job completion
     * notifies simultaneously.
     */
    private final Object lock = new Object();

    /** The job graph needs to run. */
    private final JobGraph jobGraph;

    /** Used to check whether a job needs to be run. */
    private final RunningJobsRegistry runningJobsRegistry;

    /** Leader election for this job. */
    private final LeaderElectionService leaderElectionService;

    private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

    private final Executor executor;

    private final FatalErrorHandler fatalErrorHandler;

    private final CompletableFuture<JobManagerRunnerResult> resultFuture;

    private final CompletableFuture<Void> terminationFuture;

    private final JobMasterServiceFactory jobMasterServiceFactory;

    private final ClassLoader userCodeClassLoader;

    private final long initializationTimestamp;

    private CompletableFuture<Void> leadershipOperation;

    @Nullable private JobMasterService jobMasterService;

    /** flag marking the runner as shut down. */
    private volatile boolean shutdown;

    private volatile CompletableFuture<JobMasterGateway> leaderGatewayFuture;

    // ------------------------------------------------------------------------

    /**
     * Exceptions that occur while creating the JobManager or JobManagerRunnerImpl are directly
     * thrown and not reported to the given {@code FatalErrorHandler}.
     *
     * @throws Exception Thrown if the runner cannot be set up, because either one of the required
     *     services could not be started, or the Job could not be initialized.
     */
    public JobManagerRunnerImpl(
            final JobGraph jobGraph,
            final JobMasterServiceFactory jobMasterServiceFactory,
            final HighAvailabilityServices haServices,
            final LibraryCacheManager.ClassLoaderLease classLoaderLease,
            final Executor executor,
            final FatalErrorHandler fatalErrorHandler,
            long initializationTimestamp)
            throws Exception {

        this.resultFuture = new CompletableFuture<>();
        this.terminationFuture = new CompletableFuture<>();
        this.leadershipOperation = CompletableFuture.completedFuture(null);

        this.jobGraph = checkNotNull(jobGraph);
        this.jobMasterServiceFactory = checkNotNull(jobMasterServiceFactory);
        this.classLoaderLease = checkNotNull(classLoaderLease);
        this.executor = checkNotNull(executor);
        this.fatalErrorHandler = checkNotNull(fatalErrorHandler);
        this.initializationTimestamp = initializationTimestamp;

        checkArgument(jobGraph.getNumberOfVertices() > 0, "The given job is empty");

        // libraries and class loader first
        try {
            userCodeClassLoader =
                    classLoaderLease
                            .getOrResolveClassLoader(
                                    jobGraph.getUserJarBlobKeys(), jobGraph.getClasspaths())
                            .asClassLoader();
        } catch (IOException e) {
            throw new Exception("Cannot set up the user code libraries: " + e.getMessage(), e);
        }

        // high availability services next
        this.runningJobsRegistry = haServices.getRunningJobsRegistry();
        this.leaderElectionService =
                haServices.getJobManagerLeaderElectionService(jobGraph.getJobID());

        this.leaderGatewayFuture = new CompletableFuture<>();
    }

    // ----------------------------------------------------------------------------------------------
    // Getter
    // ----------------------------------------------------------------------------------------------

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
        return leaderGatewayFuture;
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public JobID getJobID() {
        return jobGraph.getJobID();
    }

    // ----------------------------------------------------------------------------------------------
    // Lifecycle management
    // ----------------------------------------------------------------------------------------------

    @Override
    public void start() throws Exception {
        try {
            leaderElectionService.start(this);
        } catch (Exception e) {
            log.error(
                    "Could not start the JobManager because the leader election service did not start.",
                    e);
            throw new Exception("Could not start the leader election service.", e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        synchronized (lock) {
            if (!shutdown) {
                shutdown = true;

                setNewLeaderGatewayFuture();
                leaderGatewayFuture.completeExceptionally(
                        new FlinkException("JobMaster has been shut down."));

                final CompletableFuture<Void> jobManagerTerminationFuture;

                if (jobMasterService == null) {
                    jobManagerTerminationFuture = FutureUtils.completedVoidFuture();
                } else {
                    jobManagerTerminationFuture = jobMasterService.closeAsync();
                }

                jobManagerTerminationFuture.whenComplete(
                        (Void ignored, Throwable throwable) -> {
                            try {
                                leaderElectionService.stop();
                            } catch (Throwable t) {
                                throwable =
                                        ExceptionUtils.firstOrSuppressed(
                                                t,
                                                ExceptionUtils.stripCompletionException(throwable));
                            }

                            classLoaderLease.release();

                            resultFuture.complete(JobManagerRunnerResult.forJobNotFinished());

                            if (throwable != null) {
                                terminationFuture.completeExceptionally(
                                        new FlinkException(
                                                "Could not properly shut down the JobManagerRunner",
                                                throwable));
                            } else {
                                terminationFuture.complete(null);
                            }
                        });
            }

            return terminationFuture;
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Result and error handling methods
    // ----------------------------------------------------------------------------------------------

    /** Job completion notification triggered by JobManager. */
    @Override
    public void jobReachedGloballyTerminalState(ArchivedExecutionGraph executionGraph) {
        unregisterJobFromHighAvailability();
        // complete the result future with the terminal execution graph
        resultFuture.complete(JobManagerRunnerResult.forSuccess(executionGraph));
    }

    /** Job completion notification triggered by self. */
    @Override
    public void jobFinishedByOther() {
        resultFuture.complete(JobManagerRunnerResult.forJobNotFinished());
    }

    @Override
    public void jobMasterFailed(Throwable cause) {
        handleJobManagerRunnerError(cause);
    }

    private void handleJobManagerRunnerError(Throwable cause) {
        if (ExceptionUtils.isJvmFatalError(cause)) {
            fatalErrorHandler.onFatalError(cause);
        } else {
            resultFuture.completeExceptionally(cause);
        }
    }

    /**
     * Marks this runner's job as not running. Other JobManager will not recover the job after this
     * call.
     *
     * <p>This method never throws an exception.
     */
    private void unregisterJobFromHighAvailability() {
        try {
            runningJobsRegistry.setJobFinished(jobGraph.getJobID());
        } catch (Throwable t) {
            log.error(
                    "Could not un-register from high-availability services job {} ({})."
                            + "Other JobManager's may attempt to recover it and re-execute it.",
                    jobGraph.getName(),
                    jobGraph.getJobID(),
                    t);
        }
    }

    // ----------------------------------------------------------------------------------------------
    // Leadership methods
    // ----------------------------------------------------------------------------------------------

    @Override
    public void grantLeadership(final UUID leaderSessionID) {
        synchronized (lock) {
            if (shutdown) {
                log.debug(
                        "JobManagerRunner cannot be granted leadership because it is already shut down.");
                return;
            }

            leadershipOperation =
                    leadershipOperation.thenRun(
                            ThrowingRunnable.unchecked(
                                    () -> {
                                        synchronized (lock) {
                                            verifyJobSchedulingStatusAndStartJobManager(
                                                    leaderSessionID);
                                        }
                                    }));

            handleException(leadershipOperation, "Could not start the job manager.");
        }
    }

    @GuardedBy("lock")
    private void verifyJobSchedulingStatusAndStartJobManager(UUID leaderSessionId)
            throws FlinkException {
        if (shutdown) {
            log.debug("Ignoring starting JobMaster because JobManagerRunner is already shut down.");
            return;
        }

        final RunningJobsRegistry.JobSchedulingStatus jobSchedulingStatus =
                getJobSchedulingStatus();

        if (jobSchedulingStatus == RunningJobsRegistry.JobSchedulingStatus.DONE) {
            jobAlreadyDone();
        } else {
            startJobMaster(leaderSessionId);
        }
    }

    @GuardedBy("lock")
    private void startJobMaster(UUID leaderSessionId) throws FlinkException {
        log.info(
                "JobManager runner for job {} ({}) was granted leadership with session id {}.",
                jobGraph.getName(),
                jobGraph.getJobID(),
                leaderSessionId);

        try {
            runningJobsRegistry.setJobRunning(jobGraph.getJobID());
        } catch (IOException e) {
            throw new FlinkException(
                    String.format(
                            "Failed to set the job %s to running in the running jobs registry.",
                            jobGraph.getJobID()),
                    e);
        }

        startJobMasterServiceSafely(leaderSessionId);

        if (jobMasterService != null) {
            confirmLeaderSessionIdIfStillLeader(jobMasterService, leaderSessionId);
        }
    }

    private void startJobMasterServiceSafely(UUID leaderSessionId) {
        checkState(jobMasterService == null, "JobMasterService must be null before being started.");

        try {
            final JobMasterService newJobMasterService =
                    jobMasterServiceFactory.createJobMasterService(
                            jobGraph,
                            new JobMasterId(leaderSessionId),
                            this,
                            userCodeClassLoader,
                            initializationTimestamp);

            jobMasterService = newJobMasterService;

            jobMasterService
                    .getTerminationFuture()
                    .whenComplete(
                            (unused, throwable) -> {
                                if (throwable != null) {
                                    synchronized (lock) {
                                        // check that we are still running and the JobMasterService
                                        // is still valid
                                        if (!shutdown && newJobMasterService == jobMasterService) {
                                            handleJobManagerRunnerError(throwable);
                                        }
                                    }
                                }
                            });
        } catch (Exception e) {
            resultFuture.complete(
                    JobManagerRunnerResult.forInitializationFailure(
                            new JobInitializationException(
                                    jobGraph.getJobID(), "Could not start the JobMaster.", e)));
        }
    }

    private void jobAlreadyDone() {
        log.info("Granted leader ship but job {} has been finished. ", jobGraph.getJobID());
        jobFinishedByOther();
    }

    private RunningJobsRegistry.JobSchedulingStatus getJobSchedulingStatus() throws FlinkException {
        try {
            return runningJobsRegistry.getJobSchedulingStatus(jobGraph.getJobID());
        } catch (IOException e) {
            throw new FlinkException(
                    String.format(
                            "Could not retrieve the job scheduling status for job %s.",
                            jobGraph.getJobID()),
                    e);
        }
    }

    private void confirmLeaderSessionIdIfStillLeader(
            JobMasterService jobMasterService, UUID leaderSessionId) {

        if (leaderElectionService.hasLeadership(leaderSessionId)) {
            leaderGatewayFuture.complete(jobMasterService.getGateway());
            leaderElectionService.confirmLeadership(leaderSessionId, jobMasterService.getAddress());
        } else {
            log.debug(
                    "Ignoring confirmation of leader session id because {} is no longer the leader.",
                    getDescription());
        }
    }

    @Override
    public void revokeLeadership() {
        synchronized (lock) {
            if (shutdown) {
                log.debug(
                        "Ignoring revoking leadership because JobManagerRunner is already shut down.");
                return;
            }

            leadershipOperation =
                    leadershipOperation.thenCompose(
                            (ignored) -> {
                                synchronized (lock) {
                                    return revokeJobMasterLeadership();
                                }
                            });

            handleException(leadershipOperation, "Could not suspend the job manager.");
        }
    }

    @GuardedBy("lock")
    private CompletableFuture<Void> revokeJobMasterLeadership() {
        if (shutdown) {
            log.debug(
                    "Ignoring revoking JobMaster leadership because JobManagerRunner is already shut down.");
            return FutureUtils.completedVoidFuture();
        }

        if (jobMasterService != null) {
            log.info(
                    "JobManager for job {} ({}) at {} was revoked leadership.",
                    jobGraph.getName(),
                    jobGraph.getJobID(),
                    jobMasterService.getAddress());

            setNewLeaderGatewayFuture();

            final CompletableFuture<Void> jobMasterServiceTerminationFuture =
                    jobMasterService.closeAsync();
            jobMasterService = null;

            return jobMasterServiceTerminationFuture;
        } else {
            return FutureUtils.completedVoidFuture();
        }
    }

    private void handleException(CompletableFuture<Void> leadershipOperation, String message) {
        leadershipOperation.whenComplete(
                (ignored, throwable) -> {
                    if (throwable != null) {
                        handleJobManagerRunnerError(new FlinkException(message, throwable));
                    }
                });
    }

    @GuardedBy("lock")
    private void setNewLeaderGatewayFuture() {
        final CompletableFuture<JobMasterGateway> oldLeaderGatewayFuture = leaderGatewayFuture;

        leaderGatewayFuture = new CompletableFuture<>();

        if (!oldLeaderGatewayFuture.isDone()) {
            leaderGatewayFuture.whenComplete(
                    (JobMasterGateway jobMasterGateway, Throwable throwable) -> {
                        if (throwable != null) {
                            oldLeaderGatewayFuture.completeExceptionally(throwable);
                        } else {
                            oldLeaderGatewayFuture.complete(jobMasterGateway);
                        }
                    });
        }
    }

    @Override
    public void handleError(Exception exception) {
        log.error("Leader Election Service encountered a fatal error.", exception);
        handleJobManagerRunnerError(exception);
    }
}
