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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.runtime.dispatcher.JobCancellationFailedException;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;

import java.time.Duration;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Supplier;

/**
 * Leadership runner for the {@link JobMasterServiceProcess}.
 *
 * <p>The responsibility of this component is to manage the leadership of the {@link
 * JobMasterServiceProcess}. This means that the runner will create an instance of the process when
 * it obtains the leadership. The process is stopped once the leadership is revoked.
 *
 * <p>This component only accepts signals (job result completion, initialization failure) as long as
 * it is running and as long as the signals are coming from the current leader process. This ensures
 * that only the current leader can affect this component.
 *
 * <p>All leadership operations are serialized. This means that granting the leadership has to
 * complete before the leadership can be revoked and vice versa.
 *
 * <p>The {@link #resultFuture} can be completed with the following values:
 *
 * <ul>
 *   <li>{@link JobManagerRunnerResult} to signal an initialization failure of the {@link
 *       JobMasterService}, the completion of a job, or a globally terminal result observed before
 *       leadership revocation could be forwarded
 *   <li>{@link Exception} to signal an unexpected failure
 * </ul>
 */
public class JobMasterServiceLeadershipRunner implements JobManagerRunner, LeaderContender {

    private static final Logger LOG =
            LoggerFactory.getLogger(JobMasterServiceLeadershipRunner.class);

    private final Object lock = new Object();

    private final JobMasterServiceProcessFactory jobMasterServiceProcessFactory;

    private final LeaderElection leaderElection;

    private final JobResultStore jobResultStore;

    private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

    private final FatalErrorHandler fatalErrorHandler;

    private final CompletableFuture<Void> terminationFuture = new CompletableFuture<>();

    private final CompletableFuture<JobManagerRunnerResult> resultFuture =
            new CompletableFuture<>();

    @GuardedBy("lock")
    private State state = State.RUNNING;

    @GuardedBy("lock")
    private CompletableFuture<Void> sequentialOperation = FutureUtils.completedVoidFuture();

    @GuardedBy("lock")
    private JobMasterServiceProcess jobMasterServiceProcess =
            JobMasterServiceProcess.waitingForLeadership();

    @GuardedBy("lock")
    private CompletableFuture<JobMasterGateway> jobMasterGatewayFuture = new CompletableFuture<>();

    @GuardedBy("lock")
    private boolean hasCurrentLeaderBeenCancelled = false;

    @GuardedBy("lock")
    private UUID currentJobMasterServiceProcessLeaderId = null;

    /**
     * Caches a globally terminal result the moment it is observed, to close the race between such a
     * result and a leadership revocation that strips the forwarded result (see FLINK-39704). The
     * cache is populated by {@link #cacheGloballyTerminalResultIfCurrentProcess}, drained by either
     * {@link #grantLeadership} (on re-grant) or {@link #completeResultFutureAfterClose} (on close),
     * and cleared by {@link #onJobCompletion} when forwarding succeeds normally.
     */
    @GuardedBy("lock")
    private JobManagerRunnerResult pendingTerminalResult = null;

    public JobMasterServiceLeadershipRunner(
            JobMasterServiceProcessFactory jobMasterServiceProcessFactory,
            LeaderElection leaderElection,
            JobResultStore jobResultStore,
            LibraryCacheManager.ClassLoaderLease classLoaderLease,
            FatalErrorHandler fatalErrorHandler) {
        this.jobMasterServiceProcessFactory = jobMasterServiceProcessFactory;
        this.leaderElection = leaderElection;
        this.jobResultStore = jobResultStore;
        this.classLoaderLease = classLoaderLease;
        this.fatalErrorHandler = fatalErrorHandler;
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        final CompletableFuture<Void> processTerminationFuture;
        synchronized (lock) {
            if (state == State.STOPPED) {
                return terminationFuture;
            }

            state = State.STOPPED;

            LOG.debug("Terminating the leadership runner for job {}.", getJobID());

            jobMasterGatewayFuture.completeExceptionally(
                    new FlinkException(
                            "JobMasterServiceLeadershipRunner is closed. Therefore, the corresponding JobMaster will never acquire the leadership."));

            processTerminationFuture = jobMasterServiceProcess.closeAsync();
        }

        processTerminationFuture.whenComplete(
                (ignored, throwable) -> completeResultFutureAfterClose());

        final CompletableFuture<Void> serviceTerminationFuture =
                FutureUtils.runAfterwards(
                        processTerminationFuture,
                        () -> {
                            classLoaderLease.release();
                            leaderElection.close();
                        });

        FutureUtils.forward(serviceTerminationFuture, terminationFuture);

        terminationFuture.whenComplete(
                (unused, throwable) ->
                        LOG.debug("Leadership runner for job {} has been terminated.", getJobID()));
        return terminationFuture;
    }

    @Override
    public void start() throws Exception {
        LOG.debug("Start leadership runner for job {}.", getJobID());
        leaderElection.startLeaderElection(this);
    }

    @Override
    public CompletableFuture<JobMasterGateway> getJobMasterGateway() {
        synchronized (lock) {
            return jobMasterGatewayFuture;
        }
    }

    @Override
    public CompletableFuture<JobManagerRunnerResult> getResultFuture() {
        return resultFuture;
    }

    @Override
    public JobID getJobID() {
        return jobMasterServiceProcessFactory.getJobId();
    }

    @Override
    public CompletableFuture<Acknowledge> cancel(Duration timeout) {
        synchronized (lock) {
            hasCurrentLeaderBeenCancelled = true;
            return getJobMasterGateway()
                    .thenCompose(jobMasterGateway -> jobMasterGateway.cancel(timeout))
                    .exceptionally(
                            e -> {
                                throw new CompletionException(
                                        new JobCancellationFailedException(
                                                "Cancellation failed.",
                                                ExceptionUtils.stripCompletionException(e)));
                            });
        }
    }

    @Override
    public CompletableFuture<JobStatus> requestJobStatus(Duration timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                executionGraphInfo.getArchivedExecutionGraph().getState());
    }

    @Override
    public CompletableFuture<JobDetails> requestJobDetails(Duration timeout) {
        return requestJob(timeout).thenApply(JobDetails::createDetailsForJob);
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Duration timeout) {
        synchronized (lock) {
            if (state == State.RUNNING) {
                if (jobMasterServiceProcess.isInitializedAndRunning()) {
                    return getJobMasterGateway()
                            .thenCompose(jobMasterGateway -> jobMasterGateway.requestJob(timeout));
                } else {
                    return CompletableFuture.completedFuture(
                            createExecutionGraphInfoWithJobStatus(
                                    hasCurrentLeaderBeenCancelled
                                            ? JobStatus.CANCELLING
                                            : JobStatus.INITIALIZING));
                }
            } else {
                return resultFuture.thenApply(JobManagerRunnerResult::getExecutionGraphInfo);
            }
        }
    }

    @Override
    public boolean isInitialized() {
        synchronized (lock) {
            return jobMasterServiceProcess.isInitializedAndRunning();
        }
    }

    @Override
    public void grantLeadership(UUID leaderSessionID) {
        runIfStateRunning(
                () -> startJobMasterServiceProcessAsync(leaderSessionID),
                "starting a new JobMasterServiceProcess");
    }

    @GuardedBy("lock")
    private void startJobMasterServiceProcessAsync(UUID leaderSessionId) {
        sequentialOperation =
                sequentialOperation.thenCompose(
                        unused ->
                                handleCachedGloballyTerminalJobOrStartNewProcessAsync(
                                        leaderSessionId));
        handleAsyncOperationError(sequentialOperation, "Could not start the job manager.");
    }

    private CompletableFuture<Void> handleCachedGloballyTerminalJobOrStartNewProcessAsync(
            UUID leaderSessionId) {
        final JobManagerRunnerResult cachedTerminalResult;
        synchronized (lock) {
            if (!isRunning()) {
                return FutureUtils.completedVoidFuture();
            }
            cachedTerminalResult = takePendingTerminalResult();
            if (cachedTerminalResult != null) {
                state = State.JOB_COMPLETED;
                // resetting leader ID to handle concurrent
                // cacheGloballyTerminalResultIfCurrentProcess call from updating the
                // JobManagerResult
                currentJobMasterServiceProcessLeaderId = null;
            }
        }

        if (cachedTerminalResult != null) {
            LOG.info(
                    "Flushing previously observed globally terminal result for job {} on re-grant; not starting a new {}. Job state: {}.",
                    getJobID(),
                    JobMasterServiceProcess.class.getSimpleName(),
                    cachedTerminalResult
                            .getExecutionGraphInfo()
                            .getArchivedExecutionGraph()
                            .getState());
            resultFuture.complete(cachedTerminalResult);
            return FutureUtils.completedVoidFuture();
        }

        return jobResultStore
                .hasJobResultEntryAsync(getJobID())
                .thenCompose(
                        hasJobResult ->
                                hasJobResult
                                        ? handleJobAlreadyDoneIfValidLeader(leaderSessionId)
                                        : createNewJobMasterServiceProcessIfValidLeader(
                                                leaderSessionId));
    }

    @GuardedBy("lock")
    @Nullable
    private JobManagerRunnerResult takePendingTerminalResult() {
        final JobManagerRunnerResult terminalResult = pendingTerminalResult;
        pendingTerminalResult = null;
        return terminalResult;
    }

    private void completeResultFutureAfterClose() {
        JobManagerRunnerResult closeResult;
        synchronized (lock) {
            closeResult = takePendingTerminalResult();
            if (closeResult == null) {
                closeResult =
                        JobManagerRunnerResult.forSuccess(
                                createExecutionGraphInfoWithJobStatus(JobStatus.SUSPENDED));
            }
            // resetting leader ID to handle concurrent cacheGloballyTerminalResultIfCurrentProcess
            // call from updating the JobManagerResult
            currentJobMasterServiceProcessLeaderId = null;
        }

        if (isGloballyTerminalResult(closeResult)) {
            LOG.info(
                    "Flushing globally terminal result for job {} during close. Job state: {}.",
                    getJobID(),
                    closeResult.getExecutionGraphInfo().getArchivedExecutionGraph().getState());
        }
        resultFuture.complete(closeResult);
    }

    private CompletableFuture<Void> handleJobAlreadyDoneIfValidLeader(UUID leaderSessionId) {
        return runIfValidLeader(
                leaderSessionId, () -> jobAlreadyDone(leaderSessionId), "check completed job");
    }

    private CompletableFuture<Void> createNewJobMasterServiceProcessIfValidLeader(
            UUID leaderSessionId) {
        return runIfValidLeader(
                leaderSessionId,
                () ->
                        // the heavy lifting of the JobMasterServiceProcess instantiation is still
                        // done asynchronously (see
                        // DefaultJobMasterServiceFactory#createJobMasterService executing the logic
                        // on the leaderOperation thread in the DefaultLeaderElectionService should
                        // be, therefore, fine
                        ThrowingRunnable.unchecked(
                                        () -> createNewJobMasterServiceProcess(leaderSessionId))
                                .run(),
                "create new job master service process");
    }

    private void printLogIfNotValidLeader(String actionDescription, UUID leaderSessionId) {
        LOG.debug(
                "Ignore leader action '{}' because the leadership runner is no longer the valid leader for {}.",
                actionDescription,
                leaderSessionId);
    }

    private ExecutionGraphInfo createExecutionGraphInfoWithJobStatus(JobStatus jobStatus) {
        return new ExecutionGraphInfo(
                jobMasterServiceProcessFactory.createArchivedExecutionGraph(jobStatus, null));
    }

    private void jobAlreadyDone(UUID leaderSessionId) {
        LOG.info(
                "{} for job {} was granted leadership with leader id {}, but job was already done.",
                getClass().getSimpleName(),
                getJobID(),
                leaderSessionId);
        resultFuture.complete(
                JobManagerRunnerResult.forSuccess(
                        new ExecutionGraphInfo(
                                jobMasterServiceProcessFactory.createArchivedExecutionGraph(
                                        JobStatus.FAILED,
                                        new JobAlreadyDoneException(getJobID())))));
    }

    @GuardedBy("lock")
    private void createNewJobMasterServiceProcess(UUID leaderSessionId) {
        Preconditions.checkState(jobMasterServiceProcess.closeAsync().isDone());
        Preconditions.checkState(
                pendingTerminalResult == null,
                "No new JobMasterServiceProcess should be created while a terminal result is pending.");

        LOG.info(
                "{} for job {} was granted leadership with leader id {}. Creating new {}.",
                getClass().getSimpleName(),
                getJobID(),
                leaderSessionId,
                JobMasterServiceProcess.class.getSimpleName());

        currentJobMasterServiceProcessLeaderId = leaderSessionId;
        jobMasterServiceProcess = jobMasterServiceProcessFactory.create(leaderSessionId);

        forwardIfValidLeader(
                leaderSessionId,
                jobMasterServiceProcess.getJobMasterGatewayFuture(),
                jobMasterGatewayFuture,
                "JobMasterGatewayFuture from JobMasterServiceProcess");
        forwardResultFuture(leaderSessionId, jobMasterServiceProcess.getResultFuture());
        confirmLeadership(leaderSessionId, jobMasterServiceProcess.getLeaderAddressFuture());
    }

    private void confirmLeadership(
            UUID leaderSessionId, CompletableFuture<String> leaderAddressFuture) {
        FutureUtils.assertNoException(
                leaderAddressFuture.thenCompose(
                        address ->
                                callIfRunning(
                                                () -> {
                                                    LOG.debug(
                                                            "Confirm leadership {}.",
                                                            leaderSessionId);
                                                    return leaderElection.confirmLeadershipAsync(
                                                            leaderSessionId, address);
                                                },
                                                "confirming leadership")
                                        .orElse(FutureUtils.completedVoidFuture())));
    }

    private void forwardResultFuture(
            UUID leaderSessionId, CompletableFuture<JobManagerRunnerResult> resultFuture) {
        resultFuture.whenComplete(
                (jobManagerRunnerResult, throwable) -> {
                    // The JobManagerResult needs to be cached for the current process even if the
                    // leadership was lost
                    cacheGloballyTerminalResultIfCurrentProcess(
                            leaderSessionId, jobManagerRunnerResult);
                    runIfValidLeader(
                            leaderSessionId,
                            () -> onJobCompletion(jobManagerRunnerResult, throwable),
                            "result future forwarding");
                });
    }

    private void cacheGloballyTerminalResultIfCurrentProcess(
            UUID leaderSessionId, JobManagerRunnerResult jobManagerRunnerResult) {
        synchronized (lock) {
            if (resultFuture.isDone()) {
                return;
            }
            if (leaderSessionId.equals(currentJobMasterServiceProcessLeaderId)
                    // initialization failures should still result in the job being suspended;
                    // caching it would trigger a job startup retry after failover
                    && isGloballyTerminalResult(jobManagerRunnerResult)) {
                LOG.debug(
                        "Caching globally terminal job result for job {} in case leadership is lost before forwarding.",
                        getJobID());
                pendingTerminalResult = jobManagerRunnerResult;
            }
        }
    }

    private boolean isGloballyTerminalResult(JobManagerRunnerResult jobManagerRunnerResult) {
        return jobManagerRunnerResult != null
                && jobManagerRunnerResult.isSuccess()
                && jobManagerRunnerResult
                        .getExecutionGraphInfo()
                        .getArchivedExecutionGraph()
                        .getState()
                        .isGloballyTerminalState();
    }

    @GuardedBy("lock")
    private void onJobCompletion(
            JobManagerRunnerResult jobManagerRunnerResult, Throwable throwable) {
        state = State.JOB_COMPLETED;
        currentJobMasterServiceProcessLeaderId = null;
        pendingTerminalResult = null;

        LOG.debug("Completing the result for job {}.", getJobID());

        if (throwable != null) {
            resultFuture.completeExceptionally(throwable);
            jobMasterGatewayFuture.completeExceptionally(
                    new FlinkException(
                            "Could not retrieve JobMasterGateway because the JobMaster failed.",
                            throwable));
        } else {
            if (!jobManagerRunnerResult.isSuccess()) {
                jobMasterGatewayFuture.completeExceptionally(
                        new FlinkException(
                                "Could not retrieve JobMasterGateway because the JobMaster initialization failed.",
                                jobManagerRunnerResult.getInitializationFailure()));
            }

            resultFuture.complete(jobManagerRunnerResult);
        }
    }

    @Override
    public void revokeLeadership() {
        runIfStateRunning(
                this::stopJobMasterServiceProcessAsync,
                "revoke leadership from JobMasterServiceProcess");
    }

    @GuardedBy("lock")
    private void stopJobMasterServiceProcessAsync() {
        sequentialOperation =
                sequentialOperation.thenCompose(
                        ignored ->
                                callIfRunning(
                                                this::stopJobMasterServiceProcess,
                                                "stop leading JobMasterServiceProcess")
                                        .orElse(FutureUtils.completedVoidFuture()));

        handleAsyncOperationError(sequentialOperation, "Could not suspend the job manager.");
    }

    @GuardedBy("lock")
    private CompletableFuture<Void> stopJobMasterServiceProcess() {
        LOG.info(
                "{} for job {} was revoked leadership with leader id {}. Stopping current {}.",
                getClass().getSimpleName(),
                getJobID(),
                jobMasterServiceProcess.getLeaderSessionId(),
                JobMasterServiceProcess.class.getSimpleName());

        jobMasterGatewayFuture.completeExceptionally(
                new FlinkException(
                        "Cannot obtain JobMasterGateway because the JobMaster lost leadership."));
        jobMasterGatewayFuture = new CompletableFuture<>();

        hasCurrentLeaderBeenCancelled = false;

        // Intentionally NOT clearing currentJobMasterServiceProcessLeaderId here: a globally
        // terminal result from the closing process can still complete its resultFuture during
        // closeAsync, and the forwarding callback needs the matching leader id to cache it
        // (see FLINK-39704).
        return jobMasterServiceProcess.closeAsync();
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(exception);
    }

    private void handleAsyncOperationError(CompletableFuture<Void> operation, String message) {
        operation.whenComplete(
                (unused, throwable) -> {
                    if (throwable != null) {
                        runIfStateRunning(
                                () ->
                                        handleJobMasterServiceLeadershipRunnerError(
                                                new FlinkException(message, throwable)),
                                "handle JobMasterServiceLeadershipRunner error");
                    }
                });
    }

    private void handleJobMasterServiceLeadershipRunnerError(Throwable cause) {
        if (ExceptionUtils.isJvmFatalError(cause)) {
            fatalErrorHandler.onFatalError(cause);
        } else {
            resultFuture.completeExceptionally(cause);
        }
    }

    private void runIfStateRunning(Runnable action, String actionDescription) {
        synchronized (lock) {
            if (isRunning()) {
                action.run();
            } else {
                LOG.debug(
                        "Ignore '{}' because the leadership runner is no longer running.",
                        actionDescription);
            }
        }
    }

    private <T> Optional<T> callIfRunning(
            Supplier<? extends T> supplier, String supplierDescription) {
        synchronized (lock) {
            if (isRunning()) {
                return Optional.of(supplier.get());
            } else {
                LOG.debug(
                        "Ignore '{}' because the leadership runner is no longer running.",
                        supplierDescription);
                return Optional.empty();
            }
        }
    }

    @GuardedBy("lock")
    private boolean isRunning() {
        return state == State.RUNNING;
    }

    private CompletableFuture<Void> runIfValidLeader(
            UUID expectedLeaderId, Runnable action, Runnable noLeaderFallback) {
        synchronized (lock) {
            if (isRunning() && leaderElection != null) {
                return leaderElection
                        .hasLeadershipAsync(expectedLeaderId)
                        .thenAccept(
                                hasLeadership -> {
                                    synchronized (lock) {
                                        if (isRunning() && hasLeadership) {
                                            action.run();
                                        } else {
                                            noLeaderFallback.run();
                                        }
                                    }
                                });
            } else {
                noLeaderFallback.run();
                return FutureUtils.completedVoidFuture();
            }
        }
    }

    private CompletableFuture<Void> runIfValidLeader(
            UUID expectedLeaderId, Runnable action, String noLeaderFallbackCommandDescription) {
        return runIfValidLeader(
                expectedLeaderId,
                action,
                () ->
                        printLogIfNotValidLeader(
                                noLeaderFallbackCommandDescription, expectedLeaderId));
    }

    private <T> void forwardIfValidLeader(
            UUID expectedLeaderId,
            CompletableFuture<? extends T> source,
            CompletableFuture<T> target,
            String forwardDescription) {
        source.whenComplete(
                (t, throwable) ->
                        runIfValidLeader(
                                expectedLeaderId,
                                () -> {
                                    if (throwable != null) {
                                        target.completeExceptionally(throwable);
                                    } else {
                                        target.complete(t);
                                    }
                                },
                                forwardDescription));
    }

    enum State {
        RUNNING,
        STOPPED,
        JOB_COMPLETED,
    }
}
