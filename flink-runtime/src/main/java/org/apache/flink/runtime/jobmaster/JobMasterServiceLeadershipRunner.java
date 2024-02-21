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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.dispatcher.JobCancellationFailedException;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.leaderelection.LeaderContender;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.LeadershipLostException;
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

import javax.annotation.concurrent.GuardedBy;

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
 * <p>The {@link #resultFuture} can be completed with the following values: * *
 *
 * <ul>
 *   <li>{@link JobManagerRunnerResult} to signal an initialization failure of the {@link
 *       JobMasterService} or the completion of a job
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

            resultFuture.complete(
                    JobManagerRunnerResult.forSuccess(
                            createExecutionGraphInfoWithJobStatus(JobStatus.SUSPENDED)));

            processTerminationFuture = jobMasterServiceProcess.closeAsync();
        }

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
    public CompletableFuture<Acknowledge> cancel(Time timeout) {
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
    public CompletableFuture<JobStatus> requestJobStatus(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                executionGraphInfo.getArchivedExecutionGraph().getState());
    }

    @Override
    public CompletableFuture<JobDetails> requestJobDetails(Time timeout) {
        return requestJob(timeout)
                .thenApply(
                        executionGraphInfo ->
                                JobDetails.createDetailsForJob(
                                        executionGraphInfo.getArchivedExecutionGraph()));
    }

    @Override
    public CompletableFuture<ExecutionGraphInfo> requestJob(Time timeout) {
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
                                supplyAsyncIfValidLeader(
                                                leaderSessionId,
                                                () ->
                                                        jobResultStore.hasJobResultEntryAsync(
                                                                getJobID()),
                                                () ->
                                                        FutureUtils.completedExceptionally(
                                                                new LeadershipLostException(
                                                                        "The leadership is lost.")))
                                        .handle(
                                                (hasJobResult, throwable) -> {
                                                    if (throwable
                                                            instanceof LeadershipLostException) {
                                                        printLogIfNotValidLeader(
                                                                "verify job result entry",
                                                                leaderSessionId);
                                                        return null;
                                                    } else if (throwable != null) {
                                                        ExceptionUtils.rethrow(throwable);
                                                    }
                                                    if (hasJobResult) {
                                                        handleJobAlreadyDoneIfValidLeader(
                                                                leaderSessionId);
                                                    } else {
                                                        createNewJobMasterServiceProcessIfValidLeader(
                                                                leaderSessionId);
                                                    }
                                                    return null;
                                                }));
        handleAsyncOperationError(sequentialOperation, "Could not start the job manager.");
    }

    private void handleJobAlreadyDoneIfValidLeader(UUID leaderSessionId) {
        runIfValidLeader(
                leaderSessionId, () -> jobAlreadyDone(leaderSessionId), "check completed job");
    }

    private void createNewJobMasterServiceProcessIfValidLeader(UUID leaderSessionId) {
        runIfValidLeader(
                leaderSessionId,
                () ->
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
    private void createNewJobMasterServiceProcess(UUID leaderSessionId) throws FlinkException {
        Preconditions.checkState(jobMasterServiceProcess.closeAsync().isDone());

        LOG.info(
                "{} for job {} was granted leadership with leader id {}. Creating new {}.",
                getClass().getSimpleName(),
                getJobID(),
                leaderSessionId,
                JobMasterServiceProcess.class.getSimpleName());

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
                leaderAddressFuture.thenAccept(
                        address ->
                                runIfValidLeader(
                                        leaderSessionId,
                                        () -> {
                                            LOG.debug("Confirm leadership {}.", leaderSessionId);
                                            leaderElection.confirmLeadership(
                                                    leaderSessionId, address);
                                        },
                                        "confirming leadership")));
    }

    private void forwardResultFuture(
            UUID leaderSessionId, CompletableFuture<JobManagerRunnerResult> resultFuture) {
        resultFuture.whenComplete(
                (jobManagerRunnerResult, throwable) ->
                        runIfValidLeader(
                                leaderSessionId,
                                () -> onJobCompletion(jobManagerRunnerResult, throwable),
                                "result future forwarding"));
    }

    @GuardedBy("lock")
    private void onJobCompletion(
            JobManagerRunnerResult jobManagerRunnerResult, Throwable throwable) {
        state = State.JOB_COMPLETED;

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

    private void runIfValidLeader(
            UUID expectedLeaderId, Runnable action, Runnable noLeaderFallback) {
        synchronized (lock) {
            if (isValidLeader(expectedLeaderId)) {
                action.run();
            } else {
                noLeaderFallback.run();
            }
        }
    }

    private void runIfValidLeader(
            UUID expectedLeaderId, Runnable action, String noLeaderFallbackCommandDescription) {
        runIfValidLeader(
                expectedLeaderId,
                action,
                () ->
                        printLogIfNotValidLeader(
                                noLeaderFallbackCommandDescription, expectedLeaderId));
    }

    private <T> CompletableFuture<T> supplyAsyncIfValidLeader(
            UUID expectedLeaderId,
            Supplier<CompletableFuture<T>> supplier,
            Supplier<CompletableFuture<T>> noLeaderFallback) {
        final CompletableFuture<T> resultFuture = new CompletableFuture<>();
        runIfValidLeader(
                expectedLeaderId,
                () -> FutureUtils.forward(supplier.get(), resultFuture),
                () -> FutureUtils.forward(noLeaderFallback.get(), resultFuture));

        return resultFuture;
    }

    @GuardedBy("lock")
    private boolean isValidLeader(UUID expectedLeaderId) {
        return isRunning()
                && leaderElection != null
                && leaderElection.hasLeadership(expectedLeaderId);
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
