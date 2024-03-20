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

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Consumer;
import java.util.function.Function;

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
        runIfStateRunningAndFailFatallyOnError(
                () -> startJobMasterServiceProcessAsync(leaderSessionID),
                "starting a new JobMasterServiceProcess");
    }

    @GuardedBy("lock")
    private void startJobMasterServiceProcessAsync(UUID leaderSessionId) {
        sequentialOperation =
                sequentialOperation.thenCompose(
                        unused ->
                                handleLeadershipGrantBasedOnJobState(
                                        leaderSessionId,
                                        this::handleJobAlreadyDoneAsync,
                                        this::createNewJobMasterServiceProcessAsync));

        assertOnError(sequentialOperation);
    }

    private CompletableFuture<Void> handleLeadershipGrantBasedOnJobState(
            UUID leaderSessionID,
            Function<UUID, CompletableFuture<Void>> jobAlreadyFinishedHandler,
            Function<UUID, CompletableFuture<Void>> jobNotFinishedHandler) {
        return runAsyncIfLeaderAndFailFatallyOnError(
                leaderSessionID,
                "Check JobResultStore",
                () ->
                        jobResultStore
                                .hasJobResultEntryAsync(getJobID())
                                .thenCompose(
                                        hasJobResult ->
                                                hasJobResult
                                                        ? jobAlreadyFinishedHandler.apply(
                                                                leaderSessionID)
                                                        : jobNotFinishedHandler.apply(
                                                                leaderSessionID)));
    }

    private CompletableFuture<Void> handleJobAlreadyDoneAsync(UUID leaderSessionId) {
        return runAsyncIfLeaderAndFailFatallyOnError(
                leaderSessionId, "Mark job as done", () -> jobAlreadyDone(leaderSessionId));
    }

    private CompletableFuture<Void> createNewJobMasterServiceProcessAsync(UUID leaderSessionId) {
        return runAsyncIfLeader(
                        leaderSessionId,
                        "create new job master service process",
                        () -> createNewJobMasterServiceProcess(leaderSessionId))
                .exceptionally(
                        t ->
                                handleAsyncOperationError(
                                        t, "Handle JobMasterServiceLeadershipRunner start error."));
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
                "Forward gateway future from JobMasterServiceProcess");
        forwardResultFuture(leaderSessionId, jobMasterServiceProcess.getResultFuture());
        confirmLeadership(leaderSessionId, jobMasterServiceProcess.getLeaderAddressFuture());
    }

    private void confirmLeadership(
            UUID leaderSessionId, CompletableFuture<String> leaderAddressFuture) {
        FutureUtils.assertNoException(
                leaderAddressFuture.thenAccept(
                        address ->
                                triggerAsyncIfLeaderAndFailFatallyOnError(
                                        leaderSessionId,
                                        "Confirming leadership",
                                        () ->
                                                leaderElection.confirmLeadership(
                                                        leaderSessionId, address))));
    }

    private void forwardResultFuture(
            UUID leaderSessionId, CompletableFuture<JobManagerRunnerResult> resultFuture) {
        resultFuture.whenComplete(
                (jobManagerRunnerResult, throwable) ->
                        triggerAsyncIfLeaderAndFailFatallyOnError(
                                leaderSessionId,
                                "ResultFuture forwarding",
                                () -> onJobCompletion(jobManagerRunnerResult, throwable)));
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
        runIfStateRunningAndFailFatallyOnError(
                this::stopJobMasterServiceProcessAsync,
                "revoke leadership from JobMasterServiceProcess");
    }

    @GuardedBy("lock")
    private void stopJobMasterServiceProcessAsync() {
        sequentialOperation =
                sequentialOperation.thenCompose(
                        ignored -> {
                            final CompletableFuture<Void> asyncOperationFuture =
                                    new CompletableFuture<>();
                            final boolean operationTriggered =
                                    runIfStateRunningAndFailFatallyOnError(
                                            () ->
                                                    FutureUtils.forward(
                                                            stopJobMasterServiceProcess(),
                                                            asyncOperationFuture),
                                            "Stop leading JobMasterServiceProcess");

                            return operationTriggered
                                    ? asyncOperationFuture
                                    : FutureUtils.completedVoidFuture();
                        });

        assertOnError(sequentialOperation);
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

        return jobMasterServiceProcess
                .closeAsync()
                .exceptionally(
                        t -> handleAsyncOperationError(t, "Could not suspend the JobMaster."));
    }

    @Override
    public void handleError(Exception exception) {
        fatalErrorHandler.onFatalError(exception);
    }

    private Void handleAsyncOperationError(Throwable throwable, String message) {
        runIfStateRunningAndFailFatallyOnError(
                () -> {
                    final FlinkException cause = new FlinkException(message, throwable);
                    if (ExceptionUtils.isJvmFatalError(cause)) {
                        fatalErrorHandler.onFatalError(cause);
                    } else {
                        resultFuture.completeExceptionally(cause);
                    }
                },
                "Handle JobMasterServiceLeadershipRunner error");
        return null;
    }

    private boolean runIfStateRunningAndFailFatallyOnError(
            ThrowingRunnable<? extends Throwable> action, String actionDescription) {
        try {
            return runIfStateRunning(action, actionDescription);
        } catch (Throwable t) {
            fatalErrorHandler.onFatalError(t);
            return true;
        }
    }

    private boolean runIfStateRunning(
            ThrowingRunnable<? extends Throwable> action, String actionDescription)
            throws Throwable {
        synchronized (lock) {
            if (isRunning()) {
                action.run();
                return true;
            } else {
                LOG.debug(
                        "Ignore '{}' because the leadership runner is no longer running.",
                        actionDescription);
                return false;
            }
        }
    }

    @GuardedBy("lock")
    private boolean isRunning() {
        return state == State.RUNNING;
    }

    /**
     * Triggers the action and makes sure that no error appears. This instance's {@link
     * #fatalErrorHandler} will be triggered in case of an error.
     */
    private void triggerAsyncIfLeaderAndFailFatallyOnError(
            UUID expectedLeaderId,
            String eventLabelToLog,
            ThrowingRunnable<? extends Throwable> action) {
        assertOnError(
                runAsyncIfLeaderAndFailFatallyOnError(expectedLeaderId, eventLabelToLog, action));
    }

    /**
     * Runs the action and returns a future to monitor the asynchronous operation. This instance's
     * {@link #fatalErrorHandler} will be triggered in case of an error.
     */
    private CompletableFuture<Void> runAsyncIfLeaderAndFailFatallyOnError(
            UUID expectedLeaderId,
            String eventLabelToLog,
            ThrowingRunnable<? extends Throwable> action) {
        return runAsyncIfLeader(
                expectedLeaderId, eventLabelToLog, action, fatalErrorHandler::onFatalError);
    }

    /** Runs the action and makes the returning future fail if an error appeared. */
    private CompletableFuture<Void> runAsyncIfLeader(
            UUID expectedLeaderId,
            String eventLabelToLog,
            ThrowingRunnable<? extends Throwable> action) {
        return runAsyncIfLeader(
                expectedLeaderId,
                eventLabelToLog,
                action,
                t -> {
                    throw new CompletionException(t);
                });
    }

    private CompletableFuture<Void> runAsyncIfLeader(
            UUID expectedLeaderId,
            String eventLabelToLog,
            ThrowingRunnable<? extends Throwable> action,
            Consumer<Throwable> errorHandler) {
        return leaderElection
                .runAsyncIfLeader(
                        expectedLeaderId,
                        () -> runIfStateRunning(action, eventLabelToLog),
                        eventLabelToLog)
                .exceptionally(
                        t -> {
                            if (ExceptionUtils.findThrowable(t, LeadershipLostException.class)
                                    .isPresent()) {
                                LOG.debug(
                                        "Ignore leader action '{}' because the leadership runner is no longer the valid leader for {}.",
                                        eventLabelToLog,
                                        expectedLeaderId);
                            } else {
                                errorHandler.accept(t);
                            }

                            return null;
                        });
    }

    private <T> void forwardIfValidLeader(
            UUID expectedLeaderId,
            CompletableFuture<? extends T> source,
            CompletableFuture<T> target,
            String forwardDescription) {
        source.whenComplete(
                (gateway, throwable) ->
                        runAsyncIfLeaderAndFailFatallyOnError(
                                expectedLeaderId,
                                forwardDescription,
                                () -> FutureUtils.doForward(gateway, throwable, target)));
    }

    private void assertOnError(CompletableFuture<?> future) {
        FutureUtils.handleUncaughtException(
                future, (ignoredThread, t) -> fatalErrorHandler.onFatalError(t));
    }

    enum State {
        RUNNING,
        STOPPED,
        JOB_COMPLETED,
    }
}
