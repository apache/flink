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

package org.apache.flink.client.deployment.application;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutorServiceLoader;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.application.AbstractApplication;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * An implementation of {@link AbstractApplication} designed for executing the user's {@code
 * main()}.
 */
public class PackagedProgramApplication extends AbstractApplication {

    @VisibleForTesting static final String FAILED_JOB_NAME = "(application driver)";

    private static final Logger LOG = LoggerFactory.getLogger(PackagedProgramApplication.class);

    private final PackagedProgramDescriptor programDescriptor;

    private final Collection<JobID> recoveredJobIds;

    private final Configuration configuration;

    private final boolean handleFatalError;

    private final boolean enforceSingleJobExecution;

    private final boolean submitFailedJobOnApplicationError;

    private final boolean shutDownOnFinish;

    private transient PackagedProgram program;

    private transient CompletableFuture<Void> applicationCompletionFuture;

    private transient ScheduledFuture<?> applicationExecutionTask;

    private transient CompletableFuture<Acknowledge> finishApplicationFuture;

    private transient boolean isDisposing = false;

    public PackagedProgramApplication(
            final ApplicationID applicationId,
            final PackagedProgram program,
            final Collection<JobID> recoveredJobIds,
            final Configuration configuration,
            final boolean handleFatalError,
            final boolean enforceSingleJobExecution,
            final boolean submitFailedJobOnApplicationError,
            final boolean shutDownOnFinish) {
        super(applicationId);
        this.program = checkNotNull(program);
        this.recoveredJobIds = checkNotNull(recoveredJobIds);
        this.configuration = checkNotNull(configuration);
        this.handleFatalError = handleFatalError;
        this.enforceSingleJobExecution = enforceSingleJobExecution;
        this.submitFailedJobOnApplicationError = submitFailedJobOnApplicationError;
        this.shutDownOnFinish = shutDownOnFinish;
        this.programDescriptor = program.getDescriptor();
    }

    @Override
    public CompletableFuture<Acknowledge> execute(
            DispatcherGateway dispatcherGateway,
            ScheduledExecutor scheduledExecutor,
            Executor mainThreadExecutor,
            FatalErrorHandler errorHandler) {
        transitionToRunning();

        final CompletableFuture<List<JobID>> applicationExecutionFuture = new CompletableFuture<>();
        final Set<JobID> tolerateMissingResult = Collections.synchronizedSet(new HashSet<>());

        // we need to hand in a future as return value because we need to get those JobIs out
        // from the scheduled task that executes the user program
        applicationExecutionTask =
                scheduledExecutor.schedule(
                        () ->
                                runApplicationEntryPoint(
                                        applicationExecutionFuture,
                                        tolerateMissingResult,
                                        dispatcherGateway,
                                        scheduledExecutor),
                        0L,
                        TimeUnit.MILLISECONDS);

        boolean decoupleApplicationStatusFromJobStatus =
                !configuration.get(DeploymentOptions.TERMINATE_APPLICATION_ON_ANY_JOB_EXCEPTION);

        if (decoupleApplicationStatusFromJobStatus) {
            // when the application status is decoupled from the job status, we don't need to wait
            // for the job results
            applicationCompletionFuture = applicationExecutionFuture.thenApply(ignored -> null);
            finishApplicationFuture =
                    applicationCompletionFuture
                            .handleAsync(
                                    (ignored, t) -> {
                                        if (t == null) {
                                            LOG.info(
                                                    "Application completed SUCCESSFULLY (decoupled from job results)");
                                            return finishAsSucceeded(
                                                    dispatcherGateway,
                                                    scheduledExecutor,
                                                    mainThreadExecutor,
                                                    errorHandler);
                                        }

                                        return onApplicationCanceledOrFailed(
                                                dispatcherGateway,
                                                scheduledExecutor,
                                                mainThreadExecutor,
                                                errorHandler,
                                                t);
                                    },
                                    mainThreadExecutor)
                            .thenCompose(Function.identity());
        } else {
            applicationCompletionFuture =
                    applicationExecutionFuture.thenCompose(
                            jobIds ->
                                    waitForJobResults(
                                            dispatcherGateway,
                                            jobIds,
                                            tolerateMissingResult,
                                            scheduledExecutor));

            finishApplicationFuture =
                    applicationCompletionFuture
                            .handleAsync(
                                    (ignored, t) -> {
                                        if (t == null) {
                                            LOG.info("Application completed SUCCESSFULLY");
                                            transitionToFinished();
                                            return maybeShutdownCluster(
                                                    dispatcherGateway, ApplicationStatus.SUCCEEDED);
                                        }

                                        final Optional<JobStatus> maybeJobStatus =
                                                extractJobStatus(t);
                                        if (maybeJobStatus.isPresent()) {
                                            // the exception is caused by job execution results
                                            ApplicationState applicationState =
                                                    ApplicationState.fromJobStatus(
                                                            maybeJobStatus.get());
                                            LOG.info("Application {}: ", applicationState, t);
                                            if (applicationState == ApplicationState.CANCELED) {
                                                transitionToCanceling();
                                                return finishAsCanceled(
                                                        dispatcherGateway,
                                                        scheduledExecutor,
                                                        mainThreadExecutor,
                                                        errorHandler);

                                            } else {
                                                transitionToFailing();
                                                return finishAsFailed(
                                                        dispatcherGateway,
                                                        scheduledExecutor,
                                                        mainThreadExecutor,
                                                        errorHandler);
                                            }
                                        }

                                        return onApplicationCanceledOrFailed(
                                                dispatcherGateway,
                                                scheduledExecutor,
                                                mainThreadExecutor,
                                                errorHandler,
                                                t);
                                    },
                                    mainThreadExecutor)
                            .thenCompose(Function.identity());
        }

        // In Application Mode, the handleFatalError flag is set to true, and uncaught exceptions
        // are handled by the errorHandler to trigger failover.
        // In Session Mode, the handleFatalError flag may be set to false, leaving exceptions
        // unhandled. This behavior may change in the future.
        FutureUtils.handleUncaughtException(
                finishApplicationFuture,
                (t, e) -> {
                    if (handleFatalError) {
                        errorHandler.onFatalError(e);
                    }
                });

        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    @Override
    public void cancel() {
        ApplicationState currentState = getApplicationStatus();
        if (currentState == ApplicationState.CREATED) {
            // nothing to cancel
            transitionToCanceling();
            transitionToCanceled();
        } else if (currentState == ApplicationState.RUNNING) {
            transitionToCanceling();
            cancelFutures();
        }
    }

    @Override
    public void dispose() {
        isDisposing = true;
        cancelFutures();
    }

    private void cancelFutures() {
        if (applicationExecutionTask != null) {
            applicationExecutionTask.cancel(true);
        }

        if (applicationCompletionFuture != null) {
            // applicationCompletionFuture.handleAsync will not block here
            applicationCompletionFuture.cancel(true);
        }
    }

    @Override
    public String getName() {
        return programDescriptor.getMainClassName();
    }

    @VisibleForTesting
    ScheduledFuture<?> getApplicationExecutionFuture() {
        return applicationExecutionTask;
    }

    @VisibleForTesting
    CompletableFuture<Void> getApplicationCompletionFuture() {
        return applicationCompletionFuture;
    }

    @VisibleForTesting
    CompletableFuture<Acknowledge> getFinishApplicationFuture() {
        return finishApplicationFuture;
    }

    private CompletableFuture<Acknowledge> onApplicationCanceledOrFailed(
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final Executor mainThreadExecutor,
            final FatalErrorHandler errorHandler,
            final Throwable t) {
        if (t instanceof CancellationException) {
            // the applicationCompletionFuture is canceled by cancel() or dispose()
            if (isDisposing) {
                LOG.warn("Application execution is canceled because the cluster is shutting down.");
                // we don't need to do anything here during cleanup
                return CompletableFuture.completedFuture(Acknowledge.get());
            }
            LOG.info("Application execution is canceled manually.");
            return finishAsCanceled(
                    dispatcherGateway, scheduledExecutor, mainThreadExecutor, errorHandler);
        }

        LOG.warn("Application failed unexpectedly: ", t);
        transitionToFailing();
        return finishAsFailed(
                dispatcherGateway, scheduledExecutor, mainThreadExecutor, errorHandler);
    }

    private CompletableFuture<Acknowledge> finishAsCanceled(
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final Executor mainThreadExecutor,
            final FatalErrorHandler errorHandler) {
        return cancelAllJobsAndWaitForTerminalStates(dispatcherGateway, scheduledExecutor)
                .handleAsync(
                        (v, t) -> {
                            if (t != null) {
                                return onCancelAllJobsAndWaitForTerminalStatesException(
                                        errorHandler, t);
                            }
                            transitionToCanceled();
                            return maybeShutdownCluster(
                                    dispatcherGateway, ApplicationStatus.CANCELED);
                        },
                        mainThreadExecutor)
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Acknowledge> finishAsFailed(
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final Executor mainThreadExecutor,
            final FatalErrorHandler errorHandler) {
        return cancelAllJobsAndWaitForTerminalStates(dispatcherGateway, scheduledExecutor)
                .handleAsync(
                        (v, t) -> {
                            if (t != null) {
                                return onCancelAllJobsAndWaitForTerminalStatesException(
                                        errorHandler, t);
                            }
                            transitionToFailed();
                            return maybeShutdownCluster(
                                    dispatcherGateway, ApplicationStatus.FAILED);
                        },
                        mainThreadExecutor)
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Acknowledge> finishAsSucceeded(
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final Executor mainThreadExecutor,
            final FatalErrorHandler errorHandler) {
        return cancelAllJobsAndWaitForTerminalStates(dispatcherGateway, scheduledExecutor)
                .handleAsync(
                        (v, t) -> {
                            if (t != null) {
                                return onCancelAllJobsAndWaitForTerminalStatesException(
                                        errorHandler, t);
                            }
                            transitionToFinished();
                            return maybeShutdownCluster(
                                    dispatcherGateway, ApplicationStatus.SUCCEEDED);
                        },
                        mainThreadExecutor)
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Acknowledge> onCancelAllJobsAndWaitForTerminalStatesException(
            final FatalErrorHandler errorHandler, final Throwable t) {
        LOG.warn("Failed to cancel and wait for all jobs.", t);
        errorHandler.onFatalError(t);
        return CompletableFuture.completedFuture(Acknowledge.get());
    }

    /**
     * This method cancels the jobs to clean up related resources when the application ends. If any
     * job cancellation fails, the returned future will complete exceptionally.
     */
    private CompletableFuture<Void> cancelAllJobsAndWaitForTerminalStates(
            final DispatcherGateway dispatcherGateway, final ScheduledExecutor scheduledExecutor) {
        final Duration timeout = configuration.get(ClientOptions.CLIENT_TIMEOUT);

        LOG.info(
                "Start to cancel remaining jobs for application {} ({}).",
                getName(),
                getApplicationId());
        // get all jobs that needs to be canceled
        CompletableFuture<Set<JobID>> jobsToCancelFuture =
                FutureUtils.combineAll(
                                getJobs().stream()
                                        .map(
                                                jobId ->
                                                        getNonGlobalTerminalJob(
                                                                dispatcherGateway, jobId, timeout))
                                        .collect(Collectors.toList()))
                        .thenApply(
                                jobIds ->
                                        jobIds.stream()
                                                .filter(Objects::nonNull)
                                                .collect(Collectors.toSet()));

        // get all jobs that is canceling
        CompletableFuture<Set<JobID>> jobsCancelingFuture =
                jobsToCancelFuture.thenCompose(
                        jobsToCancel -> {
                            if (!jobsToCancel.isEmpty()) {
                                LOG.info(
                                        "Canceling jobs for application '{}' ({}): {}",
                                        getName(),
                                        getApplicationId(),
                                        jobsToCancel);
                            }

                            List<CompletableFuture<JobID>> cancelFutures =
                                    jobsToCancel.stream()
                                            .map(
                                                    jobId ->
                                                            cancelJob(
                                                                    dispatcherGateway,
                                                                    jobId,
                                                                    timeout))
                                            .collect(Collectors.toList());

                            return FutureUtils.combineAll(cancelFutures)
                                    .thenApply(
                                            jobIds ->
                                                    jobIds.stream()
                                                            .filter(Objects::nonNull)
                                                            .collect(Collectors.toSet()));
                        });

        // wait for all jobs to be canceled in the scheduledExecutor
        return jobsCancelingFuture.thenComposeAsync(
                jobsCanceling -> {
                    List<CompletableFuture<?>> jobResultFutures =
                            jobsCanceling.stream()
                                    .map(
                                            jobId ->
                                                    getJobResult(
                                                                    dispatcherGateway,
                                                                    jobId,
                                                                    scheduledExecutor,
                                                                    false)
                                                            .thenApply(ignored -> null))
                                    .collect(Collectors.toList());
                    return FutureUtils.waitForAll(jobResultFutures);
                },
                scheduledExecutor);
    }

    private CompletableFuture<JobID> getNonGlobalTerminalJob(
            DispatcherGateway dispatcherGateway, JobID jobId, Duration timeout) {
        return dispatcherGateway
                .requestJobStatus(jobId, timeout)
                .thenApply(
                        jobStatus -> {
                            if (!jobStatus.isGloballyTerminalState()) {
                                return jobId;
                            } else {
                                return null;
                            }
                        });
    }

    private CompletableFuture<JobID> cancelJob(
            DispatcherGateway dispatcherGateway, JobID jobId, Duration timeout) {
        return dispatcherGateway.cancelJob(jobId, timeout).thenApply(ignored -> jobId);
    }

    private CompletableFuture<Acknowledge> maybeShutdownCluster(
            DispatcherGateway dispatcherGateway, ApplicationStatus applicationStatus) {
        return shutDownOnFinish
                ? dispatcherGateway.shutDownCluster(applicationStatus)
                : CompletableFuture.completedFuture(Acknowledge.get());
    }

    private Optional<JobStatus> extractJobStatus(Throwable t) {
        final Optional<UnsuccessfulExecutionException> maybeException =
                ExceptionUtils.findThrowable(t, UnsuccessfulExecutionException.class);
        return maybeException.flatMap(UnsuccessfulExecutionException::getStatus);
    }

    /**
     * Runs the user program entrypoint and completes the given {@code jobIdsFuture} with the {@link
     * JobID JobIDs} of the submitted jobs.
     *
     * <p>This should be executed in a separate thread (or task).
     */
    private void runApplicationEntryPoint(
            final CompletableFuture<List<JobID>> jobIdsFuture,
            final Set<JobID> tolerateMissingResult,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor) {
        if (submitFailedJobOnApplicationError && !enforceSingleJobExecution) {
            jobIdsFuture.completeExceptionally(
                    new ApplicationExecutionException(
                            String.format(
                                    "Submission of failed job in case of an application error ('%s') is not supported in non-HA setups.",
                                    DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR
                                            .key())));
            return;
        }
        final List<JobID> applicationJobIds = new ArrayList<>(recoveredJobIds);
        try {
            if (program == null) {
                LOG.info("Reconstructing program from descriptor {}", programDescriptor);
                program = programDescriptor.toPackageProgram();
            }

            final PipelineExecutorServiceLoader executorServiceLoader =
                    new EmbeddedExecutorServiceLoader(
                            applicationJobIds, dispatcherGateway, scheduledExecutor);

            ClientUtils.executeProgram(
                    executorServiceLoader,
                    configuration,
                    program,
                    enforceSingleJobExecution,
                    true /* suppress sysout */);

            if (applicationJobIds.isEmpty()) {
                jobIdsFuture.completeExceptionally(
                        new ApplicationExecutionException(
                                "The application contains no execute() calls."));
            } else {
                jobIdsFuture.complete(applicationJobIds);
            }
        } catch (Throwable t) {
            // If we're running in a single job execution mode, it's safe to consider re-submission
            // of an already finished a success.
            final Optional<DuplicateJobSubmissionException> maybeDuplicate =
                    ExceptionUtils.findThrowable(t, DuplicateJobSubmissionException.class);
            if (enforceSingleJobExecution
                    && maybeDuplicate.isPresent()
                    && maybeDuplicate.get().isGloballyTerminated()) {
                final JobID jobId = maybeDuplicate.get().getJobID();
                tolerateMissingResult.add(jobId);
                jobIdsFuture.complete(Collections.singletonList(jobId));
            } else if (submitFailedJobOnApplicationError && applicationJobIds.isEmpty()) {
                final JobID failedJobId =
                        JobID.fromHexString(
                                configuration.get(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID));
                dispatcherGateway
                        .submitFailedJob(failedJobId, FAILED_JOB_NAME, t)
                        .thenAccept(
                                ignored ->
                                        jobIdsFuture.complete(
                                                Collections.singletonList(failedJobId)));
            } else {
                jobIdsFuture.completeExceptionally(
                        new ApplicationExecutionException("Could not execute application.", t));
            }
        }
    }

    private CompletableFuture<Void> waitForJobResults(
            final DispatcherGateway dispatcherGateway,
            final Collection<JobID> applicationJobIds,
            final Set<JobID> tolerateMissingResult,
            final ScheduledExecutor executor) {
        final List<CompletableFuture<?>> jobResultFutures =
                applicationJobIds.stream()
                        .map(
                                jobId ->
                                        unwrapJobResultException(
                                                getJobResult(
                                                        dispatcherGateway,
                                                        jobId,
                                                        executor,
                                                        tolerateMissingResult.contains(jobId))))
                        .collect(Collectors.toList());

        return configuration.get(DeploymentOptions.TERMINATE_APPLICATION_ON_ANY_JOB_EXCEPTION)
                ? FutureUtils.waitForAll(jobResultFutures)
                : FutureUtils.completeAll(jobResultFutures);
    }

    private CompletableFuture<JobResult> getJobResult(
            final DispatcherGateway dispatcherGateway,
            final JobID jobId,
            final ScheduledExecutor scheduledExecutor,
            final boolean tolerateMissingResult) {
        final Duration timeout = configuration.get(ClientOptions.CLIENT_TIMEOUT);
        final Duration retryPeriod = configuration.get(ClientOptions.CLIENT_RETRY_PERIOD);
        final CompletableFuture<JobResult> jobResultFuture =
                JobStatusPollingUtils.getJobResult(
                        dispatcherGateway, jobId, scheduledExecutor, timeout, retryPeriod);
        if (tolerateMissingResult) {
            // Return "unknown" job result if dispatcher no longer knows the actual result.
            return FutureUtils.handleException(
                    jobResultFuture,
                    FlinkJobNotFoundException.class,
                    exception ->
                            new JobResult.Builder()
                                    .jobId(jobId)
                                    .jobStatus(null)
                                    .netRuntime(Long.MAX_VALUE)
                                    .build());
        }
        return jobResultFuture;
    }

    /**
     * If the given {@link JobResult} indicates success, this passes through the {@link JobResult}.
     * Otherwise, this returns a future that is finished exceptionally (potentially with an
     * exception from the {@link JobResult}).
     */
    private CompletableFuture<JobResult> unwrapJobResultException(
            final CompletableFuture<JobResult> jobResult) {
        return jobResult.thenApply(
                result -> {
                    if (result.isSuccess()) {
                        return result;
                    }

                    throw new CompletionException(
                            UnsuccessfulExecutionException.fromJobResult(
                                    result, program.getUserCodeClassLoader()));
                });
    }
}
