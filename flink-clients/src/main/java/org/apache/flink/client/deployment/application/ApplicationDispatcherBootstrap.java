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

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.client.ClientUtils;
import org.apache.flink.client.cli.ClientOptions;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutorServiceLoader;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.core.execution.PipelineExecutorServiceLoader;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.concurrent.FutureUtils;
import org.apache.flink.runtime.concurrent.ScheduledExecutor;
import org.apache.flink.runtime.dispatcher.DispatcherBootstrap;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link DispatcherBootstrap} used for running the user's {@code main()} in "Application Mode"
 * (see FLIP-85).
 *
 * <p>This dispatcher bootstrap submits the recovered {@link JobGraph job graphs} for re-execution
 * (in case of recovery from a failure), and then submits the remaining jobs of the application for
 * execution.
 *
 * <p>To achieve this, it works in conjunction with the {@link EmbeddedExecutor EmbeddedExecutor}
 * which decides if it should submit a job for execution (in case of a new job) or the job was
 * already recovered and is running.
 */
@Internal
public class ApplicationDispatcherBootstrap implements DispatcherBootstrap {

    private static final Logger LOG = LoggerFactory.getLogger(ApplicationDispatcherBootstrap.class);

    public static final JobID ZERO_JOB_ID = new JobID(0, 0);

    private final PackagedProgram application;

    private final Collection<JobID> recoveredJobIds;

    private final Configuration configuration;

    private final FatalErrorHandler errorHandler;

    private final CompletableFuture<Void> applicationCompletionFuture;

    private final CompletableFuture<Acknowledge> clusterShutdownFuture;

    private ScheduledFuture<?> applicationExecutionTask;

    public ApplicationDispatcherBootstrap(
            final PackagedProgram application,
            final Collection<JobID> recoveredJobIds,
            final Configuration configuration,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final FatalErrorHandler errorHandler) {
        this.configuration = checkNotNull(configuration);
        this.recoveredJobIds = checkNotNull(recoveredJobIds);
        this.application = checkNotNull(application);
        this.errorHandler = checkNotNull(errorHandler);

        this.applicationCompletionFuture =
                fixJobIdAndRunApplicationAsync(dispatcherGateway, scheduledExecutor);

        this.clusterShutdownFuture = runApplicationAndShutdownClusterAsync(dispatcherGateway);
    }

    @Override
    public void stop() {
        if (applicationExecutionTask != null) {
            applicationExecutionTask.cancel(true);
        }

        if (applicationCompletionFuture != null) {
            applicationCompletionFuture.cancel(true);
        }
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
    CompletableFuture<Acknowledge> getClusterShutdownFuture() {
        return clusterShutdownFuture;
    }

    /**
     * Runs the user program entrypoint and shuts down the given dispatcherGateway when the
     * application completes (either successfully or in case of failure).
     */
    private CompletableFuture<Acknowledge> runApplicationAndShutdownClusterAsync(
            final DispatcherGateway dispatcherGateway) {
        return applicationCompletionFuture
                .handle(
                        (r, t) -> {
                            if (t == null) {
                                LOG.info("Application completed SUCCESSFULLY");
                                return dispatcherGateway.shutDownCluster(
                                        ApplicationStatus.SUCCEEDED);
                            }

                            final Optional<UnsuccessfulExecutionException> exception =
                                    ExceptionUtils.findThrowable(
                                            t, UnsuccessfulExecutionException.class);

                            if (exception.isPresent()) {
                                final ApplicationStatus applicationStatus =
                                        exception.get().getStatus();

                                if (applicationStatus == ApplicationStatus.CANCELED
                                        || applicationStatus == ApplicationStatus.FAILED) {
                                    LOG.info("Application {}: ", applicationStatus, t);
                                    return dispatcherGateway.shutDownCluster(applicationStatus);
                                }
                            }

                            LOG.warn("Application failed unexpectedly: ", t);
                            this.errorHandler.onFatalError(
                                    new FlinkException("Application failed unexpectedly.", t));

                            return FutureUtils.<Acknowledge>completedExceptionally(t);
                        })
                .thenCompose(Function.identity());
    }

    private CompletableFuture<Void> fixJobIdAndRunApplicationAsync(
            final DispatcherGateway dispatcherGateway, final ScheduledExecutor scheduledExecutor) {

        final Optional<String> configuredJobId =
                configuration.getOptional(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID);

        if (!HighAvailabilityMode.isHighAvailabilityModeActivated(configuration)
                && !configuredJobId.isPresent()) {
            return runApplicationAsync(dispatcherGateway, scheduledExecutor, false);
        }

        if (!configuredJobId.isPresent()) {
            configuration.set(
                    PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, ZERO_JOB_ID.toHexString());
        }
        return runApplicationAsync(dispatcherGateway, scheduledExecutor, true);
    }

    /**
     * Runs the user program entrypoint by scheduling a task on the given {@code scheduledExecutor}.
     * The returned {@link CompletableFuture} completes when all jobs of the user application
     * succeeded. if any of them fails, or if job submission fails.
     */
    private CompletableFuture<Void> runApplicationAsync(
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final boolean enforceSingleJobExecution) {
        final CompletableFuture<List<JobID>> applicationExecutionFuture = new CompletableFuture<>();

        // we need to hand in a future as return value because we need to get those JobIs out
        // from the scheduled task that executes the user program
        applicationExecutionTask =
                scheduledExecutor.schedule(
                        () ->
                                runApplicationEntryPoint(
                                        applicationExecutionFuture,
                                        dispatcherGateway,
                                        scheduledExecutor,
                                        enforceSingleJobExecution),
                        0L,
                        TimeUnit.MILLISECONDS);

        return applicationExecutionFuture.thenCompose(
                jobIds -> getApplicationResult(dispatcherGateway, jobIds, scheduledExecutor));
    }

    /**
     * Runs the user program entrypoint and completes the given {@code jobIdsFuture} with the {@link
     * JobID JobIDs} of the submitted jobs.
     *
     * <p>This should be executed in a separate thread (or task).
     */
    private void runApplicationEntryPoint(
            final CompletableFuture<List<JobID>> jobIdsFuture,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final boolean enforceSingleJobExecution) {
        try {
            final List<JobID> applicationJobIds = new ArrayList<>(recoveredJobIds);

            final PipelineExecutorServiceLoader executorServiceLoader =
                    new EmbeddedExecutorServiceLoader(
                            applicationJobIds, dispatcherGateway, scheduledExecutor);

            ClientUtils.executeProgram(
                    executorServiceLoader,
                    configuration,
                    application,
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
            jobIdsFuture.completeExceptionally(
                    new ApplicationExecutionException("Could not execute application.", t));
        }
    }

    private CompletableFuture<Void> getApplicationResult(
            final DispatcherGateway dispatcherGateway,
            final Collection<JobID> applicationJobIds,
            final ScheduledExecutor executor) {
        final List<CompletableFuture<?>> jobResultFutures =
                applicationJobIds.stream()
                        .map(
                                jobId ->
                                        unwrapJobResultException(
                                                getJobResult(dispatcherGateway, jobId, executor)))
                        .collect(Collectors.toList());
        return FutureUtils.waitForAll(jobResultFutures);
    }

    private CompletableFuture<JobResult> getJobResult(
            final DispatcherGateway dispatcherGateway,
            final JobID jobId,
            final ScheduledExecutor scheduledExecutor) {

        final Time timeout =
                Time.milliseconds(configuration.get(ClientOptions.CLIENT_TIMEOUT).toMillis());
        final Time retryPeriod =
                Time.milliseconds(configuration.get(ClientOptions.CLIENT_RETRY_PERIOD).toMillis());

        return JobStatusPollingUtils.getJobResult(
                dispatcherGateway, jobId, scheduledExecutor, timeout, retryPeriod);
    }

    /**
     * If the given {@link JobResult} indicates success, this passes through the {@link JobResult}.
     * Otherwise, this returns a future that is finished exceptionally (potentially with an
     * exception from the {@link JobResult}.
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
                                    result, application.getUserCodeClassLoader()));
                });
    }
}
