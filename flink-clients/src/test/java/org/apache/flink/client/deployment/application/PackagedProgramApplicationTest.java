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

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.testjar.FailingJob;
import org.apache.flink.client.testjar.MultiExecuteJob;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.webmonitor.TestingDispatcherGateway;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.SerializedThrowable;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for the {@link PackagedProgramApplication}. */
public class PackagedProgramApplicationTest {

    private static final int TIMEOUT_SECONDS = 10;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
    private final ScheduledExecutor scheduledExecutor =
            new ScheduledExecutorServiceAdapter(executor);
    private Executor mainThreadExecutor = Executors.newSingleThreadScheduledExecutor();

    @AfterEach
    void cleanup() {
        ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
    }

    @Test
    void testOnlyOneJobIsAllowedWhenEnforceSingleJobExecution() throws Throwable {
        final PackagedProgramApplication application =
                createAndExecuteApplication(
                        2, getConfiguration(), finishedJobGatewayBuilder().build(), true);

        assertException(application.getApplicationCompletionFuture(), FlinkRuntimeException.class);
        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFailed(application);
    }

    @Test
    void testStaticJobId() throws Throwable {
        final JobID testJobID = new JobID(0, 2);

        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());

        final CompletableFuture<JobID> submittedJobId = new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                finishedJobGatewayBuilder()
                        .setSubmitFunction(
                                jobGraph -> {
                                    submittedJobId.complete(jobGraph.getJobID());
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final PackagedProgramApplication application =
                createAndExecuteApplication(1, configurationUnderTest, dispatcherBuilder.build());

        application.getApplicationCompletionFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFinished(application);

        assertThat(submittedJobId.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(new JobID(0L, 2L));
    }

    @Test
    void testApplicationCleanupWhenOneJobFails() throws Throwable {
        final ConcurrentLinkedDeque<JobID> submittedJobIds = new ConcurrentLinkedDeque<>();
        final ConcurrentLinkedDeque<JobID> canceledJobIds = new ConcurrentLinkedDeque<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> {
                                    submittedJobIds.add(jobGraph.getJobID());
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setRequestJobStatusFunction(
                                jobId -> {
                                    // we only fail one of the jobs, the first one
                                    if (jobId.equals(submittedJobIds.peek())) {
                                        return CompletableFuture.completedFuture(JobStatus.FAILED);
                                    }
                                    if (canceledJobIds.contains(jobId)) {
                                        return CompletableFuture.completedFuture(
                                                JobStatus.CANCELED);
                                    }
                                    return CompletableFuture.completedFuture(JobStatus.RUNNING);
                                })
                        .setRequestJobResultFunction(
                                jobId -> {
                                    // we only fail one of the jobs, the first one
                                    if (jobId.equals(submittedJobIds.peek())) {
                                        return CompletableFuture.completedFuture(
                                                createFailedJobResult(jobId));
                                    }
                                    // the other jobs should be canceled
                                    return CompletableFuture.completedFuture(
                                            createCanceledJobResult(jobId));
                                })
                        .setCancelJobFunction(
                                jobId -> {
                                    canceledJobIds.add(jobId);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final PackagedProgramApplication application =
                new PackagedProgramApplication(
                        new ApplicationID(),
                        getProgram(2),
                        Collections.emptyList(),
                        getConfiguration(),
                        true,
                        false,
                        false,
                        true);

        // change mainThreadExecutor to be manually triggered
        // so that the application finish process isn't completed before adding the jobs
        mainThreadExecutor = new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutor manuallyTriggeredMainThreadExecutor =
                (ManuallyTriggeredScheduledExecutor) mainThreadExecutor;

        // wait for the application execution to be accepted
        application
                .execute(
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        mainThreadExecutor,
                        exception -> {})
                .join();

        // Wait for the task that is used to finish the application.
        while (manuallyTriggeredMainThreadExecutor.numQueuedRunnables() < 1) {
            Thread.sleep(100);
        }
        submittedJobIds.forEach(application::addJob);

        // Triggers the process to finish the application after adding the jobs. This
        // ensures that the application knows that these jobs need to be canceled.
        manuallyTriggeredMainThreadExecutor.trigger();
        final UnsuccessfulExecutionException exception =
                assertException(
                        application.getApplicationCompletionFuture(),
                        UnsuccessfulExecutionException.class);
        assertThat(exception.getStatus().orElse(null)).isEqualTo(JobStatus.FAILED);

        // Wait for the task that is used to transition to the final state.
        while (manuallyTriggeredMainThreadExecutor.numQueuedRunnables() < 1) {
            Thread.sleep(100);
        }
        manuallyTriggeredMainThreadExecutor.trigger();

        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertThat(canceledJobIds).containsOnly(submittedJobIds.peekLast());
        assertApplicationFailed(application);
    }

    @Test
    void testErrorHandlerIsCalledWhenApplicationCleanupThrowsAnException() throws Throwable {
        final ConcurrentLinkedDeque<JobID> submittedJobIds = new ConcurrentLinkedDeque<>();

        final AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    shutdownCalled.set(true);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setSubmitFunction(
                                jobGraph -> {
                                    submittedJobIds.add(jobGraph.getJobID());
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setRequestJobStatusFunction(
                                jobId -> {
                                    // we only fail one of the jobs, the first one
                                    if (jobId.equals(submittedJobIds.peek())) {
                                        return CompletableFuture.completedFuture(JobStatus.FAILED);
                                    }
                                    // never finish the other jobs
                                    return CompletableFuture.completedFuture(JobStatus.RUNNING);
                                })
                        .setRequestJobResultFunction(
                                jobId -> {
                                    // we only fail one of the jobs, the first one
                                    if (jobId.equals(submittedJobIds.peek())) {
                                        return CompletableFuture.completedFuture(
                                                createFailedJobResult(jobId));
                                    }
                                    // never finish the other jobs
                                    return new CompletableFuture<>();
                                })
                        .setCancelJobFunction(
                                jobId ->
                                        FutureUtils.completedExceptionally(
                                                new FlinkRuntimeException("Test exception.")));

        final PackagedProgramApplication application =
                new PackagedProgramApplication(
                        new ApplicationID(),
                        getProgram(2),
                        Collections.emptyList(),
                        getConfiguration(),
                        true,
                        false,
                        false,
                        true);

        // change mainThreadExecutor to be manually triggered
        // so that the application finish process isn't completed before adding the jobs
        mainThreadExecutor = new ManuallyTriggeredScheduledExecutor();
        final ManuallyTriggeredScheduledExecutor manuallyTriggeredMainThreadExecutor =
                (ManuallyTriggeredScheduledExecutor) mainThreadExecutor;

        // wait for the application execution to be accepted
        final CompletableFuture<Void> errorHandlerFuture = new CompletableFuture<>();
        application
                .execute(
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        mainThreadExecutor,
                        errorHandlerFuture::completeExceptionally)
                .join();

        // Wait for the task that is used to finish the application.
        while (manuallyTriggeredMainThreadExecutor.numQueuedRunnables() < 1) {
            Thread.sleep(100);
        }
        submittedJobIds.forEach(application::addJob);

        // Triggers the process to finish the application after adding the jobs. This
        // ensures that the application knows that these jobs need to be canceled.
        manuallyTriggeredMainThreadExecutor.trigger();
        final UnsuccessfulExecutionException exception =
                assertException(
                        application.getApplicationCompletionFuture(),
                        UnsuccessfulExecutionException.class);
        assertThat(exception.getStatus().orElse(null)).isEqualTo(JobStatus.FAILED);

        // Wait for the task that is used to transition to the final state.
        while (manuallyTriggeredMainThreadExecutor.numQueuedRunnables() < 1) {
            Thread.sleep(100);
        }
        manuallyTriggeredMainThreadExecutor.trigger();

        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFailing(application);

        // we call the error handler
        assertException(errorHandlerFuture, FlinkRuntimeException.class);

        // we didn't shut down the cluster
        assertThat(shutdownCalled.get()).isFalse();
    }

    @Test
    void testApplicationFailsAsSoonAsOneJobFails() throws Throwable {
        final ConcurrentLinkedDeque<JobID> submittedJobIds = new ConcurrentLinkedDeque<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> {
                                    submittedJobIds.add(jobGraph.getJobID());
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setRequestJobStatusFunction(
                                jobId -> {
                                    // we only fail one of the jobs, the first one
                                    if (jobId.equals(submittedJobIds.peek())) {
                                        return CompletableFuture.completedFuture(JobStatus.FAILED);
                                    }
                                    // never finish the other jobs
                                    return CompletableFuture.completedFuture(JobStatus.RUNNING);
                                })
                        .setRequestJobResultFunction(
                                jobId -> {
                                    // we only fail one of the jobs, the first one
                                    if (jobId.equals(submittedJobIds.peek())) {
                                        return CompletableFuture.completedFuture(
                                                createFailedJobResult(jobId));
                                    }
                                    // never finish the other jobs
                                    return new CompletableFuture<>();
                                });

        final PackagedProgramApplication application =
                createAndExecuteApplication(2, dispatcherBuilder.build());
        final UnsuccessfulExecutionException exception =
                assertException(
                        application.getApplicationCompletionFuture(),
                        UnsuccessfulExecutionException.class);
        assertThat(exception.getStatus().orElse(null)).isEqualTo(JobStatus.FAILED);
        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFailed(application);
    }

    @Test
    void testApplicationFinishesAfterAllJobsTerminate() throws Throwable {
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                DeploymentOptions.TERMINATE_APPLICATION_ON_ANY_JOB_EXCEPTION, false);

        final ConcurrentLinkedDeque<JobID> submittedJobIds = new ConcurrentLinkedDeque<>();

        final CompletableFuture<ApplicationStatus> clusterShutdownStatus =
                new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> {
                                    submittedJobIds.add(jobGraph.getJobID());
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setRequestJobStatusFunction(
                                jobId -> {
                                    // we only fail one of the jobs, the first one
                                    if (jobId.equals(submittedJobIds.peek())) {
                                        return CompletableFuture.completedFuture(JobStatus.FAILED);
                                    }
                                    // finish the other jobs
                                    return CompletableFuture.completedFuture(JobStatus.FINISHED);
                                })
                        .setRequestJobResultFunction(
                                jobId -> {
                                    // we only fail one of the jobs, the first one
                                    if (jobId.equals(submittedJobIds.peek())) {
                                        return CompletableFuture.completedFuture(
                                                createFailedJobResult(jobId));
                                    }
                                    // finish the other jobs
                                    return CompletableFuture.completedFuture(
                                            createJobResult(jobId, JobStatus.FINISHED));
                                })
                        .setClusterShutdownFunction(
                                status -> {
                                    clusterShutdownStatus.complete(status);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final PackagedProgramApplication application =
                createAndExecuteApplication(2, configurationUnderTest, dispatcherBuilder.build());

        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFinished(application);

        assertThat(clusterShutdownStatus.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(ApplicationStatus.SUCCEEDED);
    }

    @Test
    void testApplicationSucceedsWhenAllJobsSucceed() throws Exception {
        final PackagedProgramApplication application =
                createAndExecuteApplication(3, finishedJobGatewayBuilder().build());

        // this would block indefinitely if the applications don't finish
        application.getApplicationCompletionFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFinished(application);
    }

    @Test
    void testDispatcherIsCancelledWhenOneJobIsCancelled() throws Exception {
        final CompletableFuture<ApplicationStatus> clusterShutdownStatus =
                new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                canceledJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    clusterShutdownStatus.complete(status);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        PackagedProgramApplication application =
                createAndExecuteApplication(3, dispatcherBuilder.build());

        // wait until the application "thinks" it's done, also makes sure that we don't
        // fail the future exceptionally with a JobCancelledException
        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationCanceled(application);

        assertThat(clusterShutdownStatus.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(ApplicationStatus.CANCELED);
    }

    @Test
    void testApplicationTaskFinishesWhenApplicationFinishes() throws Exception {
        final TestingDispatcherGateway.Builder dispatcherBuilder = finishedJobGatewayBuilder();

        PackagedProgramApplication application =
                createAndExecuteApplication(3, dispatcherBuilder.build());

        final CompletableFuture<Acknowledge> completionFuture =
                application.getFinishApplicationFuture();

        ScheduledFuture<?> applicationExecutionFuture = application.getApplicationExecutionFuture();

        // wait until the application "thinks" it's done
        completionFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFinished(application);

        // make sure the task finishes
        applicationExecutionFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void testApplicationCancellationWhenNotRunning() throws Exception {
        final PackagedProgramApplication application =
                new PackagedProgramApplication(
                        new ApplicationID(),
                        getProgram(1),
                        Collections.emptyList(),
                        getConfiguration(),
                        true,
                        false,
                        false,
                        true);

        application.cancel();

        assertApplicationCanceled(application);
    }

    @Test
    void testApplicationIsCanceledWhenCancellingApplication() throws Exception {
        final ConcurrentLinkedDeque<JobID> submittedJobIds = new ConcurrentLinkedDeque<>();
        final ConcurrentLinkedDeque<JobID> canceledJobIds = new ConcurrentLinkedDeque<>();

        final AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                runningJobGatewayBuilder()
                        .setSubmitFunction(
                                jobGraph -> {
                                    submittedJobIds.add(jobGraph.getJobID());
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setRequestJobStatusFunction(
                                jobId -> {
                                    if (canceledJobIds.contains(jobId)) {
                                        return CompletableFuture.completedFuture(
                                                JobStatus.CANCELED);
                                    }
                                    return CompletableFuture.completedFuture(JobStatus.RUNNING);
                                })
                        .setRequestJobResultFunction(
                                jobId -> {
                                    // all jobs should be canceled
                                    return CompletableFuture.completedFuture(
                                            createCanceledJobResult(jobId));
                                })
                        .setCancelJobFunction(
                                jobId -> {
                                    canceledJobIds.add(jobId);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .setClusterShutdownFunction(
                                status -> {
                                    shutdownCalled.set(true);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final ManuallyTriggeredScheduledExecutor manuallyTriggeredExecutor =
                new ManuallyTriggeredScheduledExecutor();
        // we're "listening" on this to be completed to verify that the error handler is called.
        // In production, this will shut down the cluster with an exception.
        final CompletableFuture<Void> errorHandlerFuture = new CompletableFuture<>();
        final PackagedProgramApplication application =
                createAndExecuteApplication(
                        3,
                        dispatcherBuilder.build(),
                        manuallyTriggeredExecutor,
                        errorHandlerFuture::completeExceptionally);

        final CompletableFuture<Acknowledge> completionFuture =
                application.getFinishApplicationFuture();

        ScheduledFuture<?> applicationExecutionFuture = application.getApplicationExecutionFuture();

        CompletableFuture.runAsync(
                        () -> {
                            try {
                                application.cancel();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        },
                        mainThreadExecutor)
                .join();

        // Triggers the scheduled process after calling cancel. This
        // ensures that the application task isn't completed before the cancel method is called
        // which would prevent the cancel call from cancelling the task's future.
        manuallyTriggeredExecutor.triggerNonPeriodicScheduledTask();

        // we didn't call the error handler
        assertThat(errorHandlerFuture.isDone()).isFalse();

        // Wait to trigger the task that is used to finish the application. .
        while (manuallyTriggeredExecutor.numQueuedRunnables() < 1) {
            Thread.sleep(100);
        }
        manuallyTriggeredExecutor.trigger();

        // completion future gets completed normally
        completionFuture.get();

        // verify that we shut down the cluster
        assertThat(shutdownCalled.get()).isTrue();

        // verify that the application task is being cancelled
        assertThat(applicationExecutionFuture.isCancelled()).isTrue();
        assertThat(applicationExecutionFuture.isDone()).isTrue();
        assertThat(canceledJobIds).containsExactlyInAnyOrderElementsOf(submittedJobIds);
        assertApplicationCanceled(application);
    }

    @Test
    void testApplicationDispose() throws Exception {
        final AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                runningJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    shutdownCalled.set(true);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final ManuallyTriggeredScheduledExecutor manuallyTriggeredExecutor =
                new ManuallyTriggeredScheduledExecutor();
        // we're "listening" on this to be completed to verify that the error handler is called.
        // In production, this will shut down the cluster with an exception.
        final CompletableFuture<Void> errorHandlerFuture = new CompletableFuture<>();
        final PackagedProgramApplication application =
                createAndExecuteApplication(
                        3,
                        dispatcherBuilder.build(),
                        manuallyTriggeredExecutor,
                        errorHandlerFuture::completeExceptionally);

        final CompletableFuture<Acknowledge> completionFuture =
                application.getFinishApplicationFuture();

        ScheduledFuture<?> applicationExecutionFuture = application.getApplicationExecutionFuture();

        application.dispose();

        // Triggers the scheduled process after calling cancel. This
        // ensures that the application task isn't completed before the cancel method is called
        // which would prevent the cancel call from cancelling the task's future.
        manuallyTriggeredExecutor.triggerNonPeriodicScheduledTask();

        // we didn't call the error handler
        assertThat(errorHandlerFuture.isDone()).isFalse();

        // completion future gets completed normally
        completionFuture.get();

        // verify that we didn't shut down the cluster
        assertThat(shutdownCalled.get()).isFalse();

        // verify that the application task is being cancelled
        assertThat(applicationExecutionFuture.isCancelled()).isTrue();
        assertThat(applicationExecutionFuture.isDone()).isTrue();
    }

    @Test
    void testErrorHandlerIsNotCalledWhenSubmissionThrowsAnException() throws Exception {
        final AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                runningJobGatewayBuilder()
                        .setSubmitFunction(
                                jobGraph -> {
                                    throw new FlinkRuntimeException("Nope!");
                                })
                        .setClusterShutdownFunction(
                                status -> {
                                    shutdownCalled.set(true);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        // we're "listening" on this to be completed to verify that the error handler is called.
        // In production, this will shut down the cluster with an exception.
        final CompletableFuture<Void> errorHandlerFuture = new CompletableFuture<>();
        final PackagedProgramApplication application =
                createAndExecuteApplication(
                        2,
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        errorHandlerFuture::completeExceptionally);

        final CompletableFuture<Acknowledge> completionFuture =
                application.getFinishApplicationFuture();

        // we return a future that is completed normally
        completionFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFailed(application);

        // we do not call the error handler
        assertThat(errorHandlerFuture.isDone()).isFalse();

        // verify that we shut down the cluster
        assertThat(shutdownCalled.get()).isTrue();
    }

    @Test
    void testErrorHandlerIsCalledWhenShutdownCompletesExceptionally() throws Exception {
        testErrorHandlerIsCalled(
                () ->
                        FutureUtils.completedExceptionally(
                                new FlinkRuntimeException("Test exception.")));
    }

    @Test
    void testErrorHandlerIsCalledWhenShutdownThrowsAnException() throws Exception {
        testErrorHandlerIsCalled(
                () -> {
                    throw new FlinkRuntimeException("Test exception.");
                });
    }

    private void testErrorHandlerIsCalled(Supplier<CompletableFuture<Acknowledge>> shutdownFunction)
            throws Exception {
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
                        .setRequestJobStatusFunction(
                                jobId -> CompletableFuture.completedFuture(JobStatus.FINISHED))
                        .setRequestJobResultFunction(
                                jobId ->
                                        CompletableFuture.completedFuture(
                                                createJobResult(jobId, JobStatus.FINISHED)))
                        .setClusterShutdownFunction(status -> shutdownFunction.get());

        // we're "listening" on this to be completed to verify that the error handler is called.
        // In production, this will shut down the cluster with an exception.
        final CompletableFuture<Void> errorHandlerFuture = new CompletableFuture<>();
        final TestingDispatcherGateway dispatcherGateway = dispatcherBuilder.build();
        final PackagedProgramApplication application =
                createAndExecuteApplication(
                        3,
                        dispatcherGateway,
                        scheduledExecutor,
                        errorHandlerFuture::completeExceptionally);

        final CompletableFuture<Acknowledge> completionFuture =
                application.getFinishApplicationFuture();

        // we call the error handler
        assertException(errorHandlerFuture, FlinkRuntimeException.class);

        // we return a future that is completed exceptionally
        assertException(completionFuture, FlinkRuntimeException.class);

        // shut down exception should not affect application result
        assertApplicationFinished(application);
    }

    @Test
    void testClusterIsShutdownInAttachedModeWhenJobCancelled() throws Exception {
        final CompletableFuture<ApplicationStatus> clusterShutdown = new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherGatewayBuilder =
                canceledJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    clusterShutdown.complete(status);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final Configuration configuration = getConfiguration();
        configuration.set(DeploymentOptions.ATTACHED, true);

        final PackagedProgramApplication application =
                createAndExecuteApplication(2, configuration, dispatcherGatewayBuilder.build());
        assertException(
                application.getApplicationCompletionFuture(), UnsuccessfulExecutionException.class);

        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationCanceled(application);

        assertThat(clusterShutdown.get()).isEqualTo(ApplicationStatus.CANCELED);
    }

    @Test
    void testClusterShutdownWhenApplicationSucceeds() throws Exception {
        // we're "listening" on this to be completed to verify that the cluster
        // is being shut down from the PackagedProgramApplication
        final CompletableFuture<ApplicationStatus> externalShutdownFuture =
                new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                finishedJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    externalShutdownFuture.complete(status);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final PackagedProgramApplication application =
                createAndExecuteApplication(3, dispatcherBuilder.build());

        // wait until the application "thinks" it's done
        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFinished(application);

        // verify that the dispatcher is actually being shut down
        assertThat(externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(ApplicationStatus.SUCCEEDED);
    }

    @Test
    void testClusterShutdownWhenApplicationFails() throws Exception {
        // we're "listening" on this to be completed to verify that the cluster
        // is being shut down from the PackagedProgramApplication
        final CompletableFuture<ApplicationStatus> externalShutdownFuture =
                new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                failedJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    externalShutdownFuture.complete(status);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final PackagedProgramApplication application =
                createAndExecuteApplication(3, dispatcherBuilder.build());

        // wait until the application "thinks" it's done
        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFailed(application);

        // verify that the dispatcher is actually being shut down
        assertThat(externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(ApplicationStatus.FAILED);
    }

    @Test
    void testClusterShutdownWhenApplicationGetsCancelled() throws Exception {
        // we're "listening" on this to be completed to verify that the cluster
        // is being shut down from the PackagedProgramApplication
        final CompletableFuture<ApplicationStatus> externalShutdownFuture =
                new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                canceledJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    externalShutdownFuture.complete(status);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final PackagedProgramApplication application =
                createAndExecuteApplication(3, dispatcherBuilder.build());

        // wait until the application "thinks" it's done
        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationCanceled(application);

        // verify that the dispatcher is actually being shut down
        assertThat(externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(ApplicationStatus.CANCELED);
    }

    @Test
    void testErrorHandlerIsNotCalledWhenApplicationStatusIsUnknown() throws Exception {
        final AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                canceledJobGatewayBuilder()
                        .setRequestJobResultFunction(
                                jobID ->
                                        CompletableFuture.completedFuture(
                                                createUnknownJobResult(jobID)))
                        .setClusterShutdownFunction(
                                status -> {
                                    shutdownCalled.set(true);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final TestingDispatcherGateway dispatcherGateway = dispatcherBuilder.build();
        final CompletableFuture<Void> errorHandlerFuture = new CompletableFuture<>();
        final PackagedProgramApplication application =
                createAndExecuteApplication(
                        3,
                        dispatcherGateway,
                        scheduledExecutor,
                        errorHandlerFuture::completeExceptionally);

        // check that application completes exceptionally
        assertException(
                application.getApplicationCompletionFuture(), UnsuccessfulExecutionException.class);

        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFailed(application);

        // we do not call the error handler
        assertThat(errorHandlerFuture.isDone()).isFalse();

        // verify that we shut down the cluster
        assertThat(shutdownCalled.get()).isTrue();
    }

    @Test
    void testErrorHandlerIsNotCalled() throws Exception {
        final AtomicBoolean shutdownCalled = new AtomicBoolean(false);
        // Job submission error
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                finishedJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    shutdownCalled.set(true);
                                    throw new FlinkRuntimeException("Nope!");
                                });

        final AtomicBoolean errorHandlerCalled = new AtomicBoolean(false);
        final PackagedProgramApplication application =
                createAndExecuteApplication(
                        2,
                        getConfiguration(),
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        exception -> errorHandlerCalled.set(true),
                        false,
                        false,
                        true);

        final CompletableFuture<Acknowledge> completionFuture =
                application.getFinishApplicationFuture();

        // we return a future that is completed exceptionally
        assertException(completionFuture, FlinkRuntimeException.class);
        // shut down exception should not affect application result
        assertApplicationFinished(application);

        // we do not call the error handler
        assertThat(errorHandlerCalled.get()).isFalse();

        // verify that we shut down the cluster
        assertThat(shutdownCalled.get()).isTrue();
    }

    @Test
    void testDuplicateJobSubmissionWithTerminatedJobId() throws Throwable {
        final JobID testJobID = new JobID(0, 2);
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                finishedJobGatewayBuilder()
                        .setSubmitFunction(
                                jobGraph ->
                                        FutureUtils.completedExceptionally(
                                                DuplicateJobSubmissionException
                                                        .ofGloballyTerminated(testJobID)));

        final PackagedProgramApplication application =
                createAndExecuteApplication(
                        1, configurationUnderTest, dispatcherBuilder.build(), true);

        application.getApplicationCompletionFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFinished(application);
    }

    /**
     * In this scenario, job result is no longer present in the {@link
     * org.apache.flink.runtime.dispatcher.Dispatcher dispatcher} (job has terminated and job
     * manager failed over), but we know that job has already terminated from {@link
     * org.apache.flink.runtime.highavailability.JobResultStore}.
     */
    @Test
    void testDuplicateJobSubmissionWithTerminatedJobIdWithUnknownResult() throws Throwable {
        final JobID testJobID = new JobID(0, 2);
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph ->
                                        FutureUtils.completedExceptionally(
                                                DuplicateJobSubmissionException
                                                        .ofGloballyTerminated(testJobID)))
                        .setRequestJobStatusFunction(
                                jobId ->
                                        FutureUtils.completedExceptionally(
                                                new FlinkJobNotFoundException(jobId)))
                        .setRequestJobResultFunction(
                                jobId ->
                                        FutureUtils.completedExceptionally(
                                                new FlinkJobNotFoundException(jobId)));

        final PackagedProgramApplication application =
                createAndExecuteApplication(
                        1, configurationUnderTest, dispatcherBuilder.build(), true);

        application.getApplicationCompletionFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFinished(application);
    }

    @Test
    void testDuplicateJobSubmissionWithRunningJobId() throws Throwable {
        final JobID testJobID = new JobID(0, 2);
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph ->
                                        FutureUtils.completedExceptionally(
                                                DuplicateJobSubmissionException.of(testJobID)));

        final PackagedProgramApplication application =
                createAndExecuteApplication(1, configurationUnderTest, dispatcherBuilder.build());
        final CompletableFuture<Void> applicationFuture =
                application.getApplicationCompletionFuture();
        final ExecutionException executionException =
                assertThrows(
                        ExecutionException.class,
                        () -> applicationFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        final Optional<DuplicateJobSubmissionException> maybeDuplicate =
                ExceptionUtils.findThrowable(
                        executionException, DuplicateJobSubmissionException.class);
        assertThat(maybeDuplicate).isPresent();
        assertThat(maybeDuplicate.get().isGloballyTerminated()).isFalse();

        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFailed(application);
    }

    @ParameterizedTest
    @EnumSource(
            value = JobStatus.class,
            names = {"FINISHED", "CANCELED", "FAILED"})
    void testShutdownDisabled(JobStatus jobStatus) throws Exception {
        final TestingDispatcherGateway dispatcherGateway =
                dispatcherGatewayBuilder(jobStatus)
                        .setClusterShutdownFunction(
                                status -> {
                                    fail("Cluster shutdown should not be called");
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        final PackagedProgramApplication application =
                createAndExecuteApplication(
                        1,
                        getConfiguration(),
                        dispatcherGateway,
                        scheduledExecutor,
                        exception -> {},
                        true,
                        false,
                        false);

        // Wait until application is finished to make sure cluster shutdown isn't called
        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationStatus(application, ApplicationState.fromJobStatus(jobStatus));
    }

    @Test
    void testSubmitFailedJobOnApplicationErrorWhenEnforceSingleJobExecution() throws Exception {
        final Configuration configuration = getConfiguration();
        final JobID jobId = new JobID();
        configuration.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, jobId.toHexString());
        testSubmitFailedJobOnApplicationError(
                configuration,
                (id, t) -> {
                    assertThat(id).isEqualTo(jobId);
                    assertThat(t)
                            .isInstanceOf(ProgramInvocationException.class)
                            .hasRootCauseInstanceOf(RuntimeException.class)
                            .hasRootCauseMessage(FailingJob.EXCEPTION_MESSAGE);
                });
    }

    private void testSubmitFailedJobOnApplicationError(
            Configuration configuration, BiConsumer<JobID, Throwable> failedJobAssertion)
            throws Exception {
        final CompletableFuture<Void> submitted = new CompletableFuture<>();
        final TestingDispatcherGateway dispatcherGateway =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFailedFunction(
                                (jobId, jobName, t) -> {
                                    try {
                                        failedJobAssertion.accept(jobId, t);
                                        submitted.complete(null);
                                        return CompletableFuture.completedFuture(Acknowledge.get());
                                    } catch (Throwable assertion) {
                                        submitted.completeExceptionally(assertion);
                                        return FutureUtils.completedExceptionally(assertion);
                                    }
                                })
                        .setRequestJobStatusFunction(
                                jobId -> submitted.thenApply(ignored -> JobStatus.FAILED))
                        .setRequestJobResultFunction(
                                jobId ->
                                        submitted.thenApply(
                                                ignored ->
                                                        createJobResult(jobId, JobStatus.FAILED)))
                        .build();

        final PackagedProgramApplication application =
                new PackagedProgramApplication(
                        new ApplicationID(),
                        FailingJob.getProgram(),
                        Collections.emptyList(),
                        configuration,
                        true,
                        true /* enforceSingleJobExecution */,
                        true /* submitFailedJobOnApplicationError */,
                        true);

        executeApplication(application, dispatcherGateway, scheduledExecutor, exception -> {});

        application.getFinishApplicationFuture().get();
        assertApplicationFailed(application);
    }

    @Test
    void testSubmitFailedJobOnApplicationErrorWhenNotEnforceSingleJobExecution() throws Exception {
        final PackagedProgramApplication application =
                new PackagedProgramApplication(
                        new ApplicationID(),
                        FailingJob.getProgram(),
                        Collections.emptyList(),
                        getConfiguration(),
                        true,
                        false /* enforceSingleJobExecution */,
                        true /* submitFailedJobOnApplicationError */,
                        true);

        executeApplication(
                application,
                TestingDispatcherGateway.newBuilder().build(),
                scheduledExecutor,
                exception -> {});

        assertThatFuture(application.getApplicationCompletionFuture())
                .eventuallyFailsWith(ExecutionException.class)
                .extracting(Throwable::getCause)
                .satisfies(
                        e ->
                                assertThat(e)
                                        .hasMessageContaining(
                                                DeploymentOptions
                                                        .SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR
                                                        .key()));

        application.getFinishApplicationFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        assertApplicationFailed(application);
    }

    private TestingDispatcherGateway.Builder finishedJobGatewayBuilder() {
        return dispatcherGatewayBuilder(JobStatus.FINISHED);
    }

    private TestingDispatcherGateway.Builder failedJobGatewayBuilder() {
        return dispatcherGatewayBuilder(JobStatus.FAILED);
    }

    private TestingDispatcherGateway.Builder canceledJobGatewayBuilder() {
        return dispatcherGatewayBuilder(JobStatus.CANCELED);
    }

    private TestingDispatcherGateway.Builder runningJobGatewayBuilder() {
        return dispatcherGatewayBuilder(JobStatus.RUNNING);
    }

    private TestingDispatcherGateway.Builder dispatcherGatewayBuilder(JobStatus jobStatus) {
        TestingDispatcherGateway.Builder builder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()))
                        .setRequestJobStatusFunction(
                                jobId -> CompletableFuture.completedFuture(jobStatus));
        if (jobStatus != JobStatus.RUNNING) {
            builder.setRequestJobResultFunction(
                    jobID -> CompletableFuture.completedFuture(createJobResult(jobID, jobStatus)));
        }
        return builder;
    }

    private PackagedProgramApplication createAndExecuteApplication(
            final int numJobs, final DispatcherGateway dispatcherGateway) throws FlinkException {
        return createAndExecuteApplication(
                numJobs, getConfiguration(), dispatcherGateway, scheduledExecutor, exception -> {});
    }

    private PackagedProgramApplication createAndExecuteApplication(
            final int numJobs,
            final Configuration configuration,
            final DispatcherGateway dispatcherGateway)
            throws FlinkException {
        return createAndExecuteApplication(numJobs, configuration, dispatcherGateway, false);
    }

    private PackagedProgramApplication createAndExecuteApplication(
            final int numJobs,
            final Configuration configuration,
            final DispatcherGateway dispatcherGateway,
            final boolean enforceSingleJobExecution)
            throws FlinkException {
        return createAndExecuteApplication(
                numJobs,
                configuration,
                dispatcherGateway,
                scheduledExecutor,
                exception -> {},
                true,
                enforceSingleJobExecution,
                true);
    }

    private PackagedProgramApplication createAndExecuteApplication(
            final int numJobs,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final FatalErrorHandler errorHandler)
            throws FlinkException {
        return createAndExecuteApplication(
                numJobs, getConfiguration(), dispatcherGateway, scheduledExecutor, errorHandler);
    }

    private PackagedProgramApplication createAndExecuteApplication(
            final int numJobs,
            final Configuration configuration,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final FatalErrorHandler errorHandler)
            throws FlinkException {
        return createAndExecuteApplication(
                numJobs,
                configuration,
                dispatcherGateway,
                scheduledExecutor,
                errorHandler,
                true,
                false,
                true);
    }

    private PackagedProgramApplication createAndExecuteApplication(
            final int numJobs,
            final Configuration configuration,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final FatalErrorHandler errorHandler,
            boolean handleFatalError,
            boolean enforceSingleJobExecution,
            boolean shutDownOnFinish)
            throws FlinkException {

        final PackagedProgram program = getProgram(numJobs);

        final PackagedProgramApplication application =
                new PackagedProgramApplication(
                        new ApplicationID(),
                        program,
                        Collections.emptyList(),
                        configuration,
                        handleFatalError,
                        enforceSingleJobExecution,
                        false,
                        shutDownOnFinish);
        assertApplicationCreated(application);

        executeApplication(application, dispatcherGateway, scheduledExecutor, errorHandler);
        return application;
    }

    private void executeApplication(
            final PackagedProgramApplication application,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final FatalErrorHandler errorHandler) {

        CompletableFuture.runAsync(
                        () ->
                                application.execute(
                                        dispatcherGateway,
                                        scheduledExecutor,
                                        mainThreadExecutor,
                                        errorHandler),
                        mainThreadExecutor)
                .join();
    }

    private PackagedProgram getProgram(int numJobs) throws FlinkException {
        return MultiExecuteJob.getProgram(numJobs, true);
    }

    private static JobResult createFailedJobResult(final JobID jobId) {
        return createJobResult(jobId, JobStatus.FAILED);
    }

    private static JobResult createCanceledJobResult(final JobID jobId) {
        return createJobResult(jobId, JobStatus.CANCELED);
    }

    private static JobResult createUnknownJobResult(final JobID jobId) {
        return createJobResult(jobId, null);
    }

    private static JobResult createJobResult(
            final JobID jobID, @Nullable final JobStatus jobStatus) {
        JobResult.Builder builder =
                new JobResult.Builder().jobId(jobID).netRuntime(2L).jobStatus(jobStatus);
        if (jobStatus == JobStatus.CANCELED) {
            builder.serializedThrowable(
                    new SerializedThrowable(new JobCancellationException(jobID, "Hello", null)));
        } else if (jobStatus == JobStatus.FAILED || jobStatus == null) {
            builder.serializedThrowable(
                    new SerializedThrowable(new JobExecutionException(jobID, "bla bla bla")));
        }
        return builder.build();
    }

    private static <T, E extends Throwable> E assertException(
            CompletableFuture<T> future, Class<E> exceptionClass) throws Exception {

        try {
            future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Throwable e) {
            Optional<E> maybeException = ExceptionUtils.findThrowable(e, exceptionClass);
            if (!maybeException.isPresent()) {
                throw e;
            }
            return maybeException.get();
        }
        throw new Exception(
                "Future should have completed exceptionally with "
                        + exceptionClass.getCanonicalName()
                        + ".");
    }

    private Configuration getConfiguration() {
        final Configuration configuration = new Configuration();
        configuration.set(DeploymentOptions.TARGET, EmbeddedExecutor.NAME);
        return configuration;
    }

    private void assertApplicationStatus(
            PackagedProgramApplication application, ApplicationState expectedStatus) {
        assertThat(application.getApplicationStatus()).isEqualTo(expectedStatus);
    }

    private void assertApplicationCreated(PackagedProgramApplication application) {
        assertApplicationStatus(application, ApplicationState.CREATED);
    }

    private void assertApplicationFailing(PackagedProgramApplication application) {
        assertApplicationStatus(application, ApplicationState.FAILING);
    }

    private void assertApplicationCanceled(PackagedProgramApplication application) {
        assertApplicationStatus(application, ApplicationState.CANCELED);
    }

    private void assertApplicationFailed(PackagedProgramApplication application) {
        assertApplicationStatus(application, ApplicationState.FAILED);
    }

    private void assertApplicationFinished(PackagedProgramApplication application) {
        assertApplicationStatus(application, ApplicationState.FINISHED);
    }
}
