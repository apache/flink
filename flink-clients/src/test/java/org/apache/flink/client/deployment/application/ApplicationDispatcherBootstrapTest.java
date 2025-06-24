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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.client.testjar.FailingJob;
import org.apache.flink.client.testjar.MultiExecuteJob;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.DispatcherGateway;
import org.apache.flink.runtime.jobmanager.HighAvailabilityMode;
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

import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
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

/** Tests for the {@link ApplicationDispatcherBootstrap}. */
class ApplicationDispatcherBootstrapTest {

    private static final int TIMEOUT_SECONDS = 10;

    private final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
    private final ScheduledExecutor scheduledExecutor =
            new ScheduledExecutorServiceAdapter(executor);

    @AfterEach
    void cleanup() {
        ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
    }

    @Test
    void testExceptionThrownWhenApplicationContainsNoJobs() throws Throwable {
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()));

        final CompletableFuture<Void> applicationFuture = runApplication(dispatcherBuilder, 0);

        assertException(applicationFuture, ApplicationExecutionException.class);
    }

    @Test
    void testOnlyOneJobIsAllowedWithHa() throws Throwable {
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());

        final CompletableFuture<Void> applicationFuture = runApplication(configurationUnderTest, 2);

        assertException(applicationFuture, FlinkRuntimeException.class);
    }

    @Test
    void testOnlyOneJobAllowedWithStaticJobId() throws Throwable {
        final JobID testJobID = new JobID(0, 2);

        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());

        final CompletableFuture<Void> applicationFuture = runApplication(configurationUnderTest, 2);

        assertException(applicationFuture, FlinkRuntimeException.class);
    }

    @Test
    void testOnlyOneJobAllowedWithStaticJobIdAndHa() throws Throwable {
        final JobID testJobID = new JobID(0, 2);

        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());
        configurationUnderTest.set(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());

        final CompletableFuture<Void> applicationFuture = runApplication(configurationUnderTest, 2);

        assertException(applicationFuture, FlinkRuntimeException.class);
    }

    @Test
    void testJobIdDefaultsToClusterIdWithHa() throws Throwable {
        final Configuration configurationUnderTest = getConfiguration();
        final String clusterId = "cluster";
        configurationUnderTest.set(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configurationUnderTest.set(HighAvailabilityOptions.HA_CLUSTER_ID, clusterId);

        final CompletableFuture<JobID> submittedJobId = new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                finishedJobGatewayBuilder()
                        .setSubmitFunction(
                                jobGraph -> {
                                    submittedJobId.complete(jobGraph.getJobID());
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final CompletableFuture<Void> applicationFuture =
                runApplication(dispatcherBuilder, configurationUnderTest, 1);

        applicationFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(submittedJobId.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(new JobID(clusterId.hashCode(), 0L));
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

        final CompletableFuture<Void> applicationFuture =
                runApplication(dispatcherBuilder, configurationUnderTest, 1);

        applicationFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(submittedJobId.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(new JobID(0L, 2L));
    }

    @Test
    void testStaticJobIdWithHa() throws Throwable {
        final JobID testJobID = new JobID(0, 2);

        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());
        configurationUnderTest.set(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());

        final CompletableFuture<JobID> submittedJobId = new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                finishedJobGatewayBuilder()
                        .setSubmitFunction(
                                jobGraph -> {
                                    submittedJobId.complete(jobGraph.getJobID());
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final CompletableFuture<Void> applicationFuture =
                runApplication(dispatcherBuilder, configurationUnderTest, 1);

        applicationFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(submittedJobId.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(new JobID(0L, 2L));
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
                                    // we only fail one of the jobs, the first one, the others will
                                    // "keep" running
                                    // indefinitely
                                    if (jobId.equals(submittedJobIds.peek())) {
                                        return CompletableFuture.completedFuture(JobStatus.FAILED);
                                    }
                                    // never finish the other jobs
                                    return CompletableFuture.completedFuture(JobStatus.RUNNING);
                                })
                        .setRequestJobResultFunction(
                                jobId -> {
                                    // we only fail one of the jobs, the first one, the other will
                                    // "keep" running
                                    // indefinitely. If we didn't have this the test would hang
                                    // forever.
                                    if (jobId.equals(submittedJobIds.peek())) {
                                        return CompletableFuture.completedFuture(
                                                createFailedJobResult(jobId));
                                    }
                                    // never finish the other jobs
                                    return new CompletableFuture<>();
                                });

        final CompletableFuture<Void> applicationFuture = runApplication(dispatcherBuilder, 2);
        final UnsuccessfulExecutionException exception =
                assertException(applicationFuture, UnsuccessfulExecutionException.class);
        assertThat(exception.getStatus()).isEqualTo(ApplicationStatus.FAILED);
    }

    @Test
    void testApplicationSucceedsWhenAllJobsSucceed() throws Exception {
        final TestingDispatcherGateway.Builder dispatcherBuilder = finishedJobGatewayBuilder();

        final CompletableFuture<Void> applicationFuture = runApplication(dispatcherBuilder, 3);

        // this would block indefinitely if the applications don't finish
        applicationFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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

        ApplicationDispatcherBootstrap bootstrap =
                createApplicationDispatcherBootstrap(
                        3, dispatcherBuilder.build(), scheduledExecutor);

        final CompletableFuture<Acknowledge> completionFuture =
                bootstrap.getBootstrapCompletionFuture();

        // wait until the bootstrap "thinks" it's done, also makes sure that we don't
        // fail the future exceptionally with a JobCancelledException
        completionFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        assertThat(clusterShutdownStatus.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(ApplicationStatus.CANCELED);
    }

    @Test
    void testApplicationTaskFinishesWhenApplicationFinishes() throws Exception {
        final TestingDispatcherGateway.Builder dispatcherBuilder = finishedJobGatewayBuilder();

        ApplicationDispatcherBootstrap bootstrap =
                createApplicationDispatcherBootstrap(
                        3, dispatcherBuilder.build(), scheduledExecutor);

        final CompletableFuture<Acknowledge> completionFuture =
                bootstrap.getBootstrapCompletionFuture();

        ScheduledFuture<?> applicationExecutionFuture = bootstrap.getApplicationExecutionFuture();

        // wait until the bootstrap "thinks" it's done
        completionFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // make sure the task finishes
        applicationExecutionFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void testApplicationIsStoppedWhenStoppingBootstrap() throws Exception {
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
        final ApplicationDispatcherBootstrap bootstrap =
                createApplicationDispatcherBootstrap(
                        3,
                        dispatcherBuilder.build(),
                        manuallyTriggeredExecutor,
                        errorHandlerFuture::completeExceptionally);

        final CompletableFuture<Acknowledge> completionFuture =
                bootstrap.getBootstrapCompletionFuture();

        ScheduledFuture<?> applicationExecutionFuture = bootstrap.getApplicationExecutionFuture();

        bootstrap.stop();

        // Triggers the scheduled ApplicationDispatcherBootstrap process after calling stop. This
        // ensures that the bootstrap task isn't completed before the stop method is called which
        // would prevent the stop call from cancelling the task's future.
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
    void testErrorHandlerIsCalledWhenSubmissionThrowsAnException() throws Exception {
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
        final ApplicationDispatcherBootstrap bootstrap =
                createApplicationDispatcherBootstrap(
                        2,
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        errorHandlerFuture::completeExceptionally);

        final CompletableFuture<Acknowledge> completionFuture =
                bootstrap.getBootstrapCompletionFuture();

        // we call the error handler
        assertException(errorHandlerFuture, FlinkRuntimeException.class);

        // we return a future that is completed exceptionally
        assertException(completionFuture, FlinkRuntimeException.class);

        // and cluster shutdown didn't get called
        assertThat(shutdownCalled.get()).isFalse();
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
                                                createJobResult(
                                                        jobId, ApplicationStatus.SUCCEEDED)))
                        .setClusterShutdownFunction(status -> shutdownFunction.get());

        // we're "listening" on this to be completed to verify that the error handler is called.
        // In production, this will shut down the cluster with an exception.
        final CompletableFuture<Void> errorHandlerFuture = new CompletableFuture<>();
        final TestingDispatcherGateway dispatcherGateway = dispatcherBuilder.build();
        final ApplicationDispatcherBootstrap bootstrap =
                createApplicationDispatcherBootstrap(
                        3,
                        dispatcherGateway,
                        scheduledExecutor,
                        errorHandlerFuture::completeExceptionally);

        final CompletableFuture<Acknowledge> completionFuture =
                bootstrap.getBootstrapCompletionFuture();

        // we call the error handler
        assertException(errorHandlerFuture, FlinkRuntimeException.class);

        // we return a future that is completed exceptionally
        assertException(completionFuture, FlinkRuntimeException.class);
    }

    @Test
    void testClusterIsShutdownInAttachedModeWhenJobCancelled() throws Exception {
        final CompletableFuture<ApplicationStatus> clusterShutdown = new CompletableFuture<>();

        final TestingDispatcherGateway dispatcherGateway =
                canceledJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    clusterShutdown.complete(status);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        final PackagedProgram program = getProgram(2);

        final Configuration configuration = getConfiguration();
        configuration.set(DeploymentOptions.ATTACHED, true);

        final ApplicationDispatcherBootstrap bootstrap =
                new ApplicationDispatcherBootstrap(
                        program,
                        Collections.emptyList(),
                        configuration,
                        dispatcherGateway,
                        scheduledExecutor,
                        e -> {});

        final CompletableFuture<Void> applicationFuture =
                bootstrap.getApplicationCompletionFuture();
        assertException(applicationFuture, UnsuccessfulExecutionException.class);

        assertThat(clusterShutdown.get()).isEqualTo(ApplicationStatus.CANCELED);
    }

    @Test
    void testClusterShutdownWhenApplicationSucceeds() throws Exception {
        // we're "listening" on this to be completed to verify that the cluster
        // is being shut down from the ApplicationDispatcherBootstrap
        final CompletableFuture<ApplicationStatus> externalShutdownFuture =
                new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                finishedJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    externalShutdownFuture.complete(status);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        ApplicationDispatcherBootstrap bootstrap =
                createApplicationDispatcherBootstrap(
                        3, dispatcherBuilder.build(), scheduledExecutor);

        final CompletableFuture<Acknowledge> completionFuture =
                bootstrap.getBootstrapCompletionFuture();

        // wait until the bootstrap "thinks" it's done
        completionFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // verify that the dispatcher is actually being shut down
        assertThat(externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(ApplicationStatus.SUCCEEDED);
    }

    @Test
    void testClusterShutdownWhenApplicationFails() throws Exception {
        // we're "listening" on this to be completed to verify that the cluster
        // is being shut down from the ApplicationDispatcherBootstrap
        final CompletableFuture<ApplicationStatus> externalShutdownFuture =
                new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                failedJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    externalShutdownFuture.complete(status);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        ApplicationDispatcherBootstrap bootstrap =
                createApplicationDispatcherBootstrap(
                        3, dispatcherBuilder.build(), scheduledExecutor);

        final CompletableFuture<Acknowledge> completionFuture =
                bootstrap.getBootstrapCompletionFuture();

        // wait until the bootstrap "thinks" it's done
        completionFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // verify that the dispatcher is actually being shut down
        assertThat(externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(ApplicationStatus.FAILED);
    }

    @Test
    void testClusterShutdownWhenApplicationGetsCancelled() throws Exception {
        // we're "listening" on this to be completed to verify that the cluster
        // is being shut down from the ApplicationDispatcherBootstrap
        final CompletableFuture<ApplicationStatus> externalShutdownFuture =
                new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                canceledJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    externalShutdownFuture.complete(status);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        ApplicationDispatcherBootstrap bootstrap =
                createApplicationDispatcherBootstrap(
                        3, dispatcherBuilder.build(), scheduledExecutor);

        final CompletableFuture<Acknowledge> completionFuture =
                bootstrap.getBootstrapCompletionFuture();

        // wait until the bootstrap "thinks" it's done
        completionFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

        // verify that the dispatcher is actually being shut down
        assertThat(externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS))
                .isEqualTo(ApplicationStatus.CANCELED);
    }

    @Test
    void testErrorHandlerIsCalledWhenApplicationStatusIsUnknown() throws Exception {
        // we're "listening" on this to be completed to verify that the cluster
        // is being shut down from the ApplicationDispatcherBootstrap
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
        final ApplicationDispatcherBootstrap bootstrap =
                createApplicationDispatcherBootstrap(
                        3,
                        dispatcherGateway,
                        scheduledExecutor,
                        errorHandlerFuture::completeExceptionally);

        // check that bootstrap shutdown completes exceptionally
        assertException(
                bootstrap.getApplicationCompletionFuture(), UnsuccessfulExecutionException.class);
        // and exception gets propagated to error handler
        assertException(
                bootstrap.getApplicationCompletionFuture(), UnsuccessfulExecutionException.class);
        // and cluster didn't shut down
        assertThat(shutdownCalled.get()).isFalse();
    }

    @Test
    void testDuplicateJobSubmissionWithTerminatedJobId() throws Throwable {
        final JobID testJobID = new JobID(0, 2);
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());
        configurationUnderTest.set(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                finishedJobGatewayBuilder()
                        .setSubmitFunction(
                                jobGraph ->
                                        FutureUtils.completedExceptionally(
                                                DuplicateJobSubmissionException
                                                        .ofGloballyTerminated(testJobID)));
        final CompletableFuture<Void> applicationFuture =
                runApplication(dispatcherBuilder, configurationUnderTest, 1);
        applicationFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
        configurationUnderTest.set(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
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
        final CompletableFuture<Void> applicationFuture =
                runApplication(dispatcherBuilder, configurationUnderTest, 1);
        applicationFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * In this scenario, job result is no longer present in the {@link
     * org.apache.flink.runtime.dispatcher.Dispatcher dispatcher} (job has terminated and job
     * manager failed over), but we know that job has already terminated from {@link
     * org.apache.flink.runtime.highavailability.JobResultStore}.
     */
    @Test
    void testDuplicateJobSubmissionWithTerminatedJobIdWithUnknownResultAttached() throws Throwable {
        final JobID testJobID = new JobID(0, 2);
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());
        configurationUnderTest.set(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
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
        final CompletableFuture<Void> applicationFuture =
                runApplication(dispatcherBuilder, configurationUnderTest, 1);
        applicationFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void testDuplicateJobSubmissionWithRunningJobId() throws Throwable {
        final JobID testJobID = new JobID(0, 2);
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());
        configurationUnderTest.set(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                TestingDispatcherGateway.newBuilder()
                        .setSubmitFunction(
                                jobGraph ->
                                        FutureUtils.completedExceptionally(
                                                DuplicateJobSubmissionException.of(testJobID)));
        final CompletableFuture<Void> applicationFuture =
                runApplication(dispatcherBuilder, configurationUnderTest, 1);
        final ExecutionException executionException =
                assertThrows(
                        ExecutionException.class,
                        () -> applicationFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        final Optional<DuplicateJobSubmissionException> maybeDuplicate =
                ExceptionUtils.findThrowable(
                        executionException, DuplicateJobSubmissionException.class);
        assertThat(maybeDuplicate).isPresent();
        assertThat(maybeDuplicate.get().isGloballyTerminated()).isFalse();
    }

    @ParameterizedTest
    @EnumSource(
            value = JobStatus.class,
            names = {"FINISHED", "CANCELED", "FAILED"})
    void testShutdownDisabled(JobStatus jobStatus) throws Exception {
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH, false);

        final TestingDispatcherGateway dispatcherGateway =
                dispatcherGatewayBuilder(jobStatus)
                        .setClusterShutdownFunction(
                                status -> {
                                    fail("Cluster shutdown should not be called");
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        ApplicationDispatcherBootstrap bootstrap =
                createApplicationDispatcherBootstrap(
                        configurationUnderTest, dispatcherGateway, scheduledExecutor);

        // Wait until bootstrap is finished to make sure cluster shutdown isn't called
        bootstrap.getBootstrapCompletionFuture().get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    void testSubmitFailedJobOnApplicationErrorInHASetup() throws Exception {
        final Configuration configuration = getConfiguration();
        final JobID jobId = new JobID();
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configuration.set(DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR, true);
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

    @Test
    void testSubmitFailedJobOnApplicationErrorInHASetupWithCustomFixedJobId() throws Exception {
        final Configuration configuration = getConfiguration();
        final JobID customFixedJobId = new JobID();
        configuration.set(HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        configuration.set(DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR, true);
        configuration.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, customFixedJobId.toHexString());
        testSubmitFailedJobOnApplicationError(
                configuration,
                (jobId, t) -> {
                    assertThat(jobId).isEqualTo(customFixedJobId);
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
                                                        createJobResult(
                                                                jobId, ApplicationStatus.FAILED)))
                        .build();

        final ApplicationDispatcherBootstrap bootstrap =
                new ApplicationDispatcherBootstrap(
                        FailingJob.getProgram(),
                        Collections.emptyList(),
                        configuration,
                        dispatcherGateway,
                        scheduledExecutor,
                        exception -> {});

        bootstrap.getBootstrapCompletionFuture().get();
    }

    @Test
    void testSubmitFailedJobOnApplicationErrorInNonHASetup() throws Exception {
        final Configuration configuration = getConfiguration();
        configuration.set(DeploymentOptions.SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR, true);
        final ApplicationDispatcherBootstrap bootstrap =
                new ApplicationDispatcherBootstrap(
                        FailingJob.getProgram(),
                        Collections.emptyList(),
                        configuration,
                        TestingDispatcherGateway.newBuilder().build(),
                        scheduledExecutor,
                        exception -> {});
        assertThatFuture(bootstrap.getBootstrapCompletionFuture())
                .eventuallyFailsWith(ExecutionException.class)
                .extracting(Throwable::getCause)
                .satisfies(
                        e ->
                                assertThat(e)
                                        .isInstanceOf(ApplicationExecutionException.class)
                                        .hasMessageContaining(
                                                DeploymentOptions
                                                        .SUBMIT_FAILED_JOB_ON_APPLICATION_ERROR
                                                        .key()));
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
                    jobID ->
                            CompletableFuture.completedFuture(
                                    createJobResult(
                                            jobID, ApplicationStatus.fromJobStatus(jobStatus))));
        }
        return builder;
    }

    private CompletableFuture<Void> runApplication(
            TestingDispatcherGateway.Builder dispatcherBuilder, int noOfJobs)
            throws FlinkException {

        return runApplication(dispatcherBuilder, getConfiguration(), noOfJobs);
    }

    private CompletableFuture<Void> runApplication(
            final Configuration configuration, final int noOfJobs) throws Throwable {

        final TestingDispatcherGateway.Builder dispatcherBuilder = finishedJobGatewayBuilder();

        return runApplication(dispatcherBuilder, configuration, noOfJobs);
    }

    private CompletableFuture<Void> runApplication(
            TestingDispatcherGateway.Builder dispatcherBuilder,
            Configuration configuration,
            int noOfJobs)
            throws FlinkException {

        final PackagedProgram program = getProgram(noOfJobs);

        final ApplicationDispatcherBootstrap bootstrap =
                new ApplicationDispatcherBootstrap(
                        program,
                        Collections.emptyList(),
                        configuration,
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        exception -> {});

        return bootstrap.getApplicationCompletionFuture();
    }

    private ApplicationDispatcherBootstrap createApplicationDispatcherBootstrap(
            final int noOfJobs,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor)
            throws FlinkException {
        return createApplicationDispatcherBootstrap(
                noOfJobs, dispatcherGateway, scheduledExecutor, exception -> {});
    }

    private ApplicationDispatcherBootstrap createApplicationDispatcherBootstrap(
            final int noOfJobs,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final FatalErrorHandler errorHandler)
            throws FlinkException {
        return createApplicationDispatcherBootstrap(
                noOfJobs, getConfiguration(), dispatcherGateway, scheduledExecutor, errorHandler);
    }

    private ApplicationDispatcherBootstrap createApplicationDispatcherBootstrap(
            final Configuration configuration,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor)
            throws FlinkException {
        return createApplicationDispatcherBootstrap(
                1, configuration, dispatcherGateway, scheduledExecutor, exception -> {});
    }

    private ApplicationDispatcherBootstrap createApplicationDispatcherBootstrap(
            final int noOfJobs,
            final Configuration configuration,
            final DispatcherGateway dispatcherGateway,
            final ScheduledExecutor scheduledExecutor,
            final FatalErrorHandler errorHandler)
            throws FlinkException {
        final PackagedProgram program = getProgram(noOfJobs);
        return new ApplicationDispatcherBootstrap(
                program,
                Collections.emptyList(),
                configuration,
                dispatcherGateway,
                scheduledExecutor,
                errorHandler);
    }

    private PackagedProgram getProgram(int noOfJobs) throws FlinkException {
        return MultiExecuteJob.getProgram(noOfJobs, true);
    }

    private static JobResult createFailedJobResult(final JobID jobId) {
        return createJobResult(jobId, ApplicationStatus.FAILED);
    }

    private static JobResult createUnknownJobResult(final JobID jobId) {
        return createJobResult(jobId, ApplicationStatus.UNKNOWN);
    }

    private static JobResult createJobResult(
            final JobID jobID, final ApplicationStatus applicationStatus) {
        JobResult.Builder builder =
                new JobResult.Builder()
                        .jobId(jobID)
                        .netRuntime(2L)
                        .applicationStatus(applicationStatus);
        if (applicationStatus == ApplicationStatus.CANCELED) {
            builder.serializedThrowable(
                    new SerializedThrowable(new JobCancellationException(jobID, "Hello", null)));
        } else if (applicationStatus == ApplicationStatus.FAILED
                || applicationStatus == ApplicationStatus.UNKNOWN) {
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
}
