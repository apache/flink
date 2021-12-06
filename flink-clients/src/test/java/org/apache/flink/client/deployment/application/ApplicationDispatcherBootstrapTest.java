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
import org.apache.flink.client.cli.CliFrontendTestUtils;
import org.apache.flink.client.deployment.application.executors.EmbeddedExecutor;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.ProgramInvocationException;
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
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.File;
import java.io.FileNotFoundException;
import java.net.MalformedURLException;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/** Tests for the {@link ApplicationDispatcherBootstrap}. */
public class ApplicationDispatcherBootstrapTest extends TestLogger {

    private static final String MULTI_EXECUTE_JOB_CLASS_NAME =
            "org.apache.flink.client.testjar.MultiExecuteJob";
    private static final int TIMEOUT_SECONDS = 10;

    final ScheduledExecutorService executor = Executors.newScheduledThreadPool(4);
    final ScheduledExecutor scheduledExecutor = new ScheduledExecutorServiceAdapter(executor);

    @AfterEach
    public void cleanup() {
        ExecutorUtils.gracefulShutdown(5, TimeUnit.SECONDS, executor);
    }

    @Test
    public void testExceptionThrownWhenApplicationContainsNoJobs() throws Throwable {
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                new TestingDispatcherGateway.Builder()
                        .setSubmitFunction(
                                jobGraph -> CompletableFuture.completedFuture(Acknowledge.get()));

        final CompletableFuture<Void> applicationFuture = runApplication(dispatcherBuilder, 0);

        assertException(applicationFuture, ApplicationExecutionException.class);
    }

    @Test
    public void testOnlyOneJobIsAllowedWithHa() throws Throwable {
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());

        final CompletableFuture<Void> applicationFuture = runApplication(configurationUnderTest, 2);

        assertException(applicationFuture, FlinkRuntimeException.class);
    }

    @Test
    public void testOnlyOneJobAllowedWithStaticJobId() throws Throwable {
        final JobID testJobID = new JobID(0, 2);

        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());

        final CompletableFuture<Void> applicationFuture = runApplication(configurationUnderTest, 2);

        assertException(applicationFuture, FlinkRuntimeException.class);
    }

    @Test
    public void testOnlyOneJobAllowedWithStaticJobIdAndHa() throws Throwable {
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
    public void testJobIdDefaultsToZeroWithHa() throws Throwable {
        final Configuration configurationUnderTest = getConfiguration();
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

        assertThat(submittedJobId.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), is(new JobID(0L, 0L)));
    }

    @Test
    public void testStaticJobId() throws Throwable {
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

        assertThat(submittedJobId.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), is(new JobID(0L, 2L)));
    }

    @Test
    public void testStaticJobIdWithHa() throws Throwable {
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

        assertThat(submittedJobId.get(TIMEOUT_SECONDS, TimeUnit.SECONDS), is(new JobID(0L, 2L)));
    }

    @Test
    public void testApplicationFailsAsSoonAsOneJobFails() throws Throwable {
        final ConcurrentLinkedDeque<JobID> submittedJobIds = new ConcurrentLinkedDeque<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                new TestingDispatcherGateway.Builder()
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
        assertEquals(exception.getStatus(), ApplicationStatus.FAILED);
    }

    @Test
    public void testApplicationSucceedsWhenAllJobsSucceed() throws Exception {
        final TestingDispatcherGateway.Builder dispatcherBuilder = finishedJobGatewayBuilder();

        final CompletableFuture<Void> applicationFuture = runApplication(dispatcherBuilder, 3);

        // this would block indefinitely if the applications don't finish
        applicationFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
    }

    @Test
    public void testDispatcherIsCancelledWhenOneJobIsCancelled() throws Exception {
        final CompletableFuture<ApplicationStatus> clusterShutdownStatus =
                new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                canceledJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                (status) -> {
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

        assertThat(
                clusterShutdownStatus.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                is(ApplicationStatus.CANCELED));
    }

    @Test
    public void testApplicationTaskFinishesWhenApplicationFinishes() throws Exception {
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
    public void testApplicationIsStoppedWhenStoppingBootstrap() throws Exception {
        final TestingDispatcherGateway.Builder dispatcherBuilder = runningJobGatewayBuilder();

        // we're "listening" on this to be completed to verify that the error handler is called.
        // In production, this will shut down the cluster with an exception.
        final CompletableFuture<Void> errorHandlerFuture = new CompletableFuture<>();
        final ApplicationDispatcherBootstrap bootstrap =
                createApplicationDispatcherBootstrap(
                        3,
                        dispatcherBuilder.build(),
                        scheduledExecutor,
                        errorHandlerFuture::completeExceptionally);

        final CompletableFuture<Acknowledge> completionFuture =
                bootstrap.getBootstrapCompletionFuture();

        ScheduledFuture<?> applicationExecutionFuture = bootstrap.getApplicationExecutionFuture();

        bootstrap.stop();

        // we call the error handler
        assertException(errorHandlerFuture, CancellationException.class);

        // we return a future that is completed exceptionally
        assertException(completionFuture, CancellationException.class);

        // verify that the application task is being cancelled
        assertThat(applicationExecutionFuture.isCancelled(), is(true));
    }

    @Test
    public void testErrorHandlerIsCalledWhenStoppingBootstrap() throws Exception {
        final TestingDispatcherGateway.Builder dispatcherBuilder = runningJobGatewayBuilder();

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

        bootstrap.stop();

        // we call the error handler
        assertException(errorHandlerFuture, CancellationException.class);

        // we return a future that is completed exceptionally
        assertException(completionFuture, CancellationException.class);
    }

    @Test
    public void testErrorHandlerIsCalledWhenSubmissionFails() throws Exception {
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                new TestingDispatcherGateway.Builder()
                        .setSubmitFunction(
                                jobGraph -> {
                                    throw new FlinkRuntimeException("Nope!");
                                })
                        .setClusterShutdownFunction(
                                status -> {
                                    fail("We should not call shutdownCluster()");
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

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
        assertException(errorHandlerFuture, ApplicationExecutionException.class);

        // we return a future that is completed exceptionally
        assertException(completionFuture, ApplicationExecutionException.class);
    }

    @Test
    public void testClusterIsShutdownInAttachedModeWhenJobCancelled() throws Exception {
        final CompletableFuture<ApplicationStatus> clusterShutdown = new CompletableFuture<>();

        final TestingDispatcherGateway dispatcherGateway =
                canceledJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                status -> {
                                    clusterShutdown.complete(status);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        final PackagedProgram program =
                PackagedProgram.newBuilder()
                        .setUserClassPaths(
                                Collections.singletonList(
                                        new File(CliFrontendTestUtils.getTestJarPath())
                                                .toURI()
                                                .toURL()))
                        .setEntryPointClassName(MULTI_EXECUTE_JOB_CLASS_NAME)
                        .setArguments(String.valueOf(2), String.valueOf(true))
                        .build();

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

        assertEquals(clusterShutdown.get(), ApplicationStatus.CANCELED);
    }

    @Test
    public void testClusterShutdownWhenApplicationSucceeds() throws Exception {
        // we're "listening" on this to be completed to verify that the cluster
        // is being shut down from the ApplicationDispatcherBootstrap
        final CompletableFuture<ApplicationStatus> externalShutdownFuture =
                new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                finishedJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                (status) -> {
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
        assertThat(
                externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                is(ApplicationStatus.SUCCEEDED));
    }

    @Test
    public void testClusterShutdownWhenApplicationFails() throws Exception {
        // we're "listening" on this to be completed to verify that the cluster
        // is being shut down from the ApplicationDispatcherBootstrap
        final CompletableFuture<ApplicationStatus> externalShutdownFuture =
                new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                failedJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                (status) -> {
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
        assertThat(
                externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                is(ApplicationStatus.FAILED));
    }

    @Test
    public void testClusterShutdownWhenApplicationGetsCancelled() throws Exception {
        // we're "listening" on this to be completed to verify that the cluster
        // is being shut down from the ApplicationDispatcherBootstrap
        final CompletableFuture<ApplicationStatus> externalShutdownFuture =
                new CompletableFuture<>();

        final TestingDispatcherGateway.Builder dispatcherBuilder =
                canceledJobGatewayBuilder()
                        .setClusterShutdownFunction(
                                (status) -> {
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
        assertThat(
                externalShutdownFuture.get(TIMEOUT_SECONDS, TimeUnit.SECONDS),
                is(ApplicationStatus.CANCELED));
    }

    @Test
    public void testClusterDoesNotShutdownWhenApplicationStatusUnknown() throws Exception {
        // we're "listening" on this to be completed to verify that the cluster
        // is being shut down from the ApplicationDispatcherBootstrap
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                canceledJobGatewayBuilder()
                        .setRequestJobResultFunction(
                                jobID ->
                                        CompletableFuture.completedFuture(
                                                createUnknownJobResult(jobID)))
                        .setClusterShutdownFunction(
                                status -> {
                                    fail("We should not call shutdownCluster()");
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                });

        final TestingDispatcherGateway dispatcherGateway = dispatcherBuilder.build();
        final ApplicationDispatcherBootstrap bootstrap =
                createApplicationDispatcherBootstrap(3, dispatcherGateway, scheduledExecutor);

        final CompletableFuture<Acknowledge> applicationFuture =
                bootstrap.getBootstrapCompletionFuture();

        final UnsuccessfulExecutionException exception =
                assertException(applicationFuture, UnsuccessfulExecutionException.class);
        assertEquals(exception.getStatus(), ApplicationStatus.UNKNOWN);
    }

    @Test
    public void testDuplicateJobSubmissionWithTerminatedJobId() throws Throwable {
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
     * org.apache.flink.runtime.highavailability.RunningJobsRegistry running jobs registry}.
     */
    @Test
    public void testDuplicateJobSubmissionWithTerminatedJobIdWithUnknownResult() throws Throwable {
        final JobID testJobID = new JobID(0, 2);
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());
        configurationUnderTest.set(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                new TestingDispatcherGateway.Builder()
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
     * org.apache.flink.runtime.highavailability.RunningJobsRegistry running jobs registry}.
     */
    @Test
    public void testDuplicateJobSubmissionWithTerminatedJobIdWithUnknownResultAttached()
            throws Throwable {
        final JobID testJobID = new JobID(0, 2);
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());
        configurationUnderTest.set(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                new TestingDispatcherGateway.Builder()
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
    public void testDuplicateJobSubmissionWithRunningJobId() throws Throwable {
        final JobID testJobID = new JobID(0, 2);
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(
                PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, testJobID.toHexString());
        configurationUnderTest.set(
                HighAvailabilityOptions.HA_MODE, HighAvailabilityMode.ZOOKEEPER.name());
        final TestingDispatcherGateway.Builder dispatcherBuilder =
                new TestingDispatcherGateway.Builder()
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
        assertTrue(maybeDuplicate.isPresent());
        assertFalse(maybeDuplicate.get().isGloballyTerminated());
    }

    @ParameterizedTest
    @EnumSource(
            value = JobStatus.class,
            names = {"FINISHED", "CANCELED", "FAILED"})
    public void testShutdownDisabled(JobStatus jobStatus) throws Exception {
        final Configuration configurationUnderTest = getConfiguration();
        configurationUnderTest.set(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH, false);

        final TestingDispatcherGateway dispatcherGateway =
                dispatcherGatewayBuilder(jobStatus)
                        .setClusterShutdownFunction(
                                (status) -> {
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
                new TestingDispatcherGateway.Builder()
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
        try {
            return PackagedProgram.newBuilder()
                    .setUserClassPaths(
                            Collections.singletonList(
                                    new File(CliFrontendTestUtils.getTestJarPath())
                                            .toURI()
                                            .toURL()))
                    .setEntryPointClassName(MULTI_EXECUTE_JOB_CLASS_NAME)
                    .setArguments(String.valueOf(noOfJobs), Boolean.toString(true))
                    .build();
        } catch (ProgramInvocationException | FileNotFoundException | MalformedURLException e) {
            throw new FlinkException("Could not load the provided entrypoint class.", e);
        }
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
