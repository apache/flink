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

package org.apache.flink.runtime.dispatcher;

import org.apache.flink.api.common.ApplicationID;
import org.apache.flink.api.common.ApplicationState;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.DeploymentOptions;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.application.AbstractApplication;
import org.apache.flink.runtime.application.ArchivedApplication;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.client.DuplicateApplicationSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.cleanup.CheckpointResourcesCleanupRunnerFactory;
import org.apache.flink.runtime.dispatcher.cleanup.TestingCleanupRunnerFactory;
import org.apache.flink.runtime.highavailability.ApplicationResult;
import org.apache.flink.runtime.highavailability.ApplicationResultEntry;
import org.apache.flink.runtime.highavailability.EmbeddedApplicationResultStore;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedJobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.StandaloneApplicationStore;
import org.apache.flink.runtime.jobmanager.StandaloneExecutionPlanStore;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkApplicationTerminatedWithoutCancellationException;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.TestingApplicationResultStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Tests for the {@link Dispatcher} component that are related to applications. */
@ExtendWith(TestLoggerExtension.class)
public class DispatcherApplicationTest {

    static TestingRpcService rpcService;

    static final Duration TIMEOUT = Duration.ofMinutes(1L);

    @TempDir public Path tempDir;

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerResource =
            new TestingFatalErrorHandlerExtension();

    Configuration configuration;

    BlobServer blobServer;

    TestingHighAvailabilityServices haServices;

    private ApplicationID applicationId;

    private JobGraph jobGraph;

    private JobID jobId;

    /** Instance under test. */
    private TestingDispatcher dispatcher;

    @BeforeAll
    public static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @BeforeEach
    void beforeEach() throws Exception {
        haServices = new TestingHighAvailabilityServices();
        haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());
        haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
        haServices.setExecutionPlanStore(new StandaloneExecutionPlanStore());
        haServices.setJobResultStore(new EmbeddedJobResultStore());
        haServices.setApplicationStore(new StandaloneApplicationStore());
        haServices.setApplicationResultStore(new EmbeddedApplicationResultStore());

        configuration = new Configuration();
        blobServer = new BlobServer(configuration, tempDir.toFile(), new VoidBlobStore());
        jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        jobId = jobGraph.getJobID();
        applicationId = ApplicationID.fromHexString(jobId.toHexString());
        jobGraph.setApplicationId(applicationId);
    }

    @AfterEach
    void teardown() throws Exception {
        if (dispatcher != null) {
            dispatcher.close();
        }

        if (blobServer != null) {
            blobServer.close();
        }
    }

    @AfterAll
    static void teardownClass() throws ExecutionException, InterruptedException {
        if (rpcService != null) {
            rpcService.closeAsync().get();
        }
    }

    @Test
    void testApplicationStatusChange_ArchiveNotCalledForNonTerminalStatus() throws Exception {
        final CompletableFuture<Void> archiveApplicationFuture = new CompletableFuture<>();
        dispatcher =
                createTestingDispatcherBuilder()
                        .setHistoryServerArchivist(
                                TestingHistoryServerArchivist.builder()
                                        .setArchiveApplicationFunction(
                                                archivedApplication -> {
                                                    archiveApplicationFuture.complete(null);
                                                    return CompletableFuture.completedFuture(null);
                                                })
                                        .build())
                        .build(rpcService);
        dispatcher.start();
        CompletableFuture<?> applicationTerminationFuture =
                submitApplicationAndMockApplicationStatusChange(ApplicationState.RUNNING);

        // verify that archive application is not called
        assertFalse(archiveApplicationFuture.isDone());
        assertFalse(applicationTerminationFuture.isDone());
    }

    @Test
    void testApplicationStatusChange_ArchiveCalledForTerminalStatus() throws Exception {
        final CompletableFuture<ApplicationID> archiveApplicationFuture = new CompletableFuture<>();
        dispatcher =
                createTestingDispatcherBuilder()
                        .setHistoryServerArchivist(
                                TestingHistoryServerArchivist.builder()
                                        .setArchiveApplicationFunction(
                                                archivedApplication -> {
                                                    archiveApplicationFuture.complete(
                                                            archivedApplication.getApplicationId());
                                                    return CompletableFuture.completedFuture(null);
                                                })
                                        .build())
                        .build(rpcService);
        dispatcher.start();
        CompletableFuture<?> applicationTerminationFuture =
                submitApplicationAndMockApplicationStatusChange(ApplicationState.FINISHED);

        // verify that archive application is called with the application id
        assertEquals(applicationId, archiveApplicationFuture.get());
        applicationTerminationFuture.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Test
    void testApplicationStatusChange_ThrowsIfDuplicateTerminalStatus() throws Exception {
        dispatcher = createTestingDispatcherBuilder().build(rpcService);
        dispatcher.start();
        CompletableFuture<?> applicationTerminationFuture =
                submitApplicationAndMockApplicationStatusChange(ApplicationState.FINISHED);
        // wait for archive to complete
        applicationTerminationFuture.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        assertThatThrownBy(() -> mockApplicationStatusChange(ApplicationState.FAILED))
                .extracting(ExceptionUtils::stripExecutionException)
                .isInstanceOf(IllegalStateException.class);
    }

    @Test
    void testApplicationBootstrap() throws Exception {
        final OneShotLatch bootstrapLatch = new OneShotLatch();
        final AbstractApplication application =
                TestingApplication.builder()
                        .setApplicationId(applicationId)
                        .setExecuteFunction(
                                ignored -> {
                                    bootstrapLatch.trigger();
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        dispatcher =
                createTestingDispatcherBuilder()
                        .setDispatcherBootstrapFactory(
                                (ignoredDispatcherGateway,
                                        ignoredScheduledExecutor,
                                        ignoredFatalErrorHandler) ->
                                        new ApplicationBootstrap(application))
                        .setJobManagerRunnerFactory(
                                new TestingJobMasterServiceLeadershipRunnerFactory())
                        .build(rpcService);

        dispatcher.start();

        // ensure that the application execution is triggered
        bootstrapLatch.await();

        assertThat(dispatcher.getApplications().size()).isEqualTo(1);
        assertThat(dispatcher.getApplications().keySet()).contains(applicationId);

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        assertThat(application.getJobs().size()).isEqualTo(1);
        assertThat(application.getJobs()).contains(jobId);
    }

    @Test
    public void testApplicationSubmission() throws Exception {
        dispatcher = createTestingDispatcherBuilder().build(rpcService);
        dispatcher.start();
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        final CompletableFuture<ApplicationID> submittedApplicationFuture =
                new CompletableFuture<>();
        final AbstractApplication application =
                TestingApplication.builder()
                        .setApplicationId(applicationId)
                        .setExecuteFunction(
                                ignored -> {
                                    submittedApplicationFuture.complete(applicationId);
                                    return CompletableFuture.completedFuture(Acknowledge.get());
                                })
                        .build();

        dispatcherGateway.submitApplication(application, TIMEOUT).get();

        // ensure that the application execution is triggered
        assertThat(submittedApplicationFuture).isCompletedWithValue(applicationId);

        ArchivedApplication archivedApplication =
                dispatcher.requestApplication(applicationId, TIMEOUT).get();
        assertThat(archivedApplication.getApplicationId()).isEqualTo(applicationId);
    }

    @Test
    public void testDuplicateApplicationSubmission() throws Exception {
        dispatcher = createTestingDispatcherBuilder().build(rpcService);
        dispatcher.start();
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        final AbstractApplication application =
                TestingApplication.builder().setApplicationId(applicationId).build();
        // submit application
        dispatcherGateway.submitApplication(application, TIMEOUT).get();

        // duplicate submission
        final CompletableFuture<Acknowledge> submitFuture =
                dispatcherGateway.submitApplication(application, TIMEOUT);
        assertThatThrownBy(submitFuture::get)
                .hasCauseInstanceOf(DuplicateApplicationSubmissionException.class);
    }

    @Test
    public void testDuplicateApplicationSubmissionIsDetectedOnSimultaneousSubmission()
            throws Exception {
        dispatcher = createTestingDispatcherBuilder().build(rpcService);
        dispatcher.start();
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        final AbstractApplication application =
                TestingApplication.builder().setApplicationId(applicationId).build();

        final int numThreads = 5;
        final CountDownLatch prepareLatch = new CountDownLatch(numThreads);
        final OneShotLatch startLatch = new OneShotLatch();

        final Collection<Throwable> exceptions = Collections.synchronizedList(new ArrayList<>());
        final Collection<Thread> threads = new ArrayList<>();
        for (int x = 0; x < numThreads; x++) {
            threads.add(
                    new Thread(
                            () -> {
                                try {
                                    prepareLatch.countDown();
                                    startLatch.awaitQuietly();
                                    dispatcherGateway
                                            .submitApplication(application, TIMEOUT)
                                            .join();
                                } catch (Throwable t) {
                                    exceptions.add(t);
                                }
                            }));
        }

        // start worker threads and trigger submissions
        threads.forEach(Thread::start);
        prepareLatch.await();
        startLatch.trigger();

        // wait for the submissions to happen
        for (Thread thread : threads) {
            thread.join();
        }

        // verify the application was actually submitted
        ArchivedApplication archivedApplication =
                dispatcher.requestApplication(applicationId, TIMEOUT).get();
        assertThat(archivedApplication.getApplicationId()).isEqualTo(applicationId);

        // verify that all but one submission failed as duplicates
        assertThat(exceptions)
                .hasSize(numThreads - 1)
                .allSatisfy(
                        t ->
                                assertThat(t)
                                        .hasCauseInstanceOf(
                                                DuplicateApplicationSubmissionException.class));
    }

    @Test
    public void testApplicationCancellation() throws Exception {
        dispatcher = createTestingDispatcherBuilder().build(rpcService);
        dispatcher.start();
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        final CompletableFuture<Void> canceledApplicationFuture = new CompletableFuture<>();
        final AbstractApplication application =
                TestingApplication.builder()
                        .setApplicationId(applicationId)
                        .setCancelFunction(
                                ignored -> {
                                    canceledApplicationFuture.complete(null);
                                    return null;
                                })
                        .build();

        dispatcherGateway.submitApplication(application, TIMEOUT).get();

        // verify the application was actually submitted
        ArchivedApplication archivedApplication =
                dispatcher.requestApplication(applicationId, TIMEOUT).get();
        assertThat(archivedApplication.getApplicationId()).isEqualTo(applicationId);

        // submission has succeeded, now cancel the application
        dispatcherGateway.cancelApplication(applicationId, TIMEOUT).get();

        assertThatFuture(canceledApplicationFuture).isDone();
    }

    @Test
    public void testApplicationCancellationOfCanceledTerminalDoesNotThrowException()
            throws Exception {
        dispatcher = createTestingDispatcherBuilder().build(rpcService);
        dispatcher.start();
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        final AbstractApplication application =
                TestingApplication.builder()
                        .setApplicationId(applicationId)
                        .setGetApplicationStatusFunction(ignored -> ApplicationState.CANCELED)
                        .build();

        dispatcherGateway.submitApplication(application, TIMEOUT).get();

        // verify the application was actually submitted
        ArchivedApplication archivedApplication =
                dispatcher.requestApplication(applicationId, TIMEOUT).get();
        assertThat(archivedApplication.getApplicationId()).isEqualTo(applicationId);
        assertThat(archivedApplication.getApplicationStatus()).isEqualTo(ApplicationState.CANCELED);

        // cancel the application should not throw
        dispatcherGateway.cancelApplication(applicationId, TIMEOUT).get();
    }

    @Test
    public void testApplicationCancellationOfNonCanceledTerminalFailsWithAppropriateException()
            throws Exception {
        dispatcher = createTestingDispatcherBuilder().build(rpcService);
        dispatcher.start();
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        final AbstractApplication application =
                TestingApplication.builder()
                        .setApplicationId(applicationId)
                        .setGetApplicationStatusFunction(ignored -> ApplicationState.FINISHED)
                        .build();

        dispatcherGateway.submitApplication(application, TIMEOUT).get();

        // verify the application was actually submitted
        ArchivedApplication archivedApplication =
                dispatcher.requestApplication(applicationId, TIMEOUT).get();
        assertThat(archivedApplication.getApplicationId()).isEqualTo(applicationId);
        assertThat(archivedApplication.getApplicationStatus()).isEqualTo(ApplicationState.FINISHED);

        // cancel the application should throw
        final CompletableFuture<Acknowledge> cancelFuture =
                dispatcherGateway.cancelApplication(applicationId, TIMEOUT);

        FlinkAssertions.assertThatFuture(cancelFuture)
                .eventuallyFails()
                .withCauseOfType(FlinkApplicationTerminatedWithoutCancellationException.class);
    }

    @Test
    public void testShutDownFutureCompletesAfterApplicationArchivingFutures() throws Exception {
        final CompletableFuture<Acknowledge> archiveApplicationFuture = new CompletableFuture<>();
        dispatcher =
                createTestingDispatcherBuilder()
                        .setHistoryServerArchivist(
                                TestingHistoryServerArchivist.builder()
                                        .setArchiveApplicationFunction(
                                                archivedApplication -> archiveApplicationFuture)
                                        .build())
                        .build(rpcService);
        dispatcher.start();

        submitApplicationAndMockApplicationStatusChange(ApplicationState.FINISHED);

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        dispatcherGateway.shutDownCluster(ApplicationStatus.SUCCEEDED).get();
        assertThatThrownBy(() -> dispatcher.getShutDownFuture().get(100L, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);

        archiveApplicationFuture.complete(null);

        dispatcher.getShutDownFuture().get();
    }

    @Test
    public void testRecoverJobSuccessfully() throws Exception {
        final TestingJobMasterServiceLeadershipRunnerFactory jobManagerRunnerFactory =
                new TestingJobMasterServiceLeadershipRunnerFactory();

        dispatcher =
                createTestingDispatcherBuilder()
                        .setJobManagerRunnerFactory(jobManagerRunnerFactory)
                        .setRecoveredJobs(Collections.singleton(jobGraph))
                        .setDispatcherBootstrapFactory(
                                (ignoredDispatcherGateway,
                                        ignoredScheduledExecutor,
                                        ignoredFatalErrorHandler) ->
                                        new ApplicationBootstrap(
                                                TestingApplication.builder()
                                                        .setApplicationId(applicationId)
                                                        .build()))
                        .build(rpcService);

        dispatcher.start();
        dispatcher.waitUntilStarted();

        // verify that the recovered job is NOT recovered immediately
        assertThat(jobManagerRunnerFactory.getQueueSize()).isZero();
        assertThat(dispatcher.getSuspendedJobs().containsKey(jobId)).isTrue();
        assertThat(dispatcher.getSuspendedJobIdsByApplicationId().containsKey(applicationId))
                .isTrue();

        // call recoverJob RPC
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.recoverJob(jobId, TIMEOUT).get();

        // verify that the job is now recovered
        final TestingJobManagerRunner jobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        assertThat(jobManagerRunner.getJobID()).isEqualTo(jobId);
        assertThat(dispatcher.getSuspendedJobs().containsKey(jobId)).isFalse();
        assertThat(dispatcher.getSuspendedJobIdsByApplicationId().containsKey(applicationId))
                .isFalse();
    }

    @Test
    public void testRecoverJobFailsWhenJobNotFound() throws Exception {
        dispatcher =
                createTestingDispatcherBuilder()
                        .setRecoveredJobs(Collections.singleton(jobGraph))
                        .setDispatcherBootstrapFactory(
                                (ignoredDispatcherGateway,
                                        ignoredScheduledExecutor,
                                        ignoredFatalErrorHandler) ->
                                        new ApplicationBootstrap(
                                                TestingApplication.builder()
                                                        .setApplicationId(applicationId)
                                                        .build()))
                        .build(rpcService);

        dispatcher.start();
        dispatcher.waitUntilStarted();

        // try to recover a job that doesn't exist
        final JobID unknownJobId = new JobID();
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        final CompletableFuture<Acknowledge> recoverFuture =
                dispatcherGateway.recoverJob(unknownJobId, TIMEOUT);

        assertThatThrownBy(recoverFuture::get)
                .hasCauseInstanceOf(JobSubmissionException.class)
                .hasMessageContaining("Cannot find the recovered job");
    }

    @Test
    public void testRemainingSuspendedJobsCleanedWhenApplicationReachesTerminalState()
            throws Exception {
        final TestingJobMasterServiceLeadershipRunnerFactory jobManagerRunnerFactory =
                new TestingJobMasterServiceLeadershipRunnerFactory();
        final TestingCleanupRunnerFactory cleanupRunnerFactory = new TestingCleanupRunnerFactory();

        dispatcher =
                createTestingDispatcherBuilder()
                        .setJobManagerRunnerFactory(jobManagerRunnerFactory)
                        .setCleanupRunnerFactory(cleanupRunnerFactory)
                        .setRecoveredJobs(Collections.singleton(jobGraph))
                        .setDispatcherBootstrapFactory(
                                (ignoredDispatcherGateway,
                                        ignoredScheduledExecutor,
                                        ignoredFatalErrorHandler) ->
                                        new ApplicationBootstrap(
                                                TestingApplication.builder()
                                                        .setApplicationId(applicationId)
                                                        .build()))
                        .build(rpcService);

        dispatcher.start();
        dispatcher.waitUntilStarted();

        // verify that the recovered job exists
        assertThat(dispatcher.getSuspendedJobs().containsKey(jobId)).isTrue();
        assertThat(dispatcher.getSuspendedJobIdsByApplicationId().containsKey(applicationId))
                .isTrue();

        // complete the application - this should trigger cleanup of the remaining recovered job
        mockApplicationStatusChange(ApplicationState.FINISHED);

        // verify that no jobs are recovered
        assertThat(jobManagerRunnerFactory.getQueueSize()).isZero();

        // verify that the remaining recovered job is cleaned up
        final TestingJobManagerRunner cleanupRunner =
                cleanupRunnerFactory.takeCreatedJobManagerRunner();
        assertThat(cleanupRunner.getJobID()).isEqualTo(jobId);
        assertThat(dispatcher.getSuspendedJobs().containsKey(jobId)).isFalse();
        assertThat(dispatcher.getSuspendedJobIdsByApplicationId().containsKey(applicationId))
                .isFalse();
    }

    @Test
    public void testJobResultNotMarkedCleanUntilApplicationTerminates() throws Exception {
        final TestingJobMasterServiceLeadershipRunnerFactory jobManagerRunnerFactory =
                new TestingJobMasterServiceLeadershipRunnerFactory();

        dispatcher =
                createTestingDispatcherBuilder()
                        .setJobManagerRunnerFactory(jobManagerRunnerFactory)
                        .setDispatcherBootstrapFactory(
                                (ignoredDispatcherGateway,
                                        ignoredScheduledExecutor,
                                        ignoredFatalErrorHandler) ->
                                        new ApplicationBootstrap(
                                                TestingApplication.builder()
                                                        .setApplicationId(applicationId)
                                                        .build()))
                        .build(rpcService);

        dispatcher.start();
        dispatcher.waitUntilStarted();

        // submit a job
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        // complete the job
        final TestingJobManagerRunner jobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        final ExecutionGraphInfo completedExecutionGraphInfo =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(jobId)
                                .setState(JobStatus.FINISHED)
                                .setApplicationId(applicationId)
                                .build());
        jobManagerRunner.completeResultFuture(completedExecutionGraphInfo);

        // wait until the dirty job result is created
        CommonTestUtils.waitUntilCondition(
                () -> haServices.getJobResultStore().hasDirtyJobResultEntryAsync(jobId).get());

        // job termination future should not be completed
        assertThatThrownBy(
                        () ->
                                dispatcher
                                        .getJobTerminationFuture(jobId, TIMEOUT)
                                        .get(10L, TimeUnit.MILLISECONDS))
                .isInstanceOf(TimeoutException.class);

        // verify that the job result is NOT marked clean yet
        assertThat(haServices.getJobResultStore().hasCleanJobResultEntryAsync(jobId).get())
                .isFalse();

        CompletableFuture<?> applicationTerminationFuture =
                dispatcher.getApplicationTerminationFuture(applicationId);

        // complete the application
        mockApplicationStatusChange(ApplicationState.FINISHED);

        // wait for application termination
        applicationTerminationFuture.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // wait for job termination
        dispatcher
                .getJobTerminationFuture(jobId, TIMEOUT)
                .get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);

        // verify that the job result is now marked clean
        assertThat(haServices.getJobResultStore().hasCleanJobResultEntryAsync(jobId).get())
                .isTrue();
    }

    @Test
    public void testRecoveredDirtyJobResultsCleanedOnApplicationSubmission() throws Exception {
        // create a dirty job result
        final JobResult jobResult =
                new JobResult.Builder()
                        .jobId(jobId)
                        .jobStatus(JobStatus.FINISHED)
                        .netRuntime(1)
                        .applicationId(applicationId)
                        .build();

        final TestingCleanupRunnerFactory cleanupRunnerFactory = new TestingCleanupRunnerFactory();

        dispatcher =
                createTestingDispatcherBuilder()
                        .setCleanupRunnerFactory(cleanupRunnerFactory)
                        .setRecoveredDirtyJobs(Collections.singleton(jobResult))
                        .setDispatcherBootstrapFactory(
                                (ignoredDispatcherGateway,
                                        ignoredScheduledExecutor,
                                        ignoredFatalErrorHandler) ->
                                        new ApplicationBootstrap(
                                                TestingApplication.builder()
                                                        .setApplicationId(applicationId)
                                                        .build()))
                        .build(rpcService);

        // verify that the dirty job result exists
        assertThat(
                        dispatcher
                                .getRecoveredDirtyJobResultsByApplicationId()
                                .containsKey(applicationId))
                .isTrue();

        // start application - this should trigger the cleanup of dirty job results
        dispatcher.start();
        dispatcher.waitUntilStarted();

        // verify that the dirty job result was cleaned up
        final TestingJobManagerRunner cleanupRunner =
                cleanupRunnerFactory.takeCreatedJobManagerRunner();
        assertThat(cleanupRunner.getJobID()).isEqualTo(jobId);
        assertThat(
                        dispatcher
                                .getRecoveredDirtyJobResultsByApplicationId()
                                .containsKey(applicationId))
                .isFalse();
    }

    @Test
    public void testDuplicateSubmissionWithRecoveredApplication() throws Exception {
        final AbstractApplication application =
                TestingApplication.builder().setApplicationId(applicationId).build();
        dispatcher =
                createTestingDispatcherBuilder()
                        .setRecoveredApplications(Collections.singleton(application))
                        .build(rpcService);
        dispatcher.start();
        dispatcher.waitUntilStarted();
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        final CompletableFuture<Acknowledge> submitFuture =
                dispatcherGateway.submitApplication(application, TIMEOUT);

        assertThatThrownBy(submitFuture::get)
                .hasCauseInstanceOf(DuplicateApplicationSubmissionException.class);
    }

    @Test
    public void testDuplicateSubmissionWithTerminatedButDirtyApplication() throws Exception {
        final ApplicationResult applicationResult =
                TestingApplicationResultStore.createSuccessfulApplicationResult(applicationId);
        haServices
                .getApplicationResultStore()
                .createDirtyResultAsync(new ApplicationResultEntry(applicationResult))
                .get();

        assertDuplicateApplicationSubmission();
    }

    @Test
    public void testDuplicateSubmissionWithTerminatedAndCleanedApplication() throws Exception {
        final ApplicationResult applicationResult =
                TestingApplicationResultStore.createSuccessfulApplicationResult(applicationId);
        haServices
                .getApplicationResultStore()
                .createDirtyResultAsync(new ApplicationResultEntry(applicationResult))
                .get();
        haServices.getApplicationResultStore().markResultAsCleanAsync(applicationId).get();

        assertDuplicateApplicationSubmission();
    }

    private void assertDuplicateApplicationSubmission() throws Exception {
        dispatcher = createTestingDispatcherBuilder().build(rpcService);
        dispatcher.start();

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        final AbstractApplication application =
                TestingApplication.builder().setApplicationId(applicationId).build();

        final CompletableFuture<Acknowledge> submitFuture =
                dispatcherGateway.submitApplication(application, TIMEOUT);

        assertThatThrownBy(submitFuture::get)
                .hasCauseInstanceOf(DuplicateApplicationSubmissionException.class);
    }

    @Test
    public void testApplicationBootstrapWithDirtyResultTriggersShutdown() throws Exception {
        testApplicationBootstrapWithApplicationResult(false, true);
    }

    @Test
    public void testApplicationBootstrapWithDirtyResultDoesNotTriggerShutdownWhenDisabled()
            throws Exception {
        testApplicationBootstrapWithApplicationResult(false, false);
    }

    @Test
    public void testApplicationBootstrapWithCleanResultTriggersShutdown() throws Exception {
        testApplicationBootstrapWithApplicationResult(true, true);
    }

    @Test
    public void testApplicationBootstrapWithCleanResultDoesNotTriggerShutdownWhenDisabled()
            throws Exception {
        testApplicationBootstrapWithApplicationResult(true, false);
    }

    private void testApplicationBootstrapWithApplicationResult(
            boolean isCleanResult, boolean triggerShutDown) throws Exception {
        configuration.set(DeploymentOptions.SHUTDOWN_ON_APPLICATION_FINISH, triggerShutDown);

        final ApplicationResult applicationResult =
                TestingApplicationResultStore.createSuccessfulApplicationResult(applicationId);
        haServices
                .getApplicationResultStore()
                .createDirtyResultAsync(new ApplicationResultEntry(applicationResult))
                .get();
        if (isCleanResult) {
            haServices.getApplicationResultStore().markResultAsCleanAsync(applicationId).get();
        }

        final OneShotLatch bootstrapLatch = new OneShotLatch();
        final TestingDispatcher.Builder builder =
                createTestingDispatcherBuilder()
                        .setDispatcherBootstrapFactory(
                                (ignoredDispatcherGateway,
                                        ignoredScheduledExecutor,
                                        ignoredFatalErrorHandler) ->
                                        new ApplicationBootstrap(
                                                TestingApplication.builder()
                                                        .setApplicationId(applicationId)
                                                        .setExecuteFunction(
                                                                ignoredExecuteParams -> {
                                                                    bootstrapLatch.trigger();
                                                                    return CompletableFuture
                                                                            .completedFuture(
                                                                                    Acknowledge
                                                                                            .get());
                                                                })
                                                        .build()));
        if (!isCleanResult) {
            builder.setRecoveredDirtyApplications(Collections.singleton(applicationResult));
        }
        dispatcher = builder.build(rpcService);
        dispatcher.start();

        if (triggerShutDown) {
            assertEquals(
                    ApplicationStatus.SUCCEEDED,
                    dispatcher.getShutDownFuture().get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
        } else {
            assertThatThrownBy(
                            () -> dispatcher.getShutDownFuture().get(100L, TimeUnit.MILLISECONDS))
                    .isInstanceOf(TimeoutException.class);
        }

        assertFalse(bootstrapLatch.isTriggered());
    }

    @Test
    public void testThatDirtilyFinishedApplicationsNotRetriggered() {
        final AbstractApplication application =
                TestingApplication.builder().setApplicationId(applicationId).build();
        final ApplicationResult applicationResult =
                TestingApplicationResultStore.createSuccessfulApplicationResult(applicationId);

        assertThrows(
                IllegalArgumentException.class,
                () ->
                        createTestingDispatcherBuilder()
                                .setRecoveredApplications(Collections.singleton(application))
                                .setRecoveredDirtyApplications(
                                        Collections.singleton(applicationResult))
                                .build(rpcService));
    }

    @Test
    public void testApplicationCleanupWithoutRecoveredApplication() throws Exception {
        final ApplicationResult applicationResult =
                TestingApplicationResultStore.createSuccessfulApplicationResult(applicationId);
        haServices
                .getApplicationResultStore()
                .createDirtyResultAsync(new ApplicationResultEntry(applicationResult))
                .get();
        dispatcher =
                createTestingDispatcherBuilder()
                        .setRecoveredDirtyApplications(Collections.singleton(applicationResult))
                        .build(rpcService);

        dispatcher.start();
        dispatcher.waitUntilStarted();

        ArchivedApplication application =
                dispatcher.requestApplication(applicationId, TIMEOUT).get();

        assertThat(application.getApplicationId()).isEqualTo(applicationId);
        assertThat(application.getApplicationStatus()).isEqualTo(ApplicationState.FINISHED);

        CommonTestUtils.waitUntilCondition(
                () ->
                        haServices
                                .getApplicationResultStore()
                                .hasCleanApplicationResultEntryAsync(applicationId)
                                .get());
    }

    @Test
    public void testOnlyRecoveredApplicationsAreRetainedInTheBlobServer() throws Exception {
        final ApplicationID applicationId1 = new ApplicationID();
        final ApplicationID applicationId2 = new ApplicationID();
        final byte[] fileContent = {1, 2, 3, 4};
        final PermanentBlobKey blobKey1 = blobServer.putPermanent(applicationId1, fileContent);
        final PermanentBlobKey blobKey2 = blobServer.putPermanent(applicationId2, fileContent);

        final AbstractApplication application1 =
                TestingApplication.builder().setApplicationId(applicationId1).build();

        dispatcher =
                createTestingDispatcherBuilder()
                        .setRecoveredApplications(Collections.singleton(application1))
                        .build(rpcService);

        assertThat(blobServer.getFile(applicationId1, blobKey1)).hasBinaryContent(fileContent);
        assertThatThrownBy(() -> blobServer.getFile(applicationId2, blobKey2))
                .isInstanceOf(NoSuchFileException.class);
    }

    private CompletableFuture<?> submitApplicationAndMockApplicationStatusChange(
            ApplicationState targetState) throws Exception {
        CompletableFuture<?> applicationTerminationFuture = submitApplication();
        mockApplicationStatusChange(targetState);
        return applicationTerminationFuture;
    }

    private CompletableFuture<?> submitApplication() throws Exception {
        dispatcher
                .submitApplication(
                        TestingApplication.builder().setApplicationId(applicationId).build(),
                        TIMEOUT)
                .get();
        return dispatcher.getApplicationTerminationFuture(applicationId);
    }

    private void mockApplicationStatusChange(ApplicationState targetState) throws Exception {
        dispatcher
                .callAsyncInMainThread(
                        () -> {
                            dispatcher.notifyApplicationStatusChange(applicationId, targetState);
                            return CompletableFuture.completedFuture(null);
                        })
                .get();
    }

    private TestingDispatcher.Builder createTestingDispatcherBuilder() {
        return TestingDispatcher.builder()
                .setConfiguration(configuration)
                .setHighAvailabilityServices(haServices)
                .setExecutionPlanWriter(haServices.getExecutionPlanStore())
                .setJobResultStore(haServices.getJobResultStore())
                .setApplicationWriter(haServices.getApplicationStore())
                .setApplicationResultStore(haServices.getApplicationResultStore())
                .setJobManagerRunnerFactory(JobMasterServiceLeadershipRunnerFactory.INSTANCE)
                .setCleanupRunnerFactory(CheckpointResourcesCleanupRunnerFactory.INSTANCE)
                .setFatalErrorHandler(
                        testingFatalErrorHandlerResource.getTestingFatalErrorHandler())
                .setBlobServer(blobServer);
    }
}
