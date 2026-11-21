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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.application.SingleJobApplication;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobUtils;
import org.apache.flink.runtime.blob.TestingBlobStoreBuilder;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.dispatcher.cleanup.TestingResourceCleanerFactory;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerExtension;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.Reference;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.core.testutils.FlinkAssertions.anyCauseMatches;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.dispatcher.AbstractDispatcherTest.awaitStatus;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests the resource cleanup by the {@link Dispatcher}. */
class DispatcherResourceCleanupTest {

    @TempDir private File temporaryFolder;

    @RegisterExtension
    final TestingFatalErrorHandlerExtension testingFatalErrorHandlerExtension =
            new TestingFatalErrorHandlerExtension();

    private static final Duration timeout = Duration.ofSeconds(10L);

    private static TestingRpcService rpcService;

    private JobID jobId;

    private JobGraph jobGraph;

    private SingleJobApplication application;

    private ApplicationID applicationId;

    private TestingDispatcher dispatcher;

    private DispatcherGateway dispatcherGateway;

    private BlobServer blobServer;

    private CompletableFuture<JobID> localCleanupFuture;
    private CompletableFuture<JobID> globalCleanupFuture;

    @BeforeAll
    static void setupClass() {
        rpcService = new TestingRpcService();
    }

    @BeforeEach
    void setup() throws Exception {
        jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        jobId = jobGraph.getJobID();
        application = new SingleJobApplication(jobGraph);
        applicationId = application.getApplicationId();

        globalCleanupFuture = new CompletableFuture<>();
        localCleanupFuture = new CompletableFuture<>();

        blobServer =
                BlobUtils.createBlobServer(
                        new Configuration(),
                        Reference.owned(temporaryFolder),
                        new TestingBlobStoreBuilder().createTestingBlobStore());
    }

    private TestingJobManagerRunnerFactory startDispatcherAndSubmitApplication() throws Exception {
        return startDispatcherAndSubmitApplication(0);
    }

    private TestingJobManagerRunnerFactory startDispatcherAndSubmitApplication(
            int numBlockingJobManagerRunners) throws Exception {
        return startDispatcherAndSubmitApplication(
                createTestingDispatcherBuilder(), numBlockingJobManagerRunners);
    }

    private TestingJobManagerRunnerFactory startDispatcherAndSubmitApplication(
            TestingDispatcher.Builder dispatcherBuilder, int numBlockingJobManagerRunners)
            throws Exception {
        final TestingJobMasterServiceLeadershipRunnerFactory testingJobManagerRunnerFactoryNG =
                new TestingJobMasterServiceLeadershipRunnerFactory(numBlockingJobManagerRunners);
        startDispatcher(dispatcherBuilder, testingJobManagerRunnerFactoryNG);
        submitApplicationAndWait();

        return testingJobManagerRunnerFactoryNG;
    }

    private void startDispatcher(JobManagerRunnerFactory jobManagerRunnerFactory) throws Exception {
        startDispatcher(createTestingDispatcherBuilder(), jobManagerRunnerFactory);
    }

    private void startDispatcher(
            TestingDispatcher.Builder dispatcherBuilder,
            JobManagerRunnerFactory jobManagerRunnerFactory)
            throws Exception {
        dispatcher =
                dispatcherBuilder
                        .setJobManagerRunnerFactory(jobManagerRunnerFactory)
                        .build(rpcService);

        dispatcher.start();

        dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
    }

    private TestingDispatcher.Builder createTestingDispatcherBuilder() {
        final JobManagerRunnerRegistry jobManagerRunnerRegistry =
                new DefaultJobManagerRunnerRegistry(2);
        return TestingDispatcher.builder()
                .setBlobServer(blobServer)
                .setJobManagerRunnerRegistry(jobManagerRunnerRegistry)
                .setFatalErrorHandler(
                        testingFatalErrorHandlerExtension.getTestingFatalErrorHandler())
                .setResourceCleanerFactory(
                        TestingResourceCleanerFactory.builder()
                                // JobManagerRunnerRegistry needs to be added explicitly
                                // because cleaning it will trigger the closeAsync latch
                                // provided by TestingJobManagerRunner
                                .withLocallyCleanableResource(jobManagerRunnerRegistry)
                                .withGloballyCleanableResource(
                                        (jobId, ignoredExecutor) -> {
                                            globalCleanupFuture.complete(jobId);
                                            return FutureUtils.completedVoidFuture();
                                        })
                                .withLocallyCleanableResource(
                                        (jobId, ignoredExecutor) -> {
                                            localCleanupFuture.complete(jobId);
                                            return FutureUtils.completedVoidFuture();
                                        })
                                .build());
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
    void testGlobalCleanupWhenJobFinished() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication();

        // complete the job
        finishJobAndApplication(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertGlobalCleanupTriggered(jobId);
    }

    @Test
    void testGlobalCleanupWhenJobCanceled() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication();

        // complete the job
        cancelJob(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertGlobalCleanupTriggered(jobId);
    }

    private CompletableFuture<Acknowledge> submitApplication() {
        return dispatcherGateway.submitApplication(application, timeout);
    }

    private void submitApplicationAndWait() {
        submitApplication().join();
    }

    @Test
    void testLocalCleanupWhenJobNotFinished() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication();

        // job not finished
        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        suspendJob(testingJobManagerRunner);

        assertLocalCleanupTriggered(jobId);
    }

    @Test
    void testGlobalCleanupWhenJobSubmissionFails() throws Exception {
        startDispatcher(new FailingJobManagerRunnerFactory(new FlinkException("Test exception")));
        final CompletableFuture<Acknowledge> submissionFuture = submitApplication();

        assertThatThrownBy(submissionFuture::get).hasCauseInstanceOf(JobSubmissionException.class);

        assertGlobalCleanupTriggered(jobId);
    }

    @Test
    void testLocalCleanupWhenClosingDispatcher() throws Exception {
        startDispatcherAndSubmitApplication();

        dispatcher.closeAsync().get();

        assertLocalCleanupTriggered(jobId);
    }

    @Test
    void testGlobalCleanupWhenJobFinishedWhileClosingDispatcher() throws Exception {
        final TestingJobManagerRunner testingJobManagerRunner =
                TestingJobManagerRunner.newBuilder()
                        .setBlockingTermination(true)
                        .setJobId(jobId)
                        .build();

        final Queue<JobManagerRunner> jobManagerRunners =
                new ArrayDeque<>(Arrays.asList(testingJobManagerRunner));

        startDispatcher(new QueueJobManagerRunnerFactory(jobManagerRunners));
        submitApplicationAndWait();

        final CompletableFuture<Void> dispatcherTerminationFuture = dispatcher.closeAsync();

        testingJobManagerRunner.getCloseAsyncCalledLatch().await();
        testingJobManagerRunner.completeResultFuture(
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(jobId)
                                .setState(JobStatus.FINISHED)
                                .build()));

        testingJobManagerRunner.completeTerminationFuture();

        // check that no exceptions have been thrown
        dispatcherTerminationFuture.get();

        assertGlobalCleanupTriggered(jobId);
    }

    @Test
    void testJobBeingMarkedAsDirtyBeforeCleanup() throws Exception {
        final OneShotLatch markAsDirtyLatch = new OneShotLatch();

        final TestingDispatcher.Builder dispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setJobResultStore(
                                TestingJobResultStore.builder()
                                        .withCreateDirtyResultConsumer(
                                                ignoredJobResultEntry -> {
                                                    try {
                                                        markAsDirtyLatch.await();
                                                    } catch (InterruptedException e) {
                                                        Thread.currentThread().interrupt();
                                                        return FutureUtils.completedExceptionally(
                                                                e);
                                                    }
                                                    return FutureUtils.completedVoidFuture();
                                                })
                                        .build());

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication(dispatcherBuilder, 0);

        finishJobAndApplication(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertThatNoCleanupWasTriggered();

        markAsDirtyLatch.trigger();

        assertGlobalCleanupTriggered(jobId);
    }

    @Test
    void testJobBeingMarkedAsCleanAfterCleanup() throws Exception {
        final CompletableFuture<JobID> markAsCleanFuture = new CompletableFuture<>();

        final JobResultStore jobResultStore =
                TestingJobResultStore.builder()
                        .withMarkResultAsCleanConsumer(
                                jobID -> {
                                    markAsCleanFuture.complete(jobID);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();
        final OneShotLatch localCleanupLatch = new OneShotLatch();
        final OneShotLatch globalCleanupLatch = new OneShotLatch();
        final TestingResourceCleanerFactory resourceCleanerFactory =
                TestingResourceCleanerFactory.builder()
                        .withLocallyCleanableResource(
                                (ignoredJobId, ignoredExecutor) -> {
                                    try {
                                        localCleanupLatch.await();
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }

                                    return FutureUtils.completedVoidFuture();
                                })
                        .withGloballyCleanableResource(
                                (ignoredJobId, ignoredExecutor) -> {
                                    try {
                                        globalCleanupLatch.await();
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }

                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();

        final TestingDispatcher.Builder dispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setJobResultStore(jobResultStore)
                        .setResourceCleanerFactory(resourceCleanerFactory);

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication(dispatcherBuilder, 0);

        finishJobAndApplication(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertThat(markAsCleanFuture).isNotDone();

        localCleanupLatch.trigger();
        assertThat(markAsCleanFuture).isNotDone();
        globalCleanupLatch.trigger();

        assertThat(markAsCleanFuture.get()).isEqualTo(jobId);
    }

    /**
     * Tests that the previous JobManager needs to be completely terminated before a new job with
     * the same {@link JobID} is started.
     */
    @Test
    void testJobSubmissionUnderSameJobId() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication(1);

        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        suspendJob(testingJobManagerRunner);

        // wait until termination JobManagerRunner closeAsync has been called.
        // this is necessary to avoid race conditions with completion of the 1st job and the
        // submission of the 2nd job (DuplicateJobSubmissionException).
        testingJobManagerRunner.getCloseAsyncCalledLatch().await();

        final CompletableFuture<Acknowledge> submissionFuture =
                dispatcherGateway.submitJob(jobGraph, timeout);

        try {
            assertThatFuture(submissionFuture)
                    .as(
                            "The job submission future should not complete until the previous JobManager "
                                    + "termination future has been completed.")
                    .willNotCompleteWithin(Duration.ofMillis(10L));
        } finally {
            testingJobManagerRunner.completeTerminationFuture();
        }

        assertThat(submissionFuture.get()).isEqualTo(Acknowledge.get());
    }

    /**
     * Tests that a duplicate job submission won't delete any job meta data (submitted job graphs,
     * blobs, etc.).
     */
    @Test
    void testDuplicateJobSubmissionDoesNotDeleteJobMetaData() throws Exception {
        final TestingJobManagerRunnerFactory testingJobManagerRunnerFactoryNG =
                startDispatcherAndSubmitApplication();

        final CompletableFuture<Acknowledge> submissionFuture =
                dispatcherGateway.submitJob(jobGraph, timeout);

        try {
            assertThatThrownBy(submissionFuture::get)
                    .satisfies(anyCauseMatches(DuplicateJobSubmissionException.class));

            assertThatNoCleanupWasTriggered();
        } finally {
            finishJobAndApplication(testingJobManagerRunnerFactoryNG.takeCreatedJobManagerRunner());
        }

        assertGlobalCleanupTriggered(jobId);
    }

    private void finishJobAndApplication(TestingJobManagerRunner takeCreatedJobManagerRunner) {
        terminateJobWithState(takeCreatedJobManagerRunner, JobStatus.FINISHED);
        application.jobStatusChanges(
                takeCreatedJobManagerRunner.getJobID(),
                JobStatus.FINISHED,
                System.currentTimeMillis());
    }

    private void suspendJob(TestingJobManagerRunner takeCreatedJobManagerRunner) {
        terminateJobWithState(takeCreatedJobManagerRunner, JobStatus.SUSPENDED);
    }

    private void failJobAndApplication(TestingJobManagerRunner takeCreatedJobManagerRunner) {
        terminateJobWithState(takeCreatedJobManagerRunner, JobStatus.FAILED);
        application.jobStatusChanges(
                takeCreatedJobManagerRunner.getJobID(),
                JobStatus.FAILED,
                System.currentTimeMillis());
    }

    private void cancelJob(TestingJobManagerRunner takeCreatedJobManagerRunner) {
        terminateJobWithState(takeCreatedJobManagerRunner, JobStatus.CANCELED);
    }

    private void terminateJobWithState(
            TestingJobManagerRunner takeCreatedJobManagerRunner, JobStatus state) {
        final ArchivedExecutionGraphBuilder archivedExecutionGraphBuilder =
                new ArchivedExecutionGraphBuilder().setJobID(jobId).setState(state);

        if (state == JobStatus.FAILED) {
            archivedExecutionGraphBuilder.setFailureCause(
                    new ErrorInfo(
                            new FlinkException("Test job failure"), System.currentTimeMillis()));
        }

        takeCreatedJobManagerRunner.completeResultFuture(
                new ExecutionGraphInfo(archivedExecutionGraphBuilder.build()));
    }

    private void assertThatNoCleanupWasTriggered() {
        assertThat(globalCleanupFuture).isNotDone();
        assertThat(localCleanupFuture).isNotDone();
    }

    @Test
    void testDispatcherTerminationTerminatesRunningJobMasters() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication();

        dispatcher.closeAsync().get();

        final TestingJobManagerRunner jobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        assertThat(jobManagerRunner.getTerminationFuture()).isDone();
    }

    /** Tests that terminating the Dispatcher will wait for all JobMasters to be terminated. */
    @Test
    void testDispatcherTerminationWaitsForJobMasterTerminations() throws Exception {
        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication(1);

        final CompletableFuture<Void> dispatcherTerminationFuture = dispatcher.closeAsync();

        try {
            assertThatFuture(dispatcherTerminationFuture)
                    .as("We should not terminate before all running JobMasters have terminated.")
                    .willNotCompleteWithin(Duration.ofMillis(10L));
        } finally {
            jobManagerRunnerFactory.takeCreatedJobManagerRunner().completeTerminationFuture();
        }

        dispatcherTerminationFuture.get();
    }

    private void assertLocalCleanupTriggered(JobID jobId)
            throws ExecutionException, InterruptedException {
        assertThat(localCleanupFuture.get()).isEqualTo(jobId);
        assertThat(globalCleanupFuture).isNotDone();
    }

    private void assertGlobalCleanupTriggered(JobID jobId)
            throws ExecutionException, InterruptedException {
        assertThat(localCleanupFuture).isNotDone();
        assertThat(globalCleanupFuture.get()).isEqualTo(jobId);
    }

    @Test
    void testFatalErrorIfJobCannotBeMarkedDirtyInJobResultStore() throws Exception {
        final JobResultStore jobResultStore =
                TestingJobResultStore.builder()
                        .withCreateDirtyResultConsumer(
                                jobResult ->
                                        FutureUtils.completedExceptionally(
                                                new IOException("Expected IOException.")))
                        .build();

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication(
                        createTestingDispatcherBuilder().setJobResultStore(jobResultStore), 0);

        ArchivedExecutionGraph executionGraph =
                new ArchivedExecutionGraphBuilder()
                        .setJobID(jobId)
                        .setState(JobStatus.FINISHED)
                        .build();

        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        testingJobManagerRunner.completeResultFuture(new ExecutionGraphInfo(executionGraph));

        final CompletableFuture<? extends Throwable> errorFuture =
                testingFatalErrorHandlerExtension.getTestingFatalErrorHandler().getErrorFuture();
        assertThat(errorFuture.get()).isInstanceOf(FlinkException.class);
        testingFatalErrorHandlerExtension.getTestingFatalErrorHandler().clearError();
    }

    @Test
    void testErrorHandlingIfJobCannotBeMarkedAsCleanInJobResultStore() throws Exception {
        final CompletableFuture<JobResultEntry> dirtyJobFuture = new CompletableFuture<>();
        final JobResultStore jobResultStore =
                TestingJobResultStore.builder()
                        .withCreateDirtyResultConsumer(
                                jobResultEntry -> {
                                    dirtyJobFuture.complete(jobResultEntry);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .withMarkResultAsCleanConsumer(
                                jobId ->
                                        FutureUtils.completedExceptionally(
                                                new IOException("Expected IOException.")))
                        .build();

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication(
                        createTestingDispatcherBuilder().setJobResultStore(jobResultStore), 0);

        ArchivedExecutionGraph executionGraph =
                new ArchivedExecutionGraphBuilder()
                        .setJobID(jobId)
                        .setState(JobStatus.FINISHED)
                        .build();

        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();
        testingJobManagerRunner.completeResultFuture(new ExecutionGraphInfo(executionGraph));

        final CompletableFuture<? extends Throwable> errorFuture =
                testingFatalErrorHandlerExtension.getTestingFatalErrorHandler().getErrorFuture();
        assertThatFuture(errorFuture)
                .as("No error should have been reported.")
                .willNotCompleteWithin(Duration.ofMillis(100L));

        assertThat(dirtyJobFuture.get().getJobId()).isEqualTo(jobId);
    }

    /** Tests that a failing {@link JobManagerRunner} will be properly cleaned up. */
    @Test
    void testFailingJobManagerRunnerCleanup() throws Exception {
        final FlinkException testException = new FlinkException("Test exception.");
        final ArrayBlockingQueue<Optional<Exception>> queue = new ArrayBlockingQueue<>(2);

        final BlockingJobManagerRunnerFactory blockingJobManagerRunnerFactory =
                new BlockingJobManagerRunnerFactory(
                        () -> {
                            final Optional<Exception> maybeException = queue.take();
                            if (maybeException.isPresent()) {
                                throw maybeException.get();
                            }
                        });

        startDispatcher(blockingJobManagerRunnerFactory);

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        // submit a mocked application to hold the job
        dispatcherGateway.submitApplication(
                TestingApplication.builder().setApplicationId(applicationId).build(), timeout);

        // submit and fail during job master runner construction
        queue.offer(Optional.of(testException));
        assertThatThrownBy(() -> dispatcherGateway.submitJob(jobGraph, Duration.ofMinutes(1)).get())
                .hasCauseInstanceOf(FlinkException.class)
                .hasRootCauseMessage(testException.getMessage());

        // make sure we've cleaned up in correct order (including HA)
        assertGlobalCleanupTriggered(jobId);

        // don't fail this time
        queue.offer(Optional.empty());
        // submit job again
        dispatcherGateway.submitJob(jobGraph, Duration.ofMinutes(1)).get();
        blockingJobManagerRunnerFactory.setJobStatus(JobStatus.RUNNING);

        // Ensure job is running
        awaitStatus(dispatcherGateway, jobId, JobStatus.RUNNING);
    }

    @Test
    void testArchivingFinishedJobToHistoryServer() throws Exception {

        final CompletableFuture<Acknowledge> archiveFuture = new CompletableFuture<>();

        final TestingDispatcher.Builder testingDispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setHistoryServerArchivist(
                                TestingHistoryServerArchivist.builder()
                                        .setArchiveExecutionGraphFunction(
                                                (executionGraphInfo, applicationId) ->
                                                        archiveFuture)
                                        .build());

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication(testingDispatcherBuilder, 0);

        finishJobAndApplication(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        // Before the archiving is finished, the cleanup is not finished and the job is not
        // terminated
        assertThatNoCleanupWasTriggered();
        final CompletableFuture<Void> jobTerminationFuture =
                dispatcher.getJobTerminationFuture(jobId, Duration.ofHours(1));
        assertThat(jobTerminationFuture).isNotDone();

        archiveFuture.complete(Acknowledge.get());

        // Once the archive is finished, the cleanup is finished and the job is terminated.
        assertGlobalCleanupTriggered(jobId);
        jobTerminationFuture.join();
    }

    @Test
    void testNotArchivingSuspendedJobToHistoryServer() throws Exception {

        final AtomicBoolean isArchived = new AtomicBoolean(false);

        final TestingDispatcher.Builder testingDispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setHistoryServerArchivist(
                                TestingHistoryServerArchivist.builder()
                                        .setArchiveExecutionGraphFunction(
                                                (executionGraphInfo, applicationId) -> {
                                                    isArchived.set(true);
                                                    return CompletableFuture.completedFuture(
                                                            Acknowledge.get());
                                                })
                                        .build());

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication(testingDispatcherBuilder, 0);

        suspendJob(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertLocalCleanupTriggered(jobId);
        dispatcher.getJobTerminationFuture(jobId, Duration.ofHours(1)).join();

        assertThat(isArchived).isFalse();
    }

    @Test
    void testNotArchivingFinishedJobToHistoryServerWhenOnlyFailedJobsConfigured() throws Exception {

        final AtomicBoolean isArchived = new AtomicBoolean(false);

        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.ARCHIVE_ON_FAILED_JOBS_ONLY, true);

        final TestingDispatcher.Builder testingDispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setConfiguration(configuration)
                        .setHistoryServerArchivist(
                                TestingHistoryServerArchivist.builder()
                                        .setArchiveExecutionGraphFunction(
                                                (executionGraphInfo, applicationId) -> {
                                                    isArchived.set(true);
                                                    return CompletableFuture.completedFuture(
                                                            Acknowledge.get());
                                                })
                                        .build());

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication(testingDispatcherBuilder, 0);

        finishJobAndApplication(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertGlobalCleanupTriggered(jobId);
        dispatcher.getJobTerminationFuture(jobId, Duration.ofHours(1)).join();

        assertThat(isArchived).isFalse();
    }

    @Test
    void testArchivingFailedJobToHistoryServerWhenOnlyFailedJobsConfigured() throws Exception {

        final AtomicBoolean isArchived = new AtomicBoolean(false);

        final Configuration configuration = new Configuration();
        configuration.set(JobManagerOptions.ARCHIVE_ON_FAILED_JOBS_ONLY, true);

        final TestingDispatcher.Builder testingDispatcherBuilder =
                createTestingDispatcherBuilder()
                        .setConfiguration(configuration)
                        .setHistoryServerArchivist(
                                TestingHistoryServerArchivist.builder()
                                        .setArchiveExecutionGraphFunction(
                                                (executionGraphInfo, applicationId) -> {
                                                    isArchived.set(true);
                                                    return CompletableFuture.completedFuture(
                                                            Acknowledge.get());
                                                })
                                        .build());

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                startDispatcherAndSubmitApplication(testingDispatcherBuilder, 0);

        failJobAndApplication(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

        assertGlobalCleanupTriggered(jobId);
        dispatcher.getJobTerminationFuture(jobId, Duration.ofHours(1)).join();

        assertThat(isArchived).isTrue();
    }

    private static final class BlockingJobManagerRunnerFactory
            extends TestingJobMasterServiceLeadershipRunnerFactory {

        private final ThrowingRunnable<Exception> jobManagerRunnerCreationLatch;
        private TestingJobManagerRunner testingRunner;

        BlockingJobManagerRunnerFactory(ThrowingRunnable<Exception> jobManagerRunnerCreationLatch) {
            this.jobManagerRunnerCreationLatch = jobManagerRunnerCreationLatch;
        }

        @Override
        public TestingJobManagerRunner createJobManagerRunner(
                ExecutionPlan executionPlan,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerSharedServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                JobStatusListener jobStatusListener,
                long initializationTimestamp)
                throws Exception {
            jobManagerRunnerCreationLatch.run();

            this.testingRunner =
                    super.createJobManagerRunner(
                            executionPlan,
                            configuration,
                            rpcService,
                            highAvailabilityServices,
                            heartbeatServices,
                            jobManagerSharedServices,
                            jobManagerJobMetricGroupFactory,
                            fatalErrorHandler,
                            failureEnrichers,
                            null,
                            initializationTimestamp);

            TestingJobMasterGateway testingJobMasterGateway =
                    new TestingJobMasterGatewayBuilder()
                            .setRequestJobSupplier(
                                    () ->
                                            CompletableFuture.completedFuture(
                                                    new ExecutionGraphInfo(
                                                            ArchivedExecutionGraph
                                                                    .createSparseArchivedExecutionGraph(
                                                                            executionPlan
                                                                                    .getJobID(),
                                                                            executionPlan.getName(),
                                                                            JobStatus.RUNNING,
                                                                            null,
                                                                            null,
                                                                            null,
                                                                            1337))))
                            .build();
            testingRunner.completeJobMasterGatewayFuture(testingJobMasterGateway);
            return testingRunner;
        }

        public void setJobStatus(JobStatus newStatus) {
            Preconditions.checkState(
                    testingRunner != null,
                    "JobManagerRunner must be created before this method is available");
            this.testingRunner.setJobStatus(newStatus);
        }
    }

    private static final class QueueJobManagerRunnerFactory implements JobManagerRunnerFactory {
        private final Queue<? extends JobManagerRunner> jobManagerRunners;

        private QueueJobManagerRunnerFactory(Queue<? extends JobManagerRunner> jobManagerRunners) {
            this.jobManagerRunners = jobManagerRunners;
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                ExecutionPlan executionPlan,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                JobStatusListener jobStatusListener,
                long initializationTimestamp) {
            return Optional.ofNullable(jobManagerRunners.poll())
                    .orElseThrow(
                            () ->
                                    new IllegalStateException(
                                            "Cannot create more JobManagerRunners."));
        }
    }

    private class FailingJobManagerRunnerFactory implements JobManagerRunnerFactory {
        private final Exception testException;

        public FailingJobManagerRunnerFactory(FlinkException testException) {
            this.testException = testException;
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                ExecutionPlan executionPlan,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                JobStatusListener jobStatusListener,
                long initializationTimestamp)
                throws Exception {
            throw testException;
        }
    }
}
