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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.testutils.FlinkAssertions;
import org.apache.flink.core.testutils.FlinkMatchers;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.dispatcher.cleanup.TestingCleanupRunnerFactory;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.JobResultEntry;
import org.apache.flink.runtime.highavailability.JobResultStore;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerRunnerResult;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterService;
import org.apache.flink.runtime.jobmaster.JobMasterServiceLeadershipRunner;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.jobmaster.TestingJobMasterService;
import org.apache.flink.runtime.jobmaster.factories.DefaultJobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.factories.TestingJobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.LeaderElection;
import org.apache.flink.runtime.leaderelection.TestingLeaderElection;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.FlinkJobTerminatedWithoutCancellationException;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.handler.RestHandlerException;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.runtime.testutils.TestingJobResultStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.concurrent.FutureUtils;

import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.http.HttpResponseStatus;

import org.assertj.core.api.Assertions;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Test for the {@link Dispatcher} component. */
public class DispatcherTest extends AbstractDispatcherTest {

    private JobGraph jobGraph;

    private JobID jobId;

    private TestingLeaderElection jobMasterLeaderElection;

    /** Instance under test. */
    private TestingDispatcher dispatcher;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        jobId = jobGraph.getJobID();
        jobMasterLeaderElection = new TestingLeaderElection();
        haServices.setJobMasterLeaderElection(jobId, jobMasterLeaderElection);
    }

    @Nonnull
    private TestingDispatcher createAndStartDispatcher(
            HeartbeatServices heartbeatServices,
            TestingHighAvailabilityServices haServices,
            JobManagerRunnerFactory jobManagerRunnerFactory)
            throws Exception {
        final TestingDispatcher dispatcher =
                createTestingDispatcherBuilder()
                        .setHighAvailabilityServices(haServices)
                        .setHeartbeatServices(heartbeatServices)
                        .setJobManagerRunnerFactory(jobManagerRunnerFactory)
                        .setJobGraphWriter(haServices.getJobGraphStore())
                        .setJobResultStore(haServices.getJobResultStore())
                        .build(rpcService);
        dispatcher.start();
        return dispatcher;
    }

    @After
    public void tearDown() throws Exception {
        if (dispatcher != null) {
            RpcUtils.terminateRpcEndpoint(dispatcher);
        }
        // jobMasterLeaderElection is closed as part of the haServices close call
        super.tearDown();
    }

    /**
     * Tests that we can submit a job to the Dispatcher which then spawns a new JobManagerRunner.
     */
    @Test
    public void testJobSubmission() throws Exception {
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(jobId));
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        jobMasterLeaderElection.getStartFuture().get();

        assertTrue(
                "jobManagerRunner was not started",
                jobMasterLeaderElection.getStartFuture().isDone());
    }

    @Test
    public void testDuplicateJobSubmissionWithGloballyTerminatedButDirtyJob() throws Exception {
        final JobResult jobResult =
                TestingJobResultStore.createJobResult(
                        jobGraph.getJobID(), ApplicationStatus.SUCCEEDED);
        haServices.getJobResultStore().createDirtyResultAsync(new JobResultEntry(jobResult)).get();
        assertDuplicateJobSubmission();
    }

    @Test
    public void testDuplicateJobSubmissionWithGloballyTerminatedAndCleanedJob() throws Exception {
        final JobResult jobResult =
                TestingJobResultStore.createJobResult(
                        jobGraph.getJobID(), ApplicationStatus.SUCCEEDED);
        haServices.getJobResultStore().createDirtyResultAsync(new JobResultEntry(jobResult)).get();
        haServices.getJobResultStore().markResultAsCleanAsync(jobGraph.getJobID()).get();

        assertDuplicateJobSubmission();
    }

    @Test
    public void testDuplicateJobSubmissionIsDetectedOnSimultaneousSubmission() throws Exception {
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new TestingJobMasterServiceLeadershipRunnerFactory());
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

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
                                    dispatcherGateway.submitJob(jobGraph, TIMEOUT).join();
                                } catch (Throwable t) {
                                    exceptions.add(t);
                                }
                            }));
        }

        // start worker threads and trigger job submissions
        threads.forEach(Thread::start);
        prepareLatch.await();
        startLatch.trigger();

        // wait for the job submissions to happen
        for (Thread thread : threads) {
            thread.join();
        }

        // verify the job was actually submitted
        FlinkAssertions.assertThatFuture(
                        dispatcherGateway.requestJobStatus(jobGraph.getJobID(), TIMEOUT))
                .eventuallySucceeds();

        // verify that all but one submission failed as duplicates
        Assertions.assertThat(exceptions)
                .hasSize(numThreads - 1)
                .allSatisfy(
                        t ->
                                Assertions.assertThat(t)
                                        .hasCauseInstanceOf(DuplicateJobSubmissionException.class));
    }

    private void assertDuplicateJobSubmission() throws Exception {
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(jobId));
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        final CompletableFuture<Acknowledge> submitFuture =
                dispatcherGateway.submitJob(jobGraph, TIMEOUT);
        final ExecutionException executionException =
                assertThrows(ExecutionException.class, submitFuture::get);
        assertTrue(executionException.getCause() instanceof DuplicateJobSubmissionException);
        final DuplicateJobSubmissionException duplicateException =
                (DuplicateJobSubmissionException) executionException.getCause();
        assertTrue(duplicateException.isGloballyTerminated());
    }

    @Test
    public void testDuplicateJobSubmissionWithRunningJobId() throws Exception {
        dispatcher =
                createTestingDispatcherBuilder()
                        .setJobManagerRunnerFactory(new ExpectedJobIdJobManagerRunnerFactory(jobId))
                        .setRecoveredJobs(Collections.singleton(jobGraph))
                        .build(rpcService);
        dispatcher.start();
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        final CompletableFuture<Acknowledge> submitFuture =
                dispatcherGateway.submitJob(jobGraph, TIMEOUT);
        final ExecutionException executionException =
                assertThrows(ExecutionException.class, submitFuture::get);
        assertTrue(executionException.getCause() instanceof DuplicateJobSubmissionException);
        final DuplicateJobSubmissionException duplicateException =
                (DuplicateJobSubmissionException) executionException.getCause();
        assertFalse(duplicateException.isGloballyTerminated());
    }

    /**
     * Tests that we can submit a job to the Dispatcher which then spawns a new JobManagerRunner.
     */
    @Test
    public void testJobSubmissionWithPartialResourceConfigured() throws Exception {
        ResourceSpec resourceSpec = ResourceSpec.newBuilder(2.0, 10).build();

        final JobVertex firstVertex = new JobVertex("firstVertex");
        firstVertex.setInvokableClass(NoOpInvokable.class);
        firstVertex.setResources(resourceSpec, resourceSpec);

        final JobVertex secondVertex = new JobVertex("secondVertex");
        secondVertex.setInvokableClass(NoOpInvokable.class);

        JobGraph jobGraphWithTwoVertices =
                JobGraphTestUtils.streamingJobGraph(firstVertex, secondVertex);

        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(jobId));

        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        CompletableFuture<Acknowledge> acknowledgeFuture =
                dispatcherGateway.submitJob(jobGraphWithTwoVertices, TIMEOUT);

        try {
            acknowledgeFuture.get();
            fail("job submission should have failed");
        } catch (ExecutionException e) {
            assertTrue(ExceptionUtils.findThrowable(e, JobSubmissionException.class).isPresent());
        }
    }

    @Test
    public void testNonBlockingJobSubmission() throws Exception {
        JobManagerRunnerWithBlockingJobMasterFactory blockingJobMaster =
                new JobManagerRunnerWithBlockingJobMasterFactory();
        dispatcher = createAndStartDispatcher(heartbeatServices, haServices, blockingJobMaster);
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        blockingJobMaster.waitForBlockingInit();

        // ensure INITIALIZING status
        assertThat(
                dispatcherGateway.requestJobStatus(jobId, TIMEOUT).get(),
                is(JobStatus.INITIALIZING));

        // ensure correct JobDetails
        MultipleJobsDetails multiDetails =
                dispatcherGateway.requestMultipleJobDetails(TIMEOUT).get();
        assertEquals(1, multiDetails.getJobs().size());
        assertEquals(jobId, multiDetails.getJobs().iterator().next().getJobId());

        // let the initialization finish.
        blockingJobMaster.unblockJobMasterInitialization();

        // ensure job is running
        awaitStatus(dispatcherGateway, jobId, JobStatus.RUNNING);
    }

    @Test
    public void testInvalidCallDuringInitialization() throws Exception {
        JobManagerRunnerWithBlockingJobMasterFactory blockingJobMaster =
                new JobManagerRunnerWithBlockingJobMasterFactory();
        dispatcher = createAndStartDispatcher(heartbeatServices, haServices, blockingJobMaster);
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        assertThat(
                dispatcherGateway.requestJobStatus(jobId, TIMEOUT).get(),
                is(JobStatus.INITIALIZING));

        // this call is supposed to fail
        try {
            dispatcherGateway
                    .triggerSavepointAndGetLocation(
                            jobId,
                            "file:///tmp/savepoint",
                            SavepointFormatType.CANONICAL,
                            TriggerSavepointMode.SAVEPOINT,
                            TIMEOUT)
                    .get();
            fail("Previous statement should have failed");
        } catch (ExecutionException t) {
            assertTrue(t.getCause() instanceof UnavailableDispatcherOperationException);
        }
    }

    @Test
    public void testCancellationDuringInitialization() throws Exception {
        final CancellableJobManagerRunnerWithInitializedJobFactory runnerFactory =
                new CancellableJobManagerRunnerWithInitializedJobFactory(jobId);
        dispatcher = createAndStartDispatcher(heartbeatServices, haServices, runnerFactory);

        jobMasterLeaderElection.isLeader(UUID.randomUUID());
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        assertThatFuture(dispatcherGateway.submitJob(jobGraph, TIMEOUT)).eventuallySucceeds();

        assertThatFuture(dispatcherGateway.requestJobStatus(jobGraph.getJobID(), TIMEOUT))
                .eventuallySucceeds()
                .isEqualTo(JobStatus.INITIALIZING);

        // submission has succeeded, now cancel the job
        final CompletableFuture<Acknowledge> cancellationRequestFuture =
                dispatcherGateway.cancelJob(jobGraph.getJobID(), TIMEOUT);
        assertThatFuture(dispatcherGateway.requestJobStatus(jobGraph.getJobID(), TIMEOUT))
                .eventuallySucceeds()
                .isEqualTo(JobStatus.CANCELLING);
        assertThatFuture(cancellationRequestFuture).isNotDone();

        // unblock the job cancellation
        runnerFactory.unblockCancellation();
        assertThatFuture(cancellationRequestFuture).eventuallySucceeds();

        assertThatFuture(dispatcherGateway.requestJobResult(jobGraph.getJobID(), TIMEOUT))
                .eventuallySucceeds()
                .extracting(JobResult::getApplicationStatus)
                .isEqualTo(ApplicationStatus.CANCELED);
    }

    @Test
    public void testCancellationOfCanceledTerminalDoesNotThrowException() throws Exception {
        final CompletableFuture<JobManagerRunnerResult> jobTerminationFuture =
                new CompletableFuture<>();
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new FinishingJobManagerRunnerFactory(jobTerminationFuture, () -> {}));
        jobMasterLeaderElection.isLeader(UUID.randomUUID());
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        JobID jobId = jobGraph.getJobID();

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        jobTerminationFuture.complete(
                JobManagerRunnerResult.forSuccess(
                        new ExecutionGraphInfo(
                                new ArchivedExecutionGraphBuilder()
                                        .setJobID(jobId)
                                        .setState(JobStatus.CANCELED)
                                        .build())));

        // wait for job to finish
        dispatcher.getJobTerminationFuture(jobId, TIMEOUT).get();
        // sanity check
        assertThat(
                dispatcherGateway.requestJobStatus(jobId, TIMEOUT).get(), is(JobStatus.CANCELED));

        dispatcherGateway.cancelJob(jobId, TIMEOUT).get();
    }

    @Test
    public void testCancellationOfNonCanceledTerminalJobFailsWithAppropriateException()
            throws Exception {

        final CompletableFuture<JobManagerRunnerResult> jobTerminationFuture =
                new CompletableFuture<>();
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new FinishingJobManagerRunnerFactory(jobTerminationFuture, () -> {}));
        jobMasterLeaderElection.isLeader(UUID.randomUUID());
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        JobID jobId = jobGraph.getJobID();

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        jobTerminationFuture.complete(
                JobManagerRunnerResult.forSuccess(
                        new ExecutionGraphInfo(
                                new ArchivedExecutionGraphBuilder()
                                        .setJobID(jobId)
                                        .setState(JobStatus.FINISHED)
                                        .build())));

        // wait for job to finish
        dispatcher.getJobTerminationFuture(jobId, TIMEOUT).get();
        // sanity check
        assertThat(
                dispatcherGateway.requestJobStatus(jobId, TIMEOUT).get(), is(JobStatus.FINISHED));

        final CompletableFuture<Acknowledge> cancelFuture =
                dispatcherGateway.cancelJob(jobId, TIMEOUT);

        assertThat(
                cancelFuture,
                FlinkMatchers.futureWillCompleteExceptionally(
                        FlinkJobTerminatedWithoutCancellationException.class, Duration.ofHours(8)));
    }

    @Test
    public void testNoHistoryServerArchiveCreatedForSuspendedJob() throws Exception {
        final CompletableFuture<Void> archiveAttemptFuture = new CompletableFuture<>();
        final CompletableFuture<JobManagerRunnerResult> jobTerminationFuture =
                new CompletableFuture<>();
        dispatcher =
                createTestingDispatcherBuilder()
                        .setJobManagerRunnerFactory(
                                new FinishingJobManagerRunnerFactory(
                                        jobTerminationFuture, () -> {}))
                        .setHistoryServerArchivist(
                                executionGraphInfo -> {
                                    archiveAttemptFuture.complete(null);
                                    return CompletableFuture.completedFuture(null);
                                })
                        .build(rpcService);
        dispatcher.start();
        jobMasterLeaderElection.isLeader(UUID.randomUUID());
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        JobID jobId = jobGraph.getJobID();

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        jobTerminationFuture.complete(
                JobManagerRunnerResult.forSuccess(
                        new ExecutionGraphInfo(
                                new ArchivedExecutionGraphBuilder()
                                        .setJobID(jobId)
                                        .setState(JobStatus.SUSPENDED)
                                        .build())));

        // wait for job to finish
        dispatcherGateway.requestJobResult(jobId, TIMEOUT).get();
        // sanity check
        assertThat(
                dispatcherGateway.requestJobStatus(jobId, TIMEOUT).get(), is(JobStatus.SUSPENDED));

        assertThat(archiveAttemptFuture.isDone(), is(false));
    }

    @Test
    public void testJobManagerRunnerInitializationFailureFailsJob() throws Exception {
        final TestingJobMasterServiceLeadershipRunnerFactory testingJobManagerRunnerFactory =
                new TestingJobMasterServiceLeadershipRunnerFactory();

        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices, haServices, testingJobManagerRunnerFactory);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        final JobGraph emptyJobGraph =
                JobGraphBuilder.newStreamingJobGraphBuilder().setJobId(jobId).build();

        dispatcherGateway.submitJob(emptyJobGraph, TIMEOUT).get();

        final TestingJobManagerRunner testingJobManagerRunner =
                testingJobManagerRunnerFactory.takeCreatedJobManagerRunner();

        final FlinkException testFailure = new FlinkException("Test failure");
        testingJobManagerRunner.completeResultFuture(
                JobManagerRunnerResult.forInitializationFailure(
                        new ExecutionGraphInfo(
                                ArchivedExecutionGraph.createSparseArchivedExecutionGraph(
                                        jobId,
                                        jobGraph.getName(),
                                        JobStatus.FAILED,
                                        testFailure,
                                        jobGraph.getCheckpointingSettings(),
                                        1L)),
                        testFailure));

        // wait till job has failed
        dispatcher.getJobTerminationFuture(jobId, TIMEOUT).get();

        // get failure cause
        ArchivedExecutionGraph execGraph =
                dispatcherGateway.requestJob(jobGraph.getJobID(), TIMEOUT).get();
        assertThat(execGraph.getState(), is(JobStatus.FAILED));

        Assert.assertNotNull(execGraph.getFailureInfo());
        Throwable throwable =
                execGraph
                        .getFailureInfo()
                        .getException()
                        .deserializeError(ClassLoader.getSystemClassLoader());

        // ensure correct exception type
        assertThat(throwable.getMessage(), equalTo(testFailure.getMessage()));
    }

    /** Test that {@link JobResult} is cached when the job finishes. */
    @Test
    public void testCacheJobExecutionResult() throws Exception {
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(jobId));

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        final JobID failedJobId = new JobID();

        final JobStatus expectedState = JobStatus.FAILED;
        final ExecutionGraphInfo failedExecutionGraphInfo =
                new ExecutionGraphInfo(
                        new ArchivedExecutionGraphBuilder()
                                .setJobID(failedJobId)
                                .setState(expectedState)
                                .setFailureCause(
                                        new ErrorInfo(new RuntimeException("expected"), 1L))
                                .build());

        dispatcher.completeJobExecution(failedExecutionGraphInfo);

        assertThat(
                dispatcherGateway.requestJobStatus(failedJobId, TIMEOUT).get(),
                equalTo(expectedState));
        final CompletableFuture<ExecutionGraphInfo> completableFutureCompletableFuture =
                dispatcher.callAsyncInMainThread(
                        () -> dispatcher.requestExecutionGraphInfo(failedJobId, TIMEOUT));
        assertThat(completableFutureCompletableFuture.get(), is(failedExecutionGraphInfo));
    }

    @Test
    public void testRetrieveCheckpointStats() throws Exception {
        CheckpointStatsSnapshot snapshot = CheckpointStatsSnapshot.empty();
        TestingJobMasterGateway testingJobMasterGateway =
                new TestingJobMasterGatewayBuilder()
                        .setCheckpointStatsSnapshotSupplier(
                                () -> CompletableFuture.completedFuture(snapshot))
                        .build();

        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new TestingJobMasterGatewayJobManagerRunnerFactory(
                                testingJobMasterGateway));
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        CompletableFuture<CheckpointStatsSnapshot> resultsFuture =
                dispatcher.callAsyncInMainThread(
                        () -> dispatcher.requestCheckpointStats(jobId, TIMEOUT));
        Assertions.assertThat(resultsFuture).succeedsWithin(Duration.ofSeconds(1));
        Assertions.assertThat(resultsFuture).isCompletedWithValue(snapshot);
    }

    @Test
    public void testThrowExceptionIfJobExecutionResultNotFound() throws Exception {
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(jobId));

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        try {
            dispatcherGateway.requestJob(new JobID(), TIMEOUT).get();
        } catch (ExecutionException e) {
            final Throwable throwable = ExceptionUtils.stripExecutionException(e);
            assertThat(throwable, instanceOf(FlinkJobNotFoundException.class));
        }
    }

    /** Tests that we can dispose a savepoint. */
    @Test
    public void testSavepointDisposal() throws Exception {
        final URI externalPointer = createTestingSavepoint();
        final Path savepointPath = Paths.get(externalPointer);

        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(jobId));

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        assertThat(Files.exists(savepointPath), is(true));

        dispatcherGateway.disposeSavepoint(externalPointer.toString(), TIMEOUT).get();

        assertThat(Files.exists(savepointPath), is(false));
    }

    @Nonnull
    private URI createTestingSavepoint() throws IOException, URISyntaxException {
        final CheckpointStorage storage =
                Checkpoints.loadCheckpointStorage(
                        configuration, Thread.currentThread().getContextClassLoader(), log);
        final CheckpointStorageCoordinatorView checkpointStorage =
                storage.createCheckpointStorage(jobGraph.getJobID());
        final File savepointFile = temporaryFolder.newFolder();
        final long checkpointId = 1L;

        final CheckpointStorageLocation checkpointStorageLocation =
                checkpointStorage.initializeLocationForSavepoint(
                        checkpointId, savepointFile.getAbsolutePath());

        final CheckpointMetadataOutputStream metadataOutputStream =
                checkpointStorageLocation.createMetadataOutputStream();
        Checkpoints.storeCheckpointMetadata(
                new CheckpointMetadata(
                        checkpointId, Collections.emptyList(), Collections.emptyList()),
                metadataOutputStream);

        final CompletedCheckpointStorageLocation completedCheckpointStorageLocation =
                metadataOutputStream.closeAndFinalizeCheckpoint();

        return new URI(completedCheckpointStorageLocation.getExternalPointer());
    }

    @Test
    public void testFatalErrorIfRecoveredJobsCannotBeStarted() throws Exception {
        testJobManagerRunnerFailureResultingInFatalError(
                (testingJobManagerRunner, actualError) ->
                        testingJobManagerRunner.completeResultFuture(
                                // Let the initialization of the JobManagerRunner fail
                                JobManagerRunnerResult.forInitializationFailure(
                                        new ExecutionGraphInfo(
                                                ArchivedExecutionGraph
                                                        .createSparseArchivedExecutionGraph(
                                                                jobId,
                                                                jobGraph.getName(),
                                                                JobStatus.FAILED,
                                                                actualError,
                                                                jobGraph.getCheckpointingSettings(),
                                                                1L)),
                                        actualError)));
    }

    @Test
    public void testFatalErrorIfSomeOtherErrorCausedTheJobMasterToFail() throws Exception {
        testJobManagerRunnerFailureResultingInFatalError(
                TestingJobManagerRunner::completeResultFutureExceptionally);
    }

    private void testJobManagerRunnerFailureResultingInFatalError(
            BiConsumer<TestingJobManagerRunner, Exception> jobManagerRunnerWithErrorConsumer)
            throws Exception {
        final FlinkException testException = new FlinkException("Expected test exception");
        jobMasterLeaderElection.isLeader(UUID.randomUUID());

        final TestingJobMasterServiceLeadershipRunnerFactory jobManagerRunnerFactory =
                new TestingJobMasterServiceLeadershipRunnerFactory();
        dispatcher =
                createTestingDispatcherBuilder()
                        .setJobManagerRunnerFactory(jobManagerRunnerFactory)
                        .setRecoveredJobs(Collections.singleton(JobGraphTestUtils.emptyJobGraph()))
                        .build(rpcService);

        dispatcher.start();

        final TestingFatalErrorHandler fatalErrorHandler =
                testingFatalErrorHandlerResource.getFatalErrorHandler();

        jobManagerRunnerWithErrorConsumer.accept(
                jobManagerRunnerFactory.takeCreatedJobManagerRunner(), testException);

        final Throwable error =
                fatalErrorHandler
                        .getErrorFuture()
                        .get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

        assertThat(
                ExceptionUtils.findThrowableWithMessage(error, testException.getMessage())
                        .isPresent(),
                is(true));

        fatalErrorHandler.clearError();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testThatDirtilyFinishedJobsNotBeingRetriggered() throws Exception {
        final JobGraph jobGraph = JobGraphTestUtils.emptyJobGraph();
        final JobResult jobResult =
                TestingJobResultStore.createSuccessfulJobResult(jobGraph.getJobID());
        dispatcher =
                createTestingDispatcherBuilder()
                        .setRecoveredJobs(Collections.singleton(jobGraph))
                        .setRecoveredDirtyJobs(Collections.singleton(jobResult))
                        .build(rpcService);
    }

    @Test
    public void testJobCleanupWithoutRecoveredJobGraph() throws Exception {
        final JobID jobIdOfRecoveredDirtyJobs = new JobID();
        final TestingJobMasterServiceLeadershipRunnerFactory jobManagerRunnerFactory =
                new TestingJobMasterServiceLeadershipRunnerFactory();
        final TestingCleanupRunnerFactory cleanupRunnerFactory = new TestingCleanupRunnerFactory();

        final OneShotLatch dispatcherBootstrapLatch = new OneShotLatch();
        dispatcher =
                createTestingDispatcherBuilder()
                        .setJobManagerRunnerFactory(jobManagerRunnerFactory)
                        .setCleanupRunnerFactory(cleanupRunnerFactory)
                        .setRecoveredDirtyJobs(
                                Collections.singleton(
                                        new JobResult.Builder()
                                                .jobId(jobIdOfRecoveredDirtyJobs)
                                                .applicationStatus(ApplicationStatus.SUCCEEDED)
                                                .netRuntime(1)
                                                .build()))
                        .setDispatcherBootstrapFactory(
                                (ignoredDispatcherGateway,
                                        ignoredScheduledExecutor,
                                        ignoredFatalErrorHandler) -> {
                                    dispatcherBootstrapLatch.trigger();
                                    return new NoOpDispatcherBootstrap();
                                })
                        .build(rpcService);

        dispatcher.start();

        dispatcherBootstrapLatch.await();

        final TestingJobManagerRunner cleanupRunner =
                cleanupRunnerFactory.takeCreatedJobManagerRunner();
        assertThat(
                "The CleanupJobManagerRunner has the wrong job ID attached.",
                cleanupRunner.getJobID(),
                is(jobIdOfRecoveredDirtyJobs));

        assertThat(
                "No JobMaster should have been started.",
                jobManagerRunnerFactory.getQueueSize(),
                is(0));
    }

    @Test
    public void testPersistedJobGraphWhenDispatcherIsShutDown() throws Exception {
        final TestingJobGraphStore submittedJobGraphStore =
                TestingJobGraphStore.newBuilder().build();
        submittedJobGraphStore.start(null);
        haServices.setJobGraphStore(submittedJobGraphStore);

        dispatcher =
                createTestingDispatcherBuilder()
                        .setJobGraphWriter(submittedJobGraphStore)
                        .build(rpcService);

        dispatcher.start();

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        assertThat(dispatcher.getNumberJobs(TIMEOUT).get(), Matchers.is(1));

        dispatcher.close();

        assertThat(submittedJobGraphStore.contains(jobGraph.getJobID()), Matchers.is(true));
    }

    /** Tests that a submitted job is suspended if the Dispatcher is terminated. */
    @Test
    public void testJobSuspensionWhenDispatcherIsTerminated() throws Exception {
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(jobId));

        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        final CompletableFuture<JobResult> jobResultFuture =
                dispatcherGateway.requestJobResult(jobGraph.getJobID(), TIMEOUT);

        assertThat(jobResultFuture.isDone(), is(false));

        dispatcher.close();

        final JobResult jobResult = jobResultFuture.get();
        assertEquals(jobResult.getApplicationStatus(), ApplicationStatus.UNKNOWN);
    }

    @Test
    public void testJobStatusIsShownDuringTermination() throws Exception {
        final JobID blockingId = new JobID();
        haServices.setJobMasterLeaderElection(blockingId, new TestingLeaderElection());
        final JobManagerRunnerWithBlockingTerminationFactory jobManagerRunnerFactory =
                new JobManagerRunnerWithBlockingTerminationFactory(blockingId);
        dispatcher =
                createAndStartDispatcher(heartbeatServices, haServices, jobManagerRunnerFactory);
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        final JobGraph blockedJobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        blockedJobGraph.setJobID(blockingId);

        // Submit two jobs, one blocks forever
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        dispatcherGateway.submitJob(blockedJobGraph, TIMEOUT).get();

        // Trigger termination
        final CompletableFuture<Void> terminationFuture = dispatcher.closeAsync();

        // ensure job eventually transitions to SUSPENDED state
        try {
            CommonTestUtils.waitUntilCondition(
                    () -> {
                        JobStatus status =
                                dispatcherGateway
                                        .requestExecutionGraphInfo(jobId, TIMEOUT)
                                        .get()
                                        .getArchivedExecutionGraph()
                                        .getState();
                        return status == JobStatus.SUSPENDED;
                    },
                    5L);
        } finally {
            // Unblock the termination of the second job
            jobManagerRunnerFactory.unblockTermination();
            terminationFuture.get();
        }
    }

    @Test
    public void testShutDownClusterShouldCompleteShutDownFuture() throws Exception {
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        JobMasterServiceLeadershipRunnerFactory.INSTANCE);
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        dispatcherGateway.shutDownCluster().get();

        dispatcher.getShutDownFuture().get();
    }

    @Test
    public void testOnRemovedJobGraphDoesNotCleanUpHAFiles() throws Exception {
        final CompletableFuture<JobID> removeJobGraphFuture = new CompletableFuture<>();
        final CompletableFuture<JobID> releaseJobGraphFuture = new CompletableFuture<>();

        final TestingJobGraphStore testingJobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setGlobalCleanupFunction(
                                (jobId, executor) -> {
                                    removeJobGraphFuture.complete(jobId);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .setLocalCleanupFunction(
                                (jobId, executor) -> {
                                    releaseJobGraphFuture.complete(jobId);
                                    return FutureUtils.completedVoidFuture();
                                })
                        .build();
        testingJobGraphStore.start(null);

        dispatcher =
                createTestingDispatcherBuilder()
                        .setRecoveredJobs(Collections.singleton(jobGraph))
                        .setJobGraphWriter(testingJobGraphStore)
                        .build(rpcService);
        dispatcher.start();

        final CompletableFuture<Void> processFuture =
                dispatcher.onRemovedJobGraph(jobGraph.getJobID());

        processFuture.join();

        assertThat(releaseJobGraphFuture.get(), is(jobGraph.getJobID()));

        try {
            removeJobGraphFuture.get(10L, TimeUnit.MILLISECONDS);
            fail("onRemovedJobGraph should not remove the job from the JobGraphStore.");
        } catch (TimeoutException expected) {
        }
    }

    @Test
    public void testInitializationTimestampForwardedToJobManagerRunner() throws Exception {
        final BlockingQueue<Long> initializationTimestampQueue = new ArrayBlockingQueue<>(1);
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new InitializationTimestampCapturingJobManagerRunnerFactory(
                                initializationTimestampQueue));
        jobMasterLeaderElection.isLeader(UUID.randomUUID());
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        final long initializationTimestamp = initializationTimestampQueue.take();

        // ensure all statuses are set in the ExecutionGraph
        assertThat(initializationTimestamp, greaterThan(0L));
    }

    @Test
    public void testRequestMultipleJobDetails_returnsSuspendedJobs() throws Exception {
        final JobManagerRunnerFactory blockingJobMaster =
                new QueuedJobManagerRunnerFactory(
                        completedJobManagerRunnerWithJobStatus(JobStatus.SUSPENDED));

        dispatcher = createAndStartDispatcher(heartbeatServices, haServices, blockingJobMaster);
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        dispatcher.getJobTerminationFuture(jobId, TIMEOUT).get();

        assertOnlyContainsSingleJobWithState(
                JobStatus.SUSPENDED, dispatcherGateway.requestMultipleJobDetails(TIMEOUT).get());
    }

    @Test
    public void testRequestMultipleJobDetails_returnsRunningOverSuspendedJob() throws Exception {
        final JobManagerRunnerFactory blockingJobMaster =
                new QueuedJobManagerRunnerFactory(
                        completedJobManagerRunnerWithJobStatus(JobStatus.SUSPENDED),
                        runningJobManagerRunnerWithJobStatus(JobStatus.RUNNING));

        dispatcher = createAndStartDispatcher(heartbeatServices, haServices, blockingJobMaster);
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());

        // run first job, which completes with SUSPENDED
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        dispatcherGateway.requestJobResult(jobId, TIMEOUT).get();

        // run second job, which stays in RUNNING
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        assertOnlyContainsSingleJobWithState(
                JobStatus.RUNNING, dispatcherGateway.requestMultipleJobDetails(TIMEOUT).get());
    }

    @Test
    public void testRequestMultipleJobDetails_returnsFinishedOverSuspendedJob() throws Exception {
        final JobManagerRunnerFactory blockingJobMaster =
                new QueuedJobManagerRunnerFactory(
                        completedJobManagerRunnerWithJobStatus(JobStatus.SUSPENDED),
                        completedJobManagerRunnerWithJobStatus(JobStatus.FINISHED));

        dispatcher = createAndStartDispatcher(heartbeatServices, haServices, blockingJobMaster);
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());

        // run first job, which completes with SUSPENDED
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        dispatcherGateway.requestJobResult(jobId, TIMEOUT).get();

        // run second job, which completes with FINISHED
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        dispatcher.getJobTerminationFuture(jobId, TIMEOUT).get();

        assertOnlyContainsSingleJobWithState(
                JobStatus.FINISHED, dispatcherGateway.requestMultipleJobDetails(TIMEOUT).get());
    }

    @Test
    public void testRequestMultipleJobDetails_isSerializable() throws Exception {
        final JobManagerRunnerFactory blockingJobMaster =
                new QueuedJobManagerRunnerFactory(
                        completedJobManagerRunnerWithJobStatus(JobStatus.SUSPENDED));

        dispatcher = createAndStartDispatcher(heartbeatServices, haServices, blockingJobMaster);
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        dispatcher.getJobTerminationFuture(jobId, TIMEOUT).get();

        final MultipleJobsDetails multipleJobsDetails =
                dispatcherGateway.requestMultipleJobDetails(TIMEOUT).get();

        InstantiationUtil.serializeObject(multipleJobsDetails);
    }

    @Test
    public void testOverridingJobVertexParallelisms() throws Exception {
        JobVertex v1 = new JobVertex("v1");
        v1.setParallelism(1);
        JobVertex v2 = new JobVertex("v2");
        v2.setParallelism(2);
        JobVertex v3 = new JobVertex("v3");
        v3.setParallelism(3);
        jobGraph = new JobGraph(jobGraph.getJobID(), "job", v1, v2, v3);

        configuration.set(
                PipelineOptions.PARALLELISM_OVERRIDES,
                ImmutableMap.of(
                        v1.getID().toHexString(), "10",
                        // v2 is omitted
                        v3.getID().toHexString(), "42",
                        // unknown vertex added
                        new JobVertexID().toHexString(), "23"));

        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(jobId));
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        Assert.assertEquals(jobGraph.findVertexByID(v1.getID()).getParallelism(), 1);
        Assert.assertEquals(jobGraph.findVertexByID(v2.getID()).getParallelism(), 2);
        Assert.assertEquals(jobGraph.findVertexByID(v3.getID()).getParallelism(), 3);

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        Assert.assertEquals(jobGraph.findVertexByID(v1.getID()).getParallelism(), 10);
        Assert.assertEquals(jobGraph.findVertexByID(v2.getID()).getParallelism(), 2);
        Assert.assertEquals(jobGraph.findVertexByID(v3.getID()).getParallelism(), 42);
    }

    private JobManagerRunner runningJobManagerRunnerWithJobStatus(
            final JobStatus currentJobStatus) {
        Preconditions.checkArgument(!currentJobStatus.isTerminalState());

        return TestingJobManagerRunner.newBuilder()
                .setJobId(jobId)
                .setJobDetailsFunction(
                        () ->
                                JobDetails.createDetailsForJob(
                                        new ArchivedExecutionGraphBuilder()
                                                .setJobID(jobId)
                                                .setState(currentJobStatus)
                                                .build()))
                .build();
    }

    private JobManagerRunner completedJobManagerRunnerWithJobStatus(
            final JobStatus finalJobStatus) {
        Preconditions.checkArgument(finalJobStatus.isTerminalState());

        return TestingJobManagerRunner.newBuilder()
                .setJobId(jobId)
                .setResultFuture(
                        CompletableFuture.completedFuture(
                                JobManagerRunnerResult.forSuccess(
                                        new ExecutionGraphInfo(
                                                new ArchivedExecutionGraphBuilder()
                                                        .setJobID(jobId)
                                                        .setState(finalJobStatus)
                                                        .build()))))
                .build();
    }

    private static void assertOnlyContainsSingleJobWithState(
            final JobStatus expectedJobStatus, final MultipleJobsDetails multipleJobsDetails) {
        final Collection<JobDetails> finishedJobDetails = multipleJobsDetails.getJobs();
        assertEquals(1, finishedJobDetails.size());
        assertEquals(expectedJobStatus, finishedJobDetails.iterator().next().getStatus());
    }

    @Test
    public void testOnlyRecoveredJobsAreRetainedInTheBlobServer() throws Exception {
        final JobID jobId1 = new JobID();
        final JobID jobId2 = new JobID();
        final byte[] fileContent = {1, 2, 3, 4};
        final BlobServer blobServer = getBlobServer();
        final PermanentBlobKey blobKey1 = blobServer.putPermanent(jobId1, fileContent);
        final PermanentBlobKey blobKey2 = blobServer.putPermanent(jobId2, fileContent);

        dispatcher =
                createTestingDispatcherBuilder()
                        .setRecoveredJobs(Collections.singleton(new JobGraph(jobId1, "foobar")))
                        .build(rpcService);

        Assertions.assertThat(blobServer.getFile(jobId1, blobKey1)).hasBinaryContent(fileContent);
        Assertions.assertThatThrownBy(() -> blobServer.getFile(jobId2, blobKey2))
                .isInstanceOf(NoSuchFileException.class);
    }

    @Test
    public void testRetrieveJobResultAfterSubmissionOfFailedJob() throws Exception {
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(jobId));
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        final JobID failedJobId = new JobID();
        final String failedJobName = "test";
        final CompletableFuture<Acknowledge> submitFuture =
                dispatcherGateway.submitFailedJob(
                        failedJobId, failedJobName, new RuntimeException("Test exception."));
        submitFuture.get();
        final ArchivedExecutionGraph archivedExecutionGraph =
                dispatcherGateway.requestJob(failedJobId, TIMEOUT).get();
        Assertions.assertThat(archivedExecutionGraph.getJobID()).isEqualTo(failedJobId);
        Assertions.assertThat(archivedExecutionGraph.getJobName()).isEqualTo(failedJobName);
        Assertions.assertThat(archivedExecutionGraph.getState()).isEqualTo(JobStatus.FAILED);
        Assertions.assertThat(archivedExecutionGraph.getFailureInfo())
                .isNotNull()
                .extracting(ErrorInfo::getException)
                .extracting(e -> e.deserializeError(Thread.currentThread().getContextClassLoader()))
                .satisfies(
                        exception ->
                                Assertions.assertThat(exception)
                                        .isInstanceOf(RuntimeException.class)
                                        .hasMessage("Test exception."));
    }

    @Test
    public void testInvalidResourceRequirementsUpdate() throws Exception {
        // the adaptive scheduler isn't strictly required, but it simplifies testing
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);

        final AtomicReference<CompletableFuture<Void>> jobGraphPersistedFutureRef =
                new AtomicReference<>();
        final TestingJobGraphStore jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setPutJobGraphConsumer(
                                jobGraph ->
                                        Optional.ofNullable(jobGraphPersistedFutureRef.get())
                                                .map(f -> f.complete(null)))
                        .build();
        haServices.setJobGraphStore(jobGraphStore);
        jobGraphStore.start(null);

        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        JobMasterServiceLeadershipRunnerFactory.INSTANCE);
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        // We can try updating the JRR once the scheduler has been started.
        awaitStatus(dispatcherGateway, jobId, JobStatus.CREATED);

        final CompletableFuture<Void> jobGraphPersistedFuture = new CompletableFuture<>();
        jobGraphPersistedFutureRef.set(jobGraphPersistedFuture);
        assertThatFuture(
                        dispatcherGateway.updateJobResourceRequirements(
                                jobId, JobResourceRequirements.empty()))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(RestHandlerException.class);

        // verify that validation error prevents the requirement from being persisted and applied
        assertThatFuture(jobGraphPersistedFuture).willNotCompleteWithin(Duration.ofMillis(5));
        assertThatFuture(dispatcherGateway.requestJobResourceRequirements(jobId))
                .eventuallySucceeds()
                .isNotEqualTo(JobResourceRequirements.empty());
    }

    @Test
    public void testPersistErrorHandling() throws Exception {
        // the adaptive scheduler isn't strictly required, but it simplifies testing
        configuration.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);

        final TestingJobGraphStore jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setPutJobResourceRequirementsConsumer(
                                (i1, i2) -> {
                                    throw new RuntimeException("artificial persist failure");
                                })
                        .build();
        haServices.setJobGraphStore(jobGraphStore);
        jobGraphStore.start(null);

        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        JobMasterServiceLeadershipRunnerFactory.INSTANCE);
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        // We can try updating the JRR once the scheduler has been started.
        awaitStatus(dispatcherGateway, jobId, JobStatus.CREATED);

        final JobVertex vertex = jobGraph.getVertices().iterator().next();

        final JobResourceRequirements attemptedNewRequirements =
                JobResourceRequirements.newBuilder()
                        .setParallelismForJobVertex(vertex.getID(), 1, 32)
                        .build();

        assertThatFuture(
                        dispatcherGateway.updateJobResourceRequirements(
                                jobId, attemptedNewRequirements))
                .eventuallyFailsWith(ExecutionException.class)
                .havingCause()
                .isInstanceOf(RestHandlerException.class)
                .satisfies(
                        e ->
                                Assertions.assertThat(
                                                ((RestHandlerException) e).getHttpResponseStatus())
                                        .isSameAs(HttpResponseStatus.INTERNAL_SERVER_ERROR));

        // verify that persist errors prevents the requirement from being applied
        assertThatFuture(dispatcherGateway.requestJobResourceRequirements(jobId))
                .eventuallySucceeds()
                .isNotEqualTo(attemptedNewRequirements);
    }

    @Test
    public void testJobResourceRequirementsCanBeOnlyUpdatedOnInitializedJobMasters()
            throws Exception {
        final JobManagerRunnerWithBlockingJobMasterFactory blockingJobMaster =
                new JobManagerRunnerWithBlockingJobMasterFactory(this::withRequestJobResponse);
        dispatcher = createAndStartDispatcher(heartbeatServices, haServices, blockingJobMaster);
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());

        assertThatFuture(
                        dispatcherGateway.updateJobResourceRequirements(
                                jobId, JobResourceRequirements.empty()))
                .eventuallyFailsWith(ExecutionException.class)
                .withCauseInstanceOf(FlinkJobNotFoundException.class);

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        blockingJobMaster.waitForBlockingInit();

        try {
            assertThatFuture(
                            dispatcherGateway.updateJobResourceRequirements(
                                    jobId, JobResourceRequirements.empty()))
                    .eventuallyFailsWith(ExecutionException.class)
                    .withCauseInstanceOf(UnavailableDispatcherOperationException.class);
        } finally {
            // Unblocking the job master in the "finally block" prevents getting
            // stuck during the RPC system tear down in case of test failure.
            blockingJobMaster.unblockJobMasterInitialization();
        }

        // We can update the JRR once the job transitions to RUNNING.
        awaitStatus(dispatcherGateway, jobId, JobStatus.RUNNING);
        assertThatFuture(
                        dispatcherGateway.updateJobResourceRequirements(
                                jobId, getJobRequirements()))
                .eventuallySucceeds();
    }

    @Test
    public void testJobResourceRequirementsAreGuardedAgainstConcurrentModification()
            throws Exception {
        final CompletableFuture<Acknowledge> blockedUpdatesToJobMasterFuture =
                new CompletableFuture<>();
        final JobManagerRunnerWithBlockingJobMasterFactory blockingJobMaster =
                new JobManagerRunnerWithBlockingJobMasterFactory(
                        builder ->
                                withRequestJobResponse(builder)
                                        .setUpdateJobResourceRequirementsFunction(
                                                jobResourceRequirements ->
                                                        blockedUpdatesToJobMasterFuture));
        dispatcher = createAndStartDispatcher(heartbeatServices, haServices, blockingJobMaster);
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        // We intentionally perform the test on two jobs to make sure the
        // concurrent modification is only prevented on the per-job level.
        final JobGraph firstJobGraph = InstantiationUtil.clone(jobGraph);
        firstJobGraph.setJobID(JobID.generate());
        final JobGraph secondJobGraph = InstantiationUtil.clone(jobGraph);
        secondJobGraph.setJobID(JobID.generate());

        final CompletableFuture<?> firstPendingUpdateFuture =
                testConcurrentModificationIsPrevented(
                        dispatcherGateway, blockingJobMaster, firstJobGraph);
        final CompletableFuture<?> secondPendingUpdateFuture =
                testConcurrentModificationIsPrevented(
                        dispatcherGateway, blockingJobMaster, secondJobGraph);

        Assertions.assertThat(firstPendingUpdateFuture).isNotCompleted();
        Assertions.assertThat(secondPendingUpdateFuture).isNotCompleted();
        blockedUpdatesToJobMasterFuture.complete(Acknowledge.get());
        assertThatFuture(firstPendingUpdateFuture).eventuallySucceeds();
        assertThatFuture(secondPendingUpdateFuture).eventuallySucceeds();
    }

    private CompletableFuture<?> testConcurrentModificationIsPrevented(
            DispatcherGateway dispatcherGateway,
            JobManagerRunnerWithBlockingJobMasterFactory blockingJobMaster,
            JobGraph jobGraph)
            throws Exception {
        final TestingLeaderElection jobMasterLeaderElection = new TestingLeaderElection();
        haServices.setJobMasterLeaderElection(jobGraph.getJobID(), jobMasterLeaderElection);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());

        assertThatFuture(dispatcherGateway.submitJob(jobGraph, TIMEOUT)).eventuallySucceeds();
        blockingJobMaster.unblockJobMasterInitialization();
        awaitStatus(dispatcherGateway, jobGraph.getJobID(), JobStatus.RUNNING);

        final CompletableFuture<?> pendingUpdateFuture =
                dispatcherGateway.updateJobResourceRequirements(
                        jobGraph.getJobID(), getJobRequirements());

        assertThatFuture(
                        dispatcherGateway.updateJobResourceRequirements(
                                jobGraph.getJobID(), getJobRequirements()))
                .eventuallyFailsWith(ExecutionException.class)
                .havingCause()
                .isInstanceOf(RestHandlerException.class)
                .satisfies(
                        e ->
                                Assertions.assertThat(
                                                ((RestHandlerException) e).getHttpResponseStatus())
                                        .isSameAs(HttpResponseStatus.CONFLICT));
        assertThatFuture(pendingUpdateFuture).isNotCompleted();

        return pendingUpdateFuture;
    }

    @Test
    public void testJobResourceRequirementsCanBeUpdatedSequentially() throws Exception {
        final JobManagerRunnerWithBlockingJobMasterFactory blockingJobMaster =
                new JobManagerRunnerWithBlockingJobMasterFactory(
                        builder ->
                                withRequestJobResponse(builder)
                                        .setUpdateJobResourceRequirementsFunction(
                                                jobResourceRequirements ->
                                                        CompletableFuture.completedFuture(
                                                                Acknowledge.get())));
        dispatcher = createAndStartDispatcher(heartbeatServices, haServices, blockingJobMaster);
        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        jobMasterLeaderElection.isLeader(UUID.randomUUID());

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        blockingJobMaster.waitForBlockingInit();
        blockingJobMaster.unblockJobMasterInitialization();

        // We can update the JRR once the job transitions to RUNNING.
        awaitStatus(dispatcherGateway, jobId, JobStatus.RUNNING);

        for (int x = 0; x < 2; x++) {
            assertThatFuture(
                            dispatcherGateway.updateJobResourceRequirements(
                                    this.jobGraph.getJobID(), getJobRequirements()))
                    .eventuallySucceeds();
        }
    }

    private JobResourceRequirements getJobRequirements() {
        JobResourceRequirements.Builder builder = JobResourceRequirements.newBuilder();

        for (JobVertex vertex : jobGraph.getVertices()) {
            builder.setParallelismForJobVertex(vertex.getID(), 1, vertex.getParallelism());
        }
        return builder.build();
    }

    private TestingJobMasterGatewayBuilder withRequestJobResponse(
            TestingJobMasterGatewayBuilder builder) {
        return builder.setRequestJobSupplier(
                () ->
                        CompletableFuture.completedFuture(
                                new ExecutionGraphInfo(
                                        ArchivedExecutionGraph
                                                .createSparseArchivedExecutionGraphWithJobVertices(
                                                        jobGraph.getJobID(),
                                                        jobGraph.getName(),
                                                        JobStatus.RUNNING,
                                                        null,
                                                        null,
                                                        System.currentTimeMillis(),
                                                        jobGraph.getVertices(),
                                                        SchedulerBase.computeVertexParallelismStore(
                                                                jobGraph)))));
    }

    private static class CancellableJobManagerRunnerWithInitializedJobFactory
            implements JobManagerRunnerFactory {

        private final JobID expectedJobId;

        private final AtomicReference<JobStatus> jobStatus =
                new AtomicReference(JobStatus.INITIALIZING);
        private final CompletableFuture<Void> cancellationFuture = new CompletableFuture<>();

        private CancellableJobManagerRunnerWithInitializedJobFactory(JobID expectedJobId) {
            this.expectedJobId = expectedJobId;
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnricher,
                long initializationTimestamp)
                throws Exception {
            assertEquals(expectedJobId, jobGraph.getJobID());
            final JobMasterGateway jobMasterGateway =
                    new TestingJobMasterGatewayBuilder()
                            .setRequestJobSupplier(
                                    () -> {
                                        final ExecutionGraphInfo executionGraphInfo =
                                                new ExecutionGraphInfo(
                                                        new ArchivedExecutionGraphBuilder()
                                                                .setState(jobStatus.get())
                                                                .build());
                                        return CompletableFuture.completedFuture(
                                                executionGraphInfo);
                                    })
                            .setCancelFunction(
                                    () -> {
                                        jobStatus.set(JobStatus.CANCELLING);
                                        return cancellationFuture.thenApply(
                                                ignored -> {
                                                    jobStatus.set(JobStatus.CANCELED);
                                                    return Acknowledge.get();
                                                });
                                    })
                            .build();

            final JobMasterServiceFactory jobMasterServiceFactory =
                    new TestingJobMasterServiceFactory(
                            onCompletionActions -> {
                                final TestingJobMasterService jobMasterService =
                                        new TestingJobMasterService(jobMasterGateway);
                                cancellationFuture.thenRun(
                                        () ->
                                                onCompletionActions.jobReachedGloballyTerminalState(
                                                        new ExecutionGraphInfo(
                                                                new ArchivedExecutionGraphBuilder()
                                                                        .setJobID(
                                                                                jobGraph.getJobID())
                                                                        .setState(
                                                                                JobStatus.CANCELED)
                                                                        .build())));
                                return CompletableFuture.completedFuture(jobMasterService);
                            });

            return new JobMasterServiceLeadershipRunner(
                    new DefaultJobMasterServiceProcessFactory(
                            jobGraph.getJobID(),
                            jobGraph.getName(),
                            jobGraph.getCheckpointingSettings(),
                            initializationTimestamp,
                            jobMasterServiceFactory),
                    highAvailabilityServices.getJobManagerLeaderElection(jobGraph.getJobID()),
                    highAvailabilityServices.getJobResultStore(),
                    jobManagerServices
                            .getLibraryCacheManager()
                            .registerClassLoaderLease(jobGraph.getJobID()),
                    fatalErrorHandler);
        }

        public void unblockCancellation() {
            cancellationFuture.complete(null);
        }
    }

    private static class JobManagerRunnerWithBlockingJobMasterFactory
            implements JobManagerRunnerFactory {

        private final JobMasterGateway jobMasterGateway;
        private final AtomicReference<JobStatus> currentJobStatus;
        private final BlockingQueue<CompletableFuture<JobMasterService>> jobMasterServiceFutures;
        private final OneShotLatch initLatch;

        private JobManagerRunnerWithBlockingJobMasterFactory() {
            this(Function.identity());
        }

        private JobManagerRunnerWithBlockingJobMasterFactory(
                Function<TestingJobMasterGatewayBuilder, TestingJobMasterGatewayBuilder>
                        modifyGatewayBuilder) {
            this.currentJobStatus = new AtomicReference<>(JobStatus.INITIALIZING);
            this.jobMasterServiceFutures = new ArrayBlockingQueue<>(2);
            this.initLatch = new OneShotLatch();
            final TestingJobMasterGatewayBuilder builder =
                    new TestingJobMasterGatewayBuilder()
                            .setRequestJobSupplier(
                                    () ->
                                            CompletableFuture.completedFuture(
                                                    new ExecutionGraphInfo(
                                                            new ArchivedExecutionGraphBuilder()
                                                                    .setState(
                                                                            currentJobStatus.get())
                                                                    .build())));
            this.jobMasterGateway = modifyGatewayBuilder.apply(builder).build();
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp)
                throws Exception {

            return new JobMasterServiceLeadershipRunner(
                    new DefaultJobMasterServiceProcessFactory(
                            jobGraph.getJobID(),
                            jobGraph.getName(),
                            jobGraph.getCheckpointingSettings(),
                            initializationTimestamp,
                            new TestingJobMasterServiceFactory(
                                    ignored -> {
                                        initLatch.trigger();
                                        final CompletableFuture<JobMasterService> result =
                                                new CompletableFuture<>();
                                        Preconditions.checkState(
                                                jobMasterServiceFutures.offer(result));
                                        return result;
                                    })),
                    highAvailabilityServices.getJobManagerLeaderElection(jobGraph.getJobID()),
                    highAvailabilityServices.getJobResultStore(),
                    jobManagerServices
                            .getLibraryCacheManager()
                            .registerClassLoaderLease(jobGraph.getJobID()),
                    fatalErrorHandler);
        }

        public void waitForBlockingInit() throws InterruptedException {
            initLatch.await();
        }

        public void unblockJobMasterInitialization() throws InterruptedException {
            final CompletableFuture<JobMasterService> future = jobMasterServiceFutures.take();
            future.complete(new TestingJobMasterService(jobMasterGateway));
            currentJobStatus.set(JobStatus.RUNNING);
        }
    }

    private static final class JobManagerRunnerWithBlockingTerminationFactory
            implements JobManagerRunnerFactory {

        private final JobID jobIdToBlock;
        private final CompletableFuture<Void> future;

        public JobManagerRunnerWithBlockingTerminationFactory(JobID jobIdToBlock) {
            this.jobIdToBlock = jobIdToBlock;
            this.future = new CompletableFuture<>();
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp)
                throws Exception {
            return new BlockingTerminationJobManagerService(
                    jobIdToBlock,
                    future,
                    new DefaultJobMasterServiceProcessFactory(
                            jobGraph.getJobID(),
                            jobGraph.getName(),
                            jobGraph.getCheckpointingSettings(),
                            initializationTimestamp,
                            new TestingJobMasterServiceFactory()),
                    highAvailabilityServices.getJobManagerLeaderElection(jobGraph.getJobID()),
                    highAvailabilityServices.getJobResultStore(),
                    jobManagerServices
                            .getLibraryCacheManager()
                            .registerClassLoaderLease(jobGraph.getJobID()),
                    fatalErrorHandler);
        }

        public void unblockTermination() {
            future.complete(null);
        }
    }

    private static final class BlockingTerminationJobManagerService
            extends JobMasterServiceLeadershipRunner {

        private final JobID jobIdToBlock;
        private final CompletableFuture<Void> future;

        public BlockingTerminationJobManagerService(
                JobID jobIdToBlock,
                CompletableFuture<Void> future,
                JobMasterServiceProcessFactory jobMasterServiceProcessFactory,
                LeaderElection leaderElection,
                JobResultStore jobResultStore,
                LibraryCacheManager.ClassLoaderLease classLoaderLease,
                FatalErrorHandler fatalErrorHandler) {
            super(
                    jobMasterServiceProcessFactory,
                    leaderElection,
                    jobResultStore,
                    classLoaderLease,
                    fatalErrorHandler);
            this.future = future;
            this.jobIdToBlock = jobIdToBlock;
        }

        @Override
        public CompletableFuture<Void> closeAsync() {
            if (jobIdToBlock.equals(getJobID())) {
                return future.whenComplete((r, t) -> super.closeAsync());
            }
            return super.closeAsync();
        }
    }

    private static final class InitializationTimestampCapturingJobManagerRunnerFactory
            implements JobManagerRunnerFactory {
        private final BlockingQueue<Long> initializationTimestampQueue;

        private InitializationTimestampCapturingJobManagerRunnerFactory(
                BlockingQueue<Long> initializationTimestampQueue) {
            this.initializationTimestampQueue = initializationTimestampQueue;
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp) {
            initializationTimestampQueue.offer(initializationTimestamp);
            return TestingJobManagerRunner.newBuilder().setJobId(jobGraph.getJobID()).build();
        }
    }

    private static final class TestingJobMasterGatewayJobManagerRunnerFactory
            extends TestingJobMasterServiceLeadershipRunnerFactory {
        private final TestingJobMasterGateway testingJobMasterGateway;

        private TestingJobMasterGatewayJobManagerRunnerFactory(
                TestingJobMasterGateway testingJobMasterGateway) {
            this.testingJobMasterGateway = testingJobMasterGateway;
        }

        @Override
        public TestingJobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp)
                throws Exception {
            TestingJobManagerRunner runner =
                    super.createJobManagerRunner(
                            jobGraph,
                            configuration,
                            rpcService,
                            highAvailabilityServices,
                            heartbeatServices,
                            jobManagerServices,
                            jobManagerJobMetricGroupFactory,
                            fatalErrorHandler,
                            failureEnrichers,
                            initializationTimestamp);
            runner.completeJobMasterGatewayFuture(testingJobMasterGateway);
            return runner;
        }
    }

    private static final class ExpectedJobIdJobManagerRunnerFactory
            implements JobManagerRunnerFactory {

        private final JobID expectedJobId;

        private ExpectedJobIdJobManagerRunnerFactory(JobID expectedJobId) {
            this.expectedJobId = expectedJobId;
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerSharedServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp)
                throws Exception {
            assertEquals(expectedJobId, jobGraph.getJobID());

            return JobMasterServiceLeadershipRunnerFactory.INSTANCE.createJobManagerRunner(
                    jobGraph,
                    configuration,
                    rpcService,
                    highAvailabilityServices,
                    heartbeatServices,
                    jobManagerSharedServices,
                    jobManagerJobMetricGroupFactory,
                    fatalErrorHandler,
                    Collections.emptySet(),
                    initializationTimestamp);
        }
    }

    private static class QueuedJobManagerRunnerFactory implements JobManagerRunnerFactory {

        private final Queue<JobManagerRunner> resultFutureQueue;

        private QueuedJobManagerRunnerFactory(JobManagerRunner... resultFutureQueue) {
            this.resultFutureQueue = new ArrayDeque<>(Arrays.asList(resultFutureQueue));
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp)
                throws Exception {
            return resultFutureQueue.remove();
        }
    }

    private static class FinishingJobManagerRunnerFactory implements JobManagerRunnerFactory {

        private final CompletableFuture<JobManagerRunnerResult> resultFuture;
        private final Runnable onClose;

        private FinishingJobManagerRunnerFactory(
                CompletableFuture<JobManagerRunnerResult> resultFuture, Runnable onClose) {
            this.resultFuture = resultFuture;
            this.onClose = onClose;
        }

        @Override
        public JobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                Collection<FailureEnricher> failureEnrichers,
                long initializationTimestamp)
                throws Exception {
            final TestingJobManagerRunner runner =
                    TestingJobManagerRunner.newBuilder()
                            .setJobId(jobGraph.getJobID())
                            .setResultFuture(resultFuture)
                            .build();
            runner.getTerminationFuture().thenRun(onClose::run);
            return runner;
        }
    }
}
