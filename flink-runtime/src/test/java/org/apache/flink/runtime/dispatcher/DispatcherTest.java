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
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.standalone.StandaloneRunningJobsRegistry;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
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
import org.apache.flink.runtime.jobmaster.factories.JobMasterServiceProcessFactory;
import org.apache.flink.runtime.jobmaster.factories.TestingJobMasterServiceFactory;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.messages.webmonitor.JobDetails;
import org.apache.flink.runtime.messages.webmonitor.MultipleJobsDetails;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.scheduler.ExecutionGraphInfo;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageCoordinatorView;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TimeUtils;
import org.apache.flink.util.function.ThrowingRunnable;

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
import java.nio.file.Path;
import java.nio.file.Paths;
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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.core.testutils.FlinkMatchers.containsMessage;
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

    private static final String CLEANUP_JOB_GRAPH_REMOVE = "job-graph-remove";
    private static final String CLEANUP_JOB_GRAPH_RELEASE = "job-graph-release";
    private static final String CLEANUP_JOB_MANAGER_RUNNER = "job-manager-runner";
    private static final String CLEANUP_HA_SERVICES = "ha-services";
    private static final String CLEANUP_RUNNING_JOBS_REGISTRY = "running-jobs-registry";

    private JobGraph jobGraph;

    private JobID jobId;

    private TestingLeaderElectionService jobMasterLeaderElectionService;

    private CountDownLatch createdJobManagerRunnerLatch;

    /** Instance under test. */
    private TestingDispatcher dispatcher;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        jobGraph = JobGraphTestUtils.singleNoOpJobGraph();
        jobId = jobGraph.getJobID();
        jobMasterLeaderElectionService = new TestingLeaderElectionService();
        haServices.setJobMasterLeaderElectionService(jobId, jobMasterLeaderElectionService);
        createdJobManagerRunnerLatch = new CountDownLatch(2);
    }

    @Nonnull
    private TestingDispatcher createAndStartDispatcher(
            HeartbeatServices heartbeatServices,
            TestingHighAvailabilityServices haServices,
            JobManagerRunnerFactory jobManagerRunnerFactory)
            throws Exception {
        final TestingDispatcher dispatcher =
                new TestingDispatcherBuilder()
                        .setHaServices(haServices)
                        .setHeartbeatServices(heartbeatServices)
                        .setJobManagerRunnerFactory(jobManagerRunnerFactory)
                        .setJobGraphWriter(haServices.getJobGraphStore())
                        .build();
        dispatcher.start();
        return dispatcher;
    }

    @After
    public void tearDown() throws Exception {
        if (dispatcher != null) {
            RpcUtils.terminateRpcEndpoint(dispatcher, TIMEOUT);
        }
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
                        new ExpectedJobIdJobManagerRunnerFactory(
                                jobId, createdJobManagerRunnerLatch));
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

        jobMasterLeaderElectionService.getStartFuture().get();

        assertTrue(
                "jobManagerRunner was not started",
                jobMasterLeaderElectionService.getStartFuture().isDone());
    }

    @Test
    public void testDuplicateJobSubmissionWithGloballyTerminatedJobId() throws Exception {
        haServices.getRunningJobsRegistry().setJobFinished(jobGraph.getJobID());
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(
                                jobId, createdJobManagerRunnerLatch));
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
                new TestingDispatcherBuilder()
                        .setJobManagerRunnerFactory(
                                new ExpectedJobIdJobManagerRunnerFactory(
                                        jobId, createdJobManagerRunnerLatch))
                        .setInitialJobGraphs(Collections.singleton(jobGraph))
                        .build();
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
                        new ExpectedJobIdJobManagerRunnerFactory(
                                jobId, createdJobManagerRunnerLatch));

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
        jobMasterLeaderElectionService.isLeader(UUID.randomUUID());
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
                    .triggerSavepoint(jobId, "file:///tmp/savepoint", false, TIMEOUT)
                    .get();
            fail("Previous statement should have failed");
        } catch (ExecutionException t) {
            assertTrue(t.getCause() instanceof UnavailableDispatcherOperationException);
        }
    }

    @Test
    public void testCancellationDuringInitialization() throws Exception {
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(
                                jobId, createdJobManagerRunnerLatch));
        jobMasterLeaderElectionService.isLeader(UUID.randomUUID());
        DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

        // create a job graph of a job that blocks forever
        Tuple2<JobGraph, BlockingJobVertex> blockingJobGraph = getBlockingJobGraphAndVertex();
        JobID jobID = blockingJobGraph.f0.getJobID();

        dispatcherGateway.submitJob(blockingJobGraph.f0, TIMEOUT).get();

        assertThat(
                dispatcherGateway.requestJobStatus(jobID, TIMEOUT).get(),
                is(JobStatus.INITIALIZING));

        // submission has succeeded, now cancel the job
        CompletableFuture<Acknowledge> cancellationFuture =
                dispatcherGateway.cancelJob(jobID, TIMEOUT);
        assertThat(
                dispatcherGateway.requestJobStatus(jobID, TIMEOUT).get(), is(JobStatus.CANCELLING));
        assertThat(cancellationFuture.isDone(), is(false));
        // unblock
        blockingJobGraph.f1.unblock();
        // wait until cancelled
        cancellationFuture.get();
        assertThat(
                dispatcherGateway.requestJobResult(jobID, TIMEOUT).get().getApplicationStatus(),
                is(ApplicationStatus.CANCELED));
    }

    @Test
    public void testNoHistoryServerArchiveCreatedForSuspendedJob() throws Exception {
        final CompletableFuture<Void> archiveAttemptFuture = new CompletableFuture<>();
        final CompletableFuture<JobManagerRunnerResult> jobTerminationFuture =
                new CompletableFuture<>();
        dispatcher =
                new TestingDispatcherBuilder()
                        .setJobManagerRunnerFactory(
                                new FinishingJobManagerRunnerFactory(
                                        jobTerminationFuture, () -> {}))
                        .setHistoryServerArchivist(
                                executionGraphInfo -> {
                                    archiveAttemptFuture.complete(null);
                                    return CompletableFuture.completedFuture(null);
                                })
                        .build();
        dispatcher.start();
        jobMasterLeaderElectionService.isLeader(UUID.randomUUID());
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
        final TestingJobManagerRunnerFactory testingJobManagerRunnerFactory =
                new TestingJobManagerRunnerFactory();

        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices, haServices, testingJobManagerRunnerFactory);
        jobMasterLeaderElectionService.isLeader(UUID.randomUUID());
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
                                ArchivedExecutionGraph.createFromInitializingJob(
                                        jobId,
                                        jobGraph.getName(),
                                        JobStatus.FAILED,
                                        testFailure,
                                        jobGraph.getCheckpointingSettings(),
                                        1L)),
                        testFailure));

        // wait till job has failed
        dispatcherGateway.requestJobResult(jobId, TIMEOUT).get();

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
        assertThat(throwable, is(testFailure));
    }

    /** Test that {@link JobResult} is cached when the job finishes. */
    @Test
    public void testCacheJobExecutionResult() throws Exception {
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(
                                jobId, createdJobManagerRunnerLatch));

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
        assertThat(
                dispatcherGateway.requestExecutionGraphInfo(failedJobId, TIMEOUT).get(),
                equalTo(failedExecutionGraphInfo));
    }

    @Test
    public void testThrowExceptionIfJobExecutionResultNotFound() throws Exception {
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new ExpectedJobIdJobManagerRunnerFactory(
                                jobId, createdJobManagerRunnerLatch));

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
                        new ExpectedJobIdJobManagerRunnerFactory(
                                jobId, createdJobManagerRunnerLatch));

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

    /**
     * Tests that the {@link Dispatcher} fails fatally if the recovered jobs cannot be started. See
     * FLINK-9097.
     */
    @Test
    public void testFatalErrorIfRecoveredJobsCannotBeStarted() throws Exception {
        final FlinkException testException = new FlinkException("Test exception");
        jobMasterLeaderElectionService.isLeader(UUID.randomUUID());

        final TestingJobManagerRunnerFactory jobManagerRunnerFactory =
                new TestingJobManagerRunnerFactory();
        dispatcher =
                new TestingDispatcherBuilder()
                        .setJobManagerRunnerFactory(jobManagerRunnerFactory)
                        .setInitialJobGraphs(
                                Collections.singleton(JobGraphTestUtils.emptyJobGraph()))
                        .build();

        dispatcher.start();

        final TestingFatalErrorHandler fatalErrorHandler =
                testingFatalErrorHandlerResource.getFatalErrorHandler();

        final TestingJobManagerRunner testingJobManagerRunner =
                jobManagerRunnerFactory.takeCreatedJobManagerRunner();

        // Let the initialization of the JobManagerRunner fail
        testingJobManagerRunner.completeResultFuture(
                JobManagerRunnerResult.forInitializationFailure(
                        new ExecutionGraphInfo(
                                ArchivedExecutionGraph.createFromInitializingJob(
                                        jobId,
                                        jobGraph.getName(),
                                        JobStatus.FAILED,
                                        testException,
                                        jobGraph.getCheckpointingSettings(),
                                        1L)),
                        testException));

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

    /** Tests that a failing {@link JobManagerRunner} will be properly cleaned up. */
    @Test
    public void testFailingJobManagerRunnerCleanup() throws Exception {
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

        final BlockingQueue<String> cleanUpEvents = new LinkedBlockingQueue<>();

        // Track cleanup - ha-services
        final CompletableFuture<JobID> cleanupJobData = new CompletableFuture<>();
        haServices.setCleanupJobDataFuture(cleanupJobData);
        cleanupJobData.thenAccept(jobId -> cleanUpEvents.add(CLEANUP_HA_SERVICES));

        // Track cleanup - job-graph
        final TestingJobGraphStore jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setReleaseJobGraphConsumer(
                                jobId -> cleanUpEvents.add(CLEANUP_JOB_GRAPH_RELEASE))
                        .setRemoveJobGraphConsumer(
                                jobId -> cleanUpEvents.add(CLEANUP_JOB_GRAPH_REMOVE))
                        .build();
        jobGraphStore.start(null);
        haServices.setJobGraphStore(jobGraphStore);

        // Track cleanup - running jobs registry
        haServices.setRunningJobsRegistry(
                new StandaloneRunningJobsRegistry() {

                    @Override
                    public void clearJob(JobID jobID) {
                        super.clearJob(jobID);
                        cleanUpEvents.add(CLEANUP_RUNNING_JOBS_REGISTRY);
                    }
                });

        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices, haServices, blockingJobManagerRunnerFactory);

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);

        // submit and fail during job master runner construction
        queue.offer(Optional.of(testException));
        try {
            dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        } catch (Throwable expectedException) {
            assertThat(expectedException, containsCause(FlinkException.class));
            assertThat(expectedException, containsMessage(testException.getMessage()));
            // make sure we've cleaned up in correct order (including HA)
            assertThat(
                    new ArrayList<>(cleanUpEvents),
                    equalTo(
                            Arrays.asList(
                                    CLEANUP_JOB_GRAPH_REMOVE,
                                    CLEANUP_RUNNING_JOBS_REGISTRY,
                                    CLEANUP_HA_SERVICES)));
        }

        // don't fail this time
        queue.offer(Optional.empty());
        // submit job again
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        blockingJobManagerRunnerFactory.setJobStatus(JobStatus.RUNNING);

        // Ensure job is running
        awaitStatus(dispatcherGateway, jobId, JobStatus.RUNNING);
    }

    @Test
    public void testPersistedJobGraphWhenDispatcherIsShutDown() throws Exception {
        final TestingJobGraphStore submittedJobGraphStore =
                TestingJobGraphStore.newBuilder().build();
        submittedJobGraphStore.start(null);
        haServices.setJobGraphStore(submittedJobGraphStore);

        dispatcher =
                new TestingDispatcherBuilder().setJobGraphWriter(submittedJobGraphStore).build();

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
                        new ExpectedJobIdJobManagerRunnerFactory(
                                jobId, createdJobManagerRunnerLatch));

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
        haServices.setJobMasterLeaderElectionService(
                blockingId, new TestingLeaderElectionService());
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
                    Deadline.fromNow(TimeUtils.toDuration(TIMEOUT)),
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
                        .setRemoveJobGraphConsumer(removeJobGraphFuture::complete)
                        .setReleaseJobGraphConsumer(releaseJobGraphFuture::complete)
                        .build();
        testingJobGraphStore.start(null);

        dispatcher =
                new TestingDispatcherBuilder()
                        .setInitialJobGraphs(Collections.singleton(jobGraph))
                        .setJobGraphWriter(testingJobGraphStore)
                        .build();
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
        jobMasterLeaderElectionService.isLeader(UUID.randomUUID());
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
        jobMasterLeaderElectionService.isLeader(UUID.randomUUID());

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        dispatcherGateway.requestJobResult(jobId, TIMEOUT).get();

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
        jobMasterLeaderElectionService.isLeader(UUID.randomUUID());

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
        jobMasterLeaderElectionService.isLeader(UUID.randomUUID());

        // run first job, which completes with SUSPENDED
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        dispatcherGateway.requestJobResult(jobId, TIMEOUT).get();

        // run second job, which completes with FINISHED
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

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
        jobMasterLeaderElectionService.isLeader(UUID.randomUUID());

        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        dispatcherGateway.requestJobResult(jobId, TIMEOUT).get();

        final MultipleJobsDetails multipleJobsDetails =
                dispatcherGateway.requestMultipleJobDetails(TIMEOUT).get();

        InstantiationUtil.serializeObject(multipleJobsDetails);
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
    public void testJobDataAreCleanedUpInCorrectOrderOnFinishedJob() throws Exception {
        testJobDataAreCleanedUpInCorrectOrder(JobStatus.FINISHED);
    }

    @Test
    public void testJobDataAreCleanedUpInCorrectOrderOnFailedJob() throws Exception {
        testJobDataAreCleanedUpInCorrectOrder(JobStatus.FAILED);
    }

    private void testJobDataAreCleanedUpInCorrectOrder(JobStatus jobStatus) throws Exception {
        final BlockingQueue<String> cleanUpEvents = new LinkedBlockingQueue<>();

        // Track cleanup - ha-services
        final CompletableFuture<JobID> cleanupJobData = new CompletableFuture<>();
        haServices.setCleanupJobDataFuture(cleanupJobData);
        cleanupJobData.thenAccept(jobId -> cleanUpEvents.add(CLEANUP_HA_SERVICES));

        // Track cleanup - job-graph
        final TestingJobGraphStore jobGraphStore =
                TestingJobGraphStore.newBuilder()
                        .setReleaseJobGraphConsumer(
                                jobId -> cleanUpEvents.add(CLEANUP_JOB_GRAPH_RELEASE))
                        .setRemoveJobGraphConsumer(
                                jobId -> cleanUpEvents.add(CLEANUP_JOB_GRAPH_REMOVE))
                        .build();
        jobGraphStore.start(null);
        haServices.setJobGraphStore(jobGraphStore);

        // Track cleanup - running jobs registry
        haServices.setRunningJobsRegistry(
                new StandaloneRunningJobsRegistry() {

                    @Override
                    public void clearJob(JobID jobID) {
                        super.clearJob(jobID);
                        cleanUpEvents.add(CLEANUP_RUNNING_JOBS_REGISTRY);
                    }
                });

        final CompletableFuture<JobManagerRunnerResult> resultFuture = new CompletableFuture<>();
        dispatcher =
                createAndStartDispatcher(
                        heartbeatServices,
                        haServices,
                        new FinishingJobManagerRunnerFactory(
                                resultFuture, () -> cleanUpEvents.add(CLEANUP_JOB_MANAGER_RUNNER)));

        final DispatcherGateway dispatcherGateway =
                dispatcher.getSelfGateway(DispatcherGateway.class);
        dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
        resultFuture.complete(
                JobManagerRunnerResult.forSuccess(
                        new ExecutionGraphInfo(
                                new ArchivedExecutionGraphBuilder().setState(jobStatus).build())));

        // Wait for job to terminate.
        dispatcher.getJobTerminationFuture(jobId, TIMEOUT).get();

        assertThat(
                new ArrayList<>(cleanUpEvents),
                equalTo(
                        Arrays.asList(
                                CLEANUP_JOB_GRAPH_REMOVE,
                                CLEANUP_JOB_MANAGER_RUNNER,
                                CLEANUP_RUNNING_JOBS_REGISTRY,
                                CLEANUP_HA_SERVICES)));
    }

    private static class JobManagerRunnerWithBlockingJobMasterFactory
            implements JobManagerRunnerFactory {

        private final JobMasterGateway jobMasterGateway;
        private final AtomicReference<JobStatus> currentJobStatus;
        private final BlockingQueue<CompletableFuture<JobMasterService>> jobMasterServiceFutures;
        private final OneShotLatch initLatch;

        private JobManagerRunnerWithBlockingJobMasterFactory() {
            this.currentJobStatus = new AtomicReference<>(JobStatus.INITIALIZING);
            this.jobMasterServiceFutures = new ArrayBlockingQueue<>(2);
            this.initLatch = new OneShotLatch();
            this.jobMasterGateway =
                    new TestingJobMasterGatewayBuilder()
                            .setRequestJobSupplier(
                                    () ->
                                            CompletableFuture.completedFuture(
                                                    new ExecutionGraphInfo(
                                                            new ArchivedExecutionGraphBuilder()
                                                                    .setState(
                                                                            currentJobStatus.get())
                                                                    .build())))
                            .build();
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
                long initializationTimestamp)
                throws Exception {

            return new JobMasterServiceLeadershipRunner(
                    new DefaultJobMasterServiceProcessFactory(
                            jobGraph.getJobID(),
                            jobGraph.getName(),
                            jobGraph.getCheckpointingSettings(),
                            initializationTimestamp,
                            new TestingJobMasterServiceFactory(
                                    () -> {
                                        initLatch.trigger();
                                        final CompletableFuture<JobMasterService> result =
                                                new CompletableFuture<>();
                                        jobMasterServiceFutures.offer(result);
                                        return result;
                                    })),
                    highAvailabilityServices.getJobManagerLeaderElectionService(
                            jobGraph.getJobID()),
                    highAvailabilityServices.getRunningJobsRegistry(),
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
                    highAvailabilityServices.getJobManagerLeaderElectionService(
                            jobGraph.getJobID()),
                    highAvailabilityServices.getRunningJobsRegistry(),
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
                LeaderElectionService leaderElectionService,
                RunningJobsRegistry runningJobsRegistry,
                LibraryCacheManager.ClassLoaderLease classLoaderLease,
                FatalErrorHandler fatalErrorHandler) {
            super(
                    jobMasterServiceProcessFactory,
                    leaderElectionService,
                    runningJobsRegistry,
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

    private static final class BlockingJobManagerRunnerFactory
            extends TestingJobManagerRunnerFactory {

        @Nonnull private final ThrowingRunnable<Exception> jobManagerRunnerCreationLatch;
        private TestingJobManagerRunner testingRunner;

        BlockingJobManagerRunnerFactory(
                @Nonnull ThrowingRunnable<Exception> jobManagerRunnerCreationLatch) {
            this.jobManagerRunnerCreationLatch = jobManagerRunnerCreationLatch;
        }

        @Override
        public TestingJobManagerRunner createJobManagerRunner(
                JobGraph jobGraph,
                Configuration configuration,
                RpcService rpcService,
                HighAvailabilityServices highAvailabilityServices,
                HeartbeatServices heartbeatServices,
                JobManagerSharedServices jobManagerSharedServices,
                JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
                FatalErrorHandler fatalErrorHandler,
                long initializationTimestamp)
                throws Exception {
            jobManagerRunnerCreationLatch.run();

            this.testingRunner =
                    super.createJobManagerRunner(
                            jobGraph,
                            configuration,
                            rpcService,
                            highAvailabilityServices,
                            heartbeatServices,
                            jobManagerSharedServices,
                            jobManagerJobMetricGroupFactory,
                            fatalErrorHandler,
                            initializationTimestamp);

            TestingJobMasterGateway testingJobMasterGateway =
                    new TestingJobMasterGatewayBuilder()
                            .setRequestJobSupplier(
                                    () ->
                                            CompletableFuture.completedFuture(
                                                    new ExecutionGraphInfo(
                                                            ArchivedExecutionGraph
                                                                    .createFromInitializingJob(
                                                                            jobGraph.getJobID(),
                                                                            jobGraph.getName(),
                                                                            JobStatus.RUNNING,
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
                long initializationTimestamp) {
            initializationTimestampQueue.offer(initializationTimestamp);
            return TestingJobManagerRunner.newBuilder().setJobId(jobGraph.getJobID()).build();
        }
    }

    private Tuple2<JobGraph, BlockingJobVertex> getBlockingJobGraphAndVertex() {
        final BlockingJobVertex blockingJobVertex = new BlockingJobVertex("testVertex");
        blockingJobVertex.setInvokableClass(NoOpInvokable.class);
        // AdaptiveScheduler expects the parallelism to be set for each vertex
        blockingJobVertex.setParallelism(1);

        return Tuple2.of(
                JobGraphBuilder.newStreamingJobGraphBuilder()
                        .setJobId(jobId)
                        .addJobVertex(blockingJobVertex)
                        .build(),
                blockingJobVertex);
    }

    private static final class ExpectedJobIdJobManagerRunnerFactory
            implements JobManagerRunnerFactory {

        private final JobID expectedJobId;

        private final CountDownLatch createdJobManagerRunnerLatch;

        private ExpectedJobIdJobManagerRunnerFactory(
                JobID expectedJobId, CountDownLatch createdJobManagerRunnerLatch) {
            this.expectedJobId = expectedJobId;
            this.createdJobManagerRunnerLatch = createdJobManagerRunnerLatch;
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
                long initializationTimestamp)
                throws Exception {
            assertEquals(expectedJobId, jobGraph.getJobID());

            createdJobManagerRunnerLatch.countDown();

            return JobMasterServiceLeadershipRunnerFactory.INSTANCE.createJobManagerRunner(
                    jobGraph,
                    configuration,
                    rpcService,
                    highAvailabilityServices,
                    heartbeatServices,
                    jobManagerSharedServices,
                    jobManagerJobMetricGroupFactory,
                    fatalErrorHandler,
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

    private static class BlockingJobVertex extends JobVertex {
        private final OneShotLatch oneShotLatch = new OneShotLatch();

        private BlockingJobVertex(String name) {
            super(name);
        }

        @Override
        public void initializeOnMaster(ClassLoader loader) throws Exception {
            super.initializeOnMaster(loader);
            oneShotLatch.await();
        }

        public void unblock() {
            oneShotLatch.trigger();
        }
    }
}
