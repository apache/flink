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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ErrorInfo;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.messages.FlinkJobNotFoundException;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.InMemorySubmittedJobGraphStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for the {@link Dispatcher} component.
 */
public class DispatcherTest extends TestLogger {

	private static RpcService rpcService;

	private static final Time TIMEOUT = Time.seconds(10L);

	private static final JobID TEST_JOB_ID = new JobID();

	@Rule
	public TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public TestName name = new TestName();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private JobGraph jobGraph;

	private TestingFatalErrorHandler fatalErrorHandler;

	private FailableSubmittedJobGraphStore submittedJobGraphStore;

	private TestingLeaderElectionService dispatcherLeaderElectionService;

	private TestingLeaderElectionService jobMasterLeaderElectionService;

	private RunningJobsRegistry runningJobsRegistry;

	private CountDownLatch createdJobManagerRunnerLatch;

	private Configuration configuration;

	private BlobServer blobServer;

	/** Instance under test. */
	private TestingDispatcher dispatcher;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();
	}

	@AfterClass
	public static void teardownClass() throws Exception {
		if (rpcService != null) {
			RpcUtils.terminateRpcService(rpcService, TIMEOUT);

			rpcService = null;
		}
	}

	@Before
	public void setUp() throws Exception {
		final JobVertex testVertex = new JobVertex("testVertex");
		testVertex.setInvokableClass(NoOpInvokable.class);
		jobGraph = new JobGraph(TEST_JOB_ID, "testJob", testVertex);
		jobGraph.setAllowQueuedScheduling(true);

		fatalErrorHandler = new TestingFatalErrorHandler();
		final HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 10000L);
		submittedJobGraphStore = new FailableSubmittedJobGraphStore();

		dispatcherLeaderElectionService = new TestingLeaderElectionService();
		jobMasterLeaderElectionService = new TestingLeaderElectionService();

		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		haServices.setDispatcherLeaderElectionService(dispatcherLeaderElectionService);
		haServices.setSubmittedJobGraphStore(submittedJobGraphStore);
		haServices.setJobMasterLeaderElectionService(TEST_JOB_ID, jobMasterLeaderElectionService);
		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());
		haServices.setResourceManagerLeaderRetriever(new SettableLeaderRetrievalService());
		runningJobsRegistry = haServices.getRunningJobsRegistry();

		configuration = new Configuration();

		configuration.setString(
			BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		createdJobManagerRunnerLatch = new CountDownLatch(2);
		blobServer = new BlobServer(configuration, new VoidBlobStore());

		dispatcher = createDispatcher(heartbeatServices, haServices);

		dispatcher.start();
	}

	@Nonnull
	private TestingDispatcher createDispatcher(HeartbeatServices heartbeatServices, TestingHighAvailabilityServices haServices) throws Exception {
		return new TestingDispatcher(
			rpcService,
			Dispatcher.DISPATCHER_NAME + '_' + name.getMethodName(),
			configuration,
			haServices,
			new TestingResourceManagerGateway(),
			blobServer,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			null,
			new MemoryArchivedExecutionGraphStore(),
			new ExpectedJobIdJobManagerRunnerFactory(TEST_JOB_ID, createdJobManagerRunnerLatch),
			fatalErrorHandler);
	}

	@After
	public void tearDown() throws Exception {
		try {
			fatalErrorHandler.rethrowError();
		} finally {
			RpcUtils.terminateRpcEndpoint(dispatcher, TIMEOUT);
		}

		if (blobServer != null) {
			blobServer.close();
		}
	}

	/**
	 * Tests that we can submit a job to the Dispatcher which then spawns a
	 * new JobManagerRunner.
	 */
	@Test
	public void testJobSubmission() throws Exception {
		CompletableFuture<UUID> leaderFuture = dispatcherLeaderElectionService.isLeader(UUID.randomUUID());

		// wait for the leader to be elected
		leaderFuture.get();

		DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

		CompletableFuture<Acknowledge> acknowledgeFuture = dispatcherGateway.submitJob(jobGraph, TIMEOUT);

		acknowledgeFuture.get();

		assertTrue(
			"jobManagerRunner was not started",
			dispatcherLeaderElectionService.isStarted());
	}

	/**
	 * Tests that the dispatcher takes part in the leader election.
	 */
	@Test
	public void testLeaderElection() throws Exception {
		CompletableFuture<Void> jobIdsFuture = new CompletableFuture<>();
		submittedJobGraphStore.setJobIdsFunction(
			(Collection<JobID> jobIds) -> {
				jobIdsFuture.complete(null);
				return jobIds;
			});

		electDispatcher();

		// wait that we asked the SubmittedJobGraphStore for the stored jobs
		jobIdsFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	/**
	 * Test callbacks from
	 * {@link org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore.SubmittedJobGraphListener}.
	 */
	@Test
	public void testSubmittedJobGraphListener() throws Exception {
		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		final DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

		dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();
		jobMasterLeaderElectionService.isLeader(UUID.randomUUID()).get();

		final SubmittedJobGraph submittedJobGraph = submittedJobGraphStore.recoverJobGraph(TEST_JOB_ID);

		// pretend that other Dispatcher has removed job from submittedJobGraphStore
		submittedJobGraphStore.removeJobGraph(TEST_JOB_ID);
		dispatcher.onRemovedJobGraph(TEST_JOB_ID);
		assertThat(dispatcherGateway.listJobs(TIMEOUT).get(), empty());

		// pretend that other Dispatcher has added a job to submittedJobGraphStore
		runningJobsRegistry.clearJob(TEST_JOB_ID);
		submittedJobGraphStore.putJobGraph(submittedJobGraph);
		dispatcher.onAddedJobGraph(TEST_JOB_ID);
		createdJobManagerRunnerLatch.await();
		assertThat(dispatcherGateway.listJobs(TIMEOUT).get(), hasSize(1));
	}

	@Test
	public void testBlobsAreRemovedOnlyIfJobIsRemovedProperly() throws Exception {
		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();
		PermanentBlobKey key = blobServer.putPermanent(TEST_JOB_ID, new byte[128]);
		submittedJobGraphStore.setRemovalFailure(new Exception("Failed to Remove future"));
		final DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
		dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

		ArchivedExecutionGraph executionGraph = new ArchivedExecutionGraphBuilder()
			.setJobID(TEST_JOB_ID)
			.setState(JobStatus.CANCELED)
			.build();

		dispatcher.completeJobExecution(executionGraph);
		//Assert that blob was not removed, since exception was thrown while removing the job
		assertThat(blobServer.getFile(TEST_JOB_ID, key), notNullValue(File.class));
		submittedJobGraphStore.setRemovalFailure(null);
		dispatcher.completeJobExecution(executionGraph);

		//Job removing did not throw exception now, blob should be null
		expectedException.expect(NoSuchFileException.class);
		blobServer.getFile(TEST_JOB_ID, key);
	}

	@Test
	public void testOnAddedJobGraphRecoveryFailure() throws Exception {
		final FlinkException expectedFailure = new FlinkException("Expected failure");
		submittedJobGraphStore.setRecoveryFailure(expectedFailure);

		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		submittedJobGraphStore.putJobGraph(new SubmittedJobGraph(jobGraph, null));
		dispatcher.onAddedJobGraph(TEST_JOB_ID);

		final CompletableFuture<Throwable> errorFuture = fatalErrorHandler.getErrorFuture();

		final Throwable throwable = errorFuture.get();

		assertThat(ExceptionUtils.findThrowable(throwable, expectedFailure::equals).isPresent(), is(true));

		fatalErrorHandler.clearError();
	}

	@Test
	public void testOnAddedJobGraphWithFinishedJob() throws Throwable {
		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		submittedJobGraphStore.putJobGraph(new SubmittedJobGraph(jobGraph, null));
		runningJobsRegistry.setJobFinished(TEST_JOB_ID);
		dispatcher.onAddedJobGraph(TEST_JOB_ID);

		final CompletableFuture<Throwable> errorFuture = fatalErrorHandler.getErrorFuture();

		final Throwable throwable = errorFuture.get();

		assertThat(throwable, instanceOf(DispatcherException.class));

		fatalErrorHandler.clearError();
	}

	/**
	 * Test that {@link JobResult} is cached when the job finishes.
	 */
	@Test
	public void testCacheJobExecutionResult() throws Exception {
		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		final DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

		final JobID failedJobId = new JobID();

		final JobStatus expectedState = JobStatus.FAILED;
		final ArchivedExecutionGraph failedExecutionGraph = new ArchivedExecutionGraphBuilder()
			.setJobID(failedJobId)
			.setState(expectedState)
			.setFailureCause(new ErrorInfo(new RuntimeException("expected"), 1L))
			.build();

		dispatcher.completeJobExecution(failedExecutionGraph);

		assertThat(
			dispatcherGateway.requestJobStatus(failedJobId, TIMEOUT).get(),
			equalTo(expectedState));
		assertThat(
			dispatcherGateway.requestJob(failedJobId, TIMEOUT).get(),
			equalTo(failedExecutionGraph));
	}

	@Test
	public void testThrowExceptionIfJobExecutionResultNotFound() throws Exception {
		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		final DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
		try {
			dispatcherGateway.requestJob(new JobID(), TIMEOUT).get();
		} catch (ExecutionException e) {
			final Throwable throwable = ExceptionUtils.stripExecutionException(e);
			assertThat(throwable, instanceOf(FlinkJobNotFoundException.class));
		}
	}

	/**
	 * Tests that a reelected Dispatcher can recover jobs.
	 */
	@Test
	public void testJobRecovery() throws Exception {
		final DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

		// elect the initial dispatcher as the leader
		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		// submit the job to the current leader
		dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

		// check that the job has been persisted
		assertThat(submittedJobGraphStore.getJobIds(), contains(jobGraph.getJobID()));

		jobMasterLeaderElectionService.isLeader(UUID.randomUUID()).get();

		assertThat(runningJobsRegistry.getJobSchedulingStatus(jobGraph.getJobID()), is(RunningJobsRegistry.JobSchedulingStatus.RUNNING));

		// revoke the leadership which will stop all currently running jobs
		dispatcherLeaderElectionService.notLeader();

		// re-grant the leadership, this should trigger the job recovery
		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		// wait until we have recovered the job
		createdJobManagerRunnerLatch.await();

		// check whether the job has been recovered
		final Collection<JobID> jobIds = dispatcherGateway.listJobs(TIMEOUT).get();

		assertThat(jobIds, hasSize(1));
		assertThat(jobIds, contains(jobGraph.getJobID()));
	}

	/**
	 * Tests that we can dispose a savepoint.
	 */
	@Test
	public void testSavepointDisposal() throws Exception {
		final URI externalPointer = createTestingSavepoint();
		final Path savepointPath = Paths.get(externalPointer);

		final DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		assertThat(Files.exists(savepointPath), is(true));

		dispatcherGateway.disposeSavepoint(externalPointer.toString(), TIMEOUT).get();

		assertThat(Files.exists(savepointPath), is(false));
	}

	@Nonnull
	private URI createTestingSavepoint() throws IOException, URISyntaxException {
		final StateBackend stateBackend = Checkpoints.loadStateBackend(configuration, Thread.currentThread().getContextClassLoader(), log);
		final CheckpointStorage checkpointStorage = stateBackend.createCheckpointStorage(jobGraph.getJobID());
		final File savepointFile = temporaryFolder.newFolder();
		final long checkpointId = 1L;

		final CheckpointStorageLocation checkpointStorageLocation = checkpointStorage.initializeLocationForSavepoint(checkpointId, savepointFile.getAbsolutePath());

		final CheckpointMetadataOutputStream metadataOutputStream = checkpointStorageLocation.createMetadataOutputStream();
		Checkpoints.storeCheckpointMetadata(new SavepointV2(checkpointId, Collections.emptyList(), Collections.emptyList()), metadataOutputStream);

		final CompletedCheckpointStorageLocation completedCheckpointStorageLocation = metadataOutputStream.closeAndFinalizeCheckpoint();

		return new URI(completedCheckpointStorageLocation.getExternalPointer());

	}

	/**
	 * Tests that we wait until the JobMaster has gained leader ship before sending requests
	 * to it. See FLINK-8887.
	 */
	@Test
	public void testWaitingForJobMasterLeadership() throws ExecutionException, InterruptedException {
		final DispatcherGateway dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		dispatcherGateway.submitJob(jobGraph, TIMEOUT).get();

		final CompletableFuture<JobStatus> jobStatusFuture = dispatcherGateway.requestJobStatus(jobGraph.getJobID(), TIMEOUT);

		assertThat(jobStatusFuture.isDone(), is(false));

		try {
			jobStatusFuture.get(10, TimeUnit.MILLISECONDS);
			fail("Should not complete.");
		} catch (TimeoutException ignored) {
			// ignored
		}

		jobMasterLeaderElectionService.isLeader(UUID.randomUUID()).get();

		assertThat(jobStatusFuture.get(), notNullValue());
	}

	/**
	 * Tests that the {@link Dispatcher} terminates if it cannot recover jobs ids from
	 * the {@link SubmittedJobGraphStore}. See FLINK-8943.
	 */
	@Test
	public void testFatalErrorAfterJobIdRecoveryFailure() throws Exception {
		final FlinkException testException = new FlinkException("Test exception");
		submittedJobGraphStore.setJobIdsFunction(
			(Collection<JobID> jobIds) -> {
				throw testException;
			});

		electDispatcher();

		// we expect that a fatal error occurred
		final Throwable error = fatalErrorHandler.getErrorFuture().get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

		assertThat(ExceptionUtils.findThrowableWithMessage(error, testException.getMessage()).isPresent(), is(true));

		fatalErrorHandler.clearError();
	}

	/**
	 * Tests that the {@link Dispatcher} terminates if it cannot recover jobs from
	 * the {@link SubmittedJobGraphStore}. See FLINK-8943.
	 */
	@Test
	public void testFatalErrorAfterJobRecoveryFailure() throws Exception {
		final FlinkException testException = new FlinkException("Test exception");

		final SubmittedJobGraph submittedJobGraph = new SubmittedJobGraph(jobGraph, null);
		submittedJobGraphStore.putJobGraph(submittedJobGraph);

		submittedJobGraphStore.setRecoverJobGraphFunction(
			(JobID jobId, Map<JobID, SubmittedJobGraph> submittedJobs) -> {
				throw testException;
			});

		electDispatcher();

		// we expect that a fatal error occurred
		final Throwable error = fatalErrorHandler.getErrorFuture().get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

		assertThat(ExceptionUtils.findThrowableWithMessage(error, testException.getMessage()).isPresent(), is(true));

		fatalErrorHandler.clearError();
	}

	/**
	 * Tests that the {@link Dispatcher} fails fatally if the job submission of a recovered job fails.
	 * See FLINK-9097.
	 */
	@Test
	public void testJobSubmissionErrorAfterJobRecovery() throws Exception {
		final FlinkException testException = new FlinkException("Test exception");

		final JobGraph failingJobGraph = createFailingJobGraph(testException);

		final SubmittedJobGraph submittedJobGraph = new SubmittedJobGraph(failingJobGraph, null);
		submittedJobGraphStore.putJobGraph(submittedJobGraph);

		electDispatcher();

		final Throwable error = fatalErrorHandler.getErrorFuture().get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

		assertThat(ExceptionUtils.findThrowableWithMessage(error, testException.getMessage()).isPresent(), is(true));

		fatalErrorHandler.clearError();
	}

	private void electDispatcher() {
		UUID expectedLeaderSessionId = UUID.randomUUID();

		assertNull(dispatcherLeaderElectionService.getConfirmationFuture());

		dispatcherLeaderElectionService.isLeader(expectedLeaderSessionId);
	}

	private JobGraph createFailingJobGraph(Exception failureCause) {
		final FailingJobVertex jobVertex = new FailingJobVertex("Failing JobVertex", failureCause);
		jobVertex.setInvokableClass(NoOpInvokable.class);
		return new JobGraph(jobGraph.getJobID(), "Failing JobGraph", jobVertex);
	}

	private static class FailingJobVertex extends JobVertex {

		private static final long serialVersionUID = 3218428829168840760L;

		private final Exception failure;

		private FailingJobVertex(String name, Exception failure) {
			super(name);
			this.failure = failure;
		}

		@Override
		public void initializeOnMaster(ClassLoader loader) throws Exception {
			throw failure;
		}
	}

	private static final class ExpectedJobIdJobManagerRunnerFactory implements Dispatcher.JobManagerRunnerFactory {

		private final JobID expectedJobId;

		private final CountDownLatch createdJobManagerRunnerLatch;

		private ExpectedJobIdJobManagerRunnerFactory(JobID expectedJobId, CountDownLatch createdJobManagerRunnerLatch) {
			this.expectedJobId = expectedJobId;
			this.createdJobManagerRunnerLatch = createdJobManagerRunnerLatch;
		}

		@Override
		public JobManagerRunner createJobManagerRunner(
				ResourceID resourceId,
				JobGraph jobGraph,
				Configuration configuration,
				RpcService rpcService,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				BlobServer blobServer,
				JobManagerSharedServices jobManagerSharedServices,
				JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory,
				FatalErrorHandler fatalErrorHandler) throws Exception {
			assertEquals(expectedJobId, jobGraph.getJobID());

			createdJobManagerRunnerLatch.countDown();

			return Dispatcher.DefaultJobManagerRunnerFactory.INSTANCE.createJobManagerRunner(
				resourceId,
				jobGraph,
				configuration,
				rpcService,
				highAvailabilityServices,
				heartbeatServices,
				blobServer,
				jobManagerSharedServices,
				jobManagerJobMetricGroupFactory,
				fatalErrorHandler);
		}
	}

	private static final class FailableSubmittedJobGraphStore extends InMemorySubmittedJobGraphStore {

		@Nullable
		private Exception recoveryFailure = null;

		@Nullable
		private Exception removalFailure = null;

		void setRecoveryFailure(@Nullable Exception recoveryFailure) {
			this.recoveryFailure = recoveryFailure;
		}

		void setRemovalFailure(@Nullable Exception removalFailure) {
			this.removalFailure = removalFailure;
		}

		@Override
		public synchronized SubmittedJobGraph recoverJobGraph(JobID jobId) throws Exception {
			if (recoveryFailure != null) {
				throw recoveryFailure;
			} else {
				return super.recoverJobGraph(jobId);
			}
		}

		@Override
		public synchronized void removeJobGraph(JobID jobId) throws Exception {
			if (removalFailure != null) {
				throw removalFailure;
			} else {
				super.removeJobGraph(jobId);
			}
		}
	}
}
