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
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.blob.BlobKey;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.BlobStore;
import org.apache.flink.runtime.blob.PermanentBlobKey;
import org.apache.flink.runtime.blob.TestingBlobStore;
import org.apache.flink.runtime.blob.TestingBlobStoreBuilder;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Tests the resource cleanup by the {@link Dispatcher}.
 */
public class DispatcherResourceCleanupTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	private static final Time timeout = Time.seconds(10L);

	private static TestingRpcService rpcService;

	private JobID jobId;

	private JobGraph jobGraph;

	private Configuration configuration;

	private TestingLeaderElectionService dispatcherLeaderElectionService;

	private SingleRunningJobsRegistry runningJobsRegistry;

	private TestingHighAvailabilityServices highAvailabilityServices;

	private OneShotLatch clearedJobLatch;

	private TestingDispatcher dispatcher;

	private DispatcherGateway dispatcherGateway;

	private TestingFatalErrorHandler fatalErrorHandler;

	private BlobServer blobServer;

	private PermanentBlobKey permanentBlobKey;

	private File blobFile;

	private AtomicReference<Supplier<Exception>> failJobMasterCreationWith;

	private CompletableFuture<BlobKey> storedHABlobFuture;
	private CompletableFuture<JobID> deleteAllHABlobsFuture;
	private CompletableFuture<ArchivedExecutionGraph> resultFuture;
	private CompletableFuture<JobID> cleanupJobFuture;
	private CompletableFuture<Void> terminationFuture;
	private FaultySubmittedJobGraphStore submittedJobGraphStore;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();
	}

	@Before
	public void setup() throws Exception {
		final JobVertex testVertex = new JobVertex("testVertex");
		testVertex.setInvokableClass(NoOpInvokable.class);
		jobId = new JobID();
		jobGraph = new JobGraph(jobId, "testJob", testVertex);
		jobGraph.setAllowQueuedScheduling(true);

		configuration = new Configuration();
		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		highAvailabilityServices = new TestingHighAvailabilityServices();
		dispatcherLeaderElectionService = new TestingLeaderElectionService();
		highAvailabilityServices.setDispatcherLeaderElectionService(dispatcherLeaderElectionService);
		clearedJobLatch = new OneShotLatch();
		runningJobsRegistry = new SingleRunningJobsRegistry(jobId, clearedJobLatch);
		highAvailabilityServices.setRunningJobsRegistry(runningJobsRegistry);
		submittedJobGraphStore = new FaultySubmittedJobGraphStore();
		highAvailabilityServices.setSubmittedJobGraphStore(submittedJobGraphStore);

		storedHABlobFuture = new CompletableFuture<>();
		deleteAllHABlobsFuture = new CompletableFuture<>();

		final TestingBlobStore testingBlobStore = new TestingBlobStoreBuilder()
			.setPutFunction(
				putArguments -> storedHABlobFuture.complete(putArguments.f2))
			.setDeleteAllFunction(deleteAllHABlobsFuture::complete)
			.createTestingBlobStore();

		cleanupJobFuture = new CompletableFuture<>();
		terminationFuture = new CompletableFuture<>();

		blobServer = new TestingBlobServer(configuration, testingBlobStore, cleanupJobFuture);

		// upload a blob to the blob server
		permanentBlobKey = blobServer.putPermanent(jobId, new byte[256]);
		jobGraph.addUserJarBlobKey(permanentBlobKey);
		blobFile = blobServer.getStorageLocation(jobId, permanentBlobKey);

		resultFuture = new CompletableFuture<>();

		fatalErrorHandler = new TestingFatalErrorHandler();

		failJobMasterCreationWith = new AtomicReference<>();

		TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		dispatcher = new TestingDispatcher(
			rpcService,
			Dispatcher.DISPATCHER_NAME + UUID.randomUUID(),
			configuration,
			highAvailabilityServices,
			() -> CompletableFuture.completedFuture(resourceManagerGateway),
			blobServer,
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			null,
			new MemoryArchivedExecutionGraphStore(),
			new TestingJobManagerRunnerFactory(new CompletableFuture<>(), resultFuture, terminationFuture, failJobMasterCreationWith),
			fatalErrorHandler);

		dispatcher.start();

		dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		assertThat(blobFile.exists(), is(true));

		// verify that we stored the blob also in the BlobStore
		assertThat(storedHABlobFuture.get(), equalTo(permanentBlobKey));
	}

	@After
	public void teardown() throws Exception {
		if (dispatcher != null) {
			dispatcher.close();
		}

		if (fatalErrorHandler != null) {
			fatalErrorHandler.rethrowError();
		}
	}

	@AfterClass
	public static void teardownClass() throws ExecutionException, InterruptedException {
		if (rpcService != null) {
			rpcService.stopService().get();
		}
	}

	@Test
	public void testBlobServerCleanupWhenJobFinished() throws Exception {
		submitJob();

		// complete the job
		finishJob();

		assertThatHABlobsHaveBeenRemoved();
	}

	private void assertThatHABlobsHaveBeenRemoved() throws InterruptedException, ExecutionException {
		assertThat(cleanupJobFuture.get(), equalTo(jobId));

		// verify that we also cleared the BlobStore
		assertThat(deleteAllHABlobsFuture.get(), equalTo(jobId));

		assertThat(blobFile.exists(), is(false));
	}

	private void submitJob() throws InterruptedException, ExecutionException {
		final CompletableFuture<Acknowledge> submissionFuture = dispatcherGateway.submitJob(jobGraph, timeout);
		submissionFuture.get();
	}

	@Test
	public void testBlobServerCleanupWhenJobNotFinished() throws Exception {
		submitJob();

		// job not finished
		resultFuture.completeExceptionally(new JobNotFinishedException(jobId));
		terminationFuture.complete(null);

		assertThat(cleanupJobFuture.get(), equalTo(jobId));

		assertThat(blobFile.exists(), is(false));

		// verify that we did not clear the BlobStore
		try {
			deleteAllHABlobsFuture.get(50L, TimeUnit.MILLISECONDS);
			fail("We should not delete the HA blobs.");
		} catch (TimeoutException ignored) {
			// expected
		}

		assertThat(deleteAllHABlobsFuture.isDone(), is(false));
	}

	/**
	 * Tests that the uploaded blobs are being cleaned up in case of a job submission failure.
	 */
	@Test
	public void testBlobServerCleanupWhenJobSubmissionFails() throws Exception {
		failJobMasterCreationWith.set(() -> new FlinkException("Test exception."));
		final CompletableFuture<Acknowledge> submissionFuture = dispatcherGateway.submitJob(jobGraph, timeout);

		try {
			submissionFuture.get();
			fail("Job submission was expected to fail.");
		} catch (ExecutionException ee) {
			assertThat(ExceptionUtils.findThrowable(ee, JobSubmissionException.class).isPresent(), is(true));
		}

		assertThatHABlobsHaveBeenRemoved();
	}

	@Test
	public void testBlobServerCleanupWhenClosingDispatcher() throws Exception {
		submitJob();

		dispatcher.closeAsync();
		terminationFuture.complete(null);
		dispatcher.getTerminationFuture().get();

		assertThat(cleanupJobFuture.get(), equalTo(jobId));

		assertThat(blobFile.exists(), is(false));

		// verify that we did not clear the BlobStore
		try {
			deleteAllHABlobsFuture.get(50L, TimeUnit.MILLISECONDS);
			fail("We should not delete the HA blobs.");
		} catch (TimeoutException ignored) {
			// expected
		}

		assertThat(deleteAllHABlobsFuture.isDone(), is(false));
	}

	/**
	 * Tests that the {@link RunningJobsRegistry} entries are cleared after the
	 * job reached a terminal state.
	 */
	@Test
	public void testRunningJobsRegistryCleanup() throws Exception {
		submitJob();

		runningJobsRegistry.setJobRunning(jobId);
		assertThat(runningJobsRegistry.contains(jobId), is(true));

		resultFuture.complete(new ArchivedExecutionGraphBuilder().setState(JobStatus.FINISHED).setJobID(jobId).build());
		terminationFuture.complete(null);

		// wait for the clearing
		clearedJobLatch.await();

		assertThat(runningJobsRegistry.contains(jobId), is(false));
	}

	/**
	 * Tests that the previous JobManager needs to be completely terminated
	 * before a new job with the same {@link JobID} is started.
	 */
	@Test
	public void testJobSubmissionUnderSameJobId() throws Exception {
		submitJob();

		runningJobsRegistry.setJobRunning(jobId);
		resultFuture.completeExceptionally(new JobNotFinishedException(jobId));

		final CompletableFuture<Acknowledge> submissionFuture = dispatcherGateway.submitJob(jobGraph, timeout);

		try {
			submissionFuture.get(10L, TimeUnit.MILLISECONDS);
			fail("The job submission future should not complete until the previous JobManager " +
				"termination future has been completed.");
		} catch (TimeoutException ignored) {
			// expected
		} finally {
			terminationFuture.complete(null);
		}

		assertThat(submissionFuture.get(), equalTo(Acknowledge.get()));
	}

	/**
	 * Tests that a duplicate job submission won't delete any job meta data
	 * (submitted job graphs, blobs, etc.).
	 */
	@Test
	public void testDuplicateJobSubmissionDoesNotDeleteJobMetaData() throws Exception {
		submitJob();

		final CompletableFuture<Acknowledge> submissionFuture = dispatcherGateway.submitJob(jobGraph, timeout);

		try {
			try {
				submissionFuture.get();
				fail("Expected a JobSubmissionFailure.");
			} catch (ExecutionException ee) {
				assertThat(ExceptionUtils.findThrowable(ee, JobSubmissionException.class).isPresent(), is(true));
			}

			assertThatHABlobsHaveNotBeenRemoved();
		} finally {
			finishJob();
		}

		assertThatHABlobsHaveBeenRemoved();
	}

	private void finishJob() {
		resultFuture.complete(new ArchivedExecutionGraphBuilder().setJobID(jobId).setState(JobStatus.FINISHED).build());
		terminationFuture.complete(null);
	}

	private void assertThatHABlobsHaveNotBeenRemoved() {
		assertThat(cleanupJobFuture.isDone(), is(false));
		assertThat(deleteAllHABlobsFuture.isDone(), is(false));
		assertThat(blobFile.exists(), is(true));
	}

	/**
	 * Tests that recovered jobs will only be started after the complete termination of any
	 * other previously running JobMasters for the same job.
	 */
	@Test
	public void testJobRecoveryWithPendingTermination() throws Exception {
		submitJob();
		runningJobsRegistry.setJobRunning(jobId);

		dispatcherLeaderElectionService.notLeader();
		final UUID leaderSessionId = UUID.randomUUID();
		final CompletableFuture<UUID> leaderFuture = dispatcherLeaderElectionService.isLeader(leaderSessionId);

		try {
			leaderFuture.get(10L, TimeUnit.MILLISECONDS);
			fail("We should not become leader before all previously running JobMasters have terminated.");
		} catch (TimeoutException ignored) {
			// expected
		} finally {
			terminationFuture.complete(null);
		}

		assertThat(leaderFuture.get(), equalTo(leaderSessionId));
	}

	private static final class SingleRunningJobsRegistry implements RunningJobsRegistry {

		@Nonnull
		private final JobID expectedJobId;

		@Nonnull
		private final OneShotLatch clearedJobLatch;

		private JobSchedulingStatus jobSchedulingStatus = JobSchedulingStatus.PENDING;

		private boolean containsJob = false;

		private SingleRunningJobsRegistry(@Nonnull JobID expectedJobId, @Nonnull OneShotLatch clearedJobLatch) {
			this.expectedJobId = expectedJobId;
			this.clearedJobLatch = clearedJobLatch;
		}

		@Override
		public void setJobRunning(JobID jobID) {
			checkJobId(jobID);
			containsJob = true;
			jobSchedulingStatus = JobSchedulingStatus.RUNNING;
		}

		private void checkJobId(JobID jobID) {
			Preconditions.checkArgument(expectedJobId.equals(jobID));
		}

		@Override
		public void setJobFinished(JobID jobID) {
			checkJobId(jobID);
			containsJob = true;
			jobSchedulingStatus = JobSchedulingStatus.DONE;
		}

		@Override
		public JobSchedulingStatus getJobSchedulingStatus(JobID jobID) {
			checkJobId(jobID);
			return jobSchedulingStatus;
		}

		public boolean contains(JobID jobId) {
			checkJobId(jobId);
			return containsJob;
		}

		@Override
		public void clearJob(JobID jobID) {
			checkJobId(jobID);
			containsJob = false;
			clearedJobLatch.trigger();
		}
	}

	@Test
	public void testHABlobsAreNotRemovedIfHAJobGraphRemovalFails() throws Exception {
		submittedJobGraphStore.setRemovalFailure(new Exception("Failed to Remove future"));
		submitJob();

		ArchivedExecutionGraph executionGraph = new ArchivedExecutionGraphBuilder()
			.setJobID(jobId)
			.setState(JobStatus.CANCELED)
			.build();

		resultFuture.complete(executionGraph);
		terminationFuture.complete(null);

		assertThat(cleanupJobFuture.get(), equalTo(jobId));
		assertThat(deleteAllHABlobsFuture.isDone(), is(false));
	}

	@Test
	public void testHABlobsAreRemovedIfHAJobGraphRemovalSucceeds() throws Exception {
		submitJob();

		ArchivedExecutionGraph executionGraph = new ArchivedExecutionGraphBuilder()
			.setJobID(jobId)
			.setState(JobStatus.CANCELED)
			.build();

		resultFuture.complete(executionGraph);
		terminationFuture.complete(null);

		assertThat(cleanupJobFuture.get(), equalTo(jobId));
		assertThat(deleteAllHABlobsFuture.get(), equalTo(jobId));
	}

	private static final class TestingBlobServer extends BlobServer {

		private final CompletableFuture<JobID> cleanupJobFuture;

		/**
		 * Instantiates a new BLOB server and binds it to a free network port.
		 *
		 * @param config    Configuration to be used to instantiate the BlobServer
		 * @param blobStore BlobStore to store blobs persistently
		 * @param cleanupJobFuture
		 * @throws IOException thrown if the BLOB server cannot bind to a free network port or if the
		 *                     (local or distributed) file storage cannot be created or is not usable
		 */
		public TestingBlobServer(Configuration config, BlobStore blobStore, CompletableFuture<JobID> cleanupJobFuture) throws IOException {
			super(config, blobStore);
			this.cleanupJobFuture = cleanupJobFuture;
		}

		@Override
		public boolean cleanupJob(JobID jobId, boolean cleanupBlobStoreFiles) {
			final boolean result = super.cleanupJob(jobId, cleanupBlobStoreFiles);
			cleanupJobFuture.complete(jobId);
			return result;
		}
	}
}
