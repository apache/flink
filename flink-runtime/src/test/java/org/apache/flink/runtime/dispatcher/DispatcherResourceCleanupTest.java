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
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.groups.JobManagerMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.InMemorySubmittedJobGraphStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests the resource cleanup by the {@link Dispatcher}.
 */
public class DispatcherResourceCleanupTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

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

	private CompletableFuture<BlobKey> storedBlobFuture;
	private CompletableFuture<JobID> deleteAllFuture;
	private CompletableFuture<ArchivedExecutionGraph> resultFuture;
	private CompletableFuture<JobID> cleanupJobFuture;
	private CompletableFuture<Void> terminationFuture;

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
		highAvailabilityServices.setSubmittedJobGraphStore(new InMemorySubmittedJobGraphStore());

		storedBlobFuture = new CompletableFuture<>();
		deleteAllFuture = new CompletableFuture<>();

		final TestingBlobStore testingBlobStore = new TestingBlobStoreBuilder()
			.setPutFunction(
				putArguments -> storedBlobFuture.complete(putArguments.f2))
			.setDeleteAllFunction(deleteAllFuture::complete)
			.createTestingBlobStore();

		cleanupJobFuture = new CompletableFuture<>();
		terminationFuture = new CompletableFuture<>();

		blobServer = new TestingBlobServer(configuration, testingBlobStore, cleanupJobFuture);

		// upload a blob to the blob server
		permanentBlobKey = blobServer.putPermanent(jobId, new byte[256]);
		blobFile = blobServer.getStorageLocation(jobId, permanentBlobKey);

		resultFuture = new CompletableFuture<>();

		fatalErrorHandler = new TestingFatalErrorHandler();

		dispatcher = new TestingDispatcher(
			rpcService,
			Dispatcher.DISPATCHER_NAME + UUID.randomUUID(),
			configuration,
			highAvailabilityServices,
			highAvailabilityServices.getSubmittedJobGraphStore(),
			new TestingResourceManagerGateway(),
			blobServer,
			new HeartbeatServices(1000L, 1000L),
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			null,
			new MemoryArchivedExecutionGraphStore(),
			new TestingJobManagerRunnerFactory(resultFuture, terminationFuture),
			fatalErrorHandler);

		dispatcher.start();

		dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);

		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		assertThat(blobFile.exists(), is(true));

		// verify that we stored the blob also in the BlobStore
		assertThat(storedBlobFuture.get(), equalTo(permanentBlobKey));
	}

	@After
	public void teardown() throws Exception {
		if (dispatcher != null) {
			dispatcher.shutDown();
			dispatcher.getTerminationFuture().get();
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
		resultFuture.complete(new ArchivedExecutionGraphBuilder().setJobID(jobId).setState(JobStatus.FINISHED).build());
		terminationFuture.complete(null);

		assertThat(cleanupJobFuture.get(), equalTo(jobId));

		// verify that we also cleared the BlobStore
		assertThat(deleteAllFuture.get(), equalTo(jobId));

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
			deleteAllFuture.get(50L, TimeUnit.MILLISECONDS);
			fail("We should not delete the HA blobs.");
		} catch (TimeoutException ignored) {
			// expected
		}

		assertThat(deleteAllFuture.isDone(), is(false));
	}

	@Test
	public void testBlobServerCleanupWhenClosingDispatcher() throws Exception {
		submitJob();

		dispatcher.shutDown();
		terminationFuture.complete(null);
		dispatcher.getTerminationFuture().get();

		assertThat(cleanupJobFuture.get(), equalTo(jobId));

		assertThat(blobFile.exists(), is(false));

		// verify that we did not clear the BlobStore
		try {
			deleteAllFuture.get(50L, TimeUnit.MILLISECONDS);
			fail("We should not delete the HA blobs.");
		} catch (TimeoutException ignored) {
			// expected
		}

		assertThat(deleteAllFuture.isDone(), is(false));
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

	private static final class TestingDispatcher extends Dispatcher {
		TestingDispatcher(RpcService rpcService, String endpointId, Configuration configuration, HighAvailabilityServices highAvailabilityServices, SubmittedJobGraphStore submittedJobGraphStore, ResourceManagerGateway resourceManagerGateway, BlobServer blobServer, HeartbeatServices heartbeatServices, JobManagerMetricGroup jobManagerMetricGroup, @Nullable String metricServiceQueryPath, ArchivedExecutionGraphStore archivedExecutionGraphStore, JobManagerRunnerFactory jobManagerRunnerFactory, FatalErrorHandler fatalErrorHandler) throws Exception {
			super(
				rpcService,
				endpointId,
				configuration,
				highAvailabilityServices,
				submittedJobGraphStore,
				resourceManagerGateway,
				blobServer,
				heartbeatServices,
				jobManagerMetricGroup,
				metricServiceQueryPath,
				archivedExecutionGraphStore,
				jobManagerRunnerFactory,
				fatalErrorHandler,
				null,
				VoidHistoryServerArchivist.INSTANCE);
		}
	}

	private static final class TestingJobManagerRunnerFactory implements Dispatcher.JobManagerRunnerFactory {

		private final CompletableFuture<ArchivedExecutionGraph> resultFuture;

		private final CompletableFuture<Void> terminationFuture;

		private TestingJobManagerRunnerFactory(CompletableFuture<ArchivedExecutionGraph> resultFuture, CompletableFuture<Void> terminationFuture) {
			this.resultFuture = resultFuture;
			this.terminationFuture = terminationFuture;
		}

		@Override
		public JobManagerRunner createJobManagerRunner(ResourceID resourceId, JobGraph jobGraph, Configuration configuration, RpcService rpcService, HighAvailabilityServices highAvailabilityServices, HeartbeatServices heartbeatServices, BlobServer blobServer, JobManagerSharedServices jobManagerServices, JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory, FatalErrorHandler fatalErrorHandler) {
			final JobManagerRunner jobManagerRunnerMock = mock(JobManagerRunner.class);

			when(jobManagerRunnerMock.getResultFuture()).thenReturn(resultFuture);
			when(jobManagerRunnerMock.closeAsync()).thenReturn(terminationFuture);

			return jobManagerRunnerMock;
		}
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
