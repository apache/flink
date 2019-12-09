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
import org.apache.flink.runtime.client.DuplicateJobSubmissionException;
import org.apache.flink.runtime.client.JobSubmissionException;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.JobGraphWriter;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobNotFinishedException;
import org.apache.flink.runtime.jobmaster.TestingJobManagerRunner;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.TestingJobGraphStore;
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
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

	private static MetricRegistryImpl metricRegistry;

	private JobID jobId;

	private JobGraph jobGraph;

	private Configuration configuration;

	private SingleRunningJobsRegistry runningJobsRegistry;

	private TestingHighAvailabilityServices highAvailabilityServices;

	private OneShotLatch clearedJobLatch;

	private TestingDispatcher dispatcher;

	private DispatcherGateway dispatcherGateway;

	private TestingFatalErrorHandler fatalErrorHandler;

	private BlobServer blobServer;

	private PermanentBlobKey permanentBlobKey;

	private File blobFile;

	private CompletableFuture<BlobKey> storedHABlobFuture;
	private CompletableFuture<JobID> deleteAllHABlobsFuture;
	private CompletableFuture<JobID> cleanupJobFuture;
	private JobGraphWriter jobGraphWriter = NoOpJobGraphWriter.INSTANCE;

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

		configuration = new Configuration();
		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		highAvailabilityServices = new TestingHighAvailabilityServices();
		clearedJobLatch = new OneShotLatch();
		runningJobsRegistry = new SingleRunningJobsRegistry(jobId, clearedJobLatch);
		highAvailabilityServices.setRunningJobsRegistry(runningJobsRegistry);

		storedHABlobFuture = new CompletableFuture<>();
		deleteAllHABlobsFuture = new CompletableFuture<>();

		final TestingBlobStore testingBlobStore = new TestingBlobStoreBuilder()
			.setPutFunction(
				putArguments -> storedHABlobFuture.complete(putArguments.f2))
			.setDeleteAllFunction(deleteAllHABlobsFuture::complete)
			.createTestingBlobStore();

		cleanupJobFuture = new CompletableFuture<>();

		blobServer = new TestingBlobServer(configuration, testingBlobStore, cleanupJobFuture);

		// upload a blob to the blob server
		permanentBlobKey = blobServer.putPermanent(jobId, new byte[256]);
		jobGraph.addUserJarBlobKey(permanentBlobKey);
		blobFile = blobServer.getStorageLocation(jobId, permanentBlobKey);

		assertThat(blobFile.exists(), is(true));

		// verify that we stored the blob also in the BlobStore
		assertThat(storedHABlobFuture.get(), equalTo(permanentBlobKey));

		fatalErrorHandler = new TestingFatalErrorHandler();

		metricRegistry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
	}

	private TestingJobManagerRunnerFactory startDispatcherAndSubmitJob() throws Exception {
		return startDispatcherAndSubmitJob(0);
	}

	private TestingJobManagerRunnerFactory startDispatcherAndSubmitJob(int numBlockingJobManagerRunners) throws Exception {
		final TestingJobManagerRunnerFactory testingJobManagerRunnerFactoryNG = new TestingJobManagerRunnerFactory(numBlockingJobManagerRunners);
		startDispatcher(testingJobManagerRunnerFactoryNG);
		submitJob();

		return testingJobManagerRunnerFactoryNG;
	}

	private void startDispatcher(JobManagerRunnerFactory jobManagerRunnerFactory) throws Exception {
		TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		final HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 1000L);
		final MemoryArchivedExecutionGraphStore archivedExecutionGraphStore = new MemoryArchivedExecutionGraphStore();

		metricRegistry.startQueryService(rpcService, new ResourceID("mqs"));
		final String metricQueryServiceAddress = metricRegistry.getMetricQueryServiceGatewayRpcAddress();

		dispatcher = new TestingDispatcher(
			rpcService,
			Dispatcher.DISPATCHER_NAME + UUID.randomUUID(),
			DispatcherId.generate(),
			Collections.emptyList(),
			new DispatcherServices(
				configuration,
				highAvailabilityServices,
				() -> CompletableFuture.completedFuture(resourceManagerGateway),
				blobServer,
				heartbeatServices,
				archivedExecutionGraphStore,
				fatalErrorHandler,
				VoidHistoryServerArchivist.INSTANCE,
				metricQueryServiceAddress,
				UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
				jobGraphWriter,
				jobManagerRunnerFactory));

		dispatcher.start();

		dispatcherGateway = dispatcher.getSelfGateway(DispatcherGateway.class);
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

		if (metricRegistry != null) {
			metricRegistry.shutdown().get();
		}
	}

	@Test
	public void testBlobServerCleanupWhenJobFinished() throws Exception {
		final TestingJobManagerRunnerFactory jobManagerRunnerFactory = startDispatcherAndSubmitJob();

		// complete the job
		finishJob(jobManagerRunnerFactory.takeCreatedJobManagerRunner());

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
		final TestingJobManagerRunnerFactory jobManagerRunnerFactory = startDispatcherAndSubmitJob();

		// job not finished
		final TestingJobManagerRunner testingJobManagerRunner = jobManagerRunnerFactory.takeCreatedJobManagerRunner();
		testingJobManagerRunner.completeResultFutureExceptionally(new JobNotFinishedException(jobId));

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
		startDispatcher(new FailingJobManagerRunnerFactory(new FlinkException("Test exception")));
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
		startDispatcherAndSubmitJob();

		dispatcher.closeAsync().get();

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
		final TestingJobManagerRunnerFactory jobManagerRunnerFactory = startDispatcherAndSubmitJob();

		runningJobsRegistry.setJobRunning(jobId);
		assertThat(runningJobsRegistry.contains(jobId), is(true));

		final TestingJobManagerRunner testingJobManagerRunner = jobManagerRunnerFactory.takeCreatedJobManagerRunner();
		testingJobManagerRunner.completeResultFuture(new ArchivedExecutionGraphBuilder().setState(JobStatus.FINISHED).setJobID(jobId).build());

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
		final TestingJobManagerRunnerFactory jobManagerRunnerFactory = startDispatcherAndSubmitJob(1);

		runningJobsRegistry.setJobRunning(jobId);
		final TestingJobManagerRunner testingJobManagerRunner = jobManagerRunnerFactory.takeCreatedJobManagerRunner();
		testingJobManagerRunner.completeResultFutureExceptionally(new JobNotFinishedException(jobId));

		final CompletableFuture<Acknowledge> submissionFuture = dispatcherGateway.submitJob(jobGraph, timeout);

		try {
			submissionFuture.get(10L, TimeUnit.MILLISECONDS);
			fail("The job submission future should not complete until the previous JobManager " +
				"termination future has been completed.");
		} catch (TimeoutException ignored) {
			// expected
		} finally {
			testingJobManagerRunner.completeTerminationFuture();
		}

		assertThat(submissionFuture.get(), equalTo(Acknowledge.get()));
	}

	/**
	 * Tests that a duplicate job submission won't delete any job meta data
	 * (submitted job graphs, blobs, etc.).
	 */
	@Test
	public void testDuplicateJobSubmissionDoesNotDeleteJobMetaData() throws Exception {
		final TestingJobManagerRunnerFactory testingJobManagerRunnerFactoryNG = startDispatcherAndSubmitJob();

		final CompletableFuture<Acknowledge> submissionFuture = dispatcherGateway.submitJob(jobGraph, timeout);

		try {
			try {
				submissionFuture.get();
				fail("Expected a DuplicateJobSubmissionFailure.");
			} catch (ExecutionException ee) {
				assertThat(ExceptionUtils.findThrowable(ee, DuplicateJobSubmissionException.class).isPresent(), is(true));
			}

			assertThatHABlobsHaveNotBeenRemoved();
		} finally {
			finishJob(testingJobManagerRunnerFactoryNG.takeCreatedJobManagerRunner());
		}

		assertThatHABlobsHaveBeenRemoved();
	}

	private void finishJob(TestingJobManagerRunner takeCreatedJobManagerRunner) {
		takeCreatedJobManagerRunner.completeResultFuture(new ArchivedExecutionGraphBuilder().setJobID(jobId).setState(JobStatus.FINISHED).build());
	}

	private void assertThatHABlobsHaveNotBeenRemoved() {
		assertThat(cleanupJobFuture.isDone(), is(false));
		assertThat(deleteAllHABlobsFuture.isDone(), is(false));
		assertThat(blobFile.exists(), is(true));
	}

	@Test
	public void testDispatcherTerminationTerminatesRunningJobMasters() throws Exception {
		final TestingJobManagerRunnerFactory jobManagerRunnerFactory = startDispatcherAndSubmitJob();

		dispatcher.closeAsync().get();

		final TestingJobManagerRunner jobManagerRunner = jobManagerRunnerFactory.takeCreatedJobManagerRunner();
		assertThat(jobManagerRunner.getTerminationFuture().isDone(), is(true));
	}

	/**
	 * Tests that terminating the Dispatcher will wait for all JobMasters to be terminated.
	 */
	@Test
	public void testDispatcherTerminationWaitsForJobMasterTerminations() throws Exception {
		final TestingJobManagerRunnerFactory jobManagerRunnerFactory = startDispatcherAndSubmitJob(1);

		final CompletableFuture<Void> dispatcherTerminationFuture = dispatcher.closeAsync();

		try {
			dispatcherTerminationFuture.get(10L, TimeUnit.MILLISECONDS);
			fail("We should not terminate before all running JobMasters have terminated.");
		} catch (TimeoutException ignored) {
			// expected
		} finally {
			jobManagerRunnerFactory.takeCreatedJobManagerRunner().completeTerminationFuture();
		}

		dispatcherTerminationFuture.get();
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
		jobGraphWriter = TestingJobGraphStore.newBuilder()
			.setRemoveJobGraphConsumer(
				ignored -> {
					throw new Exception("Failed to Remove future");
				})
			.withAutomaticStart()
			.build();

		final TestingJobManagerRunnerFactory jobManagerRunnerFactory = startDispatcherAndSubmitJob();

		ArchivedExecutionGraph executionGraph = new ArchivedExecutionGraphBuilder()
			.setJobID(jobId)
			.setState(JobStatus.CANCELED)
			.build();

		final TestingJobManagerRunner testingJobManagerRunner = jobManagerRunnerFactory.takeCreatedJobManagerRunner();
		testingJobManagerRunner.completeResultFuture(executionGraph);

		assertThat(cleanupJobFuture.get(), equalTo(jobId));
		assertThat(deleteAllHABlobsFuture.isDone(), is(false));
	}

	@Test
	public void testHABlobsAreRemovedIfHAJobGraphRemovalSucceeds() throws Exception {
		final TestingJobManagerRunnerFactory jobManagerRunnerFactory = startDispatcherAndSubmitJob();

		ArchivedExecutionGraph executionGraph = new ArchivedExecutionGraphBuilder()
			.setJobID(jobId)
			.setState(JobStatus.CANCELED)
			.build();

		final TestingJobManagerRunner testingJobManagerRunner = jobManagerRunnerFactory.takeCreatedJobManagerRunner();
		testingJobManagerRunner.completeResultFuture(executionGraph);

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

	private class FailingJobManagerRunnerFactory implements JobManagerRunnerFactory {
		private final Exception testException;

		public FailingJobManagerRunnerFactory(FlinkException testException) {
			this.testException = testException;
		}

		@Override
		public JobManagerRunner createJobManagerRunner(JobGraph jobGraph, Configuration configuration, RpcService rpcService, HighAvailabilityServices highAvailabilityServices, HeartbeatServices heartbeatServices, JobManagerSharedServices jobManagerServices, JobManagerJobMetricGroupFactory jobManagerJobMetricGroupFactory, FatalErrorHandler fatalErrorHandler) throws Exception {
			throw testException;
		}
	}
}
