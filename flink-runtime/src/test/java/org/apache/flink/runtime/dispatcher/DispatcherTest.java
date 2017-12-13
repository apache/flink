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
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.RunningJobsRegistry;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerServices;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.testutils.InMemorySubmittedJobGraphStore;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestName;
import org.mockito.Mockito;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

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

	private JobGraph jobGraph;

	private TestingFatalErrorHandler fatalErrorHandler;

	private SubmittedJobGraphStore submittedJobGraphStore;

	private TestingLeaderElectionService dispatcherLeaderElectionService;

	private TestingLeaderElectionService jobMasterLeaderElectionService;

	private RunningJobsRegistry runningJobsRegistry;

	/** Instance under test. */
	private TestingDispatcher dispatcher;

	@BeforeClass
	public static void setup() {
		rpcService = new TestingRpcService();
	}

	@AfterClass
	public static void teardown() {
		if (rpcService != null) {
			rpcService.stopService();

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
		submittedJobGraphStore = spy(new InMemorySubmittedJobGraphStore());

		dispatcherLeaderElectionService = new TestingLeaderElectionService();
		jobMasterLeaderElectionService = new TestingLeaderElectionService();

		final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
		haServices.setDispatcherLeaderElectionService(dispatcherLeaderElectionService);
		haServices.setSubmittedJobGraphStore(submittedJobGraphStore);
		haServices.setJobMasterLeaderElectionService(TEST_JOB_ID, jobMasterLeaderElectionService);
		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());
		haServices.setResourceManagerLeaderRetriever(new TestingLeaderRetrievalService());
		runningJobsRegistry = haServices.getRunningJobsRegistry();

		final Configuration blobServerConfig = new Configuration();
		blobServerConfig.setString(
			BlobServerOptions.STORAGE_DIRECTORY,
			temporaryFolder.newFolder().getAbsolutePath());

		dispatcher = new TestingDispatcher(
			rpcService,
			Dispatcher.DISPATCHER_NAME + '_' + name.getMethodName(),
			new Configuration(),
			haServices,
			mock(ResourceManagerGateway.class),
			new BlobServer(blobServerConfig, new VoidBlobStore()),
			heartbeatServices,
			NoOpMetricRegistry.INSTANCE,
			fatalErrorHandler,
			TEST_JOB_ID);

		dispatcher.start();
	}

	@After
	public void tearDown() throws Exception {
		try {
			fatalErrorHandler.rethrowError();
		} finally {
			RpcUtils.terminateRpcEndpoint(dispatcher, TIMEOUT);
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
		UUID expectedLeaderSessionId = UUID.randomUUID();

		assertNull(dispatcherLeaderElectionService.getConfirmationFuture());

		dispatcherLeaderElectionService.isLeader(expectedLeaderSessionId);

		UUID actualLeaderSessionId = dispatcherLeaderElectionService.getConfirmationFuture()
			.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

		assertEquals(expectedLeaderSessionId, actualLeaderSessionId);

		verify(submittedJobGraphStore, Mockito.timeout(TIMEOUT.toMilliseconds()).atLeast(1)).getJobIds();
	}

	/**
	 * Test callbacks from
	 * {@link org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore.SubmittedJobGraphListener}.
	 */
	@Test
	public void testSubmittedJobGraphListener() throws Exception {
		dispatcher.recoverJobsEnabled.set(false);

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
		dispatcher.submitJobLatch.await();
		assertThat(dispatcherGateway.listJobs(TIMEOUT).get(), hasSize(1));
	}

	private static class TestingDispatcher extends Dispatcher {

		private final JobID expectedJobId;

		private final CountDownLatch submitJobLatch = new CountDownLatch(2);

		/**
		 * Controls whether existing jobs in {@link SubmittedJobGraphStore} should be recovered
		 * when {@link TestingDispatcher} is granted leadership.
		 * */
		private final AtomicBoolean recoverJobsEnabled = new AtomicBoolean(true);

		private TestingDispatcher(
				RpcService rpcService,
				String endpointId,
				Configuration configuration,
				HighAvailabilityServices highAvailabilityServices,
				ResourceManagerGateway resourceManagerGateway,
				BlobServer blobServer,
				HeartbeatServices heartbeatServices,
				MetricRegistry metricRegistry,
				FatalErrorHandler fatalErrorHandler,
				JobID expectedJobId) throws Exception {
			super(
				rpcService,
				endpointId,
				configuration,
				highAvailabilityServices,
				resourceManagerGateway,
				blobServer,
				heartbeatServices,
				metricRegistry,
				fatalErrorHandler,
				null);

			this.expectedJobId = expectedJobId;
		}

		@Override
		protected JobManagerRunner createJobManagerRunner(
				ResourceID resourceId,
				JobGraph jobGraph,
				Configuration configuration,
				RpcService rpcService,
				HighAvailabilityServices highAvailabilityServices,
				HeartbeatServices heartbeatServices,
				JobManagerServices jobManagerServices,
				MetricRegistry metricRegistry,
				OnCompletionActions onCompleteActions,
				FatalErrorHandler fatalErrorHandler) throws Exception {
			assertEquals(expectedJobId, jobGraph.getJobID());

			return new JobManagerRunner(resourceId, jobGraph, configuration, rpcService,
				highAvailabilityServices, heartbeatServices, jobManagerServices, metricRegistry,
				onCompleteActions, fatalErrorHandler, null);
		}

		@Override
		public CompletableFuture<Acknowledge> submitJob(final JobGraph jobGraph, final Time timeout) {
			final CompletableFuture<Acknowledge> submitJobFuture = super.submitJob(jobGraph, timeout);

			try {
				submitJobFuture.get();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}

			submitJobLatch.countDown();
			return submitJobFuture;
		}

		@Override
		void recoverJobs() {
			if (recoverJobsEnabled.get()) {
				super.recoverJobs();
			}
		}
	}
}
