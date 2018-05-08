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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.jobmaster.factories.JobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@link MiniDispatcher}.
 */
@Category(New.class)
public class MiniDispatcherTest extends TestLogger {

	private static final Time timeout = Time.seconds(10L);

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static JobGraph jobGraph;

	private static ArchivedExecutionGraph archivedExecutionGraph;

	private static TestingRpcService rpcService;

	private static Configuration configuration;

	private static BlobServer blobServer;

	private final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

	private final HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 1000L);

	private final ArchivedExecutionGraphStore archivedExecutionGraphStore = new MemoryArchivedExecutionGraphStore();

	private CompletableFuture<JobGraph> jobGraphFuture;

	private CompletableFuture<ArchivedExecutionGraph> resultFuture;

	private TestingLeaderElectionService dispatcherLeaderElectionService;

	private TestingHighAvailabilityServices highAvailabilityServices;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	private TestingJobManagerRunnerFactory testingJobManagerRunnerFactory;

	@BeforeClass
	public static void setupClass() throws IOException {
		jobGraph = new JobGraph();

		archivedExecutionGraph = new ArchivedExecutionGraphBuilder()
			.setJobID(jobGraph.getJobID())
			.setState(JobStatus.FINISHED)
			.build();

		rpcService = new TestingRpcService();
		configuration = new Configuration();

		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		blobServer = new BlobServer(configuration, new VoidBlobStore());
	}

	@Before
	public void setup() throws Exception {
		dispatcherLeaderElectionService = new TestingLeaderElectionService();
		highAvailabilityServices = new TestingHighAvailabilityServices();
		testingFatalErrorHandler = new TestingFatalErrorHandler();

		highAvailabilityServices.setDispatcherLeaderElectionService(dispatcherLeaderElectionService);

		jobGraphFuture = new CompletableFuture<>();
		resultFuture = new CompletableFuture<>();

		testingJobManagerRunnerFactory = new TestingJobManagerRunnerFactory(jobGraphFuture, resultFuture);
	}

	@After
	public void teardown() throws Exception {
		testingFatalErrorHandler.rethrowError();
	}

	@AfterClass
	public static void teardownClass() throws IOException, InterruptedException, ExecutionException, TimeoutException {
		if (blobServer != null) {
			blobServer.close();
		}

		if (rpcService != null) {
			RpcUtils.terminateRpcService(rpcService, timeout);
		}
	}

	/**
	 * Tests that the {@link MiniDispatcher} recovers the single job with which it
	 * was started.
	 */
	@Test
	public void testSingleJobRecovery() throws Exception {
		final MiniDispatcher miniDispatcher = createMiniDispatcher(ClusterEntrypoint.ExecutionMode.DETACHED);

		miniDispatcher.start();

		try {
			// wait until the Dispatcher is the leader
			dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

			final JobGraph actualJobGraph = jobGraphFuture.get();

			assertThat(actualJobGraph.getJobID(), is(jobGraph.getJobID()));
		} finally {
			RpcUtils.terminateRpcEndpoint(miniDispatcher, timeout);
		}
	}

	/**
	 * Tests that in detached mode, the {@link MiniDispatcher} will complete the future that
	 * signals job termination.
	 */
	@Test
	public void testTerminationAfterJobCompletion() throws Exception {
		final MiniDispatcher miniDispatcher = createMiniDispatcher(ClusterEntrypoint.ExecutionMode.DETACHED);

		miniDispatcher.start();

		try {
			// wait until the Dispatcher is the leader
			dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

			// wait until we have submitted the job
			jobGraphFuture.get();

			resultFuture.complete(archivedExecutionGraph);

			// wait until we terminate
			miniDispatcher.getJobTerminationFuture().get();
		} finally {
			RpcUtils.terminateRpcEndpoint(miniDispatcher, timeout);
		}
	}

	/**
	 * Tests that the {@link MiniDispatcher} only terminates in {@link ClusterEntrypoint.ExecutionMode#NORMAL}
	 * after it has served the {@link org.apache.flink.runtime.jobmaster.JobResult} once.
	 */
	@Test
	public void testJobResultRetrieval() throws Exception {
		final MiniDispatcher miniDispatcher = createMiniDispatcher(ClusterEntrypoint.ExecutionMode.NORMAL);

		miniDispatcher.start();

		try {
			// wait until the Dispatcher is the leader
			dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

			// wait until we have submitted the job
			jobGraphFuture.get();

			resultFuture.complete(archivedExecutionGraph);

			assertFalse(miniDispatcher.getTerminationFuture().isDone());

			final DispatcherGateway dispatcherGateway = miniDispatcher.getSelfGateway(DispatcherGateway.class);

			final CompletableFuture<JobResult> jobResultFuture = dispatcherGateway.requestJobResult(jobGraph.getJobID(), timeout);

			final JobResult jobResult = jobResultFuture.get();

			assertThat(jobResult.getJobId(), is(jobGraph.getJobID()));
		}
		finally {
			RpcUtils.terminateRpcEndpoint(miniDispatcher, timeout);
		}
	}

	// --------------------------------------------------------
	// Utilities
	// --------------------------------------------------------

	@Nonnull
	private MiniDispatcher createMiniDispatcher(ClusterEntrypoint.ExecutionMode executionMode) throws Exception {
		return new MiniDispatcher(
			rpcService,
			UUID.randomUUID().toString(),
			configuration,
			highAvailabilityServices,
			resourceManagerGateway,
			blobServer,
			heartbeatServices,
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup(),
			null,
			archivedExecutionGraphStore,
			testingJobManagerRunnerFactory,
			testingFatalErrorHandler,
			null,
			jobGraph,
			executionMode);
	}

	private static final class TestingJobManagerRunnerFactory implements Dispatcher.JobManagerRunnerFactory {

		private final CompletableFuture<JobGraph> jobGraphFuture;
		private final CompletableFuture<ArchivedExecutionGraph> resultFuture;

		private TestingJobManagerRunnerFactory(CompletableFuture<JobGraph> jobGraphFuture, CompletableFuture<ArchivedExecutionGraph> resultFuture) {
			this.jobGraphFuture = jobGraphFuture;
			this.resultFuture = resultFuture;
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
			jobGraphFuture.complete(jobGraph);

			final JobManagerRunner mock = mock(JobManagerRunner.class);
			when(mock.getResultFuture()).thenReturn(resultFuture);
			when(mock.closeAsync()).thenReturn(CompletableFuture.completedFuture(null));

			return mock;
		}
	}

}
