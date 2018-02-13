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
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.JobManagerRunner;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rest.handler.legacy.utils.ArchivedExecutionGraphBuilder;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.category.Flip6;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;

/**
 * Tests for the {@link MiniDispatcher}.
 */
@Category(Flip6.class)
public class MiniDispatcherTest extends TestLogger {

	private static final JobGraph jobGraph = new JobGraph();

	private static final Time timeout = Time.seconds(10L);

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static TestingRpcService rpcService;

	private static Configuration configuration;

	private static BlobServer blobServer;

	private MiniDispatcher miniDispatcher;

	private CompletableFuture<JobGraph> jobGraphFuture;

	private TestingLeaderElectionService dispatcherLeaderElectionService;

	@BeforeClass
	public static void setupClass() throws IOException {
		rpcService = new TestingRpcService();
		configuration = new Configuration();

		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

		blobServer = new BlobServer(configuration, new VoidBlobStore());
	}

	@Before
	public void setup() throws Exception {
		dispatcherLeaderElectionService = new TestingLeaderElectionService();
		final TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		final TestingResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();
		final HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 1000L);
		final ArchivedExecutionGraphStore archivedExecutionGraphStore = new MemoryArchivedExecutionGraphStore();
		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		highAvailabilityServices.setDispatcherLeaderElectionService(dispatcherLeaderElectionService);

		jobGraphFuture = new CompletableFuture<>();
		final TestingJobManagerRunnerFactory testingJobManagerRunnerFactory = new TestingJobManagerRunnerFactory(jobGraphFuture);

		miniDispatcher = new MiniDispatcher(
			rpcService,
			UUID.randomUUID().toString(),
			configuration,
			highAvailabilityServices,
			resourceManagerGateway,
			blobServer,
			heartbeatServices,
			NoOpMetricRegistry.INSTANCE,
			archivedExecutionGraphStore,
			testingJobManagerRunnerFactory,
			testingFatalErrorHandler,
			null,
			jobGraph,
			ClusterEntrypoint.ExecutionMode.DETACHED);

		miniDispatcher.start();
	}

	@After
	public void teardown() throws InterruptedException, ExecutionException, TimeoutException {
		if (miniDispatcher != null) {
			RpcUtils.terminateRpcEndpoint(miniDispatcher, timeout);
			miniDispatcher = null;
		}
	}

	@AfterClass
	public static void teardownClass() throws IOException {
		blobServer.close();
		rpcService.stopService();
	}

	/**
	 * Tests that the {@link MiniDispatcher} recovers the single job with which it
	 * was started.
	 */
	@Test
	public void testSingleJobRecovery() throws Exception {
		// wait until the Dispatcher is the leader
		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		final JobGraph actualJobGraph = jobGraphFuture.get();

		assertThat(actualJobGraph.getJobID(), is(jobGraph.getJobID()));
	}

	/**
	 * Tests that in detached mode, the {@link MiniDispatcher} will terminate after the job
	 * has completed.
	 */
	@Test
	public void testTerminationAfterJobCompletion() throws Exception {
		final Dispatcher.DispatcherOnCompleteActions completeActions = miniDispatcher.new DispatcherOnCompleteActions(jobGraph.getJobID());

		final ArchivedExecutionGraph archivedExecutionGraph = new ArchivedExecutionGraphBuilder()
			.setJobID(jobGraph.getJobID())
			.setState(JobStatus.FINISHED)
			.build();

		// wait until the Dispatcher is the leader
		dispatcherLeaderElectionService.isLeader(UUID.randomUUID()).get();

		// wait until we have submitted the job
		jobGraphFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);

		completeActions.jobReachedGloballyTerminalState(archivedExecutionGraph);

		final CompletableFuture<Boolean> terminationFuture = miniDispatcher.getTerminationFuture();

		// wait until we terminate
		terminationFuture.get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	private static final class TestingJobManagerRunnerFactory implements Dispatcher.JobManagerRunnerFactory {

		private final CompletableFuture<JobGraph> jobGraphFuture;

		private TestingJobManagerRunnerFactory(CompletableFuture<JobGraph> jobGraphFuture) {
			this.jobGraphFuture = jobGraphFuture;
		}

		@Override
		public JobManagerRunner createJobManagerRunner(ResourceID resourceId, JobGraph jobGraph, Configuration configuration, RpcService rpcService, HighAvailabilityServices highAvailabilityServices, HeartbeatServices heartbeatServices, BlobServer blobServer, JobManagerSharedServices jobManagerSharedServices, MetricRegistry metricRegistry, OnCompletionActions onCompleteActions, FatalErrorHandler fatalErrorHandler, @Nullable String restAddress) throws Exception {
			jobGraphFuture.complete(jobGraph);

			return mock(JobManagerRunner.class);
		}
	}

}
