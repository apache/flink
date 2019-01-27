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

package org.apache.flink.runtime.update;

import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategyFactory;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraph;
import org.apache.flink.runtime.jobmanager.SubmittedJobGraphStore;
import org.apache.flink.runtime.jobmaster.JobManagerSharedServices;
import org.apache.flink.runtime.jobmaster.JobMaster;
import org.apache.flink.runtime.jobmaster.JobMasterConfiguration;
import org.apache.flink.runtime.jobmaster.JobMasterGateway;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.jobmaster.TestingJobManagerSharedServicesBuilder;
import org.apache.flink.runtime.jobmaster.factories.UnregisteredJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultSlotPoolFactory;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testutils.InMemorySubmittedJobGraphStore;
import org.apache.flink.runtime.update.action.JobGraphReplaceAction;
import org.apache.flink.runtime.update.action.JobUpdateAction;
import org.apache.flink.runtime.update.action.JobVertexRescaleAction;
import org.apache.flink.runtime.update.action.JobVertexResourcesUpdateAction;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

/**
 * Tests for job update.
 */
public class JobUpdateTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final Time testingTimeout = Time.seconds(10L);

	private static final long fastHeartbeatInterval = 1L;
	private static final long fastHeartbeatTimeout = 5L;

	private static final long heartbeatInterval = 1000L;
	private static final long heartbeatTimeout = 5000L;

	private static final JobGraph jobGraph = new JobGraph();

	private static TestingRpcService rpcService;

	private static HeartbeatServices fastHeartbeatServices;

	private static HeartbeatServices heartbeatServices;

	private BlobServer blobServer;

	private Configuration configuration;

	private ResourceID jmResourceId;

	private JobMasterId jobMasterId;

	private TestingHighAvailabilityServices haServices;

	private SettableLeaderRetrievalService rmLeaderRetrievalService;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	private SubmittedJobGraphStore submittedJobGraphStore;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();

		fastHeartbeatServices = new TestingHeartbeatServices(fastHeartbeatInterval, fastHeartbeatTimeout, rpcService.getScheduledExecutor());
		heartbeatServices = new TestingHeartbeatServices(heartbeatInterval, heartbeatTimeout, rpcService.getScheduledExecutor());
	}

	@Before
	public void setup() throws Exception {
		configuration = new Configuration();
		haServices = new TestingHighAvailabilityServices();
		jobMasterId = JobMasterId.generate();
		jmResourceId = ResourceID.generate();

		testingFatalErrorHandler = new TestingFatalErrorHandler();

		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

		rmLeaderRetrievalService = new SettableLeaderRetrievalService(
			null,
			null);
		haServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);

		submittedJobGraphStore = new InMemorySubmittedJobGraphStore();
		submittedJobGraphStore.start(null);
		haServices.setSubmittedJobGraphStore(submittedJobGraphStore);

		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
		blobServer = new BlobServer(configuration, new VoidBlobStore());

		blobServer.start();
	}

	@After
	public void teardown() throws Exception {
		if (testingFatalErrorHandler != null) {
			testingFatalErrorHandler.rethrowError();
		}

		if (blobServer != null) {
			blobServer.close();
		}

		rpcService.clearGateways();
	}

	@AfterClass
	public static void teardownClass() {
		if (rpcService != null) {
			rpcService.stopService();
			rpcService = null;
		}
	}

	@Test
	public void testReplacingJobGraph() throws Exception {
		// build one node JobGraph
		JobVertex source = new JobVertex("vertex1");
		source.setParallelism(1);
		source.setInvokableClass(AbstractInvokable.class);
		final JobGraph jobGraph = new JobGraph(source);
		jobGraph.setAllowQueuedScheduling(true);

		// build another one node JobGraph
		JobVertex source2 = new JobVertex("vertex2");
		source2.setParallelism(2);
		source2.setInvokableClass(AbstractInvokable.class);
		final JobGraph jobGraph2 = new JobGraph(source2);
		jobGraph2.setAllowQueuedScheduling(true);

		configuration.setLong(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
		configuration.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s");
		final JobManagerSharedServices jobManagerSharedServices =
				new TestingJobManagerSharedServicesBuilder()
						.setRestartStrategyFactory(RestartStrategyFactory.createRestartStrategyFactory(configuration))
						.build();

		submittedJobGraphStore.putJobGraph(new SubmittedJobGraph(jobGraph, null));

		final JobMaster jobMaster = createJobMaster(
				configuration,
				jobGraph,
				haServices,
				jobManagerSharedServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			ExecutionGraph eg = jobMaster.getExecutionGraph();
			List<ExecutionJobVertex> ejvs = new ArrayList(eg.getAllVertices().values());
			assertEquals(ejvs.size(), 1);
			assertEquals(ejvs.get(0).getParallelism(), 1);
			assertEquals(ejvs.get(0).getJobVertexId(), source.getID());

			ExecutionGraphTestUtils.waitForAllExecutionsPredicate(
					eg,
					ExecutionGraphTestUtils.isInExecutionState(ExecutionState.SCHEDULED),
					testingTimeout.toMilliseconds());

			// Trigger job update
			List<JobUpdateAction> actions = new ArrayList<>();
			actions.add(new JobGraphReplaceAction(jobGraph2));
			JobUpdateRequest request = new JobUpdateRequest(actions);
			CompletableFuture<Acknowledge> updateFuture = jobMasterGateway.updateJob(request, testingTimeout);
			updateFuture.get(testingTimeout.getSize(), testingTimeout.getUnit());

			// Verify if the new JobGraph is working
			ExecutionGraph eg2 = jobMaster.getExecutionGraph();
			List<ExecutionJobVertex> ejvs2 = new ArrayList(eg2.getAllVertices().values());
			assertEquals(ejvs2.size(), 1);
			assertEquals(ejvs2.get(0).getParallelism(), 2);
			assertEquals(ejvs2.get(0).getJobVertexId(), source2.getID());

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testRescalingVertex() throws Exception {
		// build one node JobGraph
		JobVertex source = new JobVertex("vertex1");
		source.setParallelism(1);
		source.setInvokableClass(AbstractInvokable.class);
		final JobGraph jobGraph = new JobGraph(source);
		jobGraph.setAllowQueuedScheduling(true);

		configuration.setLong(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
		configuration.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s");
		final JobManagerSharedServices jobManagerSharedServices =
				new TestingJobManagerSharedServicesBuilder()
						.setRestartStrategyFactory(RestartStrategyFactory.createRestartStrategyFactory(configuration))
						.build();

		submittedJobGraphStore.putJobGraph(new SubmittedJobGraph(jobGraph, null));

		final JobMaster jobMaster = createJobMaster(
				configuration,
				jobGraph,
				haServices,
				jobManagerSharedServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			ExecutionGraph eg = jobMaster.getExecutionGraph();
			List<ExecutionJobVertex> ejvs = new ArrayList(eg.getAllVertices().values());
			assertEquals(ejvs.size(), 1);
			assertEquals(ejvs.get(0).getParallelism(), 1);
			assertEquals(ejvs.get(0).getJobVertexId(), source.getID());

			ExecutionGraphTestUtils.waitForAllExecutionsPredicate(
					eg,
					ExecutionGraphTestUtils.isInExecutionState(ExecutionState.SCHEDULED),
					testingTimeout.toMilliseconds());

			// Trigger job update
			List<JobUpdateAction> actions = new ArrayList<>();
			actions.add(new JobVertexRescaleAction(source.getID(), 5));
			JobUpdateRequest request = new JobUpdateRequest(actions);
			CompletableFuture<Acknowledge> updateFuture = jobMasterGateway.updateJob(request, testingTimeout);
			updateFuture.get(testingTimeout.getSize(), testingTimeout.getUnit());

			// Verify if the new JobGraph is working
			ExecutionGraph eg2 = jobMaster.getExecutionGraph();
			List<ExecutionJobVertex> ejvs2 = new ArrayList(eg2.getAllVertices().values());
			assertEquals(ejvs2.size(), 1);
			assertEquals(ejvs2.get(0).getParallelism(), 5);
			assertEquals(ejvs2.get(0).getJobVertexId(), source.getID());

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testUpdatingVertexResources() throws Exception {
		// build one node JobGraph
		JobVertex source = new JobVertex("vertex1");
		source.setParallelism(1);
		source.setInvokableClass(AbstractInvokable.class);
		ResourceSpec resourceSpec = new ResourceSpec.Builder().setCpuCores(1).setHeapMemoryInMB(100).build();
		source.setResources(resourceSpec, resourceSpec);
		final JobGraph jobGraph = new JobGraph(source);
		jobGraph.setAllowQueuedScheduling(true);

		configuration.setLong(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 1);
		configuration.setString(ConfigConstants.RESTART_STRATEGY_FIXED_DELAY_DELAY, "0 s");
		final JobManagerSharedServices jobManagerSharedServices =
				new TestingJobManagerSharedServicesBuilder()
						.setRestartStrategyFactory(RestartStrategyFactory.createRestartStrategyFactory(configuration))
						.build();

		submittedJobGraphStore.putJobGraph(new SubmittedJobGraph(jobGraph, null));

		final JobMaster jobMaster = createJobMaster(
				configuration,
				jobGraph,
				haServices,
				jobManagerSharedServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			ExecutionGraph eg = jobMaster.getExecutionGraph();
			List<ExecutionJobVertex> ejvs = new ArrayList(eg.getAllVertices().values());
			assertEquals(ejvs.size(), 1);
			assertEquals(ejvs.get(0).getJobVertex().getPreferredResources().getCpuCores(), 1, 0.00001);
			assertEquals(ejvs.get(0).getJobVertex().getPreferredResources().getHeapMemory(), 100);
			assertEquals(ejvs.get(0).getJobVertexId(), source.getID());

			ExecutionGraphTestUtils.waitForAllExecutionsPredicate(
					eg,
					ExecutionGraphTestUtils.isInExecutionState(ExecutionState.SCHEDULED),
					testingTimeout.toMilliseconds());

			// Trigger job update
			List<JobUpdateAction> actions = new ArrayList<>();
			ResourceSpec resourceSpec2 = new ResourceSpec.Builder().setCpuCores(2).setHeapMemoryInMB(200).build();
			actions.add(new JobVertexResourcesUpdateAction(source.getID(), resourceSpec2));
			JobUpdateRequest request = new JobUpdateRequest(actions);
			CompletableFuture<Acknowledge> updateFuture = jobMasterGateway.updateJob(request, testingTimeout);
			updateFuture.get(testingTimeout.getSize(), testingTimeout.getUnit());

			// Verify if the new JobGraph is working
			ExecutionGraph eg2 = jobMaster.getExecutionGraph();
			List<ExecutionJobVertex> ejvs2 = new ArrayList(eg2.getAllVertices().values());
			assertEquals(ejvs2.size(), 1);
			assertEquals(ejvs2.get(0).getJobVertex().getPreferredResources().getCpuCores(), 2, 0.00001);
			assertEquals(ejvs2.get(0).getJobVertex().getPreferredResources().getHeapMemory(), 200);
			assertEquals(ejvs2.get(0).getJobVertexId(), source.getID());

			// check if a concurrent error occurred
			testingFatalErrorHandler.rethrowError();

		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Nonnull
	private JobMaster createJobMaster(
			Configuration configuration,
			JobGraph jobGraph,
			HighAvailabilityServices highAvailabilityServices,
			JobManagerSharedServices jobManagerSharedServices) throws Exception {
		return createJobMaster(
			configuration,
			jobGraph,
			highAvailabilityServices,
			jobManagerSharedServices,
			fastHeartbeatServices);
	}

	@Nonnull
	private JobMaster createJobMaster(
			Configuration configuration,
			JobGraph jobGraph,
			HighAvailabilityServices highAvailabilityServices,
			JobManagerSharedServices jobManagerSharedServices,
			HeartbeatServices heartbeatServices) throws Exception {

		final JobMasterConfiguration jobMasterConfiguration = JobMasterConfiguration.fromConfiguration(configuration);

		return new JobMaster(
			rpcService,
			jobMasterConfiguration,
			jmResourceId,
			jobGraph,
			highAvailabilityServices,
			DefaultSlotPoolFactory.fromConfiguration(configuration, rpcService),
			jobManagerSharedServices,
			heartbeatServices,
			blobServer,
			UnregisteredJobManagerJobMetricGroupFactory.INSTANCE,
			new NoOpOnCompletionActions(),
			testingFatalErrorHandler,
			JobUpdateTest.class.getClassLoader(),
			highAvailabilityServices.getSubmittedJobGraphStore());
	}

	/**
	 * No op implementation of {@link OnCompletionActions}.
	 */
	private static final class NoOpOnCompletionActions implements OnCompletionActions {

		@Override
		public void jobReachedGloballyTerminalState(ArchivedExecutionGraph executionGraph) {

		}

		@Override
		public void jobFinishedByOther() {

		}

		@Override
		public void jobMasterFailed(Throwable cause) {

		}
	}
}
