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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.io.InputSplit;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.core.io.InputSplitSource;
import org.apache.flink.runtime.blob.BlobServer;
import org.apache.flink.runtime.blob.VoidBlobStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.event.ExecutionVertexFailoverEvent;
import org.apache.flink.runtime.event.ExecutionVertexStateChangedEvent;
import org.apache.flink.runtime.event.ResultPartitionConsumableEvent;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ArchivedExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.restart.FixedDelayRestartStrategy;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.ExecutionVertexID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobStatus;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmanager.StandaloneSubmittedJobGraphStore;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.factories.UnregisteredJobManagerJobMetricGroupFactory;
import org.apache.flink.runtime.jobmaster.failover.MemoryOperationLogStore;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultSlotPoolFactory;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.schedule.EagerSchedulingPlugin;
import org.apache.flink.runtime.schedule.StepwiseSchedulingPlugin;
import org.apache.flink.runtime.taskexecutor.TaskExecutionStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorReportResponse;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the failover of {@link JobMaster}.
 */
public class JobMasterFailoverTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final Time testingTimeout = Time.seconds(10L);

	private static final long heartbeatInterval = 1000L;
	private static final long heartbeatTimeout = 5000L;

	private static TestingRpcService rpcService;

	private static HeartbeatServices heartbeatServices;

	private BlobServer blobServer;

	private Configuration configuration;

	private ResourceID jmResourceId;

	private JobMasterId jobMasterId;

	private TestingHighAvailabilityServices haServices;

	private SettableLeaderRetrievalService rmLeaderRetrievalService;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();

		heartbeatServices = new TestingHeartbeatServices(heartbeatInterval, heartbeatTimeout, rpcService.getScheduledExecutor());
	}

	@Before
	public void setup() throws IOException {
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
		haServices.setSubmittedJobGraphStore(new StandaloneSubmittedJobGraphStore());

		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
		configuration.setString(JobManagerOptions.OPERATION_LOG_STORE, "memory");
		configuration.setString(ScheduleMode.class.getName(), "EAGER");
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
	public void testFailToReplayLogWillFallBackToStartANewJob() throws Exception {
		final JobGraph producerConsumerJobGraph = producerConsumerJobGraph(configuration, ResultPartitionType.BLOCKING);
		final JobMaster jobMaster = createJobMaster(
			configuration,
			producerConsumerJobGraph,
			haServices,
			new TestingJobManagerSharedServicesBuilder().build(),
			heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			//clear the log genereated by other cases;
			jobMaster.getExecutionGraph().getGraphManager().reset();

			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
			testingResourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			rpcService.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			final CompletableFuture<TaskDeploymentDescriptor> tddFuture = new CompletableFuture<>();
			final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
				.setSubmitTaskConsumer((taskDeploymentDescriptor, jobMasterId) -> {
					tddFuture.complete(taskDeploymentDescriptor);
					return CompletableFuture.completedFuture(Acknowledge.get());
				})
				.createTestingTaskExecutorGateway();
			rpcService.registerGateway(testingTaskExecutorGateway.getAddress(), testingTaskExecutorGateway);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			rmLeaderRetrievalService.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final AllocationID allocationId = allocationIdFuture.get();

			final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(testingTaskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN);

			final Collection<SlotOffer> slotOffers = jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout).get();

			assertThat(slotOffers, hasSize(1));
			assertThat(slotOffers, contains(slotOffer));

			// obtain tdd for the result partition ids
			final TaskDeploymentDescriptor tdd = tddFuture.get();

			final ExecutionAttemptID executionAttemptId = tdd.getExecutionAttemptId();
			final ExecutionVertexID executionVertexID = jobMaster.getExecutionGraph().getRegisteredExecutions()
				.get(executionAttemptId).getVertex().getExecutionVertexID();

			// finish the producer task
			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
				producerConsumerJobGraph.getJobID(), executionAttemptId, ExecutionState.FINISHED)).get();

			// disable the operationLogStore, thus replay would fail.
			MemoryOperationLogStore.disable();

			// Another master, but fail to replay the log as the use the same log store.
			final JobMaster newJobMaster = createJobMaster(
				configuration,
				producerConsumerJobGraph,
				haServices,
				new TestingJobManagerSharedServicesBuilder().build(),
				heartbeatServices);

			// let the new job master replay log
			newJobMaster.reconcile();

			// check that we start a new job.
			final ExecutionJobVertex createdEJV = newJobMaster.getExecutionGraph().getAllVertices()
				.get(executionVertexID.getJobVertexID());
			assertEquals(ExecutionState.CREATED, createdEJV.getAggregateState());
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that a JobMaster will replay the operation after started.
	 */
	@Test
	public void testReplayLog() throws Exception {
		TestingEagerSchedulingPlugin.init();
		configuration.setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, TestingEagerSchedulingPlugin.class.getName());
		final JobGraph producerConsumerJobGraph = producerConsumerJobGraph(configuration, ResultPartitionType.BLOCKING);
		final JobMaster jobMaster = createJobMaster(
				configuration,
				producerConsumerJobGraph,
				haServices,
				new TestingJobManagerSharedServicesBuilder().build(),
				heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			//clear the log genereated by other cases;
			jobMaster.getExecutionGraph().getGraphManager().reset();

			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
			testingResourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			rpcService.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			final CompletableFuture<TaskDeploymentDescriptor> tddFuture = new CompletableFuture<>();
			final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
					.setSubmitTaskConsumer((taskDeploymentDescriptor, jobMasterId) -> {
						tddFuture.complete(taskDeploymentDescriptor);
						return CompletableFuture.completedFuture(Acknowledge.get());
					})
					.createTestingTaskExecutorGateway();
			rpcService.registerGateway(testingTaskExecutorGateway.getAddress(), testingTaskExecutorGateway);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			rmLeaderRetrievalService.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final AllocationID allocationId = allocationIdFuture.get();

			final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(testingTaskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN);

			final Collection<SlotOffer> slotOffers = jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout).get();

			assertThat(slotOffers, hasSize(1));
			assertThat(slotOffers, contains(slotOffer));

			// obtain tdd for the result partition ids
			final TaskDeploymentDescriptor tdd = tddFuture.get();

			final ExecutionAttemptID executionAttemptId = tdd.getExecutionAttemptId();
			final ExecutionVertexID executionVertexID = jobMaster.getExecutionGraph().getRegisteredExecutions()
					.get(executionAttemptId).getVertex().getExecutionVertexID();
			JobVertexID theOther = null;
			for (ExecutionJobVertex ejv : jobMaster.getExecutionGraph().getAllVertices().values()) {
				if (ejv.getJobVertexId() != executionVertexID.getJobVertexID()) {
					theOther = ejv.getJobVertexId();
					break;
				}
			}

			// finish the producer task
			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					producerConsumerJobGraph.getJobID(), executionAttemptId, ExecutionState.FINISHED)).get();

			assertEquals(0, TestingEagerSchedulingPlugin.getCloseCount());
			assertEquals(1, TestingEagerSchedulingPlugin.getResetCount());
			if (!tdd.getProducedPartitions().isEmpty()) {
				assertEquals(1, TestingEagerSchedulingPlugin.getConsumableCount());
			}
			assertEquals(2, TestingEagerSchedulingPlugin.getDeployingCount());
			assertEquals(1, TestingEagerSchedulingPlugin.getFinishedCount());
			assertEquals(0, TestingEagerSchedulingPlugin.getRunningCount());

			// Another master can replay the log as the use the same log store.
			final JobMaster newJobMaster = createJobMaster(
					configuration,
					producerConsumerJobGraph,
					haServices,
					new TestingJobManagerSharedServicesBuilder().build(),
					heartbeatServices);

			// let the new job master replay log
			newJobMaster.reconcile();

			// check that the status are the same with before failover.
			ExecutionJobVertex finishedEJV = newJobMaster.getExecutionGraph().getAllVertices()
					.get(executionVertexID.getJobVertexID());
			assertEquals(ExecutionState.FINISHED, finishedEJV.getAggregateState());
			if (!tdd.getProducedPartitions().isEmpty()) {
				assertTrue(finishedEJV.getProducedDataSets()[0].getPartitions()[0].isConsumable());
			}
			assertEquals(ExecutionState.DEPLOYING, newJobMaster.getExecutionGraph().getAllVertices().get(theOther)
					.getTaskVertices()[0].getExecutionState());

			assertEquals(0, TestingEagerSchedulingPlugin.getCloseCount());
			assertEquals(1, TestingEagerSchedulingPlugin.getResetCount());
			if (!tdd.getProducedPartitions().isEmpty()) {
				assertEquals(2, TestingEagerSchedulingPlugin.getConsumableCount());
			}
			assertEquals(4, TestingEagerSchedulingPlugin.getDeployingCount());
			assertEquals(2, TestingEagerSchedulingPlugin.getFinishedCount());
			assertEquals(0, TestingEagerSchedulingPlugin.getRunningCount());
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that the JobMaster replay logs of a job that has failover history.
	 */
	@Test
	public void testReplayLogWithFailover() throws Exception {
		TestingEagerSchedulingPlugin.init();
		configuration.setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, TestingEagerSchedulingPlugin.class.getName());
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
		final JobGraph producerConsumerJobGraph = producerConsumerJobGraph(configuration, ResultPartitionType.BLOCKING);
		JobManagerSharedServices sharedServices = new TestingJobManagerSharedServicesBuilder()
				.setRestartStrategyFactory(new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(1, 0)).build();
		final JobMaster jobMaster = createJobMaster(
				configuration,
				producerConsumerJobGraph,
				haServices,
				sharedServices,
				heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			//clear the log genereated by other cases;
			jobMaster.getExecutionGraph().getGraphManager().reset();

			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
			testingResourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			rpcService.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			final CompletableFuture<TaskDeploymentDescriptor> tddFuture = new CompletableFuture<>();
			final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
					.setSubmitTaskConsumer((taskDeploymentDescriptor, jobMasterId) -> {
						tddFuture.complete(taskDeploymentDescriptor);
						return CompletableFuture.completedFuture(Acknowledge.get());
					})
					.createTestingTaskExecutorGateway();
			rpcService.registerGateway(testingTaskExecutorGateway.getAddress(), testingTaskExecutorGateway);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			rmLeaderRetrievalService.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final AllocationID allocationId = allocationIdFuture.get();

			final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(testingTaskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN);

			final Collection<SlotOffer> slotOffers = jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout).get();

			assertThat(slotOffers, hasSize(1));
			assertThat(slotOffers, contains(slotOffer));

			// obtain tdd for the result partition ids
			final TaskDeploymentDescriptor tdd = tddFuture.get();

			final ExecutionAttemptID executionAttemptId = tdd.getExecutionAttemptId();
			final ExecutionVertexID executionVertexID = jobMaster.getExecutionGraph().getRegisteredExecutions()
					.get(executionAttemptId).getVertex().getExecutionVertexID();
			JobVertexID theOtherJobVertexID = null;
			ExecutionAttemptID theOther = null;
			ExecutionVertex theOtherEV = null;
			for (Execution execution : jobMaster.getExecutionGraph().getRegisteredExecutions().values()) {
				if (execution.getAttemptId() != executionAttemptId) {
					theOther = execution.getAttemptId();
					theOtherJobVertexID = execution.getVertex().getJobvertexId();
					theOtherEV = execution.getVertex();
					break;
				}
			}

			// finish the producer task
			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					producerConsumerJobGraph.getJobID(), executionAttemptId, ExecutionState.FINISHED)).get();

			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					producerConsumerJobGraph.getJobID(), theOther, ExecutionState.FAILED)).get();

			ExecutionGraphTestUtils.waitUntilExecutionVertexState(theOtherEV, ExecutionState.DEPLOYING, 2000L);

			assertEquals(1, TestingEagerSchedulingPlugin.getFailoverCount());

			// Another master can replay the log as the use the same log store.
			final JobMaster newJobMaster = createJobMaster(
					configuration,
					producerConsumerJobGraph,
					haServices,
					new TestingJobManagerSharedServicesBuilder().build(),
					heartbeatServices);

			// let the new job master replay log
			newJobMaster.reconcile();

			// check that the status are the same with before failover.
			ExecutionJobVertex finishedEJV = newJobMaster.getExecutionGraph().getAllVertices()
					.get(executionVertexID.getJobVertexID());
			assertEquals(ExecutionState.FINISHED, finishedEJV.getAggregateState());
			if (!tdd.getProducedPartitions().isEmpty()) {
				assertTrue(finishedEJV.getProducedDataSets()[0].getPartitions()[0].isConsumable());
			}
			assertEquals(ExecutionState.DEPLOYING, newJobMaster.getExecutionGraph().getAllVertices()
					.get(theOtherJobVertexID).getTaskVertices()[0].getExecutionState());

			assertEquals(0, TestingEagerSchedulingPlugin.getCloseCount());
			assertEquals(1, TestingEagerSchedulingPlugin.getResetCount());
			if (!tdd.getProducedPartitions().isEmpty()) {
				assertEquals(2, TestingEagerSchedulingPlugin.getConsumableCount());
			}
			assertEquals(6, TestingEagerSchedulingPlugin.getDeployingCount());
			assertEquals(2, TestingEagerSchedulingPlugin.getFinishedCount());
			assertEquals(0, TestingEagerSchedulingPlugin.getRunningCount());
			assertEquals(2, TestingEagerSchedulingPlugin.getFailoverCount());
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that the JobMaster replay logs of a job that has a full restart history.
	 */
	@Test
	public void testReplayLogWithFullRestart() throws Exception {
		TestingEagerSchedulingPlugin.init();
		configuration.setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, TestingEagerSchedulingPlugin.class.getName());
		final JobGraph producerConsumerJobGraph = producerConsumerJobGraph(configuration, ResultPartitionType.BLOCKING);
		JobManagerSharedServices sharedServices = new TestingJobManagerSharedServicesBuilder()
				.setRestartStrategyFactory(new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(1, 0)).build();
		final JobMaster jobMaster = createJobMaster(
				configuration,
				producerConsumerJobGraph,
				haServices,
				sharedServices,
				heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			//clear the log genereated by other cases;
			jobMaster.getExecutionGraph().getGraphManager().reset();

			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
			testingResourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			rpcService.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			final CompletableFuture<TaskDeploymentDescriptor> tddFuture = new CompletableFuture<>();
			final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
					.setSubmitTaskConsumer((taskDeploymentDescriptor, jobMasterId) -> {
						tddFuture.complete(taskDeploymentDescriptor);
						return CompletableFuture.completedFuture(Acknowledge.get());
					})
					.createTestingTaskExecutorGateway();
			rpcService.registerGateway(testingTaskExecutorGateway.getAddress(), testingTaskExecutorGateway);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			rmLeaderRetrievalService.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final AllocationID allocationId = allocationIdFuture.get();

			final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(testingTaskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN);

			final Collection<SlotOffer> slotOffers = jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout).get();

			assertThat(slotOffers, hasSize(1));
			assertThat(slotOffers, contains(slotOffer));

			// obtain tdd for the result partition ids
			final TaskDeploymentDescriptor tdd = tddFuture.get();

			final ExecutionAttemptID executionAttemptId = tdd.getExecutionAttemptId();
			final ExecutionVertexID executionVertexID = jobMaster.getExecutionGraph().getRegisteredExecutions()
					.get(executionAttemptId).getVertex().getExecutionVertexID();
			JobVertexID theOtherJobVertexID = null;
			ExecutionAttemptID theOther = null;
			ExecutionVertex theOtherEV = null;
			for (Execution execution : jobMaster.getExecutionGraph().getRegisteredExecutions().values()) {
				if (execution.getAttemptId() != executionAttemptId) {
					theOther = execution.getAttemptId();
					theOtherJobVertexID = execution.getVertex().getJobvertexId();
					theOtherEV = execution.getVertex();
					break;
				}
			}

			// finish the producer task
			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					producerConsumerJobGraph.getJobID(), executionAttemptId, ExecutionState.FINISHED)).get();

			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					producerConsumerJobGraph.getJobID(), theOther, ExecutionState.FAILED)).get();

			ExecutionGraphTestUtils.waitUntilExecutionVertexState(theOtherEV, ExecutionState.DEPLOYING, 2000L);
			ExecutionGraphTestUtils.waitUntilExecutionVertexState(
					jobMaster.getExecutionGraph().getAllVertices().get(executionVertexID.getJobVertexID()).getTaskVertices()[0],
					ExecutionState.DEPLOYING,
					2000L);

			assertEquals(0, TestingEagerSchedulingPlugin.getFailoverCount());

			// Another master can replay the log as the use the same log store.
			final JobMaster newJobMaster = createJobMaster(
					configuration,
					producerConsumerJobGraph,
					haServices,
					new TestingJobManagerSharedServicesBuilder().build(),
					heartbeatServices);

			// let the new job master replay log
			newJobMaster.reconcile();

			// check that the status are the same with before failover.
			ExecutionJobVertex finishedEJV = newJobMaster.getExecutionGraph().getAllVertices()
					.get(executionVertexID.getJobVertexID());
			assertEquals(ExecutionState.DEPLOYING, finishedEJV.getTaskVertices()[0].getExecutionState());
			assertEquals(ExecutionState.DEPLOYING, newJobMaster.getExecutionGraph().getAllVertices()
					.get(theOtherJobVertexID).getTaskVertices()[0].getExecutionState());

			assertEquals(0, TestingEagerSchedulingPlugin.getCloseCount());
			assertEquals(2, TestingEagerSchedulingPlugin.getResetCount());
			if (!tdd.getProducedPartitions().isEmpty()) {
				assertEquals(1, TestingEagerSchedulingPlugin.getConsumableCount());
			}
			assertEquals(6, TestingEagerSchedulingPlugin.getDeployingCount());
			assertEquals(1, TestingEagerSchedulingPlugin.getFinishedCount());
			assertEquals(0, TestingEagerSchedulingPlugin.getRunningCount());
			assertEquals(0, TestingEagerSchedulingPlugin.getFailoverCount());
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that the after replay logs, the vertices will be scheduled still lazy from source.
	 */
	@Test
	public void testReplayLogLazyFromSource() throws Exception {
		TestingStepwiseSchedulingPlugin.init();
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
		configuration.setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, TestingStepwiseSchedulingPlugin.class.getName());
		final JobGraph producerConsumerJobGraph = producerConsumerJobGraph(configuration, ResultPartitionType.PIPELINED);
		JobManagerSharedServices sharedServices = new TestingJobManagerSharedServicesBuilder()
				.setRestartStrategyFactory(new FixedDelayRestartStrategy.FixedDelayRestartStrategyFactory(1, 0)).build();
		final JobMaster jobMaster = createJobMaster(
				configuration,
				producerConsumerJobGraph,
				haServices,
				sharedServices,
				heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			//clear the log genereated by other cases;
			jobMaster.getExecutionGraph().getGraphManager().reset();

			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
			testingResourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			rpcService.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			final CompletableFuture<TaskDeploymentDescriptor> tddFuture = new CompletableFuture<>();
			final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
					.setSubmitTaskConsumer((taskDeploymentDescriptor, jobMasterId) -> {
						tddFuture.complete(taskDeploymentDescriptor);
						return CompletableFuture.completedFuture(Acknowledge.get());
					})
					.createTestingTaskExecutorGateway();
			rpcService.registerGateway(testingTaskExecutorGateway.getAddress(), testingTaskExecutorGateway);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			rmLeaderRetrievalService.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final AllocationID allocationId = allocationIdFuture.get();

			final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(testingTaskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN);

			final Collection<SlotOffer> slotOffers = jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout).get();

			assertThat(slotOffers, hasSize(1));
			assertThat(slotOffers, contains(slotOffer));

			// obtain tdd for the result partition ids
			final TaskDeploymentDescriptor tdd = tddFuture.get();

			final ExecutionAttemptID producerAttemptId = tdd.getExecutionAttemptId();
			final ExecutionVertexID executionVertexID = jobMaster.getExecutionGraph().getRegisteredExecutions()
					.get(producerAttemptId).getVertex().getExecutionVertexID();
			JobVertexID theOtherJobVertexID = null;
			ExecutionAttemptID theOther = null;
			ExecutionVertex theOtherEV = null;
			for (Execution execution : jobMaster.getExecutionGraph().getRegisteredExecutions().values()) {
				if (execution.getAttemptId() != producerAttemptId) {
					theOther = execution.getAttemptId();
					theOtherJobVertexID = execution.getVertex().getJobvertexId();
					theOtherEV = execution.getVertex();
					break;
				}
			}

			// finish the producer task
			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					producerConsumerJobGraph.getJobID(), producerAttemptId, ExecutionState.RUNNING)).get();

			jobMasterGateway.scheduleOrUpdateConsumers(
					new ResultPartitionID(tdd.getProducedPartitions().iterator().next().getPartitionId(), producerAttemptId),
					Time.seconds(1)).get();

			ExecutionGraphTestUtils.waitUntilExecutionVertexState(theOtherEV, ExecutionState.DEPLOYING, 2000L);

			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					producerConsumerJobGraph.getJobID(), theOther, ExecutionState.FAILED)).get();

			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					producerConsumerJobGraph.getJobID(), producerAttemptId, ExecutionState.CANCELED)).get();

			ExecutionGraphTestUtils.waitUntilExecutionVertexState(
					jobMaster.getExecutionGraph().getAllVertices().get(executionVertexID.getJobVertexID()).getTaskVertices()[0],
					ExecutionState.DEPLOYING,
					2000L);

			assertEquals(1, TestingStepwiseSchedulingPlugin.getFailoverCount());
			assertEquals(ExecutionState.CREATED, theOtherEV.getExecutionState());

			// Another master can replay the log as the use the same log store.
			final JobMaster newJobMaster = createJobMaster(
					configuration,
					producerConsumerJobGraph,
					haServices,
					new TestingJobManagerSharedServicesBuilder().build(),
					heartbeatServices);

			// let the new job master replay log
			newJobMaster.reconcile();

			// check that the status are the same with before failover.
			ExecutionJobVertex producerEJV = newJobMaster.getExecutionGraph().getAllVertices()
					.get(executionVertexID.getJobVertexID());
			assertEquals(ExecutionState.DEPLOYING, producerEJV.getTaskVertices()[0].getExecutionState());
			assertEquals(ExecutionState.CREATED, newJobMaster.getExecutionGraph().getAllVertices()
					.get(theOtherJobVertexID).getTaskVertices()[0].getExecutionState());

			assertEquals(0, TestingStepwiseSchedulingPlugin.getCloseCount());
			assertEquals(1, TestingStepwiseSchedulingPlugin.getResetCount());
			assertEquals(2, TestingStepwiseSchedulingPlugin.getConsumableCount());
			assertEquals(6, TestingStepwiseSchedulingPlugin.getDeployingCount());
			assertEquals(0, TestingStepwiseSchedulingPlugin.getFinishedCount());
			assertEquals(2, TestingStepwiseSchedulingPlugin.getRunningCount());
			assertEquals(2, TestingStepwiseSchedulingPlugin.getFailoverCount());
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that the JobMaster reconcile with task executor after replay log.
	 */
	@Test
	public void testReconcileWithTaskExecutor() throws Exception {
		TestingEagerSchedulingPlugin.init();
		configuration.setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, TestingEagerSchedulingPlugin.class.getName());
		configuration.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
		final JobGraph producerConsumerJobGraph = producerConsumerJobGraph(configuration, ResultPartitionType.BLOCKING);
		final JobMaster jobMaster = createJobMaster(
				configuration,
				producerConsumerJobGraph,
				haServices,
				new TestingJobManagerSharedServicesBuilder().build(),
				heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			//clear the log genereated by other cases;
			jobMaster.getExecutionGraph().getGraphManager().reset();

			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
			testingResourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			rpcService.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			final CompletableFuture<TaskDeploymentDescriptor> tddFuture = new CompletableFuture<>();
			final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
					.setSubmitTaskConsumer((taskDeploymentDescriptor, jobMasterId) -> {
						tddFuture.complete(taskDeploymentDescriptor);
						return CompletableFuture.completedFuture(Acknowledge.get());
					})
					.createTestingTaskExecutorGateway();
			rpcService.registerGateway(testingTaskExecutorGateway.getAddress(), testingTaskExecutorGateway);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			rmLeaderRetrievalService.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final AllocationID allocationId = allocationIdFuture.get();

			final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(testingTaskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN);

			final Collection<SlotOffer> slotOffers = jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout).get();

			assertThat(slotOffers, hasSize(1));
			assertThat(slotOffers, contains(slotOffer));

			// obtain tdd for the result partition ids
			final TaskDeploymentDescriptor tdd = tddFuture.get();

			final ExecutionAttemptID producerAttemptId = tdd.getExecutionAttemptId();
			ExecutionVertexID producerVertexID = jobMaster.getExecutionGraph().getRegisteredExecutions()
					.get(producerAttemptId).getVertex().getExecutionVertexID();
			JobVertexID theOtherJobVertexID = null;
			ExecutionAttemptID theOther = null;
			for (Execution execution : jobMaster.getExecutionGraph().getRegisteredExecutions().values()) {
				if (execution.getAttemptId() != producerAttemptId) {
					theOther = execution.getAttemptId();
					theOtherJobVertexID = execution.getVertex().getJobvertexId();
					break;
				}
			}

			// finish the producer task
			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					producerConsumerJobGraph.getJobID(), producerAttemptId, ExecutionState.RUNNING)).get();

			ExecutionGraphTestUtils.waitUntilExecutionState(
					jobMaster.getExecutionGraph().getRegisteredExecutions().get(theOther),
					ExecutionState.DEPLOYING,
					2000L);
			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					producerConsumerJobGraph.getJobID(), theOther, ExecutionState.RUNNING)).get();

			// Another master can replay the log as the use the same log store.
			final JobMaster newJobMaster = createJobMaster(
					configuration,
					producerConsumerJobGraph,
					haServices,
					new TestingJobManagerSharedServicesBuilder().build(),
					heartbeatServices);

			// let the new job master replay log
			newJobMaster.reconcile();

			// check that the status are the same with before failover.
			ExecutionJobVertex producerEJV = newJobMaster.getExecutionGraph().getAllVertices()
					.get(producerVertexID.getJobVertexID());
			assertEquals(ExecutionState.RUNNING, producerEJV.getAggregateState());
			assertEquals(ExecutionState.RUNNING, newJobMaster.getExecutionGraph().getAllVertices().get(theOtherJobVertexID).getAggregateState());

			assertEquals(0, TestingEagerSchedulingPlugin.getCloseCount());
			assertEquals(1, TestingEagerSchedulingPlugin.getResetCount());
			assertEquals(0, TestingEagerSchedulingPlugin.getConsumableCount());
			assertEquals(4, TestingEagerSchedulingPlugin.getDeployingCount());
			assertEquals(0, TestingEagerSchedulingPlugin.getFinishedCount());
			assertEquals(4, TestingEagerSchedulingPlugin.getRunningCount());
			assertEquals(0, TestingEagerSchedulingPlugin.getFailoverCount());

			newJobMaster.start(jobMasterId, testingTimeout).get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			newJobMaster.registerTaskManager(testingTaskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

			boolean isConsumer = tdd.getProducedPartitions().isEmpty();
			ResultPartitionID producerPartitionID = new ResultPartitionID();
			ResultPartitionID theOtherPartitionID = new ResultPartitionID();
			ResultPartitionID[] reportIDForProducer = new ResultPartitionID[1];
			ResultPartitionID[] reportIDForTheOther = new ResultPartitionID[1];
			reportIDForProducer[0] = producerPartitionID;
			reportIDForTheOther[0] = theOtherPartitionID;
			List<TaskExecutionStatus> taskExecutionStatuses = new ArrayList<>(2);
			taskExecutionStatuses.add(new TaskExecutionStatus(
					isConsumer ? ExecutionState.FINISHED : ExecutionState.RUNNING,
					0,
					1,
					producerVertexID.getJobVertexID(),
					producerAttemptId,
					0,
					isConsumer ? new ResultPartitionID[0] : reportIDForProducer,
					isConsumer ? new boolean[0] : new boolean[1],
					Collections.emptyMap(),
					slotOffer));
			taskExecutionStatuses.add(new TaskExecutionStatus(
					isConsumer ? ExecutionState.RUNNING : ExecutionState.FINISHED,
					0,
					2,
					theOtherJobVertexID,
					theOther,
					0,
					isConsumer ? reportIDForTheOther : new ResultPartitionID[0],
					isConsumer ? new boolean[1] :new boolean[0],
					Collections.emptyMap(),
					slotOffer));

			// finish the producer task
			TaskExecutorReportResponse response = newJobMaster.reportTasksExecutionStatus(
					taskManagerLocation.getResourceID(),
					taskExecutionStatuses,
					Time.seconds(10)).get();

			assertTrue(response instanceof TaskExecutorReportResponse.Success);
			assertEquals(1, ((TaskExecutorReportResponse.Success) response).getRejectedIndexes().size());
			assertEquals(isConsumer ? 0 : 1, ((TaskExecutorReportResponse.Success) response).getRejectedIndexes().get(0).intValue());

			if (isConsumer) {
				assertEquals(theOtherPartitionID.getPartitionId(), newJobMaster.getExecutionGraph().getRegisteredExecutions().get(theOther)
						.getVertex().getProducedPartitions().values().iterator().next().getPartitionId());
				assertEquals(2, newJobMaster.getExecutionGraph().getRegisteredExecutions().get(theOther)
						.getVertex().getCurrentExecutionAttempt().getStateTimestamp(ExecutionState.CREATED));
			} else {
				assertEquals(producerPartitionID.getPartitionId(), newJobMaster.getExecutionGraph().getRegisteredExecutions().get(producerAttemptId)
						.getVertex().getProducedPartitions().values().iterator().next().getPartitionId());
				assertEquals(1, newJobMaster.getExecutionGraph().getRegisteredExecutions().get(producerAttemptId)
						.getVertex().getCurrentExecutionAttempt().getStateTimestamp(ExecutionState.CREATED));
			}
			RpcUtils.terminateRpcEndpoint(newJobMaster, testingTimeout);
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that a JobMaster will replay the input splits for finished excutions after started.
	 */
	@Test
	public void testReplayInputSplits() throws Exception {
		TestingEagerSchedulingPlugin.init();

		InputSplitSource<TestingInputSplit> inputSplitSource = new TestingInputSplitSource();
		final OperatorID operatorID = new OperatorID();

		final JobVertex producer = new JobVertex("Producer");
		producer.setInvokableClass(NoOpInvokable.class);
		producer.setParallelism(2);
		producer.setInputSplitSource(operatorID, inputSplitSource);

		final JobGraph jobGraph = new JobGraph(producer);
		jobGraph.setAllowQueuedScheduling(true);
		jobGraph.getSchedulingConfiguration().setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, TestingEagerSchedulingPlugin.class.getName());

		final JobMaster jobMaster = createJobMaster(
				configuration,
				jobGraph,
				haServices,
				new TestingJobManagerSharedServicesBuilder().build(),
				heartbeatServices);

		//clear the log genereated by other cases;
		jobMaster.getExecutionGraph().getGraphManager().reset();
		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
			testingResourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			rpcService.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			final CompletableFuture<TaskDeploymentDescriptor> tddFuture = new CompletableFuture<>();
			final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
					.setSubmitTaskConsumer((taskDeploymentDescriptor, jobMasterId) -> {
						tddFuture.complete(taskDeploymentDescriptor);
						return CompletableFuture.completedFuture(Acknowledge.get());
					})
					.createTestingTaskExecutorGateway();
			rpcService.registerGateway(testingTaskExecutorGateway.getAddress(), testingTaskExecutorGateway);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			rmLeaderRetrievalService.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final AllocationID allocationId = allocationIdFuture.get();

			final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(testingTaskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

			final SlotOffer slotOffer1 = new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN);
			final SlotOffer slotOffer2 = new SlotOffer(new AllocationID(), 1, ResourceProfile.UNKNOWN);

			final Collection<SlotOffer> slotOffers = jobMasterGateway.offerSlots(
					taskManagerLocation.getResourceID(),
					Arrays.asList(slotOffer1, slotOffer2),
					testingTimeout).get();

			assertThat(slotOffers, hasSize(2));
			assertTrue(slotOffers.contains(slotOffer1));
			assertTrue(slotOffers.contains(slotOffer2));

			// obtain tdd for the result partition ids
			final TaskDeploymentDescriptor tdd = tddFuture.get();

			final ExecutionAttemptID executionAttemptId = tdd.getExecutionAttemptId();
			final ExecutionVertexID executionVertexID = jobMaster.getExecutionGraph().getRegisteredExecutions()
					.get(executionAttemptId).getVertex().getExecutionVertexID();
			ExecutionAttemptID theOther = null;
			ExecutionVertexID theOtherVertexId = null;
			int theOtherSubIndex = 0;
			for (Execution execution : jobMaster.getExecutionGraph().getRegisteredExecutions().values()) {
				if (execution.getAttemptId() != executionAttemptId) {
					theOther = execution.getAttemptId();
					theOtherVertexId = execution.getVertex().getExecutionVertexID();
					theOtherSubIndex = execution.getParallelSubtaskIndex();
					break;
				}
			}

			ExecutionGraphTestUtils.waitUntilExecutionState(
					jobMaster.getExecutionGraph().getRegisteredExecutions().get(theOther),
					ExecutionState.DEPLOYING,
					2000L);
			verifyGetNextInputSplit(0, jobMaster, executionVertexID.getJobVertexID(), executionAttemptId, operatorID);

			// finish the producer task
			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					jobGraph.getJobID(), executionAttemptId, ExecutionState.FINISHED)).get();

			jobMasterGateway.updateTaskExecutionState(new TaskExecutionState(
					jobGraph.getJobID(), theOther, ExecutionState.RUNNING)).get();

			assertEquals(0, TestingEagerSchedulingPlugin.getCloseCount());
			assertEquals(1, TestingEagerSchedulingPlugin.getResetCount());
			assertEquals(0, TestingEagerSchedulingPlugin.getConsumableCount());
			assertEquals(2, TestingEagerSchedulingPlugin.getDeployingCount());
			assertEquals(1, TestingEagerSchedulingPlugin.getFinishedCount());
			assertEquals(1, TestingEagerSchedulingPlugin.getRunningCount());

			// Another master can replay the log as the use the same log store.
			final JobMaster newJobMaster = createJobMaster(
					configuration,
					jobGraph,
					haServices,
					new TestingJobManagerSharedServicesBuilder().build(),
					heartbeatServices);

			// let the new job master replay log
			newJobMaster.reconcile();

			newJobMaster.start(jobMasterId, testingTimeout).get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			newJobMaster.registerTaskManager(testingTaskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

			Map<OperatorID, List<InputSplit>> inputSplitsMap = new HashMap<>();
			inputSplitsMap.put(operatorID, Arrays.asList(new TestingInputSplit(1)));
			List<TaskExecutionStatus> taskExecutionStatuses = new ArrayList<>(2);
			taskExecutionStatuses.add(new TaskExecutionStatus(
					ExecutionState.RUNNING,
					0,
					1,
					theOtherVertexId.getJobVertexID(),
					theOther,
					theOtherSubIndex,
					new ResultPartitionID[0],
					new boolean[0],
					inputSplitsMap,
					slotOffer1));

			// finish the producer task
			TaskExecutorReportResponse response = newJobMaster.reportTasksExecutionStatus(
					taskManagerLocation.getResourceID(),
					taskExecutionStatuses,
					Time.seconds(10)).get();

			assertTrue(response instanceof TaskExecutorReportResponse.Success);
			// check that the input splits are the recovered after failover.
			final ExecutionAttemptID unfinishedAttempID = newJobMaster.getExecutionGraph().
					getRegisteredExecutions().keySet().iterator().next();
			verifyGetNextInputSplit(2, newJobMaster, executionVertexID.getJobVertexID(), unfinishedAttempID, operatorID);

			assertEquals(0, TestingEagerSchedulingPlugin.getCloseCount());
			assertEquals(1, TestingEagerSchedulingPlugin.getResetCount());
			assertEquals(0, TestingEagerSchedulingPlugin.getConsumableCount());
			assertEquals(4, TestingEagerSchedulingPlugin.getDeployingCount());
			assertEquals(2, TestingEagerSchedulingPlugin.getFinishedCount());
			assertEquals(2, TestingEagerSchedulingPlugin.getRunningCount());
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that the JobMaster is able to continue to schedule a execution after replaying a empty log.
	 */
	@Test
	public void testContinueScheduleAfterReplayingEmptyLog() throws Exception {
		TestingEagerSchedulingPlugin.init();
		configuration.setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, TestingEagerSchedulingPlugin.class.getName());
		final JobGraph producerConsumerJobGraph = producerConsumerJobGraph(configuration, ResultPartitionType.PIPELINED);
		final JobMaster jobMaster = createJobMaster(
				configuration,
				producerConsumerJobGraph,
				haServices,
				new TestingJobManagerSharedServicesBuilder().build(),
				heartbeatServices);

		//clear the log genereated by other cases;
		jobMaster.getExecutionGraph().getGraphManager().reset();

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			ExecutionGraphTestUtils.waitUntilExecutionState(
					jobMaster.getExecutionGraph().getRegisteredExecutions().values().iterator().next(),
					ExecutionState.SCHEDULED,
					2000L);

			// Another master can replay the log as the use the same log store.
			final JobMaster newJobMaster = createJobMaster(
					configuration,
					producerConsumerJobGraph,
					haServices,
					new TestingJobManagerSharedServicesBuilder().build(),
					heartbeatServices);

			// let the new job master replay log
			newJobMaster.reconcile();

			// check that the status are the same with before failover.

			newJobMaster.start(jobMasterId, testingTimeout).get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);

			ExecutionGraphTestUtils.waitUntilExecutionState(
					newJobMaster.getExecutionGraph().getRegisteredExecutions().values().iterator().next(),
					ExecutionState.SCHEDULED,
					2000L);

			RpcUtils.terminateRpcEndpoint(newJobMaster, testingTimeout);
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	/**
	 * Tests that the JobMaster will failover the job if reconciling all deploying executions.
	 */
	@Test
	public void testFailoverAfterReconcileDeployingExecutions() throws Exception {
		TestingEagerSchedulingPlugin.init();
		configuration.setString(JobManagerOptions.GRAPH_MANAGER_PLUGIN, TestingEagerSchedulingPlugin.class.getName());
		configuration.setLong(JobManagerOptions.JOB_RECONCILE_TIMEOUT, 1L);
		final JobGraph producerConsumerJobGraph = producerConsumerJobGraph(configuration, ResultPartitionType.BLOCKING);
		final JobMaster jobMaster = createJobMaster(
				configuration,
				producerConsumerJobGraph,
				haServices,
				new TestingJobManagerSharedServicesBuilder().build(),
				heartbeatServices);

		CompletableFuture<Acknowledge> startFuture = jobMaster.start(jobMasterId, testingTimeout);

		try {
			// wait for the start to complete
			startFuture.get(testingTimeout.toMilliseconds(), TimeUnit.MILLISECONDS);
			//clear the log genereated by other cases;
			jobMaster.getExecutionGraph().getGraphManager().reset();

			final TestingResourceManagerGateway testingResourceManagerGateway = new TestingResourceManagerGateway();

			final CompletableFuture<AllocationID> allocationIdFuture = new CompletableFuture<>();
			testingResourceManagerGateway.setRequestSlotConsumer(slotRequest -> allocationIdFuture.complete(slotRequest.getAllocationId()));

			rpcService.registerGateway(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway);

			final CompletableFuture<TaskDeploymentDescriptor> tddFuture = new CompletableFuture<>();
			final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
					.setSubmitTaskConsumer((taskDeploymentDescriptor, jobMasterId) -> {
						tddFuture.complete(taskDeploymentDescriptor);
						return CompletableFuture.completedFuture(Acknowledge.get());
					})
					.createTestingTaskExecutorGateway();
			rpcService.registerGateway(testingTaskExecutorGateway.getAddress(), testingTaskExecutorGateway);

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			rmLeaderRetrievalService.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());

			final AllocationID allocationId = allocationIdFuture.get();

			final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			jobMasterGateway.registerTaskManager(testingTaskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

			final SlotOffer slotOffer = new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN);

			final Collection<SlotOffer> slotOffers = jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), Collections.singleton(slotOffer), testingTimeout).get();

			assertThat(slotOffers, hasSize(1));
			assertThat(slotOffers, contains(slotOffer));

			// obtain tdd for the result partition ids
			final TaskDeploymentDescriptor tdd = tddFuture.get();

			final ExecutionAttemptID producerAttemptId = tdd.getExecutionAttemptId();
			ExecutionAttemptID theOther = null;
			for (Execution execution : jobMaster.getExecutionGraph().getRegisteredExecutions().values()) {
				if (execution.getAttemptId() != producerAttemptId) {
					theOther = execution.getAttemptId();
					break;
				}
			}

			ExecutionGraphTestUtils.waitUntilExecutionState(
					jobMaster.getExecutionGraph().getRegisteredExecutions().get(theOther),
					ExecutionState.DEPLOYING,
					2000L);

			// Another master can replay the log as the use the same log store.
			final JobMaster newJobMaster = createJobMaster(
					configuration,
					producerConsumerJobGraph,
					haServices,
					new TestingJobManagerSharedServicesBuilder().build(),
					heartbeatServices);

			// let the new job master replay log
			newJobMaster.reconcile();

			ExecutionGraph newExecutionGraph = newJobMaster.getExecutionGraph();
			Collection<ExecutionAttemptID> failedExecutions = newExecutionGraph.getReconcileFuture().get();

			// check that after reconciling, the job status are running, some executions need to be mark failed.
			assertEquals(JobStatus.RUNNING, newExecutionGraph.getState());
			Iterator<ExecutionAttemptID> attemptIDIterator = failedExecutions.iterator();
			assertTrue(newExecutionGraph.getRegisteredExecutions().containsKey(attemptIDIterator.next()));
			assertTrue(newExecutionGraph.getRegisteredExecutions().containsKey(attemptIDIterator.next()));

			RpcUtils.terminateRpcEndpoint(newJobMaster, testingTimeout);
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	private JobGraph producerConsumerJobGraph(Configuration configuration, ResultPartitionType resultPartitionType) {
		final SlotSharingGroup sharingGroup = new SlotSharingGroup();

		final JobVertex producer = new JobVertex("Producer");
		producer.setInvokableClass(NoOpInvokable.class);
		producer.setParallelism(1);
		producer.setSlotSharingGroup(sharingGroup);

		final JobVertex consumer = new JobVertex("Consumer");
		consumer.setInvokableClass(NoOpInvokable.class);
		consumer.setParallelism(1);
		consumer.setSlotSharingGroup(sharingGroup);

		consumer.connectNewDataSetAsInput(producer, DistributionPattern.POINTWISE, resultPartitionType);

		final JobGraph jobGraph = new JobGraph(producer, consumer);
		jobGraph.setAllowQueuedScheduling(true);
		jobGraph.getSchedulingConfiguration().addAll(configuration);

		return jobGraph;
	}

	private void verifyGetNextInputSplit(
			int expectedSplitNumber,
			final JobMaster jobMaster,
			final JobVertexID jobVertexID,
			final ExecutionAttemptID attemptID,
			final OperatorID operatorID) throws Exception {

		SerializedInputSplit serializedInputSplit = jobMaster.requestNextInputSplit(
				jobVertexID,
				operatorID,
				attemptID)
				.get(testingTimeout.getSize(), testingTimeout.getUnit());

		InputSplit inputSplit = InstantiationUtil.deserializeObject(
				serializedInputSplit.getInputSplitData(), Thread.currentThread().getContextClassLoader());

		assertEquals(expectedSplitNumber, inputSplit.getSplitNumber());
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
			JobMasterFailoverTest.class.getClassLoader(),
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

	/**
	 * A testing version of EagerSchedulingPlugin.
	 */
	public static final class TestingEagerSchedulingPlugin extends EagerSchedulingPlugin {

		private static int resetCount = 0;

		private static int closeCount = 0;

		private static int consumableCount = 0;

		private static int finishedCount = 0;

		private static int deployingCount = 0;

		private static int runningCount = 0;

		private static int failoverCount = 0;

		public static void init() {
			resetCount = 0;
			closeCount = 0;
			consumableCount = 0;
			finishedCount = 0;
			deployingCount = 0;
			runningCount = 0;
			failoverCount = 0;
		}

		@Override
		public void close() {
			closeCount++;
		}

		@Override
		public void reset() {
			resetCount++;
		}

		@Override
		public void onSchedulingStarted() {
			super.onSchedulingStarted();
		}

		@Override
		public void onResultPartitionConsumable(ResultPartitionConsumableEvent event) {
			consumableCount++;
			super.onResultPartitionConsumable(event);
		}

		@Override
		public void onExecutionVertexStateChanged(ExecutionVertexStateChangedEvent event) {
			if (ExecutionState.RUNNING.equals(event.getNewExecutionState())) {
				runningCount++;
			} else if (ExecutionState.DEPLOYING.equals(event.getNewExecutionState())) {
				deployingCount++;
			} else if (ExecutionState.FINISHED.equals(event.getNewExecutionState())) {
				finishedCount++;
			}
			super.onExecutionVertexStateChanged(event);
		}

		@Override
		public void onExecutionVertexFailover(ExecutionVertexFailoverEvent event) {
			failoverCount++;
			super.onExecutionVertexFailover(event);
		}

		@Override
		public boolean allowLazyDeployment() {
			return true;
		}

		public static int getResetCount() {
			return resetCount;
		}

		public static int getCloseCount() {
			return closeCount;
		}

		public static int getDeployingCount() {
			return deployingCount;
		}

		public static int getRunningCount() {
			return runningCount;
		}

		public static int getFinishedCount() {
			return finishedCount;
		}

		public static int getConsumableCount() {
			return consumableCount;
		}

		public static int getFailoverCount(){
			return failoverCount;
		}
	}

	/**
	 * A testing version of StepwiseSchedulingPlugin.
	 */
	public static final class TestingStepwiseSchedulingPlugin extends StepwiseSchedulingPlugin {

		private static int resetCount = 0;

		private static int closeCount = 0;

		private static int consumableCount = 0;

		private static int finishedCount = 0;

		private static int deployingCount = 0;

		private static int runningCount = 0;

		private static int failoverCount = 0;

		public static void init() {
			resetCount = 0;
			closeCount = 0;
			consumableCount = 0;
			finishedCount = 0;
			deployingCount = 0;
			runningCount = 0;
			failoverCount = 0;
		}

		@Override
		public void close() {
			closeCount++;
		}

		@Override
		public void reset() {
			resetCount++;
		}

		@Override
		public void onSchedulingStarted() {
			super.onSchedulingStarted();
		}

		@Override
		public void onResultPartitionConsumable(ResultPartitionConsumableEvent event) {
			consumableCount++;
			super.onResultPartitionConsumable(event);
		}

		@Override
		public void onExecutionVertexStateChanged(ExecutionVertexStateChangedEvent event) {
			if (ExecutionState.RUNNING.equals(event.getNewExecutionState())) {
				runningCount++;
			} else if (ExecutionState.DEPLOYING.equals(event.getNewExecutionState())) {
				deployingCount++;
			} else if (ExecutionState.FINISHED.equals(event.getNewExecutionState())) {
				finishedCount++;
			}
			super.onExecutionVertexStateChanged(event);
		}

		@Override
		public void onExecutionVertexFailover(ExecutionVertexFailoverEvent event) {
			failoverCount++;
			super.onExecutionVertexFailover(event);
		}

		@Override
		public boolean allowLazyDeployment() {
			return true;
		}

		public static int getResetCount() {
			return resetCount;
		}

		public static int getCloseCount() {
			return closeCount;
		}

		public static int getDeployingCount() {
			return deployingCount;
		}

		public static int getRunningCount() {
			return runningCount;
		}

		public static int getFinishedCount() {
			return finishedCount;
		}

		public static int getConsumableCount() {
			return consumableCount;
		}

		public static int getFailoverCount(){
			return failoverCount;
		}
	}

	private static final class TestingInputSplitSource implements InputSplitSource<TestingInputSplit> {
		@Override
		public TestingInputSplit[] createInputSplits(int minNumSplits) {
			return (TestingInputSplit[]) Arrays.asList(
					new TestingInputSplit(0),
					new TestingInputSplit(1),
					new TestingInputSplit(2)).toArray();
		}

		@Override
		public InputSplitAssigner getInputSplitAssigner(TestingInputSplit[] inputSplits) {
			return new TestingInputSplitAssigner(inputSplits);
		}
	}

	private static final class TestingInputSplitAssigner implements InputSplitAssigner {

		private final List<TestingInputSplit> testingInputSplits;

		TestingInputSplitAssigner(TestingInputSplit[] inputSplits) {
			testingInputSplits = new ArrayList<>(inputSplits.length);
			for (TestingInputSplit inputSplit : inputSplits) {
				testingInputSplits.add(inputSplit);
			}
		}

		@Override
		public InputSplit getNextInputSplit(String host, int taskId){
			return testingInputSplits.remove(0);
		}

		@Override
		public void inputSplitsAssigned(int taskId, List<InputSplit> inputSplits) {
			for (InputSplit inputSplit : inputSplits) {
				boolean found = false;
				for (InputSplit split : testingInputSplits) {
					if (split.getSplitNumber() == inputSplit.getSplitNumber()) {
						testingInputSplits.remove(split);
						found = true;
						break;
					}
				}
				if (!found) {
					throw new FlinkRuntimeException("InputSplit not found for " + inputSplit.getSplitNumber());
				}
			}
		}
	}

	private static final class TestingInputSplit implements InputSplit {

		private final int splitNumber;

		public TestingInputSplit(int number) {
			this.splitNumber = number;
		}

		public int getSplitNumber() {
			return splitNumber;
		}
	}
}
