/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.jobgraph.utils.JobGraphTestUtils;
import org.apache.flink.runtime.jobmanager.OnCompletionActions;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.AccumulatorReport;
import org.apache.flink.runtime.taskexecutor.ExecutionDeploymentReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorToJobManagerHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.UnresolvedTaskManagerLocation;
import org.apache.flink.runtime.util.TestingFatalErrorHandlerResource;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;

/**
 * Tests for the execution deployment-reconciliation logic in the {@link JobMaster}.
 */
public class JobMasterExecutionDeploymentReconciliationTest extends TestLogger {

	private static final Time testingTimeout = Time.seconds(10L);

	private static TestingRpcService rpcService;

	private final HeartbeatServices heartbeatServices = new HeartbeatServices(Integer.MAX_VALUE, Integer.MAX_VALUE);

	private final TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
	private final SettableLeaderRetrievalService resourceManagerLeaderRetriever = new SettableLeaderRetrievalService();
	private final TestingLeaderElectionService resourceManagerLeaderElectionService = new TestingLeaderElectionService();

	@Rule
	public final TestingFatalErrorHandlerResource testingFatalErrorHandlerResource = new TestingFatalErrorHandlerResource();

	@Before
	public void setup() {
		haServices.setResourceManagerLeaderRetriever(resourceManagerLeaderRetriever);
		haServices.setResourceManagerLeaderElectionService(resourceManagerLeaderElectionService);
		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());
	}

	@After
	public void shutdown() {
		rpcService.clearGateways();
	}

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();
	}

	@AfterClass
	public static void shutdownClass() throws ExecutionException, InterruptedException {
		rpcService.stopService().get();
	}

	@Test
	public void testExecutionDeploymentReconciliation() throws Exception {
		JobMasterBuilder.TestingOnCompletionActions onCompletionActions = new JobMasterBuilder.TestingOnCompletionActions();
		JobMaster jobMaster = createAndStartJobMaster(onCompletionActions);
		JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);
		rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);

		final JobMasterPartitionReleaseTest.AllocationIdsResourceManagerGateway resourceManagerGateway = createResourceManagerGateway();

		final CompletableFuture<Void> taskSubmissionFuture = new CompletableFuture<>();
		final CompletableFuture<ExecutionAttemptID> taskCancellationFuture = new CompletableFuture<>();
		TaskExecutorGateway taskExecutorGateway = createTaskExecutorGateway(taskSubmissionFuture, taskCancellationFuture);
		LocalUnresolvedTaskManagerLocation localUnresolvedTaskManagerLocation = new LocalUnresolvedTaskManagerLocation();

		registerTaskExecutorAndOfferSlots(resourceManagerGateway, jobMasterGateway, taskExecutorGateway, localUnresolvedTaskManagerLocation);

		taskSubmissionFuture.get();
		assertFalse(taskCancellationFuture.isDone());

		ExecutionAttemptID unknownDeployment = new ExecutionAttemptID();
		//  the deployment report is missing the just deployed task, but contains the ID of some other unknown deployment
		//  the job master should cancel the unknown deployment, and fail the job
		jobMasterGateway.heartbeatFromTaskManager(localUnresolvedTaskManagerLocation.getResourceID(), new TaskExecutorToJobManagerHeartbeatPayload(
			new AccumulatorReport(Collections.emptyList()),
			new ExecutionDeploymentReport(Collections.singleton(unknownDeployment))
		));

		assertThat(taskCancellationFuture.get(), is(unknownDeployment));

		assertThat(onCompletionActions.getJobReachedGloballyTerminalStateFuture().get().getState(), is(JobStatus.FAILED));
	}

	private JobMaster createAndStartJobMaster(OnCompletionActions onCompletionActions) throws Exception {
		JobMaster jobMaster = new JobMasterBuilder(JobGraphTestUtils.createSingleVertexJobGraph(), rpcService)
			.withFatalErrorHandler(testingFatalErrorHandlerResource.getFatalErrorHandler())
			.withHighAvailabilityServices(haServices)
			.withHeartbeatServices(heartbeatServices)
			.withOnCompletionActions(onCompletionActions)
			.createJobMaster();

		jobMaster.start(JobMasterId.generate()).get();

		return jobMaster;
	}

	private JobMasterPartitionReleaseTest.AllocationIdsResourceManagerGateway createResourceManagerGateway() {
		JobMasterPartitionReleaseTest.AllocationIdsResourceManagerGateway resourceManagerGateway = new JobMasterPartitionReleaseTest.AllocationIdsResourceManagerGateway();
		rpcService.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
		resourceManagerLeaderRetriever.notifyListener(resourceManagerGateway.getAddress(), resourceManagerGateway.getFencingToken().toUUID());
		return resourceManagerGateway;
	}

	private TaskExecutorGateway createTaskExecutorGateway(CompletableFuture<Void> taskSubmissionFuture, CompletableFuture<ExecutionAttemptID> taskCancellationFuture) {
		TestingTaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setAddress(UUID.randomUUID().toString())
			.setSubmitTaskConsumer((taskDeploymentDescriptor, jobMasterId) -> {
				taskSubmissionFuture.complete(null);
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.setCancelTaskFunction(executionAttemptId -> {
				taskCancellationFuture.complete(executionAttemptId);
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.createTestingTaskExecutorGateway();

		rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

		return taskExecutorGateway;
	}

	private void registerTaskExecutorAndOfferSlots(
			JobMasterPartitionReleaseTest.AllocationIdsResourceManagerGateway resourceManagerGateway,
			JobMasterGateway jobMasterGateway,
			TaskExecutorGateway taskExecutorGateway,
			UnresolvedTaskManagerLocation taskManagerLocation) throws ExecutionException, InterruptedException {
		jobMasterGateway.registerTaskManager(taskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

		final AllocationID allocationId = resourceManagerGateway.takeAllocationId();
		Collection<SlotOffer> slotOffers = Collections.singleton(new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN));

		jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), slotOffers, testingTimeout).get();
	}
}
