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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.BlobServerOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.utils.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the partition release logic of the {@link JobMaster}.
 */
public class JobMasterPartitionReleaseTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final Time testingTimeout = Time.seconds(10L);

	private static final long heartbeatInterval = 1000L;
	private static final long heartbeatTimeout = 5_000_000L;

	private static TestingRpcService rpcService;

	private static HeartbeatServices heartbeatServices;

	private Configuration configuration;

	private JobMasterId jobMasterId;

	private TestingHighAvailabilityServices haServices;

	private SettableLeaderRetrievalService rmLeaderRetrievalService;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();

		heartbeatServices = new HeartbeatServices(heartbeatInterval, heartbeatTimeout);
	}

	@Before
	public void setup() throws IOException {
		configuration = new Configuration();
		haServices = new TestingHighAvailabilityServices();
		jobMasterId = JobMasterId.generate();

		testingFatalErrorHandler = new TestingFatalErrorHandler();

		haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

		rmLeaderRetrievalService = new SettableLeaderRetrievalService(
			null,
			null);
		haServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);

		configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());
	}

	@After
	public void teardown() throws Exception {
		if (testingFatalErrorHandler != null) {
			testingFatalErrorHandler.rethrowError();
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
	public void testPartitionTableCleanupOnDisconnect() throws Exception {
		final JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();
		final JobGraph jobGraph = JobGraphTestUtils.createSingleVertexJobGraph();

		final CompletableFuture<ResourceID> partitionCleanupTaskExecutorId = new CompletableFuture<>();
		final TestingJobMasterPartitionTracker partitionTracker = new TestingJobMasterPartitionTracker();
		partitionTracker.setStopTrackingAllPartitionsConsumer(partitionCleanupTaskExecutorId::complete);

		final JobMaster jobMaster = new JobMasterBuilder(jobGraph, rpcService)
			.withConfiguration(configuration)
			.withHighAvailabilityServices(haServices)
			.withJobManagerSharedServices(jobManagerSharedServices)
			.withHeartbeatServices(heartbeatServices)
			.withPartitionTrackerFactory(ignored -> partitionTracker)
			.createJobMaster();

		final CompletableFuture<JobID> disconnectTaskExecutorFuture = new CompletableFuture<>();
		final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setDisconnectJobManagerConsumer((jobID, throwable) -> disconnectTaskExecutorFuture.complete(jobID))
			.createTestingTaskExecutorGateway();

		try {
			jobMaster.start(jobMasterId).get();

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			// register a slot to establish a connection
			final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			registerSlotsAtJobMaster(1, jobMasterGateway, testingTaskExecutorGateway, taskManagerLocation);

			jobMasterGateway.disconnectTaskManager(taskManagerLocation.getResourceID(), new Exception("test"));
			disconnectTaskExecutorFuture.get();

			assertThat(partitionCleanupTaskExecutorId.get(), equalTo(taskManagerLocation.getResourceID()));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	@Test
	public void testPartitionReleaseOrPromotionOnJobTermination() throws Exception {
		final JobManagerSharedServices jobManagerSharedServices = new TestingJobManagerSharedServicesBuilder().build();
		final JobGraph jobGraph = JobGraphTestUtils.createSingleVertexJobGraph();

		final CompletableFuture<TaskDeploymentDescriptor> taskDeploymentDescriptorFuture = new CompletableFuture<>();
		final CompletableFuture<Tuple2<JobID, Collection<ResultPartitionID>>> releasePartitionsFuture = new CompletableFuture<>();
		final CompletableFuture<JobID> disconnectTaskExecutorFuture = new CompletableFuture<>();
		final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setReleaseOrPromotePartitionsConsumer((jobId, partitionsToRelease, partitionsToPromote) -> releasePartitionsFuture.complete(Tuple2.of(jobId, partitionsToRelease)))
			.setDisconnectJobManagerConsumer((jobID, throwable) -> disconnectTaskExecutorFuture.complete(jobID))
			.setSubmitTaskConsumer((tdd, ignored) -> {
				taskDeploymentDescriptorFuture.complete(tdd);
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.createTestingTaskExecutorGateway();

		final CompletableFuture<ResourceID> taskExecutorIdForPartitionRelease = new CompletableFuture<>();
		final CompletableFuture<ResourceID> taskExecutorIdForPartitionReleaseOrPromote = new CompletableFuture<>();
		final TestingJobMasterPartitionTracker partitionTracker = new TestingJobMasterPartitionTracker();
		partitionTracker.setStopTrackingAndReleaseAllPartitionsConsumer(taskExecutorIdForPartitionRelease::complete);
		partitionTracker.setStopTrackingAndReleaseOrPromotePartitionsConsumer(taskExecutorIdForPartitionReleaseOrPromote::complete);

		final JobMaster jobMaster = new JobMasterBuilder(jobGraph, rpcService)
			.withConfiguration(configuration)
			.withHighAvailabilityServices(haServices)
			.withJobManagerSharedServices(jobManagerSharedServices)
			.withHeartbeatServices(heartbeatServices)
			.withPartitionTrackerFactory(ignord -> partitionTracker)
			.createJobMaster();

		try {
			jobMaster.start(jobMasterId).get();

			final JobMasterGateway jobMasterGateway = jobMaster.getSelfGateway(JobMasterGateway.class);

			final LocalTaskManagerLocation taskManagerLocation = new LocalTaskManagerLocation();
			registerSlotsAtJobMaster(1, jobMasterGateway, testingTaskExecutorGateway, taskManagerLocation);

			// update the execution state of the only execution to FINISHED
			// this should trigger the job to finish
			final TaskDeploymentDescriptor taskDeploymentDescriptor = taskDeploymentDescriptorFuture.get();
			jobMasterGateway.updateTaskExecutionState(
				new TaskExecutionState(
					taskDeploymentDescriptor.getJobId(),
					taskDeploymentDescriptor.getExecutionAttemptId(),
					ExecutionState.FINISHED));

			assertThat(taskExecutorIdForPartitionReleaseOrPromote.get(), equalTo(taskManagerLocation.getResourceID()));
		} finally {
			RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
		}
	}

	private Collection<SlotOffer> registerSlotsAtJobMaster(
			int numberSlots,
			JobMasterGateway jobMasterGateway,
			TaskExecutorGateway taskExecutorGateway,
			TaskManagerLocation taskManagerLocation) throws ExecutionException, InterruptedException {
		final AllocationIdsResourceManagerGateway allocationIdsResourceManagerGateway = new AllocationIdsResourceManagerGateway();
		rpcService.registerGateway(allocationIdsResourceManagerGateway.getAddress(), allocationIdsResourceManagerGateway);
		notifyResourceManagerLeaderListeners(allocationIdsResourceManagerGateway);

		rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

		jobMasterGateway.registerTaskManager(taskExecutorGateway.getAddress(), taskManagerLocation, testingTimeout).get();

		Collection<SlotOffer> slotOffers = IntStream
			.range(0, numberSlots)
			.mapToObj(
				index -> {
					final AllocationID allocationId = allocationIdsResourceManagerGateway.takeAllocationId();
					return new SlotOffer(allocationId, index, ResourceProfile.UNKNOWN);
				})
			.collect(Collectors.toList());

		return jobMasterGateway.offerSlots(taskManagerLocation.getResourceID(), slotOffers, testingTimeout).get();
	}

	private void notifyResourceManagerLeaderListeners(TestingResourceManagerGateway testingResourceManagerGateway) {
		rmLeaderRetrievalService.notifyListener(testingResourceManagerGateway.getAddress(), testingResourceManagerGateway.getFencingToken().toUUID());
	}

	private static final class AllocationIdsResourceManagerGateway extends TestingResourceManagerGateway {
		private final BlockingQueue<AllocationID> allocationIds;

		private AllocationIdsResourceManagerGateway() {
			this.allocationIds = new ArrayBlockingQueue<>(10);
			setRequestSlotConsumer(
				slotRequest -> allocationIds.offer(slotRequest.getAllocationId())
			);
		}

		AllocationID takeAllocationId() {
			try {
				return allocationIds.take();
			} catch (InterruptedException e) {
				ExceptionUtils.rethrow(e);
				return null;
			}
		}
	}
}
