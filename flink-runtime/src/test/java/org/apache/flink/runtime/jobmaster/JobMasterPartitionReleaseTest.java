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
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.utils.JobGraphTestUtils;
import org.apache.flink.runtime.jobmaster.utils.JobMasterBuilder;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.messages.Acknowledge;
import org.apache.flink.runtime.resourcemanager.AllocationIdsExposingResourceManagerGateway;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.slot.SlotOffer;
import org.apache.flink.runtime.taskmanager.LocalUnresolvedTaskManagerLocation;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
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
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests for the partition release logic of the {@link JobMaster}.
 */
public class JobMasterPartitionReleaseTest extends TestLogger {

	@ClassRule
	public static TemporaryFolder temporaryFolder = new TemporaryFolder();

	private static final Time testingTimeout = Time.seconds(10L);

	private static TestingRpcService rpcService;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();
	}

	@Before
	public void setup() throws IOException {
		testingFatalErrorHandler = new TestingFatalErrorHandler();
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
		final CompletableFuture<JobID> disconnectTaskExecutorFuture = new CompletableFuture<>();
		final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setDisconnectJobManagerConsumer((jobID, throwable) -> disconnectTaskExecutorFuture.complete(jobID))
			.createTestingTaskExecutorGateway();

		try (final TestSetup testSetup = new TestSetup(rpcService, testingFatalErrorHandler, testingTaskExecutorGateway)) {
			final JobMasterGateway jobMasterGateway = testSetup.jobMaster.getSelfGateway(JobMasterGateway.class);

			jobMasterGateway.disconnectTaskManager(testSetup.getTaskExecutorResourceID(), new Exception("test"));
			disconnectTaskExecutorFuture.get();

			assertThat(testSetup.getStopTrackingPartitionsTargetResourceId().get(), equalTo(testSetup.getTaskExecutorResourceID()));
		}
	}

	@Test
	public void testPartitionReleaseOrPromotionOnJobSuccess() throws Exception {
		testPartitionReleaseOrPromotionOnJobTermination(TestSetup::getReleaseOrPromotePartitionsTargetResourceId, ExecutionState.FINISHED);
	}

	@Test
	public void testPartitionReleaseOrPromotionOnJobFailure() throws Exception {
		testPartitionReleaseOrPromotionOnJobTermination(TestSetup::getReleasePartitionsTargetResourceId, ExecutionState.FAILED);
	}

	private void testPartitionReleaseOrPromotionOnJobTermination(Function<TestSetup, CompletableFuture<ResourceID>> taskExecutorCallSelector, ExecutionState finalExecutionState) throws Exception {
		final CompletableFuture<TaskDeploymentDescriptor> taskDeploymentDescriptorFuture = new CompletableFuture<>();
		final TestingTaskExecutorGateway testingTaskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setSubmitTaskConsumer((tdd, ignored) -> {
				taskDeploymentDescriptorFuture.complete(tdd);
				return CompletableFuture.completedFuture(Acknowledge.get());
			})
			.createTestingTaskExecutorGateway();

		try (final TestSetup testSetup = new TestSetup(rpcService, testingFatalErrorHandler, testingTaskExecutorGateway)) {
			final JobMasterGateway jobMasterGateway = testSetup.getJobMasterGateway();

			// update the execution state of the only execution to target state
			// this should trigger the job to finish
			final TaskDeploymentDescriptor taskDeploymentDescriptor = taskDeploymentDescriptorFuture.get();
			jobMasterGateway.updateTaskExecutionState(
				new TaskExecutionState(
					taskDeploymentDescriptor.getJobId(),
					taskDeploymentDescriptor.getExecutionAttemptId(),
					finalExecutionState));

			assertThat(taskExecutorCallSelector.apply(testSetup).get(), equalTo(testSetup.getTaskExecutorResourceID()));
		}
	}

	private static class TestSetup implements AutoCloseable {

		private final TemporaryFolder temporaryFolder = new TemporaryFolder();

		private final LocalUnresolvedTaskManagerLocation localTaskManagerUnresolvedLocation = new LocalUnresolvedTaskManagerLocation();

		private final CompletableFuture<ResourceID> taskExecutorIdForStopTracking = new CompletableFuture<>();
		private final CompletableFuture<ResourceID> taskExecutorIdForPartitionRelease = new CompletableFuture<>();
		private final CompletableFuture<ResourceID> taskExecutorIdForPartitionReleaseOrPromote = new CompletableFuture<>();

		private JobMaster jobMaster;

		public TestSetup(TestingRpcService rpcService, FatalErrorHandler fatalErrorHandler, TaskExecutorGateway taskExecutorGateway) throws Exception {

			temporaryFolder.create();

			TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
			haServices.setCheckpointRecoveryFactory(new StandaloneCheckpointRecoveryFactory());

			SettableLeaderRetrievalService rmLeaderRetrievalService = new SettableLeaderRetrievalService(
				null,
				null);
			haServices.setResourceManagerLeaderRetriever(rmLeaderRetrievalService);

			final TestingJobMasterPartitionTracker partitionTracker = new TestingJobMasterPartitionTracker();

			partitionTracker.setStopTrackingAllPartitionsConsumer(taskExecutorIdForStopTracking::complete);
			partitionTracker.setStopTrackingAndReleaseAllPartitionsConsumer(taskExecutorIdForPartitionRelease::complete);
			partitionTracker.setStopTrackingAndReleaseOrPromotePartitionsConsumer(taskExecutorIdForPartitionReleaseOrPromote::complete);

			Configuration configuration = new Configuration();
			configuration.setString(BlobServerOptions.STORAGE_DIRECTORY, temporaryFolder.newFolder().getAbsolutePath());

			HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, 5_000_000L);

			jobMaster = new JobMasterBuilder(JobGraphTestUtils.createSingleVertexJobGraph(), rpcService)
				.withConfiguration(configuration)
				.withHighAvailabilityServices(haServices)
				.withJobManagerSharedServices(new TestingJobManagerSharedServicesBuilder().build())
				.withFatalErrorHandler(fatalErrorHandler)
				.withHeartbeatServices(heartbeatServices)
				.withPartitionTrackerFactory(ignored -> partitionTracker)
				.createJobMaster();

			jobMaster.start(JobMasterId.generate()).get();

			registerTaskExecutorAtJobMaster(
				rpcService,
				getJobMasterGateway(),
				taskExecutorGateway,
				rmLeaderRetrievalService
			);
		}

		private void registerTaskExecutorAtJobMaster(
				TestingRpcService rpcService,
				JobMasterGateway jobMasterGateway,
				TaskExecutorGateway taskExecutorGateway,
				SettableLeaderRetrievalService rmLeaderRetrievalService) throws ExecutionException, InterruptedException {

			final AllocationIdsExposingResourceManagerGateway resourceManagerGateway = new AllocationIdsExposingResourceManagerGateway();
			rpcService.registerGateway(resourceManagerGateway.getAddress(), resourceManagerGateway);
			rmLeaderRetrievalService.notifyListener(resourceManagerGateway.getAddress(), resourceManagerGateway.getFencingToken().toUUID());

			rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

			jobMasterGateway.registerTaskManager(taskExecutorGateway.getAddress(), localTaskManagerUnresolvedLocation, testingTimeout).get();

			final AllocationID allocationId = resourceManagerGateway.takeAllocationId();
			Collection<SlotOffer> slotOffers = Collections.singleton(new SlotOffer(allocationId, 0, ResourceProfile.UNKNOWN));

			jobMasterGateway.offerSlots(localTaskManagerUnresolvedLocation.getResourceID(), slotOffers, testingTimeout).get();
		}

		public JobMasterGateway getJobMasterGateway() {
			return jobMaster.getSelfGateway(JobMasterGateway.class);
		}

		public ResourceID getTaskExecutorResourceID() {
			return localTaskManagerUnresolvedLocation.getResourceID();
		}

		public CompletableFuture<ResourceID> getStopTrackingPartitionsTargetResourceId() {
			return taskExecutorIdForStopTracking;
		}

		public CompletableFuture<ResourceID> getReleasePartitionsTargetResourceId() {
			return taskExecutorIdForPartitionRelease;
		}

		public CompletableFuture<ResourceID> getReleaseOrPromotePartitionsTargetResourceId() {
			return taskExecutorIdForPartitionReleaseOrPromote;
		}

		public void close() throws Exception {
			try {
				if (jobMaster != null) {
					RpcUtils.terminateRpcEndpoint(jobMaster, testingTimeout);
				}
			} finally {
				temporaryFolder.delete();
			}
		}
	}
}
