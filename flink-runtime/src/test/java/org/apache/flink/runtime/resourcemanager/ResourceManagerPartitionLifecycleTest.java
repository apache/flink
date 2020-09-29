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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerImpl;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerBuilder;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorHeartbeatPayload;
import org.apache.flink.runtime.taskexecutor.TaskExecutorMemoryConfiguration;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.taskexecutor.partition.ClusterPartitionReport;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;

/**
 * Tests for the partition-lifecycle logic in the {@link ResourceManager}.
 */
public class ResourceManagerPartitionLifecycleTest extends TestLogger {

	private static final Time TIMEOUT = Time.minutes(2L);

	private static TestingRpcService rpcService;

	private TestingHighAvailabilityServices highAvailabilityServices;

	private TestingLeaderElectionService resourceManagerLeaderElectionService;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	private TestingResourceManager resourceManager;

	@BeforeClass
	public static void setupClass() {
		rpcService = new TestingRpcService();
	}

	@Before
	public void setup() throws Exception {
		highAvailabilityServices = new TestingHighAvailabilityServices();
		resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		highAvailabilityServices.setResourceManagerLeaderElectionService(resourceManagerLeaderElectionService);
		testingFatalErrorHandler = new TestingFatalErrorHandler();
	}

	@After
	public void after() throws Exception {
		if (resourceManager != null) {
			RpcUtils.terminateRpcEndpoint(resourceManager, TIMEOUT);
		}

		if (highAvailabilityServices != null) {
			highAvailabilityServices.closeAndCleanupAllData();
		}

		if (testingFatalErrorHandler.hasExceptionOccurred()) {
			testingFatalErrorHandler.rethrowError();
		}
	}

	@AfterClass
	public static void tearDownClass() throws Exception {
		if (rpcService != null) {
			RpcUtils.terminateRpcServices(TIMEOUT, rpcService);
		}
	}

	@Test
	public void testClusterPartitionReportHandling() throws Exception {
		final CompletableFuture<Collection<IntermediateDataSetID>> clusterPartitionReleaseFuture = new CompletableFuture<>();
		runTest(
			builder -> builder.setReleaseClusterPartitionsConsumer(clusterPartitionReleaseFuture::complete),
			(resourceManagerGateway, taskManagerId1, ignored) -> {
				IntermediateDataSetID dataSetID = new IntermediateDataSetID();
				ResultPartitionID resultPartitionID = new ResultPartitionID();

				resourceManagerGateway.heartbeatFromTaskManager(
					taskManagerId1,
					createTaskExecutorHeartbeatPayload(dataSetID, 2, resultPartitionID, new ResultPartitionID()));

				// send a heartbeat containing 1 partition less -> partition loss -> should result in partition release
				resourceManagerGateway.heartbeatFromTaskManager(
					taskManagerId1,
					createTaskExecutorHeartbeatPayload(dataSetID, 2, resultPartitionID));

				Collection<IntermediateDataSetID> intermediateDataSetIDS = clusterPartitionReleaseFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
				assertThat(intermediateDataSetIDS, contains(dataSetID));
			});
	}

	@Test
	public void testTaskExecutorShutdownHandling() throws Exception {
		final CompletableFuture<Collection<IntermediateDataSetID>> clusterPartitionReleaseFuture = new CompletableFuture<>();
		runTest(
			builder -> builder.setReleaseClusterPartitionsConsumer(clusterPartitionReleaseFuture::complete),
			(resourceManagerGateway, taskManagerId1, taskManagerId2) -> {
				IntermediateDataSetID dataSetID = new IntermediateDataSetID();

				resourceManagerGateway.heartbeatFromTaskManager(
					taskManagerId1,
					createTaskExecutorHeartbeatPayload(dataSetID, 2, new ResultPartitionID()));

				// we need a partition on another task executor so that there's something to release when one task executor goes down
				resourceManagerGateway.heartbeatFromTaskManager(
					taskManagerId2,
					createTaskExecutorHeartbeatPayload(dataSetID, 2, new ResultPartitionID()));

				resourceManagerGateway.disconnectTaskManager(taskManagerId2, new RuntimeException("test exception"));
				Collection<IntermediateDataSetID> intermediateDataSetIDS = clusterPartitionReleaseFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
				assertThat(intermediateDataSetIDS, contains(dataSetID));
			});
	}

	private void runTest(TaskExecutorSetup taskExecutorBuilderSetup, TestAction testAction) throws Exception {
		final ResourceManagerGateway resourceManagerGateway = createAndStartResourceManager();

		TestingTaskExecutorGatewayBuilder testingTaskExecutorGateway1Builder = new TestingTaskExecutorGatewayBuilder();
		taskExecutorBuilderSetup.accept(testingTaskExecutorGateway1Builder);
		final TaskExecutorGateway taskExecutorGateway1 = testingTaskExecutorGateway1Builder
			.setAddress(UUID.randomUUID().toString())
			.createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorGateway1.getAddress(), taskExecutorGateway1);

		final TaskExecutorGateway taskExecutorGateway2 = new TestingTaskExecutorGatewayBuilder()
			.setAddress(UUID.randomUUID().toString())
			.createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorGateway2.getAddress(), taskExecutorGateway2);

		final ResourceID taskManagerId1 = ResourceID.generate();
		final ResourceID taskManagerId2 = ResourceID.generate();
		registerTaskExecutor(resourceManagerGateway, taskManagerId1, taskExecutorGateway1.getAddress());
		registerTaskExecutor(resourceManagerGateway, taskManagerId2, taskExecutorGateway2.getAddress());

		testAction.accept(resourceManagerGateway, taskManagerId1, taskManagerId2);
	}

	public static void registerTaskExecutor(ResourceManagerGateway resourceManagerGateway, ResourceID taskExecutorId, String taskExecutorAddress) throws Exception {
		final TaskExecutorRegistration taskExecutorRegistration = new TaskExecutorRegistration(
			taskExecutorAddress,
			taskExecutorId,
			1234,
			23456,
			new HardwareDescription(42, 1337L, 1337L, 0L),
			new TaskExecutorMemoryConfiguration(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L),
			ResourceProfile.ZERO,
			ResourceProfile.ZERO);
		final CompletableFuture<RegistrationResponse> registrationFuture = resourceManagerGateway.registerTaskExecutor(
			taskExecutorRegistration,
			TestingUtils.TIMEOUT());

		assertThat(registrationFuture.get(), instanceOf(RegistrationResponse.Success.class));
	}

	private ResourceManagerGateway createAndStartResourceManager() throws Exception {
		final SlotManager slotManager = SlotManagerBuilder.newBuilder()
			.setScheduledExecutor(rpcService.getScheduledExecutor())
			.build();
		final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			highAvailabilityServices,
			rpcService.getScheduledExecutor(),
			TestingUtils.infiniteTime());

		final TestingResourceManager resourceManager = new TestingResourceManager(
			rpcService,
			ResourceID.generate(),
			highAvailabilityServices,
			new HeartbeatServices(100000L, 1000000L),
			slotManager,
			ResourceManagerPartitionTrackerImpl::new,
			jobLeaderIdService,
			testingFatalErrorHandler,
			UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup());

		resourceManager.start();

		// first make the ResourceManager the leader
		resourceManagerLeaderElectionService.isLeader(ResourceManagerId.generate().toUUID()).get();

		this.resourceManager = resourceManager;

		return resourceManager.getSelfGateway(ResourceManagerGateway.class);
	}

	private static TaskExecutorHeartbeatPayload createTaskExecutorHeartbeatPayload(IntermediateDataSetID dataSetId, int numTotalPartitions, ResultPartitionID... partitionIds) {
		return new TaskExecutorHeartbeatPayload(
			new SlotReport(),
			new ClusterPartitionReport(Collections.singletonList(
				new ClusterPartitionReport.ClusterPartitionReportEntry(dataSetId, new HashSet<>(Arrays.asList(partitionIds)), numTotalPartitions)
			)));
	}

	@FunctionalInterface
	private interface TaskExecutorSetup {
		void accept(TestingTaskExecutorGatewayBuilder taskExecutorGatewayBuilder) throws Exception;
	}

	@FunctionalInterface
	private interface TestAction {
		void accept(ResourceManagerGateway resourceManagerGateway, ResourceID taskExecutorId1, ResourceID taskExecutorId2) throws Exception;
	}
}
