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

package org.apache.flink.runtime.resourcemanager;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGateway;
import org.apache.flink.runtime.jobmaster.utils.TestingJobMasterGatewayBuilder;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.leaderretrieval.LeaderRetrievalService;
import org.apache.flink.runtime.leaderretrieval.SettableLeaderRetrievalService;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerBuilder;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for the new ResourceManager.
 */
public class ResourceManagerTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);

	private TestingRpcService rpcService;

	private TestingHighAvailabilityServices highAvailabilityServices;

	@Before
	public void setUp() {
		highAvailabilityServices = new TestingHighAvailabilityServices();
		rpcService = new TestingRpcService();
	}

	@After
	public void tearDown() throws Exception {
		if (highAvailabilityServices != null) {
			highAvailabilityServices.closeAndCleanupAllData();
			highAvailabilityServices = null;
		}

		if (rpcService != null) {
			rpcService.stopService().get();
			rpcService = null;
		}
	}

	/**
	 * Tests that we can retrieve the correct {@link TaskManagerInfo} from the {@link ResourceManager}.
	 */
	@Test
	public void testRequestTaskManagerInfo() throws Exception {
		final TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		final SlotManager slotManager = new SlotManager(
			rpcService.getScheduledExecutor(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime(),
			TestingUtils.infiniteTime());
		final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			highAvailabilityServices,
			rpcService.getScheduledExecutor(),
			TestingUtils.infiniteTime());
		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		final TestingLeaderElectionService resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		highAvailabilityServices.setResourceManagerLeaderElectionService(resourceManagerLeaderElectionService);

		final TestingResourceManager resourceManager = new TestingResourceManager(
			rpcService,
			ResourceManager.RESOURCE_MANAGER_NAME,
			ResourceID.generate(),
			highAvailabilityServices,
			new HeartbeatServices(1000L, 10000L),
			slotManager,
			NoOpMetricRegistry.INSTANCE,
			jobLeaderIdService,
			testingFatalErrorHandler,
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup());

		resourceManager.start();

		try {
			final ResourceID taskManagerId = ResourceID.generate();
			final ResourceManagerGateway resourceManagerGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
			final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder().createTestingTaskExecutorGateway();

			// first make the ResourceManager the leader
			resourceManagerLeaderElectionService.isLeader(UUID.randomUUID()).get();

			rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

			final HardwareDescription hardwareDescription = new HardwareDescription(
				42,
				1337L,
				1337L,
				0L);

			final int dataPort = 1234;

			CompletableFuture<RegistrationResponse> registrationResponseFuture = resourceManagerGateway.registerTaskExecutor(
				taskExecutorGateway.getAddress(),
				taskManagerId,
				dataPort,
				hardwareDescription,
				TestingUtils.TIMEOUT());

			Assert.assertTrue(registrationResponseFuture.get() instanceof RegistrationResponse.Success);

			CompletableFuture<TaskManagerInfo> taskManagerInfoFuture = resourceManagerGateway.requestTaskManagerInfo(
				taskManagerId,
				TestingUtils.TIMEOUT());

			TaskManagerInfo taskManagerInfo = taskManagerInfoFuture.get();

			Assert.assertEquals(taskManagerId, taskManagerInfo.getResourceId());
			Assert.assertEquals(hardwareDescription, taskManagerInfo.getHardwareDescription());
			Assert.assertEquals(taskExecutorGateway.getAddress(), taskManagerInfo.getAddress());
			Assert.assertEquals(dataPort, taskManagerInfo.getDataPort());
			Assert.assertEquals(0, taskManagerInfo.getNumberSlots());
			Assert.assertEquals(0, taskManagerInfo.getNumberAvailableSlots());

			testingFatalErrorHandler.rethrowError();
		} finally {
			RpcUtils.terminateRpcEndpoint(resourceManager, TestingUtils.TIMEOUT());
			highAvailabilityServices.close();
		}
	}

	@Test
	public void testHeartbeatTimeoutWithJobMaster() throws Exception {
		final CompletableFuture<ResourceID> heartbeatRequestFuture = new CompletableFuture<>();
		final CompletableFuture<ResourceManagerId> disconnectFuture = new CompletableFuture<>();
		final TestingJobMasterGateway jobMasterGateway = new TestingJobMasterGatewayBuilder()
			.setResourceManagerHeartbeatConsumer(heartbeatRequestFuture::complete)
			.setDisconnectResourceManagerConsumer(disconnectFuture::complete)
			.build();
		rpcService.registerGateway(jobMasterGateway.getAddress(), jobMasterGateway);
		final JobID jobId = new JobID();
		final ResourceID jobMasterResourceId = ResourceID.generate();
		final LeaderRetrievalService jobMasterLeaderRetrievalService = new SettableLeaderRetrievalService(jobMasterGateway.getAddress(), jobMasterGateway.getFencingToken().toUUID());

		highAvailabilityServices.setJobMasterLeaderRetriever(jobId, jobMasterLeaderRetrievalService);
		final ResourceManagerId resourceManagerId = ResourceManagerId.generate();

		runHeartbeatTimeoutTest(
			resourceManagerId,
			resourceManagerGateway -> {
				final CompletableFuture<RegistrationResponse> registrationFuture = resourceManagerGateway.registerJobManager(
					jobMasterGateway.getFencingToken(),
					jobMasterResourceId,
					jobMasterGateway.getAddress(),
					jobId,
					TIMEOUT);

				assertThat(registrationFuture.get(), instanceOf(RegistrationResponse.Success.class));
			},
			resourceManagerResourceId -> {
				final ResourceID optionalHeartbeatRequest = heartbeatRequestFuture.getNow(null);
				assertThat(optionalHeartbeatRequest, anyOf(is(resourceManagerResourceId), is(nullValue())));
				assertThat(disconnectFuture.get(), is(equalTo(resourceManagerId)));
			});
	}

	@Test
	public void testHeartbeatTimeoutWithTaskExecutor() throws Exception {
		final ResourceID taskExecutorId = ResourceID.generate();
		final CompletableFuture<ResourceID> heartbeatRequestFuture = new CompletableFuture<>();
		final CompletableFuture<Exception> disconnectFuture = new CompletableFuture<>();
		final TaskExecutorGateway taskExecutorGateway = new TestingTaskExecutorGatewayBuilder()
			.setDisconnectResourceManagerConsumer(disconnectFuture::complete)
			.setHeartbeatResourceManagerConsumer(heartbeatRequestFuture::complete)
			.createTestingTaskExecutorGateway();
		rpcService.registerGateway(taskExecutorGateway.getAddress(), taskExecutorGateway);

		runHeartbeatTimeoutTest(
			ResourceManagerId.generate(),
			resourceManagerGateway -> {
				registerTaskExecutor(resourceManagerGateway, taskExecutorId, taskExecutorGateway.getAddress());
			},
			resourceManagerResourceId -> {
				final ResourceID optionalHeartbeatRequest = heartbeatRequestFuture.getNow(null);
				assertThat(optionalHeartbeatRequest, anyOf(is(resourceManagerResourceId), is(nullValue())));
				assertThat(disconnectFuture.get(), instanceOf(TimeoutException.class));
			}
		);
	}

	private void runHeartbeatTimeoutTest(
		ResourceManagerId resourceManagerId,
		ThrowingConsumer<ResourceManagerGateway, Exception> registerComponentAtResourceManager,
		ThrowingConsumer<ResourceID, Exception> verifyHeartbeatTimeout) throws Exception {
		final TestingLeaderElectionService resourceManagerLeaderElectionService = new TestingLeaderElectionService();
		highAvailabilityServices.setResourceManagerLeaderElectionService(resourceManagerLeaderElectionService);

		final ResourceID resourceManagerResourceId = ResourceID.generate();
		final TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();
		final org.apache.flink.runtime.resourcemanager.TestingResourceManager testingResourceManager = createAndStartResourceManager(
			rpcService,
			resourceManagerResourceId,
			new HeartbeatServices(1L, 1L),
			testingFatalErrorHandler);

		// first make the ResourceManager the leader
		resourceManagerLeaderElectionService.isLeader(resourceManagerId.toUUID()).get();

		final ResourceManagerGateway resourceManagerGateway = testingResourceManager.getSelfGateway(ResourceManagerGateway.class);

		try {
			registerComponentAtResourceManager.accept(resourceManagerGateway);
			verifyHeartbeatTimeout.accept(resourceManagerResourceId);
		} finally {
			RpcUtils.terminateRpcEndpoint(testingResourceManager, TIMEOUT);
		}

		testingFatalErrorHandler.rethrowError();
	}

	private org.apache.flink.runtime.resourcemanager.TestingResourceManager createAndStartResourceManager(
		RpcService rpcService,
		ResourceID resourceManagerResourceId,
		HeartbeatServices heartbeatServices,
		TestingFatalErrorHandler testingFatalErrorHandler) throws Exception {
		final SlotManager slotManager = SlotManagerBuilder.newBuilder()
			.setScheduledExecutor(rpcService.getScheduledExecutor())
			.build();
		final JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			highAvailabilityServices,
			rpcService.getScheduledExecutor(),
			TestingUtils.infiniteTime());

		final org.apache.flink.runtime.resourcemanager.TestingResourceManager resourceManager = new org.apache.flink.runtime.resourcemanager.TestingResourceManager(
			rpcService,
			ResourceManager.RESOURCE_MANAGER_NAME + UUID.randomUUID(),
			resourceManagerResourceId,
			highAvailabilityServices,
			heartbeatServices,
			slotManager,
			NoOpMetricRegistry.INSTANCE,
			jobLeaderIdService,
			testingFatalErrorHandler,
			UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup());

		resourceManager.start();

		return resourceManager;
	}

	private void registerTaskExecutor(ResourceManagerGateway resourceManagerGateway, ResourceID taskExecutorId, String taskExecutorAddress) throws Exception {
		final CompletableFuture<RegistrationResponse> registrationFuture = resourceManagerGateway.registerTaskExecutor(
			taskExecutorAddress,
			taskExecutorId,
			1234,
			new HardwareDescription(42, 1337L, 1337L, 0L),
			TestingUtils.TIMEOUT());

		assertThat(registrationFuture.get(), instanceOf(RegistrationResponse.Success.class));
	}
}
