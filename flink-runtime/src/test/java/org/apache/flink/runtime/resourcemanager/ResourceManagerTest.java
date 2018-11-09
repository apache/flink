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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.NoOpMetricRegistry;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TestingTaskExecutorGatewayBuilder;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class ResourceManagerTest extends TestLogger {

	private TestingRpcService rpcService;

	@Before
	public void setUp() {
		rpcService = new TestingRpcService();
	}

	@After
	public void tearDown() throws ExecutionException, InterruptedException {
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
}
