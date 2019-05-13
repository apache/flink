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

import org.apache.flink.api.common.time.Time;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.clusterframework.types.SlotID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.instance.HardwareDescription;
import org.apache.flink.runtime.leaderelection.LeaderElectionService;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerBuilder;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.rpc.exceptions.FencingTokenException;
import org.apache.flink.runtime.taskexecutor.SlotReport;
import org.apache.flink.runtime.taskexecutor.SlotStatus;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.TestLogger;

import akka.pattern.AskTimeoutException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ResourceManagerTaskExecutorTest extends TestLogger {

	private static final Time TIMEOUT = Time.seconds(10L);

	private static final long HEARTBEAT_TIMEOUT = 5000;

	private static TestingRpcService rpcService;

	private int dataPort = 1234;

	private HardwareDescription hardwareDescription = new HardwareDescription(1, 2L, 3L, 4L);

	private static String taskExecutorAddress = "/taskExecutor1";

	private ResourceID taskExecutorResourceID;

	private ResourceID resourceManagerResourceID;

	private StandaloneResourceManager resourceManager;

	private ResourceManagerGateway rmGateway;

	private ResourceManagerGateway wronglyFencedGateway;

	private TestingFatalErrorHandler testingFatalErrorHandler;

	@Before
	public void setup() throws Exception {
		rpcService = new TestingRpcService();

		taskExecutorResourceID = mockTaskExecutor(taskExecutorAddress);
		resourceManagerResourceID = ResourceID.generate();
		testingFatalErrorHandler = new TestingFatalErrorHandler();
		TestingLeaderElectionService rmLeaderElectionService = new TestingLeaderElectionService();
		resourceManager = createAndStartResourceManager(rmLeaderElectionService, testingFatalErrorHandler);
		rmGateway = resourceManager.getSelfGateway(ResourceManagerGateway.class);
		wronglyFencedGateway = rpcService.connect(resourceManager.getAddress(), ResourceManagerId.generate(), ResourceManagerGateway.class)
			.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);

		grantLeadership(rmLeaderElectionService).get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
	}

	@After
	public void teardown() throws Exception {
		RpcUtils.terminateRpcService(rpcService, TIMEOUT);
	}

	/**
	 * Test receive normal registration from task executor and receive duplicate registration
	 * from task executor.
	 */
	@Test
	public void testRegisterTaskExecutor() throws Exception {
		try {
			// test response successful
			CompletableFuture<RegistrationResponse> successfulFuture =
				rmGateway.registerTaskExecutor(taskExecutorAddress, taskExecutorResourceID, dataPort, hardwareDescription, TIMEOUT);
			RegistrationResponse response = successfulFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
			assertTrue(response instanceof TaskExecutorRegistrationSuccess);
			final TaskManagerInfo taskManagerInfo = rmGateway.requestTaskManagerInfo(
				taskExecutorResourceID,
				TIMEOUT).get();
			assertThat(taskManagerInfo.getResourceId(), equalTo(taskExecutorResourceID));

			// test response successful with instanceID not equal to previous when receive duplicate registration from taskExecutor
			CompletableFuture<RegistrationResponse> duplicateFuture =
				rmGateway.registerTaskExecutor(taskExecutorAddress, taskExecutorResourceID, dataPort, hardwareDescription, TIMEOUT);
			RegistrationResponse duplicateResponse = duplicateFuture.get();
			assertTrue(duplicateResponse instanceof TaskExecutorRegistrationSuccess);
			assertNotEquals(((TaskExecutorRegistrationSuccess) response).getRegistrationId(), ((TaskExecutorRegistrationSuccess) duplicateResponse).getRegistrationId());
		} finally {
			if (testingFatalErrorHandler.hasExceptionOccurred()) {
				testingFatalErrorHandler.rethrowError();
			}
		}
	}

	/**
	 * Test receive registration with unmatched leadershipId from task executor.
	 */
	@Test
	public void testRegisterTaskExecutorWithUnmatchedLeaderSessionId() throws Exception {
		try {
			// test throw exception when receive a registration from taskExecutor which takes unmatched leaderSessionId
			CompletableFuture<RegistrationResponse> unMatchedLeaderFuture =
				wronglyFencedGateway.registerTaskExecutor(taskExecutorAddress, taskExecutorResourceID, dataPort, hardwareDescription, TIMEOUT);

			try {
				unMatchedLeaderFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
				fail("Should have failed because we are using a wrongly fenced ResourceManagerGateway.");
			} catch (ExecutionException e) {
				assertTrue(ExceptionUtils.stripExecutionException(e) instanceof FencingTokenException);
			}
		} finally {
			if (testingFatalErrorHandler.hasExceptionOccurred()) {
				testingFatalErrorHandler.rethrowError();
			}
		}
	}

	/**
	 * Test receive registration with invalid address from task executor.
	 */
	@Test
	public void testRegisterTaskExecutorFromInvalidAddress() throws Exception {
		try {
			// test throw exception when receive a registration from taskExecutor which takes invalid address
			String invalidAddress = "/taskExecutor2";
			CompletableFuture<RegistrationResponse> invalidAddressFuture =
				rmGateway.registerTaskExecutor(invalidAddress, taskExecutorResourceID, dataPort, hardwareDescription, TIMEOUT);
			assertTrue(invalidAddressFuture.get(TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS) instanceof RegistrationResponse.Decline);
		} finally {
			if (testingFatalErrorHandler.hasExceptionOccurred()) {
				testingFatalErrorHandler.rethrowError();
			}
		}
	}

	private ResourceID mockTaskExecutor(String taskExecutorAddress) {
		TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		when(taskExecutorGateway.getAddress()).thenReturn(taskExecutorAddress);

		ResourceID taskExecutorResourceID = ResourceID.generate();
		rpcService.registerGateway(taskExecutorAddress, taskExecutorGateway);
		return taskExecutorResourceID;
	}

	private StandaloneResourceManager createAndStartResourceManager(LeaderElectionService rmLeaderElectionService, FatalErrorHandler fatalErrorHandler) throws Exception {
		TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		HeartbeatServices heartbeatServices = new HeartbeatServices(1000L, HEARTBEAT_TIMEOUT);
		highAvailabilityServices.setResourceManagerLeaderElectionService(rmLeaderElectionService);

		SlotManager slotManager = SlotManagerBuilder.newBuilder()
			.setScheduledExecutor(rpcService.getScheduledExecutor())
			.build();

		MetricRegistryImpl metricRegistry = mock(MetricRegistryImpl.class);
		JobLeaderIdService jobLeaderIdService = new JobLeaderIdService(
			highAvailabilityServices,
			rpcService.getScheduledExecutor(),
			Time.minutes(5L));

		StandaloneResourceManager resourceManager =
			new StandaloneResourceManager(
				rpcService,
				FlinkResourceManager.RESOURCE_MANAGER_NAME,
				resourceManagerResourceID,
				highAvailabilityServices,
				heartbeatServices,
				slotManager,
				metricRegistry,
				jobLeaderIdService,
				new ClusterInformation("localhost", 1234),
				fatalErrorHandler,
				UnregisteredMetricGroups.createUnregisteredJobManagerMetricGroup());

		resourceManager.start();

		return resourceManager;
	}

	private CompletableFuture<UUID> grantLeadership(TestingLeaderElectionService leaderElectionService) {
		UUID leaderSessionId = UUID.randomUUID();
		return leaderElectionService.isLeader(leaderSessionId);
	}

	/**
	 * Test delayed registration of task executor where the delay is introduced during connection from resource manager
	 * to the registering task executor.
	 */
	@Test
	public void testDelayedRegisterTaskExecutor() throws Exception {
		final Time fastTimeout = Time.milliseconds(1L);
		try {
			final OneShotLatch startConnection = new OneShotLatch();
			final OneShotLatch finishConnection = new OneShotLatch();

			// first registration is with blocking connection
			rpcService.setRpcGatewayFutureFunction(rpcGateway ->
				CompletableFuture.supplyAsync(
					() -> {
						startConnection.trigger();
						try {
							finishConnection.await();
						} catch (InterruptedException ignored) {}
						return rpcGateway;
					},
					TestingUtils.defaultExecutor()));

			CompletableFuture<RegistrationResponse> firstFuture =
				rmGateway.registerTaskExecutor(taskExecutorAddress, taskExecutorResourceID, dataPort, hardwareDescription, fastTimeout);
			try {
				firstFuture.get();
				fail("Should have failed because connection to taskmanager is delayed beyond timeout");
			} catch (Exception e) {
				assertThat(ExceptionUtils.stripExecutionException(e), instanceOf(AskTimeoutException.class));
			}

			startConnection.await();

			// second registration after timeout is with no delay, expecting it to be succeeded
			rpcService.resetRpcGatewayFutureFunction();
			CompletableFuture<RegistrationResponse> secondFuture =
				rmGateway.registerTaskExecutor(taskExecutorAddress, taskExecutorResourceID, dataPort, hardwareDescription, TIMEOUT);
			RegistrationResponse response = secondFuture.get();
			assertTrue(response instanceof TaskExecutorRegistrationSuccess);

			// on success, send slot report for taskmanager registration
			final SlotReport slotReport = new SlotReport(new SlotStatus(new SlotID(taskExecutorResourceID, 0), ResourceProfile.UNKNOWN));
			rmGateway.sendSlotReport(taskExecutorResourceID,
				((TaskExecutorRegistrationSuccess) response).getRegistrationId(), slotReport, TIMEOUT).get();

			// let the remaining part of the first registration proceed
			finishConnection.trigger();
			Thread.sleep(1L);

			// verify that the latest registration is valid not being unregistered by the delayed one
			final TaskManagerInfo taskManagerInfo = rmGateway.requestTaskManagerInfo(
				taskExecutorResourceID,
				TIMEOUT).get();
			assertThat(taskManagerInfo.getResourceId(), equalTo(taskExecutorResourceID));
			assertThat(taskManagerInfo.getNumberSlots(), equalTo(1));
		} finally {
			rpcService.resetRpcGatewayFutureFunction();
		}
	}
}
