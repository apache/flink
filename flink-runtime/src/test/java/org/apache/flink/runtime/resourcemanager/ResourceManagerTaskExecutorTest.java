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
import org.apache.flink.runtime.concurrent.Future;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.resourcemanager.slotmanager.SimpleSlotManager;
import org.apache.flink.runtime.rpc.TestingSerialRpcService;
import org.apache.flink.runtime.registration.RegistrationResponse;
import org.apache.flink.runtime.rpc.exceptions.LeaderSessionIDException;
import org.apache.flink.runtime.taskexecutor.TaskExecutorGateway;
import org.apache.flink.runtime.taskexecutor.TaskExecutorRegistrationSuccess;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class ResourceManagerTaskExecutorTest {

	private TestingSerialRpcService rpcService;

	@Before
	public void setup() throws Exception {
		rpcService = new TestingSerialRpcService();
	}

	@After
	public void teardown() throws Exception {
		rpcService.stopService();
	}

	/**
	 * Test receive normal registration from task executor and receive duplicate registration from task executor
	 */
	@Test
	public void testRegisterTaskExecutor() throws Exception {
		String taskExecutorAddress = "/taskExecutor1";
		ResourceID taskExecutorResourceID = mockTaskExecutor(taskExecutorAddress);
		TestingLeaderElectionService rmLeaderElectionService = new TestingLeaderElectionService();
		final ResourceManager resourceManager = createAndStartResourceManager(rmLeaderElectionService);
		final UUID leaderSessionId = grantLeadership(rmLeaderElectionService);

		// test response successful
		Future<RegistrationResponse> successfulFuture = resourceManager.registerTaskExecutor(leaderSessionId, taskExecutorAddress, taskExecutorResourceID);
		RegistrationResponse response = successfulFuture.get(5, TimeUnit.SECONDS);
		assertTrue(response instanceof TaskExecutorRegistrationSuccess);

		// test response successful with instanceID not equal to previous when receive duplicate registration from taskExecutor
		Future<RegistrationResponse> duplicateFuture = resourceManager.registerTaskExecutor(leaderSessionId, taskExecutorAddress, taskExecutorResourceID);
		RegistrationResponse duplicateResponse = duplicateFuture.get();
		assertTrue(duplicateResponse instanceof TaskExecutorRegistrationSuccess);
		assertNotEquals(((TaskExecutorRegistrationSuccess) response).getRegistrationId(), ((TaskExecutorRegistrationSuccess) duplicateResponse).getRegistrationId());
	}

	/**
	 * Test receive registration with unmatched leadershipId from task executor
	 */
	@Test
	public void testRegisterTaskExecutorWithUnmatchedLeaderSessionId() throws Exception {
		String taskExecutorAddress = "/taskExecutor1";
		ResourceID taskExecutorResourceID = mockTaskExecutor(taskExecutorAddress);
		TestingLeaderElectionService rmLeaderElectionService = new TestingLeaderElectionService();
		final ResourceManager resourceManager = createAndStartResourceManager(rmLeaderElectionService);
		final UUID leaderSessionId = grantLeadership(rmLeaderElectionService);

		// test throw exception when receive a registration from taskExecutor which takes unmatched leaderSessionId
		UUID differentLeaderSessionID = UUID.randomUUID();
		Future<RegistrationResponse> unMatchedLeaderFuture = resourceManager.registerTaskExecutor(differentLeaderSessionID, taskExecutorAddress, taskExecutorResourceID);
		assertTrue(unMatchedLeaderFuture.get(5, TimeUnit.SECONDS) instanceof RegistrationResponse.Decline);
	}

	/**
	 * Test receive registration with invalid address from task executor
	 */
	@Test
	public void testRegisterTaskExecutorFromInvalidAddress() throws Exception {
		String taskExecutorAddress = "/taskExecutor1";
		ResourceID taskExecutorResourceID = mockTaskExecutor(taskExecutorAddress);
		TestingLeaderElectionService leaderElectionService = new TestingLeaderElectionService();
		final ResourceManager resourceManager = createAndStartResourceManager(leaderElectionService);
		final UUID leaderSessionId = grantLeadership(leaderElectionService);

		// test throw exception when receive a registration from taskExecutor which takes invalid address
		String invalidAddress = "/taskExecutor2";
		Future<RegistrationResponse> invalidAddressFuture = resourceManager.registerTaskExecutor(leaderSessionId, invalidAddress, taskExecutorResourceID);
		assertTrue(invalidAddressFuture.get(5, TimeUnit.SECONDS) instanceof RegistrationResponse.Decline);
	}

	private ResourceID mockTaskExecutor(String taskExecutorAddress) {
		TaskExecutorGateway taskExecutorGateway = mock(TaskExecutorGateway.class);
		ResourceID taskExecutorResourceID = ResourceID.generate();
		rpcService.registerGateway(taskExecutorAddress, taskExecutorGateway);
		return taskExecutorResourceID;
	}

	private ResourceManager createAndStartResourceManager(TestingLeaderElectionService rmLeaderElectionService) {
		TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		highAvailabilityServices.setResourceManagerLeaderElectionService(rmLeaderElectionService);
		ResourceManager resourceManager = new StandaloneResourceManager(rpcService, highAvailabilityServices, new SimpleSlotManager());
		resourceManager.start();
		return resourceManager;
	}

	private UUID grantLeadership(TestingLeaderElectionService leaderElectionService) {
		UUID leaderSessionId = UUID.randomUUID();
		leaderElectionService.isLeader(leaderSessionId);
		return leaderSessionId;
	}

}
