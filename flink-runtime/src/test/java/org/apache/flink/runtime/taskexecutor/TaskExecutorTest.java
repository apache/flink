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

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.highavailability.NonHaServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.TestingLeaderRetrievalService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.util.UUID;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TaskExecutorTest extends TestLogger {

	@Test
	public void testImmediatelyRegistersIfLeaderIsKnown() throws Exception {
		final ResourceID resourceID = ResourceID.generate();
		final String resourceManagerAddress = "/resource/manager/address/one";

		final TestingRpcService rpc = new TestingRpcService();
		try {
			// register a mock resource manager gateway
			ResourceManagerGateway rmGateway = mock(ResourceManagerGateway.class);
			rpc.registerGateway(resourceManagerAddress, rmGateway);

			NonHaServices haServices = new NonHaServices(resourceManagerAddress);
			TaskExecutor taskManager = TaskExecutor.startTaskManagerComponentsAndActor(
				new Configuration(), resourceID, rpc, "localhost", haServices, true);
			String taskManagerAddress = taskManager.getAddress();
			taskManager.start();

			verify(rmGateway, timeout(5000)).registerTaskExecutor(
					any(UUID.class), eq(taskManagerAddress), eq(resourceID), any(Time.class));
		}
		finally {
			rpc.stopService();
		}
	}

	@Test
	public void testTriggerRegistrationOnLeaderChange() throws Exception {
		final ResourceID resourceID = ResourceID.generate();

		final String address1 = "/resource/manager/address/one";
		final String address2 = "/resource/manager/address/two";
		final UUID leaderId1 = UUID.randomUUID();
		final UUID leaderId2 = UUID.randomUUID();

		final TestingRpcService rpc = new TestingRpcService();
		try {
			// register the mock resource manager gateways
			ResourceManagerGateway rmGateway1 = mock(ResourceManagerGateway.class);
			ResourceManagerGateway rmGateway2 = mock(ResourceManagerGateway.class);
			rpc.registerGateway(address1, rmGateway1);
			rpc.registerGateway(address2, rmGateway2);

			TestingLeaderRetrievalService testLeaderService = new TestingLeaderRetrievalService();

			TestingHighAvailabilityServices haServices = new TestingHighAvailabilityServices();
			haServices.setResourceManagerLeaderRetriever(testLeaderService);

			TaskExecutor taskManager = TaskExecutor.startTaskManagerComponentsAndActor(
				new Configuration(), resourceID, rpc, "localhost", haServices, true);
			String taskManagerAddress = taskManager.getAddress();
			taskManager.start();

			// no connection initially, since there is no leader
			assertNull(taskManager.getResourceManagerConnection());

			// define a leader and see that a registration happens
			testLeaderService.notifyListener(address1, leaderId1);

			verify(rmGateway1, timeout(5000)).registerTaskExecutor(
					eq(leaderId1), eq(taskManagerAddress), eq(resourceID), any(Time.class));
			assertNotNull(taskManager.getResourceManagerConnection());

			// cancel the leader 
			testLeaderService.notifyListener(null, null);

			// set a new leader, see that a registration happens 
			testLeaderService.notifyListener(address2, leaderId2);

			verify(rmGateway2, timeout(5000)).registerTaskExecutor(
					eq(leaderId2), eq(taskManagerAddress), eq(resourceID), any(Time.class));
			assertNotNull(taskManager.getResourceManagerConnection());
		}
		finally {
			rpc.stopService();
		}
	}
}
