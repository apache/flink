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

import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.MainThreadExecutor;
import org.apache.flink.runtime.rpc.RpcEndpoint;
import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.StartStoppable;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * resourceManager HA test, including grant leadership and revoke leadership
 */
public class ResourceManagerHATest {

	@Test
	public void testGrantAndRevokeLeadership() throws Exception {
		// mock a RpcService which will return a special RpcGateway when call its startServer method, the returned RpcGateway directly execute runAsync call
		TestingResourceManagerGatewayProxy gateway = mock(TestingResourceManagerGatewayProxy.class);
		doCallRealMethod().when(gateway).runAsync(any(Runnable.class));

		RpcService rpcService = mock(RpcService.class);
		when(rpcService.startServer(any(RpcEndpoint.class))).thenReturn(gateway);

		TestingLeaderElectionService leaderElectionService = new TestingLeaderElectionService();
		TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		highAvailabilityServices.setResourceManagerLeaderElectionService(leaderElectionService);

		SlotManager slotManager = mock(SlotManager.class);
		final ResourceManager resourceManager = new ResourceManager(rpcService, highAvailabilityServices, slotManager);
		resourceManager.start();
		// before grant leadership, resourceManager's leaderId is null
		Assert.assertNull(resourceManager.getLeaderSessionID());
		final UUID leaderId = UUID.randomUUID();
		leaderElectionService.isLeader(leaderId);
		// after grant leadership, resourceManager's leaderId has value
		Assert.assertEquals(leaderId, resourceManager.getLeaderSessionID());
		// then revoke leadership, resourceManager's leaderId is null again
		leaderElectionService.notLeader();
		Assert.assertNull(resourceManager.getLeaderSessionID());
	}

	private static abstract class TestingResourceManagerGatewayProxy implements MainThreadExecutor, StartStoppable, RpcGateway {
		@Override
		public void runAsync(Runnable runnable) {
			runnable.run();
		}
	}

}
