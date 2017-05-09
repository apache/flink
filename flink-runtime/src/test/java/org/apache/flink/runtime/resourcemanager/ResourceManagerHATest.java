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
import org.apache.flink.runtime.clusterframework.FlinkResourceManager;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistry;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerConfiguration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingSerialRpcService;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;

import static org.mockito.Mockito.mock;

/**
 * resourceManager HA test, including grant leadership and revoke leadership
 */
public class ResourceManagerHATest extends TestLogger {

	@Test
	public void testGrantAndRevokeLeadership() throws Exception {
		ResourceID rmResourceId = ResourceID.generate();
		RpcService rpcService = new TestingSerialRpcService();

		TestingLeaderElectionService leaderElectionService = new TestingLeaderElectionService();
		TestingHighAvailabilityServices highAvailabilityServices = new TestingHighAvailabilityServices();
		highAvailabilityServices.setResourceManagerLeaderElectionService(leaderElectionService);

		HeartbeatServices heartbeatServices = mock(HeartbeatServices.class);

		ResourceManagerConfiguration resourceManagerConfiguration = new ResourceManagerConfiguration(
			Time.seconds(5L),
			Time.seconds(5L));

		ResourceManagerRuntimeServicesConfiguration resourceManagerRuntimeServicesConfiguration = new ResourceManagerRuntimeServicesConfiguration(
			Time.seconds(5L),
			new SlotManagerConfiguration(
				TestingUtils.infiniteTime(),
				TestingUtils.infiniteTime(),
				TestingUtils.infiniteTime()));
		ResourceManagerRuntimeServices resourceManagerRuntimeServices = ResourceManagerRuntimeServices.fromConfiguration(
			resourceManagerRuntimeServicesConfiguration,
			highAvailabilityServices,
			rpcService.getScheduledExecutor());

		MetricRegistry metricRegistry = mock(MetricRegistry.class);

		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		final ResourceManager resourceManager =
			new StandaloneResourceManager(
				rpcService,
				FlinkResourceManager.RESOURCE_MANAGER_NAME,
				rmResourceId,
				resourceManagerConfiguration,
				highAvailabilityServices,
				heartbeatServices,
				resourceManagerRuntimeServices.getSlotManager(),
				metricRegistry,
				resourceManagerRuntimeServices.getJobLeaderIdService(),
				testingFatalErrorHandler);
		resourceManager.start();
		// before grant leadership, resourceManager's leaderId is null
		Assert.assertEquals(null, resourceManager.getLeaderSessionId());
		final UUID leaderId = UUID.randomUUID();
		leaderElectionService.isLeader(leaderId);
		// after grant leadership, resourceManager's leaderId has value
		Assert.assertEquals(leaderId, resourceManager.getLeaderSessionId());
		// then revoke leadership, resourceManager's leaderId is null again
		leaderElectionService.notLeader();
		Assert.assertEquals(null, resourceManager.getLeaderSessionId());

		if (testingFatalErrorHandler.hasExceptionOccurred()) {
			testingFatalErrorHandler.rethrowError();
		}
	}
}
