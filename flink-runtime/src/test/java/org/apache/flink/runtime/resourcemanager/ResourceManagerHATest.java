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
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerConfiguration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.testutils.category.New;
import org.apache.flink.util.TestLogger;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.mockito.Mockito.mock;

/**
 * resourceManager HA test, including grant leadership and revoke leadership
 */
@Category(New.class)
public class ResourceManagerHATest extends TestLogger {

	@Test
	public void testGrantAndRevokeLeadership() throws Exception {
		ResourceID rmResourceId = ResourceID.generate();
		RpcService rpcService = new TestingRpcService();

		CompletableFuture<UUID> leaderSessionIdFuture = new CompletableFuture<>();

		TestingLeaderElectionService leaderElectionService = new TestingLeaderElectionService() {
			@Override
			public void confirmLeaderSessionID(UUID leaderId) {
				leaderSessionIdFuture.complete(leaderId);
			}
		};

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

		MetricRegistryImpl metricRegistry = mock(MetricRegistryImpl.class);

		TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

		CompletableFuture<ResourceManagerId> revokedLeaderIdFuture = new CompletableFuture<>();

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
				new ClusterInformation("localhost", 1234),
				testingFatalErrorHandler) {

				@Override
				public void revokeLeadership() {
					super.revokeLeadership();
					runAsyncWithoutFencing(
						() -> revokedLeaderIdFuture.complete(getFencingToken()));
				}
			};

		try {
			resourceManager.start();

			Assert.assertNull(resourceManager.getFencingToken());
			final UUID leaderId = UUID.randomUUID();
			leaderElectionService.isLeader(leaderId);
			// after grant leadership, resourceManager's leaderId has value
			Assert.assertEquals(leaderId, leaderSessionIdFuture.get());
			// then revoke leadership, resourceManager's leaderId should be different
			leaderElectionService.notLeader();
			Assert.assertNotEquals(leaderId, revokedLeaderIdFuture.get());

			if (testingFatalErrorHandler.hasExceptionOccurred()) {
				testingFatalErrorHandler.rethrowError();
			}
		} finally {
			rpcService.stopService().get();
		}
	}
}
