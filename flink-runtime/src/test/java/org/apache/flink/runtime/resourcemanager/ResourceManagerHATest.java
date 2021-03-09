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
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ResourceManagerOptions;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.TestingHeartbeatServices;
import org.apache.flink.runtime.highavailability.TestingHighAvailabilityServices;
import org.apache.flink.runtime.io.network.partition.NoOpResourceManagerPartitionTracker;
import org.apache.flink.runtime.leaderelection.TestingLeaderElectionService;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.resourcemanager.slotmanager.AnyMatchingSlotMatchingStrategy;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManagerConfiguration;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.rpc.TestingRpcService;
import org.apache.flink.runtime.testingUtils.TestingUtils;
import org.apache.flink.runtime.util.TestingFatalErrorHandler;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;

/** ResourceManager HA test, including grant leadership and revoke leadership. */
public class ResourceManagerHATest extends TestLogger {

    @Test
    public void testGrantAndRevokeLeadership() throws Exception {
        ResourceID rmResourceId = ResourceID.generate();
        RpcService rpcService = new TestingRpcService();

        CompletableFuture<UUID> leaderSessionIdFuture = new CompletableFuture<>();

        TestingLeaderElectionService leaderElectionService =
                new TestingLeaderElectionService() {
                    @Override
                    public void confirmLeadership(UUID leaderId, String leaderAddress) {
                        leaderSessionIdFuture.complete(leaderId);
                    }
                };

        TestingHighAvailabilityServices highAvailabilityServices =
                new TestingHighAvailabilityServices();
        highAvailabilityServices.setResourceManagerLeaderElectionService(leaderElectionService);

        TestingHeartbeatServices heartbeatServices = new TestingHeartbeatServices();

        final Configuration configuration = new Configuration();
        ResourceManagerRuntimeServicesConfiguration resourceManagerRuntimeServicesConfiguration =
                new ResourceManagerRuntimeServicesConfiguration(
                        Time.seconds(5L),
                        new SlotManagerConfiguration(
                                TestingUtils.infiniteTime(),
                                TestingUtils.infiniteTime(),
                                TestingUtils.infiniteTime(),
                                true,
                                AnyMatchingSlotMatchingStrategy.INSTANCE,
                                WorkerResourceSpec.ZERO,
                                1,
                                ResourceManagerOptions.MAX_SLOT_NUM.defaultValue(),
                                ResourceManagerOptions.REDUNDANT_TASK_MANAGER_NUM.defaultValue()),
                        ClusterOptions.isDeclarativeResourceManagementEnabled(configuration),
                        ClusterOptions.isFineGrainedResourceManagementEnabled(configuration));
        ResourceManagerRuntimeServices resourceManagerRuntimeServices =
                ResourceManagerRuntimeServices.fromConfiguration(
                        resourceManagerRuntimeServicesConfiguration,
                        highAvailabilityServices,
                        rpcService.getScheduledExecutor(),
                        UnregisteredMetricGroups.createUnregisteredSlotManagerMetricGroup());

        TestingFatalErrorHandler testingFatalErrorHandler = new TestingFatalErrorHandler();

        CompletableFuture<ResourceManagerId> revokedLeaderIdFuture = new CompletableFuture<>();

        final ResourceManager resourceManager =
                new StandaloneResourceManager(
                        rpcService,
                        rmResourceId,
                        highAvailabilityServices,
                        heartbeatServices,
                        resourceManagerRuntimeServices.getSlotManager(),
                        NoOpResourceManagerPartitionTracker::get,
                        resourceManagerRuntimeServices.getJobLeaderIdService(),
                        new ClusterInformation("localhost", 1234),
                        testingFatalErrorHandler,
                        UnregisteredMetricGroups.createUnregisteredResourceManagerMetricGroup(),
                        Time.minutes(5L),
                        RpcUtils.INF_TIMEOUT,
                        ForkJoinPool.commonPool()) {

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
