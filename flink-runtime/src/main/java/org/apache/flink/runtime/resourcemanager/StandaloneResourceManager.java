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
import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.clusterframework.ApplicationStatus;
import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.entrypoint.ClusterInformation;
import org.apache.flink.runtime.heartbeat.HeartbeatServices;
import org.apache.flink.runtime.io.network.partition.ResourceManagerPartitionTrackerFactory;
import org.apache.flink.runtime.metrics.groups.ResourceManagerMetricGroup;
import org.apache.flink.runtime.resourcemanager.exceptions.ResourceManagerException;
import org.apache.flink.runtime.resourcemanager.slotmanager.NonSupportedResourceAllocatorImpl;
import org.apache.flink.runtime.resourcemanager.slotmanager.ResourceAllocator;
import org.apache.flink.runtime.resourcemanager.slotmanager.SlotManager;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.rpc.RpcService;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;

/**
 * A standalone implementation of the resource manager. Used when the system is started in
 * standalone mode (via scripts), rather than via a resource framework like YARN.
 *
 * <p>This ResourceManager doesn't acquire new resources.
 */
public class StandaloneResourceManager extends ResourceManager<ResourceID> {

    /** The duration of the startup period. A duration of zero means there is no startup period. */
    private final Time startupPeriodTime;

    public StandaloneResourceManager(
            RpcService rpcService,
            UUID leaderSessionId,
            ResourceID resourceId,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            SlotManager slotManager,
            ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
            BlocklistHandler.Factory blocklistHandlerFactory,
            JobLeaderIdService jobLeaderIdService,
            ClusterInformation clusterInformation,
            FatalErrorHandler fatalErrorHandler,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            Time startupPeriodTime,
            Time rpcTimeout,
            Executor ioExecutor) {
        super(
                rpcService,
                leaderSessionId,
                resourceId,
                heartbeatServices,
                delegationTokenManager,
                slotManager,
                clusterPartitionTrackerFactory,
                blocklistHandlerFactory,
                jobLeaderIdService,
                clusterInformation,
                fatalErrorHandler,
                resourceManagerMetricGroup,
                rpcTimeout,
                ioExecutor);
        this.startupPeriodTime = Preconditions.checkNotNull(startupPeriodTime);
    }

    @Override
    protected void initialize() throws ResourceManagerException {
        startStartupPeriod();
    }

    @Override
    protected void terminate() {
        // noop
    }

    @Override
    protected void internalDeregisterApplication(
            ApplicationStatus finalStatus, @Nullable String diagnostics) {}

    @Override
    protected Optional<ResourceID> getWorkerNodeIfAcceptRegistration(ResourceID resourceID) {
        return Optional.of(resourceID);
    }

    private void startStartupPeriod() {
        setFailUnfulfillableRequest(false);

        final long startupPeriodMillis = startupPeriodTime.toMilliseconds();

        if (startupPeriodMillis > 0) {
            scheduleRunAsync(
                    () -> setFailUnfulfillableRequest(true),
                    startupPeriodMillis,
                    TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public CompletableFuture<Void> getReadyToServeFuture() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    protected ResourceAllocator getResourceAllocator() {
        return NonSupportedResourceAllocatorImpl.INSTANCE;
    }
}
