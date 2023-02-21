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
import org.apache.flink.runtime.rpc.RpcUtils;
import org.apache.flink.runtime.security.token.DelegationTokenManager;
import org.apache.flink.util.TimeUtils;

import javax.annotation.Nullable;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Consumer;

/** Simple {@link ResourceManager} implementation for testing purposes. */
public class TestingResourceManager extends ResourceManager<ResourceID> {

    private final Consumer<ResourceID> stopWorkerConsumer;
    private final CompletableFuture<Void> readyToServeFuture;

    public TestingResourceManager(
            RpcService rpcService,
            UUID leaderSessionId,
            ResourceID resourceId,
            HeartbeatServices heartbeatServices,
            DelegationTokenManager delegationTokenManager,
            SlotManager slotManager,
            ResourceManagerPartitionTrackerFactory clusterPartitionTrackerFactory,
            BlocklistHandler.Factory blocklistHandlerFactory,
            JobLeaderIdService jobLeaderIdService,
            FatalErrorHandler fatalErrorHandler,
            ResourceManagerMetricGroup resourceManagerMetricGroup,
            Consumer<ResourceID> stopWorkerConsumer,
            CompletableFuture<Void> readyToServeFuture) {
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
                new ClusterInformation("localhost", 1234),
                fatalErrorHandler,
                resourceManagerMetricGroup,
                RpcUtils.INF_TIMEOUT,
                ForkJoinPool.commonPool());

        this.stopWorkerConsumer = stopWorkerConsumer;
        this.readyToServeFuture = readyToServeFuture;
    }

    @Override
    protected void initialize() throws ResourceManagerException {
        // noop
    }

    @Override
    protected void terminate() {
        // noop
    }

    @Override
    protected void internalDeregisterApplication(
            ApplicationStatus finalStatus, @Nullable String diagnostics)
            throws ResourceManagerException {
        // noop
    }

    @Override
    protected Optional<ResourceID> getWorkerNodeIfAcceptRegistration(ResourceID resourceID) {
        return Optional.of(resourceID);
    }

    @Override
    public void stopWorkerIfSupported(ResourceID worker) {
        stopWorkerConsumer.accept(worker);
    }

    @Override
    public CompletableFuture<Void> getReadyToServeFuture() {
        return readyToServeFuture;
    }

    @Override
    protected ResourceAllocator getResourceAllocator() {
        return NonSupportedResourceAllocatorImpl.INSTANCE;
    }

    public <T> CompletableFuture<T> runInMainThread(Callable<T> callable, Time timeout) {
        return callAsync(callable, TimeUtils.toDuration(timeout));
    }
}
