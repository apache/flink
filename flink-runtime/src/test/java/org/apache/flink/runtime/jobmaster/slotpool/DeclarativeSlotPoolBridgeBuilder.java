/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster.slotpool;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.jobmaster.JobMasterId;
import org.apache.flink.runtime.resourcemanager.ResourceManagerGateway;
import org.apache.flink.runtime.resourcemanager.utils.TestingResourceManagerGateway;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.configuration.JobManagerOptions.SLOT_REQUEST_MAX_INTERVAL;
import static org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter.forMainThread;

/** Builder for a {@link DeclarativeSlotPoolBridge}. */
public class DeclarativeSlotPoolBridgeBuilder {

    private JobID jobId = new JobID();
    private Duration batchSlotTimeout = JobManagerOptions.SLOT_IDLE_TIMEOUT.defaultValue();
    private Duration idleSlotTimeout = TestingUtils.infiniteDuration();
    private Clock clock = SystemClock.getInstance();
    private Duration slotRequestMaxInterval = SLOT_REQUEST_MAX_INTERVAL.defaultValue();
    private ComponentMainThreadExecutor mainThreadExecutor = forMainThread();
    private boolean slotBatchAllocatable = false;

    @Nullable
    private ResourceManagerGateway resourceManagerGateway = new TestingResourceManagerGateway();

    private RequestSlotMatchingStrategy requestSlotMatchingStrategy =
            SimpleRequestSlotMatchingStrategy.INSTANCE;

    public DeclarativeSlotPoolBridgeBuilder setResourceManagerGateway(
            @Nullable ResourceManagerGateway resourceManagerGateway) {
        this.resourceManagerGateway = resourceManagerGateway;
        return this;
    }

    public DeclarativeSlotPoolBridgeBuilder setBatchSlotTimeout(Duration batchSlotTimeout) {
        this.batchSlotTimeout = batchSlotTimeout;
        return this;
    }

    public DeclarativeSlotPoolBridgeBuilder setIdleSlotTimeout(Duration idleSlotTimeout) {
        this.idleSlotTimeout = idleSlotTimeout;
        return this;
    }

    public DeclarativeSlotPoolBridgeBuilder setSlotRequestMaxInterval(
            Duration slotRequestMaxInterval) {
        this.slotRequestMaxInterval = slotRequestMaxInterval;
        return this;
    }

    public DeclarativeSlotPoolBridgeBuilder setClock(Clock clock) {
        this.clock = clock;
        return this;
    }

    public DeclarativeSlotPoolBridgeBuilder setJobId(JobID jobId) {
        this.jobId = jobId;
        return this;
    }

    public DeclarativeSlotPoolBridgeBuilder setRequestSlotMatchingStrategy(
            RequestSlotMatchingStrategy requestSlotMatchingStrategy) {
        this.requestSlotMatchingStrategy = requestSlotMatchingStrategy;
        return this;
    }

    public DeclarativeSlotPoolBridgeBuilder setMainThreadExecutor(
            ComponentMainThreadExecutor mainThreadExecutor) {
        this.mainThreadExecutor = mainThreadExecutor;
        return this;
    }

    public DeclarativeSlotPoolBridgeBuilder setSlotBatchAllocatable(boolean slotBatchAllocatable) {
        this.slotBatchAllocatable = slotBatchAllocatable;
        return this;
    }

    public DeclarativeSlotPoolBridge build() {
        return new DeclarativeSlotPoolBridge(
                jobId,
                new DefaultDeclarativeSlotPoolFactory(),
                clock,
                TestingUtils.infiniteDuration(),
                idleSlotTimeout,
                batchSlotTimeout,
                requestSlotMatchingStrategy,
                slotRequestMaxInterval,
                slotBatchAllocatable,
                mainThreadExecutor);
    }

    public DeclarativeSlotPoolBridge buildAndStart() throws Exception {
        final DeclarativeSlotPoolBridge slotPool =
                new DeclarativeSlotPoolBridge(
                        jobId,
                        new DefaultDeclarativeSlotPoolFactory(),
                        clock,
                        TestingUtils.infiniteDuration(),
                        idleSlotTimeout,
                        batchSlotTimeout,
                        requestSlotMatchingStrategy,
                        slotRequestMaxInterval,
                        slotBatchAllocatable,
                        mainThreadExecutor);

        slotPool.start(JobMasterId.generate(), "foobar");

        if (resourceManagerGateway != null) {
            CompletableFuture.runAsync(
                            () -> slotPool.connectToResourceManager(resourceManagerGateway),
                            mainThreadExecutor)
                    .join();
        }

        return slotPool;
    }
}
