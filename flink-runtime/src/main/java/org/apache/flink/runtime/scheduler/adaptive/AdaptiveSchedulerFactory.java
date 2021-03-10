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

package org.apache.flink.runtime.scheduler.adaptive;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.ExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.DefaultExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.ExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotSharingSlotAllocator;
import org.apache.flink.runtime.shuffle.ShuffleMaster;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/** Factory for the adaptive scheduler. */
public class AdaptiveSchedulerFactory implements SchedulerNGFactory {

    private final Duration initialResourceAllocationTimeout;
    private final Duration resourceStabilizationTimeout;

    public AdaptiveSchedulerFactory(
            Duration initialResourceAllocationTimeout, Duration resourceStabilizationTimeout) {
        this.initialResourceAllocationTimeout = initialResourceAllocationTimeout;
        this.resourceStabilizationTimeout = resourceStabilizationTimeout;
    }

    @Override
    public SchedulerNG createInstance(
            Logger log,
            JobGraph jobGraph,
            Executor ioExecutor,
            Configuration jobMasterConfiguration,
            SlotPoolService slotPoolService,
            ScheduledExecutorService futureExecutor,
            ClassLoader userCodeLoader,
            CheckpointRecoveryFactory checkpointRecoveryFactory,
            Time rpcTimeout,
            BlobWriter blobWriter,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            Time slotRequestTimeout,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            ExecutionDeploymentTracker executionDeploymentTracker,
            long initializationTimestamp,
            ComponentMainThreadExecutor mainThreadExecutor,
            FatalErrorHandler fatalErrorHandler,
            JobStatusListener jobStatusListener)
            throws Exception {
        final DeclarativeSlotPool declarativeSlotPool =
                slotPoolService
                        .castInto(DeclarativeSlotPool.class)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "The AdaptiveScheduler requires a DeclarativeSlotPool."));
        final RestartBackoffTimeStrategy restartBackoffTimeStrategy =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                                jobGraph.getSerializedExecutionConfig()
                                        .deserializeValue(userCodeLoader)
                                        .getRestartStrategy(),
                                jobMasterConfiguration,
                                jobGraph.isCheckpointingEnabled())
                        .create();
        log.info(
                "Using restart back off time strategy {} for {} ({}).",
                restartBackoffTimeStrategy,
                jobGraph.getName(),
                jobGraph.getJobID());

        final SlotSharingSlotAllocator slotAllocator =
                createSlotSharingSlotAllocator(declarativeSlotPool);

        final ExecutionGraphFactory executionGraphFactory =
                new DefaultExecutionGraphFactory(
                        jobMasterConfiguration,
                        userCodeLoader,
                        executionDeploymentTracker,
                        futureExecutor,
                        ioExecutor,
                        rpcTimeout,
                        jobManagerJobMetricGroup,
                        blobWriter,
                        shuffleMaster,
                        partitionTracker);

        return new AdaptiveScheduler(
                jobGraph,
                jobMasterConfiguration,
                declarativeSlotPool,
                slotAllocator,
                ioExecutor,
                userCodeLoader,
                checkpointRecoveryFactory,
                initialResourceAllocationTimeout,
                resourceStabilizationTimeout,
                jobManagerJobMetricGroup,
                restartBackoffTimeStrategy,
                initializationTimestamp,
                mainThreadExecutor,
                fatalErrorHandler,
                jobStatusListener,
                executionGraphFactory);
    }

    @Override
    public JobManagerOptions.SchedulerType getSchedulerType() {
        return JobManagerOptions.SchedulerType.Adaptive;
    }

    public static SlotSharingSlotAllocator createSlotSharingSlotAllocator(
            DeclarativeSlotPool declarativeSlotPool) {
        return SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                declarativeSlotPool::reserveFreeSlot,
                declarativeSlotPool::freeReservedSlot,
                declarativeSlotPool::containsFreeSlot);
    }
}
