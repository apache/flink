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

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blocklist.BlocklistOperations;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.RestartBackoffTimeStrategyFactoryLoader;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
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
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.FlinkException;

import org.slf4j.Logger;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/** Factory for the adaptive scheduler. */
public class AdaptiveSchedulerFactory implements SchedulerNGFactory {

    public AdaptiveSchedulerFactory() {}

    @Override
    public SchedulerNG createInstance(
            Logger log,
            ExecutionPlan executionPlan,
            Executor ioExecutor,
            Configuration jobMasterConfiguration,
            SlotPoolService slotPoolService,
            ScheduledExecutorService futureExecutor,
            ClassLoader userCodeLoader,
            CheckpointRecoveryFactory checkpointRecoveryFactory,
            Duration rpcTimeout,
            BlobWriter blobWriter,
            JobManagerJobMetricGroup jobManagerJobMetricGroup,
            Duration slotRequestTimeout,
            ShuffleMaster<?> shuffleMaster,
            JobMasterPartitionTracker partitionTracker,
            ExecutionDeploymentTracker executionDeploymentTracker,
            long initializationTimestamp,
            ComponentMainThreadExecutor mainThreadExecutor,
            FatalErrorHandler fatalErrorHandler,
            JobStatusListener jobStatusListener,
            Collection<FailureEnricher> failureEnrichers,
            BlocklistOperations blocklistOperations)
            throws Exception {
        JobGraph jobGraph;

        if (executionPlan instanceof JobGraph) {
            jobGraph = (JobGraph) executionPlan;
        } else if (executionPlan instanceof StreamGraph) {
            jobGraph = ((StreamGraph) executionPlan).getJobGraph(userCodeLoader);
        } else {
            throw new FlinkException(
                    "Unsupported execution plan " + executionPlan.getClass().getCanonicalName());
        }

        final DeclarativeSlotPool declarativeSlotPool =
                slotPoolService
                        .castInto(DeclarativeSlotPool.class)
                        .orElseThrow(
                                () ->
                                        new IllegalStateException(
                                                "The AdaptiveScheduler requires a DeclarativeSlotPool."));
        final RestartBackoffTimeStrategy restartBackoffTimeStrategy =
                RestartBackoffTimeStrategyFactoryLoader.createRestartBackoffTimeStrategyFactory(
                                jobGraph.getJobConfiguration(),
                                jobMasterConfiguration,
                                jobGraph.isCheckpointingEnabled())
                        .create();
        log.info(
                "Using restart back off time strategy {} for {} ({}).",
                restartBackoffTimeStrategy,
                jobGraph.getName(),
                jobGraph.getJobID());

        final SlotSharingSlotAllocator slotAllocator =
                createSlotSharingSlotAllocator(
                        declarativeSlotPool,
                        jobMasterConfiguration.get(StateRecoveryOptions.LOCAL_RECOVERY));

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
                AdaptiveScheduler.Settings.of(
                        jobMasterConfiguration, jobGraph.getCheckpointingSettings()),
                jobGraph,
                JobResourceRequirements.readFromExecutionPlan(jobGraph).orElse(null),
                jobMasterConfiguration,
                declarativeSlotPool,
                slotAllocator,
                ioExecutor,
                userCodeLoader,
                new CheckpointsCleaner(),
                checkpointRecoveryFactory,
                jobManagerJobMetricGroup,
                restartBackoffTimeStrategy,
                initializationTimestamp,
                mainThreadExecutor,
                fatalErrorHandler,
                jobStatusListener,
                failureEnrichers,
                executionGraphFactory);
    }

    @Override
    public JobManagerOptions.SchedulerType getSchedulerType() {
        return JobManagerOptions.SchedulerType.Adaptive;
    }

    public static SlotSharingSlotAllocator createSlotSharingSlotAllocator(
            DeclarativeSlotPool declarativeSlotPool, boolean localRecoveryEnabled) {
        return SlotSharingSlotAllocator.createSlotSharingSlotAllocator(
                declarativeSlotPool::reserveFreeSlot,
                declarativeSlotPool::freeReservedSlot,
                declarativeSlotPool::containsFreeSlot,
                localRecoveryEnabled);
    }
}
