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

package org.apache.flink.runtime.jobmaster;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blocklist.BlocklistOperations;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolBridgeServiceFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolServiceFactory;
import org.apache.flink.runtime.jobmaster.slotpool.PreferredAllocationRequestSlotMatchingStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.RequestSlotMatchingStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SimpleRequestSlotMatchingStrategy;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolServiceFactory;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.DefaultSchedulerFactory;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveSchedulerFactory;
import org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchSchedulerFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.streaming.api.graph.ExecutionPlan;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.time.Duration;
import java.util.Collection;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.configuration.JobManagerOptions.SLOT_REQUEST_MAX_INTERVAL;

/** Default {@link SlotPoolServiceSchedulerFactory} implementation. */
public final class DefaultSlotPoolServiceSchedulerFactory
        implements SlotPoolServiceSchedulerFactory {

    private static final Logger LOG =
            LoggerFactory.getLogger(DefaultSlotPoolServiceSchedulerFactory.class);

    private final SlotPoolServiceFactory slotPoolServiceFactory;

    private final SchedulerNGFactory schedulerNGFactory;

    private DefaultSlotPoolServiceSchedulerFactory(
            SlotPoolServiceFactory slotPoolServiceFactory, SchedulerNGFactory schedulerNGFactory) {
        this.slotPoolServiceFactory = slotPoolServiceFactory;
        this.schedulerNGFactory = schedulerNGFactory;
    }

    @VisibleForTesting
    SchedulerNGFactory getSchedulerNGFactory() {
        return schedulerNGFactory;
    }

    @Override
    public SlotPoolService createSlotPoolService(
            JobID jid,
            DeclarativeSlotPoolFactory declarativeSlotPoolFactory,
            @Nonnull ComponentMainThreadExecutor componentMainThreadExecutor) {
        return slotPoolServiceFactory.createSlotPoolService(
                jid, declarativeSlotPoolFactory, componentMainThreadExecutor);
    }

    @Override
    public JobManagerOptions.SchedulerType getSchedulerType() {
        return schedulerNGFactory.getSchedulerType();
    }

    @Override
    public SchedulerNG createScheduler(
            Logger log,
            ExecutionPlan executionPlan,
            Executor ioExecutor,
            Configuration configuration,
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
        return schedulerNGFactory.createInstance(
                log,
                executionPlan,
                ioExecutor,
                configuration,
                slotPoolService,
                futureExecutor,
                userCodeLoader,
                checkpointRecoveryFactory,
                rpcTimeout,
                blobWriter,
                jobManagerJobMetricGroup,
                slotRequestTimeout,
                shuffleMaster,
                partitionTracker,
                executionDeploymentTracker,
                initializationTimestamp,
                mainThreadExecutor,
                fatalErrorHandler,
                jobStatusListener,
                failureEnrichers,
                blocklistOperations);
    }

    public static DefaultSlotPoolServiceSchedulerFactory create(
            SlotPoolServiceFactory slotPoolServiceFactory, SchedulerNGFactory schedulerNGFactory) {
        return new DefaultSlotPoolServiceSchedulerFactory(
                slotPoolServiceFactory, schedulerNGFactory);
    }

    public static DefaultSlotPoolServiceSchedulerFactory fromConfiguration(
            Configuration configuration, JobType jobType, boolean isDynamicGraph) {

        final Duration rpcTimeout = configuration.get(RpcOptions.ASK_TIMEOUT_DURATION);
        final Duration slotIdleTimeout = configuration.get(JobManagerOptions.SLOT_IDLE_TIMEOUT);
        final Duration batchSlotTimeout = configuration.get(JobManagerOptions.SLOT_REQUEST_TIMEOUT);

        final SlotPoolServiceFactory slotPoolServiceFactory;
        final SchedulerNGFactory schedulerNGFactory;

        JobManagerOptions.SchedulerType schedulerType =
                getSchedulerType(configuration, jobType, isDynamicGraph);

        final Duration slotRequestMaxInterval = configuration.get(SLOT_REQUEST_MAX_INTERVAL);

        // TODO: It will be assigned by the corresponding logic after
        //  https://issues.apache.org/jira/browse/FLINK-35966
        final boolean slotBatchAllocatable = false;

        if (configuration
                .getOptional(JobManagerOptions.HYBRID_PARTITION_DATA_CONSUME_CONSTRAINT)
                .isPresent()) {
            Preconditions.checkState(
                    schedulerType == JobManagerOptions.SchedulerType.AdaptiveBatch,
                    "Only adaptive batch scheduler supports setting "
                            + JobManagerOptions.HYBRID_PARTITION_DATA_CONSUME_CONSTRAINT.key());
        }

        switch (schedulerType) {
            case Default:
                schedulerNGFactory = new DefaultSchedulerFactory();
                slotPoolServiceFactory =
                        new DeclarativeSlotPoolBridgeServiceFactory(
                                SystemClock.getInstance(),
                                rpcTimeout,
                                slotIdleTimeout,
                                batchSlotTimeout,
                                slotRequestMaxInterval,
                                slotBatchAllocatable,
                                getRequestSlotMatchingStrategy(configuration, jobType));
                break;
            case Adaptive:
                schedulerNGFactory = new AdaptiveSchedulerFactory();
                slotPoolServiceFactory =
                        new DeclarativeSlotPoolServiceFactory(
                                SystemClock.getInstance(),
                                slotIdleTimeout,
                                rpcTimeout,
                                slotRequestMaxInterval);
                break;
            case AdaptiveBatch:
                schedulerNGFactory = new AdaptiveBatchSchedulerFactory();
                slotPoolServiceFactory =
                        new DeclarativeSlotPoolBridgeServiceFactory(
                                SystemClock.getInstance(),
                                rpcTimeout,
                                slotIdleTimeout,
                                batchSlotTimeout,
                                slotRequestMaxInterval,
                                slotBatchAllocatable,
                                getRequestSlotMatchingStrategy(configuration, jobType));
                break;
            default:
                throw new IllegalArgumentException(
                        String.format(
                                "Illegal value [%s] for config option [%s]",
                                schedulerType, JobManagerOptions.SCHEDULER.key()));
        }

        return new DefaultSlotPoolServiceSchedulerFactory(
                slotPoolServiceFactory, schedulerNGFactory);
    }

    private static JobManagerOptions.SchedulerType getSchedulerType(
            Configuration configuration, JobType jobType, boolean isDynamicGraph) {
        JobManagerOptions.SchedulerType schedulerType;
        if (jobType == JobType.BATCH) {
            if (configuration.get(JobManagerOptions.SCHEDULER_MODE)
                            == SchedulerExecutionMode.REACTIVE
                    || configuration.get(JobManagerOptions.SCHEDULER)
                            == JobManagerOptions.SchedulerType.Adaptive) {
                LOG.info(
                        "Adaptive Scheduler configured, but Batch job detected. Changing scheduler type to 'AdaptiveBatch'.");
                // overwrite
                schedulerType = JobManagerOptions.SchedulerType.AdaptiveBatch;
            } else {
                schedulerType =
                        configuration
                                .getOptional(JobManagerOptions.SCHEDULER)
                                .orElse(
                                        isDynamicGraph
                                                ? JobManagerOptions.SchedulerType.AdaptiveBatch
                                                : JobManagerOptions.SchedulerType.Default);
            }
        } else {
            if (configuration.get(JobManagerOptions.SCHEDULER_MODE)
                    == SchedulerExecutionMode.REACTIVE) {
                schedulerType = JobManagerOptions.SchedulerType.Adaptive;
            } else {
                schedulerType =
                        configuration
                                .getOptional(JobManagerOptions.SCHEDULER)
                                .orElse(
                                        System.getProperties()
                                                        .containsKey(
                                                                "flink.tests.enable-adaptive-scheduler")
                                                ? JobManagerOptions.SchedulerType.Adaptive
                                                : JobManagerOptions.SchedulerType.Default);
            }
        }

        return schedulerType;
    }

    @VisibleForTesting
    static RequestSlotMatchingStrategy getRequestSlotMatchingStrategy(
            Configuration configuration, JobType jobType) {
        final boolean isLocalRecoveryEnabled =
                configuration.get(StateRecoveryOptions.LOCAL_RECOVERY);

        if (isLocalRecoveryEnabled) {
            if (jobType == JobType.STREAMING) {
                return PreferredAllocationRequestSlotMatchingStrategy.INSTANCE;
            } else {
                LOG.warn(
                        "Batch jobs do not support local recovery. Falling back for request slot matching strategy to {}.",
                        SimpleRequestSlotMatchingStrategy.class.getSimpleName());
                return SimpleRequestSlotMatchingStrategy.INSTANCE;
            }
        } else {
            return SimpleRequestSlotMatchingStrategy.INSTANCE;
        }
    }
}
