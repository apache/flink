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
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ClusterOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.SchedulerExecutionMode;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolBridgeServiceFactory;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPoolServiceFactory;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolService;
import org.apache.flink.runtime.jobmaster.slotpool.SlotPoolServiceFactory;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.DefaultSchedulerFactory;
import org.apache.flink.runtime.scheduler.SchedulerNG;
import org.apache.flink.runtime.scheduler.SchedulerNGFactory;
import org.apache.flink.runtime.scheduler.adaptive.AdaptiveSchedulerFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.util.clock.SystemClock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.ScheduledExecutorService;

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
    public SlotPoolService createSlotPoolService(JobID jid) {
        return slotPoolServiceFactory.createSlotPoolService(jid);
    }

    @Override
    public JobManagerOptions.SchedulerType getSchedulerType() {
        return schedulerNGFactory.getSchedulerType();
    }

    @Override
    public SchedulerNG createScheduler(
            Logger log,
            JobGraph jobGraph,
            ScheduledExecutorService scheduledExecutorService,
            Configuration configuration,
            SlotPoolService slotPoolService,
            ScheduledExecutorService executorService,
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
        return schedulerNGFactory.createInstance(
                log,
                jobGraph,
                scheduledExecutorService,
                configuration,
                slotPoolService,
                executorService,
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
                jobStatusListener);
    }

    public static DefaultSlotPoolServiceSchedulerFactory create(
            SlotPoolServiceFactory slotPoolServiceFactory, SchedulerNGFactory schedulerNGFactory) {
        return new DefaultSlotPoolServiceSchedulerFactory(
                slotPoolServiceFactory, schedulerNGFactory);
    }

    public static DefaultSlotPoolServiceSchedulerFactory fromConfiguration(
            Configuration configuration, JobType jobType) {

        final Time rpcTimeout = AkkaUtils.getTimeoutAsTime(configuration);
        final Time slotIdleTimeout =
                Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_IDLE_TIMEOUT));
        final Time batchSlotTimeout =
                Time.milliseconds(configuration.getLong(JobManagerOptions.SLOT_REQUEST_TIMEOUT));

        final SlotPoolServiceFactory slotPoolServiceFactory;
        final SchedulerNGFactory schedulerNGFactory;

        JobManagerOptions.SchedulerType schedulerType =
                ClusterOptions.getSchedulerType(configuration);
        if (schedulerType == JobManagerOptions.SchedulerType.Adaptive && jobType == JobType.BATCH) {
            LOG.info(
                    "Adaptive Scheduler configured, but Batch job detected. Changing scheduler type to NG / DefaultScheduler.");
            // overwrite
            schedulerType = JobManagerOptions.SchedulerType.Ng;
        }

        switch (schedulerType) {
            case Ng:
                schedulerNGFactory = new DefaultSchedulerFactory();
                slotPoolServiceFactory =
                        new DeclarativeSlotPoolBridgeServiceFactory(
                                SystemClock.getInstance(),
                                rpcTimeout,
                                slotIdleTimeout,
                                batchSlotTimeout);
                break;
            case Adaptive:
                schedulerNGFactory = getAdaptiveSchedulerFactoryFromConfiguration(configuration);
                slotPoolServiceFactory =
                        new DeclarativeSlotPoolServiceFactory(
                                SystemClock.getInstance(), slotIdleTimeout, rpcTimeout);
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

    private static AdaptiveSchedulerFactory getAdaptiveSchedulerFactoryFromConfiguration(
            Configuration configuration) {
        Duration allocationTimeoutDefault = JobManagerOptions.RESOURCE_WAIT_TIMEOUT.defaultValue();
        Duration stabilizationTimeoutDefault =
                JobManagerOptions.RESOURCE_STABILIZATION_TIMEOUT.defaultValue();

        if (configuration.get(JobManagerOptions.SCHEDULER_MODE)
                == SchedulerExecutionMode.REACTIVE) {
            allocationTimeoutDefault = Duration.ofMillis(-1);
            stabilizationTimeoutDefault = Duration.ZERO;
        }

        final Duration initialResourceAllocationTimeout =
                configuration
                        .getOptional(JobManagerOptions.RESOURCE_WAIT_TIMEOUT)
                        .orElse(allocationTimeoutDefault);

        final Duration resourceStabilizationTimeout =
                configuration
                        .getOptional(JobManagerOptions.RESOURCE_STABILIZATION_TIMEOUT)
                        .orElse(stabilizationTimeoutDefault);

        return new AdaptiveSchedulerFactory(
                initialResourceAllocationTimeout, resourceStabilizationTimeout);
    }
}
