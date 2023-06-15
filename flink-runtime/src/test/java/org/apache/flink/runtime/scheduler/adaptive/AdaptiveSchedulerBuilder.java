/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.failover.flip1.NoRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobmaster.DefaultExecutionDeploymentTracker;
import org.apache.flink.runtime.jobmaster.slotpool.DeclarativeSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultAllocatedSlotPool;
import org.apache.flink.runtime.jobmaster.slotpool.DefaultDeclarativeSlotPool;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.rpc.FatalErrorHandler;
import org.apache.flink.runtime.scheduler.DefaultExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.ExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.adaptive.allocator.SlotAllocator;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleTestUtils;
import org.apache.flink.util.FatalExitExceptionHandler;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ScheduledExecutorService;

/** Builder for {@link AdaptiveScheduler}. */
public class AdaptiveSchedulerBuilder {
    private static final Time DEFAULT_TIMEOUT = Time.seconds(300);

    private final JobGraph jobGraph;

    private final ComponentMainThreadExecutor mainThreadExecutor;
    private final ScheduledExecutorService executorService;

    @Nullable private JobResourceRequirements jobResourceRequirements;

    private Configuration jobMasterConfiguration = new Configuration();
    private ClassLoader userCodeLoader = ClassLoader.getSystemClassLoader();
    private CheckpointsCleaner checkpointsCleaner = new CheckpointsCleaner();
    private CheckpointRecoveryFactory checkpointRecoveryFactory =
            new StandaloneCheckpointRecoveryFactory();
    private DeclarativeSlotPool declarativeSlotPool;
    private Time rpcTimeout = DEFAULT_TIMEOUT;
    private BlobWriter blobWriter = VoidBlobWriter.getInstance();
    private JobManagerJobMetricGroup jobManagerJobMetricGroup =
            UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup();
    private ShuffleMaster<?> shuffleMaster = ShuffleTestUtils.DEFAULT_SHUFFLE_MASTER;
    private JobMasterPartitionTracker partitionTracker = NoOpJobMasterPartitionTracker.INSTANCE;
    private RestartBackoffTimeStrategy restartBackoffTimeStrategy =
            NoRestartBackoffTimeStrategy.INSTANCE;
    private FatalErrorHandler fatalErrorHandler =
            error ->
                    FatalExitExceptionHandler.INSTANCE.uncaughtException(
                            Thread.currentThread(), error);
    private JobStatusListener jobStatusListener = (ignoredA, ignoredB, ignoredC) -> {};
    private Collection<FailureEnricher> failureEnrichers = Collections.emptySet();
    private long initializationTimestamp = System.currentTimeMillis();

    @Nullable private SlotAllocator slotAllocator;

    public AdaptiveSchedulerBuilder(
            final JobGraph jobGraph,
            ComponentMainThreadExecutor mainThreadExecutor,
            ScheduledExecutorService executorService) {
        this.jobGraph = jobGraph;
        this.mainThreadExecutor = mainThreadExecutor;

        this.declarativeSlotPool =
                new DefaultDeclarativeSlotPool(
                        jobGraph.getJobID(),
                        new DefaultAllocatedSlotPool(),
                        ignored -> {},
                        DEFAULT_TIMEOUT,
                        rpcTimeout);
        this.executorService = executorService;
    }

    public AdaptiveSchedulerBuilder setJobResourceRequirements(
            JobResourceRequirements jobResourceRequirements) {
        this.jobResourceRequirements = jobResourceRequirements;
        return this;
    }

    public AdaptiveSchedulerBuilder setJobMasterConfiguration(
            final Configuration jobMasterConfiguration) {
        this.jobMasterConfiguration = jobMasterConfiguration;
        return this;
    }

    public AdaptiveSchedulerBuilder setUserCodeLoader(final ClassLoader userCodeLoader) {
        this.userCodeLoader = userCodeLoader;
        return this;
    }

    public AdaptiveSchedulerBuilder setCheckpointCleaner(
            final CheckpointsCleaner checkpointsCleaner) {
        this.checkpointsCleaner = checkpointsCleaner;
        return this;
    }

    public AdaptiveSchedulerBuilder setCheckpointRecoveryFactory(
            final CheckpointRecoveryFactory checkpointRecoveryFactory) {
        this.checkpointRecoveryFactory = checkpointRecoveryFactory;
        return this;
    }

    public AdaptiveSchedulerBuilder setRpcTimeout(final Time rpcTimeout) {
        this.rpcTimeout = rpcTimeout;
        return this;
    }

    public AdaptiveSchedulerBuilder setBlobWriter(final BlobWriter blobWriter) {
        this.blobWriter = blobWriter;
        return this;
    }

    public AdaptiveSchedulerBuilder setJobManagerJobMetricGroup(
            final JobManagerJobMetricGroup jobManagerJobMetricGroup) {
        this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
        return this;
    }

    public AdaptiveSchedulerBuilder setShuffleMaster(final ShuffleMaster<?> shuffleMaster) {
        this.shuffleMaster = shuffleMaster;
        return this;
    }

    public AdaptiveSchedulerBuilder setPartitionTracker(
            final JobMasterPartitionTracker partitionTracker) {
        this.partitionTracker = partitionTracker;
        return this;
    }

    public AdaptiveSchedulerBuilder setDeclarativeSlotPool(
            DeclarativeSlotPool declarativeSlotPool) {
        this.declarativeSlotPool = declarativeSlotPool;
        return this;
    }

    public AdaptiveSchedulerBuilder setRestartBackoffTimeStrategy(
            final RestartBackoffTimeStrategy restartBackoffTimeStrategy) {
        this.restartBackoffTimeStrategy = restartBackoffTimeStrategy;
        return this;
    }

    public AdaptiveSchedulerBuilder setFatalErrorHandler(FatalErrorHandler fatalErrorHandler) {
        this.fatalErrorHandler = fatalErrorHandler;
        return this;
    }

    public AdaptiveSchedulerBuilder setJobStatusListener(JobStatusListener jobStatusListener) {
        this.jobStatusListener = jobStatusListener;
        return this;
    }

    public AdaptiveSchedulerBuilder setFailureEnrichers(
            Collection<FailureEnricher> failureEnrichers) {
        this.failureEnrichers = failureEnrichers;
        return this;
    }

    public AdaptiveSchedulerBuilder setInitializationTimestamp(long initializationTimestamp) {
        this.initializationTimestamp = initializationTimestamp;
        return this;
    }

    public AdaptiveSchedulerBuilder setSlotAllocator(SlotAllocator slotAllocator) {
        this.slotAllocator = slotAllocator;
        return this;
    }

    public AdaptiveScheduler build() throws Exception {
        final ExecutionGraphFactory executionGraphFactory =
                new DefaultExecutionGraphFactory(
                        jobMasterConfiguration,
                        userCodeLoader,
                        new DefaultExecutionDeploymentTracker(),
                        executorService,
                        executorService,
                        rpcTimeout,
                        jobManagerJobMetricGroup,
                        blobWriter,
                        shuffleMaster,
                        partitionTracker);

        return new AdaptiveScheduler(
                jobGraph,
                jobResourceRequirements,
                jobMasterConfiguration,
                declarativeSlotPool,
                slotAllocator == null
                        ? AdaptiveSchedulerFactory.createSlotSharingSlotAllocator(
                                declarativeSlotPool)
                        : slotAllocator,
                executorService,
                userCodeLoader,
                checkpointsCleaner,
                checkpointRecoveryFactory,
                jobMasterConfiguration.get(JobManagerOptions.RESOURCE_WAIT_TIMEOUT),
                jobMasterConfiguration.get(JobManagerOptions.RESOURCE_STABILIZATION_TIMEOUT),
                jobManagerJobMetricGroup,
                restartBackoffTimeStrategy,
                initializationTimestamp,
                mainThreadExecutor,
                fatalErrorHandler,
                jobStatusListener,
                failureEnrichers,
                executionGraphFactory);
    }
}
