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

package org.apache.flink.runtime.scheduler;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.blocklist.BlocklistOperations;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointRecoveryFactory;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.SpeculativeExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.NoRestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartPipelinedRegionFailoverStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.DefaultExecutionDeploymentTracker;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.adaptivebatch.AdaptiveBatchScheduler;
import org.apache.flink.runtime.scheduler.adaptivebatch.SpeculativeScheduler;
import org.apache.flink.runtime.scheduler.adaptivebatch.VertexParallelismDecider;
import org.apache.flink.runtime.scheduler.strategy.PipelinedRegionSchedulingStrategy;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.scheduler.strategy.VertexwiseSchedulingStrategy;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleTestUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.scheduler.SchedulerBase.computeVertexParallelismStore;

/** A builder to create {@link DefaultScheduler} or its subclass instances for testing. */
public class DefaultSchedulerBuilder {
    private static final Logger LOG = LoggerFactory.getLogger(DefaultSchedulerBuilder.class);

    private final JobGraph jobGraph;
    private final ComponentMainThreadExecutor mainThreadExecutor;

    private Executor ioExecutor;
    private ScheduledExecutorService futureExecutor;
    private ScheduledExecutor delayExecutor;
    private Logger log = LOG;
    private Configuration jobMasterConfiguration = new Configuration();
    private ClassLoader userCodeLoader = ClassLoader.getSystemClassLoader();
    private CheckpointsCleaner checkpointCleaner = new CheckpointsCleaner();
    private CheckpointRecoveryFactory checkpointRecoveryFactory =
            new StandaloneCheckpointRecoveryFactory();
    private Time rpcTimeout = Time.seconds(300);
    private BlobWriter blobWriter = VoidBlobWriter.getInstance();
    private JobManagerJobMetricGroup jobManagerJobMetricGroup =
            UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup();
    private ShuffleMaster<?> shuffleMaster = ShuffleTestUtils.DEFAULT_SHUFFLE_MASTER;
    private JobMasterPartitionTracker partitionTracker = NoOpJobMasterPartitionTracker.INSTANCE;
    private SchedulingStrategyFactory schedulingStrategyFactory =
            new PipelinedRegionSchedulingStrategy.Factory();
    private FailoverStrategy.Factory failoverStrategyFactory =
            new RestartPipelinedRegionFailoverStrategy.Factory();
    private RestartBackoffTimeStrategy restartBackoffTimeStrategy =
            NoRestartBackoffTimeStrategy.INSTANCE;
    private ExecutionOperations executionOperations = new DefaultExecutionOperations();
    private ExecutionVertexVersioner executionVertexVersioner = new ExecutionVertexVersioner();
    private ExecutionSlotAllocatorFactory executionSlotAllocatorFactory =
            new TestExecutionSlotAllocatorFactory();
    private JobStatusListener jobStatusListener = (ignoredA, ignoredB, ignoredC) -> {};
    private ExecutionDeployer.Factory executionDeployerFactory =
            new DefaultExecutionDeployer.Factory();
    private VertexParallelismDecider vertexParallelismDecider = (ignored) -> 0;
    private int defaultMaxParallelism =
            JobManagerOptions.ADAPTIVE_BATCH_SCHEDULER_MAX_PARALLELISM.defaultValue();
    private BlocklistOperations blocklistOperations = ignore -> {};

    public DefaultSchedulerBuilder(
            JobGraph jobGraph,
            ComponentMainThreadExecutor mainThreadExecutor,
            ScheduledExecutorService generalExecutorService) {
        this(
                jobGraph,
                mainThreadExecutor,
                generalExecutorService,
                generalExecutorService,
                new ScheduledExecutorServiceAdapter(generalExecutorService));
    }

    public DefaultSchedulerBuilder(
            JobGraph jobGraph,
            ComponentMainThreadExecutor mainThreadExecutor,
            Executor ioExecutor,
            ScheduledExecutorService futureExecutor,
            ScheduledExecutor delayExecutor) {
        this.jobGraph = jobGraph;
        this.mainThreadExecutor = mainThreadExecutor;
        this.ioExecutor = ioExecutor;
        this.futureExecutor = futureExecutor;
        this.delayExecutor = delayExecutor;
    }

    public DefaultSchedulerBuilder setIoExecutor(Executor ioExecutor) {
        this.ioExecutor = ioExecutor;
        return this;
    }

    public DefaultSchedulerBuilder setFutureExecutor(ScheduledExecutorService futureExecutor) {
        this.futureExecutor = futureExecutor;
        return this;
    }

    public DefaultSchedulerBuilder setDelayExecutor(ScheduledExecutor delayExecutor) {
        this.delayExecutor = delayExecutor;
        return this;
    }

    public DefaultSchedulerBuilder setLogger(Logger log) {
        this.log = log;
        return this;
    }

    public DefaultSchedulerBuilder setJobMasterConfiguration(Configuration jobMasterConfiguration) {
        this.jobMasterConfiguration = jobMasterConfiguration;
        return this;
    }

    public DefaultSchedulerBuilder setUserCodeLoader(ClassLoader userCodeLoader) {
        this.userCodeLoader = userCodeLoader;
        return this;
    }

    public DefaultSchedulerBuilder setCheckpointCleaner(CheckpointsCleaner checkpointsCleaner) {
        this.checkpointCleaner = checkpointsCleaner;
        return this;
    }

    public DefaultSchedulerBuilder setCheckpointRecoveryFactory(
            CheckpointRecoveryFactory checkpointRecoveryFactory) {
        this.checkpointRecoveryFactory = checkpointRecoveryFactory;
        return this;
    }

    public DefaultSchedulerBuilder setRpcTimeout(Time rpcTimeout) {
        this.rpcTimeout = rpcTimeout;
        return this;
    }

    public DefaultSchedulerBuilder setBlobWriter(BlobWriter blobWriter) {
        this.blobWriter = blobWriter;
        return this;
    }

    public DefaultSchedulerBuilder setJobManagerJobMetricGroup(
            JobManagerJobMetricGroup jobManagerJobMetricGroup) {
        this.jobManagerJobMetricGroup = jobManagerJobMetricGroup;
        return this;
    }

    public DefaultSchedulerBuilder setShuffleMaster(ShuffleMaster<?> shuffleMaster) {
        this.shuffleMaster = shuffleMaster;
        return this;
    }

    public DefaultSchedulerBuilder setPartitionTracker(JobMasterPartitionTracker partitionTracker) {
        this.partitionTracker = partitionTracker;
        return this;
    }

    public DefaultSchedulerBuilder setSchedulingStrategyFactory(
            SchedulingStrategyFactory schedulingStrategyFactory) {
        this.schedulingStrategyFactory = schedulingStrategyFactory;
        return this;
    }

    public DefaultSchedulerBuilder setFailoverStrategyFactory(
            FailoverStrategy.Factory failoverStrategyFactory) {
        this.failoverStrategyFactory = failoverStrategyFactory;
        return this;
    }

    public DefaultSchedulerBuilder setRestartBackoffTimeStrategy(
            RestartBackoffTimeStrategy restartBackoffTimeStrategy) {
        this.restartBackoffTimeStrategy = restartBackoffTimeStrategy;
        return this;
    }

    public DefaultSchedulerBuilder setExecutionOperations(ExecutionOperations executionOperations) {
        this.executionOperations = executionOperations;
        return this;
    }

    public DefaultSchedulerBuilder setExecutionVertexVersioner(
            ExecutionVertexVersioner executionVertexVersioner) {
        this.executionVertexVersioner = executionVertexVersioner;
        return this;
    }

    public DefaultSchedulerBuilder setExecutionSlotAllocatorFactory(
            ExecutionSlotAllocatorFactory executionSlotAllocatorFactory) {
        this.executionSlotAllocatorFactory = executionSlotAllocatorFactory;
        return this;
    }

    public DefaultSchedulerBuilder setJobStatusListener(JobStatusListener jobStatusListener) {
        this.jobStatusListener = jobStatusListener;
        return this;
    }

    public DefaultSchedulerBuilder setExecutionDeployerFactory(
            ExecutionDeployer.Factory executionDeployerFactory) {
        this.executionDeployerFactory = executionDeployerFactory;
        return this;
    }

    public DefaultSchedulerBuilder setVertexParallelismDecider(
            VertexParallelismDecider vertexParallelismDecider) {
        this.vertexParallelismDecider = vertexParallelismDecider;
        return this;
    }

    public DefaultSchedulerBuilder setDefaultMaxParallelism(int defaultMaxParallelism) {
        this.defaultMaxParallelism = defaultMaxParallelism;
        return this;
    }

    public DefaultSchedulerBuilder setBlocklistOperations(BlocklistOperations blocklistOperations) {
        this.blocklistOperations = blocklistOperations;
        return this;
    }

    public DefaultScheduler build() throws Exception {
        return new DefaultScheduler(
                log,
                jobGraph,
                ioExecutor,
                jobMasterConfiguration,
                componentMainThreadExecutor -> {},
                delayExecutor,
                userCodeLoader,
                checkpointCleaner,
                checkpointRecoveryFactory,
                jobManagerJobMetricGroup,
                schedulingStrategyFactory,
                failoverStrategyFactory,
                restartBackoffTimeStrategy,
                executionOperations,
                executionVertexVersioner,
                executionSlotAllocatorFactory,
                System.currentTimeMillis(),
                mainThreadExecutor,
                jobStatusListener,
                createExecutionGraphFactory(false),
                shuffleMaster,
                rpcTimeout,
                computeVertexParallelismStore(jobGraph),
                executionDeployerFactory);
    }

    public AdaptiveBatchScheduler buildAdaptiveBatchJobScheduler() throws Exception {
        return new AdaptiveBatchScheduler(
                log,
                jobGraph,
                ioExecutor,
                jobMasterConfiguration,
                componentMainThreadExecutor -> {},
                delayExecutor,
                userCodeLoader,
                checkpointCleaner,
                checkpointRecoveryFactory,
                jobManagerJobMetricGroup,
                new VertexwiseSchedulingStrategy.Factory(),
                failoverStrategyFactory,
                restartBackoffTimeStrategy,
                executionOperations,
                executionVertexVersioner,
                executionSlotAllocatorFactory,
                System.currentTimeMillis(),
                mainThreadExecutor,
                jobStatusListener,
                createExecutionGraphFactory(true),
                shuffleMaster,
                rpcTimeout,
                vertexParallelismDecider,
                defaultMaxParallelism);
    }

    public SpeculativeScheduler buildSpeculativeScheduler() throws Exception {
        return new SpeculativeScheduler(
                log,
                jobGraph,
                ioExecutor,
                jobMasterConfiguration,
                componentMainThreadExecutor -> {},
                delayExecutor,
                userCodeLoader,
                checkpointCleaner,
                checkpointRecoveryFactory,
                jobManagerJobMetricGroup,
                new VertexwiseSchedulingStrategy.Factory(),
                failoverStrategyFactory,
                restartBackoffTimeStrategy,
                executionOperations,
                executionVertexVersioner,
                executionSlotAllocatorFactory,
                System.currentTimeMillis(),
                mainThreadExecutor,
                jobStatusListener,
                createExecutionGraphFactory(true, new SpeculativeExecutionJobVertex.Factory()),
                shuffleMaster,
                rpcTimeout,
                vertexParallelismDecider,
                defaultMaxParallelism,
                blocklistOperations);
    }

    private ExecutionGraphFactory createExecutionGraphFactory(boolean isDynamicGraph) {
        return createExecutionGraphFactory(isDynamicGraph, new ExecutionJobVertex.Factory());
    }

    private ExecutionGraphFactory createExecutionGraphFactory(
            boolean isDynamicGraph, ExecutionJobVertex.Factory executionJobVertexFactory) {
        return new DefaultExecutionGraphFactory(
                jobMasterConfiguration,
                userCodeLoader,
                new DefaultExecutionDeploymentTracker(),
                futureExecutor,
                ioExecutor,
                rpcTimeout,
                jobManagerJobMetricGroup,
                blobWriter,
                shuffleMaster,
                partitionTracker,
                isDynamicGraph,
                executionJobVertexFactory);
    }
}
