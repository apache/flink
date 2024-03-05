/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.environment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointStatsTracker;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.deployment.TaskDeploymentDescriptorFactory;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraphBuilder;
import org.apache.flink.runtime.executiongraph.DefaultVertexAttemptNumberStore;
import org.apache.flink.runtime.executiongraph.ExecutionDeploymentListener;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionStateUpdateListener;
import org.apache.flink.runtime.executiongraph.MarkPartitionFinishedStrategy;
import org.apache.flink.runtime.executiongraph.NoOpExecutionDeploymentListener;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.JobGraph;

import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.VertexParallelismStore;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMaster;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

/** Builder of {@link ExecutionGraph} used in testing. */
public class TestingDefaultExecutionGraphBuilder {

    private static final Logger LOG =
            LoggerFactory.getLogger(TestingDefaultExecutionGraphBuilder.class);

    public static TestingDefaultExecutionGraphBuilder newBuilder() {
        return new TestingDefaultExecutionGraphBuilder();
    }

    private Time rpcTimeout = Time.fromDuration(RpcOptions.ASK_TIMEOUT_DURATION.defaultValue());
    private ClassLoader userClassLoader = DefaultExecutionGraph.class.getClassLoader();
    private BlobWriter blobWriter = VoidBlobWriter.getInstance();
    private ShuffleMaster<?> shuffleMaster = new NettyShuffleMaster(new Configuration());
    private JobMasterPartitionTracker partitionTracker = NoOpJobMasterPartitionTracker.INSTANCE;
    private Configuration jobMasterConfig = new Configuration();
    private JobGraph jobGraph = JobGraphBuilder.newStreamingJobGraphBuilder().build();
    private CompletedCheckpointStore completedCheckpointStore =
            new StandaloneCompletedCheckpointStore(1);
    private CheckpointIDCounter checkpointIdCounter = new StandaloneCheckpointIDCounter();
    private ExecutionDeploymentListener executionDeploymentListener =
            NoOpExecutionDeploymentListener.get();
    private ExecutionStateUpdateListener executionStateUpdateListener =
            (execution, previousState, newState) -> {};
    private VertexParallelismStore vertexParallelismStore;
    private ExecutionJobVertex.Factory executionJobVertexFactory = new ExecutionJobVertex.Factory();

    private MarkPartitionFinishedStrategy markPartitionFinishedStrategy =
            ResultPartitionType::isBlockingOrBlockingPersistentResultPartition;

    private boolean nonFinishedHybridPartitionShouldBeUnknown = false;

    private TestingDefaultExecutionGraphBuilder() {}

    public TestingDefaultExecutionGraphBuilder setJobMasterConfig(Configuration jobMasterConfig) {
        this.jobMasterConfig = jobMasterConfig;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setJobGraph(JobGraph jobGraph) {
        this.jobGraph = jobGraph;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setRpcTimeout(Time rpcTimeout) {
        this.rpcTimeout = rpcTimeout;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setUserClassLoader(ClassLoader userClassLoader) {
        this.userClassLoader = userClassLoader;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setBlobWriter(BlobWriter blobWriter) {
        this.blobWriter = blobWriter;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setShuffleMaster(ShuffleMaster<?> shuffleMaster) {
        this.shuffleMaster = shuffleMaster;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setPartitionTracker(
            JobMasterPartitionTracker partitionTracker) {
        this.partitionTracker = partitionTracker;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setCompletedCheckpointStore(
            CompletedCheckpointStore completedCheckpointStore) {
        this.completedCheckpointStore = completedCheckpointStore;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setCheckpointIdCounter(
            CheckpointIDCounter checkpointIdCounter) {
        this.checkpointIdCounter = checkpointIdCounter;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setExecutionDeploymentListener(
            ExecutionDeploymentListener executionDeploymentListener) {
        this.executionDeploymentListener = executionDeploymentListener;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setExecutionStateUpdateListener(
            ExecutionStateUpdateListener executionStateUpdateListener) {
        this.executionStateUpdateListener = executionStateUpdateListener;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setVertexParallelismStore(
            VertexParallelismStore store) {
        this.vertexParallelismStore = store;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setExecutionJobVertexFactory(
            ExecutionJobVertex.Factory executionJobVertexFactory) {
        this.executionJobVertexFactory = executionJobVertexFactory;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setMarkPartitionFinishedStrategy(
            MarkPartitionFinishedStrategy markPartitionFinishedStrategy) {
        this.markPartitionFinishedStrategy = markPartitionFinishedStrategy;
        return this;
    }

    public TestingDefaultExecutionGraphBuilder setNonFinishedHybridPartitionShouldBeUnknown(
            boolean nonFinishedHybridPartitionShouldBeUnknown) {
        this.nonFinishedHybridPartitionShouldBeUnknown = nonFinishedHybridPartitionShouldBeUnknown;
        return this;
    }

    private DefaultExecutionGraph build(
            boolean isDynamicGraph, ScheduledExecutorService executorService)
            throws JobException, JobExecutionException {
        return DefaultExecutionGraphBuilder.buildGraph(
                jobGraph,
                jobMasterConfig,
                executorService,
                executorService,
                userClassLoader,
                completedCheckpointStore,
                new CheckpointsCleaner(),
                checkpointIdCounter,
                rpcTimeout,
                blobWriter,
                LOG,
                shuffleMaster,
                partitionTracker,
                TaskDeploymentDescriptorFactory.PartitionLocationConstraint.fromJobType(
                        jobGraph.getJobType()),
                executionDeploymentListener,
                executionStateUpdateListener,
                System.currentTimeMillis(),
                new DefaultVertexAttemptNumberStore(),
                Optional.ofNullable(vertexParallelismStore)
                        .orElseGet(() -> SchedulerBase.computeVertexParallelismStore(jobGraph)),
                () -> new CheckpointStatsTracker(0, new UnregisteredMetricsGroup(), new JobID()),
                isDynamicGraph,
                executionJobVertexFactory,
                markPartitionFinishedStrategy,
                nonFinishedHybridPartitionShouldBeUnknown,
                UnregisteredMetricGroups.createUnregisteredJobManagerJobMetricGroup());
    }

    public DefaultExecutionGraph build(ScheduledExecutorService executorService)
            throws JobException, JobExecutionException {
        return build(false, executorService);
    }

    public DefaultExecutionGraph buildDynamicGraph(ScheduledExecutorService executorService)
            throws JobException, JobExecutionException {
        return build(true, executorService);
    }
}
