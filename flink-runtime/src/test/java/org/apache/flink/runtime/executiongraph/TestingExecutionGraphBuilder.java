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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.runtime.JobException;
import org.apache.flink.runtime.akka.AkkaUtils;
import org.apache.flink.runtime.blob.BlobWriter;
import org.apache.flink.runtime.blob.VoidBlobWriter;
import org.apache.flink.runtime.checkpoint.CheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStore;
import org.apache.flink.runtime.checkpoint.StandaloneCheckpointIDCounter;
import org.apache.flink.runtime.checkpoint.StandaloneCompletedCheckpointStore;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.NoOpJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobmaster.TestingLogicalSlotBuilder;
import org.apache.flink.runtime.jobmaster.slotpool.SlotProvider;
import org.apache.flink.runtime.shuffle.NettyShuffleMaster;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.testingUtils.TestingUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

/** Builder of {@link ExecutionGraph} used in testing. */
public class TestingExecutionGraphBuilder {

    private static final Logger LOG = LoggerFactory.getLogger(TestingExecutionGraphBuilder.class);

    public static TestingExecutionGraphBuilder newBuilder() {
        return new TestingExecutionGraphBuilder();
    }

    private ScheduledExecutorService futureExecutor = TestingUtils.defaultExecutor();
    private Executor ioExecutor = TestingUtils.defaultExecutor();
    private Time rpcTimeout = AkkaUtils.getDefaultTimeout();
    private SlotProvider slotProvider =
            new TestingSlotProvider(
                    slotRequestId ->
                            CompletableFuture.completedFuture(
                                    new TestingLogicalSlotBuilder().createTestingLogicalSlot()));
    private ClassLoader userClassLoader = ExecutionGraph.class.getClassLoader();
    private BlobWriter blobWriter = VoidBlobWriter.getInstance();
    private Time allocationTimeout = AkkaUtils.getDefaultTimeout();
    private ShuffleMaster<?> shuffleMaster = NettyShuffleMaster.INSTANCE;
    private JobMasterPartitionTracker partitionTracker = NoOpJobMasterPartitionTracker.INSTANCE;
    private Configuration jobMasterConfig = new Configuration();
    private JobGraph jobGraph = new JobGraph();
    private MetricGroup metricGroup = new UnregisteredMetricsGroup();
    private CompletedCheckpointStore completedCheckpointStore =
            new StandaloneCompletedCheckpointStore(1);
    private CheckpointIDCounter checkpointIdCounter = new StandaloneCheckpointIDCounter();
    private ExecutionDeploymentListener executionDeploymentListener =
            NoOpExecutionDeploymentListener.get();
    private ExecutionStateUpdateListener executionStateUpdateListener = (execution, newState) -> {};

    private TestingExecutionGraphBuilder() {}

    public TestingExecutionGraphBuilder setJobMasterConfig(Configuration jobMasterConfig) {
        this.jobMasterConfig = jobMasterConfig;
        return this;
    }

    public TestingExecutionGraphBuilder setJobGraph(JobGraph jobGraph) {
        this.jobGraph = jobGraph;
        return this;
    }

    public TestingExecutionGraphBuilder setFutureExecutor(ScheduledExecutorService futureExecutor) {
        this.futureExecutor = futureExecutor;
        return this;
    }

    public TestingExecutionGraphBuilder setIoExecutor(Executor ioExecutor) {
        this.ioExecutor = ioExecutor;
        return this;
    }

    public TestingExecutionGraphBuilder setRpcTimeout(Time rpcTimeout) {
        this.rpcTimeout = rpcTimeout;
        return this;
    }

    public TestingExecutionGraphBuilder setSlotProvider(SlotProvider slotProvider) {
        this.slotProvider = slotProvider;
        return this;
    }

    public TestingExecutionGraphBuilder setUserClassLoader(ClassLoader userClassLoader) {
        this.userClassLoader = userClassLoader;
        return this;
    }

    public TestingExecutionGraphBuilder setBlobWriter(BlobWriter blobWriter) {
        this.blobWriter = blobWriter;
        return this;
    }

    public TestingExecutionGraphBuilder setAllocationTimeout(Time allocationTimeout) {
        this.allocationTimeout = allocationTimeout;
        return this;
    }

    public TestingExecutionGraphBuilder setShuffleMaster(ShuffleMaster<?> shuffleMaster) {
        this.shuffleMaster = shuffleMaster;
        return this;
    }

    public TestingExecutionGraphBuilder setPartitionTracker(
            JobMasterPartitionTracker partitionTracker) {
        this.partitionTracker = partitionTracker;
        return this;
    }

    public TestingExecutionGraphBuilder setMetricGroup(MetricGroup metricGroup) {
        this.metricGroup = metricGroup;
        return this;
    }

    public TestingExecutionGraphBuilder setCompletedCheckpointStore(
            CompletedCheckpointStore completedCheckpointStore) {
        this.completedCheckpointStore = completedCheckpointStore;
        return this;
    }

    public TestingExecutionGraphBuilder setCheckpointIdCounter(
            CheckpointIDCounter checkpointIdCounter) {
        this.checkpointIdCounter = checkpointIdCounter;
        return this;
    }

    public TestingExecutionGraphBuilder setExecutionDeploymentListener(
            ExecutionDeploymentListener executionDeploymentListener) {
        this.executionDeploymentListener = executionDeploymentListener;
        return this;
    }

    public TestingExecutionGraphBuilder setExecutionStateUpdateListener(
            ExecutionStateUpdateListener executionStateUpdateListener) {
        this.executionStateUpdateListener = executionStateUpdateListener;
        return this;
    }

    public ExecutionGraph build() throws JobException, JobExecutionException {
        return ExecutionGraphBuilder.buildGraph(
                jobGraph,
                jobMasterConfig,
                futureExecutor,
                ioExecutor,
                slotProvider,
                userClassLoader,
                completedCheckpointStore,
                new CheckpointsCleaner(),
                checkpointIdCounter,
                rpcTimeout,
                metricGroup,
                blobWriter,
                allocationTimeout,
                LOG,
                shuffleMaster,
                partitionTracker,
                executionDeploymentListener,
                executionStateUpdateListener,
                System.currentTimeMillis());
    }
}
