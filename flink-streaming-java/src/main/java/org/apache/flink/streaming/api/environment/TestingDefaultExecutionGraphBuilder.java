/*
* Ported from the unit test utility of flink-runtime
* @ org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
* We should remove this once we find a better way to generate execution graph
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



    public TestingDefaultExecutionGraphBuilder setJobGraph(JobGraph jobGraph) {
        this.jobGraph = jobGraph;
        return this;
    }



    public TestingDefaultExecutionGraphBuilder setUserClassLoader(ClassLoader userClassLoader) {
        this.userClassLoader = userClassLoader;
        return this;
    }


    public TestingDefaultExecutionGraphBuilder setVertexParallelismStore(
            VertexParallelismStore store) {
        this.vertexParallelismStore = store;
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


}
