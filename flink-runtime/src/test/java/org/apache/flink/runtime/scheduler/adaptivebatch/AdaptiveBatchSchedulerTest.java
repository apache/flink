/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.api.common.JobID;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.core.failure.FailureEnricher;
import org.apache.flink.core.failure.TestingFailureEnricher;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.executiongraph.failover.flip1.FixedDelayRestartBackoffTimeStrategy.FixedDelayRestartBackoffTimeStrategyFactory;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.exceptionhistory.RootExceptionHistoryEntry;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.runtime.testtasks.NoOpInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.concurrent.ManuallyTriggeredScheduledExecutor;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder.createCustomParallelismDecider;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.createFailedTaskExecutionState;
import static org.apache.flink.runtime.scheduler.SchedulerTestingUtils.createFinishedTaskExecutionState;
import static org.apache.flink.runtime.scheduler.adaptivebatch.DefaultVertexParallelismAndInputInfosDeciderTest.createDecider;
import static org.apache.flink.shaded.guava31.com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link AdaptiveBatchScheduler}. */
class AdaptiveBatchSchedulerTest {

    private static final int SOURCE_PARALLELISM_1 = 6;
    private static final int SOURCE_PARALLELISM_2 = 4;
    private static final long SUBPARTITION_BYTES = 100L;

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private ComponentMainThreadExecutor mainThreadExecutor;
    private ManuallyTriggeredScheduledExecutor taskRestartExecutor;

    @BeforeEach
    void setUp() {
        mainThreadExecutor = ComponentMainThreadExecutorServiceAdapter.forMainThread();
        taskRestartExecutor = new ManuallyTriggeredScheduledExecutor();
    }

    @Test
    void testVertexInitializationFailureIsLabeled() throws Exception {
        final JobGraph jobGraph = createBrokenJobGraph();
        final TestingFailureEnricher failureEnricher = new TestingFailureEnricher();
        final RestartBackoffTimeStrategy restartStrategy =
                new FixedDelayRestartBackoffTimeStrategyFactory(Integer.MAX_VALUE, 0L).create();
        final SchedulerBase scheduler =
                createScheduler(jobGraph, Collections.singleton(failureEnricher), restartStrategy);
        // Triggered failure on initializeJobVertex that should be labeled
        assertThatThrownBy(scheduler::startScheduling).isInstanceOf(IllegalStateException.class);
        final Iterable<RootExceptionHistoryEntry> exceptionHistory =
                scheduler.requestJob().getExceptionHistory();
        final RootExceptionHistoryEntry failure = exceptionHistory.iterator().next();
        assertThat(failure.getException()).hasMessageContaining("The failure is not recoverable");
        assertThat(failure.getFailureLabels()).isEqualTo(failureEnricher.getFailureLabels());
    }

    @Test
    void testAdaptiveBatchScheduler() throws Exception {
        JobGraph jobGraph = createJobGraph();
        Iterator<JobVertex> jobVertexIterator = jobGraph.getVertices().iterator();
        JobVertex source1 = jobVertexIterator.next();
        JobVertex source2 = jobVertexIterator.next();
        JobVertex sink = jobVertexIterator.next();

        SchedulerBase scheduler = createScheduler(jobGraph);

        final DefaultExecutionGraph graph = (DefaultExecutionGraph) scheduler.getExecutionGraph();
        final ExecutionJobVertex sinkExecutionJobVertex = graph.getJobVertex(sink.getID());

        scheduler.startScheduling();
        assertThat(sinkExecutionJobVertex.getParallelism()).isEqualTo(-1);

        // trigger source1 finished.
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, source1);
        assertThat(sinkExecutionJobVertex.getParallelism()).isEqualTo(-1);

        // trigger source2 finished.
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, source2);
        assertThat(sinkExecutionJobVertex.getParallelism()).isEqualTo(10);

        // check that the jobGraph is updated
        assertThat(sink.getParallelism()).isEqualTo(10);

        // check aggregatedInputDataBytes of each ExecutionVertex calculated. Total number of
        // subpartitions of source1 is ceil(128 / 6) * 6 = 132, total number of subpartitions of
        // source2 is ceil(128 / 4) * 4 = 128, so total bytes is (132 + 128) * SUBPARTITION_BYTES =
        // 26_000L.
        checkAggregatedInputDataBytesIsCalculated(sinkExecutionJobVertex, 26_000L);
    }

    @Test
    void testDecideParallelismForForwardTarget() throws Exception {
        final JobVertex source = createJobVertex("source", SOURCE_PARALLELISM_1);
        final JobVertex map = createJobVertex("map", -1);
        final JobVertex sink = createJobVertex("sink", -1);

        map.connectNewDataSetAsInput(
                source, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        sink.connectNewDataSetAsInput(
                map, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        map.getProducedDataSets().get(0).getConsumers().get(0).setForward(true);

        SchedulerBase scheduler =
                createScheduler(
                        new JobGraph(new JobID(), "test job", source, map, sink),
                        createCustomParallelismDecider(
                                jobVertexId -> {
                                    if (jobVertexId.equals(map.getID())) {
                                        return 5;
                                    } else {
                                        return 10;
                                    }
                                }),
                        128);

        final DefaultExecutionGraph graph = (DefaultExecutionGraph) scheduler.getExecutionGraph();
        final ExecutionJobVertex mapExecutionJobVertex = graph.getJobVertex(map.getID());
        final ExecutionJobVertex sinkExecutionJobVertex = graph.getJobVertex(sink.getID());

        scheduler.startScheduling();
        assertThat(mapExecutionJobVertex.getParallelism()).isEqualTo(-1);
        assertThat(sinkExecutionJobVertex.getParallelism()).isEqualTo(-1);

        // trigger source finished.
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, source);
        assertThat(mapExecutionJobVertex.getParallelism()).isEqualTo(5);
        assertThat(sinkExecutionJobVertex.getParallelism()).isEqualTo(-1);

        // trigger map finished.
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, map);
        assertThat(mapExecutionJobVertex.getParallelism()).isEqualTo(5);
        assertThat(sinkExecutionJobVertex.getParallelism()).isEqualTo(5);

        // check that the jobGraph is updated
        assertThat(sink.getParallelism()).isEqualTo(5);

        // check aggregatedInputDataBytes of each ExecutionVertex calculated. Total number of
        // subpartitions of map is ceil(128 / 5) * 5 = 130, so total bytes sink consume is 130 *
        // SUBPARTITION_BYTES = 13_000L.
        checkAggregatedInputDataBytesIsCalculated(sinkExecutionJobVertex, 13_000L);
    }

    @Test
    void testUpdateBlockingResultInfoWhileScheduling() throws Exception {
        JobGraph jobGraph = createJobGraph();
        Iterator<JobVertex> jobVertexIterator = jobGraph.getVertices().iterator();
        JobVertex source1 = jobVertexIterator.next();
        JobVertex source2 = jobVertexIterator.next();
        JobVertex sink = jobVertexIterator.next();

        final TestingJobMasterPartitionTracker partitionTracker =
                new TestingJobMasterPartitionTracker();
        partitionTracker.setIsPartitionTrackedFunction(ignore -> true);
        int maxParallelism = 6;

        AdaptiveBatchScheduler scheduler =
                new DefaultSchedulerBuilder(
                                jobGraph, mainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                        .setDelayExecutor(taskRestartExecutor)
                        .setPartitionTracker(partitionTracker)
                        .setRestartBackoffTimeStrategy(
                                new FixedDelayRestartBackoffTimeStrategyFactory(10, 0).create())
                        .setVertexParallelismAndInputInfosDecider(
                                createCustomParallelismDecider(maxParallelism))
                        .setDefaultMaxParallelism(maxParallelism)
                        .buildAdaptiveBatchJobScheduler();

        final DefaultExecutionGraph graph = (DefaultExecutionGraph) scheduler.getExecutionGraph();
        final ExecutionJobVertex source1ExecutionJobVertex = graph.getJobVertex(source1.getID());
        final ExecutionJobVertex sinkExecutionJobVertex = graph.getJobVertex(sink.getID());

        PointwiseBlockingResultInfo blockingResultInfo;

        scheduler.startScheduling();
        // trigger source1 finished.
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, source1);
        blockingResultInfo =
                (PointwiseBlockingResultInfo) getBlockingResultInfo(scheduler, source1);
        assertThat(blockingResultInfo.getNumOfRecordedPartitions()).isEqualTo(SOURCE_PARALLELISM_1);

        // trigger source2 finished.
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, source2);
        blockingResultInfo =
                (PointwiseBlockingResultInfo) getBlockingResultInfo(scheduler, source2);
        assertThat(blockingResultInfo.getNumOfRecordedPartitions()).isEqualTo(SOURCE_PARALLELISM_2);

        // trigger sink fail with partition not found
        triggerFailedByPartitionNotFound(
                scheduler,
                source1ExecutionJobVertex.getTaskVertices()[0],
                sinkExecutionJobVertex.getTaskVertices()[0]);

        taskRestartExecutor.triggerScheduledTasks();

        // check the partition info is reset
        assertThat(
                        ((PointwiseBlockingResultInfo) getBlockingResultInfo(scheduler, source1))
                                .getNumOfRecordedPartitions())
                .isEqualTo(SOURCE_PARALLELISM_1 - 1);
    }

    /**
     * Test a vertex has multiple job edges connecting to the same intermediate result. In this
     * case, the amount of data consumed by this vertex should be (N * the amount of data of the
     * result).
     */
    @Test
    void testConsumeOneResultTwice() throws Exception {
        final JobVertex source = createJobVertex("source1", 1);
        final JobVertex sink = createJobVertex("sink", -1);
        final IntermediateDataSetID intermediateDataSetId = new IntermediateDataSetID();

        // sink consume the same result twice
        sink.connectNewDataSetAsInput(
                source,
                DistributionPattern.ALL_TO_ALL,
                ResultPartitionType.BLOCKING,
                intermediateDataSetId,
                false);
        sink.connectNewDataSetAsInput(
                source,
                DistributionPattern.ALL_TO_ALL,
                ResultPartitionType.BLOCKING,
                intermediateDataSetId,
                false);

        SchedulerBase scheduler =
                createScheduler(
                        new JobGraph(new JobID(), "test job", source, sink),
                        createDecider(1, 16, 4 * SUBPARTITION_BYTES),
                        16);

        final DefaultExecutionGraph graph = (DefaultExecutionGraph) scheduler.getExecutionGraph();
        final ExecutionJobVertex sinkExecutionJobVertex = graph.getJobVertex(sink.getID());

        scheduler.startScheduling();
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, source);

        // check sink's parallelism
        assertThat(sinkExecutionJobVertex.getParallelism()).isEqualTo(8);
        assertThat(sink.getParallelism()).isEqualTo(8);
    }

    @Test
    void testParallelismDecidedVerticesCanBeInitializedEarlier() throws Exception {
        final JobVertex source = createJobVertex("source", 8);
        final JobVertex sink = createJobVertex("sink", 8);
        sink.connectNewDataSetAsInput(
                source, DistributionPattern.ALL_TO_ALL, ResultPartitionType.BLOCKING);

        SchedulerBase scheduler =
                createScheduler(new JobGraph(new JobID(), "test job", source, sink));
        final DefaultExecutionGraph graph = (DefaultExecutionGraph) scheduler.getExecutionGraph();
        final ExecutionJobVertex sinkExecutionJobVertex = graph.getJobVertex(sink.getID());

        scheduler.startScheduling();
        // check sink is not initialized
        assertThat(sinkExecutionJobVertex.isInitialized()).isTrue();
    }

    @Test
    void testUserConfiguredMaxParallelismIsLargerThanGlobalMaxParallelism() throws Exception {
        testUserConfiguredMaxParallelism(1, 32, 128, 1L, 32);
    }

    @Test
    void testUserConfiguredMaxParallelismIsSmallerThanGlobalMaxParallelism() throws Exception {
        testUserConfiguredMaxParallelism(1, 128, 32, 1L, 32);
    }

    @Test
    void testUserConfiguredMaxParallelismIsSmallerThanGlobalMinParallelism() throws Exception {
        testUserConfiguredMaxParallelism(16, 128, 8, 4 * SUBPARTITION_BYTES, 8);
    }

    @Test
    void testUserConfiguredMaxParallelismIsSmallerThanGlobalDefaultSourceParallelism()
            throws Exception {
        final JobVertex source = createJobVertex("source", -1);
        source.setMaxParallelism(8);

        SchedulerBase scheduler =
                createScheduler(
                        new JobGraph(new JobID(), "test job", source),
                        createDecider(1, 128, 1L, 32),
                        128);

        scheduler.startScheduling();

        // check source's parallelism
        assertThat(source.getParallelism()).isEqualTo(8);
    }

    void testUserConfiguredMaxParallelism(
            int globalMinParallelism,
            int globalMaxParallelism,
            int userConfiguredMaxParallelism,
            long dataVolumePerTask,
            int expectedParallelism)
            throws Exception {
        final JobVertex source = createJobVertex("source", 8);
        final JobVertex sink = createJobVertex("sink", -1);
        sink.setMaxParallelism(userConfiguredMaxParallelism);

        sink.connectNewDataSetAsInput(
                source, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

        SchedulerBase scheduler =
                createScheduler(
                        new JobGraph(new JobID(), "test job", source, sink),
                        createDecider(
                                globalMinParallelism, globalMaxParallelism, dataVolumePerTask),
                        globalMaxParallelism);

        scheduler.startScheduling();
        transitionExecutionsState(scheduler, ExecutionState.FINISHED, source);

        // check sink's parallelism
        assertThat(sink.getParallelism()).isEqualTo(expectedParallelism);
    }

    private BlockingResultInfo getBlockingResultInfo(
            AdaptiveBatchScheduler scheduler, JobVertex jobVertex) {
        return scheduler.getBlockingResultInfo(
                getOnlyElement(jobVertex.getProducedDataSets()).getId());
    }

    private void checkAggregatedInputDataBytesIsCalculated(
            ExecutionJobVertex sinkExecutionJobVertex, long expectedTotalBytes) {
        final ExecutionVertex[] executionVertices = sinkExecutionJobVertex.getTaskVertices();
        long totalInputBytes = 0;
        for (ExecutionVertex ev : executionVertices) {
            long executionInputBytes = ev.getInputBytes();
            assertThat(executionInputBytes).isNotEqualTo(-1);
            totalInputBytes += executionInputBytes;
        }

        assertThat(totalInputBytes).isEqualTo(expectedTotalBytes);
    }

    private void triggerFailedByPartitionNotFound(
            SchedulerBase scheduler,
            ExecutionVertex producerVertex,
            ExecutionVertex consumerVertex) {
        final Execution execution = consumerVertex.getCurrentExecutionAttempt();
        final IntermediateResultPartitionID partitionId =
                getOnlyElement(producerVertex.getProducedPartitions().values()).getPartitionId();
        // trigger execution vertex failed by partition not found.
        transitionExecutionsState(
                scheduler,
                ExecutionState.FAILED,
                Collections.singletonList(execution),
                new PartitionNotFoundException(
                        new ResultPartitionID(
                                partitionId,
                                producerVertex.getCurrentExecutionAttempt().getAttemptId())));
    }

    /** Transit the state of all executions. */
    public static void transitionExecutionsState(
            final SchedulerBase scheduler,
            final ExecutionState state,
            List<Execution> executions,
            @Nullable Throwable throwable) {
        for (Execution execution : executions) {
            TaskExecutionState taskExecutionState;
            if (state == ExecutionState.FINISHED) {
                taskExecutionState =
                        createFinishedTaskExecutionState(
                                execution.getAttemptId(),
                                createResultPartitionBytesForExecution(execution));
            } else if (state == ExecutionState.FAILED) {
                taskExecutionState =
                        createFailedTaskExecutionState(execution.getAttemptId(), throwable);
            } else {
                throw new UnsupportedOperationException("Unsupported state " + state);
            }
            scheduler.updateTaskExecutionState(taskExecutionState);
        }
    }

    static Map<IntermediateResultPartitionID, ResultPartitionBytes>
            createResultPartitionBytesForExecution(Execution execution) {
        Map<IntermediateResultPartitionID, ResultPartitionBytes> partitionBytes = new HashMap<>();
        execution
                .getVertex()
                .getProducedPartitions()
                .forEach(
                        (partitionId, partition) -> {
                            int numOfSubpartitions = partition.getNumberOfSubpartitions();
                            partitionBytes.put(
                                    partitionId,
                                    new ResultPartitionBytes(
                                            LongStream.range(0, numOfSubpartitions)
                                                    .boxed()
                                                    .mapToLong(ignored -> SUBPARTITION_BYTES)
                                                    .toArray()));
                        });
        return partitionBytes;
    }

    /** Transit the state of all executions in the Job Vertex. */
    public static void transitionExecutionsState(
            final SchedulerBase scheduler, final ExecutionState state, final JobVertex jobVertex) {
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();
        List<Execution> executions =
                Arrays.asList(executionGraph.getJobVertex(jobVertex.getID()).getTaskVertices())
                        .stream()
                        .map(ExecutionVertex::getCurrentExecutionAttempt)
                        .collect(Collectors.toList());
        transitionExecutionsState(scheduler, state, executions, null);
    }

    public JobVertex createJobVertex(String jobVertexName, int parallelism) {
        final JobVertex jobVertex = new JobVertex(jobVertexName);
        jobVertex.setInvokableClass(NoOpInvokable.class);
        if (parallelism > 0) {
            jobVertex.setParallelism(parallelism);
        }
        return jobVertex;
    }

    public JobGraph createJobGraph() {
        return createJobGraph(false);
    }

    private JobGraph createBrokenJobGraph() {
        // this will break the JobGraph by using the same dataset id twice
        return createJobGraph(true);
    }

    public JobGraph createJobGraph(boolean broken) {
        final JobVertex source1 = createJobVertex("source1", SOURCE_PARALLELISM_1);
        final JobVertex source2 = createJobVertex("source2", SOURCE_PARALLELISM_2);
        final JobVertex sink = createJobVertex("sink", -1);
        final IntermediateDataSetID sharedDataSetId = new IntermediateDataSetID();
        sink.connectNewDataSetAsInput(
                source1,
                DistributionPattern.POINTWISE,
                ResultPartitionType.BLOCKING,
                broken ? sharedDataSetId : new IntermediateDataSetID(),
                false);
        sink.connectNewDataSetAsInput(
                source2,
                DistributionPattern.POINTWISE,
                ResultPartitionType.BLOCKING,
                broken ? sharedDataSetId : new IntermediateDataSetID(),
                false);
        return new JobGraph(new JobID(), "test job", source1, source2, sink);
    }

    private SchedulerBase createScheduler(JobGraph jobGraph) throws Exception {
        return createScheduler(
                jobGraph,
                createCustomParallelismDecider(10),
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM.defaultValue());
    }

    private SchedulerBase createScheduler(
            JobGraph jobGraph,
            VertexParallelismAndInputInfosDecider vertexParallelismAndInputInfosDecider,
            int defaultMaxParallelism)
            throws Exception {
        return createSchedulerBuilder(jobGraph)
                .setVertexParallelismAndInputInfosDecider(vertexParallelismAndInputInfosDecider)
                .setDefaultMaxParallelism(defaultMaxParallelism)
                .buildAdaptiveBatchJobScheduler();
    }

    private SchedulerBase createScheduler(
            JobGraph jobGraph,
            Collection<FailureEnricher> failureEnrichers,
            RestartBackoffTimeStrategy strategy)
            throws Exception {
        return createSchedulerBuilder(jobGraph)
                .setRestartBackoffTimeStrategy(strategy)
                .setFailureEnrichers(failureEnrichers)
                .buildAdaptiveBatchJobScheduler();
    }

    private DefaultSchedulerBuilder createSchedulerBuilder(JobGraph jobGraph) {
        return new DefaultSchedulerBuilder(
                        jobGraph, mainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                .setDelayExecutor(taskRestartExecutor);
    }
}
