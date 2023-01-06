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
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.ResultPartitionBytes;
import org.apache.flink.runtime.executiongraph.failover.flip1.FixedDelayRestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.PartitionNotFoundException;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
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
import static org.apache.flink.shaded.guava30.com.google.common.collect.Iterables.getOnlyElement;
import static org.assertj.core.api.Assertions.assertThat;

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
    void testAdaptiveBatchScheduler() throws Exception {
        JobGraph jobGraph = createJobGraph(false);
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
    }

    @Test
    void testDecideParallelismForForwardTarget() throws Exception {
        JobGraph jobGraph = createJobGraph(true);
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
        assertThat(sinkExecutionJobVertex.getParallelism()).isEqualTo(SOURCE_PARALLELISM_1);

        // check that the jobGraph is updated
        assertThat(sink.getParallelism()).isEqualTo(SOURCE_PARALLELISM_1);
    }

    @Test
    void testUpdateBlockingResultInfoWhileScheduling() throws Exception {
        JobGraph jobGraph = createJobGraph(false);
        Iterator<JobVertex> jobVertexIterator = jobGraph.getVertices().iterator();
        JobVertex source1 = jobVertexIterator.next();
        JobVertex source2 = jobVertexIterator.next();
        JobVertex sink = jobVertexIterator.next();

        Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.AdaptiveBatch);

        final TestingJobMasterPartitionTracker partitionTracker =
                new TestingJobMasterPartitionTracker();
        partitionTracker.setIsPartitionTrackedFunction(ignore -> true);
        int maxParallelism = 6;

        AdaptiveBatchScheduler scheduler =
                new DefaultSchedulerBuilder(
                                jobGraph, mainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                        .setDelayExecutor(taskRestartExecutor)
                        .setJobMasterConfiguration(configuration)
                        .setPartitionTracker(partitionTracker)
                        .setRestartBackoffTimeStrategy(
                                new FixedDelayRestartBackoffTimeStrategy
                                                .FixedDelayRestartBackoffTimeStrategyFactory(10, 0)
                                        .create())
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

    private BlockingResultInfo getBlockingResultInfo(
            AdaptiveBatchScheduler scheduler, JobVertex jobVertex) {
        return scheduler.getBlockingResultInfo(
                getOnlyElement(jobVertex.getProducedDataSets()).getId());
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

    public JobGraph createJobGraph(boolean withForwardEdge) {
        final JobVertex source1 = createJobVertex("source1", SOURCE_PARALLELISM_1);
        final JobVertex source2 = createJobVertex("source2", SOURCE_PARALLELISM_2);
        final JobVertex sink = createJobVertex("sink", -1);
        sink.connectNewDataSetAsInput(
                source1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        sink.connectNewDataSetAsInput(
                source2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        if (withForwardEdge) {
            source1.getProducedDataSets().get(0).getConsumers().get(0).setForward(true);
        }
        return new JobGraph(new JobID(), "test job", source1, source2, sink);
    }

    private SchedulerBase createScheduler(
            JobGraph jobGraph)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.AdaptiveBatch);

        return new DefaultSchedulerBuilder(
                        jobGraph, mainThreadExecutor, EXECUTOR_RESOURCE.getExecutor())
                .setDelayExecutor(taskRestartExecutor)
                .setJobMasterConfiguration(configuration)
                .setVertexParallelismAndInputInfosDecider(createCustomParallelismDecider(10))
                .buildAdaptiveBatchJobScheduler();
    }
}
