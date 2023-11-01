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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.PartitionGroupReleaseStrategy;
import org.apache.flink.runtime.io.network.partition.JobMasterPartitionTracker;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.io.network.partition.TestingJobMasterPartitionTracker;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.scheduler.DefaultSchedulerBuilder;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.SchedulerTestingUtils;
import org.apache.flink.runtime.taskmanager.TaskExecutionState;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for the interactions of the {@link ExecutionGraph} and {@link
 * PartitionGroupReleaseStrategy}.
 */
class ExecutionGraphPartitionReleaseTest {

    @RegisterExtension
    public static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    @RegisterExtension
    public static final TestingComponentMainThreadExecutor.Extension MAIN_THREAD_EXTENSION =
            new TestingComponentMainThreadExecutor.Extension();

    private final TestingComponentMainThreadExecutor mainThreadExecutor =
            MAIN_THREAD_EXTENSION.getComponentMainThreadTestExecutor();

    @Test
    void testStrategyNotifiedOfFinishedVerticesAndResultsRespected() throws Exception {
        // setup a simple pipeline of 3 operators with blocking partitions
        final JobVertex sourceVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobVertex operatorVertex = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobVertex sinkVertex = ExecutionGraphTestUtils.createNoOpVertex(1);

        operatorVertex.connectNewDataSetAsInput(
                sourceVertex, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        sinkVertex.connectNewDataSetAsInput(
                operatorVertex, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

        // setup partition tracker to intercept partition release calls
        final TestingJobMasterPartitionTracker partitionTracker =
                new TestingJobMasterPartitionTracker();
        final Queue<ResultPartitionID> releasedPartitions = new ArrayDeque<>();
        partitionTracker.setStopTrackingAndReleasePartitionsConsumer(
                partitionIds -> releasedPartitions.add(partitionIds.iterator().next()));

        final SchedulerBase scheduler =
                createScheduler(partitionTracker, sourceVertex, operatorVertex, sinkVertex);
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        // finish vertices one after another, and verify that the appropriate partitions are
        // released
        mainThreadExecutor.execute(
                () -> {
                    final Execution sourceExecution =
                            getCurrentExecution(sourceVertex, executionGraph);
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionState(
                                    sourceExecution.getAttemptId(), ExecutionState.FINISHED));
                    assertThat(releasedPartitions).isEmpty();
                });

        mainThreadExecutor.execute(
                () -> {
                    final Execution sourceExecution =
                            getCurrentExecution(sourceVertex, executionGraph);
                    final Execution operatorExecution =
                            getCurrentExecution(operatorVertex, executionGraph);
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionState(
                                    operatorExecution.getAttemptId(), ExecutionState.FINISHED));
                    assertThat(releasedPartitions).hasSize(1);
                    assertThat(releasedPartitions.remove())
                            .isEqualTo(
                                    new ResultPartitionID(
                                            sourceExecution
                                                    .getVertex()
                                                    .getProducedPartitions()
                                                    .keySet()
                                                    .iterator()
                                                    .next(),
                                            sourceExecution.getAttemptId()));
                });

        mainThreadExecutor.execute(
                () -> {
                    final Execution operatorExecution =
                            getCurrentExecution(operatorVertex, executionGraph);
                    final Execution sinkExecution = getCurrentExecution(sinkVertex, executionGraph);
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionState(
                                    sinkExecution.getAttemptId(), ExecutionState.FINISHED));

                    assertThat(releasedPartitions).hasSize(1);
                    assertThat(releasedPartitions.remove())
                            .isEqualTo(
                                    new ResultPartitionID(
                                            operatorExecution
                                                    .getVertex()
                                                    .getProducedPartitions()
                                                    .keySet()
                                                    .iterator()
                                                    .next(),
                                            operatorExecution.getAttemptId()));
                });
    }

    @Test
    void testStrategyNotifiedOfUnFinishedVertices() throws Exception {
        // setup a pipeline of 2 failover regions (f1 -> f2), where
        // f1 is just a source
        // f2 consists of 3 operators (o1,o2,o3), where o1 consumes f1, and o2/o3 consume o1
        final JobVertex sourceVertex = ExecutionGraphTestUtils.createNoOpVertex("source", 1);
        final JobVertex operator1Vertex = ExecutionGraphTestUtils.createNoOpVertex("operator1", 1);
        final JobVertex operator2Vertex = ExecutionGraphTestUtils.createNoOpVertex("operator2", 1);
        final JobVertex operator3Vertex = ExecutionGraphTestUtils.createNoOpVertex("operator3", 1);

        operator1Vertex.connectNewDataSetAsInput(
                sourceVertex, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        operator2Vertex.connectNewDataSetAsInput(
                operator1Vertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);
        operator3Vertex.connectNewDataSetAsInput(
                operator1Vertex, DistributionPattern.ALL_TO_ALL, ResultPartitionType.PIPELINED);

        // setup partition tracker to intercept partition release calls
        final TestingJobMasterPartitionTracker partitionTracker =
                new TestingJobMasterPartitionTracker();
        final Queue<ResultPartitionID> releasedPartitions = new ArrayDeque<>();
        partitionTracker.setStopTrackingAndReleasePartitionsConsumer(
                partitionIds -> releasedPartitions.add(partitionIds.iterator().next()));

        final SchedulerBase scheduler =
                createScheduler(
                        partitionTracker,
                        sourceVertex,
                        operator1Vertex,
                        operator2Vertex,
                        operator3Vertex);
        final ExecutionGraph executionGraph = scheduler.getExecutionGraph();

        mainThreadExecutor.execute(
                () -> {
                    final Execution sourceExecution =
                            getCurrentExecution(sourceVertex, executionGraph);
                    // finish the source; this should not result in any release calls since the
                    // consumer o1 was not finished
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionState(
                                    sourceExecution.getAttemptId(), ExecutionState.FINISHED));
                    assertThat(releasedPartitions).isEmpty();
                });

        mainThreadExecutor.execute(
                () -> {
                    final Execution operator1Execution =
                            getCurrentExecution(operator1Vertex, executionGraph);
                    // finish o1 and schedule the consumers (o2,o3); this should not result in any
                    // release calls since not all operators of the pipelined region are finished
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionState(
                                    operator1Execution.getAttemptId(), ExecutionState.FINISHED));
                    assertThat(releasedPartitions).isEmpty();
                });

        mainThreadExecutor.execute(
                () -> {
                    final Execution operator2Execution =
                            getCurrentExecution(operator2Vertex, executionGraph);
                    // finish o2; this should not result in any release calls since o3 was not
                    // finished
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionState(
                                    operator2Execution.getAttemptId(), ExecutionState.FINISHED));
                    assertThat(releasedPartitions).isEmpty();
                });

        mainThreadExecutor.execute(
                () -> {
                    final Execution operator2Execution =
                            getCurrentExecution(operator2Vertex, executionGraph);
                    // reset o2
                    operator2Execution.getVertex().resetForNewExecution();
                    assertThat(releasedPartitions).isEmpty();
                });

        mainThreadExecutor.execute(
                () -> {
                    final Execution operator3Execution =
                            getCurrentExecution(operator3Vertex, executionGraph);
                    // finish o3; this should not result in any release calls since o2 was reset
                    scheduler.updateTaskExecutionState(
                            new TaskExecutionState(
                                    operator3Execution.getAttemptId(), ExecutionState.FINISHED));
                    assertThat(releasedPartitions).isEmpty();
                });
    }

    private static Execution getCurrentExecution(
            final JobVertex jobVertex, final ExecutionGraph executionGraph) {
        return executionGraph
                .getJobVertex(jobVertex.getID())
                .getTaskVertices()[0]
                .getCurrentExecutionAttempt();
    }

    private SchedulerBase createScheduler(
            final JobMasterPartitionTracker partitionTracker, final JobVertex... vertices)
            throws Exception {

        final JobGraph jobGraph = JobGraphTestUtils.batchJobGraph(vertices);
        final SchedulerBase scheduler =
                new DefaultSchedulerBuilder(
                                jobGraph,
                                mainThreadExecutor.getMainThreadExecutor(),
                                EXECUTOR_EXTENSION.getExecutor())
                        .setExecutionSlotAllocatorFactory(
                                SchedulerTestingUtils.newSlotSharingExecutionSlotAllocatorFactory())
                        .setPartitionTracker(partitionTracker)
                        .build();

        mainThreadExecutor.execute(scheduler::startScheduling);

        return scheduler;
    }
}
