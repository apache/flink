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

package org.apache.flink.runtime.scheduler.strategy;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.executiongraph.TestingDefaultExecutionGraphBuilder;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphBuilder;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.scheduler.strategy.StrategyTestUtil.assertLatestScheduledVerticesAreEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link PipelinedRegionSchedulingStrategy}. */
class PipelinedRegionSchedulingStrategyTest {
    @RegisterExtension
    public static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorExtension();

    private TestingSchedulerOperations testingSchedulerOperation;

    private static final int PARALLELISM = 2;

    private TestingSchedulingTopology testingSchedulingTopology;

    private List<TestingSchedulingExecutionVertex> source;

    private List<TestingSchedulingExecutionVertex> map1;

    private List<TestingSchedulingExecutionVertex> map2;

    private List<TestingSchedulingExecutionVertex> map3;

    private List<TestingSchedulingExecutionVertex> sink;

    @BeforeEach
    void setUp() {
        testingSchedulerOperation = new TestingSchedulerOperations();

        buildTopology();
    }

    private void buildTopology() {
        testingSchedulingTopology = new TestingSchedulingTopology();

        source =
                testingSchedulingTopology
                        .addExecutionVertices()
                        .withParallelism(PARALLELISM)
                        .finish();
        map1 =
                testingSchedulingTopology
                        .addExecutionVertices()
                        .withParallelism(PARALLELISM)
                        .finish();

        map2 =
                testingSchedulingTopology
                        .addExecutionVertices()
                        .withParallelism(PARALLELISM)
                        .finish();

        map3 =
                testingSchedulingTopology
                        .addExecutionVertices()
                        .withParallelism(PARALLELISM)
                        .finish();

        sink =
                testingSchedulingTopology
                        .addExecutionVertices()
                        .withParallelism(PARALLELISM)
                        .finish();

        testingSchedulingTopology
                .connectPointwise(source, map1)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.PIPELINED_BOUNDED)
                .finish();
        testingSchedulingTopology
                .connectPointwise(map1, map2)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.HYBRID_FULL)
                .finish();
        testingSchedulingTopology
                .connectPointwise(map2, map3)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.HYBRID_SELECTIVE)
                .finish();
        testingSchedulingTopology
                .connectAllToAll(map3, sink)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();
    }

    @Test
    void testStartScheduling() {
        startScheduling(testingSchedulingTopology);

        final List<List<TestingSchedulingExecutionVertex>> expectedScheduledVertices =
                new ArrayList<>();
        expectedScheduledVertices.add(Arrays.asList(source.get(0), map1.get(0)));
        expectedScheduledVertices.add(Arrays.asList(source.get(1), map1.get(1)));
        expectedScheduledVertices.add(Arrays.asList(map2.get(0)));
        expectedScheduledVertices.add(Arrays.asList(map2.get(1)));
        expectedScheduledVertices.add(Arrays.asList(map3.get(0)));
        expectedScheduledVertices.add(Arrays.asList(map3.get(1)));
        assertLatestScheduledVerticesAreEqualTo(
                expectedScheduledVertices, testingSchedulerOperation);
    }

    @Test
    void testRestartTasks() {
        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                startScheduling(testingSchedulingTopology);

        final Set<ExecutionVertexID> verticesToRestart =
                Stream.of(source, map1, map2, map3, sink)
                        .flatMap(List::stream)
                        .map(TestingSchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet());

        schedulingStrategy.restartTasks(verticesToRestart);

        final List<List<TestingSchedulingExecutionVertex>> expectedScheduledVertices =
                new ArrayList<>();
        expectedScheduledVertices.add(Arrays.asList(source.get(0), map1.get(0)));
        expectedScheduledVertices.add(Arrays.asList(source.get(1), map1.get(1)));
        expectedScheduledVertices.add(Arrays.asList(map2.get(0)));
        expectedScheduledVertices.add(Arrays.asList(map2.get(1)));
        expectedScheduledVertices.add(Arrays.asList(map3.get(0)));
        expectedScheduledVertices.add(Arrays.asList(map3.get(1)));
        assertLatestScheduledVerticesAreEqualTo(
                expectedScheduledVertices, testingSchedulerOperation);
    }

    @Test
    void testNotifyingBlockingResultPartitionProducerFinished() {
        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                startScheduling(testingSchedulingTopology);

        final TestingSchedulingExecutionVertex upstream1 = map3.get(0);
        upstream1.getProducedResults().iterator().next().markFinished();
        schedulingStrategy.onExecutionStateChange(upstream1.getId(), ExecutionState.FINISHED);

        // sinks' inputs are not all consumable yet so they are not scheduled
        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(6);

        final TestingSchedulingExecutionVertex upstream2 = map3.get(1);
        upstream2.getProducedResults().iterator().next().markFinished();
        schedulingStrategy.onExecutionStateChange(upstream2.getId(), ExecutionState.FINISHED);

        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(8);

        final List<List<TestingSchedulingExecutionVertex>> expectedScheduledVertices =
                new ArrayList<>();
        expectedScheduledVertices.add(Arrays.asList(sink.get(0)));
        expectedScheduledVertices.add(Arrays.asList(sink.get(1)));
        assertLatestScheduledVerticesAreEqualTo(
                expectedScheduledVertices, testingSchedulerOperation);
    }

    @Test
    void testSchedulingTopologyWithPersistentBlockingEdges() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> v1 =
                topology.addExecutionVertices().withParallelism(1).finish();
        final List<TestingSchedulingExecutionVertex> v2 =
                topology.addExecutionVertices().withParallelism(1).finish();

        topology.connectPointwise(v1, v2)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.BLOCKING_PERSISTENT)
                .finish();

        startScheduling(topology);

        final List<List<TestingSchedulingExecutionVertex>> expectedScheduledVertices =
                new ArrayList<>();
        expectedScheduledVertices.add(Arrays.asList(v1.get(0)));
        assertLatestScheduledVerticesAreEqualTo(
                expectedScheduledVertices, testingSchedulerOperation);
    }

    @Test
    void testComputingCrossRegionConsumedPartitionGroupsCorrectly() throws Exception {
        final JobVertex v1 = createJobVertex("v1", 4);
        final JobVertex v2 = createJobVertex("v2", 3);
        final JobVertex v3 = createJobVertex("v3", 2);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        v3.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2, v3));
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder().addJobVertices(ordered).build();
        final ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final SchedulingTopology schedulingTopology = executionGraph.getSchedulingTopology();

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(
                        testingSchedulerOperation, schedulingTopology);

        final Set<ConsumedPartitionGroup> crossRegionConsumedPartitionGroups =
                schedulingStrategy.getCrossRegionConsumedPartitionGroups();

        assertThat(crossRegionConsumedPartitionGroups).hasSize(1);

        final ConsumedPartitionGroup expected =
                executionGraph
                        .getJobVertex(v3.getID())
                        .getTaskVertices()[1]
                        .getAllConsumedPartitionGroups()
                        .get(0);

        assertThat(crossRegionConsumedPartitionGroups).contains(expected);
    }

    @Test
    void testNoCrossRegionConsumedPartitionGroupsWithAllToAllBlockingEdge() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producer =
                topology.addExecutionVertices().withParallelism(4).finish();
        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(4).finish();

        topology.connectAllToAll(producer, consumer)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(testingSchedulerOperation, topology);

        final Set<ConsumedPartitionGroup> crossRegionConsumedPartitionGroups =
                schedulingStrategy.getCrossRegionConsumedPartitionGroups();

        assertThat(crossRegionConsumedPartitionGroups).isEmpty();
    }

    @Test
    void testSchedulingTopologyWithBlockingCrossRegionConsumedPartitionGroups() throws Exception {
        final JobVertex v1 = createJobVertex("v1", 4);
        final JobVertex v2 = createJobVertex("v2", 3);
        final JobVertex v3 = createJobVertex("v3", 2);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        v3.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2, v3));
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder().addJobVertices(ordered).build();
        final ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final SchedulingTopology schedulingTopology = executionGraph.getSchedulingTopology();

        // Test whether the topology is built correctly
        final List<SchedulingPipelinedRegion> regions = new ArrayList<>();
        schedulingTopology.getAllPipelinedRegions().forEach(regions::add);
        assertThat(regions).hasSize(2);

        final ExecutionVertex v31 = executionGraph.getJobVertex(v3.getID()).getTaskVertices()[0];

        final Set<ExecutionVertexID> region1 = new HashSet<>();
        schedulingTopology
                .getPipelinedRegionOfVertex(v31.getID())
                .getVertices()
                .forEach(vertex -> region1.add(vertex.getId()));
        assertThat(region1).hasSize(5);

        final ExecutionVertex v32 = executionGraph.getJobVertex(v3.getID()).getTaskVertices()[1];

        final Set<ExecutionVertexID> region2 = new HashSet<>();
        schedulingTopology
                .getPipelinedRegionOfVertex(v32.getID())
                .getVertices()
                .forEach(vertex -> region2.add(vertex.getId()));
        assertThat(region2).hasSize(4);

        // Test whether region 1 is scheduled correctly
        PipelinedRegionSchedulingStrategy schedulingStrategy = startScheduling(schedulingTopology);

        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(1);
        final List<ExecutionVertexID> scheduledVertices1 =
                testingSchedulerOperation.getScheduledVertices().get(0);
        assertThat(scheduledVertices1).hasSize(5);

        for (ExecutionVertexID vertexId : scheduledVertices1) {
            assertThat(region1).contains(vertexId);
        }

        // Test whether the region 2 is scheduled correctly when region 1 is finished
        final ExecutionVertex v22 = executionGraph.getJobVertex(v2.getID()).getTaskVertices()[1];
        v22.finishPartitionsIfNeeded();

        schedulingStrategy.onExecutionStateChange(v22.getID(), ExecutionState.FINISHED);
        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(2);
        final List<ExecutionVertexID> scheduledVertices2 =
                testingSchedulerOperation.getScheduledVertices().get(1);
        assertThat(scheduledVertices2).hasSize(4);

        for (ExecutionVertexID vertexId : scheduledVertices2) {
            assertThat(region2).contains(vertexId);
        }
    }

    @Test
    void testSchedulingTopologyWithHybridCrossRegionConsumedPartitionGroups() throws Exception {
        final JobVertex v1 = createJobVertex("v1", 4);
        final JobVertex v2 = createJobVertex("v2", 3);
        final JobVertex v3 = createJobVertex("v3", 2);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.HYBRID_FULL);
        v3.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2, v3));
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder().addJobVertices(ordered).build();
        final ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final SchedulingTopology schedulingTopology = executionGraph.getSchedulingTopology();

        // Test whether the topology is built correctly
        final List<SchedulingPipelinedRegion> regions = new ArrayList<>();
        schedulingTopology.getAllPipelinedRegions().forEach(regions::add);
        assertThat(regions).hasSize(2);

        final ExecutionVertex v31 = executionGraph.getJobVertex(v3.getID()).getTaskVertices()[0];

        final Set<ExecutionVertexID> region1 = new HashSet<>();
        schedulingTopology
                .getPipelinedRegionOfVertex(v31.getID())
                .getVertices()
                .forEach(vertex -> region1.add(vertex.getId()));
        assertThat(region1).hasSize(5);

        final ExecutionVertex v32 = executionGraph.getJobVertex(v3.getID()).getTaskVertices()[1];

        final Set<ExecutionVertexID> region2 = new HashSet<>();
        schedulingTopology
                .getPipelinedRegionOfVertex(v32.getID())
                .getVertices()
                .forEach(vertex -> region2.add(vertex.getId()));
        assertThat(region2).hasSize(4);

        startScheduling(schedulingTopology);

        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(2);

        // Test whether region 1 is scheduled correctly
        final List<ExecutionVertexID> scheduledVertices1 =
                testingSchedulerOperation.getScheduledVertices().get(0);
        assertThat(scheduledVertices1).hasSize(5);

        for (ExecutionVertexID vertexId : scheduledVertices1) {
            assertThat(region1).contains(vertexId);
        }

        // Test whether region 2 is scheduled correctly
        final List<ExecutionVertexID> scheduledVertices2 =
                testingSchedulerOperation.getScheduledVertices().get(1);
        assertThat(scheduledVertices2).hasSize(4);

        for (ExecutionVertexID vertexId : scheduledVertices2) {
            assertThat(region2).contains(vertexId);
        }
    }

    @Test
    void testScheduleBlockingDownstreamTaskIndividually() throws Exception {
        final JobVertex v1 = createJobVertex("v1", 2);
        final JobVertex v2 = createJobVertex("v2", 2);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder().addJobVertices(ordered).build();
        final ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final SchedulingTopology schedulingTopology = executionGraph.getSchedulingTopology();

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                startScheduling(schedulingTopology);

        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(2);

        final ExecutionVertex v11 = executionGraph.getJobVertex(v1.getID()).getTaskVertices()[0];
        v11.finishPartitionsIfNeeded();

        schedulingStrategy.onExecutionStateChange(v11.getID(), ExecutionState.FINISHED);
        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(3);
    }

    @Test
    void testFinishHybridPartitionWillNotRescheduleDownstream() throws Exception {
        final JobVertex v1 = createJobVertex("v1", 1);
        final JobVertex v2 = createJobVertex("v2", 1);
        final JobVertex v3 = createJobVertex("v3", 1);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.HYBRID_FULL);
        v3.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.HYBRID_SELECTIVE);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2, v3));
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder().addJobVertices(ordered).build();
        final ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final SchedulingTopology schedulingTopology = executionGraph.getSchedulingTopology();

        PipelinedRegionSchedulingStrategy schedulingStrategy = startScheduling(schedulingTopology);

        // all regions will be scheduled
        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(3);

        final ExecutionVertex v11 = executionGraph.getJobVertex(v1.getID()).getTaskVertices()[0];
        schedulingStrategy.onExecutionStateChange(v11.getID(), ExecutionState.FINISHED);

        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(3);
    }

    /**
     * Source and it's downstream with hybrid edge will be scheduled. When blocking result partition
     * finished, it's downstream will be scheduled.
     *
     * <pre>
     * V1 ----> V2 ----> V3 ----> V4
     *     |        |         |
     *  Hybrid   Blocking   Hybrid
     * </pre>
     */
    @Test
    void testScheduleTopologyWithHybridAndBlockingEdge() throws Exception {
        final JobVertex v1 = createJobVertex("v1", 1);
        final JobVertex v2 = createJobVertex("v2", 1);
        final JobVertex v3 = createJobVertex("v3", 1);
        final JobVertex v4 = createJobVertex("v4", 1);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.HYBRID_FULL);
        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        v4.connectNewDataSetAsInput(
                v3, DistributionPattern.POINTWISE, ResultPartitionType.HYBRID_SELECTIVE);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2, v3, v4));
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder().addJobVertices(ordered).build();
        final ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final SchedulingTopology schedulingTopology = executionGraph.getSchedulingTopology();

        PipelinedRegionSchedulingStrategy schedulingStrategy = startScheduling(schedulingTopology);

        // v1 & v2 will be scheduled as v1 is a source and v1 -> v2 is a hybrid downstream.
        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(2);
        final ExecutionVertex v11 = executionGraph.getJobVertex(v1.getID()).getTaskVertices()[0];
        final ExecutionVertex v21 = executionGraph.getJobVertex(v2.getID()).getTaskVertices()[0];
        assertThat(testingSchedulerOperation.getScheduledVertices().get(0))
                .containsExactly(v11.getID());
        assertThat(testingSchedulerOperation.getScheduledVertices().get(1))
                .containsExactly(v21.getID());
        // finish v2 to trigger new round of scheduling.
        v21.finishPartitionsIfNeeded();
        schedulingStrategy.onExecutionStateChange(v21.getID(), ExecutionState.FINISHED);
        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(4);
        final ExecutionVertex v31 = executionGraph.getJobVertex(v3.getID()).getTaskVertices()[0];
        final ExecutionVertex v41 = executionGraph.getJobVertex(v4.getID()).getTaskVertices()[0];
        assertThat(testingSchedulerOperation.getScheduledVertices().get(2))
                .containsExactly(v31.getID());
        assertThat(testingSchedulerOperation.getScheduledVertices().get(3))
                .containsExactly(v41.getID());
    }

    /** Inner non-pipelined edge will not affect it's region be scheduled. */
    @Test
    void testSchedulingRegionWithInnerNonPipelinedEdge() throws Exception {
        final JobVertex v1 = createJobVertex("v1", 1);
        final JobVertex v2 = createJobVertex("v2", 1);
        final JobVertex v3 = createJobVertex("v3", 1);
        final JobVertex v4 = createJobVertex("v4", 1);
        final JobVertex v5 = createJobVertex("v5", 1);

        v2.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        v3.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        v4.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        v5.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        v3.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.HYBRID_FULL);
        v4.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.HYBRID_SELECTIVE);
        v4.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2, v3, v4, v5));
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder().addJobVertices(ordered).build();
        final ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final SchedulingTopology schedulingTopology = executionGraph.getSchedulingTopology();

        startScheduling(schedulingTopology);

        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(1);
        List<ExecutionVertexID> executionVertexIds =
                testingSchedulerOperation.getScheduledVertices().get(0);
        assertThat(executionVertexIds).hasSize(5);
    }

    /**
     * If a region have blocking and non-blocking input edge at the same time, it will be scheduled
     * after it's all blocking edge finished, non-blocking edge don't block scheduling.
     */
    @Test
    void testDownstreamRegionWillBeBlockedByBlockingEdge() throws Exception {
        final JobVertex v1 = createJobVertex("v1", 1);
        final JobVertex v2 = createJobVertex("v2", 1);
        final JobVertex v3 = createJobVertex("v3", 1);
        final JobVertex v4 = createJobVertex("v4", 1);

        v4.connectNewDataSetAsInput(
                v1, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        v4.connectNewDataSetAsInput(
                v2, DistributionPattern.POINTWISE, ResultPartitionType.HYBRID_FULL);
        v4.connectNewDataSetAsInput(
                v3, DistributionPattern.POINTWISE, ResultPartitionType.HYBRID_SELECTIVE);

        final List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2, v3, v4));
        final JobGraph jobGraph =
                JobGraphBuilder.newBatchJobGraphBuilder().addJobVertices(ordered).build();
        final ExecutionGraph executionGraph =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setJobGraph(jobGraph)
                        .build(EXECUTOR_RESOURCE.getExecutor());

        final SchedulingTopology schedulingTopology = executionGraph.getSchedulingTopology();

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                startScheduling(schedulingTopology);

        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(3);

        final ExecutionVertex v11 = executionGraph.getJobVertex(v1.getID()).getTaskVertices()[0];
        v11.finishPartitionsIfNeeded();
        schedulingStrategy.onExecutionStateChange(v11.getID(), ExecutionState.FINISHED);
        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(4);
    }

    private static JobVertex createJobVertex(String vertexName, int parallelism) {
        JobVertex jobVertex = new JobVertex(vertexName);
        jobVertex.setParallelism(parallelism);
        jobVertex.setInvokableClass(AbstractInvokable.class);
        return jobVertex;
    }

    private PipelinedRegionSchedulingStrategy startScheduling(
            SchedulingTopology schedulingTopology) {
        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(
                        testingSchedulerOperation, schedulingTopology);
        schedulingStrategy.startScheduling();
        return schedulingStrategy;
    }
}
