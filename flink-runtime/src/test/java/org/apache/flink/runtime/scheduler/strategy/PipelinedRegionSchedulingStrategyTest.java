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
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Unit tests for {@link PipelinedRegionSchedulingStrategy}. */
public class PipelinedRegionSchedulingStrategyTest extends TestLogger {

    private TestingSchedulerOperations testingSchedulerOperation;

    private static final int PARALLELISM = 2;

    private TestingSchedulingTopology testingSchedulingTopology;

    private List<TestingSchedulingExecutionVertex> source;

    private List<TestingSchedulingExecutionVertex> map;

    private List<TestingSchedulingExecutionVertex> sink;

    @Before
    public void setUp() {
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
        map =
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
                .connectPointwise(source, map)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.PIPELINED_BOUNDED)
                .finish();
        testingSchedulingTopology
                .connectAllToAll(map, sink)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();
    }

    @Test
    public void testStartScheduling() {
        startScheduling(testingSchedulingTopology);

        final List<List<TestingSchedulingExecutionVertex>> expectedScheduledVertices =
                new ArrayList<>();
        expectedScheduledVertices.add(Arrays.asList(source.get(0), map.get(0)));
        expectedScheduledVertices.add(Arrays.asList(source.get(1), map.get(1)));
        assertLatestScheduledVerticesAreEqualTo(expectedScheduledVertices);
    }

    @Test
    public void testRestartTasks() {
        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                startScheduling(testingSchedulingTopology);

        final Set<ExecutionVertexID> verticesToRestart =
                Stream.of(source, map, sink)
                        .flatMap(List::stream)
                        .map(TestingSchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet());

        schedulingStrategy.restartTasks(verticesToRestart);

        final List<List<TestingSchedulingExecutionVertex>> expectedScheduledVertices =
                new ArrayList<>();
        expectedScheduledVertices.add(Arrays.asList(source.get(0), map.get(0)));
        expectedScheduledVertices.add(Arrays.asList(source.get(1), map.get(1)));
        assertLatestScheduledVerticesAreEqualTo(expectedScheduledVertices);
    }

    @Test
    public void testNotifyingBlockingResultPartitionProducerFinished() {
        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                startScheduling(testingSchedulingTopology);

        final TestingSchedulingExecutionVertex map1 = map.get(0);
        map1.getProducedResults().iterator().next().setState(ResultPartitionState.CONSUMABLE);
        schedulingStrategy.onExecutionStateChange(map1.getId(), ExecutionState.FINISHED);

        // sinks' inputs are not all consumable yet so they are not scheduled
        assertThat(testingSchedulerOperation.getScheduledVertices(), hasSize(2));

        final TestingSchedulingExecutionVertex map2 = map.get(1);
        map2.getProducedResults().iterator().next().setState(ResultPartitionState.CONSUMABLE);
        schedulingStrategy.onExecutionStateChange(map2.getId(), ExecutionState.FINISHED);

        assertThat(testingSchedulerOperation.getScheduledVertices(), hasSize(4));

        final List<List<TestingSchedulingExecutionVertex>> expectedScheduledVertices =
                new ArrayList<>();
        expectedScheduledVertices.add(Arrays.asList(sink.get(0)));
        expectedScheduledVertices.add(Arrays.asList(sink.get(1)));
        assertLatestScheduledVerticesAreEqualTo(expectedScheduledVertices);
    }

    @Test
    public void testFinishedBlockingResultPartitionProducerDoNotScheduleNonCreatedRegions() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producer =
                topology.addExecutionVertices().withParallelism(2).finish();
        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(2).finish();

        topology.connectPointwise(producer, consumer)
                .withResultPartitionState(ResultPartitionState.CONSUMABLE)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();

        final PipelinedRegionSchedulingStrategy schedulingStrategy = startScheduling(topology);

        consumer.get(0).setState(ExecutionState.SCHEDULED);
        schedulingStrategy.onExecutionStateChange(producer.get(0).getId(), ExecutionState.FINISHED);

        // non-CREATED regions should not be re-scheduled
        assertThat(testingSchedulerOperation.getScheduledVertices(), hasSize(3));

        final List<List<TestingSchedulingExecutionVertex>> expectedScheduledVertices =
                new ArrayList<>();
        expectedScheduledVertices.add(Collections.singletonList(producer.get(0)));
        expectedScheduledVertices.add(Collections.singletonList(producer.get(1)));
        expectedScheduledVertices.add(Collections.singletonList(consumer.get(1)));
        assertLatestScheduledVerticesAreEqualTo(expectedScheduledVertices);
    }

    @Test
    public void testSchedulingTopologyWithPersistentBlockingEdges() {
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
        assertLatestScheduledVerticesAreEqualTo(expectedScheduledVertices);
    }

    @Test
    public void testComputingCrossRegionConsumedPartitionGroupsCorrectly() throws Exception {
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
                TestingDefaultExecutionGraphBuilder.newBuilder().setJobGraph(jobGraph).build();

        final SchedulingTopology schedulingTopology = executionGraph.getSchedulingTopology();

        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(
                        testingSchedulerOperation, schedulingTopology);

        final Set<ConsumedPartitionGroup> crossRegionConsumedPartitionGroups =
                schedulingStrategy.getCrossRegionConsumedPartitionGroups();

        assertEquals(1, crossRegionConsumedPartitionGroups.size());

        final ConsumedPartitionGroup expected =
                executionGraph
                        .getJobVertex(v3.getID())
                        .getTaskVertices()[1]
                        .getAllConsumedPartitionGroups()
                        .get(0);

        assertTrue(crossRegionConsumedPartitionGroups.contains(expected));
    }

    @Test
    public void testNoCrossRegionConsumedPartitionGroupsWithAllToAllBlockingEdge() {
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

        assertEquals(0, crossRegionConsumedPartitionGroups.size());
    }

    @Test
    public void testSchedulingTopologyWithCrossRegionConsumedPartitionGroups() throws Exception {
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
                TestingDefaultExecutionGraphBuilder.newBuilder().setJobGraph(jobGraph).build();

        final SchedulingTopology schedulingTopology = executionGraph.getSchedulingTopology();

        // Test whether the topology is built correctly
        final List<SchedulingPipelinedRegion> regions = new ArrayList<>();
        schedulingTopology.getAllPipelinedRegions().forEach(regions::add);
        assertEquals(2, regions.size());

        final ExecutionVertex v31 = executionGraph.getJobVertex(v3.getID()).getTaskVertices()[0];

        final Set<ExecutionVertexID> region1 = new HashSet<>();
        schedulingTopology
                .getPipelinedRegionOfVertex(v31.getID())
                .getVertices()
                .forEach(vertex -> region1.add(vertex.getId()));
        assertEquals(5, region1.size());

        final ExecutionVertex v32 = executionGraph.getJobVertex(v3.getID()).getTaskVertices()[1];

        final Set<ExecutionVertexID> region2 = new HashSet<>();
        schedulingTopology
                .getPipelinedRegionOfVertex(v32.getID())
                .getVertices()
                .forEach(vertex -> region2.add(vertex.getId()));
        assertEquals(4, region2.size());

        // Test whether the correct region is scheduled correctly
        startScheduling(schedulingTopology);

        assertEquals(1, testingSchedulerOperation.getScheduledVertices().size());
        final List<ExecutionVertexDeploymentOption> deploymentOptions =
                testingSchedulerOperation.getScheduledVertices().get(0);
        assertEquals(5, deploymentOptions.size());

        for (ExecutionVertexDeploymentOption deploymentOption : deploymentOptions) {
            assertTrue(region1.contains(deploymentOption.getExecutionVertexId()));
        }
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

    private void assertLatestScheduledVerticesAreEqualTo(
            final List<List<TestingSchedulingExecutionVertex>> expected) {
        final List<List<ExecutionVertexDeploymentOption>> deploymentOptions =
                testingSchedulerOperation.getScheduledVertices();
        final int expectedScheduledBulks = expected.size();
        assertThat(expectedScheduledBulks, lessThanOrEqualTo(deploymentOptions.size()));
        for (int i = 0; i < expectedScheduledBulks; i++) {
            assertEquals(
                    idsFromVertices(expected.get(expectedScheduledBulks - i - 1)),
                    idsFromDeploymentOptions(
                            deploymentOptions.get(deploymentOptions.size() - i - 1)));
        }
    }

    private static List<ExecutionVertexID> idsFromVertices(
            final List<TestingSchedulingExecutionVertex> vertices) {
        return vertices.stream()
                .map(TestingSchedulingExecutionVertex::getId)
                .collect(Collectors.toList());
    }

    private static List<ExecutionVertexID> idsFromDeploymentOptions(
            final List<ExecutionVertexDeploymentOption> deploymentOptions) {

        return deploymentOptions.stream()
                .map(ExecutionVertexDeploymentOption::getExecutionVertexId)
                .collect(Collectors.toList());
    }
}
