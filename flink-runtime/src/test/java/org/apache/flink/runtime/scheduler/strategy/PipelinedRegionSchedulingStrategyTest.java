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
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.scheduler.ExecutionVertexDeploymentOption;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

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

    private PipelinedRegionSchedulingStrategy startScheduling(
            TestingSchedulingTopology testingSchedulingTopology) {
        final PipelinedRegionSchedulingStrategy schedulingStrategy =
                new PipelinedRegionSchedulingStrategy(
                        testingSchedulerOperation, testingSchedulingTopology);
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
