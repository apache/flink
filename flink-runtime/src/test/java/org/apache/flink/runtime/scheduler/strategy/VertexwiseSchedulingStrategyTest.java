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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.runtime.scheduler.strategy.StrategyTestUtil.assertLatestScheduledVerticesAreEqualTo;
import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link VertexwiseSchedulingStrategy}. */
class VertexwiseSchedulingStrategyTest {

    private TestingSchedulerOperations testingSchedulerOperation;

    private static final int PARALLELISM = 2;

    private TestingSchedulingTopology testingSchedulingTopology;

    private List<TestingSchedulingExecutionVertex> source;

    private List<TestingSchedulingExecutionVertex> map;

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
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();
        testingSchedulingTopology
                .connectAllToAll(map, sink)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();
    }

    @Test
    void testStartScheduling() {
        startScheduling(testingSchedulingTopology);

        final List<List<TestingSchedulingExecutionVertex>> expectedScheduledVertices =
                new ArrayList<>();
        expectedScheduledVertices.add(Collections.singletonList(source.get(0)));
        expectedScheduledVertices.add(Collections.singletonList(source.get(1)));

        assertLatestScheduledVerticesAreEqualTo(
                expectedScheduledVertices, testingSchedulerOperation);
    }

    @Test
    void testRestartTasks() {
        final VertexwiseSchedulingStrategy schedulingStrategy =
                startScheduling(testingSchedulingTopology);

        final Set<ExecutionVertexID> verticesToRestart =
                Stream.of(source, map, sink)
                        .flatMap(List::stream)
                        .map(TestingSchedulingExecutionVertex::getId)
                        .collect(Collectors.toSet());

        schedulingStrategy.restartTasks(verticesToRestart);

        final List<List<TestingSchedulingExecutionVertex>> expectedScheduledVertices =
                new ArrayList<>();
        expectedScheduledVertices.add(Collections.singletonList(source.get(0)));
        expectedScheduledVertices.add(Collections.singletonList(source.get(1)));
        assertLatestScheduledVerticesAreEqualTo(
                expectedScheduledVertices, testingSchedulerOperation);
    }

    @Test
    void testOnExecutionStateChangeToFinished() {
        // trigger source1, source2 scheduled.
        final VertexwiseSchedulingStrategy schedulingStrategy =
                startScheduling(testingSchedulingTopology);
        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(2);

        // trigger map1 scheduled
        final TestingSchedulingExecutionVertex source1 = source.get(0);
        source1.getProducedResults().iterator().next().setState(ResultPartitionState.CONSUMABLE);
        schedulingStrategy.onExecutionStateChange(source1.getId(), ExecutionState.FINISHED);
        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(3);

        // trigger map2 scheduled
        final TestingSchedulingExecutionVertex source2 = source.get(1);
        source2.getProducedResults().iterator().next().setState(ResultPartitionState.CONSUMABLE);
        schedulingStrategy.onExecutionStateChange(source2.getId(), ExecutionState.FINISHED);
        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(4);

        // sinks' inputs are not all consumable yet so they are not scheduled
        final TestingSchedulingExecutionVertex map1 = map.get(0);
        map1.getProducedResults().iterator().next().setState(ResultPartitionState.CONSUMABLE);
        schedulingStrategy.onExecutionStateChange(map1.getId(), ExecutionState.FINISHED);
        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(4);

        // trigger sink1, sink2 scheduled
        final TestingSchedulingExecutionVertex map2 = map.get(1);
        map2.getProducedResults().iterator().next().setState(ResultPartitionState.CONSUMABLE);
        schedulingStrategy.onExecutionStateChange(map2.getId(), ExecutionState.FINISHED);
        assertThat(testingSchedulerOperation.getScheduledVertices()).hasSize(6);

        final List<List<TestingSchedulingExecutionVertex>> expectedScheduledVertices =
                new ArrayList<>();
        expectedScheduledVertices.add(Collections.singletonList(source.get(0)));
        expectedScheduledVertices.add(Collections.singletonList(source.get(1)));
        expectedScheduledVertices.add(Collections.singletonList(map.get(0)));
        expectedScheduledVertices.add(Collections.singletonList(map.get(1)));
        expectedScheduledVertices.add(Collections.singletonList(sink.get(0)));
        expectedScheduledVertices.add(Collections.singletonList(sink.get(1)));
        assertLatestScheduledVerticesAreEqualTo(
                expectedScheduledVertices, testingSchedulerOperation);
    }

    @Test
    void testUpdateStrategyWithAllToAll() {
        testUpdateStrategyOnTopologyUpdate(true);
    }

    @Test
    void testUpdateStrategyWithPointWise() {
        testUpdateStrategyOnTopologyUpdate(false);
    }

    private void testUpdateStrategyOnTopologyUpdate(boolean allToAll) {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers =
                topology.addExecutionVertices().withParallelism(2).finish();

        final VertexwiseSchedulingStrategy schedulingStrategy = startScheduling(topology);

        final List<TestingSchedulingExecutionVertex> consumers =
                topology.addExecutionVertices().withParallelism(2).finish();

        // producer_0 finished
        schedulingStrategy.onExecutionStateChange(
                producers.get(0).getId(), ExecutionState.FINISHED);

        // add consumers to scheduling strategy.
        if (allToAll) {
            topology.connectAllToAll(producers, consumers)
                    .withResultPartitionState(ResultPartitionState.CONSUMABLE)
                    .withResultPartitionType(ResultPartitionType.BLOCKING)
                    .finish();
        } else {
            topology.connectPointwise(producers, consumers)
                    .withResultPartitionState(ResultPartitionState.CONSUMABLE)
                    .withResultPartitionType(ResultPartitionType.BLOCKING)
                    .finish();
        }

        // producer_1 finished, consumer_0 and consumer_1 be added.
        schedulingStrategy.notifySchedulingTopologyUpdated(
                topology,
                consumers.stream()
                        .map(TestingSchedulingExecutionVertex::getId)
                        .collect(Collectors.toList()));
        schedulingStrategy.onExecutionStateChange(
                producers.get(1).getId(), ExecutionState.FINISHED);

        // consumers are properly scheduled indicates that the consuming relationship and
        // correlation are successfully built
        assertLatestScheduledVerticesAreEqualTo(
                Arrays.asList(
                        Collections.singletonList(producers.get(0)),
                        Collections.singletonList(producers.get(1)),
                        Collections.singletonList(consumers.get(0)),
                        Collections.singletonList(consumers.get(1))),
                testingSchedulerOperation);
    }

    VertexwiseSchedulingStrategy startScheduling(SchedulingTopology schedulingTopology) {
        final VertexwiseSchedulingStrategy schedulingStrategy =
                new VertexwiseSchedulingStrategy(testingSchedulerOperation, schedulingTopology);
        schedulingStrategy.startScheduling();
        return schedulingStrategy;
    }
}
