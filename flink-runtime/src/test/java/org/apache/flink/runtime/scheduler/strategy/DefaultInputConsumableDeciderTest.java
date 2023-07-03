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

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link DefaultInputConsumableDecider}. */
class DefaultInputConsumableDeciderTest {
    @Test
    void testNotFinishedBlockingInput() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers =
                topology.addExecutionVertices().withParallelism(2).finish();

        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(2).finish();

        topology.connectAllToAll(producers, consumer)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();

        DefaultInputConsumableDecider inputConsumableDecider =
                createDefaultInputConsumableDecider(Collections.emptySet(), topology);

        consumer.forEach(
                vertex ->
                        vertex.getConsumedPartitionGroups()
                                .forEach(
                                        group ->
                                                assertThat(
                                                                inputConsumableDecider
                                                                        .isConsumableBasedOnFinishedProducers(
                                                                                group))
                                                        .isFalse()));

        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(0), Collections.emptySet(), new HashMap<>()))
                .isFalse();
        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(1), Collections.emptySet(), new HashMap<>()))
                .isFalse();
    }

    @Test
    void testAllFinishedBlockingInput() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers =
                topology.addExecutionVertices().withParallelism(2).finish();

        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(2).finish();

        topology.connectAllToAll(producers, consumer)
                .withResultPartitionState(ResultPartitionState.ALL_DATA_PRODUCED)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();

        DefaultInputConsumableDecider inputConsumableDecider =
                createDefaultInputConsumableDecider(Collections.emptySet(), topology);

        consumer.forEach(
                vertex ->
                        vertex.getConsumedPartitionGroups()
                                .forEach(
                                        group ->
                                                assertThat(
                                                                inputConsumableDecider
                                                                        .isConsumableBasedOnFinishedProducers(
                                                                                group))
                                                        .isTrue()));

        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(0), Collections.emptySet(), new HashMap<>()))
                .isTrue();
        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(1), Collections.emptySet(), new HashMap<>()))
                .isTrue();
    }

    @Test
    void testUpstreamNotScheduledHybridInput() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers =
                topology.addExecutionVertices().withParallelism(2).finish();

        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(2).finish();

        topology.connectAllToAll(producers, consumer)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.HYBRID_FULL)
                .finish();

        DefaultInputConsumableDecider inputConsumableDecider =
                createDefaultInputConsumableDecider(Collections.emptySet(), topology);

        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(0), Collections.emptySet(), new HashMap<>()))
                .isFalse();
        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(1), Collections.emptySet(), new HashMap<>()))
                .isFalse();
    }

    @Test
    void testUpstreamAllScheduledHybridInput() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers =
                topology.addExecutionVertices().withParallelism(2).finish();

        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(2).finish();

        topology.connectAllToAll(producers, consumer)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.HYBRID_FULL)
                .finish();

        HashSet<ExecutionVertexID> scheduledVertices = new HashSet<>();
        DefaultInputConsumableDecider inputConsumableDecider =
                createDefaultInputConsumableDecider(scheduledVertices, topology);
        scheduledVertices.add(producers.get(0).getId());
        HashSet<ExecutionVertexID> vertexToDeploy = new HashSet<>();
        vertexToDeploy.add(producers.get(1).getId());

        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(0), vertexToDeploy, new HashMap<>()))
                .isTrue();
        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(1), vertexToDeploy, new HashMap<>()))
                .isTrue();
    }

    @Test
    void testHybridAndBlockingInputButBlockingInputNotFinished() {
        final TestingSchedulingTopology topology = new TestingSchedulingTopology();

        final List<TestingSchedulingExecutionVertex> producers1 =
                topology.addExecutionVertices().withParallelism(1).finish();

        final List<TestingSchedulingExecutionVertex> producers2 =
                topology.addExecutionVertices().withParallelism(1).finish();

        final List<TestingSchedulingExecutionVertex> consumer =
                topology.addExecutionVertices().withParallelism(1).finish();

        topology.connectAllToAll(producers1, consumer)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.BLOCKING)
                .finish();

        topology.connectAllToAll(producers2, consumer)
                .withResultPartitionState(ResultPartitionState.CREATED)
                .withResultPartitionType(ResultPartitionType.HYBRID_FULL)
                .finish();

        DefaultInputConsumableDecider inputConsumableDecider =
                createDefaultInputConsumableDecider(
                        Collections.singleton(producers2.get(0).getId()), topology);

        assertThat(
                        inputConsumableDecider.isInputConsumable(
                                consumer.get(0), Collections.emptySet(), new HashMap<>()))
                .isFalse();
    }

    private DefaultInputConsumableDecider createDefaultInputConsumableDecider(
            Set<ExecutionVertexID> scheduledVertices, SchedulingTopology schedulingTopology) {
        return new DefaultInputConsumableDecider(
                scheduledVertices::contains, schedulingTopology::getResultPartition);
    }
}
