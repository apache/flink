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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.RegionPartitionGroupReleaseStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.util.IterableUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RegionPartitionGroupReleaseStrategy}. */
class RegionPartitionGroupReleaseStrategyTest {

    private TestingSchedulingTopology testingSchedulingTopology;

    @BeforeEach
    void setUp() throws Exception {
        testingSchedulingTopology = new TestingSchedulingTopology();
    }

    @Test
    void releasePartitionsIfDownstreamRegionIsFinished() {
        final List<TestingSchedulingExecutionVertex> producers =
                testingSchedulingTopology.addExecutionVertices().finish();
        final List<TestingSchedulingExecutionVertex> consumers =
                testingSchedulingTopology.addExecutionVertices().finish();
        final List<TestingSchedulingResultPartition> resultPartitions =
                testingSchedulingTopology.connectPointwise(producers, consumers).finish();

        final ExecutionVertexID onlyConsumerVertexId = consumers.get(0).getId();
        final IntermediateResultPartitionID onlyResultPartitionId = resultPartitions.get(0).getId();

        final RegionPartitionGroupReleaseStrategy regionPartitionGroupReleaseStrategy =
                new RegionPartitionGroupReleaseStrategy(testingSchedulingTopology);

        final List<IntermediateResultPartitionID> partitionsToRelease =
                getReleasablePartitions(regionPartitionGroupReleaseStrategy, onlyConsumerVertexId);
        assertThat(partitionsToRelease).contains(onlyResultPartitionId);
    }

    @Test
    void releasePartitionsIfDownstreamRegionWithMultipleOperatorsIsFinished() {
        final List<TestingSchedulingExecutionVertex> sourceVertices =
                testingSchedulingTopology.addExecutionVertices().finish();
        final List<TestingSchedulingExecutionVertex> intermediateVertices =
                testingSchedulingTopology.addExecutionVertices().finish();
        final List<TestingSchedulingExecutionVertex> sinkVertices =
                testingSchedulingTopology.addExecutionVertices().finish();
        final List<TestingSchedulingResultPartition> sourceResultPartitions =
                testingSchedulingTopology
                        .connectAllToAll(sourceVertices, intermediateVertices)
                        .finish();
        testingSchedulingTopology
                .connectAllToAll(intermediateVertices, sinkVertices)
                .withResultPartitionType(ResultPartitionType.PIPELINED)
                .finish();

        final ExecutionVertexID onlyIntermediateVertexId = intermediateVertices.get(0).getId();
        final ExecutionVertexID onlySinkVertexId = sinkVertices.get(0).getId();
        final IntermediateResultPartitionID onlySourceResultPartitionId =
                sourceResultPartitions.get(0).getId();

        final RegionPartitionGroupReleaseStrategy regionPartitionGroupReleaseStrategy =
                new RegionPartitionGroupReleaseStrategy(testingSchedulingTopology);

        regionPartitionGroupReleaseStrategy.vertexFinished(onlyIntermediateVertexId);
        final List<IntermediateResultPartitionID> partitionsToRelease =
                getReleasablePartitions(regionPartitionGroupReleaseStrategy, onlySinkVertexId);
        assertThat(partitionsToRelease).contains(onlySourceResultPartitionId);
    }

    @Test
    void notReleasePartitionsIfDownstreamRegionIsNotFinished() {
        final List<TestingSchedulingExecutionVertex> producers =
                testingSchedulingTopology.addExecutionVertices().finish();
        final List<TestingSchedulingExecutionVertex> consumers =
                testingSchedulingTopology.addExecutionVertices().withParallelism(2).finish();
        testingSchedulingTopology.connectAllToAll(producers, consumers).finish();

        final ExecutionVertexID consumerVertex1 = consumers.get(0).getId();

        final RegionPartitionGroupReleaseStrategy regionPartitionGroupReleaseStrategy =
                new RegionPartitionGroupReleaseStrategy(testingSchedulingTopology);

        final List<IntermediateResultPartitionID> partitionsToRelease =
                getReleasablePartitions(regionPartitionGroupReleaseStrategy, consumerVertex1);
        assertThat(partitionsToRelease).isEmpty();
    }

    @Test
    void toggleVertexFinishedUnfinished() {
        final List<TestingSchedulingExecutionVertex> producers =
                testingSchedulingTopology.addExecutionVertices().finish();
        final List<TestingSchedulingExecutionVertex> consumers =
                testingSchedulingTopology.addExecutionVertices().withParallelism(2).finish();
        testingSchedulingTopology.connectAllToAll(producers, consumers).finish();

        final ExecutionVertexID consumerVertex1 = consumers.get(0).getId();
        final ExecutionVertexID consumerVertex2 = consumers.get(1).getId();

        final RegionPartitionGroupReleaseStrategy regionPartitionGroupReleaseStrategy =
                new RegionPartitionGroupReleaseStrategy(testingSchedulingTopology);

        regionPartitionGroupReleaseStrategy.vertexFinished(consumerVertex1);
        regionPartitionGroupReleaseStrategy.vertexFinished(consumerVertex2);
        regionPartitionGroupReleaseStrategy.vertexUnfinished(consumerVertex2);

        final List<IntermediateResultPartitionID> partitionsToRelease =
                getReleasablePartitions(regionPartitionGroupReleaseStrategy, consumerVertex1);
        assertThat(partitionsToRelease).isEmpty();
    }

    private static List<IntermediateResultPartitionID> getReleasablePartitions(
            final RegionPartitionGroupReleaseStrategy regionPartitionGroupReleaseStrategy,
            final ExecutionVertexID finishedVertex) {

        return regionPartitionGroupReleaseStrategy.vertexFinished(finishedVertex).stream()
                .flatMap(IterableUtils::toStream)
                .collect(Collectors.toList());
    }

    @Test
    void updateStrategyOnTopologyUpdate() {
        final TestingSchedulingExecutionVertex ev1 = testingSchedulingTopology.newExecutionVertex();

        final RegionPartitionGroupReleaseStrategy regionPartitionReleaseStrategy =
                new RegionPartitionGroupReleaseStrategy(testingSchedulingTopology);

        regionPartitionReleaseStrategy.vertexFinished(ev1.getId());

        final TestingSchedulingExecutionVertex ev2 = testingSchedulingTopology.newExecutionVertex();
        testingSchedulingTopology.connect(ev1, ev2, ResultPartitionType.BLOCKING);

        regionPartitionReleaseStrategy.notifySchedulingTopologyUpdated(
                testingSchedulingTopology, Collections.singletonList(ev2.getId()));

        // this check ensures that existing region views are not affected
        assertThat(regionPartitionReleaseStrategy.isRegionOfVertexFinished(ev1.getId())).isTrue();

        assertThat(regionPartitionReleaseStrategy.isRegionOfVertexFinished(ev2.getId())).isFalse();

        List<IntermediateResultPartitionID> releasablePartitions =
                getReleasablePartitions(regionPartitionReleaseStrategy, ev2.getId());
        assertThat(regionPartitionReleaseStrategy.isRegionOfVertexFinished(ev2.getId())).isTrue();
        assertThat(releasablePartitions)
                .contains(ev1.getProducedResults().iterator().next().getId());
    }
}
