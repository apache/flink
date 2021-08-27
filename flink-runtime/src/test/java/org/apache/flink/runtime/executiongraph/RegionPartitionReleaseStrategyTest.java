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

import org.apache.flink.runtime.executiongraph.failover.flip1.partitionrelease.RegionPartitionReleaseStrategy;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingExecutionVertex;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingResultPartition;
import org.apache.flink.runtime.scheduler.strategy.TestingSchedulingTopology;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

/** Tests for {@link RegionPartitionReleaseStrategy}. */
public class RegionPartitionReleaseStrategyTest extends TestLogger {

    private TestingSchedulingTopology testingSchedulingTopology;

    @Before
    public void setUp() throws Exception {
        testingSchedulingTopology = new TestingSchedulingTopology();
    }

    @Test
    public void releasePartitionsIfDownstreamRegionIsFinished() {
        final List<TestingSchedulingExecutionVertex> producers =
                testingSchedulingTopology.addExecutionVertices().finish();
        final List<TestingSchedulingExecutionVertex> consumers =
                testingSchedulingTopology.addExecutionVertices().finish();
        final List<TestingSchedulingResultPartition> resultPartitions =
                testingSchedulingTopology.connectPointwise(producers, consumers).finish();

        final ExecutionVertexID onlyConsumerVertexId = consumers.get(0).getId();
        final IntermediateResultPartitionID onlyResultPartitionId = resultPartitions.get(0).getId();

        final RegionPartitionReleaseStrategy regionPartitionReleaseStrategy =
                new RegionPartitionReleaseStrategy(testingSchedulingTopology);

        final List<IntermediateResultPartitionID> partitionsToRelease =
                regionPartitionReleaseStrategy.vertexFinished(onlyConsumerVertexId);
        assertThat(partitionsToRelease, contains(onlyResultPartitionId));
    }

    @Test
    public void releasePartitionsIfDownstreamRegionWithMultipleOperatorsIsFinished() {
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

        final RegionPartitionReleaseStrategy regionPartitionReleaseStrategy =
                new RegionPartitionReleaseStrategy(testingSchedulingTopology);

        regionPartitionReleaseStrategy.vertexFinished(onlyIntermediateVertexId);
        final List<IntermediateResultPartitionID> partitionsToRelease =
                regionPartitionReleaseStrategy.vertexFinished(onlySinkVertexId);
        assertThat(partitionsToRelease, contains(onlySourceResultPartitionId));
    }

    @Test
    public void notReleasePartitionsIfDownstreamRegionIsNotFinished() {
        final List<TestingSchedulingExecutionVertex> producers =
                testingSchedulingTopology.addExecutionVertices().finish();
        final List<TestingSchedulingExecutionVertex> consumers =
                testingSchedulingTopology.addExecutionVertices().withParallelism(2).finish();
        testingSchedulingTopology.connectAllToAll(producers, consumers).finish();

        final ExecutionVertexID consumerVertex1 = consumers.get(0).getId();

        final RegionPartitionReleaseStrategy regionPartitionReleaseStrategy =
                new RegionPartitionReleaseStrategy(testingSchedulingTopology);

        final List<IntermediateResultPartitionID> partitionsToRelease =
                regionPartitionReleaseStrategy.vertexFinished(consumerVertex1);
        assertThat(partitionsToRelease, is(empty()));
    }

    @Test
    public void toggleVertexFinishedUnfinished() {
        final List<TestingSchedulingExecutionVertex> producers =
                testingSchedulingTopology.addExecutionVertices().finish();
        final List<TestingSchedulingExecutionVertex> consumers =
                testingSchedulingTopology.addExecutionVertices().withParallelism(2).finish();
        testingSchedulingTopology.connectAllToAll(producers, consumers).finish();

        final ExecutionVertexID consumerVertex1 = consumers.get(0).getId();
        final ExecutionVertexID consumerVertex2 = consumers.get(1).getId();

        final RegionPartitionReleaseStrategy regionPartitionReleaseStrategy =
                new RegionPartitionReleaseStrategy(testingSchedulingTopology);

        regionPartitionReleaseStrategy.vertexFinished(consumerVertex1);
        regionPartitionReleaseStrategy.vertexFinished(consumerVertex2);
        regionPartitionReleaseStrategy.vertexUnfinished(consumerVertex2);

        final List<IntermediateResultPartitionID> partitionsToRelease =
                regionPartitionReleaseStrategy.vertexFinished(consumerVertex1);
        assertThat(partitionsToRelease, is(empty()));
    }
}
