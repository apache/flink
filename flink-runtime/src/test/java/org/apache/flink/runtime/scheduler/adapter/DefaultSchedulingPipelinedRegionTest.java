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

package org.apache.flink.runtime.scheduler.adapter;

import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.DefaultExecutionGraph;
import org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.scheduler.strategy.ConsumedPartitionGroup;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava30.com.google.common.collect.Iterables;

import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.fail;

/** Unit tests for {@link DefaultSchedulingPipelinedRegion}. */
public class DefaultSchedulingPipelinedRegionTest extends TestLogger {

    @Test
    public void gettingUnknownVertexThrowsException() {
        final Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionById =
                Collections.emptyMap();
        final DefaultSchedulingPipelinedRegion pipelinedRegion =
                new DefaultSchedulingPipelinedRegion(
                        Collections.emptySet(), resultPartitionById::get);
        final ExecutionVertexID unknownVertexId = new ExecutionVertexID(new JobVertexID(), 0);
        try {
            pipelinedRegion.getVertex(unknownVertexId);
            fail("Expected exception not thrown");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString(unknownVertexId + " not found"));
        }
    }

    @Test
    public void returnsVertices() {
        final DefaultExecutionVertex vertex =
                new DefaultExecutionVertex(
                        new ExecutionVertexID(new JobVertexID(), 0),
                        Collections.emptyList(),
                        () -> ExecutionState.CREATED);

        final Set<DefaultExecutionVertex> vertices = Collections.singleton(vertex);
        final Map<IntermediateResultPartitionID, DefaultResultPartition> resultPartitionById =
                Collections.emptyMap();
        final DefaultSchedulingPipelinedRegion pipelinedRegion =
                new DefaultSchedulingPipelinedRegion(vertices, resultPartitionById::get);
        final Iterator<DefaultExecutionVertex> vertexIterator =
                pipelinedRegion.getVertices().iterator();

        assertThat(vertexIterator.hasNext(), is(true));
        assertThat(vertexIterator.next(), is(sameInstance(vertex)));
        assertThat(vertexIterator.hasNext(), is(false));
    }

    /**
     * Tests if the consumed inputs of the pipelined regions are computed correctly using the Job
     * graph below.
     *
     * <pre>
     *          c
     *        /  X
     * a -+- b   e
     *       \  /
     *        d
     * </pre>
     *
     * <p>Pipelined regions: {a}, {b, c, d, e}
     */
    @Test
    public void returnsIncidentBlockingPartitions() throws Exception {
        final JobVertex a = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobVertex b = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobVertex c = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobVertex d = ExecutionGraphTestUtils.createNoOpVertex(1);
        final JobVertex e = ExecutionGraphTestUtils.createNoOpVertex(1);

        b.connectNewDataSetAsInput(a, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        c.connectNewDataSetAsInput(b, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        d.connectNewDataSetAsInput(b, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);
        e.connectNewDataSetAsInput(c, DistributionPattern.POINTWISE, ResultPartitionType.BLOCKING);
        e.connectNewDataSetAsInput(d, DistributionPattern.POINTWISE, ResultPartitionType.PIPELINED);

        final DefaultExecutionGraph simpleTestGraph =
                ExecutionGraphTestUtils.createSimpleTestGraph(a, b, c, d, e);
        final DefaultExecutionTopology topology =
                DefaultExecutionTopology.fromExecutionGraph(simpleTestGraph);

        final DefaultSchedulingPipelinedRegion firstPipelinedRegion =
                topology.getPipelinedRegionOfVertex(new ExecutionVertexID(a.getID(), 0));
        final DefaultSchedulingPipelinedRegion secondPipelinedRegion =
                topology.getPipelinedRegionOfVertex(new ExecutionVertexID(e.getID(), 0));

        final DefaultExecutionVertex vertexB0 =
                topology.getVertex(new ExecutionVertexID(b.getID(), 0));
        final IntermediateResultPartitionID b0ConsumedResultPartition =
                Iterables.getOnlyElement(vertexB0.getConsumedResults()).getId();

        final Set<IntermediateResultPartitionID> secondPipelinedRegionConsumedResults =
                new HashSet<>();
        for (ConsumedPartitionGroup consumedPartitionGroup :
                secondPipelinedRegion.getAllBlockingConsumedPartitionGroups()) {
            for (IntermediateResultPartitionID partitionId : consumedPartitionGroup) {
                if (!secondPipelinedRegion.contains(
                        topology.getResultPartition(partitionId).getProducer().getId())) {
                    secondPipelinedRegionConsumedResults.add(partitionId);
                }
            }
        }

        assertThat(
                firstPipelinedRegion.getAllBlockingConsumedPartitionGroups().iterator().hasNext(),
                is(false));
        assertThat(secondPipelinedRegionConsumedResults, contains(b0ConsumedResultPartition));
    }
}
