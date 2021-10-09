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

package org.apache.flink.runtime.executiongraph;

import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.DistributionPattern;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.scheduler.SchedulerBase;
import org.apache.flink.runtime.scheduler.strategy.ConsumerVertexGroup;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.POINTWISE;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link EdgeManagerBuildUtil} to verify the max number of connecting edges between
 * vertices for pattern of both {@link DistributionPattern#POINTWISE} and {@link
 * DistributionPattern#ALL_TO_ALL}.
 */
public class EdgeManagerBuildUtilTest {

    @Test
    public void testGetMaxNumEdgesToTargetInPointwiseConnection() throws Exception {
        testGetMaxNumEdgesToTarget(17, 17, POINTWISE);
        testGetMaxNumEdgesToTarget(17, 23, POINTWISE);
        testGetMaxNumEdgesToTarget(17, 34, POINTWISE);
        testGetMaxNumEdgesToTarget(34, 17, POINTWISE);
        testGetMaxNumEdgesToTarget(23, 17, POINTWISE);
    }

    @Test
    public void testGetMaxNumEdgesToTargetInAllToAllConnection() throws Exception {
        testGetMaxNumEdgesToTarget(17, 17, ALL_TO_ALL);
        testGetMaxNumEdgesToTarget(17, 23, ALL_TO_ALL);
        testGetMaxNumEdgesToTarget(17, 34, ALL_TO_ALL);
        testGetMaxNumEdgesToTarget(34, 17, ALL_TO_ALL);
        testGetMaxNumEdgesToTarget(23, 17, ALL_TO_ALL);
    }

    private void testGetMaxNumEdgesToTarget(
            int upstream, int downstream, DistributionPattern pattern) throws Exception {

        Pair<ExecutionJobVertex, ExecutionJobVertex> pair =
                setupExecutionGraph(upstream, downstream, pattern);
        ExecutionJobVertex upstreamEJV = pair.getLeft();
        ExecutionJobVertex downstreamEJV = pair.getRight();

        int calculatedMaxForUpstream =
                EdgeManagerBuildUtil.computeMaxEdgesToTargetExecutionVertex(
                        upstream, downstream, pattern);
        int actualMaxForUpstream = -1;
        for (ExecutionVertex ev : upstreamEJV.getTaskVertices()) {
            assertEquals(1, ev.getProducedPartitions().size());

            IntermediateResultPartition partition =
                    ev.getProducedPartitions().values().iterator().next();
            assertEquals(1, partition.getConsumerVertexGroups().size());

            ConsumerVertexGroup consumerVertexGroup = partition.getConsumerVertexGroups().get(0);
            int actual = consumerVertexGroup.size();
            if (actual > actualMaxForUpstream) {
                actualMaxForUpstream = actual;
            }
        }
        assertEquals(actualMaxForUpstream, calculatedMaxForUpstream);

        int calculatedMaxForDownstream =
                EdgeManagerBuildUtil.computeMaxEdgesToTargetExecutionVertex(
                        downstream, upstream, pattern);
        int actualMaxForDownstream = -1;
        for (ExecutionVertex ev : downstreamEJV.getTaskVertices()) {
            assertEquals(1, ev.getNumberOfInputs());

            int actual = ev.getConsumedPartitionGroup(0).size();
            if (actual > actualMaxForDownstream) {
                actualMaxForDownstream = actual;
            }
        }
        assertEquals(actualMaxForDownstream, calculatedMaxForDownstream);
    }

    private Pair<ExecutionJobVertex, ExecutionJobVertex> setupExecutionGraph(
            int upstream, int downstream, DistributionPattern pattern) throws Exception {
        JobVertex v1 = new JobVertex("vertex1");
        JobVertex v2 = new JobVertex("vertex2");

        v1.setParallelism(upstream);
        v2.setParallelism(downstream);

        v1.setInvokableClass(AbstractInvokable.class);
        v2.setInvokableClass(AbstractInvokable.class);

        v2.connectNewDataSetAsInput(v1, pattern, ResultPartitionType.PIPELINED);

        List<JobVertex> ordered = new ArrayList<>(Arrays.asList(v1, v2));

        ExecutionGraph eg =
                TestingDefaultExecutionGraphBuilder.newBuilder()
                        .setVertexParallelismStore(
                                SchedulerBase.computeVertexParallelismStore(ordered))
                        .build();
        eg.attachJobGraph(ordered);
        return Pair.of(eg.getAllVertices().get(v1.getID()), eg.getAllVertices().get(v2.getID()));
    }
}
