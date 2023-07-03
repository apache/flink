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

package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobGraphTestUtils;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava31.com.google.common.collect.Iterables;

import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.BLOCKING;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.topology.DefaultLogicalResultTest.assertResultsEquals;
import static org.apache.flink.runtime.jobgraph.topology.DefaultLogicalVertexTest.assertVertexInfoEquals;
import static org.junit.Assert.assertEquals;

/** Unit tests for {@link DefaultLogicalTopology}. */
public class DefaultLogicalTopologyTest extends TestLogger {

    private JobGraph jobGraph;

    private DefaultLogicalTopology logicalTopology;

    @Before
    public void setUp() throws Exception {
        jobGraph = createJobGraph();
        logicalTopology = DefaultLogicalTopology.fromJobGraph(jobGraph);
    }

    @Test
    public void testGetVertices() {
        // vertices from getVertices() should be topologically sorted
        final Iterable<JobVertex> jobVertices =
                jobGraph.getVerticesSortedTopologicallyFromSources();
        final Iterable<DefaultLogicalVertex> logicalVertices = logicalTopology.getVertices();

        assertEquals(Iterables.size(jobVertices), Iterables.size(logicalVertices));

        final Iterator<JobVertex> jobVertexIterator = jobVertices.iterator();
        final Iterator<DefaultLogicalVertex> logicalVertexIterator = logicalVertices.iterator();
        while (jobVertexIterator.hasNext()) {
            assertVertexAndConnectedResultsEquals(
                    jobVertexIterator.next(), logicalVertexIterator.next());
        }
    }

    @Test
    public void testGetLogicalPipelinedRegions() {
        assertEquals(2, IterableUtils.toStream(logicalTopology.getAllPipelinedRegions()).count());
    }

    private JobGraph createJobGraph() {
        final JobVertex[] jobVertices = new JobVertex[3];
        final int parallelism = 3;
        jobVertices[0] = createNoOpVertex("v1", parallelism);
        jobVertices[1] = createNoOpVertex("v2", parallelism);
        jobVertices[2] = createNoOpVertex("v3", parallelism);
        jobVertices[1].connectNewDataSetAsInput(jobVertices[0], ALL_TO_ALL, PIPELINED);
        jobVertices[2].connectNewDataSetAsInput(jobVertices[1], ALL_TO_ALL, BLOCKING);

        return JobGraphTestUtils.streamingJobGraph(jobVertices);
    }

    private static void assertVertexAndConnectedResultsEquals(
            final JobVertex jobVertex, final DefaultLogicalVertex logicalVertex) {

        assertVertexInfoEquals(jobVertex, logicalVertex);

        final List<IntermediateDataSet> consumedResults =
                jobVertex.getInputs().stream().map(JobEdge::getSource).collect(Collectors.toList());
        assertResultsEquals(consumedResults, logicalVertex.getConsumedResults());

        final List<IntermediateDataSet> producedResults = jobVertex.getProducedDataSets();
        assertResultsEquals(producedResults, logicalVertex.getProducedResults());
    }
}
