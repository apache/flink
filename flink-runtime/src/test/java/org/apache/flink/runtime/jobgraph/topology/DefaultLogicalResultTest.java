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
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.executiongraph.ExecutionGraphTestUtils.createNoOpVertex;
import static org.apache.flink.runtime.io.network.partition.ResultPartitionType.PIPELINED;
import static org.apache.flink.runtime.jobgraph.DistributionPattern.ALL_TO_ALL;
import static org.apache.flink.runtime.jobgraph.topology.DefaultLogicalVertexTest.assertVertexInfoEquals;
import static org.apache.flink.runtime.jobgraph.topology.DefaultLogicalVertexTest.assertVerticesEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Unit tests for {@link DefaultLogicalResult}. */
public class DefaultLogicalResultTest extends TestLogger {

    private IntermediateDataSet result;

    private DefaultLogicalResult logicalResult;

    private Map<JobVertexID, JobVertex> vertexMap;

    private JobVertex producerVertex;

    private Set<JobVertex> consumerVertices;

    @Before
    public void setUp() throws Exception {
        buildVerticesAndResults();

        logicalResult =
                new DefaultLogicalResult(
                        result, vid -> new DefaultLogicalVertex(vertexMap.get(vid), rid -> null));
    }

    @Test
    public void testConstructor() {
        assertResultInfoEquals(result, logicalResult);
    }

    @Test
    public void testGetProducer() {
        assertVertexInfoEquals(producerVertex, logicalResult.getProducer());
    }

    @Test
    public void testGetConsumers() {
        assertVerticesEquals(consumerVertices, logicalResult.getConsumers());
    }

    private void buildVerticesAndResults() {
        vertexMap = new HashMap<>();
        consumerVertices = new HashSet<>();

        final int parallelism = 3;
        producerVertex = createNoOpVertex(parallelism);
        vertexMap.put(producerVertex.getID(), producerVertex);

        result = producerVertex.createAndAddResultDataSet(PIPELINED);

        for (int i = 0; i < 5; i++) {
            final JobVertex consumerVertex = createNoOpVertex(parallelism);
            consumerVertex.connectDataSetAsInput(result, ALL_TO_ALL);
            consumerVertices.add(consumerVertex);
            vertexMap.put(consumerVertex.getID(), consumerVertex);
        }
    }

    static void assertResultsEquals(
            final Iterable<IntermediateDataSet> results,
            final Iterable<DefaultLogicalResult> logicalResults) {

        final Map<IntermediateDataSetID, DefaultLogicalResult> logicalResultMap =
                IterableUtils.toStream(logicalResults)
                        .collect(
                                Collectors.toMap(DefaultLogicalResult::getId, Function.identity()));

        for (IntermediateDataSet result : results) {
            final DefaultLogicalResult logicalResult = logicalResultMap.remove(result.getId());

            assertNotNull(logicalResult);
            assertResultInfoEquals(result, logicalResult);
        }

        // this ensures the two collections exactly matches
        assertEquals(0, logicalResultMap.size());
    }

    static void assertResultInfoEquals(
            final IntermediateDataSet result, final DefaultLogicalResult logicalResult) {

        assertEquals(result.getId(), logicalResult.getId());
        assertEquals(result.getResultType(), logicalResult.getResultType());
    }
}
