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

package org.apache.flink.graph.generator;

import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

/** Tests for {@link StarGraph}. */
public class StarGraphTest extends GraphGeneratorTestBase {

    @Test
    public void testGraph() throws Exception {
        int vertexCount = 10;

        Graph<LongValue, NullValue, NullValue> graph = new StarGraph(env, vertexCount).generate();

        String vertices = "0; 1; 2; 3; 4; 5; 6; 7; 8; 9";
        String edges =
                "0,1; 1,0; 0,2; 2,0; 0,3; 3,0; 0,4; 4,0; 0,5; 5,0;"
                        + "0,6; 6,0; 0,7; 7,0; 0,8; 8,0; 0,9; 9,0";

        TestUtils.compareGraph(graph, vertices, edges);
    }

    @Test
    public void testGraphMetrics() throws Exception {
        int vertexCount = 100;

        Graph<LongValue, NullValue, NullValue> graph = new StarGraph(env, vertexCount).generate();

        assertEquals(vertexCount, graph.numberOfVertices());
        assertEquals(2 * (vertexCount - 1), graph.numberOfEdges());

        long minInDegree = graph.inDegrees().min(1).collect().get(0).f1.getValue();
        long minOutDegree = graph.outDegrees().min(1).collect().get(0).f1.getValue();
        long maxInDegree = graph.inDegrees().max(1).collect().get(0).f1.getValue();
        long maxOutDegree = graph.outDegrees().max(1).collect().get(0).f1.getValue();

        assertEquals(1, minInDegree);
        assertEquals(1, minOutDegree);
        assertEquals(vertexCount - 1, maxInDegree);
        assertEquals(vertexCount - 1, maxOutDegree);
    }

    @Test
    public void testParallelism() throws Exception {
        int parallelism = 2;

        Graph<LongValue, NullValue, NullValue> graph =
                new StarGraph(env, 100).setParallelism(parallelism).generate();

        graph.getVertices().output(new DiscardingOutputFormat<>());
        graph.getEdges().output(new DiscardingOutputFormat<>());

        TestUtils.verifyParallelism(env, parallelism);
    }
}
