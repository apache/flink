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

package org.apache.flink.graph.bipartite;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.test.util.TestBaseUtils;

import org.junit.Test;

import java.util.Arrays;

import static org.apache.flink.graph.generator.TestUtils.compareGraph;
import static org.junit.Assert.assertEquals;

/** Tests for {@link BipartiteGraph}. */
public class BipartiteGraphTest {

    @Test
    public void testGetTopVertices() throws Exception {
        BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph =
                createBipartiteGraph();

        assertEquals(
                Arrays.asList(
                        new Vertex<>(4, "top4"), new Vertex<>(5, "top5"), new Vertex<>(6, "top6")),
                bipartiteGraph.getTopVertices().collect());
    }

    @Test
    public void testGetBottomVertices() throws Exception {
        BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph =
                createBipartiteGraph();

        assertEquals(
                Arrays.asList(
                        new Vertex<>(1, "bottom1"),
                        new Vertex<>(2, "bottom2"),
                        new Vertex<>(3, "bottom3")),
                bipartiteGraph.getBottomVertices().collect());
    }

    @Test
    public void testSimpleTopProjection() throws Exception {
        BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph =
                createBipartiteGraph();
        Graph<Integer, String, Tuple2<String, String>> graph = bipartiteGraph.projectionTopSimple();

        compareGraph(graph, "4; 5; 6", "5,4; 4,5; 5,6; 6,5");

        String expected =
                "(5,4,(5-1,4-1))\n" + "(4,5,(4-1,5-1))\n" + "(6,5,(6-2,5-2))\n" + "(5,6,(5-2,6-2))";
        TestBaseUtils.compareResultAsText(graph.getEdges().collect(), expected);
    }

    @Test
    public void testSimpleBottomProjection() throws Exception {
        BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph =
                createBipartiteGraph();
        Graph<Integer, String, Tuple2<String, String>> graph =
                bipartiteGraph.projectionBottomSimple();

        compareGraph(graph, "1; 2; 3", "1,2; 2,1; 2,3; 3,2");

        String expected =
                "(3,2,(6-3,6-2))\n" + "(2,3,(6-2,6-3))\n" + "(2,1,(5-2,5-1))\n" + "(1,2,(5-1,5-2))";
        TestBaseUtils.compareResultAsText(graph.getEdges().collect(), expected);
    }

    @Test
    public void testFullTopProjection() throws Exception {
        BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph =
                createBipartiteGraph();
        Graph<Integer, String, Projection<Integer, String, String, String>> graph =
                bipartiteGraph.projectionTopFull();

        graph.getEdges().print();
        compareGraph(graph, "4; 5; 6", "5,4; 4,5; 5,6; 6,5");

        String expected =
                "(5,4,(1,bottom1,top5,top4,5-1,4-1))\n"
                        + "(4,5,(1,bottom1,top4,top5,4-1,5-1))\n"
                        + "(6,5,(2,bottom2,top6,top5,6-2,5-2))\n"
                        + "(5,6,(2,bottom2,top5,top6,5-2,6-2))";
        TestBaseUtils.compareResultAsText(graph.getEdges().collect(), expected);
    }

    @Test
    public void testFullBottomProjection() throws Exception {
        BipartiteGraph<Integer, Integer, String, String, String> bipartiteGraph =
                createBipartiteGraph();
        Graph<Integer, String, Projection<Integer, String, String, String>> graph =
                bipartiteGraph.projectionBottomFull();

        compareGraph(graph, "1; 2; 3", "1,2; 2,1; 2,3; 3,2");

        String expected =
                "(3,2,(6,top6,bottom3,bottom2,6-3,6-2))\n"
                        + "(2,3,(6,top6,bottom2,bottom3,6-2,6-3))\n"
                        + "(2,1,(5,top5,bottom2,bottom1,5-2,5-1))\n"
                        + "(1,2,(5,top5,bottom1,bottom2,5-1,5-2))";
        TestBaseUtils.compareResultAsText(graph.getEdges().collect(), expected);
    }

    private BipartiteGraph<Integer, Integer, String, String, String> createBipartiteGraph() {
        ExecutionEnvironment executionEnvironment =
                ExecutionEnvironment.createCollectionsEnvironment();

        DataSet<Vertex<Integer, String>> topVertices =
                executionEnvironment.fromCollection(
                        Arrays.asList(
                                new Vertex<>(4, "top4"),
                                new Vertex<>(5, "top5"),
                                new Vertex<>(6, "top6")));

        DataSet<Vertex<Integer, String>> bottomVertices =
                executionEnvironment.fromCollection(
                        Arrays.asList(
                                new Vertex<>(1, "bottom1"),
                                new Vertex<>(2, "bottom2"),
                                new Vertex<>(3, "bottom3")));

        DataSet<BipartiteEdge<Integer, Integer, String>> edges =
                executionEnvironment.fromCollection(
                        Arrays.asList(
                                new BipartiteEdge<>(4, 1, "4-1"),
                                new BipartiteEdge<>(5, 1, "5-1"),
                                new BipartiteEdge<>(5, 2, "5-2"),
                                new BipartiteEdge<>(6, 2, "6-2"),
                                new BipartiteEdge<>(6, 3, "6-3")));

        return BipartiteGraph.fromDataSet(topVertices, bottomVertices, edges, executionEnvironment);
    }
}
