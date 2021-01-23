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

package org.apache.flink.graph.test.operations;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.test.TestGraphUtils;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.apache.flink.types.NullValue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

/** Tests for {@link Graph} operations. */
@RunWith(Parameterized.class)
public class GraphOperationsITCase extends MultipleProgramsTestBase {

    public GraphOperationsITCase(TestExecutionMode mode) {
        super(mode);
    }

    private String expectedResult;

    @Test
    public void testUndirected() throws Exception {
        /*
         * Test getUndirected()
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Edge<Long, Long>> data = graph.getUndirected().getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,12\n"
                        + "2,1,12\n"
                        + "1,3,13\n"
                        + "3,1,13\n"
                        + "2,3,23\n"
                        + "3,2,23\n"
                        + "3,4,34\n"
                        + "4,3,34\n"
                        + "3,5,35\n"
                        + "5,3,35\n"
                        + "4,5,45\n"
                        + "5,4,45\n"
                        + "5,1,51\n"
                        + "1,5,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testReverse() throws Exception {
        /*
         * Test reverse()
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Edge<Long, Long>> data = graph.reverse().getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "2,1,12\n"
                        + "3,1,13\n"
                        + "3,2,23\n"
                        + "4,3,34\n"
                        + "5,3,35\n"
                        + "5,4,45\n"
                        + "1,5,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @SuppressWarnings("serial")
    @Test
    public void testSubGraph() throws Exception {
        /*
         * Test subgraph:
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Edge<Long, Long>> data =
                graph.subgraph(
                                new FilterFunction<Vertex<Long, Long>>() {
                                    public boolean filter(Vertex<Long, Long> vertex)
                                            throws Exception {
                                        return (vertex.getValue() > 2);
                                    }
                                },
                                new FilterFunction<Edge<Long, Long>>() {
                                    public boolean filter(Edge<Long, Long> edge) throws Exception {
                                        return (edge.getValue() > 34);
                                    }
                                })
                        .getEdges();

        List<Edge<Long, Long>> result = data.collect();

        expectedResult = "3,5,35\n" + "4,5,45\n";

        compareResultAsTuples(result, expectedResult);
    }

    @SuppressWarnings("serial")
    @Test
    public void testFilterVertices() throws Exception {
        /*
         * Test filterOnVertices:
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Edge<Long, Long>> data =
                graph.filterOnVertices(
                                new FilterFunction<Vertex<Long, Long>>() {
                                    public boolean filter(Vertex<Long, Long> vertex)
                                            throws Exception {
                                        return (vertex.getValue() > 2);
                                    }
                                })
                        .getEdges();

        List<Edge<Long, Long>> result = data.collect();

        expectedResult = "3,4,34\n" + "3,5,35\n" + "4,5,45\n";

        compareResultAsTuples(result, expectedResult);
    }

    @SuppressWarnings("serial")
    @Test
    public void testFilterEdges() throws Exception {
        /*
         * Test filterOnEdges:
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Edge<Long, Long>> data =
                graph.filterOnEdges(
                                new FilterFunction<Edge<Long, Long>>() {
                                    public boolean filter(Edge<Long, Long> edge) throws Exception {
                                        return (edge.getValue() > 34);
                                    }
                                })
                        .getEdges();

        List<Edge<Long, Long>> result = data.collect();

        expectedResult = "3,5,35\n" + "4,5,45\n" + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testNumberOfVertices() throws Exception {
        /*
         * Test numberOfVertices()
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);
        DataSet<Long> data = env.fromElements(graph.numberOfVertices());

        List<Long> result = data.collect();

        expectedResult = "5";

        compareResultAsText(result, expectedResult);
    }

    @Test
    public void testNumberOfEdges() throws Exception {
        /*
         * Test numberOfEdges()
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);
        DataSet<Long> data = env.fromElements(graph.numberOfEdges());

        List<Long> result = data.collect();

        expectedResult = "7";

        compareResultAsText(result, expectedResult);
    }

    @Test
    public void testVertexIds() throws Exception {
        /*
         * Test getVertexIds()
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Long> data = graph.getVertexIds();
        List<Long> result = data.collect();

        expectedResult = "1\n2\n3\n4\n5\n";

        compareResultAsText(result, expectedResult);
    }

    @Test
    public void testEdgesIds() throws Exception {
        /*
         * Test getEdgeIds()
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Tuple2<Long, Long>> data = graph.getEdgeIds();
        List<Tuple2<Long, Long>> result = data.collect();

        expectedResult = "1,2\n" + "1,3\n" + "2,3\n" + "3,4\n" + "3,5\n" + "4,5\n" + "5,1\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testUnion() throws Exception {
        /*
         * Test union()
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        List<Vertex<Long, Long>> vertices = new ArrayList<>();
        List<Edge<Long, Long>> edges = new ArrayList<>();

        vertices.add(new Vertex<>(6L, 6L));
        edges.add(new Edge<>(6L, 1L, 61L));

        graph = graph.union(Graph.fromCollection(vertices, edges, env));

        DataSet<Edge<Long, Long>> data = graph.getEdges();
        List<Edge<Long, Long>> result = data.collect();

        expectedResult =
                "1,2,12\n"
                        + "1,3,13\n"
                        + "2,3,23\n"
                        + "3,4,34\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,51\n"
                        + "6,1,61\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testDifference() throws Exception {
        /*Test  difference() method  by checking    the output  for getEdges()   on  the resultant   graph
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> graph2 =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexDataDifference(env),
                        TestGraphUtils.getLongLongEdgeDataDifference(env),
                        env);

        graph = graph.difference(graph2);

        List<Edge<Long, Long>> result = graph.getEdges().collect();

        expectedResult = "4,5,45\n";
        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testDifferenceVertices() throws Exception {
        /*Test  difference() method  by checking    the output  for getVertices()   on  the resultant   graph
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        Graph<Long, Long, Long> graph2 =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexDataDifference(env),
                        TestGraphUtils.getLongLongEdgeDataDifference(env),
                        env);

        graph = graph.difference(graph2);

        List<Vertex<Long, Long>> result = graph.getVertices().collect();

        expectedResult = "2,2\n" + "4,4\n" + "5,5\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public void testDifference2() throws Exception {
        /*
         * Test difference() such that no common vertices are there
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Vertex<Long, Long>> vertex = env.fromElements(new Vertex<>(6L, 6L));

        Graph<Long, Long, Long> graph2 =
                Graph.fromDataSet(vertex, TestGraphUtils.getLongLongEdgeDataDifference2(env), env);

        graph = graph.difference(graph2);

        List<Edge<Long, Long>> result = graph.getEdges().collect();

        expectedResult =
                "1,2,12\n"
                        + "1,3,13\n"
                        + "2,3,23\n"
                        + "3,4,34\n"
                        + "3,5,35\n"
                        + "4,5,45\n"
                        + "5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }

    @Test
    public final void testIntersect() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        List<Edge<Long, Long>> edges1 = new ArrayList<>();
        edges1.add(new Edge<>(1L, 3L, 12L));
        edges1.add(new Edge<>(1L, 3L, 13L)); // needs to be in the output
        edges1.add(new Edge<>(1L, 3L, 14L));

        @SuppressWarnings("unchecked")
        List<Edge<Long, Long>> edges2 = new ArrayList<>();
        edges2.add(new Edge<>(1L, 3L, 13L));

        Graph<Long, NullValue, Long> graph1 = Graph.fromCollection(edges1, env);
        Graph<Long, NullValue, Long> graph2 = Graph.fromCollection(edges2, env);

        Graph<Long, NullValue, Long> intersect = graph1.intersect(graph2, true);

        List<Vertex<Long, NullValue>> vertices = new ArrayList<>();
        List<Edge<Long, Long>> edges = new ArrayList<>();

        intersect.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
        intersect.getEdges().output(new LocalCollectionOutputFormat<>(edges));

        env.execute();

        String expectedVertices = "1,(null)\n" + "3,(null)\n";

        String expectedEdges = "1,3,13\n";

        compareResultAsTuples(vertices, expectedVertices);
        compareResultAsTuples(edges, expectedEdges);
    }

    @Test
    public final void testIntersectWithPairs() throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        @SuppressWarnings("unchecked")
        List<Edge<Long, Long>> edges1 = new ArrayList<>();
        edges1.add(new Edge<>(1L, 3L, 12L));
        edges1.add(new Edge<>(1L, 3L, 13L));
        edges1.add(new Edge<>(1L, 3L, 13L)); // output
        edges1.add(new Edge<>(1L, 3L, 13L)); // output
        edges1.add(new Edge<>(1L, 3L, 14L)); // output

        @SuppressWarnings("unchecked")
        List<Edge<Long, Long>> edges2 = new ArrayList<>();
        edges2.add(new Edge<>(1L, 3L, 13L)); // output
        edges2.add(new Edge<>(1L, 3L, 13L)); // output
        edges2.add(new Edge<>(1L, 3L, 14L)); // output

        Graph<Long, NullValue, Long> graph1 = Graph.fromCollection(edges1, env);
        Graph<Long, NullValue, Long> graph2 = Graph.fromCollection(edges2, env);

        Graph<Long, NullValue, Long> intersect = graph1.intersect(graph2, false);

        List<Vertex<Long, NullValue>> vertices = new ArrayList<>();
        List<Edge<Long, Long>> edges = new ArrayList<>();

        intersect.getVertices().output(new LocalCollectionOutputFormat<>(vertices));
        intersect.getEdges().output(new LocalCollectionOutputFormat<>(edges));

        env.execute();

        String expectedVertices = "1,(null)\n" + "3,(null)\n";

        String expectedEdges =
                "1,3,13\n" + "1,3,13\n" + "1,3,13\n" + "1,3,13\n" + "1,3,14\n" + "1,3,14";

        compareResultAsTuples(vertices, expectedVertices);
        compareResultAsTuples(edges, expectedEdges);
    }

    @Test
    public void testTriplets() throws Exception {
        /*
         * Test getTriplets()
         */
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Graph<Long, Long, Long> graph =
                Graph.fromDataSet(
                        TestGraphUtils.getLongLongVertexData(env),
                        TestGraphUtils.getLongLongEdgeData(env),
                        env);

        DataSet<Triplet<Long, Long, Long>> data = graph.getTriplets();
        List<Triplet<Long, Long, Long>> result = data.collect();

        expectedResult =
                "1,2,1,2,12\n"
                        + "1,3,1,3,13\n"
                        + "2,3,2,3,23\n"
                        + "3,4,3,4,34\n"
                        + "3,5,3,5,35\n"
                        + "4,5,4,5,45\n"
                        + "5,1,5,1,51\n";

        compareResultAsTuples(result, expectedResult);
    }
}
