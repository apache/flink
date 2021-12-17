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

package org.apache.flink.graph.asm;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.generator.CompleteGraph;
import org.apache.flink.graph.generator.EmptyGraph;
import org.apache.flink.graph.generator.RMatGraph;
import org.apache.flink.graph.generator.StarGraph;
import org.apache.flink.graph.generator.random.JDKRandomGeneratorFactory;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

import org.junit.Before;

import java.util.LinkedList;
import java.util.List;

/** Simple graphs for testing graph assembly functions. */
public class AsmTestBase {

    protected ExecutionEnvironment env;

    protected static final double ACCURACY = 0.000001;

    // simple graph
    protected Graph<IntValue, NullValue, NullValue> directedSimpleGraph;

    protected Graph<IntValue, NullValue, NullValue> undirectedSimpleGraph;

    // complete graph
    protected final long completeGraphVertexCount = 47;

    protected Graph<LongValue, NullValue, NullValue> completeGraph;

    // empty graph
    protected final long emptyGraphVertexCount = 3;

    protected Graph<LongValue, NullValue, NullValue> emptyGraphWithVertices;

    protected Graph<LongValue, NullValue, NullValue> emptyGraphWithoutVertices;

    // star graph
    protected final long starGraphVertexCount = 29;

    protected Graph<LongValue, NullValue, NullValue> starGraph;

    @Before
    public void setup() throws Exception {
        env = ExecutionEnvironment.createCollectionsEnvironment();
        env.getConfig().enableObjectReuse();

        // a "fish" graph
        Object[][] edges =
                new Object[][] {
                    new Object[] {0, 1},
                    new Object[] {0, 2},
                    new Object[] {2, 1},
                    new Object[] {2, 3},
                    new Object[] {3, 1},
                    new Object[] {3, 4},
                    new Object[] {5, 3},
                };

        List<Edge<IntValue, NullValue>> directedEdgeList = new LinkedList<>();

        for (Object[] edge : edges) {
            directedEdgeList.add(
                    new Edge<>(
                            new IntValue((int) edge[0]),
                            new IntValue((int) edge[1]),
                            NullValue.getInstance()));
        }

        directedSimpleGraph = Graph.fromCollection(directedEdgeList, env);
        undirectedSimpleGraph = directedSimpleGraph.getUndirected();

        // complete graph
        completeGraph = new CompleteGraph(env, completeGraphVertexCount).generate();

        // empty graph with vertices but no edges
        emptyGraphWithVertices = new EmptyGraph(env, emptyGraphVertexCount).generate();

        // empty graph with no vertices or edges
        emptyGraphWithoutVertices = new EmptyGraph(env, 0).generate();

        // star graph
        starGraph = new StarGraph(env, starGraphVertexCount).generate();
    }

    /**
     * Generate a directed RMat graph. Tests are usually run on a graph with scale=10 and
     * edgeFactor=16 but algorithms generating very large DataSets require smaller input graphs.
     *
     * <p>The examples program can write this graph as a CSV file for verifying algorithm results
     * with external libraries:
     *
     * <pre>
     * ./bin/flink run examples/flink-gelly-examples_*.jar --algorithm EdgeList \
     *     --input RMatGraph --type long --simplify directed --scale $SCALE --edge_factor $EDGE_FACTOR \
     *     --output csv --filename directedRMatGraph.csv
     * </pre>
     *
     * @param scale vertices are generated in the range [0, 2<sup>scale</sup>)
     * @param edgeFactor the edge count is {@code edgeFactor} * 2<sup>scale</sup>
     * @return directed RMat graph
     * @throws Exception on error
     */
    protected Graph<LongValue, NullValue, NullValue> directedRMatGraph(int scale, int edgeFactor)
            throws Exception {
        long vertexCount = 1L << scale;
        long edgeCount = edgeFactor * vertexCount;

        return new RMatGraph<>(env, new JDKRandomGeneratorFactory(), vertexCount, edgeCount)
                .generate()
                .run(new org.apache.flink.graph.asm.simple.directed.Simplify<>());
    }

    /**
     * Generate an undirected RMat graph. Tests are usually run on a graph with scale=10 and
     * edgeFactor=16 but algorithms generating very large DataSets require smaller input graphs.
     *
     * <p>The examples program can write this graph as a CSV file for verifying algorithm results
     * with external libraries:
     *
     * <pre>
     * ./bin/flink run examples/flink-gelly-examples_*.jar --algorithm EdgeList \
     *     --input RMatGraph --type long --simplify undirected --scale $SCALE --edge_factor $EDGE_FACTOR \
     *     --output csv --filename undirectedRMatGraph.csv
     * </pre>
     *
     * @param scale vertices are generated in the range [0, 2<sup>scale</sup>)
     * @param edgeFactor the edge count is {@code edgeFactor} * 2<sup>scale</sup>
     * @return undirected RMat graph
     * @throws Exception on error
     */
    protected Graph<LongValue, NullValue, NullValue> undirectedRMatGraph(int scale, int edgeFactor)
            throws Exception {
        long vertexCount = 1L << scale;
        long edgeCount = edgeFactor * vertexCount;

        return new RMatGraph<>(env, new JDKRandomGeneratorFactory(), vertexCount, edgeCount)
                .generate()
                .run(new org.apache.flink.graph.asm.simple.undirected.Simplify<>(false));
    }
}
