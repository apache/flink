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

package org.apache.flink.examples.java.graph;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.examples.java.graph.util.EnumTrianglesData;
import org.apache.flink.examples.java.graph.util.EnumTrianglesDataTypes.Edge;
import org.apache.flink.examples.java.graph.util.EnumTrianglesDataTypes.Triad;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Triangle enumeration is a pre-processing step to find closely connected parts in graphs. A
 * triangle consists of three edges that connect three vertices with each other.
 *
 * <p>The algorithm works as follows: It groups all edges that share a common vertex and builds
 * triads, i.e., triples of vertices that are connected by two edges. Finally, all triads are
 * filtered for which no third edge exists that closes the triangle.
 *
 * <p>Input files are plain text files and must be formatted as follows:
 *
 * <ul>
 *   <li>Edges are represented as pairs for vertex IDs which are separated by space characters.
 *       Edges are separated by new-line characters.<br>
 *       For example <code>"1 2\n2 12\n1 12\n42 63"</code> gives four (undirected) edges (1)-(2),
 *       (2)-(12), (1)-(12), and (42)-(63) that include a triangle
 * </ul>
 *
 * <pre>
 *     (1)
 *     /  \
 *   (2)-(12)
 * </pre>
 *
 * <p>Usage: <code>EnumTriangleBasic --edges &lt;path&gt; --output &lt;path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link
 * EnumTrianglesData}.
 *
 * <p>This example shows how to use:
 *
 * <ul>
 *   <li>Custom Java objects which extend Tuple
 *   <li>Group Sorting
 * </ul>
 */
@SuppressWarnings("serial")
public class EnumTriangles {

    // *************************************************************************
    //     PROGRAM
    // *************************************************************************

    public static void main(String[] args) throws Exception {

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        // read input data
        DataSet<Edge> edges;
        if (params.has("edges")) {
            edges =
                    env.readCsvFile(params.get("edges"))
                            .fieldDelimiter(" ")
                            .includeFields(true, true)
                            .types(Integer.class, Integer.class)
                            .map(new TupleEdgeConverter());
        } else {
            System.out.println("Executing EnumTriangles example with default edges data set.");
            System.out.println("Use --edges to specify file input.");
            edges = EnumTrianglesData.getDefaultEdgeDataSet(env);
        }

        // project edges by vertex id
        DataSet<Edge> edgesById = edges.map(new EdgeByIdProjector());

        DataSet<Triad> triangles =
                edgesById
                        // build triads
                        .groupBy(Edge.V1)
                        .sortGroup(Edge.V2, Order.ASCENDING)
                        .reduceGroup(new TriadBuilder())
                        // filter triads
                        .join(edgesById)
                        .where(Triad.V2, Triad.V3)
                        .equalTo(Edge.V1, Edge.V2)
                        .with(new TriadFilter());

        // emit result
        if (params.has("output")) {
            triangles.writeAsCsv(params.get("output"), "\n", ",");
            // execute program
            env.execute("Basic Triangle Enumeration Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            triangles.print();
        }
    }

    // *************************************************************************
    //     USER FUNCTIONS
    // *************************************************************************

    /** Converts a Tuple2 into an Edge. */
    @ForwardedFields("0;1")
    public static class TupleEdgeConverter implements MapFunction<Tuple2<Integer, Integer>, Edge> {
        private final Edge outEdge = new Edge();

        @Override
        public Edge map(Tuple2<Integer, Integer> t) throws Exception {
            outEdge.copyVerticesFromTuple2(t);
            return outEdge;
        }
    }

    /**
     * Projects an edge (pair of vertices) such that the id of the first is smaller than the id of
     * the second.
     */
    private static class EdgeByIdProjector implements MapFunction<Edge, Edge> {

        @Override
        public Edge map(Edge inEdge) throws Exception {

            // flip vertices if necessary
            if (inEdge.getFirstVertex() > inEdge.getSecondVertex()) {
                inEdge.flipVertices();
            }

            return inEdge;
        }
    }

    /**
     * Builds triads (triples of vertices) from pairs of edges that share a vertex. The first vertex
     * of a triad is the shared vertex, the second and third vertex are ordered by vertexId. Assumes
     * that input edges share the first vertex and are in ascending order of the second vertex.
     */
    @ForwardedFields("0")
    private static class TriadBuilder implements GroupReduceFunction<Edge, Triad> {
        private final List<Integer> vertices = new ArrayList<Integer>();
        private final Triad outTriad = new Triad();

        @Override
        public void reduce(Iterable<Edge> edgesIter, Collector<Triad> out) throws Exception {

            final Iterator<Edge> edges = edgesIter.iterator();

            // clear vertex list
            vertices.clear();

            // read first edge
            Edge firstEdge = edges.next();
            outTriad.setFirstVertex(firstEdge.getFirstVertex());
            vertices.add(firstEdge.getSecondVertex());

            // build and emit triads
            while (edges.hasNext()) {
                Integer higherVertexId = edges.next().getSecondVertex();

                // combine vertex with all previously read vertices
                for (Integer lowerVertexId : vertices) {
                    outTriad.setSecondVertex(lowerVertexId);
                    outTriad.setThirdVertex(higherVertexId);
                    out.collect(outTriad);
                }
                vertices.add(higherVertexId);
            }
        }
    }

    /** Filters triads (three vertices connected by two edges) without a closing third edge. */
    private static class TriadFilter implements JoinFunction<Triad, Edge, Triad> {

        @Override
        public Triad join(Triad triad, Edge edge) throws Exception {
            return triad;
        }
    }
}
