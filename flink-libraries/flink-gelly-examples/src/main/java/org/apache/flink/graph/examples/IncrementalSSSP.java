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

package org.apache.flink.graph.examples;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.examples.data.IncrementalSSSPData;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.spargel.ScatterGatherConfiguration;

/**
 * This example illustrates how to
 *
 * <ul>
 *   <li>create a Graph directly from CSV files
 *   <li>use the scatter-gather iteration's messaging direction configuration option
 * </ul>
 *
 * <p>Incremental Single Sink Shortest Paths Example. Shortest Paths are incrementally updated upon
 * edge removal.
 *
 * <p>The program takes as input the resultant graph after a SSSP computation, an edge to be removed
 * and the initial graph (i.e. before SSSP was computed). In the following description, SP-graph is
 * used as an abbreviation for the graph resulted from the SSSP computation. We denote the edges
 * that belong to this graph by SP-edges. - If the removed edge does not belong to the SP-graph then
 * no computation is necessary and the edge is simply removed from the graph. - If the removed edge
 * is an SP-edge, then all nodes, whose shortest path contains the removed edge, potentially require
 * re-computation.
 *
 * <p>When the edge <code>&lt;u, v&gt;</code> is removed, v checks if it has another out-going
 * SP-edge. If yes, no further computation is required. If v has no other out-going SP-edge, it
 * invalidates its current value, by setting it to INF. Then, it informs all its SP-in-neighbors by
 * sending them an INVALIDATE message. When a vertex u receives an INVALIDATE message from v, it
 * checks whether it has another out-going SP-edge. If not, it invalidates its current value and
 * propagates the INVALIDATE message. The propagation stops when a vertex with an alternative
 * shortest path is reached or when we reach a vertex with no SP-in-neighbors.
 *
 * <p>Usage <code>IncrementalSSSP &lt;vertex path&gt; &lt;edge path&gt; &lt;edges in SSSP&gt;
 * &lt;src id edge to be removed&gt; &lt;trg id edge to be removed&gt; &lt;val edge to be removed&gt;
 * &lt;result path&gt; &lt;number of iterations&gt;</code><br>
 * If no parameters are provided, the program is run with default data from {@link
 * IncrementalSSSPData}
 */
@SuppressWarnings("serial")
public class IncrementalSSSP implements ProgramDescription {

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Edge<Long, Double> edgeToBeRemoved = getEdgeToBeRemoved();

        Graph<Long, Double, Double> graph = IncrementalSSSP.getGraph(env);

        // Assumption: all minimum weight paths are kept
        Graph<Long, Double, Double> ssspGraph = IncrementalSSSP.getSSSPGraph(env);

        // remove the edge
        graph.removeEdge(edgeToBeRemoved);

        // configure the iteration
        ScatterGatherConfiguration parameters = new ScatterGatherConfiguration();

        if (isInSSSP(edgeToBeRemoved, ssspGraph.getEdges())) {

            parameters.setDirection(EdgeDirection.IN);
            parameters.setOptDegrees(true);

            // run the scatter-gather iteration to propagate info
            Graph<Long, Double, Double> result =
                    ssspGraph.runScatterGatherIteration(
                            new InvalidateMessenger(edgeToBeRemoved),
                            new VertexDistanceUpdater(),
                            maxIterations,
                            parameters);

            DataSet<Vertex<Long, Double>> resultedVertices = result.getVertices();

            // Emit results
            if (fileOutput) {
                resultedVertices.writeAsCsv(outputPath, "\n", ",");
                env.execute("Incremental SSSP Example");
            } else {
                resultedVertices.print();
            }
        } else {
            // print the vertices
            if (fileOutput) {
                graph.getVertices().writeAsCsv(outputPath, "\n", ",");
                env.execute("Incremental SSSP Example");
            } else {
                graph.getVertices().print();
            }
        }
    }

    @Override
    public String getDescription() {
        return "Incremental Single Sink Shortest Paths Example";
    }

    // ******************************************************************************************************************
    // IncrementalSSSP METHODS
    // ******************************************************************************************************************

    /**
     * Function that verifies whether the edge to be removed is part of the SSSP or not. If it is,
     * the src vertex will be invalidated.
     *
     * @param edgeToBeRemoved
     * @param edgesInSSSP
     * @return true or false
     */
    public static boolean isInSSSP(
            final Edge<Long, Double> edgeToBeRemoved, DataSet<Edge<Long, Double>> edgesInSSSP)
            throws Exception {

        return edgesInSSSP
                        .filter(
                                new FilterFunction<Edge<Long, Double>>() {
                                    @Override
                                    public boolean filter(Edge<Long, Double> edge)
                                            throws Exception {
                                        return edge.equals(edgeToBeRemoved);
                                    }
                                })
                        .count()
                > 0;
    }

    /** Initiate or propagate INVALIDATE messages. */
    public static final class InvalidateMessenger
            extends ScatterFunction<Long, Double, Double, Double> {

        private Edge<Long, Double> edgeToBeRemoved;

        public InvalidateMessenger(Edge<Long, Double> edgeToBeRemoved) {
            this.edgeToBeRemoved = edgeToBeRemoved;
        }

        @Override
        public void sendMessages(Vertex<Long, Double> vertex) throws Exception {

            if (getSuperstepNumber() == 1) {
                if (vertex.getId().equals(edgeToBeRemoved.getSource())) {
                    // activate the edge target
                    sendMessageTo(edgeToBeRemoved.getSource(), Double.MAX_VALUE);
                }
            }

            if (getSuperstepNumber() > 1) {
                // invalidate all edges
                for (Edge<Long, Double> edge : getEdges()) {
                    sendMessageTo(edge.getSource(), Double.MAX_VALUE);
                }
            }
        }
    }

    /**
     * When an INVALIDATE message indicates that the only shortest path containing this vertex has
     * been removed then set the vertex distance to infinity.
     */
    public static final class VertexDistanceUpdater extends GatherFunction<Long, Double, Double> {

        @Override
        public void updateVertex(Vertex<Long, Double> vertex, MessageIterator<Double> inMessages)
                throws Exception {
            if (inMessages.hasNext()) {
                Long outDegree = getOutDegree() - 1;
                // check if the vertex has another SP-Edge
                if (outDegree <= 0) {
                    // set own value to infinity
                    setNewVertexValue(Double.MAX_VALUE);
                }
            }
        }
    }

    // ******************************************************************************************************************
    // UTIL METHODS
    // ******************************************************************************************************************

    private static boolean fileOutput = false;

    private static String verticesInputPath = null;

    private static String edgesInputPath = null;

    private static String edgesInSSSPInputPath = null;

    private static Long srcEdgeToBeRemoved = null;

    private static Long trgEdgeToBeRemoved = null;

    private static Double valEdgeToBeRemoved = null;

    private static String outputPath = null;

    private static int maxIterations = 5;

    private static boolean parseParameters(String[] args) {
        if (args.length > 0) {
            if (args.length == 8) {
                fileOutput = true;
                verticesInputPath = args[0];
                edgesInputPath = args[1];
                edgesInSSSPInputPath = args[2];
                srcEdgeToBeRemoved = Long.parseLong(args[3]);
                trgEdgeToBeRemoved = Long.parseLong(args[4]);
                valEdgeToBeRemoved = Double.parseDouble(args[5]);
                outputPath = args[6];
                maxIterations = Integer.parseInt(args[7]);
            } else {
                System.out.println(
                        "Executing IncrementalSSSP example with default parameters and built-in default data.");
                System.out.println("Provide parameters to read input data from files.");
                System.out.println("See the documentation for the correct format of input files.");
                System.out.println(
                        "Usage: IncrementalSSSP <vertex path> <edge path> <edges in SSSP> "
                                + "<src id edge to be removed> <trg id edge to be removed> <val edge to be removed> "
                                + "<output path> <max iterations>");

                return false;
            }
        }
        return true;
    }

    private static Graph<Long, Double, Double> getGraph(ExecutionEnvironment env) {
        if (fileOutput) {
            return Graph.fromCsvReader(verticesInputPath, edgesInputPath, env)
                    .lineDelimiterEdges("\n")
                    .types(Long.class, Double.class, Double.class);
        } else {
            return Graph.fromDataSet(
                    IncrementalSSSPData.getDefaultVertexDataSet(env),
                    IncrementalSSSPData.getDefaultEdgeDataSet(env),
                    env);
        }
    }

    private static Graph<Long, Double, Double> getSSSPGraph(ExecutionEnvironment env) {
        if (fileOutput) {
            return Graph.fromCsvReader(verticesInputPath, edgesInSSSPInputPath, env)
                    .lineDelimiterEdges("\n")
                    .types(Long.class, Double.class, Double.class);
        } else {
            return Graph.fromDataSet(
                    IncrementalSSSPData.getDefaultVertexDataSet(env),
                    IncrementalSSSPData.getDefaultEdgesInSSSP(env),
                    env);
        }
    }

    private static Edge<Long, Double> getEdgeToBeRemoved() {
        if (fileOutput) {
            return new Edge<>(srcEdgeToBeRemoved, trgEdgeToBeRemoved, valEdgeToBeRemoved);
        } else {
            return IncrementalSSSPData.getDefaultEdgeToBeRemoved();
        }
    }
}
