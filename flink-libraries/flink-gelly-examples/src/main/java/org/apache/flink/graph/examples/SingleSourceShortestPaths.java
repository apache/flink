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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.examples.data.SingleSourceShortestPathsData;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.graph.spargel.ScatterFunction;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;

/**
 * This example shows how to use Gelly's scatter-gather iterations.
 *
 * <p>It is an implementation of the Single-Source-Shortest-Paths algorithm. For a gather-sum-apply
 * implementation of the same algorithm, please refer to {@link GSASingleSourceShortestPaths}.
 *
 * <p>The input file is a plain text file and must be formatted as follows: Edges are represented by
 * tuples of srcVertexId, trgVertexId, distance which are separated by tabs. Edges themselves are
 * separated by newlines. For example: <code>1\t2\t0.1\n1\t3\t1.4\n</code> defines two edges, edge
 * 1-2 with distance 0.1, and edge 1-3 with distance 1.4.
 *
 * <p>If no parameters are provided, the program is run with default data from {@link
 * SingleSourceShortestPathsData}
 */
public class SingleSourceShortestPaths implements ProgramDescription {

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Edge<Long, Double>> edges = getEdgesDataSet(env);

        Graph<Long, Double, Double> graph =
                Graph.fromDataSet(edges, new InitVertices(srcVertexId), env);

        // Execute the scatter-gather iteration
        Graph<Long, Double, Double> result =
                graph.runScatterGatherIteration(
                        new MinDistanceMessenger(), new VertexDistanceUpdater(), maxIterations);

        // Extract the vertices as the result
        DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();

        // emit result
        if (fileOutput) {
            singleSourceShortestPaths.writeAsCsv(outputPath, "\n", ",");

            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("Single Source Shortest Paths Example");
        } else {
            singleSourceShortestPaths.print();
        }
    }

    // --------------------------------------------------------------------------------------------
    //  Single Source Shortest Path UDFs
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings("serial")
    private static final class InitVertices implements MapFunction<Long, Double> {

        private long srcId;

        public InitVertices(long srcId) {
            this.srcId = srcId;
        }

        public Double map(Long id) {
            if (id.equals(srcId)) {
                return 0.0;
            } else {
                return Double.POSITIVE_INFINITY;
            }
        }
    }

    /**
     * Distributes the minimum distance associated with a given vertex among all the target vertices
     * summed up with the edge's value.
     */
    @SuppressWarnings("serial")
    private static final class MinDistanceMessenger
            extends ScatterFunction<Long, Double, Double, Double> {

        @Override
        public void sendMessages(Vertex<Long, Double> vertex) {
            if (vertex.getValue() < Double.POSITIVE_INFINITY) {
                for (Edge<Long, Double> edge : getEdges()) {
                    sendMessageTo(edge.getTarget(), vertex.getValue() + edge.getValue());
                }
            }
        }
    }

    /**
     * Function that updates the value of a vertex by picking the minimum distance from all incoming
     * messages.
     */
    @SuppressWarnings("serial")
    private static final class VertexDistanceUpdater extends GatherFunction<Long, Double, Double> {

        @Override
        public void updateVertex(Vertex<Long, Double> vertex, MessageIterator<Double> inMessages) {

            Double minDistance = Double.MAX_VALUE;

            for (double msg : inMessages) {
                if (msg < minDistance) {
                    minDistance = msg;
                }
            }

            if (vertex.getValue() > minDistance) {
                setNewVertexValue(minDistance);
            }
        }
    }

    // ******************************************************************************************************************
    // UTIL METHODS
    // ******************************************************************************************************************

    private static boolean fileOutput = false;

    private static Long srcVertexId = 1L;

    private static String edgesInputPath = null;

    private static String outputPath = null;

    private static int maxIterations = 5;

    private static boolean parseParameters(String[] args) {

        if (args.length > 0) {
            if (args.length != 4) {
                System.err.println(
                        "Usage: SingleSourceShortestPaths <source vertex id>"
                                + " <input edges path> <output path> <num iterations>");
                return false;
            }

            fileOutput = true;
            srcVertexId = Long.parseLong(args[0]);
            edgesInputPath = args[1];
            outputPath = args[2];
            maxIterations = Integer.parseInt(args[3]);
        } else {
            System.out.println(
                    "Executing Single Source Shortest Paths example "
                            + "with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println(
                    "Usage: SingleSourceShortestPaths <source vertex id>"
                            + " <input edges path> <output path> <num iterations>");
        }
        return true;
    }

    private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {
        if (fileOutput) {
            return env.readCsvFile(edgesInputPath)
                    .lineDelimiter("\n")
                    .fieldDelimiter("\t")
                    .types(Long.class, Long.class, Double.class)
                    .map(new Tuple3ToEdgeMap<>());
        } else {
            return SingleSourceShortestPathsData.getDefaultEdgeDataSet(env);
        }
    }

    @Override
    public String getDescription() {
        return "Scatter-gather Single Source Shortest Paths";
    }
}
