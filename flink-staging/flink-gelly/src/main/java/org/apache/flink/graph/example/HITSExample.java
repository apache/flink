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

package org.apache.flink.graph.example;

import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.HITS;
import org.apache.flink.graph.utils.Hits;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.util.Collector;

/**
 * This program is an example for HITS algorithm.
 * the result is either a hub value or authority value base user selection.
 *
 * If no arguments are provided, the example runs with a random graph of 10 vertices
 * and random edge weights.
 */

public class HITSExample implements ProgramDescription {
    @SuppressWarnings("serial")
    public static void main(String[] args) throws Exception {

        if(!parseParameters(args)) {
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Edge<Long, String>> links = getEdgesDataSet(env);

        Graph<Long, Double, String> network = Graph.fromDataSet(links, new MapFunction<Long, Double>() {

            public Double map(Long value) throws Exception {
                return 1.0;
            }
        }, env);

    // add  graph to HITS class with iteration value and hub or authority enum value.
        DataSet<Vertex<Long, Double>> HitsValue =network.run(
                    new HITS<Long>(Hits.AUTHORITY,maxIterations)).getVertices();

        if (fileOutput) {
            HitsValue.writeAsCsv(outputPath, "\n", "\t");
            env.execute("HITS algorithm");
        } else {
            HitsValue.print();
//            env.execute("HITS algorithm");
        }

    }

    @Override
    public String getDescription() {
        return "HITS algorithm example";
    }

    // *************************************************************************
    //     UTIL METHODS
    // *************************************************************************

    private static boolean fileOutput = false;
    private static long numPages = 10;
    private static String edgeInputPath = null;
    private static String outputPath = null;
    private static int maxIterations = 5;

    private static boolean parseParameters(String[] args) {

        if(args.length > 0) {
            if(args.length != 3) {
                System.err.println("Usage: HITS <input edges path> <output path> <num iterations>");
                return false;
            }

            fileOutput = true;
            edgeInputPath = args[0];
            outputPath = args[1];
            maxIterations = Integer.parseInt(args[2]);
        } else {
            System.out.println("Executing HITS example with default parameters and built-in default data.");
            System.out.println("  Provide parameters to read input data from files.");
            System.out.println("  See the documentation for the correct format of input files.");
            System.out.println("  Usage: HITS <input edges path> <output path> <num iterations>");
        }
        return true;
    }

    @SuppressWarnings("serial")
    private static DataSet<Edge<Long, String>> getEdgesDataSet(ExecutionEnvironment env) {

        if (fileOutput) {
            return env.readCsvFile(edgeInputPath)
                    .fieldDelimiter("\t")
                    .lineDelimiter("\n")
                    .types(Long.class, Long.class, String.class)
                    .map(new Tuple3ToEdgeMap<Long, String>());
        }

        return env.generateSequence(1, numPages).flatMap(
                new FlatMapFunction<Long, Edge<Long, String>>() {
                    @Override
                    public void flatMap(Long key, Collector<Edge<Long, String>> out) throws Exception {
                        int numOutEdges = (int) (Math.random() * (numPages / 2));
                        for (int i = 0; i < numOutEdges; i++) {
                            long target = (long) (Math.random() * numPages) + 1;
                            out.collect(new Edge<Long, String>(key, target, " "));
                        }
                    }
                });
    }
}
