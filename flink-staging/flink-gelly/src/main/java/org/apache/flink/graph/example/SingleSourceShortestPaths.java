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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.SingleSourceShortestPathsData;
import org.apache.flink.graph.library.SingleSourceShortestPathsAlgorithm;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;

/**
 * This example implements the Single Source Shortest Paths algorithm,
 * using a vertex-centric iteration.
 *
 * The input file is expected to contain one edge per line, with long IDs
 * and double weights, separated by tabs, in the following format:
 * "<sourceVertexID>\t<targetVertexID>\t<edgeValue>".
 *
 * If no arguments are provided, the example runs with default data from {@link SingleSourceShortestPathsData}.
 *
 */
public class SingleSourceShortestPaths implements ProgramDescription {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, Double>> edges = getEdgesDataSet(env);

		Graph<Long, Double, Double> graph = Graph.fromDataSet(edges,
				new MapFunction<Long, Double>() {

					public Double map(Long value) {
						return Double.MAX_VALUE;
					}
		}, env);

		DataSet<Vertex<Long, Double>> singleSourceShortestPaths = graph
				.run(new SingleSourceShortestPathsAlgorithm<Long>(srcVertexId, maxIterations))
				.getVertices();

		// emit result
		if (fileOutput) {
			singleSourceShortestPaths.writeAsCsv(outputPath, "\n", ",");
		} else {
			singleSourceShortestPaths.print();
		}

		env.execute("Single Source Shortest Paths Example");
	}

	@Override
	public String getDescription() {
		return "Single Source Shortest Paths";
	}

	// ******************************************************************************************************************
	// UTIL METHODS
	// ******************************************************************************************************************

	private static boolean fileOutput = false;

	private static Long srcVertexId = 1l;

	private static String edgesInputPath = null;

	private static String outputPath = null;

	private static int maxIterations = 5;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 4) {
				System.err.println("Usage: SingleSourceShortestPaths <source vertex id>" +
						" <input edges path> <output path> <num iterations>");
				return false;
			}

			fileOutput = true;
			srcVertexId = Long.parseLong(args[0]);
			edgesInputPath = args[1];
			outputPath = args[2];
			maxIterations = Integer.parseInt(args[3]);
		} else {
				System.out.println("Executing Single Source Shortest Paths example "
						+ "with default parameters and built-in default data.");
				System.out.println("  Provide parameters to read input data from files.");
				System.out.println("  See the documentation for the correct format of input files.");
				System.out.println("Usage: SingleSourceShortestPaths <source vertex id>" +
						" <input edges path> <output path> <num iterations>");
		}
		return true;
	}

	private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgesInputPath)
					.lineDelimiter("\n")
					.fieldDelimiter("\t")
					.types(Long.class, Long.class, Double.class)
					.map(new Tuple3ToEdgeMap<Long, Double>());
		} else {
			return SingleSourceShortestPathsData.getDefaultEdgeDataSet(env);
		}
	}
}
