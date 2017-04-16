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

import org.apache.flink.graph.example.utils.MinSpanningTreeData;
import org.apache.flink.graph.library.MinSpanningTree;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.api.common.ProgramDescription;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

/**
 * This example shows how to use the {@link org.apache.flink.graph.library.MinSpanningTree}
 * library method:
 * <ul>
 *     <li> with the vertex and the edge data sets given as parameters
 *     <li> with default data
 * </ul>
 */
public class MinSpanningTreeExample implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Vertex<Long, String>> vertices = getVerticesDataSet(env);

		DataSet<Edge<Long, Double>> edges = getEdgesDataSet(env);

		Graph<Long, String, Double> graph = Graph.fromDataSet(vertices, edges, env);

		DataSet<Edge<Long, Double>> minimumSpanningTree = graph.run(new MinSpanningTree(maxIterations)).getEdges();

		// emit result
		if (fileOutput) {
			minimumSpanningTree.writeAsCsv(outputPath, "\n", ",");
		} else {
			minimumSpanningTree.print();
		}

		minimumSpanningTree.getExecutionEnvironment().execute("Executing MinSpanningTree Example");
	}

	@Override
	public String getDescription() {
		return "A Parallel Version of Boruvka's Minimum Spanning Tree Algorithm";
	}

	// ******************************************************************************************************************
	// UTIL METHODS
	// ******************************************************************************************************************

	private static boolean fileOutput = false;

	private static String verticesInputPath = null;

	private static String edgesInputPath = null;

	private static String outputPath = null;

	private static int maxIterations = 10;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length == 4) {
				fileOutput = true;
				verticesInputPath = args[0];
				edgesInputPath = args[1];
				outputPath = args[2];
				maxIterations = Integer.parseInt(args[3]);
			} else {
				System.err.println("Usage: MinSpanningTree <input vertices path> <input edges path> <output path> " + "<num iterations>");
				return false;
			}
		}
		return true;
	}

	private static DataSet<Vertex<Long, String>> getVerticesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(verticesInputPath)
				.lineDelimiter("\n")
				.types(String.class)
				.map(new MapFunction<Tuple1<String>, Vertex<Long, String>>() {

					@Override
					public Vertex<Long, String> map(Tuple1<String> longTuple1) throws Exception {
						return new Vertex<Long, String>(Long.parseLong(longTuple1.f0), "");
					}
				});
		} else {
			System.err.println("Usage: MinSpanningTree <input vertices path> <input edges path> <output path> " + "<num iterations>");
			return MinSpanningTreeData.getDefaultVertexDataSet(env);
		}
	}

	private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgesInputPath)
				.lineDelimiter("\n")
				.types(String.class, Long.class, String.class)
				.map(new MapFunction<Tuple3<String, Long, String>, Edge<Long, Double>>() {

					@Override
					public Edge<Long, Double> map(Tuple3<String, Long, String> tuple3) throws Exception {
						return new Edge(Long.parseLong(tuple3.f0), tuple3.f1, Double.parseDouble(tuple3.f2));
					}
				});
		} else {
			System.err.println("Usage: MinSpanningTree <input vertices path> <input edges path> <output path> " + "<num iterations>");
			return MinSpanningTreeData.getDefaultEdgeDataSet(env);
		}
	}
}
