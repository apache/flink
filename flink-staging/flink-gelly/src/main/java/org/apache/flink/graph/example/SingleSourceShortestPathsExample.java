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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.SingleSourceShortestPathsData;
import org.apache.flink.graph.library.SingleSourceShortestPaths;

public class SingleSourceShortestPathsExample implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Vertex<Long, Double>> vertices = getVerticesDataSet(env);

		DataSet<Edge<Long, Double>> edges = getEdgesDataSet(env);

		Graph<Long, Double, Double> graph = Graph.fromDataSet(vertices, edges, env);

		DataSet<Vertex<Long, Double>> singleSourceShortestPaths = graph
				.run(new SingleSourceShortestPaths<Long>(srcVertexId,
						maxIterations)).getVertices();

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

	private static Long srcVertexId = null;

	private static String verticesInputPath = null;

	private static String edgesInputPath = null;

	private static String outputPath = null;

	private static int maxIterations = 5;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length == 5) {
				fileOutput = true;
				srcVertexId = Long.parseLong(args[0]);
				verticesInputPath = args[1];
				edgesInputPath = args[2];
				outputPath = args[3];
				maxIterations = Integer.parseInt(args[4]);
			} else {
				System.err.println("Usage: SingleSourceShortestPaths <source vertex id>" +
						" <input vertices path> <input edges path> <output path> <num iterations>");
				return false;
			}
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Vertex<Long, Double>> getVerticesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(verticesInputPath)
					.lineDelimiter("\n")
					.types(Long.class, Double.class)
					.map(new MapFunction<Tuple2<Long, Double>, Vertex<Long, Double>>() {

						@Override
						public Vertex<Long, Double> map(Tuple2<Long, Double> tuple2) throws Exception {
							return new Vertex<Long, Double>(tuple2.f0, tuple2.f1);
						}
					});
		} else {
			System.err.println("Usage: SingleSourceShortestPaths <source vertex id>" +
					" <input vertices path> <input edges path> <output path> <num iterations>");
			return SingleSourceShortestPathsData.getDefaultVertexDataSet(env);
		}
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgesInputPath)
					.lineDelimiter("\n")
					.types(Long.class, Long.class, Double.class)
					.map(new MapFunction<Tuple3<Long, Long, Double>, Edge<Long, Double>>() {

						@Override
						public Edge<Long, Double> map(Tuple3<Long, Long, Double> tuple3) throws Exception {
							return new Edge<Long, Double>(tuple3.f0, tuple3.f1, tuple3.f2);
						}
					});
		} else {
			System.err.println("Usage: SingleSourceShortestPaths <source vertex id>" +
					" <input vertices path> <input edges path> <output path> <num iterations>");
			return SingleSourceShortestPathsData.getDefaultEdgeDataSet(env);
		}
	}
}
