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
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.ExampleUtils;

import java.util.HashSet;

public class LocalClusteringCoefficientExample implements ProgramDescription {

	// --------------------------------------------------------------------------------------------
	//  Program
	// --------------------------------------------------------------------------------------------

	public static void main (String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Vertex<Long, Double>> vertices = getVertexDataSet(env);
		DataSet<Edge<Long, Double>> edges = getEdgeDataSet(env);
		Graph<Long, Double, Double> graph = Graph.fromDataSet(vertices, edges, env);

		// Get the neighbors of each vertex in a HashSet
		DataSet<Tuple2<Long, HashSet<Long>>> neighborhoods = graph
				.reduceOnEdges(new NeighborhoodEdgesFunction(), EdgeDirection.OUT);

		// Construct a new graph where the neighborhood is the vertex value
		Graph<Long, HashSet<Long>, Double> newGraph = graph
				.mapVertices(new EmptyVertexMapFunction())
				.joinWithVertices(neighborhoods, new NeighborhoodVertexMapFunction());

		// Calculate clustering coefficient
		DataSet<Tuple2<Long, Double>> clusteringCoefficients = newGraph
				.reduceOnNeighbors(new ClusteringCoefficientNeighborsFunction(), EdgeDirection.OUT);

		// Emit results
		if(fileOutput) {
			clusteringCoefficients.writeAsCsv(outputPath, "\n", ",");
		} else {
			clusteringCoefficients.print();
		}

		env.execute("Local Clustering Coefficient Example");
	}

	// --------------------------------------------------------------------------------------------
	//  Clustering Coefficient Functions
	// --------------------------------------------------------------------------------------------

	private static final class NeighborhoodEdgesFunction
			implements EdgesFunction<Long, Double, Tuple2<Long, HashSet<Long>>> {

		@Override
		public Tuple2<Long, HashSet<Long>> iterateEdges(
				Iterable<Tuple2<Long, Edge<Long, Double>>> edges) throws Exception {

			Long vertexId = null;
			HashSet<Long> neighbors = new HashSet<Long>();

			for (Tuple2<Long, Edge<Long, Double>> edge : edges) {
				vertexId = edge.f0;
				neighbors.add(edge.f1.f1);
			}

			return new Tuple2<Long, HashSet<Long>>(vertexId, neighbors);
		}
	}

	private static final class EmptyVertexMapFunction
			implements MapFunction<Vertex<Long, Double>, HashSet<Long>> {

		@Override
		public HashSet<Long> map(Vertex<Long, Double> arg) throws Exception {
			return new HashSet<Long>();
		}
	}

	private static final class NeighborhoodVertexMapFunction
			implements MapFunction<Tuple2<HashSet<Long>, HashSet<Long>>, HashSet<Long>> {

		@Override
		public HashSet<Long> map(Tuple2<HashSet<Long>, HashSet<Long>> arg) throws Exception {
			return arg.f1;
		}
	}

	private static final class ClusteringCoefficientNeighborsFunction
			implements NeighborsFunctionWithVertexValue<Long, HashSet<Long>, Double, Tuple2<Long, Double>> {

		@Override
		public Tuple2<Long, Double> iterateNeighbors(Vertex<Long, HashSet<Long>> vertex,
				Iterable<Tuple2<Edge<Long, Double>, Vertex<Long, HashSet<Long>>>> neighbors) throws Exception {

			int deg = vertex.getValue().size();
			int e = 0;

			// Calculate common neighbor count (e)
			for (Tuple2<Edge<Long, Double>, Vertex<Long, HashSet<Long>>> neighbor : neighbors) {
				// Iterate neighbor's neighbors
				for (Long nn : neighbor.f1.f1) {
					if (vertex.getValue().contains(nn)) {
						e++;
					}
				}
			}

			// Calculate clustering coefficient
			double cc;

			if (deg > 1) {
				cc = (double) e / (double) (deg * (deg - 1));
			} else {
				cc = 0.0;
			}

			return new Tuple2<Long, Double>(vertex.getId(), cc);
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Util Methods
	// --------------------------------------------------------------------------------------------

	private static boolean fileOutput = false;
	private static String vertexInputPath = null;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 3) {
				System.err.println("Usage: LocalClusteringCoefficient <vertex path> <edge path> <output path>");
				return false;
			}

			fileOutput = true;
			vertexInputPath = args[0];
			edgeInputPath = args[1];
			outputPath = args[2];
		} else {
			System.out.println("Executing LocalClusteringCoefficient example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: LocalClusteringCoefficient <vertex path> <edge path> <output path>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Vertex<Long, Double>> getVertexDataSet(ExecutionEnvironment env) {

		if (fileOutput) {
			return env.readCsvFile(vertexInputPath)
					.fieldDelimiter(" ")
					.lineDelimiter("\n")
					.types(Long.class, Double.class)
					.map(new MapFunction<Tuple2<Long, Double>, Vertex<Long, Double>>() {
						@Override
						public Vertex<Long, Double> map(Tuple2<Long, Double> value) throws Exception {
							return new Vertex<Long, Double>(value.f0, value.f1);
						}
					});
		}

		return ExampleUtils.getLongDoubleVertexData(env);
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, Double>> getEdgeDataSet(ExecutionEnvironment env) {

		if (fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.fieldDelimiter(" ")
					.lineDelimiter("\n")
					.types(Long.class, Long.class, Double.class)
					.map(new MapFunction<Tuple3<Long, Long, Double>, Edge<Long, Double>>() {
						@Override
						public Edge<Long, Double> map(Tuple3<Long, Long, Double> value) throws Exception {
							return new Edge<Long, Double>(value.f0, value.f1, value.f2);
						}
					});
		}

		return ExampleUtils.getLongDoubleEdgeData(env);
	}

	@Override
	public String getDescription() {
		return "Local Clustering Coefficient Example";
	}
}
