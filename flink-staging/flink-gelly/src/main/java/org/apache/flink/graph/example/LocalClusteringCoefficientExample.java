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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeDirection;
import org.apache.flink.graph.EdgesFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.NeighborsFunctionWithVertexValue;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.LocalClusteringCoefficientData;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

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

		// Assume an undirected graph without multiple edges, so we create all reverse edges and call distinct
		DataSet<Edge<Long, NullValue>> edges = getEdgeDataSet(env)
				.flatMap(new FlatMapFunction<Edge<Long, NullValue>, Edge<Long, NullValue>>() {
					@Override
					public void flatMap(Edge<Long, NullValue> edge, Collector<Edge<Long, NullValue>> out)
							throws Exception {
						out.collect(edge);
						out.collect(edge.reverse());
					}
				})
				.distinct();

		Graph<Long, NullValue, NullValue> graph = Graph.fromDataSet(edges, env);

		// Get the neighbors of each vertex in a HashSet
		DataSet<Tuple2<Long, HashSet<Long>>> neighborhoods = graph
				.reduceOnEdges(new NeighborhoodEdgesFunction(), EdgeDirection.OUT);

		// Construct a new graph where the neighborhood is the vertex value
		Graph<Long, HashSet<Long>, NullValue> newGraph = graph
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
			implements EdgesFunction<Long, NullValue, Tuple2<Long, HashSet<Long>>> {

		@Override
		public Tuple2<Long, HashSet<Long>> iterateEdges(
				Iterable<Tuple2<Long, Edge<Long, NullValue>>> edges) throws Exception {

			Long vertexId = null;
			HashSet<Long> neighbors = new HashSet<Long>();

			for (Tuple2<Long, Edge<Long, NullValue>> edge : edges) {
				vertexId = edge.f0;
				neighbors.add(edge.f1.getTarget());
			}

			return new Tuple2<Long, HashSet<Long>>(vertexId, neighbors);
		}
	}

	private static final class EmptyVertexMapFunction
			implements MapFunction<Vertex<Long, NullValue>, HashSet<Long>> {

		@Override
		public HashSet<Long> map(Vertex<Long, NullValue> arg) throws Exception {
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
			implements NeighborsFunctionWithVertexValue<Long, HashSet<Long>, NullValue, Tuple2<Long, Double>> {

		@Override
		public Tuple2<Long, Double> iterateNeighbors(Vertex<Long, HashSet<Long>> vertex,
				Iterable<Tuple2<Edge<Long, NullValue>, Vertex<Long, HashSet<Long>>>> neighbors) throws Exception {

			int deg = vertex.getValue().size();
			int e = 0;

			// Calculate common neighbor count (e)
			for (Tuple2<Edge<Long, NullValue>, Vertex<Long, HashSet<Long>>> neighbor : neighbors) {
				// Iterate neighbor's neighbors
				for (Long nn : neighbor.f1.getValue()) {
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
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 2) {
				System.err.println("Usage: LocalClusteringCoefficient <edge path> <output path>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
		} else {
			System.out.println("Executing LocalClusteringCoefficient example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: LocalClusteringCoefficient <edge path> <output path>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, NullValue>> getEdgeDataSet(ExecutionEnvironment env) {

		if (fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.fieldDelimiter(" ")
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {
						@Override
						public Edge<Long, NullValue> map(Tuple2<Long, Long> value) throws Exception {
							return new Edge<Long, NullValue>(value.f0, value.f1, NullValue.getInstance());
						}
					});
		}

		return LocalClusteringCoefficientData.getDefaultEdgeDataSet(env);
	}

	@Override
	public String getDescription() {
		return "Local Clustering Coefficient Example";
	}
}
