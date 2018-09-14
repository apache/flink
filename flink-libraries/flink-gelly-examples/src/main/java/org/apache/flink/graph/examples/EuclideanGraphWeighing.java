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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.EdgeJoinFunction;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Triplet;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.examples.data.EuclideanGraphData;

import java.io.Serializable;

/**
 * This example shows how to use Gelly's {@link Graph#getTriplets()} and
 * {@link Graph#joinWithEdges(DataSet, EdgeJoinFunction)} methods.
 *
 * <p>Given a directed, unweighted graph, with vertex values representing points in a plain,
 * return a weighted graph where the edge weights are equal to the Euclidean distance between the
 * src and the trg vertex values.
 *
 * <p>Input files are plain text files and must be formatted as follows:
 * <ul>
 * 	<li> Vertices are represented by their vertexIds and vertex values and are separated by newlines,
 * 	the value being formed of two doubles separated by a comma.
 * 	For example: <code>1,1.0,1.0\n2,2.0,2.0\n3,3.0,3.0\n</code> defines a data set of three vertices
 * 	<li> Edges are represented by pairs of srcVertexId, trgVertexId separated by commas.
 * 	Edges themselves are separated by newlines.
 * 	For example: <code>1,2\n1,3\n</code> defines two edges 1-2 and 1-3.
 * </ul>
 *
 * <p>Usage <code>EuclideanGraphWeighing &lt;vertex path&gt; &lt;edge path&gt; &lt;result path&gt;</code><br>
 * If no parameters are provided, the program is run with default data from
 * {@link EuclideanGraphData}
 */
@SuppressWarnings("serial")
public class EuclideanGraphWeighing implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Vertex<Long, Point>> vertices = getVerticesDataSet(env);

		DataSet<Edge<Long, Double>> edges = getEdgesDataSet(env);

		Graph<Long, Point, Double> graph = Graph.fromDataSet(vertices, edges, env);

		// the edge value will be the Euclidean distance between its src and trg vertex
		DataSet<Tuple3<Long, Long, Double>> edgesWithEuclideanWeight = graph.getTriplets()
				.map(new MapFunction<Triplet<Long, Point, Double>, Tuple3<Long, Long, Double>>() {

					@Override
					public Tuple3<Long, Long, Double> map(Triplet<Long, Point, Double> triplet)
							throws Exception {

						Vertex<Long, Point> srcVertex = triplet.getSrcVertex();
						Vertex<Long, Point> trgVertex = triplet.getTrgVertex();

						return new Tuple3<>(srcVertex.getId(), trgVertex.getId(),
							srcVertex.getValue().euclideanDistance(trgVertex.getValue()));
					}
				});

		Graph<Long, Point, Double> resultedGraph = graph.joinWithEdges(edgesWithEuclideanWeight,
				new EdgeJoinFunction<Double, Double>() {

					public Double edgeJoin(Double edgeValue, Double inputValue) {
						return inputValue;
					}
				});

		// retrieve the edges from the final result
		DataSet<Edge<Long, Double>> result = resultedGraph.getEdges();

		// emit result
		if (fileOutput) {
			result.writeAsCsv(outputPath, "\n", ",");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("Euclidean Graph Weighing Example");
		} else {
			result.print();
		}

	}

	@Override
	public String getDescription() {
		return "Weighing a graph by computing the Euclidean distance " +
				"between its vertices";
	}

	// *************************************************************************
	//     DATA TYPES
	// *************************************************************************

	/**
	 * A simple two-dimensional point.
	 */
	public static class Point implements Serializable {

		public double x, y;

		public Point() {}

		public Point(double x, double y) {
			this.x = x;
			this.y = y;
		}

		public double euclideanDistance(Point other) {
			return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
		}

		@Override
		public String toString() {
			return x + " " + y;
		}
	}

	// ******************************************************************************************************************
	// UTIL METHODS
	// ******************************************************************************************************************

	private static boolean fileOutput = false;

	private static String verticesInputPath = null;

	private static String edgesInputPath = null;

	private static String outputPath = null;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			if (args.length == 3) {
				fileOutput = true;
				verticesInputPath = args[0];
				edgesInputPath = args[1];
				outputPath = args[2];
			} else {
				System.out.println("Executing Euclidean Graph Weighing example with default parameters and built-in default data.");
				System.out.println("Provide parameters to read input data from files.");
				System.out.println("See the documentation for the correct format of input files.");
				System.err.println("Usage: EuclideanGraphWeighing <input vertices path> <input edges path>" +
						" <output path>");
				return false;
			}
		}
		return true;
	}

	private static DataSet<Vertex<Long, Point>> getVerticesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(verticesInputPath)
					.lineDelimiter("\n")
					.types(Long.class, Double.class, Double.class)
					.map(new MapFunction<Tuple3<Long, Double, Double>, Vertex<Long, Point>>() {

						@Override
						public Vertex<Long, Point> map(Tuple3<Long, Double, Double> value) throws Exception {
							return new Vertex<>(value.f0, new Point(value.f1, value.f2));
						}
					});
		} else {
			return EuclideanGraphData.getDefaultVertexDataSet(env);
		}
	}

	private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgesInputPath)
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, Double>>() {

						@Override
						public Edge<Long, Double> map(Tuple2<Long, Long> tuple2) throws Exception {
							return new Edge<>(tuple2.f0, tuple2.f1, 0.0);
						}
					});
		} else {
			return EuclideanGraphData.getDefaultEdgeDataSet(env);
		}
	}
}
