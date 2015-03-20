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
import org.apache.flink.graph.example.utils.SimpleCommunityDetectionData;
import org.apache.flink.graph.library.SimpleCommunityDetection;

/**
 * This example shows how to use the {@link org.apache.flink.graph.library.SimpleCommunityDetection}
 * library method:
 * <ul>
 * 	<li> with the edge data set given as a parameter
 * 	<li> with default data
 * </ul>
 */
public class SimpleCommunityDetectionExample implements ProgramDescription {

	public static void main(String [] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		// set up the execution environment
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// set up the graph
		DataSet<Edge<Long, Double>> edges = getEdgesDataSet(env);
		DataSet<Vertex<Long, Tuple2<Long, Double>>> vertices = assignInitialVertexValues(edges);
		Graph<Long, Tuple2<Long, Double>, Double> graph = Graph.fromDataSet(vertices, edges, env);

		// set up the program
		Graph<Long, Tuple2<Long, Double>, Double> communityGraph =
				graph.run(new SimpleCommunityDetection(maxIterations));

		// the result is in the form of <vertexId, communityId>, where the communityId is the label
		// which the vertex converged to
		DataSet<Vertex<Long, Tuple2<Long, Double>>> communityVertices = communityGraph.getVertices();

		DataSet<Tuple2<Long, Long>> result = communityVertices
				.map(new MapFunction<Vertex<Long, Tuple2<Long, Double>>, Tuple2<Long, Long>>() {

					@Override
					public Tuple2<Long, Long> map(Vertex<Long, Tuple2<Long, Double>> vertex) throws Exception {
						return new Tuple2<Long, Long>(vertex.f0, vertex.f1.f0);
					}
				});

		// emit result
		if (fileOutput) {
			result.writeAsCsv(outputPath, "\n", ",");
		} else {
			result.print();
		}

		env.execute("Executing Simple Community Detection Example");
	}

	@Override
	public String getDescription() {
		return "Simple Community Detection Example";
	}

	// *************************************************************************
	// UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static Integer maxIterations = SimpleCommunityDetectionData.MAX_ITERATIONS;

	private static boolean parseParameters(String [] args) {
		if(args.length > 0) {
			if(args.length != 3) {
				System.err.println("Usage SimpleCommunityDetection <edge path> <output path> " +
						"<num iterations>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);

		} else {
			System.out.println("Executing SimpleCommunityDetection example with default parameters and built-in default data.");
			System.out.println("Provide parameters to read input data from files.");
			System.out.println("Usage SimpleCommunityDetection <edge path> <output path> " +
					"<num iterations>");
		}

		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, Double>> getEdgesDataSet(ExecutionEnvironment env) {

		if(fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.ignoreComments("#")
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Long.class, Long.class, Double.class)
					.map(new MapFunction<Tuple3<Long, Long, Double>, Edge<Long, Double>>() {

						@Override
						public Edge<Long, Double> map(Tuple3<Long, Long, Double> tuple3) throws Exception {
							return new Edge<Long, Double>(tuple3.f0, tuple3.f1, tuple3.f2);
						}
					});
		} else {
			return SimpleCommunityDetectionData.getDefaultEdgeDataSet(env);
		}
	}

	private static DataSet<Vertex<Long, Tuple2<Long, Double>>> assignInitialVertexValues(DataSet<Edge<Long, Double>> edges) {
		return edges.map(new MapFunction<Edge<Long, Double>, Vertex<Long, Tuple2<Long, Double>>>() {

			@Override
			public Vertex<Long, Tuple2<Long, Double>> map(Edge<Long, Double> edge) throws Exception {
				return new Vertex<Long, Tuple2<Long, Double>>(edge.getSource(), new Tuple2<Long, Double>(edge.getSource(),
						1.0));
			}
		}).union(edges.map(new MapFunction<Edge<Long, Double>, Vertex<Long, Tuple2<Long, Double>>>() {

			@Override
			public Vertex<Long, Tuple2<Long, Double>> map(Edge<Long, Double> edge) throws Exception {
				return new Vertex<Long, Tuple2<Long, Double>>(edge.getTarget(), new Tuple2<Long, Double>(edge.getTarget(),
						1.0));
			}
		})).distinct();
	}
}
