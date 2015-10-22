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
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.example.utils.ExampleUtils;
import org.apache.flink.types.NullValue;

/**
 * This example illustrates how to use Gelly metrics methods and get simple statistics
 * from the input graph.  
 * 
 * The program creates a random graph and computes and prints
 * the following metrics:
 * - number of vertices
 * - number of edges
 * - average node degree
 * - the vertex ids with the max/min in- and out-degrees
 *
 * The input file is expected to contain one edge per line,
 * with long IDs and no values, in the following format:
 * "<sourceVertexID>\t<targetVertexID>".
 * If no arguments are provided, the example runs with a random graph of 100 vertices.
 *
 */
public class GraphMetrics implements ProgramDescription {

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		/** create the graph **/
		Graph<Long, NullValue, NullValue> graph = Graph.fromDataSet(getEdgesDataSet(env), env);
		
		/** get the number of vertices **/
		long numVertices = graph.numberOfVertices();
		
		/** get the number of edges **/
		long numEdges = graph.numberOfEdges();
		
		/** compute the average node degree **/
		DataSet<Tuple2<Long, Long>> verticesWithDegrees = graph.getDegrees();

		DataSet<Double> avgNodeDegree = verticesWithDegrees
				.aggregate(Aggregations.SUM, 1).map(new AvgNodeDegreeMapper(numVertices));
		
		/** find the vertex with the maximum in-degree **/
		DataSet<Long> maxInDegreeVertex = graph.inDegrees().maxBy(1).map(new ProjectVertexId());

		/** find the vertex with the minimum in-degree **/
		DataSet<Long> minInDegreeVertex = graph.inDegrees().minBy(1).map(new ProjectVertexId());

		/** find the vertex with the maximum out-degree **/
		DataSet<Long> maxOutDegreeVertex = graph.outDegrees().maxBy(1).map(new ProjectVertexId());

		/** find the vertex with the minimum out-degree **/
		DataSet<Long> minOutDegreeVertex = graph.outDegrees().minBy(1).map(new ProjectVertexId());
		
		/** print the results **/
		ExampleUtils.printResult(env.fromElements(numVertices), "Total number of vertices");
		ExampleUtils.printResult(env.fromElements(numEdges), "Total number of edges");
		ExampleUtils.printResult(avgNodeDegree, "Average node degree");
		ExampleUtils.printResult(maxInDegreeVertex, "Vertex with Max in-degree");
		ExampleUtils.printResult(minInDegreeVertex, "Vertex with Min in-degree");
		ExampleUtils.printResult(maxOutDegreeVertex, "Vertex with Max out-degree");
		ExampleUtils.printResult(minOutDegreeVertex, "Vertex with Min out-degree");

		env.execute();
	}

	@SuppressWarnings("serial")
	private static final class AvgNodeDegreeMapper implements MapFunction<Tuple2<Long, Long>, Double> {

		private long numberOfVertices;

		public AvgNodeDegreeMapper(long numberOfVertices) {
			this.numberOfVertices = numberOfVertices;
		}

		public Double map(Tuple2<Long, Long> sumTuple) {
			return (double) (sumTuple.f1 / numberOfVertices) ;
		}
	}

	@SuppressWarnings("serial")
	private static final class ProjectVertexId implements MapFunction<Tuple2<Long,Long>, Long> {
		public Long map(Tuple2<Long, Long> value) { return value.f0; }
	}

	@Override
	public String getDescription() {
		return "Graph Metrics Example";
	}

	// ******************************************************************************************************************
	// UTIL METHODS
	// ******************************************************************************************************************

	private static boolean fileOutput = false;

	private static String edgesInputPath = null;

	static final int NUM_VERTICES = 100;

	static final long SEED = 9876;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 1) {
				System.err.println("Usage: GraphMetrics <input edges>");
				return false;
			}

			fileOutput = true;
			edgesInputPath = args[0];
		} else {
			System.out.println("Executing Graph Metrics example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("Usage: GraphMetrics <input edges>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, NullValue>> getEdgesDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgesInputPath)
					.lineDelimiter("\n").fieldDelimiter("\t")
					.types(Long.class, Long.class).map(
							new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {

								public Edge<Long, NullValue> map(Tuple2<Long, Long> value) {
									return new Edge<Long, NullValue>(value.f0, value.f1, 
											NullValue.getInstance());
								}
					});
		} else {
			return ExampleUtils.getRandomEdges(env, NUM_VERTICES);
		}
	}
}