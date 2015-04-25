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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.example.utils.SingleSourceShortestPathsData;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.util.Collector;

/**
 * This is an implementation of the Single Source Shortest Paths algorithm, using a gather-sum-apply iteration
 */
public class GSASingleSourceShortestPathsExample implements ProgramDescription {

	// --------------------------------------------------------------------------------------------
	//  Program
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, Double>> edges = getEdgeDataSet(env);
		DataSet<Vertex<Long, Double>> vertices = edges
				.flatMap(new InitVerticesMapper(srcVertexId))
				.distinct();

		Graph<Long, Double, Double> graph = Graph.fromDataSet(vertices, edges, env);

		// The path from src to trg through edge e costs src + e
		GatherFunction<Double, Double, Double> gather = new SingleSourceShortestPathGather();

		// Return the smaller path length to minimize distance
		SumFunction<Double, Double, Double> sum = new SingleSourceShortestPathSum();

		// Iterate as long as the distance is updated
		ApplyFunction<Double, Double, Double> apply = new SingleSourceShortestPathApply();

		// Execute the GSA iteration
		Graph<Long, Double, Double> result = graph
				.runGatherSumApplyIteration(gather, sum, apply, maxIterations);

		// Extract the vertices as the result
		DataSet<Vertex<Long, Double>> singleSourceShortestPaths = result.getVertices();

		// emit result
		if(fileOutput) {
			singleSourceShortestPaths.writeAsCsv(outputPath, "\n", " ");
		} else {
			singleSourceShortestPaths.print();
		}

		env.execute("GSA Single Source Shortest Paths Example");
	}

	private static final class InitVerticesMapper
			implements FlatMapFunction<Edge<Long, Double>, Vertex<Long, Double>>{

		private long srcVertexId;

		public InitVerticesMapper(long srcId) {
			this.srcVertexId = srcId;
		}

		@Override
		public void flatMap(Edge<Long, Double> edge, Collector<Vertex<Long, Double>> out) throws Exception {
			if (edge.getSource() == srcVertexId) {
				out.collect(new Vertex<Long, Double>(edge.getSource(), 0.0));
			} else {
				out.collect(new Vertex<Long, Double>(edge.getSource(), Double.POSITIVE_INFINITY));
			}

			if (edge.getTarget() == srcVertexId) {
				out.collect(new Vertex<Long, Double>(edge.getTarget(), 0.0));
			} else {
				out.collect(new Vertex<Long, Double>(edge.getTarget(), Double.POSITIVE_INFINITY));
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Single Source Shortest Path UDFs
	// --------------------------------------------------------------------------------------------

	private static final class SingleSourceShortestPathGather
			extends GatherFunction<Double, Double, Double> {
		@Override
		public Double gather(Neighbor<Double, Double> richEdge) {
			return richEdge.getSrcVertexValue() + richEdge.getEdgeValue();
		}
	};

	private static final class SingleSourceShortestPathSum
			extends SumFunction<Double, Double, Double> {
		@Override
		public Double sum(Double newValue, Double currentValue) {
			return Math.min(newValue, currentValue);
		}
	};

	private static final class SingleSourceShortestPathApply
			extends ApplyFunction<Double, Double, Double> {
		@Override
		public void apply(Double summed, Double target) {
			if (summed < target) {
				setResult(summed);
			}
		}
	};

	// --------------------------------------------------------------------------------------------
	//  Util methods
	// --------------------------------------------------------------------------------------------

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static int maxIterations = 2;
	private static long srcVertexId = 1;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;

			if (args.length != 4) {
				System.err.println("Usage: GSASingleSourceShortestPathsExample <edge path> " +
						"<result path> <src vertex> <max iterations>");
				return false;
			}

			edgeInputPath = args[0];
			outputPath = args[1];
			srcVertexId = Long.parseLong(args[2]);
			maxIterations = Integer.parseInt(args[3]);
		} else {
			System.out.println("Executing GSA Single Source Shortest Paths example with built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: GSASingleSourceShortestPathsExample <edge path> <result path> <src vertex> "
					+ "<max iterations>");
		}
		return true;
	}

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
		} else {
			return SingleSourceShortestPathsData.getDefaultEdgeDataSet(env);
		}
	}

	@Override
	public String getDescription() {
		return "GSA Single Source Shortest Paths";
	}
}
