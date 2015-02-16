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
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.GatherSumApplyIteration;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.gsa.RichEdge;
import org.apache.flink.util.Collector;

import java.util.HashSet;

/**
 * This is an implementation of the Greedy Graph Coloring algorithm, using a gather-sum-apply iteration
 */
public class GSAGreedyGraphColoringExample implements ProgramDescription {

	// --------------------------------------------------------------------------------------------
	//  Program
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Vertex<Long, Double>> vertices = getVertexDataSet(env);
		DataSet<Edge<Long, Double>> edges = getEdgeDataSet(env);

		Graph<Long, Double, Double> graph = Graph.fromDataSet(vertices, edges, env);

		// Gather the target vertices into a one-element set
		GatherFunction<Double, Double, HashSet<Double>> gather = new GreedyGraphColoringGather();

		// Merge the sets between neighbors
		SumFunction<Double, Double, HashSet<Double>> sum = new GreedyGraphColoringSum();

		// Find the minimum vertex id in the set which will be propagated
		ApplyFunction<Double, Double, HashSet<Double>> apply = new GreedyGraphColoringApply();

		// Execute the GSA iteration
		GatherSumApplyIteration<Long, Double, Double, HashSet<Double>> iteration =
				graph.createGatherSumApplyIteration(gather, sum, apply, maxIterations);
		Graph<Long, Double, Double> result = graph.runGatherSumApplyIteration(iteration);

		// Extract the vertices as the result
		DataSet<Vertex<Long, Double>> greedyGraphColoring = result.getVertices();

		// emit result
		if (fileOutput) {
			greedyGraphColoring.writeAsCsv(outputPath, "\n", " ");
		} else {
			greedyGraphColoring.print();
		}

		env.execute("GSA Greedy Graph Coloring");
	}

	// --------------------------------------------------------------------------------------------
	//  Greedy Graph Coloring UDFs
	// --------------------------------------------------------------------------------------------

	private static final class GreedyGraphColoringGather
			extends GatherFunction<Double, Double, HashSet<Double>> {
		@Override
		public HashSet<Double> gather(RichEdge<Double, Double> richEdge) {

			HashSet<Double> result = new HashSet<Double>();
			result.add(richEdge.getSrcVertexValue());

			return result;
		}
	};

	private static final class GreedyGraphColoringSum
			extends SumFunction<Double, Double, HashSet<Double>> {
		@Override
		public HashSet<Double> sum(HashSet<Double> newValue, HashSet<Double> currentValue) {

			HashSet<Double> result = new HashSet<Double>();
			result.addAll(newValue);
			result.addAll(currentValue);

			return result;
		}
	};

	private static final class GreedyGraphColoringApply
			extends ApplyFunction<Double, Double, HashSet<Double>> {
		@Override
		public void apply(HashSet<Double> set, Double src) {
			double minValue = src;
			for (Double d : set) {
				if (d < minValue) {
					minValue = d;
				}
			}

			// This is the condition that enables the termination of the iteration
			if (minValue < src) {
				setResult(minValue);
			}
		}
	};

	// --------------------------------------------------------------------------------------------
	//  Util methods
	// --------------------------------------------------------------------------------------------

	private static boolean fileOutput = false;
	private static String vertexInputPath = null;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static int maxIterations = 16;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			// parse input arguments
			fileOutput = true;

			if(args.length != 4) {
				System.err.println("Usage: GSAGreedyGraphColoringExample <vertex path> <edge path> " +
						"<result path> <max iterations>");
				return false;
			}

			vertexInputPath = args[0];
			edgeInputPath = args[1];
			outputPath = args[2];
			maxIterations = Integer.parseInt(args[3]);
		} else {
			System.out.println("Executing GSA Greedy Graph Coloring example with built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: GSAGreedyGraphColoringExample <vertex path> <edge path> "
					+ "<result path> <max iterations>");
		}
		return true;
	}

	private static DataSet<Vertex<Long, Double>> getVertexDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			return env
					.readCsvFile(vertexInputPath)
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

		return env.generateSequence(0, 5).map(new MapFunction<Long, Vertex<Long, Double>>() {
			@Override
			public Vertex<Long, Double> map(Long value) throws Exception {
				return new Vertex<Long, Double>(value, (double) value);
			}
		});
	}

	private static DataSet<Edge<Long, Double>> getEdgeDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
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

		return env.generateSequence(0, 5).flatMap(new FlatMapFunction<Long, Edge<Long, Double>>() {
			@Override
			public void flatMap(Long value, Collector<Edge<Long, Double>> out) throws Exception {
				out.collect(new Edge<Long, Double>(value, (value + 1) % 6, 0.0));
				out.collect(new Edge<Long, Double>(value, (value + 2) % 6, 0.0));
			}
		});
	}

	@Override
	public String getDescription() {
		return "GSA Greedy Graph Coloring";
	}

}
