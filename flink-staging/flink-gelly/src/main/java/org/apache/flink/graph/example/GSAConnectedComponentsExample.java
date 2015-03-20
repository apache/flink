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
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.gsa.ApplyFunction;
import org.apache.flink.graph.gsa.GatherFunction;
import org.apache.flink.graph.gsa.GatherSumApplyIteration;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.gsa.RichEdge;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * This is an implementation of the connected components algorithm, using a gather-sum-apply iteration
 */
public class GSAConnectedComponentsExample implements ProgramDescription {

	// --------------------------------------------------------------------------------------------
	//  Program
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Vertex<Long, Long>> vertices = getVertexDataSet(env);
		DataSet<Edge<Long, NullValue>> edges = getEdgeDataSet(env);

		Graph<Long, Long, NullValue> graph = Graph.fromDataSet(vertices, edges, env);

		// Simply return the vertex value of each vertex
		GatherFunction<Long, NullValue, Long> gather = new ConnectedComponentsGather();

		// Select the lower value among neighbors
		SumFunction<Long, NullValue, Long> sum = new ConnectedComponentsSum();

		// Set the lower value for each vertex
		ApplyFunction<Long, NullValue, Long> apply = new ConnectedComponentsApply();

		// Execute the GSA iteration
		GatherSumApplyIteration<Long, Long, NullValue, Long> iteration =
				graph.createGatherSumApplyIteration(gather, sum, apply, maxIterations);
		Graph<Long, Long, NullValue> result = graph.runGatherSumApplyIteration(iteration);

		// Extract the vertices as the result
		DataSet<Vertex<Long, Long>> greedyGraphColoring = result.getVertices();

		// emit result
		if (fileOutput) {
			greedyGraphColoring.writeAsCsv(outputPath, "\n", " ");
		} else {
			greedyGraphColoring.print();
		}

		env.execute("GSA Connected Components");
	}

	// --------------------------------------------------------------------------------------------
	//  Connected Components UDFs
	// --------------------------------------------------------------------------------------------

	private static final class ConnectedComponentsGather
			extends GatherFunction<Long, NullValue, Long> {
		@Override
		public Long gather(RichEdge<Long, NullValue> richEdge) {

			return richEdge.getSrcVertexValue();
		}
	};

	private static final class ConnectedComponentsSum
			extends SumFunction<Long, NullValue, Long> {
		@Override
		public Long sum(Long newValue, Long currentValue) {

			return Math.min(newValue, currentValue);
		}
	};

	private static final class ConnectedComponentsApply
			extends ApplyFunction<Long, NullValue, Long> {
		@Override
		public void apply(Long summedValue, Long origValue) {

			if (summedValue < origValue) {
				setResult(summedValue);
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
				System.err.println("Usage: GSAConnectedComponentsExample <vertex path> <edge path> " +
						"<result path> <max iterations>");
				return false;
			}

			vertexInputPath = args[0];
			edgeInputPath = args[1];
			outputPath = args[2];
			maxIterations = Integer.parseInt(args[3]);
		} else {
			System.out.println("Executing GSA Connected Components example with built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: GSAConnectedComponentsExample <vertex path> <edge path> "
					+ "<result path> <max iterations>");
		}
		return true;
	}

	private static DataSet<Vertex<Long, Long>> getVertexDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			return env
					.readCsvFile(vertexInputPath)
					.fieldDelimiter(" ")
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Vertex<Long, Long>>() {
						@Override
						public Vertex<Long, Long> map(Tuple2<Long, Long> value) throws Exception {
							return new Vertex<Long, Long>(value.f0, value.f1);
						}
					});
		}

		return env.generateSequence(0, 5).map(new MapFunction<Long, Vertex<Long, Long>>() {
			@Override
			public Vertex<Long, Long> map(Long value) throws Exception {
				return new Vertex<Long, Long>(value, value);
			}
		});
	}

	private static DataSet<Edge<Long, NullValue>> getEdgeDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
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

		// Generates 3 components of size 2
		return env.generateSequence(0, 2).flatMap(new FlatMapFunction<Long, Edge<Long, NullValue>>() {
			@Override
			public void flatMap(Long value, Collector<Edge<Long, NullValue>> out) throws Exception {
				out.collect(new Edge<Long, NullValue>(value, value + 3, NullValue.getInstance()));
			}
		});
	}

	@Override
	public String getDescription() {
		return "GSA Greedy Graph Coloring";
	}

}
