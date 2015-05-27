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
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

/**
 * This is an implementation of the Connected Components algorithm, using a gather-sum-apply iteration
 */
public class GSAConnectedComponents implements ProgramDescription {

	// --------------------------------------------------------------------------------------------
	//  Program
	// --------------------------------------------------------------------------------------------

	public static void main(String[] args) throws Exception {

		if (!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, NullValue>> edges = getEdgeDataSet(env);

		Graph<Long, Long, NullValue> graph = Graph.fromDataSet(edges, new InitVertices(), env);

		// Execute the GSA iteration
		Graph<Long, Long, NullValue> result =
				graph.runGatherSumApplyIteration(new GatherNeighborIds(), new SelectMinId(),
						new UpdateComponentId(), maxIterations);

		// Extract the vertices as the result
		DataSet<Vertex<Long, Long>> connectedComponents = result.getVertices();

		// emit result
		if (fileOutput) {
			connectedComponents.writeAsCsv(outputPath, "\n", " ");
		} else {
			connectedComponents.print();
		}

		env.execute("GSA Connected Components");
	}

	@SuppressWarnings("serial")
	private static final class InitVertices	implements MapFunction<Long, Long> {

		public Long map(Long vertexId) {
			return vertexId;
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Connected Components UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class GatherNeighborIds extends GatherFunction<Long, NullValue, Long> {

		public Long gather(Neighbor<Long, NullValue> neighbor) {
			return neighbor.getNeighborValue();
		}
	};

	@SuppressWarnings("serial")
	private static final class SelectMinId extends SumFunction<Long, NullValue, Long> {

		public Long sum(Long newValue, Long currentValue) {
			return Math.min(newValue, currentValue);
		}
	};

	@SuppressWarnings("serial")
	private static final class UpdateComponentId extends ApplyFunction<Long, Long, Long> {

		public void apply(Long summedValue, Long origValue) {
			if (summedValue < origValue) {
				setResult(summedValue);
			}
		}
	}

	// --------------------------------------------------------------------------------------------
	//  Util methods
	// --------------------------------------------------------------------------------------------

	private static boolean fileOutput = false;
	private static String edgeInputPath = null;
	private static String outputPath = null;

	private static int maxIterations = 16;

	private static boolean parseParameters(String[] args) {

		if (args.length > 0) {
			// parse input arguments
			fileOutput = true;

			if (args.length != 3) {
				System.err.println("Usage: GSAConnectedComponents <edge path> " +
						"<result path> <max iterations>");
				return false;
			}

			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
		} else {
			System.out.println("Executing GSA Connected Components example with built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: GSAConnectedComponents <edge path> <result path> <max iterations>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, NullValue>> getEdgeDataSet(ExecutionEnvironment env) {
		if (fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Long.class, Long.class)
					.map(new MapFunction<Tuple2<Long, Long>, Edge<Long, NullValue>>() {

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
		return "GSA Connected Components";
	}
}
