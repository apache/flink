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
import org.apache.flink.graph.gsa.Neighbor;
import org.apache.flink.graph.gsa.SumFunction;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.util.Collector;

/**
 * This example implements a simple PageRank algorithm, using a gather-sum-apply iteration.
 *
 * The edges input file is expected to contain one edge per line, with long IDs and double
 * values, in the following format:"<sourceVertexID>\t<targetVertexID>\t<edgeValue>".
 *
 * If no arguments are provided, the example runs with a random graph of 10 vertices
 * and random edge weights.
 */
public class GSAPageRank implements ProgramDescription {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, Double>> links = getLinksDataSet(env);

		Graph<Long, Double, Double> network = Graph.fromDataSet(links, new MapFunction<Long, Double>() {

			@Override
			public Double map(Long value) throws Exception {
				return 1.0;
			}
		}, env);

		DataSet<Tuple2<Long, Long>> vertexOutDegrees = network.outDegrees();

		// Assign the transition probabilities as the edge weights
		Graph<Long, Double, Double> networkWithWeights = network
				.joinWithEdgesOnSource(vertexOutDegrees,
						new MapFunction<Tuple2<Double, Long>, Double>() {

							@Override
							public Double map(Tuple2<Double, Long> value) {
								return value.f0 / value.f1;
							}
						});

		long numberOfVertices = networkWithWeights.numberOfVertices();

		// Execute the GSA iteration
		Graph<Long, Double, Double> result = networkWithWeights
				.runGatherSumApplyIteration(new GatherRanks(numberOfVertices), new SumRanks(),
						new UpdateRanks(numberOfVertices), maxIterations);

		// Extract the vertices as the result
		DataSet<Vertex<Long, Double>> pageRanks = result.getVertices();

		// emit result
		if (fileOutput) {
			pageRanks.writeAsCsv(outputPath, "\n", "\t");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute("GSA Page Ranks");
		} else {
			pageRanks.print();
		}

	}

	// --------------------------------------------------------------------------------------------
	//  Page Rank UDFs
	// --------------------------------------------------------------------------------------------

	@SuppressWarnings("serial")
	private static final class GatherRanks extends GatherFunction<Double, Double, Double> {

		long numberOfVertices;

		public GatherRanks(long numberOfVertices) {
			this.numberOfVertices = numberOfVertices;
		}

		@Override
		public Double gather(Neighbor<Double, Double> neighbor) {
			double neighborRank = neighbor.getNeighborValue();

			if(getSuperstepNumber() == 1) {
				neighborRank = 1.0 / numberOfVertices;
			}

			return neighborRank * neighbor.getEdgeValue();
		}
	}

	@SuppressWarnings("serial")
	private static final class SumRanks extends SumFunction<Double, Double, Double> {

		@Override
		public Double sum(Double newValue, Double currentValue) {
			return newValue + currentValue;
		}
	}

	@SuppressWarnings("serial")
	private static final class UpdateRanks extends ApplyFunction<Long, Double, Double> {

		long numberOfVertices;

		public UpdateRanks(long numberOfVertices) {
			this.numberOfVertices = numberOfVertices;
		}

		@Override
		public void apply(Double rankSum, Double currentValue) {
			setResult((1-DAMPENING_FACTOR)/numberOfVertices + DAMPENING_FACTOR * rankSum);
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static final double DAMPENING_FACTOR = 0.85;
	private static long numPages = 10;
	private static String edgeInputPath = null;
	private static String outputPath = null;
	private static int maxIterations = 10;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length != 3) {
				System.err.println("Usage: GSAPageRank <input edges path> <output path> <num iterations>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
		} else {
			System.out.println("Executing GSAPageRank example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: GSAPageRank <input edges path> <output path> <num iterations>");
		}
		return true;
	}

	@SuppressWarnings("serial")
	private static DataSet<Edge<Long, Double>> getLinksDataSet(ExecutionEnvironment env) {

		if (fileOutput) {
			return env.readCsvFile(edgeInputPath)
					.fieldDelimiter("\t")
					.lineDelimiter("\n")
					.types(Long.class, Long.class, Double.class)
					.map(new Tuple3ToEdgeMap<Long, Double>());
		}

		return env.generateSequence(1, numPages).flatMap(
				new FlatMapFunction<Long, Edge<Long, Double>>() {
					@Override
					public void flatMap(Long key,
										Collector<Edge<Long, Double>> out) throws Exception {
						int numOutEdges = (int) (Math.random() * (numPages / 2));
						for (int i = 0; i < numOutEdges; i++) {
							long target = (long) (Math.random() * numPages) + 1;
							out.collect(new Edge<Long, Double>(key, target, 1.0));
						}
					}
				});
	}

	@Override
	public String getDescription() {
		return "GSA Page Rank";
	}
}
