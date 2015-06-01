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
import org.apache.flink.graph.library.PageRankAlgorithm;
import org.apache.flink.graph.utils.Tuple3ToEdgeMap;
import org.apache.flink.util.Collector;

/**
 * This example implements a simple PageRank algorithm, using a vertex-centric iteration.
 *
 * The edges input file is expected to contain one edge per line, with long IDs and double
 * values, in the following format:"<sourceVertexID>\t<targetVertexID>\t<edgeValue>".
 *
 * If no arguments are provided, the example runs with a random graph of 10 vertices
 * and random edge weights.
 *
 */
public class PageRank implements ProgramDescription {

	@SuppressWarnings("serial")
	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Edge<Long, Double>> links = getLinksDataSet(env);

		Graph<Long, Double, Double> network = Graph.fromDataSet(links, new MapFunction<Long, Double>() {

			public Double map(Long value) throws Exception {
				return 1.0;
			}
		}, env);

		DataSet<Tuple2<Long, Long>> vertexOutDegrees = network.outDegrees();

		// assign the transition probabilities as the edge weights
		Graph<Long, Double, Double> networkWithWeights = network
				.joinWithEdgesOnSource(vertexOutDegrees,
						new MapFunction<Tuple2<Double, Long>, Double>() {
							public Double map(Tuple2<Double, Long> value) {
								return value.f0 / value.f1;
							}
						});

		DataSet<Vertex<Long, Double>> pageRanks = networkWithWeights.run(
				new PageRankAlgorithm<Long>(DAMPENING_FACTOR, maxIterations))
				.getVertices();

		if (fileOutput) {
			pageRanks.writeAsCsv(outputPath, "\n", "\t");

			// since file sinks are lazy, we trigger the execution explicitly
			env.execute();
		} else {
			pageRanks.print();
		}

	}

	@Override
	public String getDescription() {
		return "PageRank example";
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
				System.err.println("Usage: PageRank <input edges path> <output path> <num iterations>");
				return false;
			}

			fileOutput = true;
			edgeInputPath = args[0];
			outputPath = args[1];
			maxIterations = Integer.parseInt(args[2]);
		} else {
			System.out.println("Executing PageRank example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: PageRank <input edges path> <output path> <num iterations>");
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
}
