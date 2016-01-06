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

package org.apache.flink.tez.examples;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.examples.java.graph.util.PageRankData;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;

public class PageRankBasicStep {

	private static final double DAMPENING_FACTOR = 0.85;
	private static final double EPSILON = 0.0001;

	// *************************************************************************
	//     PROGRAM
	// *************************************************************************

	public static void main(String[] args) throws Exception {

		if(!parseParameters(args)) {
			return;
		}

		// set up execution environment
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		// get input data
		DataSet<Long> pagesInput = getPagesDataSet(env);
		DataSet<Tuple2<Long, Long>> linksInput = getLinksDataSet(env);

		// assign initial rank to pages
		DataSet<Tuple2<Long, Double>> pagesWithRanks = pagesInput.
				map(new RankAssigner((1.0d / numPages)));

		// build adjacency list from link input
		DataSet<Tuple2<Long, Long[]>> adjacencyListInput =
				linksInput.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());

		DataSet<Tuple2<Long, Double>> newRanks = pagesWithRanks
				.join(adjacencyListInput).where(0).equalTo(0)
				.flatMap(new JoinVertexWithEdgesMatch())
				.groupBy(0).aggregate(SUM, 1)
				.map(new Dampener(DAMPENING_FACTOR, numPages));


		// emit result
		if(fileOutput) {
			newRanks.writeAsCsv(outputPath, "\n", " ");
		} else {
			newRanks.print();
		}

		// execute program
		env.execute("Basic Page Rank Example");

	}

	// *************************************************************************
	//     USER FUNCTIONS
	// *************************************************************************

	/**
	 * A map function that assigns an initial rank to all pages.
	 */
	public static final class RankAssigner implements MapFunction<Long, Tuple2<Long, Double>> {
		Tuple2<Long, Double> outPageWithRank;

		public RankAssigner(double rank) {
			this.outPageWithRank = new Tuple2<Long, Double>(-1l, rank);
		}

		@Override
		public Tuple2<Long, Double> map(Long page) {
			outPageWithRank.f0 = page;
			return outPageWithRank;
		}
	}

	/**
	 * A reduce function that takes a sequence of edges and builds the adjacency list for the vertex where the edges
	 * originate. Run as a pre-processing step.
	 */
	@ForwardedFields("0")
	public static final class BuildOutgoingEdgeList implements GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

		private final ArrayList<Long> neighbors = new ArrayList<Long>();

		@Override
		public void reduce(Iterable<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) {
			neighbors.clear();
			Long id = 0L;

			for (Tuple2<Long, Long> n : values) {
				id = n.f0;
				neighbors.add(n.f1);
			}
			out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
		}
	}

	/**
	 * Join function that distributes a fraction of a vertex's rank to all neighbors.
	 */
	public static final class JoinVertexWithEdgesMatch implements FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>> {

		@Override
		public void flatMap(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value, Collector<Tuple2<Long, Double>> out){
			Long[] neigbors = value.f1.f1;
			double rank = value.f0.f1;
			double rankToDistribute = rank / ((double) neigbors.length);

			for (int i = 0; i < neigbors.length; i++) {
				out.collect(new Tuple2<Long, Double>(neigbors[i], rankToDistribute));
			}
		}
	}

	/**
	 * The function that applies the page rank dampening formula
	 */
	@ForwardedFields("0")
	public static final class Dampener implements MapFunction<Tuple2<Long,Double>, Tuple2<Long,Double>> {

		private final double dampening;
		private final double randomJump;

		public Dampener(double dampening, double numVertices) {
			this.dampening = dampening;
			this.randomJump = (1 - dampening) / numVertices;
		}

		@Override
		public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
			value.f1 = (value.f1 * dampening) + randomJump;
			return value;
		}
	}

	/**
	 * Filter that filters vertices where the rank difference is below a threshold.
	 */
	public static final class EpsilonFilter implements FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

		@Override
		public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) {
			return Math.abs(value.f0.f1 - value.f1.f1) > EPSILON;
		}
	}

	// *************************************************************************
	//     UTIL METHODS
	// *************************************************************************

	private static boolean fileOutput = false;
	private static String pagesInputPath = null;
	private static String linksInputPath = null;
	private static String outputPath = null;
	private static long numPages = 0;
	private static int maxIterations = 10;

	private static boolean parseParameters(String[] args) {

		if(args.length > 0) {
			if(args.length == 5) {
				fileOutput = true;
				pagesInputPath = args[0];
				linksInputPath = args[1];
				outputPath = args[2];
				numPages = Integer.parseInt(args[3]);
				maxIterations = Integer.parseInt(args[4]);
			} else {
				System.err.println("Usage: PageRankBasic <pages path> <links path> <output path> <num pages> <num iterations>");
				return false;
			}
		} else {
			System.out.println("Executing PageRank Basic example with default parameters and built-in default data.");
			System.out.println("  Provide parameters to read input data from files.");
			System.out.println("  See the documentation for the correct format of input files.");
			System.out.println("  Usage: PageRankBasic <pages path> <links path> <output path> <num pages> <num iterations>");

			numPages = PageRankData.getNumberOfPages();
		}
		return true;
	}

	private static DataSet<Long> getPagesDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			return env
					.readCsvFile(pagesInputPath)
					.fieldDelimiter(' ')
					.lineDelimiter("\n")
					.types(Long.class)
					.map(new MapFunction<Tuple1<Long>, Long>() {
						@Override
						public Long map(Tuple1<Long> v) { return v.f0; }
					});
		} else {
			return PageRankData.getDefaultPagesDataSet(env);
		}
	}

	private static DataSet<Tuple2<Long, Long>> getLinksDataSet(ExecutionEnvironment env) {
		if(fileOutput) {
			return env.readCsvFile(linksInputPath)
					.fieldDelimiter(' ')
					.lineDelimiter("\n")
					.types(Long.class, Long.class);
		} else {
			return PageRankData.getDefaultEdgeDataSet(env);
		}
	}
}
