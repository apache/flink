/***********************************************************************************************************************
 *
 * Copyright (C) 2010-2013 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/
package eu.stratosphere.example.java.graph;

import static eu.stratosphere.api.java.aggregation.Aggregations.SUM;

import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.functions.FilterFunction;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;

@SuppressWarnings("serial")
public class SimplePageRank {
	
	private static final double DAMPENING_FACTOR = 0.85;
	
	private static final double EPSILON = 0.0001;
	
	
	public static void main(String[] args) throws Exception {
		
		if (args.length < 5) {
			System.err.println("Usage: SimpePageRank <vertex with initial rank input> <edges path> <output path> <num vertices> <num iterations>");
			return;
		}
		
		final String pageWithRankInputPath = args[0];
		final String adjacencyListInputPath = args[1];
		final String outputPath = args[2];
		final int numVertices = Integer.parseInt(args[3]);
		final int maxIterations = Integer.parseInt(args[4]);
		
		runPageRank(pageWithRankInputPath, adjacencyListInputPath, outputPath, numVertices, maxIterations);
		
	}
	
	public static void runPageRank(String verticesPath, String edgesPath, String outputPath, int numVertices, int maxIterations) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		// vertices: vertexID, initialRank
		DataSet<Tuple2<Long, Double>> pageWithRankInput = env.readCsvFile(verticesPath).types(Long.class, Double.class);

		// edge adjacency list: vertexID, vertexID[]
		DataSet<Tuple2<Long, Long[]>> adjacencyListInput = env.readCsvFile(edgesPath).types(Long.class, Long.class)
				.groupBy(0).reduceGroup(new OutgoingEdgeCounter());
		
		IterativeDataSet<Tuple2<Long, Double>> iteration = pageWithRankInput.iterate(maxIterations);
		
		DataSet<Tuple2<Long, Double>> newRanks = iteration
				.join(adjacencyListInput).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch())
				.groupBy(0).aggregate(SUM, 1)
				.map(new Dampener(numVertices));
		
		iteration.closeWith(newRanks,
							newRanks.join(iteration).where(0).equalTo(0).filter(new EpsilonFilter())) // termination condition
				.writeAsCsv(outputPath);
		
		env.execute();
	}
	
	
	// --------------------------------------------------------------------------------------------
	// The user-defined functions for parts of page rank
	// --------------------------------------------------------------------------------------------
	
	/**
	 * A reduce function that takes a sequence of edges and builds the adjacency list for the vertex where the edges
	 * originate. Run as a preprocessing step.
	 */
	public static final class OutgoingEdgeCounter extends GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {
		
		private final ArrayList<Long> neighbors = new ArrayList<Long>();
		
		@Override
		public void reduce(Iterator<Tuple2<Long, Long>> values, Collector<Tuple2<Long, Long[]>> out) {
			neighbors.clear();
			Long id = 0L;
			
			while (values.hasNext()) {
				Tuple2<Long, Long> n = values.next();
				id = n.f0;
				neighbors.add(n.f1);
			}
			out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
		}
	}
	
	/**
	 * Join function that distributes a fraction of a vertex's rank to all neighbors.
	 */
	public static final class JoinVertexWithEdgesMatch extends FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>> {

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
	public static final class Dampener extends MapFunction<Tuple2<Long,Double>, Tuple2<Long,Double>> {
		
		private final double numVertices;
		
		public Dampener(double numVertices) {
			this.numVertices = numVertices;
		}

		@Override
		public Tuple2<Long, Double> map(Tuple2<Long, Double> value) {
			value.f1 = DAMPENING_FACTOR*value.f1 + (1-DAMPENING_FACTOR)/numVertices;
			return value;
		}
	}
	
	/**
	 * Filter that filters vertices where the rank difference is below a threshold.
	 */
	public static final class EpsilonFilter extends FilterFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>>> {

		@Override
		public boolean filter(Tuple2<Tuple2<Long, Double>, Tuple2<Long, Double>> value) {
			return Math.abs(value.f0.f1 - value.f1.f1) > EPSILON;
		}
	}
}
