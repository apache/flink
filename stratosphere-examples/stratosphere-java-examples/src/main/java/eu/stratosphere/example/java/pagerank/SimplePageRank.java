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
package eu.stratosphere.example.java.pagerank;


import java.util.ArrayList;
import java.util.Iterator;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.functions.FlatMapFunction;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.util.Collector;

public class SimplePageRank {
	
	public static final class OutgoingEdgeCounter extends GroupReduceFunction<Tuple2<Long, Long>, Tuple2<Long, Long[]>> {

		private static final long serialVersionUID = 1L;
		
		@Override
		public void reduce(Iterator<Tuple2<Long, Long>> values,
				Collector<Tuple2<Long, Long[]>> out) throws Exception {

			long id = 0;
			ArrayList<Long> neighbors = new ArrayList<Long>();
			
			while(values.hasNext()) {
				Tuple2<Long, Long> n = values.next();
				id = n.f0;
				neighbors.add(n.f1);
			}
			
			out.collect(new Tuple2<Long, Long[]>(id, neighbors.toArray(new Long[neighbors.size()])));
		}

		
	}
	
	public static final class JoinVertexWithEdgesMatch extends FlatMapFunction<Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>>, Tuple2<Long, Double>> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(
				Tuple2<Tuple2<Long, Double>, Tuple2<Long, Long[]>> value,
				Collector<Tuple2<Long, Double>> out) throws Exception {

			double rankToDistribute = value.f0.f1 / (double) (value.f1.f1.length);
				
			for(int i = 0; i < value.f1.f1.length; i++) {
				out.collect(new Tuple2<Long, Double>(value.f1.f1[i], rankToDistribute));
			}
			
		}
	
	}
	
	public static final class AggregatingReduce extends ReduceFunction<Tuple2<Long, Double>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Double> reduce(Tuple2<Long, Double> rec1,
				Tuple2<Long, Double> rec2) throws Exception {

			double rankSum = rec1.f1;
			rankSum += rec2.f1;
			
			return new Tuple2<Long, Double>(rec1.f0, rankSum);
		}
		
	}
	
	public static final class JoinOldAndNew extends JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> first,
				Tuple2<Long, Double> second) {
			
			double epsilon = 0.05;
			double criterion = first.f1 - second.f1;
			
			if(Math.abs(criterion) > epsilon)
			{
				return second;
			}
			
			return null;
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		if (args.length < 5) {
			System.err.println("Usage: SimpePageRank <DOP> <pageWithRankInputPath> <adjacencyListInputPath> <outputPath> <numIterations>");
			return;
		}
		
		final int dop = Integer.parseInt(args[0]);
		final String pageWithRankInputPath = args[1];
		final String adjacencyListInputPath = args[2];
		final String outputPath = args[3];
		final int maxIterations = Integer.parseInt(args[4]);
		
		run(dop, pageWithRankInputPath, adjacencyListInputPath, outputPath, maxIterations, true);
		
	}
	
	public static void run(int dop, String pageWithRankInputPath, String adjacencyListInputPath, String outputPath, int maxIterations, boolean terminationCriterion) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(dop);
		
		// vertexID, initialRank
		DataSet<Tuple2<Long, Double>> pageWithRankInput = env.readCsvFile(pageWithRankInputPath).types(Long.class, Double.class);
		
		IterativeDataSet<Tuple2<Long, Double>> iteration = pageWithRankInput.iterate(maxIterations);
		
		// vertexID, vertexID[]
		DataSet<Tuple2<Long, Long[]>> adjacencyListInput = env.readCsvFile(adjacencyListInputPath).types(Long.class, Long.class)
				// vertexID, numNeighbors
				.groupBy(0).reduceGroup(new OutgoingEdgeCounter());
		
		DataSet<Tuple2<Long, Double>> iterationInner = iteration.join(adjacencyListInput).where(0).equalTo(0).flatMap(new JoinVertexWithEdgesMatch())
				.groupBy(0).reduce(new AggregatingReduce());
		
		DataSet<Tuple2<Long, Double>> termination =  iterationInner.join(iteration).where(0).equalTo(0).with(new JoinOldAndNew());
		
		iteration.closeWith(iterationInner, termination).writeAsCsv(outputPath);
		
		env.execute();
		
	}

}
