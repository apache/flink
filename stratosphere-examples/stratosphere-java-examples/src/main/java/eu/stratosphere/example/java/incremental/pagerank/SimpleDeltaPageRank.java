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
package eu.stratosphere.example.java.incremental.pagerank;


import java.util.Iterator;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIterativeDataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.util.Collector;

public class SimpleDeltaPageRank {
	
	public static final class RankComparisonMatch extends JoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {
		
		private static final long serialVersionUID = 1L;
		
		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> vertexWithDelta,
				Tuple2<Long, Double> vertexWithOldRank) throws Exception {
			
			return new Tuple2<Long, Double>(vertexWithOldRank.f0, vertexWithDelta.f1 + vertexWithOldRank.f1);
		}
	}
	
//	public static final class UpdateRankReduceDelta extends ReduceFunction<Tuple2<Long, Double>> {
//		
//		private static final long serialVersionUID = 1L;
//		
//		@Override
//		public Tuple2<Long, Double> reduce(Tuple2<Long, Double> value1,
//				Tuple2<Long, Double> value2) throws Exception {
//			
//			double rankSum = value1.f1 + value2.f1;
//			
//			// ignore small deltas
//			if (Math.abs(rankSum) > 0.00001) {
//				return new Tuple2<Long, Double>(value1.f0, rankSum);
//			}
//			return null;
//		}
//	}
	
	public static final class UpdateRankReduceDelta extends GroupReduceFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {
		
		private static final long serialVersionUID = 1L;

		@Override
		public void reduce(Iterator<Tuple2<Long, Double>> values,
				Collector<Tuple2<Long, Double>> out) throws Exception {

			double rankSum = 0.0;
			Tuple2<Long, Double> currentTuple = null;

			while (values.hasNext()) {
				currentTuple = values.next();
				rankSum += currentTuple.f1;
			}
			
			// ignore small deltas
			if (Math.abs(rankSum) > 0.00001) {
				out.collect(new Tuple2<Long, Double>(currentTuple.f0, rankSum));
			}
		}
	}
	
	
	public static final class PRDependenciesComputationMatchDelta extends JoinFunction<Tuple2<Long, Double>, Tuple3<Long, Long, Long>, Tuple2<Long, Double>> {

		private static final long serialVersionUID = 1L;

		/*
		 * (vId, rank) x (srcId, trgId, weight) => (trgId, rank / weight)
		 */
		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> vertexWithRank,
				Tuple3<Long, Long, Long> edgeWithWeight) throws Exception {

			return new Tuple2<Long, Double>(edgeWithWeight.f1, (Double) (vertexWithRank.f1 / edgeWithWeight.f2));
		}
	}
	
	public static final class Mapper extends MapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

		private static final long serialVersionUID = 1L;

		@Override
		public Tuple2<Long, Double> map(Tuple2<Long, Double> value)
				throws Exception {
			
			return value;
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		
		if (args.length < 5) {
			System.err.println("Usage: SimpePageRank <DOP> <pageWithRankInputPath> <adjacencyListInputPath> <outputPath> <numIterations>");
			return;
		}
		
		final int dop = Integer.parseInt(args[0]);
		final String solutionSetInputPath = args[1];
		final String deltasInputPath = args[2];
		final String dependencySetInputPath = args[3];
		final String outputPath = args[4];
		final int maxIterations = Integer.parseInt(args[5]);
		
		run(dop, solutionSetInputPath, deltasInputPath, dependencySetInputPath, outputPath, maxIterations, true);
		
	}
	
	public static void run(int dop, String solutionSetInputPath, String deltasInputPath, String dependencySetInputPath, String outputPath, int maxIterations, boolean terminationCriterion) throws Exception {
		
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(dop);
		
		DataSet<Tuple2<Long, Double>> initialSolutionSet = env.readCsvFile(solutionSetInputPath).fieldDelimiter(' ').types(Long.class, Double.class).map(new Mapper());
		
		DataSet<Tuple2<Long, Double>> initialDeltaSet = env.readCsvFile(deltasInputPath).fieldDelimiter(' ').types(Long.class, Double.class);
		
		DataSet<Tuple3<Long, Long, Long>> dependencySetInput = env.readCsvFile(dependencySetInputPath).fieldDelimiter(' ').types(Long.class, Long.class, Long.class);
		
		int keyPosition = 0;
		DeltaIterativeDataSet<Tuple2<Long, Double>, Tuple2<Long, Double>> iteration = initialSolutionSet.iterateDelta(initialDeltaSet, maxIterations, keyPosition);
		
		DataSet<Tuple2<Long, Double>> updateRanks = iteration.join(dependencySetInput).where(0).equalTo(0).with(new PRDependenciesComputationMatchDelta())
				.groupBy(0).reduceGroup(new UpdateRankReduceDelta());
		
		DataSet<Tuple2<Long, Double>> oldRankComparison = updateRanks.join(iteration.getSolutionSet()).where(0).equalTo(0).with(new RankComparisonMatch());
		
		iteration.closeWith(oldRankComparison, updateRanks).writeAsCsv(outputPath);
		
		env.execute();
		
	}

}
