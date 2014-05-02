/***********************************************************************************************************************
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
 **********************************************************************************************************************/

package eu.stratosphere.example.java.record.incremental.pagerank;

import java.util.Iterator;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.common.operators.DeltaIteration;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.java.record.functions.FunctionAnnotation.ConstantFieldsSecond;
import eu.stratosphere.api.java.record.functions.JoinFunction;
import eu.stratosphere.api.java.record.functions.ReduceFunction;
import eu.stratosphere.api.java.record.io.CsvInputFormat;
import eu.stratosphere.api.java.record.io.CsvOutputFormat;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.api.java.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.types.DoubleValue;
import eu.stratosphere.types.LongValue;
import eu.stratosphere.types.Record;
import eu.stratosphere.util.Collector;

public class DeltaPageRankWithInitialDeltas implements Program, ProgramDescription {

	private static final long serialVersionUID = 1L;


	@ConstantFieldsSecond(0)
	public static final class RankComparisonMatch extends JoinFunction {
		
		private static final long serialVersionUID = 1L;
		
		private final DoubleValue newRank = new DoubleValue();
		
		@Override
		public void join(Record vertexWithDelta, Record vertexWithOldRank, Collector<Record> out) throws Exception {			
			DoubleValue deltaVal = vertexWithDelta.getField(1, DoubleValue.class);
			DoubleValue currentVal = vertexWithOldRank.getField(1, DoubleValue.class);
			
			newRank.setValue(deltaVal.getValue() + currentVal.getValue());
			vertexWithOldRank.setField(1, newRank);
			
			out.collect(vertexWithOldRank);
		}
	}
	
	@Combinable
	@ConstantFields(0)
	public static final class UpdateRankReduceDelta extends ReduceFunction {
		
		private static final long serialVersionUID = 1L;
		
		private final DoubleValue newRank = new DoubleValue();
		
		@Override
		public void reduce(Iterator<Record> records, Collector<Record> out) {
			
			double rankSum = 0.0;
			double rank;
			Record rec = null;

			while (records.hasNext()) {
				rec = records.next();
				rank = rec.getField(1, DoubleValue.class).getValue();
				rankSum += rank;
			}
			
			// ignore small deltas
			if (Math.abs(rankSum) > 0.00001) {
				newRank.setValue(rankSum);
				rec.setField(1, newRank);
				out.collect(rec);
			}
		}
	}
	
	public class PRDependenciesComputationMatchDelta extends JoinFunction {

		private static final long serialVersionUID = 1L;
		
		private final Record result = new Record();
		private final DoubleValue partRank = new DoubleValue();
		
		/*
		 * (srcId, trgId, weight) x (vId, rank) => (trgId, rank / weight)
		 */
		@Override
		public void join(Record vertexWithRank, Record edgeWithWeight, Collector<Record> out) throws Exception {
			result.setField(0, edgeWithWeight.getField(1, LongValue.class));
			final long outLinks = edgeWithWeight.getField(2, LongValue.class).getValue();
			final double rank = vertexWithRank.getField(1, DoubleValue.class).getValue();
			partRank.setValue(rank / (double) outLinks);
			result.setField(1, partRank);
			
			out.collect(result);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public Plan getPlan(String... args) {
		// parse job parameters
		final int numSubTasks = (args.length > 0 ? Integer.parseInt(args[0]) : 1);
		final String solutionSetInput = (args.length > 1 ? args[1] : "");
		final String deltasInput = (args.length > 2 ? args[2] : "");
		final String dependencySetInput = (args.length > 3 ? args[3] : "");
		final String output = (args.length > 4 ? args[4] : "");
		final int maxIterations = (args.length > 5 ? Integer.parseInt(args[5]) : 1);
		
		// create DataSourceContract for the initalSolutionSet
		FileDataSource initialSolutionSet = new FileDataSource(new CsvInputFormat(' ', LongValue.class, DoubleValue.class), solutionSetInput, "Initial Solution Set");

		// create DataSourceContract for the initalDeltaSet
		FileDataSource initialDeltaSet = new FileDataSource(new CsvInputFormat(' ', LongValue.class, DoubleValue.class), deltasInput, "Initial DeltaSet");
				
		// create DataSourceContract for the edges
		FileDataSource dependencySet = new FileDataSource(new CsvInputFormat(' ', LongValue.class, LongValue.class, LongValue.class), dependencySetInput, "Dependency Set");
		
		DeltaIteration iteration = new DeltaIteration(0, "Delta PageRank");
		iteration.setInitialSolutionSet(initialSolutionSet);
		iteration.setInitialWorkset(initialDeltaSet);
		iteration.setMaximumNumberOfIterations(maxIterations);
		
		JoinOperator dependenciesMatch = JoinOperator.builder(PRDependenciesComputationMatchDelta.class, 
				LongValue.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(dependencySet)
				.name("calculate dependencies")
				.build();
		
		ReduceOperator updateRanks = ReduceOperator.builder(UpdateRankReduceDelta.class, LongValue.class, 0)
				.input(dependenciesMatch)
				.name("update ranks")
				.build();
		
		JoinOperator oldRankComparison = JoinOperator.builder(RankComparisonMatch.class, LongValue.class, 0, 0)
				.input1(updateRanks)
				.input2(iteration.getSolutionSet())
				.name("comparison with old ranks")
				.build();

		iteration.setNextWorkset(updateRanks);
		iteration.setSolutionSetDelta(oldRankComparison);
		
		// create DataSinkContract for writing the final ranks
		FileDataSink result = new FileDataSink(CsvOutputFormat.class, output, iteration, "Final Ranks");
		CsvOutputFormat.configureRecordFormat(result)
			.recordDelimiter('\n')
			.fieldDelimiter(' ')
			.field(LongValue.class, 0)
			.field(DoubleValue.class, 1);
		
		// return the PACT plan
		Plan plan = new Plan(result, "Delta PageRank");
		plan.setDefaultParallelism(numSubTasks);
		return plan;
		
	}

	
	@Override
	public String getDescription() {
		return "Parameters: <numberOfSubTasks> <initialSolutionSet(pageId, rank)> <deltas(pageId, delta)> <dependencySet(srcId, trgId, out_links)> <out> <maxIterations>";
	}
}
