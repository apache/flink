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

package eu.stratosphere.example.record.incremental.pagerank;

import java.util.Iterator;

import eu.stratosphere.api.Plan;
import eu.stratosphere.api.Program;
import eu.stratosphere.api.ProgramDescription;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.record.operators.JoinOperator;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.api.record.operators.ReduceOperator.Combinable;
import eu.stratosphere.api.record.io.CsvOutputFormat;
import eu.stratosphere.util.Collector;
import eu.stratosphere.api.record.functions.JoinFunction;
import eu.stratosphere.api.record.functions.ReduceFunction;
import eu.stratosphere.api.record.functions.FunctionAnnotation.ConstantFields;
import eu.stratosphere.api.record.functions.FunctionAnnotation.ConstantFieldsSecond;
import eu.stratosphere.types.PactRecord;
import eu.stratosphere.types.PactDouble;
import eu.stratosphere.types.PactLong;
import eu.stratosphere.api.operators.WorksetIteration;

public class DeltaPageRankWithInitialDeltas implements Program, ProgramDescription {

	@ConstantFieldsSecond(0)
	public static final class RankComparisonMatch extends JoinFunction {
		
		private final PactDouble newRank = new PactDouble();
		
		@Override
		public void match(PactRecord vertexWithDelta, PactRecord vertexWithOldRank, Collector<PactRecord> out) throws Exception {			
			PactDouble deltaVal = vertexWithDelta.getField(1, PactDouble.class);
			PactDouble currentVal = vertexWithOldRank.getField(1, PactDouble.class);
			
			newRank.setValue(deltaVal.getValue() + currentVal.getValue());
			vertexWithOldRank.setField(1, newRank);
			
			out.collect(vertexWithOldRank);
		}
	}
	
	@Combinable
	@ConstantFields(0)
	public static final class UpdateRankReduceDelta extends ReduceFunction {
		
		private final PactDouble newRank = new PactDouble();
		
		@Override
		public void reduce(Iterator<PactRecord> records, Collector<PactRecord> out) {
			
			double rankSum = 0.0;
			double rank;
			PactRecord rec = null;

			while (records.hasNext()) {
				rec = records.next();
				rank = rec.getField(1, PactDouble.class).getValue();
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
		FileDataSource initialSolutionSet = new FileDataSource(InitialRankInputFormat.class, solutionSetInput, "Initial Solution Set");		

		// create DataSourceContract for the initalDeltaSet
		FileDataSource initialDeltaSet = new FileDataSource(InitialRankInputFormat.class, deltasInput, "Initial DeltaSet");		
				
		// create DataSourceContract for the edges
		FileDataSource dependencySet = new FileDataSource(EdgesWithWeightInputFormat.class, dependencySetInput, "Dependency Set");
		
		WorksetIteration iteration = new WorksetIteration(0, "Delta PageRank");
		iteration.setInitialSolutionSet(initialSolutionSet);
		iteration.setInitialWorkset(initialDeltaSet);
		iteration.setMaximumNumberOfIterations(maxIterations);
		
		JoinOperator dependenciesMatch = JoinOperator.builder(PRDependenciesComputationMatchDelta.class, 
				PactLong.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(dependencySet)
				.name("calculate dependencies")
				.build();
		
		ReduceOperator updateRanks = ReduceOperator.builder(UpdateRankReduceDelta.class, PactLong.class, 0)
				.input(dependenciesMatch)
				.name("update ranks")
				.build();
		
		JoinOperator oldRankComparison = JoinOperator.builder(RankComparisonMatch.class, PactLong.class, 0, 0)
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
			.field(PactLong.class, 0)
			.field(PactDouble.class, 1);
		
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