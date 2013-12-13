/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.example.pagerank;

import java.io.Serializable;
import java.util.Iterator;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.contract.ReduceContract.Combinable;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.PlanAssembler;
import eu.stratosphere.pact.common.plan.PlanAssemblerDescription;
import eu.stratosphere.pact.common.stubs.Collector;
import eu.stratosphere.pact.common.stubs.MatchStub;
import eu.stratosphere.pact.common.stubs.ReduceStub;
import eu.stratosphere.pact.common.stubs.StubAnnotation.ConstantFields;
import eu.stratosphere.pact.common.type.PactRecord;
import eu.stratosphere.pact.common.type.base.PactDouble;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.generic.contract.BulkIteration;


public class SimplePageRank implements PlanAssembler, PlanAssemblerDescription {
	
	private static final String NUM_VERTICES_CONFIG_PARAM = "pageRank.numVertices";
	
	// --------------------------------------------------------------------------------------------

	public static final class JoinVerexWithEdgesMatch extends MatchStub implements Serializable {
		private static final long serialVersionUID = 1L;

		private PactRecord record = new PactRecord();
		private PactLong vertexID = new PactLong();
		private PactDouble partialRank = new PactDouble();
		private PactDouble rank = new PactDouble();

		private LongArrayView adjacentNeighbors = new LongArrayView();
		
		@Override
		public void match(PactRecord pageWithRank, PactRecord edges, Collector<PactRecord> out) throws Exception {
			rank = pageWithRank.getField(1, rank);
			adjacentNeighbors = edges.getField(1, adjacentNeighbors);
			int numNeighbors = adjacentNeighbors.size();

			double rankToDistribute = rank.getValue() / (double) numNeighbors;

			partialRank.setValue(rankToDistribute);
			record.setField(1, partialRank);

			for (int n = 0; n < numNeighbors; n++) {
				vertexID.setValue(adjacentNeighbors.getQuick(n));
				record.setField(0, vertexID);
				out.collect(record);
			}
		}
	}
	
	@Combinable
	@ConstantFields(0)
	public static final class AggregatingReduce extends ReduceStub implements Serializable {
		private static final long serialVersionUID = 1L;
		
		private final PactDouble sum = new PactDouble();

		@Override
		public void reduce(Iterator<PactRecord> pageWithPartialRank, Collector<PactRecord> out) throws Exception {
			PactRecord rec = null;
			double rankSum = 0.0;
			while (pageWithPartialRank.hasNext()) {
				rec = pageWithPartialRank.next();
				rankSum += rec.getField(1, PactDouble.class).getValue();
			}
			sum.setValue(rankSum);
			rec.setField(1, sum);
			out.collect(rec);
		}
	}
	
	// --------------------------------------------------------------------------------------------
	
	public Plan getPlan(String ... args) {
		int dop = 1;
		String pageWithRankInputPath = "";
		String adjacencyListInputPath = "";
		String outputPath = "";
		int numIterations = 25;
		long numVertices = 5;

		if (args.length >= 6) {
			dop = Integer.parseInt(args[0]);
			pageWithRankInputPath = args[1];
			adjacencyListInputPath = args[2];
			outputPath = args[3];
			numIterations = Integer.parseInt(args[4]);
			numVertices = Long.parseLong(args[5]);
		}
		
		FileDataSource pageWithRankInput = new FileDataSource(new DanglingPageRankInputFormat(),
			pageWithRankInputPath, "PageWithRank Input");
		pageWithRankInput.getParameters().setLong(NUM_VERTICES_CONFIG_PARAM, numVertices);
		
		BulkIteration iteration = new BulkIteration("Page Rank Loop");
		iteration.setInput(pageWithRankInput);
		
		FileDataSource adjacencyListInput = new FileDataSource(new ImprovedAdjacencyListInputFormat(),
			adjacencyListInputPath, "AdjancencyListInput");
		
		MatchContract join = MatchContract.builder(new JoinVerexWithEdgesMatch(), PactLong.class, 0, 0)
				.input1(iteration.getPartialSolution())
				.input2(adjacencyListInput)
				.name("Join with Edges")
				.build();
		
		ReduceContract rankAggregation = ReduceContract.builder(new AggregatingReduce(), PactLong.class, 0)
				.input(join)
				.name("Rank Aggregation")
				.build();
		
		iteration.setNextPartialSolution(rankAggregation);
		iteration.setMaximumNumberOfIterations(numIterations);
		
		FileDataSink out = new FileDataSink(new PageWithRankOutFormat(), outputPath, iteration, "Final Ranks");

		Plan p = new Plan(out, "Simple PageRank");
		p.setDefaultParallelism(dop);
		return p;
	}

	@Override
	public String getDescription() {
		return "Parameters: <degree-of-parallelism> <pages-input-path> <edges-input-path> <output-path> <max-iterations> <num-vertices> <num-dangling-vertices>";
	}
}
