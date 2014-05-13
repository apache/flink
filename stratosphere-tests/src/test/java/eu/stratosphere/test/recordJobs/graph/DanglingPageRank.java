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

package eu.stratosphere.test.recordJobs.graph;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.Program;
import eu.stratosphere.api.common.ProgramDescription;
import eu.stratosphere.api.java.record.operators.BulkIteration;
import eu.stratosphere.api.java.record.operators.FileDataSink;
import eu.stratosphere.api.java.record.operators.FileDataSource;
import eu.stratosphere.api.java.record.operators.CoGroupOperator;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.test.recordJobs.graph.pageRankUtil.DanglingPageRankInputFormat;
import eu.stratosphere.test.recordJobs.graph.pageRankUtil.DiffL1NormConvergenceCriterion;
import eu.stratosphere.test.recordJobs.graph.pageRankUtil.DotProductCoGroup;
import eu.stratosphere.test.recordJobs.graph.pageRankUtil.DotProductMatch;
import eu.stratosphere.test.recordJobs.graph.pageRankUtil.ImprovedAdjacencyListInputFormat;
import eu.stratosphere.test.recordJobs.graph.pageRankUtil.PageRankStatsAggregator;
import eu.stratosphere.test.recordJobs.graph.pageRankUtil.PageWithRankOutFormat;
import eu.stratosphere.types.LongValue;


public class DanglingPageRank implements Program, ProgramDescription {
	
	private static final long serialVersionUID = 1L;
	
	public static final String NUM_VERTICES_CONFIG_PARAM = "pageRank.numVertices";
		
	public Plan getPlan(String ... args) {
		int dop = 1;
		String pageWithRankInputPath = "";
		String adjacencyListInputPath = "";
		String outputPath = "";
		int numIterations = 25;
		long numVertices = 5;
		long numDanglingVertices = 1;

		if (args.length >= 7) {
			dop = Integer.parseInt(args[0]);
			pageWithRankInputPath = args[1];
			adjacencyListInputPath = args[2];
			outputPath = args[3];
			numIterations = Integer.parseInt(args[4]);
			numVertices = Long.parseLong(args[5]);
			numDanglingVertices = Long.parseLong(args[6]);
		}
		
		FileDataSource pageWithRankInput = new FileDataSource(new DanglingPageRankInputFormat(),
			pageWithRankInputPath, "DanglingPageWithRankInput");
		pageWithRankInput.getParameters().setLong(DanglingPageRankInputFormat.NUM_VERTICES_PARAMETER, numVertices);
		
		BulkIteration iteration = new BulkIteration("Page Rank Loop");
		iteration.setInput(pageWithRankInput);
		
		FileDataSource adjacencyListInput = new FileDataSource(new ImprovedAdjacencyListInputFormat(),
			adjacencyListInputPath, "AdjancencyListInput");
		
		JoinOperator join = JoinOperator.builder(new DotProductMatch(), LongValue.class, 0, 0)
				.input1(iteration.getPartialSolution())
				.input2(adjacencyListInput)
				.name("Join with Edges")
				.build();
		
		CoGroupOperator rankAggregation = CoGroupOperator.builder(new DotProductCoGroup(), LongValue.class, 0, 0)
				.input1(iteration.getPartialSolution())
				.input2(join)
				.name("Rank Aggregation")
				.build();
		rankAggregation.getParameters().setLong(DotProductCoGroup.NUM_VERTICES_PARAMETER, numVertices);
		rankAggregation.getParameters().setLong(DotProductCoGroup.NUM_DANGLING_VERTICES_PARAMETER, numDanglingVertices);
		
		iteration.setNextPartialSolution(rankAggregation);
		iteration.setMaximumNumberOfIterations(numIterations);
		iteration.getAggregators().registerAggregationConvergenceCriterion(DotProductCoGroup.AGGREGATOR_NAME, PageRankStatsAggregator.class, DiffL1NormConvergenceCriterion.class);
		
		FileDataSink out = new FileDataSink(new PageWithRankOutFormat(), outputPath, iteration, "Final Ranks");

		Plan p = new Plan(out, "Dangling PageRank");
		p.setDefaultParallelism(dop);
		return p;
	}

	@Override
	public String getDescription() {
		return "Parameters: <degree-of-parallelism> <pages-input-path> <edges-input-path> <output-path> <max-iterations> <num-vertices> <num-dangling-vertices>";
	}
}
