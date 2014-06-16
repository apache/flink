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

package eu.stratosphere.test.compiler.iterations;

import static eu.stratosphere.api.java.aggregation.Aggregations.SUM;
import static org.junit.Assert.fail;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.BulkIterationPlanNode;
import eu.stratosphere.compiler.plan.BulkPartialSolutionPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.SinkPlanNode;
import eu.stratosphere.configuration.Configuration;
import eu.stratosphere.example.java.graph.PageRankBasic.BuildOutgoingEdgeList;
import eu.stratosphere.example.java.graph.PageRankBasic.Dampener;
import eu.stratosphere.example.java.graph.PageRankBasic.EpsilonFilter;
import eu.stratosphere.example.java.graph.PageRankBasic.JoinVertexWithEdgesMatch;
import eu.stratosphere.example.java.graph.PageRankBasic.RankAssigner;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.test.compiler.util.CompilerTestBase;

public class PageRankCompilerTest extends CompilerTestBase{
	
	@Test
	public void testPageRank() {
		try {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			// get input data
			@SuppressWarnings("unchecked")
			DataSet<Tuple1<Long>> pagesInput = env.fromElements(new Tuple1<Long>(1l));
			@SuppressWarnings("unchecked")
			DataSet<Tuple2<Long, Long>> linksInput =env.fromElements(new Tuple2<Long, Long>(1l, 2l));
			
			// assign initial rank to pages
			DataSet<Tuple2<Long, Double>> pagesWithRanks = pagesInput.
					map(new RankAssigner((1.0d / 10)));
			
			// build adjacency list from link input
			DataSet<Tuple2<Long, Long[]>> adjacencyListInput = 
					linksInput.groupBy(0).reduceGroup(new BuildOutgoingEdgeList());
			
			// set iterative data set
			IterativeDataSet<Tuple2<Long, Double>> iteration = pagesWithRanks.iterate(10);
			
			Configuration cfg = new Configuration();
			cfg.setString(PactCompiler.HINT_LOCAL_STRATEGY, PactCompiler.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND);
			
			DataSet<Tuple2<Long, Double>> newRanks = iteration
					// join pages with outgoing edges and distribute rank
					.join(adjacencyListInput).where(0).equalTo(0).withParameters(cfg)
					.flatMap(new JoinVertexWithEdgesMatch())
					// collect and sum ranks
					.groupBy(0).aggregate(SUM, 1)
					// apply dampening factor
					.map(new Dampener(0.85, 10));
			
			DataSet<Tuple2<Long, Double>> finalPageRanks = iteration.closeWith(
					newRanks, 
					newRanks.join(iteration).where(0).equalTo(0)
					// termination condition
					.filter(new EpsilonFilter()));
	
			finalPageRanks.print();
	
			// get the plan and compile it
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			SinkPlanNode sinkPlanNode = (SinkPlanNode) op.getDataSinks().iterator().next();
			BulkIterationPlanNode iterPlanNode = (BulkIterationPlanNode) sinkPlanNode.getInput().getSource();
			
			// check that the partitioning is pushed out of the first loop
			Assert.assertEquals(ShipStrategyType.PARTITION_HASH, iterPlanNode.getInput().getShipStrategy());
			Assert.assertEquals(LocalStrategy.NONE, iterPlanNode.getInput().getLocalStrategy());
			
			BulkPartialSolutionPlanNode partSolPlanNode = iterPlanNode.getPartialSolutionPlanNode();
			Assert.assertEquals(ShipStrategyType.FORWARD, partSolPlanNode.getOutgoingChannels().get(0).getShipStrategy());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
