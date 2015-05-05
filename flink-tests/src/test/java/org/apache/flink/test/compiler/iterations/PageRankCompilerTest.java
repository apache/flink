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


package org.apache.flink.test.compiler.iterations;

import static org.apache.flink.api.java.aggregation.Aggregations.SUM;
import static org.junit.Assert.fail;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.Optimizer;
import org.apache.flink.optimizer.plan.BulkIterationPlanNode;
import org.apache.flink.optimizer.plan.BulkPartialSolutionPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.examples.java.graph.PageRankBasic.BuildOutgoingEdgeList;
import org.apache.flink.examples.java.graph.PageRankBasic.Dampener;
import org.apache.flink.examples.java.graph.PageRankBasic.EpsilonFilter;
import org.apache.flink.examples.java.graph.PageRankBasic.JoinVertexWithEdgesMatch;
import org.apache.flink.examples.java.graph.PageRankBasic.RankAssigner;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.IterativeDataSet;

public class PageRankCompilerTest extends CompilerTestBase{
	
	@Test
	public void testPageRank() {
		try {
			final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			// get input data
			DataSet<Long> pagesInput = env.fromElements(1l);
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
			cfg.setString(Optimizer.HINT_LOCAL_STRATEGY, Optimizer.HINT_LOCAL_STRATEGY_HASH_BUILD_SECOND);
			
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
	
			finalPageRanks.output(new DiscardingOutputFormat<Tuple2<Long, Double>>());
	
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
