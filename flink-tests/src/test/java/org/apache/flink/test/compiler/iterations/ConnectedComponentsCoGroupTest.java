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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.optimizer.dag.TempMode;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.optimizer.plandump.PlanJSONDumpGenerator;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.test.recordJobs.graph.ConnectedComponentsWithCoGroup;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ConnectedComponentsCoGroupTest extends CompilerTestBase {
	
	private static final String VERTEX_SOURCE = "Vertices";
	
	private static final String ITERATION_NAME = "Connected Components Iteration";
	
	private static final String EDGES_SOURCE = "Edges";
	private static final String JOIN_NEIGHBORS_MATCH = "Join Candidate Id With Neighbor";
	private static final String MIN_ID_AND_UPDATE = "Min Id and Update";
	
	private static final String SINK = "Result";
	
	private static final boolean PRINT_PLAN = false;
	
	private final FieldList set0 = new FieldList(0);
	
	
	@Test
	public void testWorksetConnectedComponents() {
		ConnectedComponentsWithCoGroup cc = new ConnectedComponentsWithCoGroup();

		Plan plan = cc.getPlan(String.valueOf(DEFAULT_PARALLELISM),
				IN_FILE, IN_FILE, OUT_FILE, String.valueOf(100));
		plan.setExecutionConfig(new ExecutionConfig());
		OptimizedPlan optPlan = compileNoStats(plan);
		OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(optPlan);
		
		if (PRINT_PLAN) {
			PlanJSONDumpGenerator dumper = new PlanJSONDumpGenerator();
			String json = dumper.getOptimizerPlanAsJSON(optPlan);
			System.out.println(json);
		}
		
		SourcePlanNode vertexSource = or.getNode(VERTEX_SOURCE);
		SourcePlanNode edgesSource = or.getNode(EDGES_SOURCE);
		SinkPlanNode sink = or.getNode(SINK);
		WorksetIterationPlanNode iter = or.getNode(ITERATION_NAME);
		
		DualInputPlanNode neighborsJoin = or.getNode(JOIN_NEIGHBORS_MATCH);
		DualInputPlanNode cogroup = or.getNode(MIN_ID_AND_UPDATE);
		
		// --------------------------------------------------------------------
		// Plan validation:
		//
		// We expect the plan to go with a sort-merge join, because the CoGroup
		// sorts and the join in the successive iteration can re-exploit the sorting.
		// --------------------------------------------------------------------
		
		// test all drivers
		Assert.assertEquals(DriverStrategy.NONE, sink.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.NONE, vertexSource.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.NONE, edgesSource.getDriverStrategy());
		
		Assert.assertEquals(DriverStrategy.MERGE, neighborsJoin.getDriverStrategy());
		Assert.assertEquals(set0, neighborsJoin.getKeysForInput1());
		Assert.assertEquals(set0, neighborsJoin.getKeysForInput2());
		
		Assert.assertEquals(DriverStrategy.CO_GROUP, cogroup.getDriverStrategy());
		Assert.assertEquals(set0, cogroup.getKeysForInput1());
		Assert.assertEquals(set0, cogroup.getKeysForInput2());
		
		// test all the shipping strategies
		Assert.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, iter.getInitialSolutionSetInput().getShipStrategy());
		Assert.assertEquals(set0, iter.getInitialSolutionSetInput().getShipStrategyKeys());
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, iter.getInitialWorksetInput().getShipStrategy());
		Assert.assertEquals(set0, iter.getInitialWorksetInput().getShipStrategyKeys());
		
		Assert.assertEquals(ShipStrategyType.FORWARD, neighborsJoin.getInput1().getShipStrategy()); // workset
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, neighborsJoin.getInput2().getShipStrategy()); // edges
		Assert.assertEquals(set0, neighborsJoin.getInput2().getShipStrategyKeys());
		Assert.assertTrue(neighborsJoin.getInput2().getTempMode().isCached());
		
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, cogroup.getInput1().getShipStrategy()); // min id
		Assert.assertEquals(ShipStrategyType.FORWARD, cogroup.getInput2().getShipStrategy()); // solution set
		
		// test all the local strategies
		Assert.assertEquals(LocalStrategy.NONE, sink.getInput().getLocalStrategy());
		Assert.assertEquals(LocalStrategy.NONE, iter.getInitialSolutionSetInput().getLocalStrategy());
		
		// the sort for the neighbor join in the first iteration is pushed out of the loop
		Assert.assertEquals(LocalStrategy.SORT, iter.getInitialWorksetInput().getLocalStrategy());
		Assert.assertEquals(LocalStrategy.NONE, neighborsJoin.getInput1().getLocalStrategy()); // workset
		Assert.assertEquals(LocalStrategy.SORT, neighborsJoin.getInput2().getLocalStrategy()); // edges
		
		Assert.assertEquals(LocalStrategy.SORT, cogroup.getInput1().getLocalStrategy());
		Assert.assertEquals(LocalStrategy.NONE, cogroup.getInput2().getLocalStrategy()); // solution set
		
		// check the caches
		Assert.assertTrue(TempMode.CACHED == neighborsJoin.getInput2().getTempMode());
		
		JobGraphGenerator jgg = new JobGraphGenerator();
		jgg.compileJobGraph(optPlan);
	}
}
