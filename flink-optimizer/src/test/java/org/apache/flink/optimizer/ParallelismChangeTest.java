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
package org.apache.flink.optimizer;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducer;
import org.apache.flink.optimizer.testfunctions.IdentityJoiner;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.Assert;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.util.Visitor;
import org.junit.Test;

/**
 * Tests in this class:
 * <ul>
 *   <li>Tests that check the correct handling of the properties and strategies in the case where the
 *       parallelism between tasks is increased or decreased.
 * </ul>
 */
@SuppressWarnings({"serial"})
public class ParallelismChangeTest extends CompilerTestBase {
	
	/**
	 * Simple Job: Map -> Reduce -> Map -> Reduce. All functions preserve all fields (hence all properties).
	 * 
	 * Increases parallelism between 1st reduce and 2nd map, so the hash partitioning from 1st reduce is not reusable.
	 * Expected to re-establish partitioning between reduce and map, via hash, because random is a full network
	 * transit as well.
	 */
	@Test
	public void checkPropertyHandlingWithIncreasingGlobalParallelism1() {
		final int p = DEFAULT_PARALLELISM;

		// construct the plan
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(p);
		DataSet<Long> set1 = env.generateSequence(0,1).setParallelism(p);

		set1.map(new IdentityMapper<Long>())
					.withForwardedFields("*").setParallelism(p).name("Map1")
				.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>())
					.withForwardedFields("*").setParallelism(p).name("Reduce1")
				.map(new IdentityMapper<Long>())
					.withForwardedFields("*").setParallelism(p * 2).name("Map2")
				.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>())
					.withForwardedFields("*").setParallelism(p * 2).name("Reduce2")
				.output(new DiscardingOutputFormat<Long>()).setParallelism(p * 2).name("Sink");

		Plan plan = env.createProgramPlan();
		// submit the plan to the compiler
		OptimizedPlan oPlan = compileNoStats(plan);
		
		// check the optimized Plan
		// when reducer 1 distributes its data across the instances of map2, it needs to employ a local hash method,
		// because map2 has twice as many instances and key/value pairs with the same key need to be processed by the same
		// mapper respectively reducer
		SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
		SingleInputPlanNode red2Node = (SingleInputPlanNode) sinkNode.getPredecessor();
		SingleInputPlanNode map2Node = (SingleInputPlanNode) red2Node.getPredecessor();
		
		ShipStrategyType mapIn = map2Node.getInput().getShipStrategy();
		ShipStrategyType redIn = red2Node.getInput().getShipStrategy();
		
		Assert.assertEquals("Invalid ship strategy for an operator.", ShipStrategyType.PARTITION_HASH, mapIn);
		Assert.assertEquals("Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, redIn);
	}
	
	/**
	 * Simple Job: Map -> Reduce -> Map -> Reduce. All functions preserve all fields (hence all properties).
	 * 
	 * Increases parallelism between 2nd map and 2nd reduce, so the hash partitioning from 1st reduce is not reusable.
	 * Expected to re-establish partitioning between map and reduce (hash).
	 */
	@Test
	public void checkPropertyHandlingWithIncreasingGlobalParallelism2() {
		final int p = DEFAULT_PARALLELISM;

		// construct the plan
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(p);
		DataSet<Long> set1 = env.generateSequence(0,1).setParallelism(p);

		set1.map(new IdentityMapper<Long>())
				.withForwardedFields("*").setParallelism(p).name("Map1")
				.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>())
				.withForwardedFields("*").setParallelism(p).name("Reduce1")
				.map(new IdentityMapper<Long>())
				.withForwardedFields("*").setParallelism(p).name("Map2")
				.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>())
				.withForwardedFields("*").setParallelism(p * 2).name("Reduce2")
				.output(new DiscardingOutputFormat<Long>()).setParallelism(p * 2).name("Sink");

		Plan plan = env.createProgramPlan();
		
		// submit the plan to the compiler
		OptimizedPlan oPlan = compileNoStats(plan);
		
		// check the optimized Plan
		// when reducer 1 distributes its data across the instances of map2, it needs to employ a local hash method,
		// because map2 has twice as many instances and key/value pairs with the same key need to be processed by the same
		// mapper respectively reducer
		SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
		SingleInputPlanNode red2Node = (SingleInputPlanNode) sinkNode.getPredecessor();
		SingleInputPlanNode map2Node = (SingleInputPlanNode) red2Node.getPredecessor();
		
		ShipStrategyType mapIn = map2Node.getInput().getShipStrategy();
		ShipStrategyType reduceIn = red2Node.getInput().getShipStrategy();
		
		Assert.assertEquals("Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, mapIn);
		Assert.assertEquals("Invalid ship strategy for an operator.", ShipStrategyType.PARTITION_HASH, reduceIn);
	}
	
	/**
	 * Simple Job: Map -> Reduce -> Map -> Reduce. All functions preserve all fields (hence all properties).
	 * 
	 * Increases parallelism between 1st reduce and 2nd map, such that more tasks are on one instance.
	 * Expected to re-establish partitioning between map and reduce via a local hash.
	 */
	@Test
	public void checkPropertyHandlingWithIncreasingLocalParallelism() {
		final int p = DEFAULT_PARALLELISM * 2;

		// construct the plan
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(p);
		DataSet<Long> set1 = env.generateSequence(0,1).setParallelism(p);

		set1.map(new IdentityMapper<Long>())
				.withForwardedFields("*").setParallelism(p).name("Map1")
				.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>())
				.withForwardedFields("*").setParallelism(p).name("Reduce1")
				.map(new IdentityMapper<Long>())
				.withForwardedFields("*").setParallelism(p * 2).name("Map2")
				.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>())
				.withForwardedFields("*").setParallelism(p * 2).name("Reduce2")
				.output(new DiscardingOutputFormat<Long>()).setParallelism(p * 2).name("Sink");

		Plan plan = env.createProgramPlan();
		// submit the plan to the compiler
		OptimizedPlan oPlan = compileNoStats(plan);
		
		// check the optimized Plan
		// when reducer 1 distributes its data across the instances of map2, it needs to employ a local hash method,
		// because map2 has twice as many instances and key/value pairs with the same key need to be processed by the same
		// mapper respectively reducer
		SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
		SingleInputPlanNode red2Node = (SingleInputPlanNode) sinkNode.getPredecessor();
		SingleInputPlanNode map2Node = (SingleInputPlanNode) red2Node.getPredecessor();
		
		ShipStrategyType mapIn = map2Node.getInput().getShipStrategy();
		ShipStrategyType reduceIn = red2Node.getInput().getShipStrategy();
		
		Assert.assertTrue("Invalid ship strategy for an operator.", 
				(ShipStrategyType.PARTITION_RANDOM ==  mapIn && ShipStrategyType.PARTITION_HASH == reduceIn) || 
				(ShipStrategyType.PARTITION_HASH == mapIn && ShipStrategyType.FORWARD == reduceIn));
	}
	
	@Test
	public void checkPropertyHandlingWithDecreasingParallelism() {
		final int p = DEFAULT_PARALLELISM;

		// construct the plan
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(p);

		env
			.generateSequence(0, 1).setParallelism(p * 2)
			.map(new IdentityMapper<Long>())
				.withForwardedFields("*").setParallelism(p * 2).name("Map1")
			.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>())
				.withForwardedFields("*").setParallelism(p * 2).name("Reduce1")
			.map(new IdentityMapper<Long>())
				.withForwardedFields("*").setParallelism(p).name("Map2")
			.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>())
				.withForwardedFields("*").setParallelism(p).name("Reduce2")
			.output(new DiscardingOutputFormat<Long>()).setParallelism(p).name("Sink");

		Plan plan = env.createProgramPlan();
		// submit the plan to the compiler
		OptimizedPlan oPlan = compileNoStats(plan);

		// check the optimized Plan
		// when reducer 1 distributes its data across the instances of map2, it needs to employ a local hash method,
		// because map2 has twice as many instances and key/value pairs with the same key need to be processed by the same
		// mapper respectively reducer
		SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
		SingleInputPlanNode red2Node = (SingleInputPlanNode) sinkNode.getPredecessor();
		SingleInputPlanNode map2Node = (SingleInputPlanNode) red2Node.getPredecessor();

		Assert.assertTrue("The no sorting local strategy.",
				LocalStrategy.SORT == red2Node.getInput().getLocalStrategy() ||
						LocalStrategy.SORT == map2Node.getInput().getLocalStrategy());

		Assert.assertTrue("The no partitioning ship strategy.",
				ShipStrategyType.PARTITION_HASH == red2Node.getInput().getShipStrategy() ||
						ShipStrategyType.PARTITION_HASH == map2Node.getInput().getShipStrategy());
	}

	/**
	 * Checks that re-partitioning happens when the inputs of a two-input contract have different parallelisms.
	 * 
	 * Test Plan:
	 * <pre>
	 * 
	 * (source) -> reduce -\
	 *                      Match -> (sink)
	 * (source) -> reduce -/
	 * 
	 * </pre>
	 * 
	 */
	@Test
	public void checkPropertyHandlingWithTwoInputs() {

		// construct the plan
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);

		DataSet<Long> set1 = env.generateSequence(0,1).setParallelism(5);
		DataSet<Long> set2 = env.generateSequence(0,1).setParallelism(7);

		DataSet<Long> reduce1 = set1
				.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>())
					.withForwardedFields("*").setParallelism(5);
		DataSet<Long> reduce2 = set2
				.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>())
					.withForwardedFields("*").setParallelism(7);

		reduce1.join(reduce2).where("*").equalTo("*")
					.with(new IdentityJoiner<Long>()).setParallelism(5)
				.output(new DiscardingOutputFormat<Long>()).setParallelism(5);

		Plan plan = env.createProgramPlan();
		// submit the plan to the compiler
		OptimizedPlan oPlan = compileNoStats(plan);

		JobGraphGenerator jobGen = new JobGraphGenerator();
		
		//Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
		
		oPlan.accept(new Visitor<PlanNode>() {
			
			@Override
			public boolean preVisit(PlanNode visitable) {
				if (visitable instanceof DualInputPlanNode) {
					DualInputPlanNode node = (DualInputPlanNode) visitable;
					Channel c1 = node.getInput1();
					Channel c2 = node.getInput2();
					
					Assert.assertEquals("Incompatible shipping strategy chosen for match", ShipStrategyType.FORWARD, c1.getShipStrategy());
					Assert.assertEquals("Incompatible shipping strategy chosen for match", ShipStrategyType.PARTITION_HASH, c2.getShipStrategy());
					return false;
				}
				return true;
			}
			
			@Override
			public void postVisit(PlanNode visitable) {
				// DO NOTHING
			}
		});
	}
}
