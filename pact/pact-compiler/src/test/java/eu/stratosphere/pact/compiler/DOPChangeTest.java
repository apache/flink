/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

package eu.stratosphere.pact.compiler;

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.common.util.Visitor;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyMatchStub;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityMap;
import eu.stratosphere.pact.compiler.util.IdentityReduce;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;

/**
 * Tests in this class:
 * <ul>
 *   <li>Tests that check the correct handling of the properties and strategies in the case where the degree of
 *       parallelism between tasks is increased or decreased.
 * </ul>
 */
public class DOPChangeTest extends CompilerTestBase {
	
	/**
	 * Simple Job: Map -> Reduce -> Map -> Reduce. All functions preserve all fields (hence all properties).
	 * 
	 * Increases DOP between 1st reduce and 2nd map, so the hash partitioning from 1st reduce is not reusable.
	 * Expected to re-establish partitioning between reduce and map, via hash, because random is a full network
	 * transit as well.
	 */
	@Test
	public void checkPropertyHandlingWithIncreasingGlobalParallelism1() {
		final int degOfPar = DEFAULT_PARALLELISM;
		
		// construct the plan
		FileDataSource source = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source");
		source.setDegreeOfParallelism(degOfPar);
		
		MapContract map1 = MapContract.builder(IdentityMap.class).name("Map1").build();
		map1.setDegreeOfParallelism(degOfPar);
		map1.setInput(source);
		
		ReduceContract reduce1 = ReduceContract.builder(IdentityReduce.class, PactInteger.class, 0).name("Reduce 1").build();
		reduce1.setDegreeOfParallelism(degOfPar);
		reduce1.setInput(map1);
		
		MapContract map2 = MapContract.builder(IdentityMap.class).name("Map2").build();
		map2.setDegreeOfParallelism(degOfPar * 2);
		map2.setInput(reduce1);
		
		ReduceContract reduce2 = ReduceContract.builder(IdentityReduce.class, PactInteger.class, 0).name("Reduce 2").build();
		reduce2.setDegreeOfParallelism(degOfPar * 2);
		reduce2.setInput(map2);
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE, "Sink");
		sink.setDegreeOfParallelism(degOfPar * 2);
		sink.setInput(reduce2);
		
		Plan plan = new Plan(sink, "Test Increasing Degree Of Parallelism");
		
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
	 * Increases DOP between 2nd map and 2nd reduce, so the hash partitioning from 1st reduce is not reusable.
	 * Expected to re-establish partitioning between map and reduce (hash).
	 */
	@Test
	public void checkPropertyHandlingWithIncreasingGlobalParallelism2() {
		final int degOfPar = DEFAULT_PARALLELISM;
		
		// construct the plan
		FileDataSource source = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source");
		source.setDegreeOfParallelism(degOfPar);
		
		MapContract map1 = MapContract.builder(IdentityMap.class).name("Map1").build();
		map1.setDegreeOfParallelism(degOfPar);
		map1.setInput(source);
		
		ReduceContract reduce1 = ReduceContract.builder(IdentityReduce.class, PactInteger.class, 0).name("Reduce 1").build();
		reduce1.setDegreeOfParallelism(degOfPar);
		reduce1.setInput(map1);
		
		MapContract map2 = MapContract.builder(IdentityMap.class).name("Map2").build();
		map2.setDegreeOfParallelism(degOfPar);
		map2.setInput(reduce1);
		
		ReduceContract reduce2 = ReduceContract.builder(IdentityReduce.class, PactInteger.class, 0).name("Reduce 2").build();
		reduce2.setDegreeOfParallelism(degOfPar * 2);
		reduce2.setInput(map2);
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE, "Sink");
		sink.setDegreeOfParallelism(degOfPar * 2);
		sink.setInput(reduce2);
		
		Plan plan = new Plan(sink, "Test Increasing Degree Of Parallelism");
		
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
	 * Increases DOP between 1st reduce and 2nd map, such that more tasks are on one instance.
	 * Expected to re-establish partitioning between map and reduce via a local hash.
	 */
	@Test
	public void checkPropertyHandlingWithIncreasingLocalParallelism() {
		final int degOfPar = 2 * DEFAULT_PARALLELISM;
		
		// construct the plan
		FileDataSource source = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source");
		source.setDegreeOfParallelism(degOfPar);
		
		MapContract map1 = MapContract.builder(IdentityMap.class).name("Map1").build();
		map1.setDegreeOfParallelism(degOfPar);
		map1.setInput(source);
		
		ReduceContract reduce1 = ReduceContract.builder(IdentityReduce.class, PactInteger.class, 0).name("Reduce 1").build();
		reduce1.setDegreeOfParallelism(degOfPar);
		reduce1.setInput(map1);
		
		MapContract map2 = MapContract.builder(IdentityMap.class).name("Map2").build();
		map2.setDegreeOfParallelism(degOfPar * 2);
		map2.setInput(reduce1);
		
		ReduceContract reduce2 = ReduceContract.builder(IdentityReduce.class, PactInteger.class, 0).name("Reduce 2").build();
		reduce2.setDegreeOfParallelism(degOfPar * 2);
		reduce2.setInput(map2);
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE, "Sink");
		sink.setDegreeOfParallelism(degOfPar * 2);
		sink.setInput(reduce2);
		
		Plan plan = new Plan(sink, "Test Increasing Degree Of Parallelism");
		
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
		
		Assert.assertEquals("Invalid ship strategy for an operator.", ShipStrategyType.PARTITION_LOCAL_HASH, mapIn);
		Assert.assertEquals("Invalid ship strategy for an operator.", ShipStrategyType.FORWARD, reduceIn);
	}
	
	
	
	@Test
	public void checkPropertyHandlingWithDecreasingDegreeOfParallelism()
	{
		final int degOfPar = DEFAULT_PARALLELISM;
		
		// construct the plan
		FileDataSource source = new FileDataSource(DummyInputFormat.class, IN_FILE, "Source");
		source.setDegreeOfParallelism(degOfPar * 2);
		
		MapContract map1 = MapContract.builder(IdentityMap.class).name("Map1").build();
		map1.setDegreeOfParallelism(degOfPar * 2);
		map1.setInput(source);
		
		ReduceContract reduce1 = ReduceContract.builder(IdentityReduce.class, PactInteger.class, 0).name("Reduce 1").build();
		reduce1.setDegreeOfParallelism(degOfPar * 2);
		reduce1.setInput(map1);
		
		MapContract map2 = MapContract.builder(IdentityMap.class).name("Map2").build();
		map2.setDegreeOfParallelism(degOfPar);
		map2.setInput(reduce1);
		
		ReduceContract reduce2 = ReduceContract.builder(IdentityReduce.class, PactInteger.class, 0).name("Reduce 2").build();
		reduce2.setDegreeOfParallelism(degOfPar);
		reduce2.setInput(map2);
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE, "Sink");
		sink.setDegreeOfParallelism(degOfPar);
		sink.setInput(reduce2);
		
		Plan plan = new Plan(sink, "Test Increasing Degree Of Parallelism");
		
		// submit the plan to the compiler
		OptimizedPlan oPlan = compileNoStats(plan);
		
		// check the optimized Plan
		// when reducer 1 distributes its data across the instances of map2, it needs to employ a local hash method,
		// because map2 has twice as many instances and key/value pairs with the same key need to be processed by the same
		// mapper respectively reducer
		SinkPlanNode sinkNode = oPlan.getDataSinks().iterator().next();
		SingleInputPlanNode red2Node = (SingleInputPlanNode) sinkNode.getPredecessor();
		
		Assert.assertEquals("The Reduce 2 Node has an invalid local strategy.", LocalStrategy.SORT, red2Node.getInput().getLocalStrategy());
	}

	/**
	 * Checks that re-partitioning happens when the inputs of a two-input contract have different DOPs.
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

		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE);
		FileDataSource sourceB = new FileDataSource(DummyInputFormat.class, IN_FILE);
		
		ReduceContract redA = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(sourceA)
			.build();
		ReduceContract redB = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(sourceB)
			.build();
		
		MatchContract mat = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(redA)
			.input2(redB)
			.build();
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE, mat);
		
		sourceA.setDegreeOfParallelism(5);
		sourceB.setDegreeOfParallelism(7);
		redA.setDegreeOfParallelism(5);
		redB.setDegreeOfParallelism(7);
		
		mat.setDegreeOfParallelism(5);
		
		sink.setDegreeOfParallelism(5);
		
		
		// return the PACT plan
		Plan plan = new Plan(sink, "Partition on DoP Change");
		
		OptimizedPlan oPlan = compileNoStats(plan);
		
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		
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
