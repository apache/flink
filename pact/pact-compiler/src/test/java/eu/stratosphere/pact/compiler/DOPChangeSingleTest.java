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
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityMap;
import eu.stratosphere.pact.compiler.util.IdentityReduce;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;

/**
 * Tests in this class:
 * <ul>
 *   <li>Tests that check the correct handling of the properties and strategies in the case where the degree of
 *       parallelism between tasks is increased or decreased.
 * </ul>
 */
public class DOPChangeSingleTest extends CompilerTestBase {
	
	/**
	 * Simple Job: Map -> Reduce -> Map -> Reduce. All functions preserve all fields (hence all properties).
	 * 
	 * Increases DOP between 1st reduce and 2nd map, so the hash partitioning from 1st reduce is not reusable.
	 * Expected to re-establish partitioning between reduce and map, via hash, because random is a full network
	 * transit as well.
	 */
	@Test
	public void checkPropertyHandlingWithIncreasingGlobalParallelism1() {
		final int degOfPar = defaultParallelism;
		
		// construct the plan
		FileDataSource source = new FileDataSource(DummyInputFormat.class, IN_FILE_1, "Source");
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
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, "Sink");
		sink.setDegreeOfParallelism(degOfPar * 2);
		sink.setInput(reduce2);
		
		Plan plan = new Plan(sink, "Test Increasing Degree Of Parallelism");
		
		// submit the plan to the compiler
		OptimizedPlan oPlan = compile(plan);
		
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
		final int degOfPar = defaultParallelism;
		
		// construct the plan
		FileDataSource source = new FileDataSource(DummyInputFormat.class, IN_FILE_1, "Source");
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
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, "Sink");
		sink.setDegreeOfParallelism(degOfPar * 2);
		sink.setInput(reduce2);
		
		Plan plan = new Plan(sink, "Test Increasing Degree Of Parallelism");
		
		// submit the plan to the compiler
		OptimizedPlan oPlan = compile(plan);
		
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
		final int degOfPar = 2 * defaultParallelism;
		
		// construct the plan
		FileDataSource source = new FileDataSource(DummyInputFormat.class, IN_FILE_1, "Source");
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
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, "Sink");
		sink.setDegreeOfParallelism(degOfPar * 2);
		sink.setInput(reduce2);
		
		Plan plan = new Plan(sink, "Test Increasing Degree Of Parallelism");
		
		// submit the plan to the compiler
		OptimizedPlan oPlan = compile(plan);
		
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
	
	
	
//	@Test
//	public void checkPropertyHandlingWithDecreasingDegreeOfParallelism()
//	{
//		final int degOfPar = defaultParallelism;
//		
//		// construct the plan
//		FileDataSourceContract<PactInteger, PactInteger> source = new FileDataSourceContract<PactInteger, PactInteger>(DummyInputFormat.class, IN_FILE_1, "Source");
//		source.setDegreeOfParallelism(degOfPar * 2);
//		
//		MapContract<PactInteger, PactInteger, PactInteger, PactInteger> map1 = new MapContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityMap.class, "Map1");
//		map1.setDegreeOfParallelism(degOfPar * 2);
//		map1.setInput(source);
//		
//		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> reduce1 = new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityReduce.class, "Reduce 1");
//		reduce1.setDegreeOfParallelism(degOfPar * 2);
//		reduce1.setInput(map1);
//		
//		MapContract<PactInteger, PactInteger, PactInteger, PactInteger> map2 = new MapContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityMap.class, "Map2");
//		map2.setDegreeOfParallelism(degOfPar);
//		map2.setInput(reduce1);
//		
//		ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger> reduce2 = new ReduceContract<PactInteger, PactInteger, PactInteger, PactInteger>(IdentityReduce.class, "Reduce 2");
//		reduce2.setDegreeOfParallelism(degOfPar);
//		reduce2.setInput(map2);
//		
//		FileDataSinkContract<PactInteger, PactInteger> sink = new FileDataSinkContract<PactInteger, PactInteger>(DummyOutputFormat.class, OUT_FILE_1, "Sink");
//		sink.setDegreeOfParallelism(degOfPar);
//		sink.setInput(reduce2);
//		
//		Plan plan = new Plan(sink, "Test Increasing Degree Of Parallelism");
//		
//		// submit the plan to the compiler
//		OptimizedPlan oPlan = this.compiler.compile(plan, this.instanceType);
//		
//		// check the optimized Plan
//		// when reducer 1 distributes its data across the instances of map2, it needs to employ a local hash method,
//		// because map2 has twice as many instances and key/value pairs with the same key need to be processed by the same
//		// mapper respectively reducer
//		DataSinkNode sinkNode = oPlan.getDataSinks().iterator().next();
//		ReduceNode red2Node = (ReduceNode) sinkNode.getInputConnection().getSourcePact();
//		
//		Assert.assertEquals("The Reduce 2 Node has an invalid local strategy.", LocalStrategy.SORT, red2Node.getLocalStrategy());
//	}

}
