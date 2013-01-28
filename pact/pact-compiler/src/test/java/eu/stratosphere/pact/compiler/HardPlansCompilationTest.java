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

import org.junit.Test;

import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.compiler.util.DummyCrossStub;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityMap;
import eu.stratosphere.pact.compiler.util.IdentityReduce;

/**
 * This class tests plans that once failed because of a bug:
 * <ul>
 *   <li> Ticket 158
 * </ul>
 */
public class HardPlansCompilationTest extends CompilerTestBase
{
	
	/**
	 * Source -> Map -> Reduce -> Cross -> Reduce -> Cross -> Reduce ->
     * |--------------------------/                  /
     * |--------------------------------------------/
     * 
     * First cross has SameKeyFirst output contract
	 */
	@Test
	public void testTicket158()
	{
		// construct the plan
		FileDataSource source = new FileDataSource(DummyInputFormat.class, IN_FILE_1, "Source");
		
		MapContract map = MapContract.builder(IdentityMap.class).name("Map1").input(source).build();
		
		ReduceContract reduce1 = ReduceContract.builder(IdentityReduce.class, PactInteger.class, 0).name("Reduce1").input(map).build();
		
		CrossContract cross1 = CrossContract.builder(DummyCrossStub.class).name("Cross1").input1(reduce1).input2(source).build();
		
		ReduceContract reduce2 = ReduceContract.builder(IdentityReduce.class, PactInteger.class, 0).name("Reduce2").input(cross1).build();
		
		CrossContract cross2 = CrossContract.builder(DummyCrossStub.class).name("Cross2").input1(reduce2).input2(source).build();
		
		ReduceContract reduce3 = ReduceContract.builder(IdentityReduce.class, PactInteger.class, 0).name("Reduce3").input(cross2).build();
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, "Sink");
		sink.setInput(reduce3);
		
		Plan plan = new Plan(sink, "Test Temp Task");
		plan.setDefaultParallelism(defaultParallelism);
		
		OptimizedPlan oPlan = compile(plan);
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		jobGen.compileJobGraph(oPlan);
	}
}
