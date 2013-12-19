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

package eu.stratosphere.pact.compiler;

import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.record.operators.CrossOperator;
import eu.stratosphere.api.record.operators.MapOperator;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.compiler.util.DummyCrossStub;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityMap;
import eu.stratosphere.pact.compiler.util.IdentityReduce;
import eu.stratosphere.types.IntValue;

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
	public void testTicket158() {
		// construct the plan
		FileDataSource source = new FileDataSource(new DummyInputFormat(), IN_FILE, "Source");
		
		MapOperator map = MapOperator.builder(new IdentityMap()).name("Map1").input(source).build();
		
		ReduceOperator reduce1 = ReduceOperator.builder(new IdentityReduce(), IntValue.class, 0).name("Reduce1").input(map).build();
		
		CrossOperator cross1 = CrossOperator.builder(new DummyCrossStub()).name("Cross1").input1(reduce1).input2(source).build();
		
		ReduceOperator reduce2 = ReduceOperator.builder(new IdentityReduce(), IntValue.class, 0).name("Reduce2").input(cross1).build();
		
		CrossOperator cross2 = CrossOperator.builder(new DummyCrossStub()).name("Cross2").input1(reduce2).input2(source).build();
		
		ReduceOperator reduce3 = ReduceOperator.builder(new IdentityReduce(), IntValue.class, 0).name("Reduce3").input(cross2).build();
		
		FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, "Sink");
		sink.setInput(reduce3);
		
		Plan plan = new Plan(sink, "Test Temp Task");
		plan.setDefaultParallelism(DEFAULT_PARALLELISM);
		
		OptimizedPlan oPlan = compileNoStats(plan);
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		jobGen.compileJobGraph(oPlan);
	}
}
