/***********************************************************************************************************************
*
* Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import eu.stratosphere.api.Job;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.record.operators.CrossOperator;
import eu.stratosphere.api.record.operators.CrossWithLargeOperator;
import eu.stratosphere.api.record.operators.CrossWithSmallOperator;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.pact.compiler.util.DummyCrossStub;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;


/**
* Tests that validate optimizer choices when using operators that are requesting certain specific execution
* strategies.
*/
public class AdditionalOperatorsTest extends CompilerTestBase {

	@Test
	public void testCrossWithSmall() {
		// construct the plan
		FileDataSource source1 = new FileDataSource(new DummyInputFormat(), IN_FILE, "Source 1");
		FileDataSource source2 = new FileDataSource(new DummyInputFormat(), IN_FILE, "Source 2");
		
		CrossOperator cross = CrossWithSmallOperator.builder(new DummyCrossStub())
				.input1(source1).input2(source2)
				.name("Cross").build();
	
		FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, cross, "Sink");
		
		Job plan = new Job(sink);
		plan.setDefaultParallelism(DEFAULT_PARALLELISM);
		
		
		try {
			OptimizedPlan oPlan = compileNoStats(plan);
			OptimizerPlanNodeResolver resolver = new OptimizerPlanNodeResolver(oPlan);
			
			DualInputPlanNode crossPlanNode = resolver.getNode("Cross");
			Channel in1 = crossPlanNode.getInput1();
			Channel in2 = crossPlanNode.getInput2();
			
			assertEquals(ShipStrategyType.FORWARD, in1.getShipStrategy());
			assertEquals(ShipStrategyType.BROADCAST, in2.getShipStrategy());
		} catch(CompilerException ce) {
			ce.printStackTrace();
			fail("The pact compiler is unable to compile this plan correctly.");
		}
	}
	
	@Test
	public void testCrossWithLarge() {
		// construct the plan
		FileDataSource source1 = new FileDataSource(new DummyInputFormat(), IN_FILE, "Source 1");
		FileDataSource source2 = new FileDataSource(new DummyInputFormat(), IN_FILE, "Source 2");
		
		CrossOperator cross= CrossWithLargeOperator.builder(new DummyCrossStub())
				.input1(source1).input2(source2)
				.name("Cross").build();
	
		FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, cross, "Sink");
		
		Job plan = new Job(sink);
		plan.setDefaultParallelism(DEFAULT_PARALLELISM);
		
		
		try {
			OptimizedPlan oPlan = compileNoStats(plan);
			OptimizerPlanNodeResolver resolver = new OptimizerPlanNodeResolver(oPlan);
			
			DualInputPlanNode crossPlanNode = resolver.getNode("Cross");
			Channel in1 = crossPlanNode.getInput1();
			Channel in2 = crossPlanNode.getInput2();
			
			assertEquals(ShipStrategyType.BROADCAST, in1.getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, in2.getShipStrategy());
		} catch(CompilerException ce) {
			ce.printStackTrace();
			fail("The pact compiler is unable to compile this plan correctly.");
		}
	}
}

