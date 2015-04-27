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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.operators.CrossOperator;
import org.apache.flink.api.java.record.operators.CrossWithLargeOperator;
import org.apache.flink.api.java.record.operators.CrossWithSmallOperator;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.util.DummyCrossStub;
import org.apache.flink.optimizer.util.DummyInputFormat;
import org.apache.flink.optimizer.util.DummyOutputFormat;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.Test;

/**
* Tests that validate optimizer choices when using operators that are requesting certain specific execution
* strategies.
*/
@SuppressWarnings({"serial", "deprecation"})
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
		
		Plan plan = new Plan(sink);
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
		
		Plan plan = new Plan(sink);
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

