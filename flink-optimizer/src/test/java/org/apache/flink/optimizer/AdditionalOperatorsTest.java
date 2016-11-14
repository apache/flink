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
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.Test;

/**
* Tests that validate optimizer choices when using operators that are requesting certain specific execution
* strategies.
*/
@SuppressWarnings({"serial"})
public class AdditionalOperatorsTest extends CompilerTestBase {

	@Test
	public void testCrossWithSmall() {
		// construct the plan
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);
		DataSet<Long> set1 = env.generateSequence(0,1);
		DataSet<Long> set2 = env.generateSequence(0,1);

		set1.crossWithTiny(set2).name("Cross")
				.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

		try {
			Plan plan = env.createProgramPlan();
			OptimizedPlan oPlan = compileWithStats(plan);
			OptimizerPlanNodeResolver resolver = new OptimizerPlanNodeResolver(oPlan);
			
			DualInputPlanNode crossPlanNode = resolver.getNode("Cross");
			Channel in1 = crossPlanNode.getInput1();
			Channel in2 = crossPlanNode.getInput2();
			
			assertEquals(ShipStrategyType.FORWARD, in1.getShipStrategy());
			assertEquals(ShipStrategyType.BROADCAST, in2.getShipStrategy());
		} catch(CompilerException ce) {
			ce.printStackTrace();
			fail("The Flink optimizer is unable to compile this plan correctly.");
		}
	}
	
	@Test
	public void testCrossWithLarge() {
		// construct the plan
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);
		DataSet<Long> set1 = env.generateSequence(0,1);
		DataSet<Long> set2 = env.generateSequence(0,1);

		set1.crossWithHuge(set2).name("Cross")
				.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

		try {
			Plan plan = env.createProgramPlan();
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

