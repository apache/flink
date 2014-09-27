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


package org.apache.flink.compiler;

import static org.junit.Assert.*;

import org.apache.flink.api.common.Plan;
import org.junit.Test;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.compiler.dag.TempMode;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plan.SingleInputPlanNode;
import org.apache.flink.compiler.plan.SinkPlanNode;
import org.apache.flink.compiler.testfunctions.IdentityMapper;

@SuppressWarnings("serial")
public class BroadcastVariablePipelinebreakerTest extends CompilerTestBase {

	@Test
	public void testNoBreakerForIndependentVariable() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<String> source1 = env.fromElements("test");
			DataSet<String> source2 = env.fromElements("test");
			
			source1.map(new IdentityMapper<String>()).withBroadcastSet(source2, "some name").print();
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			SinkPlanNode sink = op.getDataSinks().iterator().next();
			SingleInputPlanNode mapper = (SingleInputPlanNode) sink.getInput().getSource();
			
			assertEquals(TempMode.NONE, mapper.getInput().getTempMode());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
	
	 @Test
	public void testBreakerForDependentVariable() {
			try {
				ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
				
				DataSet<String> source1 = env.fromElements("test");
				
				source1.map(new IdentityMapper<String>()).map(new IdentityMapper<String>()).withBroadcastSet(source1, "some name").print();
				
				Plan p = env.createProgramPlan();
				OptimizedPlan op = compileNoStats(p);
				
				SinkPlanNode sink = op.getDataSinks().iterator().next();
				SingleInputPlanNode mapper = (SingleInputPlanNode) sink.getInput().getSource();
				
				assertEquals(TempMode.PIPELINE_BREAKER, mapper.getInput().getTempMode());
			}
			catch (Exception e) {
				e.printStackTrace();
				fail(e.getMessage());
			}
	}
}
