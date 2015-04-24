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

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.Test;

import static org.junit.Assert.fail;

@SuppressWarnings("serial")
public class UnionReplacementTest extends CompilerTestBase {

	@Test
	public void testUnionReplacement() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			DataSet<String> input1 = env.fromElements("test1");
			DataSet<String> input2 = env.fromElements("test2");
	
			DataSet<String> union = input1.union(input2);
	
			union.output(new DiscardingOutputFormat<String>());
			union.output(new DiscardingOutputFormat<String>());
	
			Plan plan = env.createProgramPlan();
			OptimizedPlan oPlan = compileNoStats(plan);
			JobGraphGenerator jobGen = new JobGraphGenerator();
			jobGen.compileJobGraph(oPlan);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
