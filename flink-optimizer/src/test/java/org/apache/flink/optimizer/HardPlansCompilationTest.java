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
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.testfunctions.IdentityCrosser;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducer;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.Test;

/**
 * This class tests plans that once failed because of a bug:
 * <ul>
 *   <li> Ticket 158
 * </ul>
 */
@SuppressWarnings({"serial"})
public class HardPlansCompilationTest extends CompilerTestBase {
	
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
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);
		DataSet<Long> set1 = env.generateSequence(0,1);

		set1.map(new IdentityMapper<Long>()).name("Map1")
				.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>()).name("Reduce1")
				.cross(set1).with(new IdentityCrosser<Long>()).withForwardedFieldsFirst("*").name("Cross1")
				.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>()).name("Reduce2")
				.cross(set1).with(new IdentityCrosser<Long>()).name("Cross2")
				.groupBy("*").reduceGroup(new IdentityGroupReducer<Long>()).name("Reduce3")
				.output(new DiscardingOutputFormat<Long>()).name("Sink");

		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileNoStats(plan);

		JobGraphGenerator jobGen = new JobGraphGenerator();
		jobGen.compileJobGraph(oPlan);
	}
}
