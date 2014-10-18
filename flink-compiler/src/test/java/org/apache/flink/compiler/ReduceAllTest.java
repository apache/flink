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

import static org.junit.Assert.fail;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plantranslate.NepheleJobGraphGenerator;
import org.apache.flink.compiler.util.DummyInputFormat;
import org.apache.flink.compiler.util.DummyOutputFormat;
import org.apache.flink.compiler.util.IdentityReduce;
import org.junit.Test;

/**
 * This test case has been created to validate a bug that occurred when
 * the ReduceOperator was used without a grouping key.
 */
@SuppressWarnings({"serial", "deprecation"})
public class ReduceAllTest extends CompilerTestBase {

	@Test
	public void testReduce() {
		// construct the plan
		FileDataSource source = new FileDataSource(new DummyInputFormat(), IN_FILE, "Source");
		ReduceOperator reduce1 = ReduceOperator.builder(new IdentityReduce()).name("Reduce1").input(source).build();
		FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, "Sink");
		sink.setInput(reduce1);
		Plan plan = new Plan(sink, "AllReduce Test");
		plan.setDefaultParallelism(DEFAULT_PARALLELISM);
		
		
		try {
			OptimizedPlan oPlan = compileNoStats(plan);
			NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
			jobGen.compileJobGraph(oPlan);
		} catch(CompilerException ce) {
			ce.printStackTrace();
			fail("The pact compiler is unable to compile this plan correctly");
		}
	}
}
