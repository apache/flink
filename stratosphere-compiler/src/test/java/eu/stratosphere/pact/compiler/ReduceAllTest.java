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

import static org.junit.Assert.fail;

import org.junit.Test;

import eu.stratosphere.api.Plan;
import eu.stratosphere.api.operators.FileDataSink;
import eu.stratosphere.api.operators.FileDataSource;
import eu.stratosphere.api.record.operators.ReduceOperator;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityReduce;

/**
 * This test case has been created to validate a bug that occurred when
 * the ReduceOperator was used without a grouping key.
 */
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
