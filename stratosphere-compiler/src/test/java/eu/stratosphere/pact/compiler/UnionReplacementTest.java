/*
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
 */

package eu.stratosphere.pact.compiler;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.IterativeDataSet;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import org.junit.Test;
import static org.junit.Assert.fail;

public class UnionReplacementTest extends CompilerTestBase {

	@Test
	public void testUnionReplacement(){
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<String> input1 = env.fromElements("test1");
		DataSet<String> input2 = env.fromElements("test2");

		DataSet<String> union = input1.union(input2);

		union.print();
		union.print();

		Plan plan = env.createProgramPlan();
		try{
			OptimizedPlan oPlan = this.compileNoStats(plan);
			NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
			jobGen.compileJobGraph(oPlan);
		}catch(CompilerException co){
			co.printStackTrace();
			fail("The Pact compiler is unable to compile this plan correctly.");
		}
	}
}
