/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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

import org.junit.Test;

import eu.stratosphere.pact.common.contract.CrossContract;
import eu.stratosphere.pact.common.contract.GenericDataSink;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.example.iterative.IterativeKMeans;
import eu.stratosphere.pact.example.iterative.WorksetConnectedComponents;
import eu.stratosphere.pact.generic.contract.BulkIteration;


/**
 *
 */
public class IterativeJobCompilerTest extends CompilerTestBase
{
	// --------------------------------------------------------------------------------------------
	//  K-Means (Bulk Iteration)
	// --------------------------------------------------------------------------------------------
	
//	@Test
	public void testCompileKMeansIteration1() {
		IterativeKMeans kmi = new IterativeKMeans();

		Plan plan = kmi.getPlan(String.valueOf(DEFAULT_PARALLELISM),
				IN_FILE_1, IN_FILE_1, OUT_FILE_1, String.valueOf(20));
		setParameterToCross(plan, "INPUT_LEFT_SHIP_STRATEGY", "SHIP_FORWARD");

		OptimizedPlan op = compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		jgg.compileJobGraph(op);
	}
	
//	@Test
	public void testCompileKMeansIteration2() {
		IterativeKMeans kmi = new IterativeKMeans();

		Plan plan = kmi.getPlan(String.valueOf(DEFAULT_PARALLELISM),
				IN_FILE_1, IN_FILE_1, OUT_FILE_1, String.valueOf(20));
		setParameterToCross(plan, "INPUT_RIGHT_SHIP_STRATEGY", "SHIP_FORWARD");

		OptimizedPlan op = compile(plan);

		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		jgg.compileJobGraph(op);
	}
	
	public static void setParameterToCross(Plan p, String key, String value) {
		GenericDataSink sink = p.getDataSinks().iterator().next();
		BulkIteration iter = (BulkIteration) sink.getInputs().get(0);
		ReduceContract reduce2 = (ReduceContract) iter.getNextPartialSolution();
		ReduceContract reduce1 = (ReduceContract) reduce2.getInputs().get(0);
		CrossContract cross = (CrossContract) reduce1.getInputs().get(0);
		cross.getParameters().setString(key, value);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Connected Components (Workset Iteration)
	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testCompileConnectedComponents() {
		WorksetConnectedComponents cc = new WorksetConnectedComponents();

		Plan plan = cc.getPlan(String.valueOf(DEFAULT_PARALLELISM),
				IN_FILE_1, IN_FILE_1, OUT_FILE_1, String.valueOf(100));

		OptimizedPlan op = compile(plan);
		NepheleJobGraphGenerator jgg = new NepheleJobGraphGenerator();
		jgg.compileJobGraph(op);
	}
}
