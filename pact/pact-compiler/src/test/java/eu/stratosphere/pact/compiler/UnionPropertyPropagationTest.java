/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
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

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityReduce;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;

/**
 */
public class UnionPropertyPropagationTest extends CompilerTestBase {

	@Test
	public void testUnionPropertyPropagation() {
		// construct the plan

		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE);
		FileDataSource sourceB = new FileDataSource(DummyInputFormat.class, IN_FILE);
		
		ReduceContract redA = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(sourceA)
			.build();
		ReduceContract redB = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(sourceB)
			.build();
		
		ReduceContract globalRed = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0).build();
		globalRed.addInput(redA);
		globalRed.addInput(redB);
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE, globalRed);
		
		// return the PACT plan
		Plan plan = new Plan(sink, "Union Property Propagation");
		
		OptimizedPlan oPlan = compileNoStats(plan);
		
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		
		//Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
		
		oPlan.accept(new Visitor<PlanNode>() {
			
			@Override
			public boolean preVisit(PlanNode visitable) {
				if (visitable instanceof SingleInputPlanNode && visitable.getPactContract() instanceof ReduceContract) {
					for (Iterator<Channel> inputs = visitable.getInputs(); inputs.hasNext();) {
						final Channel inConn = inputs.next();
						Assert.assertTrue("Reduce should just forward the input if it is already partitioned",
								inConn.getShipStrategy() == ShipStrategyType.FORWARD); 
					}
					//just check latest ReduceNode
					return false;
				}
				return true;
			}
			
			@Override
			public void postVisit(PlanNode visitable) {
				// DO NOTHING
			}
		});
	}
}
