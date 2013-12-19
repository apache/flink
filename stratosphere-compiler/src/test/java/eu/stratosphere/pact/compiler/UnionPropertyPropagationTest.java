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

import java.util.Iterator;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSink;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.java.record.operators.ReduceOperator;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityReduce;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.types.IntValue;
import eu.stratosphere.util.Visitor;

/**
 */
public class UnionPropertyPropagationTest extends CompilerTestBase {

	@Test
	public void testUnionPropertyPropagation() {
		// construct the plan

		FileDataSource sourceA = new FileDataSource(new DummyInputFormat(), IN_FILE);
		FileDataSource sourceB = new FileDataSource(new DummyInputFormat(), IN_FILE);
		
		ReduceOperator redA = ReduceOperator.builder(new IdentityReduce(), IntValue.class, 0)
			.input(sourceA)
			.build();
		ReduceOperator redB = ReduceOperator.builder(new IdentityReduce(), IntValue.class, 0)
			.input(sourceB)
			.build();
		
		ReduceOperator globalRed = ReduceOperator.builder(new IdentityReduce(), IntValue.class, 0).build();
		globalRed.addInput(redA);
		globalRed.addInput(redB);
		
		FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, globalRed);
		
		// return the PACT plan
		Plan plan = new Plan(sink, "Union Property Propagation");
		
		OptimizedPlan oPlan = compileNoStats(plan);
		
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		
		//Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
		
		oPlan.accept(new Visitor<PlanNode>() {
			
			@Override
			public boolean preVisit(PlanNode visitable) {
				if (visitable instanceof SingleInputPlanNode && visitable.getPactContract() instanceof ReduceOperator) {
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
