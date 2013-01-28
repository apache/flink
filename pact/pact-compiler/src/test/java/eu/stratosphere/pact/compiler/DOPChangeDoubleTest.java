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

import junit.framework.Assert;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.plan.Visitor;
import eu.stratosphere.pact.common.type.base.PactInteger;
import eu.stratosphere.pact.compiler.plan.candidate.Channel;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.PlanNode;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyMatchStub;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityReduce;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;

/**
 */
public class DOPChangeDoubleTest extends CompilerTestBase {
	
	@Test
	public void testPartitionDoPChange() {
		// construct the plan

		FileDataSource sourceA = new FileDataSource(DummyInputFormat.class, IN_FILE_1);
		FileDataSource sourceB = new FileDataSource(DummyInputFormat.class, IN_FILE_1);
		
		ReduceContract redA = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(sourceA)
			.build();
		ReduceContract redB = new ReduceContract.Builder(IdentityReduce.class, PactInteger.class, 0)
			.input(sourceB)
			.build();
		
		MatchContract mat = MatchContract.builder(DummyMatchStub.class, PactInteger.class, 0, 0)
			.input1(redA)
			.input2(redB)
			.build();
		
		FileDataSink sink = new FileDataSink(DummyOutputFormat.class, OUT_FILE_1, mat);
		
		sourceA.setDegreeOfParallelism(5);
		sourceB.setDegreeOfParallelism(7);
		redA.setDegreeOfParallelism(5);
		redB.setDegreeOfParallelism(7);
		
		mat.setDegreeOfParallelism(5);
		
		sink.setDegreeOfParallelism(5);
		
		
		// return the PACT plan
		Plan plan = new Plan(sink, "Partition on DoP Change");
		
		OptimizedPlan oPlan = compile(plan);
		
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		
		//Compile plan to verify that no error is thrown
		jobGen.compileJobGraph(oPlan);
		
		oPlan.accept(new Visitor<PlanNode>() {
			
			@Override
			public boolean preVisit(PlanNode visitable) {
				if (visitable instanceof DualInputPlanNode) {
					DualInputPlanNode node = (DualInputPlanNode) visitable;
					Channel c1 = node.getInput1();
					Channel c2 = node.getInput2();
					
					Assert.assertEquals("Incompatible shipping strategy chosen for match", ShipStrategyType.FORWARD, c1.getShipStrategy());
					Assert.assertEquals("Incompatible shipping strategy chosen for match", ShipStrategyType.PARTITION_HASH, c2.getShipStrategy());
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
