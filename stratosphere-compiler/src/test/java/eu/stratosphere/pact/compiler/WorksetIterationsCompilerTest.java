/***********************************************************************************************************************
*
* Copyright (C) 2013 by the Stratosphere project (http://stratosphere.eu)
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

import static org.junit.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import eu.stratosphere.pact.common.contract.FileDataSink;
import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.contract.MapContract;
import eu.stratosphere.pact.common.contract.MatchContract;
import eu.stratosphere.pact.common.contract.ReduceContract;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.type.base.PactLong;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.compiler.util.DummyInputFormat;
import eu.stratosphere.pact.compiler.util.DummyMatchStub;
import eu.stratosphere.pact.compiler.util.DummyNonPreservingMatchStub;
import eu.stratosphere.pact.compiler.util.DummyOutputFormat;
import eu.stratosphere.pact.compiler.util.IdentityMap;
import eu.stratosphere.pact.compiler.util.IdentityReduce;
import eu.stratosphere.pact.generic.contract.WorksetIteration;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;


/**
* Tests that validate optimizer choices when using operators that are requesting certain specific execution
* strategies.
*/
public class WorksetIterationsCompilerTest extends CompilerTestBase {
	
	private static final String ITERATION_NAME = "Test Workset Iteration";
	private static final String JOIN_WITH_INVARIANT_NAME = "Test Join Invariant";
	private static final String JOIN_WITH_SOLUTION_SET = "Test Join SolutionSet";
	private static final String NEXT_WORKSET_REDUCER_NAME = "Test Reduce Workset";
	private static final String SOLUTION_DELTA_MAPPER_NAME = "Test Map Delta";
	
	private final FieldList list0 = new FieldList(0);

	@Test
	public void testWithDeferredSoltionSetUpdateWithMapper() {
		Plan plan = getTestPlan(false, true);
		
		OptimizedPlan oPlan;
		try {
			oPlan = compileNoStats(plan);
		} catch(CompilerException ce) {
			ce.printStackTrace();
			fail("The pact compiler is unable to compile this plan correctly.");
			return; // silence the compiler
		}
		
		OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
		DualInputPlanNode joinWithInvariantNode = resolver.getNode(JOIN_WITH_INVARIANT_NAME);
		DualInputPlanNode joinWithSolutionSetNode = resolver.getNode(JOIN_WITH_SOLUTION_SET);
		SingleInputPlanNode worksetReducer = resolver.getNode(NEXT_WORKSET_REDUCER_NAME);
		SingleInputPlanNode deltaMapper = resolver.getNode(SOLUTION_DELTA_MAPPER_NAME);
		
		// iteration preserves partitioning in reducer, so the first partitioning is out of the loop, 
		// the in-loop partitioning is before the final reducer
		
		// verify joinWithInvariant
		assertEquals(ShipStrategyType.FORWARD, joinWithInvariantNode.getInput1().getShipStrategy()); 
		assertEquals(ShipStrategyType.PARTITION_HASH, joinWithInvariantNode.getInput2().getShipStrategy());
		assertEquals(list0, joinWithInvariantNode.getKeysForInput1());
		assertEquals(list0, joinWithInvariantNode.getKeysForInput2());
		
		// verify joinWithSolutionSet
		assertEquals(ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput1().getShipStrategy());
		assertEquals(ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput2().getShipStrategy());
		
		// verify reducer
		assertEquals(ShipStrategyType.PARTITION_HASH, worksetReducer.getInput().getShipStrategy());
		assertEquals(list0, worksetReducer.getKeys());
		
		// currently, the system may partition before or after the mapper
		ShipStrategyType ss1 = deltaMapper.getInput().getShipStrategy();
		ShipStrategyType ss2 = deltaMapper.getOutgoingChannels().get(0).getShipStrategy();
		
		assertTrue( (ss1 == ShipStrategyType.FORWARD && ss2 == ShipStrategyType.PARTITION_HASH) ||
					(ss2 == ShipStrategyType.FORWARD && ss1 == ShipStrategyType.PARTITION_HASH) );
		
		new NepheleJobGraphGenerator().compileJobGraph(oPlan);
	}
	
	@Test
	public void testWithDeferredSoltionSetUpdateWithNonPreservingJoin() {
		Plan plan = getTestPlan(false, false);
		
		OptimizedPlan oPlan;
		try {
			oPlan = compileNoStats(plan);
		} catch(CompilerException ce) {
			ce.printStackTrace();
			fail("The pact compiler is unable to compile this plan correctly.");
			return; // silence the compiler
		}
		
		OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
		DualInputPlanNode joinWithInvariantNode = resolver.getNode(JOIN_WITH_INVARIANT_NAME);
		DualInputPlanNode joinWithSolutionSetNode = resolver.getNode(JOIN_WITH_SOLUTION_SET);
		SingleInputPlanNode worksetReducer = resolver.getNode(NEXT_WORKSET_REDUCER_NAME);
		
		// iteration preserves partitioning in reducer, so the first partitioning is out of the loop, 
		// the in-loop partitioning is before the final reducer
		
		// verify joinWithInvariant
		assertEquals(ShipStrategyType.FORWARD, joinWithInvariantNode.getInput1().getShipStrategy()); 
		assertEquals(ShipStrategyType.PARTITION_HASH, joinWithInvariantNode.getInput2().getShipStrategy());
		assertEquals(list0, joinWithInvariantNode.getKeysForInput1());
		assertEquals(list0, joinWithInvariantNode.getKeysForInput2());
		
		// verify joinWithSolutionSet
		assertEquals(ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput1().getShipStrategy());
		assertEquals(ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput2().getShipStrategy());
		
		// verify reducer
		assertEquals(ShipStrategyType.PARTITION_HASH, worksetReducer.getInput().getShipStrategy());
		assertEquals(list0, worksetReducer.getKeys());
		
		
		// verify solution delta
		assertEquals(2, joinWithSolutionSetNode.getOutgoingChannels().size());
		assertEquals(ShipStrategyType.PARTITION_HASH, joinWithSolutionSetNode.getOutgoingChannels().get(0).getShipStrategy());
		assertEquals(ShipStrategyType.PARTITION_HASH, joinWithSolutionSetNode.getOutgoingChannels().get(1).getShipStrategy());
		
		new NepheleJobGraphGenerator().compileJobGraph(oPlan);
	}
	
	@Test
	public void testWithDirectSoltionSetUpdate() {
		Plan plan = getTestPlan(true, false);
		
		OptimizedPlan oPlan;
		try {
			oPlan = compileNoStats(plan);
		} catch(CompilerException ce) {
			ce.printStackTrace();
			fail("The pact compiler is unable to compile this plan correctly.");
			return; // silence the compiler
		}
		
		OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
		DualInputPlanNode joinWithInvariantNode = resolver.getNode(JOIN_WITH_INVARIANT_NAME);
		DualInputPlanNode joinWithSolutionSetNode = resolver.getNode(JOIN_WITH_SOLUTION_SET);
		SingleInputPlanNode worksetReducer = resolver.getNode(NEXT_WORKSET_REDUCER_NAME);
		
		// iteration preserves partitioning in reducer, so the first partitioning is out of the loop, 
		// the in-loop partitioning is before the final reducer
		
		// verify joinWithInvariant
		assertEquals(ShipStrategyType.FORWARD, joinWithInvariantNode.getInput1().getShipStrategy()); 
		assertEquals(ShipStrategyType.PARTITION_HASH, joinWithInvariantNode.getInput2().getShipStrategy());
		assertEquals(list0, joinWithInvariantNode.getKeysForInput1());
		assertEquals(list0, joinWithInvariantNode.getKeysForInput2());
		
		// verify joinWithSolutionSet
		assertEquals(ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput1().getShipStrategy());
		assertEquals(ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput2().getShipStrategy());
		
		// verify reducer
		assertEquals(ShipStrategyType.FORWARD, worksetReducer.getInput().getShipStrategy());
		assertEquals(list0, worksetReducer.getKeys());
		
		
		// verify solution delta
		assertEquals(1, joinWithSolutionSetNode.getOutgoingChannels().size());
		assertEquals(ShipStrategyType.FORWARD, joinWithSolutionSetNode.getOutgoingChannels().get(0).getShipStrategy());
		
		new NepheleJobGraphGenerator().compileJobGraph(oPlan);
	}
	
	private Plan getTestPlan(boolean joinPreservesSolutionSet, boolean mapBeforeSolutionDelta) {
		FileDataSource solutionSetInput = new FileDataSource(new DummyInputFormat(), IN_FILE, "Solution Set");
		FileDataSource worksetInput = new FileDataSource(new DummyInputFormat(), IN_FILE, "Workset");
		
		FileDataSource invariantInput = new FileDataSource(new DummyInputFormat(), IN_FILE, "Invariant Input");
		
		WorksetIteration iteration = new WorksetIteration(0, ITERATION_NAME);
		iteration.setInitialSolutionSet(solutionSetInput);
		iteration.setInitialWorkset(worksetInput);
		iteration.setMaximumNumberOfIterations(100);

		MatchContract joinWithInvariant = MatchContract.builder(new DummyMatchStub(), PactLong.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(invariantInput)
				.name(JOIN_WITH_INVARIANT_NAME)
				.build();

		MatchContract joinWithSolutionSet = MatchContract.builder(
				joinPreservesSolutionSet ? new DummyMatchStub() : new DummyNonPreservingMatchStub(), PactLong.class, 0, 0)
				.input1(iteration.getSolutionSet())
				.input2(joinWithInvariant)
				.name(JOIN_WITH_SOLUTION_SET)
				.build();
		
		ReduceContract nextWorkset = ReduceContract.builder(new IdentityReduce(), PactLong.class, 0)
				.input(joinWithSolutionSet)
				.name(NEXT_WORKSET_REDUCER_NAME)
				.build();
		
		if (mapBeforeSolutionDelta) {
			MapContract mapper = MapContract.builder(new IdentityMap())
				.input(joinWithSolutionSet)
				.name(SOLUTION_DELTA_MAPPER_NAME)
				.build();
			iteration.setSolutionSetDelta(mapper);
		} else {
			iteration.setSolutionSetDelta(joinWithSolutionSet);
		}
		
		iteration.setNextWorkset(nextWorkset);

		FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, iteration, "Sink");
		
		Plan plan = new Plan(sink);
		plan.setDefaultParallelism(DEFAULT_PARALLELISM);
		return plan;
	}
}

