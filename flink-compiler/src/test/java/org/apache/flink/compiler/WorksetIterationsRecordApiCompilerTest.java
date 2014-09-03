/**
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.record.operators.DeltaIteration;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.JoinOperator;
import org.apache.flink.api.java.record.operators.MapOperator;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.compiler.CompilerException;
import org.apache.flink.compiler.plan.DualInputPlanNode;
import org.apache.flink.compiler.plan.OptimizedPlan;
import org.apache.flink.compiler.plan.SingleInputPlanNode;
import org.apache.flink.compiler.plantranslate.NepheleJobGraphGenerator;
import org.apache.flink.compiler.util.DummyInputFormat;
import org.apache.flink.compiler.util.DummyMatchStub;
import org.apache.flink.compiler.util.DummyNonPreservingMatchStub;
import org.apache.flink.compiler.util.DummyOutputFormat;
import org.apache.flink.compiler.util.IdentityMap;
import org.apache.flink.compiler.util.IdentityReduce;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.types.LongValue;
import org.junit.Test;


/**
* Tests that validate optimizer choices when using operators that are requesting certain specific execution
* strategies.
*/
public class WorksetIterationsRecordApiCompilerTest extends CompilerTestBase {
	
	private static final long serialVersionUID = 1L;
	
	private static final String ITERATION_NAME = "Test Workset Iteration";
	private static final String JOIN_WITH_INVARIANT_NAME = "Test Join Invariant";
	private static final String JOIN_WITH_SOLUTION_SET = "Test Join SolutionSet";
	private static final String NEXT_WORKSET_REDUCER_NAME = "Test Reduce Workset";
	private static final String SOLUTION_DELTA_MAPPER_NAME = "Test Map Delta";
	
	private final FieldList list0 = new FieldList(0);

	@Test
	public void testRecordApiWithDeferredSoltionSetUpdateWithMapper() {
		Plan plan = getRecordTestPlan(false, true);
		
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
	public void testRecordApiWithDeferredSoltionSetUpdateWithNonPreservingJoin() {
		Plan plan = getRecordTestPlan(false, false);
		
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
	public void testRecordApiWithDirectSoltionSetUpdate() {
		Plan plan = getRecordTestPlan(true, false);
		
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
	
	private Plan getRecordTestPlan(boolean joinPreservesSolutionSet, boolean mapBeforeSolutionDelta) {
		FileDataSource solutionSetInput = new FileDataSource(new DummyInputFormat(), IN_FILE, "Solution Set");
		FileDataSource worksetInput = new FileDataSource(new DummyInputFormat(), IN_FILE, "Workset");
		
		FileDataSource invariantInput = new FileDataSource(new DummyInputFormat(), IN_FILE, "Invariant Input");
		
		DeltaIteration iteration = new DeltaIteration(0, ITERATION_NAME);
		iteration.setInitialSolutionSet(solutionSetInput);
		iteration.setInitialWorkset(worksetInput);
		iteration.setMaximumNumberOfIterations(100);

		JoinOperator joinWithInvariant = JoinOperator.builder(new DummyMatchStub(), LongValue.class, 0, 0)
				.input1(iteration.getWorkset())
				.input2(invariantInput)
				.name(JOIN_WITH_INVARIANT_NAME)
				.build();

		JoinOperator joinWithSolutionSet = JoinOperator.builder(
				joinPreservesSolutionSet ? new DummyMatchStub() : new DummyNonPreservingMatchStub(), LongValue.class, 0, 0)
				.input1(iteration.getSolutionSet())
				.input2(joinWithInvariant)
				.name(JOIN_WITH_SOLUTION_SET)
				.build();
		
		ReduceOperator nextWorkset = ReduceOperator.builder(new IdentityReduce(), LongValue.class, 0)
				.input(joinWithSolutionSet)
				.name(NEXT_WORKSET_REDUCER_NAME)
				.build();
		
		if (mapBeforeSolutionDelta) {
			MapOperator mapper = MapOperator.builder(new IdentityMap())
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

