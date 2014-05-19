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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Iterator;

import org.junit.Test;

import eu.stratosphere.api.common.InvalidProgramException;
import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.util.Collector;

/**
* Tests that validate optimizer choices when using operators that are requesting certain specific execution
* strategies.
*/
@SuppressWarnings("serial")
public class WorksetIterationsJavaApiCompilerTest extends CompilerTestBase {
	
	private static final String JOIN_WITH_INVARIANT_NAME = "Test Join Invariant";
	private static final String JOIN_WITH_SOLUTION_SET = "Test Join SolutionSet";
	private static final String NEXT_WORKSET_REDUCER_NAME = "Test Reduce Workset";
	private static final String SOLUTION_DELTA_MAPPER_NAME = "Test Map Delta";

	@Test
	public void testJavaApiWithDeferredSoltionSetUpdateWithMapper() {
		try {
			Plan plan = getJavaTestPlan(false, true);
			
			OptimizedPlan oPlan = compileNoStats(plan);
	
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
			assertEquals(new FieldList(1, 2), joinWithInvariantNode.getKeysForInput1());
			assertEquals(new FieldList(1, 2), joinWithInvariantNode.getKeysForInput2());
			
			// verify joinWithSolutionSet
			assertEquals(ShipStrategyType.PARTITION_HASH, joinWithSolutionSetNode.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput2().getShipStrategy());
			assertEquals(new FieldList(0, 1), joinWithSolutionSetNode.getKeysForInput1());
			
			
			// verify reducer
			assertEquals(ShipStrategyType.PARTITION_HASH, worksetReducer.getInput().getShipStrategy());
			assertEquals(new FieldList(1, 2), worksetReducer.getKeys());
			
			// currently, the system may partition before or after the mapper
			ShipStrategyType ss1 = deltaMapper.getInput().getShipStrategy();
			ShipStrategyType ss2 = deltaMapper.getOutgoingChannels().get(0).getShipStrategy();
			
			assertTrue( (ss1 == ShipStrategyType.FORWARD && ss2 == ShipStrategyType.PARTITION_HASH) ||
						(ss2 == ShipStrategyType.FORWARD && ss1 == ShipStrategyType.PARTITION_HASH) );
		
			new NepheleJobGraphGenerator().compileJobGraph(oPlan);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test errored: " + e.getMessage());
		}
	}
	
	@Test
	public void testRecordApiWithDeferredSoltionSetUpdateWithNonPreservingJoin() {
		try {
			Plan plan = getJavaTestPlan(false, false);
			
			OptimizedPlan oPlan = compileNoStats(plan);
			
			OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
			DualInputPlanNode joinWithInvariantNode = resolver.getNode(JOIN_WITH_INVARIANT_NAME);
			DualInputPlanNode joinWithSolutionSetNode = resolver.getNode(JOIN_WITH_SOLUTION_SET);
			SingleInputPlanNode worksetReducer = resolver.getNode(NEXT_WORKSET_REDUCER_NAME);
			
			// iteration preserves partitioning in reducer, so the first partitioning is out of the loop, 
			// the in-loop partitioning is before the final reducer
			
			// verify joinWithInvariant
			assertEquals(ShipStrategyType.FORWARD, joinWithInvariantNode.getInput1().getShipStrategy()); 
			assertEquals(ShipStrategyType.PARTITION_HASH, joinWithInvariantNode.getInput2().getShipStrategy());
			assertEquals(new FieldList(1, 2), joinWithInvariantNode.getKeysForInput1());
			assertEquals(new FieldList(1, 2), joinWithInvariantNode.getKeysForInput2());
			
			// verify joinWithSolutionSet
			assertEquals(ShipStrategyType.PARTITION_HASH, joinWithSolutionSetNode.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput2().getShipStrategy());
			assertEquals(new FieldList(0, 1), joinWithSolutionSetNode.getKeysForInput1());
			
			// verify reducer
			assertEquals(ShipStrategyType.PARTITION_HASH, worksetReducer.getInput().getShipStrategy());
			assertEquals(new FieldList(1, 2), worksetReducer.getKeys());
			
			// verify solution delta
			assertEquals(2, joinWithSolutionSetNode.getOutgoingChannels().size());
			assertEquals(ShipStrategyType.PARTITION_HASH, joinWithSolutionSetNode.getOutgoingChannels().get(0).getShipStrategy());
			assertEquals(ShipStrategyType.PARTITION_HASH, joinWithSolutionSetNode.getOutgoingChannels().get(1).getShipStrategy());
			
			new NepheleJobGraphGenerator().compileJobGraph(oPlan);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test errored: " + e.getMessage());
		}
	}
	
	@Test
	public void testRecordApiWithDirectSoltionSetUpdate() {
		try {
			Plan plan = getJavaTestPlan(true, false);
			
			OptimizedPlan oPlan = compileNoStats(plan);
	
			
			OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
			DualInputPlanNode joinWithInvariantNode = resolver.getNode(JOIN_WITH_INVARIANT_NAME);
			DualInputPlanNode joinWithSolutionSetNode = resolver.getNode(JOIN_WITH_SOLUTION_SET);
			SingleInputPlanNode worksetReducer = resolver.getNode(NEXT_WORKSET_REDUCER_NAME);
			
			// iteration preserves partitioning in reducer, so the first partitioning is out of the loop, 
			// the in-loop partitioning is before the final reducer
			
			// verify joinWithInvariant
			assertEquals(ShipStrategyType.FORWARD, joinWithInvariantNode.getInput1().getShipStrategy()); 
			assertEquals(ShipStrategyType.PARTITION_HASH, joinWithInvariantNode.getInput2().getShipStrategy());
			assertEquals(new FieldList(1, 2), joinWithInvariantNode.getKeysForInput1());
			assertEquals(new FieldList(1, 2), joinWithInvariantNode.getKeysForInput2());
			
			// verify joinWithSolutionSet
			assertEquals(ShipStrategyType.PARTITION_HASH, joinWithSolutionSetNode.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, joinWithSolutionSetNode.getInput2().getShipStrategy());
			assertEquals(new FieldList(0, 1), joinWithSolutionSetNode.getKeysForInput1());
			
			// verify reducer
			assertEquals(ShipStrategyType.FORWARD, worksetReducer.getInput().getShipStrategy());
			assertEquals(new FieldList(1, 2), worksetReducer.getKeys());
			
			
			// verify solution delta
			assertEquals(1, joinWithSolutionSetNode.getOutgoingChannels().size());
			assertEquals(ShipStrategyType.FORWARD, joinWithSolutionSetNode.getOutgoingChannels().get(0).getShipStrategy());
			
			new NepheleJobGraphGenerator().compileJobGraph(oPlan);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test errored: " + e.getMessage());
		}
	}
	
	
	@Test
	public void testRejectPlanIfSolutionSetKeysAndJoinKeysDontMatch() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.setDegreeOfParallelism(DEFAULT_PARALLELISM);
			
			@SuppressWarnings("unchecked")
			DataSet<Tuple3<Long, Long, Long>> solutionSetInput = env.fromElements(new Tuple3<Long, Long, Long>(1L, 2L, 3L)).name("Solution Set");
			@SuppressWarnings("unchecked")
			DataSet<Tuple3<Long, Long, Long>> worksetInput = env.fromElements(new Tuple3<Long, Long, Long>(1L, 2L, 3L)).name("Workset");
			@SuppressWarnings("unchecked")
			DataSet<Tuple3<Long, Long, Long>> invariantInput = env.fromElements(new Tuple3<Long, Long, Long>(1L, 2L, 3L)).name("Invariant Input");
			
			DeltaIteration<Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>> iter = solutionSetInput.iterateDelta(worksetInput, 100, 1, 2);
			
			
			DataSet<Tuple3<Long, Long, Long>> result = 
			
			iter.getWorkset().join(invariantInput)
				.where(1, 2)
				.equalTo(1, 2)
				.with(new JoinFunction<Tuple3<Long,Long,Long>, Tuple3<Long, Long, Long>, Tuple3<Long,Long,Long>>() {
					public Tuple3<Long, Long, Long> join(Tuple3<Long, Long, Long> first, Tuple3<Long, Long, Long> second) {
						return first;
					}
				});
			
			try {
			result.join(iter.getSolutionSet())
				.where(1, 0)
				.equalTo(0, 2)
				.with(new JoinFunction<Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>>() {
					public Tuple3<Long, Long, Long> join(Tuple3<Long, Long, Long> first, Tuple3<Long, Long, Long> second) {
						return second;
					}
				});
				fail("The join should be rejected with key type mismatches.");
			}
			catch (InvalidProgramException e) {
				// expected!
			}
			
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test errored: " + e.getMessage());
		}
	}
	
	private Plan getJavaTestPlan(boolean joinPreservesSolutionSet, boolean mapBeforeSolutionDelta) {
		
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(DEFAULT_PARALLELISM);
		
		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> solutionSetInput = env.fromElements(new Tuple3<Long, Long, Long>(1L, 2L, 3L)).name("Solution Set");
		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> worksetInput = env.fromElements(new Tuple3<Long, Long, Long>(1L, 2L, 3L)).name("Workset");
		@SuppressWarnings("unchecked")
		DataSet<Tuple3<Long, Long, Long>> invariantInput = env.fromElements(new Tuple3<Long, Long, Long>(1L, 2L, 3L)).name("Invariant Input");
		
		DeltaIteration<Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>> iter = solutionSetInput.iterateDelta(worksetInput, 100, 1, 2);
		
		
		DataSet<Tuple3<Long, Long, Long>> joinedWithSolutionSet = 
		
		iter.getWorkset().join(invariantInput)
			.where(1, 2)
			.equalTo(1, 2)
			.with(new JoinFunction<Tuple3<Long,Long,Long>, Tuple3<Long, Long, Long>, Tuple3<Long,Long,Long>>() {
				public Tuple3<Long, Long, Long> join(Tuple3<Long, Long, Long> first, Tuple3<Long, Long, Long> second) {
					return first;
				}
			})
			.name(JOIN_WITH_INVARIANT_NAME)
		
		.join(iter.getSolutionSet())
			.where(1, 0)
			.equalTo(1, 2)
			.with(new JoinFunction<Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>, Tuple3<Long, Long, Long>>() {
				public Tuple3<Long, Long, Long> join(Tuple3<Long, Long, Long> first, Tuple3<Long, Long, Long> second) {
					return second;
				}
			})
			.name(JOIN_WITH_SOLUTION_SET)
			.withConstantSetSecond(joinPreservesSolutionSet ? new String[] {"0->0", "1->1", "2->2" } : null);
			
		DataSet<Tuple3<Long, Long, Long>> nextWorkset = joinedWithSolutionSet.groupBy(1, 2)
			.reduceGroup(new GroupReduceFunction<Tuple3<Long,Long,Long>, Tuple3<Long,Long,Long>>() {
				public void reduce(Iterator<Tuple3<Long, Long, Long>> values, Collector<Tuple3<Long, Long, Long>> out) {}
			})
			.name(NEXT_WORKSET_REDUCER_NAME)
			.withConstantSet("1->1","2->2","0->0");
		
		
		DataSet<Tuple3<Long, Long, Long>> nextSolutionSet = mapBeforeSolutionDelta ?
				joinedWithSolutionSet.map(new MapFunction<Tuple3<Long, Long, Long>,Tuple3<Long, Long, Long>>() { public Tuple3<Long, Long, Long> map(Tuple3<Long, Long, Long> value) { return value; } })
					.name(SOLUTION_DELTA_MAPPER_NAME).withConstantSet("0->0","1->1","2->2") :
				joinedWithSolutionSet;
		
		iter.closeWith(nextSolutionSet, nextWorkset)
			.print();
		
		return env.createProgramPlan();
	}
}

