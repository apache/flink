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

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.DeltaIteration;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.CoGroupFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.compiler.CompilerException;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.WorksetIterationPlanNode;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.util.Collector;
import eu.stratosphere.util.Visitor;

import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;

@SuppressWarnings("serial")
public class CoGroupSolutionSetFirstTest extends CompilerTestBase {
	
	public static class SimpleCGroup extends CoGroupFunction<Tuple1<Integer>, Tuple1<Integer>, Tuple1<Integer>> {
		@Override
		public void coGroup(Iterator<Tuple1<Integer>> first, Iterator<Tuple1<Integer>> second, Collector<Tuple1<Integer>> out) throws Exception {
		}
	}

	public static class SimpleMap extends MapFunction<Tuple1<Integer>, Tuple1<Integer>> {
		@Override
		public Tuple1<Integer> map(Tuple1<Integer> value) throws Exception {
			return null;
		}
	}

	@Test
	public void testCoGroupSolutionSet() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple1<Integer>> raw = env.readCsvFile(IN_FILE).types(Integer.class);

		DeltaIteration<Tuple1<Integer>, Tuple1<Integer>> iteration = raw.iterateDelta(raw, 1000, 0);

		DataSet<Tuple1<Integer>> test = iteration.getWorkset().map(new SimpleMap());
		DataSet<Tuple1<Integer>> delta = iteration.getSolutionSet().coGroup(test).where(0).equalTo(0).with(new SimpleCGroup());
		DataSet<Tuple1<Integer>> feedback = iteration.getWorkset().map(new SimpleMap());
		DataSet<Tuple1<Integer>> result = iteration.closeWith(delta, feedback);

		result.print();

		Plan plan = env.createProgramPlan();
		OptimizedPlan oPlan = null;
		try {
			oPlan = compileNoStats(plan);
		} catch(CompilerException e) {
			Assert.fail(e.getMessage());
		}

		oPlan.accept(new Visitor<PlanNode>() {
			@Override
			public boolean preVisit(PlanNode visitable) {
				System.out.println(visitable);
				if (visitable instanceof WorksetIterationPlanNode) {
					PlanNode deltaNode = ((WorksetIterationPlanNode) visitable).getSolutionSetDeltaPlanNode();

					//get the CoGroup
					DualInputPlanNode dpn = (DualInputPlanNode) deltaNode.getInputs().iterator().next().getSource();
					Channel in1 = dpn.getInput1();
					Channel in2 = dpn.getInput2();

					Assert.assertTrue(in1.getLocalProperties().getOrdering() == null);
					Assert.assertTrue(in2.getLocalProperties().getOrdering() != null);
					Assert.assertTrue(in2.getLocalProperties().getOrdering().getInvolvedIndexes().contains(0));
					Assert.assertTrue(in1.getShipStrategy() == ShipStrategyType.FORWARD);
					Assert.assertTrue(in2.getShipStrategy() == ShipStrategyType.PARTITION_HASH);
					return false;
				}
				return true;
			}

			@Override
			public void postVisit(PlanNode visitable) {}
		});
	}
}
