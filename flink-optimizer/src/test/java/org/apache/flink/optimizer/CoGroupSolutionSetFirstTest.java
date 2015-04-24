/*
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

package org.apache.flink.optimizer;

import org.apache.flink.api.common.functions.RichCoGroupFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.Assert;
import org.junit.Test;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.PlanNode;
import org.apache.flink.optimizer.plan.WorksetIterationPlanNode;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Collector;
import org.apache.flink.util.Visitor;

@SuppressWarnings("serial")
public class CoGroupSolutionSetFirstTest extends CompilerTestBase {
	
	public static class SimpleCGroup extends RichCoGroupFunction<Tuple1<Integer>, Tuple1<Integer>, Tuple1<Integer>> {
		@Override
		public void coGroup(Iterable<Tuple1<Integer>> first, Iterable<Tuple1<Integer>> second, Collector<Tuple1<Integer>> out) {}
	}

	public static class SimpleMap extends RichMapFunction<Tuple1<Integer>, Tuple1<Integer>> {
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

		result.output(new DiscardingOutputFormat<Tuple1<Integer>>());

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
