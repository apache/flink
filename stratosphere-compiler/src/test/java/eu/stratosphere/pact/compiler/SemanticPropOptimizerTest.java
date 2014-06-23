/***********************************************************************************************************************
 *
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
 *
 **********************************************************************************************************************/

package eu.stratosphere.pact.compiler;

import eu.stratosphere.api.common.operators.base.JoinOperatorBase;
import eu.stratosphere.api.common.operators.base.MapOperatorBase;
import eu.stratosphere.api.common.operators.base.ReduceOperatorBase;
import eu.stratosphere.api.common.operators.util.FieldSet;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.JoinFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.ReduceFunction;
import eu.stratosphere.api.java.operators.translation.JavaPlan;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.compiler.dataproperties.GlobalProperties;
import eu.stratosphere.compiler.dataproperties.LocalProperties;
import eu.stratosphere.compiler.dataproperties.PartitioningProperty;
import eu.stratosphere.compiler.plan.Channel;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.PlanNode;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.util.Visitor;
import org.junit.Assert;
import org.junit.Test;

public class SemanticPropOptimizerTest extends CompilerTestBase {

	public static class SimpleReducer extends ReduceFunction<Tuple3<Integer, Integer, Integer>> {
		@Override
		public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> value1, Tuple3<Integer, Integer, Integer> value2) throws Exception {
			return null;
		}
	}

	public static class SimpleMap extends MapFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>> {
		@Override
		public Tuple3<Integer, Integer, Integer> map(Tuple3<Integer, Integer, Integer> value) throws Exception {
			return null;
		}
	}

	@Test
	public void forwardFieldsTestMapReduce() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> set = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		set = set.map(new SimpleMap()).withConstantSet("*")
				.groupBy(0)
				.reduce(new SimpleReducer()).withConstantSet("0->1")
				.map(new SimpleMap()).withConstantSet("*")
				.groupBy(1)
				.reduce(new SimpleReducer()).withConstantSet("*");

		set.print();
		JavaPlan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		oPlan.accept(new Visitor<PlanNode>() {
			@Override
			public boolean preVisit(PlanNode visitable) {
				if (visitable instanceof SingleInputPlanNode && visitable.getPactContract() instanceof ReduceOperatorBase) {
					for (Channel input: visitable.getInputs()) {
						GlobalProperties gprops = visitable.getGlobalProperties();
						LocalProperties lprops = visitable.getLocalProperties();

						Assert.assertTrue("Reduce should just forward the input if it is already partitioned",
								input.getShipStrategy() == ShipStrategyType.FORWARD);
						Assert.assertTrue("Wrong GlobalProperties on Reducer",
								gprops.isPartitionedOnFields(new FieldSet(1)));
						Assert.assertTrue("Wrong GlobalProperties on Reducer",
								gprops.getPartitioning() == PartitioningProperty.HASH_PARTITIONED);
						Assert.assertTrue("Wrong LocalProperties on Reducer",
								lprops.getGroupedFields().contains(1));
					}
				}
				if (visitable instanceof SingleInputPlanNode && visitable.getPactContract() instanceof MapOperatorBase) {
					for (Channel input: visitable.getInputs()) {
						GlobalProperties gprops = visitable.getGlobalProperties();
						LocalProperties lprops = visitable.getLocalProperties();

						Assert.assertTrue("Map should just forward the input if it is already partitioned",
								input.getShipStrategy() == ShipStrategyType.FORWARD);
						Assert.assertTrue("Wrong GlobalProperties on Mapper",
								gprops.isPartitionedOnFields(new FieldSet(1)));
						Assert.assertTrue("Wrong GlobalProperties on Mapper",
								gprops.getPartitioning() == PartitioningProperty.HASH_PARTITIONED);
						Assert.assertTrue("Wrong LocalProperties on Mapper",
								lprops.getGroupedFields().contains(1));
					}
					return false;
				}
				return true;
			}

			@Override
			public void postVisit(PlanNode visitable) {

			}
		});
	}

	@Test
	public void forwardFieldsTestJoin() {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		DataSet<Tuple3<Integer, Integer, Integer>> in1 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		DataSet<Tuple3<Integer, Integer, Integer>> in2 = env.readCsvFile(IN_FILE).types(Integer.class, Integer.class, Integer.class);
		in1 = in1.map(new SimpleMap()).withConstantSet("*")
				.groupBy(0)
				.reduce(new SimpleReducer()).withConstantSet("0->1");
		in2 = in2.map(new SimpleMap()).withConstantSet("*")
				.groupBy(1)
				.reduce(new SimpleReducer()).withConstantSet("1->2");
		DataSet<Tuple3<Integer, Integer, Integer>> out = in1.join(in2).where(1).equalTo(2).with(new JoinFunction<Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>, Tuple3<Integer, Integer, Integer>>() {
			@Override
			public Tuple3<Integer, Integer, Integer> join(Tuple3<Integer, Integer, Integer> first, Tuple3<Integer, Integer, Integer> second) throws Exception {
				return null;
			}
		});

		out.print();
		JavaPlan plan = env.createProgramPlan();
		OptimizedPlan oPlan = compileWithStats(plan);

		oPlan.accept(new Visitor<PlanNode>() {
			@Override
			public boolean preVisit(PlanNode visitable) {
				if (visitable instanceof DualInputPlanNode && visitable.getPactContract() instanceof JoinOperatorBase) {
					DualInputPlanNode node = ((DualInputPlanNode) visitable);

					final Channel inConn1 = node.getInput1();
					final Channel inConn2 = node.getInput2();

					Assert.assertTrue("Join should just forward the input if it is already partitioned",
							inConn1.getShipStrategy() == ShipStrategyType.FORWARD);
					Assert.assertTrue("Join should just forward the input if it is already partitioned",
							inConn2.getShipStrategy() == ShipStrategyType.FORWARD);
					return false;
				}
				return true;
			}

			@Override
			public void postVisit(PlanNode visitable) {

			}
		});
	}
}
