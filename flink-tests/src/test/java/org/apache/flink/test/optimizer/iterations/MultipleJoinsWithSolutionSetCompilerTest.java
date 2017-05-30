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

package org.apache.flink.test.optimizer.iterations;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.DeltaIteration;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SolutionSetPlanNode;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Collector;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test multiple joins with the solution set.
 */
@SuppressWarnings("serial")
public class MultipleJoinsWithSolutionSetCompilerTest extends CompilerTestBase {

	private static final String JOIN_1 = "join1";
	private static final String JOIN_2 = "join2";

	@Test
	public void testMultiSolutionSetJoinPlan() {
		try {

			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			@SuppressWarnings("unchecked")
			DataSet<Tuple2<Long, Double>> inputData = env.fromElements(new Tuple2<Long, Double>(1L, 1.0));
			DataSet<Tuple2<Long, Double>> result = constructPlan(inputData, 10);

			// add two sinks, to test the case of branching after an iteration
			result.output(new DiscardingOutputFormat<Tuple2<Long, Double>>());
			result.output(new DiscardingOutputFormat<Tuple2<Long, Double>>());

			Plan p = env.createProgramPlan();

			OptimizedPlan optPlan = compileNoStats(p);

			OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(optPlan);

			DualInputPlanNode join1 = or.getNode(JOIN_1);
			DualInputPlanNode join2 = or.getNode(JOIN_2);

			assertEquals(DriverStrategy.HYBRIDHASH_BUILD_FIRST, join1.getDriverStrategy());
			assertEquals(DriverStrategy.HYBRIDHASH_BUILD_SECOND, join2.getDriverStrategy());

			assertEquals(ShipStrategyType.PARTITION_HASH, join1.getInput2().getShipStrategy());
			assertEquals(ShipStrategyType.PARTITION_HASH, join2.getInput1().getShipStrategy());

			assertEquals(SolutionSetPlanNode.class, join1.getInput1().getSource().getClass());
			assertEquals(SolutionSetPlanNode.class, join2.getInput2().getSource().getClass());

			new JobGraphGenerator().compileJobGraph(optPlan);
		}
		catch (Exception e) {
			System.err.println(e.getMessage());
			e.printStackTrace();
			fail("Test erroneous: " + e.getMessage());
		}
	}

	public static DataSet<Tuple2<Long, Double>> constructPlan(DataSet<Tuple2<Long, Double>> initialData, int numIterations) {

		DeltaIteration<Tuple2<Long, Double>, Tuple2<Long, Double>> iteration = initialData.iterateDelta(initialData, numIterations, 0);

		DataSet<Tuple2<Long, Double>> delta = iteration.getSolutionSet()
				.join(iteration.getWorkset().flatMap(new Duplicator())).where(0).equalTo(0).with(new SummingJoin()).name(JOIN_1)
				.groupBy(0).aggregate(Aggregations.MIN, 1).map(new Expander())
				.join(iteration.getSolutionSet()).where(0).equalTo(0).with(new SummingJoinProject()).name(JOIN_2);

		DataSet<Tuple2<Long, Double>> changes = delta.groupBy(0).aggregate(Aggregations.SUM, 1);

		DataSet<Tuple2<Long, Double>> result = iteration.closeWith(delta, changes);

		return result;
	}

	private static final class SummingJoin extends RichJoinFunction<Tuple2<Long, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

		@Override
		public Tuple2<Long, Double> join(Tuple2<Long, Double> first, Tuple2<Long, Double> second) {
			return new Tuple2<Long, Double>(first.f0, first.f1 + second.f1);
		}
	}

	private static final class SummingJoinProject extends RichJoinFunction<Tuple3<Long, Double, Double>, Tuple2<Long, Double>, Tuple2<Long, Double>> {

		@Override
		public Tuple2<Long, Double> join(Tuple3<Long, Double, Double> first, Tuple2<Long, Double> second) {
			return new Tuple2<Long, Double>(first.f0, first.f1 + first.f2 + second.f1);
		}
	}

	private static final class Duplicator extends RichFlatMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {

		@Override
		public void flatMap(Tuple2<Long, Double> value, Collector<Tuple2<Long, Double>> out) {
			out.collect(value);
			out.collect(value);
		}
	}

	private static final class Expander extends RichMapFunction<Tuple2<Long, Double>, Tuple3<Long, Double, Double>> {

		@Override
		public Tuple3<Long, Double, Double> map(Tuple2<Long, Double> value) {
			return new Tuple3<Long, Double, Double>(value.f0, value.f1, value.f1 * 2);
		}
	}
}
