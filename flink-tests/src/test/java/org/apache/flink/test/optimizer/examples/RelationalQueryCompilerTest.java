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

package org.apache.flink.test.optimizer.examples;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.operators.DualInputOperator;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.SingleInputOperator;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFields;
import org.apache.flink.api.java.functions.FunctionAnnotation.ForwardedFieldsFirst;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.optimizer.util.OperatorResolver;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.util.Collector;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Tests TPCH Q3 (simplified) under various input conditions.
 */
@SuppressWarnings("serial")
public class RelationalQueryCompilerTest extends CompilerTestBase {

	private static final String ORDERS = "Orders";
	private static final String LINEITEM = "LineItems";
	private static final String MAPPER_NAME = "FilterO";
	private static final String JOIN_NAME = "JoinLiO";
	private static final String REDUCE_NAME = "AggLiO";
	private static final String SINK = "Output";

	private final FieldList set0 = new FieldList(0);
	private final FieldList set01 = new FieldList(0, 1);
	private final ExecutionConfig defaultExecutionConfig = new ExecutionConfig();

	// ------------------------------------------------------------------------

	/**
	 * Verifies that a robust repartitioning plan with a hash join is created in the absence of statistics.
	 */
	@Test
	public void testQueryNoStatistics() {
		try {
			Plan p = getTPCH3Plan();
			p.setExecutionConfig(defaultExecutionConfig);
			// compile
			final OptimizedPlan plan = compileNoStats(p);

			final OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(plan);

			// get the nodes from the final plan
			final SinkPlanNode sink = or.getNode(SINK);
			final SingleInputPlanNode reducer = or.getNode(REDUCE_NAME);
			final SingleInputPlanNode combiner = reducer.getPredecessor() instanceof SingleInputPlanNode ?
					(SingleInputPlanNode) reducer.getPredecessor() : null;
			final DualInputPlanNode join = or.getNode(JOIN_NAME);
			final SingleInputPlanNode filteringMapper = or.getNode(MAPPER_NAME);

			// verify the optimizer choices
			checkStandardStrategies(filteringMapper, join, combiner, reducer, sink);
			Assert.assertTrue(checkRepartitionShipStrategies(join, reducer, combiner));
			Assert.assertTrue(checkHashJoinStrategies(join, reducer, true) || checkHashJoinStrategies(join, reducer, false));
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	/**
	 * Checks if any valid plan is produced. Hash joins are expected to build the orders side, as the statistics
	 * indicate this to be the smaller one.
	 */
	@Test
	public void testQueryAnyValidPlan() throws Exception {
		testQueryGeneric(1024 * 1024 * 1024L, 8 * 1024 * 1024 * 1024L, 0.05f, 0.05f, true, true, true, false, true);
	}

	/**
	 * Verifies that the plan compiles in the presence of empty size=0 estimates.
	 */
	@Test
	public void testQueryWithSizeZeroInputs() throws Exception {
		testQueryGeneric(0, 0, 0.1f, 0.5f, true, true, true, false, true);
	}

	/**
	 * Statistics that push towards a broadcast join.
	 */
	@Test
	public void testQueryWithStatsForBroadcastHash() throws Exception {
		testQueryGeneric(1024L * 1024 * 1024 * 1024, 1024L * 1024 * 1024 * 1024, 0.01f, 0.05f, true, false, true, false, false);
	}

	/**
	 * Statistics that push towards a broadcast join.
	 */
	@Test
	public void testQueryWithStatsForRepartitionAny() throws Exception {
		testQueryGeneric(100L * 1024 * 1024 * 1024 * 1024, 100L * 1024 * 1024 * 1024 * 1024, 0.1f, 0.5f, false, true, true, true, true);
	}

	/**
	 * Statistics that push towards a repartition merge join. If the join blows the data volume up significantly,
	 * re-exploiting the sorted order is cheaper.
	 */
	@Test
	public void testQueryWithStatsForRepartitionMerge() throws Exception {
		Plan p = getTPCH3Plan();
		p.setExecutionConfig(defaultExecutionConfig);
		// set compiler hints
		OperatorResolver cr = getContractResolver(p);
		DualInputOperator<?, ?, ?, ?> match = cr.getNode(JOIN_NAME);
		match.getCompilerHints().setFilterFactor(100f);

		testQueryGeneric(100L * 1024 * 1024 * 1024 * 1024, 100L * 1024 * 1024 * 1024 * 1024, 0.01f, 100f, false, true, false, false, true);
	}

	// ------------------------------------------------------------------------
	private void testQueryGeneric(long orderSize, long lineItemSize,
			float ordersFilterFactor, float joinFilterFactor,
			boolean broadcastOkay, boolean partitionedOkay,
			boolean hashJoinFirstOkay, boolean hashJoinSecondOkay, boolean mergeJoinOkay) throws Exception {
		Plan p = getTPCH3Plan();
		p.setExecutionConfig(defaultExecutionConfig);
		testQueryGeneric(p, orderSize, lineItemSize, ordersFilterFactor, joinFilterFactor, broadcastOkay, partitionedOkay, hashJoinFirstOkay, hashJoinSecondOkay, mergeJoinOkay);
	}

	private void testQueryGeneric(Plan p, long orderSize, long lineitemSize,
			float orderSelectivity, float joinSelectivity,
			boolean broadcastOkay, boolean partitionedOkay,
			boolean hashJoinFirstOkay, boolean hashJoinSecondOkay, boolean mergeJoinOkay) {
		try {
			// set statistics
			OperatorResolver cr = getContractResolver(p);
			GenericDataSourceBase<?, ?> ordersSource = cr.getNode(ORDERS);
			GenericDataSourceBase<?, ?> lineItemSource = cr.getNode(LINEITEM);
			SingleInputOperator<?, ?, ?> mapper = cr.getNode(MAPPER_NAME);
			DualInputOperator<?, ?, ?, ?> joiner = cr.getNode(JOIN_NAME);
			setSourceStatistics(ordersSource, orderSize, 100f);
			setSourceStatistics(lineItemSource, lineitemSize, 140f);
			mapper.getCompilerHints().setAvgOutputRecordSize(16f);
			mapper.getCompilerHints().setFilterFactor(orderSelectivity);
			joiner.getCompilerHints().setFilterFactor(joinSelectivity);

			// compile
			final OptimizedPlan plan = compileWithStats(p);
			final OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(plan);

			// get the nodes from the final plan
			final SinkPlanNode sink = or.getNode(SINK);
			final SingleInputPlanNode reducer = or.getNode(REDUCE_NAME);
			final SingleInputPlanNode combiner = reducer.getPredecessor() instanceof SingleInputPlanNode ?
					(SingleInputPlanNode) reducer.getPredecessor() : null;
			final DualInputPlanNode join = or.getNode(JOIN_NAME);
			final SingleInputPlanNode filteringMapper = or.getNode(MAPPER_NAME);

			checkStandardStrategies(filteringMapper, join, combiner, reducer, sink);

			// check the possible variants and that the variant ia allowed in this specific setting
			if (checkBroadcastShipStrategies(join, reducer, combiner)) {
				Assert.assertTrue("Broadcast join incorrectly chosen.", broadcastOkay);

				if (checkHashJoinStrategies(join, reducer, true)) {
					Assert.assertTrue("Hash join (build orders) incorrectly chosen", hashJoinFirstOkay);
				} else if (checkHashJoinStrategies(join, reducer, false)) {
					Assert.assertTrue("Hash join (build lineitem) incorrectly chosen", hashJoinSecondOkay);
				} else if (checkBroadcastMergeJoin(join, reducer)) {
					Assert.assertTrue("Merge join incorrectly chosen", mergeJoinOkay);
				} else {
					Assert.fail("Plan has no correct hash join or merge join strategies.");
				}
			}
			else if (checkRepartitionShipStrategies(join, reducer, combiner)) {
				Assert.assertTrue("Partitioned join incorrectly chosen.", partitionedOkay);

				if (checkHashJoinStrategies(join, reducer, true)) {
					Assert.assertTrue("Hash join (build orders) incorrectly chosen", hashJoinFirstOkay);
				} else if (checkHashJoinStrategies(join, reducer, false)) {
					Assert.assertTrue("Hash join (build lineitem) incorrectly chosen", hashJoinSecondOkay);
				} else if (checkRepartitionMergeJoin(join, reducer)) {
					Assert.assertTrue("Merge join incorrectly chosen", mergeJoinOkay);
				} else {
					Assert.fail("Plan has no correct hash join or merge join strategies.");
				}
			} else {
				Assert.fail("Plan has neither correct BC join or partitioned join configuration.");
			}
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------
	//  Checks for special conditions
	// ------------------------------------------------------------------------

	private void checkStandardStrategies(SingleInputPlanNode map, DualInputPlanNode join, SingleInputPlanNode combiner,
			SingleInputPlanNode reducer, SinkPlanNode sink) {
		// check ship strategies that are always fix
		Assert.assertEquals(ShipStrategyType.FORWARD, map.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());

		// check the driver strategies that are always fix
		Assert.assertEquals(DriverStrategy.FLAT_MAP, map.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.SORTED_GROUP_REDUCE, reducer.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.NONE, sink.getDriverStrategy());
		if (combiner != null) {
			Assert.assertEquals(DriverStrategy.SORTED_GROUP_COMBINE, combiner.getDriverStrategy());
			Assert.assertEquals(LocalStrategy.NONE, combiner.getInput().getLocalStrategy());
		}
	}

	private boolean checkBroadcastShipStrategies(DualInputPlanNode join, SingleInputPlanNode reducer,
			SingleInputPlanNode combiner) {
		if (ShipStrategyType.BROADCAST == join.getInput1().getShipStrategy() &&
			ShipStrategyType.FORWARD == join.getInput2().getShipStrategy() &&
			ShipStrategyType.PARTITION_HASH == reducer.getInput().getShipStrategy()) {

			// check combiner
			Assert.assertNotNull("Plan should have a combiner", combiner);
			Assert.assertEquals(ShipStrategyType.FORWARD, combiner.getInput().getShipStrategy());
			return true;
		} else {
			return false;
		}
	}

	private boolean checkRepartitionShipStrategies(DualInputPlanNode join, SingleInputPlanNode reducer,
			SingleInputPlanNode combiner) {
		if (ShipStrategyType.PARTITION_HASH == join.getInput1().getShipStrategy() &&
			ShipStrategyType.PARTITION_HASH == join.getInput2().getShipStrategy() &&
			ShipStrategyType.FORWARD == reducer.getInput().getShipStrategy()) {

			// check combiner
			Assert.assertNull("Plan should not have a combiner", combiner);
			return true;
		} else {
			return false;
		}
	}

	private boolean checkHashJoinStrategies(DualInputPlanNode join, SingleInputPlanNode reducer, boolean buildFirst) {
		if ((buildFirst && DriverStrategy.HYBRIDHASH_BUILD_FIRST == join.getDriverStrategy()) ||
			(!buildFirst && DriverStrategy.HYBRIDHASH_BUILD_SECOND == join.getDriverStrategy())) {

			// driver keys
			Assert.assertEquals(set0, join.getKeysForInput1());
			Assert.assertEquals(set0, join.getKeysForInput2());

			// local strategies
			Assert.assertEquals(LocalStrategy.NONE, join.getInput1().getLocalStrategy());
			Assert.assertEquals(LocalStrategy.NONE, join.getInput2().getLocalStrategy());
			Assert.assertEquals(LocalStrategy.COMBININGSORT, reducer.getInput().getLocalStrategy());

			// local strategy keys
			Assert.assertEquals(set01, reducer.getInput().getLocalStrategyKeys());
			Assert.assertEquals(set01, reducer.getKeys(0));
			Assert.assertTrue(Arrays.equals(reducer.getInput().getLocalStrategySortOrder(), reducer.getSortOrders(0)));
			return true;
		} else {
			return false;
		}
	}

	private boolean checkBroadcastMergeJoin(DualInputPlanNode join, SingleInputPlanNode reducer) {
		if (DriverStrategy.INNER_MERGE == join.getDriverStrategy()) {
			// driver keys
			Assert.assertEquals(set0, join.getKeysForInput1());
			Assert.assertEquals(set0, join.getKeysForInput2());

			// local strategies
			Assert.assertEquals(LocalStrategy.SORT, join.getInput1().getLocalStrategy());
			Assert.assertEquals(LocalStrategy.SORT, join.getInput2().getLocalStrategy());
			Assert.assertEquals(LocalStrategy.COMBININGSORT, reducer.getInput().getLocalStrategy());

			// local strategy keys
			Assert.assertEquals(set0, join.getInput1().getLocalStrategyKeys());
			Assert.assertEquals(set0, join.getInput2().getLocalStrategyKeys());
			Assert.assertTrue(Arrays.equals(join.getInput1().getLocalStrategySortOrder(), join.getInput2().getLocalStrategySortOrder()));
			Assert.assertEquals(set01, reducer.getInput().getLocalStrategyKeys());
			Assert.assertEquals(set01, reducer.getKeys(0));
			Assert.assertTrue(Arrays.equals(reducer.getInput().getLocalStrategySortOrder(), reducer.getSortOrders(0)));
			return true;
		} else {
			return false;
		}
	}

	private boolean checkRepartitionMergeJoin(DualInputPlanNode join, SingleInputPlanNode reducer) {
		if (DriverStrategy.INNER_MERGE == join.getDriverStrategy()) {
			// driver keys
			Assert.assertEquals(set0, join.getKeysForInput1());
			Assert.assertEquals(set0, join.getKeysForInput2());

			// local strategies
			Assert.assertEquals(LocalStrategy.SORT, join.getInput1().getLocalStrategy());
			Assert.assertEquals(LocalStrategy.SORT, join.getInput2().getLocalStrategy());
			Assert.assertEquals(LocalStrategy.NONE, reducer.getInput().getLocalStrategy());

			// local strategy keys
			Assert.assertEquals(set01, join.getInput1().getLocalStrategyKeys());
			Assert.assertEquals(set0, join.getInput2().getLocalStrategyKeys());
			Assert.assertTrue(join.getInput1().getLocalStrategySortOrder()[0] == join.getInput2().getLocalStrategySortOrder()[0]);
			Assert.assertEquals(set01, reducer.getKeys(0));
			Assert.assertTrue(Arrays.equals(join.getInput1().getLocalStrategySortOrder(), reducer.getSortOrders(0)));
			return true;
		} else {
			return false;
		}
	}

	public static Plan getTPCH3Plan() throws Exception {
		return tcph3(new String[]{DEFAULT_PARALLELISM_STRING, IN_FILE, IN_FILE, OUT_FILE});
	}

	public static Plan tcph3(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(Integer.parseInt(args[0]));

		//order id, order status, order data, order prio, ship prio
		DataSet<Tuple5<Long, String, String, String, Integer>> orders =
				env.readCsvFile(args[1])
				.fieldDelimiter("|").lineDelimiter("\n")
				.includeFields("101011001").types(Long.class, String.class, String.class, String.class, Integer.class)
				.name(ORDERS);

		//order id, extended price
		DataSet<Tuple2<Long, Double>> lineItems =
				env.readCsvFile(args[2])
				.fieldDelimiter("|").lineDelimiter("\n")
				.includeFields("100001").types(Long.class, Double.class)
				.name(LINEITEM);

		DataSet<Tuple2<Long, Integer>> filterO = orders.flatMap(new FilterO()).name(MAPPER_NAME);

		DataSet<Tuple3<Long, Integer, Double>> joinLiO = filterO.join(lineItems).where(0).equalTo(0).with(new JoinLiO()).name(JOIN_NAME);

		DataSet<Tuple3<Long, Integer, Double>> aggLiO = joinLiO.groupBy(0, 1).reduceGroup(new AggLiO()).name(REDUCE_NAME);

		aggLiO.writeAsCsv(args[3], "\n", "|").name(SINK);

		return env.createProgramPlan();
	}

	@ForwardedFields("f0; f4->f1")
	private static class FilterO implements FlatMapFunction<Tuple5<Long, String, String, String, Integer>, Tuple2<Long, Integer>> {
		@Override
		public void flatMap(Tuple5<Long, String, String, String, Integer> value, Collector<Tuple2<Long, Integer>> out) throws Exception {
			// not going to be executed
		}
	}

	@ForwardedFieldsFirst("f0; f1")
	private static class JoinLiO implements FlatJoinFunction<Tuple2<Long, Integer>, Tuple2<Long, Double>, Tuple3<Long, Integer, Double>> {
		@Override
		public void join(Tuple2<Long, Integer> first, Tuple2<Long, Double> second, Collector<Tuple3<Long, Integer, Double>> out) throws Exception {
			// not going to be executed
		}
	}

	@ForwardedFields("f0; f1")
	private static class AggLiO implements
			GroupReduceFunction<Tuple3<Long, Integer, Double>, Tuple3<Long, Integer, Double>>,
			GroupCombineFunction<Tuple3<Long, Integer, Double>, Tuple3<Long, Integer, Double>> {
		@Override
		public void reduce(Iterable<Tuple3<Long, Integer, Double>> values, Collector<Tuple3<Long, Integer, Double>> out) throws Exception {
			// not going to be executed
		}

		@Override
		public void combine(Iterable<Tuple3<Long, Integer, Double>> values, Collector<Tuple3<Long, Integer, Double>> out) throws Exception {
			// not going to be executed
		}
	}
}
