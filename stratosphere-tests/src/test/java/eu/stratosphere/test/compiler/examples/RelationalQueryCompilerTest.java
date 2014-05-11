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
package eu.stratosphere.test.compiler.examples;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.java.record.operators.JoinOperator;
import eu.stratosphere.api.java.record.operators.MapOperator;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plan.SinkPlanNode;
import eu.stratosphere.example.java.record.relational.TPCHQuery3;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.test.compiler.CompilerTestBase;

/**
 * Tests TPCH Q3 (simplified) under various input conditions.
 */
public class RelationalQueryCompilerTest extends CompilerTestBase {
	
	private static final String ORDERS = "Orders";
	private static final String LINEITEM = "LineItems";
	private static final String MAPPER_NAME = "FilterO";
	private static final String JOIN_NAME = "JoinLiO";
	
	private final FieldList set0 = new FieldList(0);
	private final FieldList set01 = new FieldList(new int[] {0,1});
	
	// ------------------------------------------------------------------------
	
	
	/**
	 * Verifies that a robust repartitioning plan with a hash join is created in the absence of statistics.
	 */
	@Test
	public void testQueryNoStatistics() {
		try {
			TPCHQuery3 query = new TPCHQuery3();
			Plan p = query.getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, IN_FILE, OUT_FILE);
			
			// compile
			final OptimizedPlan plan = compileNoStats(p);
			final OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(plan);
			
			// get the nodes from the final plan
			final SinkPlanNode sink = or.getNode("Output");
			final SingleInputPlanNode reducer = or.getNode("AggLio");
			final SingleInputPlanNode combiner = reducer.getPredecessor() instanceof SingleInputPlanNode ?
					(SingleInputPlanNode) reducer.getPredecessor() : null;
			final DualInputPlanNode join = or.getNode("JoinLiO");
			final SingleInputPlanNode filteringMapper = or.getNode("FilterO");
			
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
	public void testQueryAnyValidPlan() {
		testQueryGeneric(1024*1024*1024L, 8*1024*1024*1024L, 0.05f, true, true, true, false, true);
	}
	
	/**
	 * Verifies that the plan compiles in the presence of empty size=0 estimates.
	 */
	@Test
	public void testQueryWithSizeZeroInputs() {
		testQueryGeneric(0, 0, 0.5f, true, true, true, false, true);
	}
	
	/**
	 * Statistics that push towards a broadcast join.
	 */
	@Test
	public void testQueryWithStatsForBroadcastHash() {
		testQueryGeneric(1024l*1024*1024*1024, 1024l*1024*1024*1024, 0.05f, true, false, true, false, false);
	}
	
	/**
	 * Statistics that push towards a broadcast join.
	 */
	@Test
	public void testQueryWithStatsForRepartitionAny() {
		testQueryGeneric(100l*1024*1024*1024*1024, 100l*1024*1024*1024*1024, 0.5f, false, true, true, true, true);
	}
	
	/**
	 * Statistics that push towards a repartition merge join. If the join blows the data volume up significantly,
	 * re-exploiting the sorted order is cheaper.
	 */
	@Test
	public void testQueryWithStatsForRepartitionMerge() {
		TPCHQuery3 query = new TPCHQuery3();
		Plan p = query.getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, IN_FILE, OUT_FILE);
		
		// set compiler hints
		ContractResolver cr = getContractResolver(p);
		JoinOperator match = cr.getNode("JoinLiO");
		match.getCompilerHints().setFilterFactor(100f);
		
		testQueryGeneric(100l*1024*1024*1024*1024, 100l*1024*1024*1024*1024, 0.05f, 100f, false, true, false, false, true);
	}
	
	// ------------------------------------------------------------------------
	
	private void testQueryGeneric(long orderSize, long lineItemSize, 
			float ordersFilterFactor, 
			boolean broadcastOkay, boolean partitionedOkay,
			boolean hashJoinFirstOkay, boolean hashJoinSecondOkay, boolean mergeJoinOkay)
	{
		testQueryGeneric(orderSize, lineItemSize, ordersFilterFactor, ordersFilterFactor, broadcastOkay, partitionedOkay, hashJoinFirstOkay, hashJoinSecondOkay, mergeJoinOkay);
	}
	
	private void testQueryGeneric(long orderSize, long lineItemSize, 
			float ordersFilterFactor, float joinFilterFactor,
			boolean broadcastOkay, boolean partitionedOkay,
			boolean hashJoinFirstOkay, boolean hashJoinSecondOkay, boolean mergeJoinOkay)
	{
		TPCHQuery3 query = new TPCHQuery3();
		Plan p = query.getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, IN_FILE, OUT_FILE);
		testQueryGeneric(p, orderSize, lineItemSize, ordersFilterFactor, joinFilterFactor, broadcastOkay, partitionedOkay, hashJoinFirstOkay, hashJoinSecondOkay, mergeJoinOkay);
	}
		
	private void testQueryGeneric(Plan p, long orderSize, long lineitemSize, 
			float orderSelectivity, float joinSelectivity, 
			boolean broadcastOkay, boolean partitionedOkay,
			boolean hashJoinFirstOkay, boolean hashJoinSecondOkay, boolean mergeJoinOkay)
	{
		try {
			// set statistics
			ContractResolver cr = getContractResolver(p);
			FileDataSource ordersSource = cr.getNode(ORDERS);
			FileDataSource lineItemSource = cr.getNode(LINEITEM);
			MapOperator mapper = cr.getNode(MAPPER_NAME);
			JoinOperator joiner = cr.getNode(JOIN_NAME);
			setSourceStatistics(ordersSource, orderSize, 100f);
			setSourceStatistics(lineItemSource, lineitemSize, 140f);
			mapper.getCompilerHints().setAvgOutputRecordSize(16f);
			mapper.getCompilerHints().setFilterFactor(orderSelectivity);
			joiner.getCompilerHints().setFilterFactor(joinSelectivity);
			
			// compile
			final OptimizedPlan plan = compileWithStats(p);
			final OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(plan);
			
			// get the nodes from the final plan
			final SinkPlanNode sink = or.getNode("Output");
			final SingleInputPlanNode reducer = or.getNode("AggLio");
			final SingleInputPlanNode combiner = reducer.getPredecessor() instanceof SingleInputPlanNode ?
					(SingleInputPlanNode) reducer.getPredecessor() : null;
			final DualInputPlanNode join = or.getNode("JoinLiO");
			final SingleInputPlanNode filteringMapper = or.getNode("FilterO");
			
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
			SingleInputPlanNode reducer, SinkPlanNode sink)
	{
		// check ship strategies that are always fix
		Assert.assertEquals(ShipStrategyType.FORWARD, map.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
		
		// check the driver strategies that are always fix
		Assert.assertEquals(DriverStrategy.COLLECTOR_MAP, map.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.SORTED_GROUP_REDUCE, reducer.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.NONE, sink.getDriverStrategy());
		if (combiner != null) {
			Assert.assertEquals(DriverStrategy.SORTED_GROUP_COMBINE, combiner.getDriverStrategy());
			Assert.assertEquals(LocalStrategy.NONE, combiner.getInput().getLocalStrategy());
		}
	}
	
	private boolean checkBroadcastShipStrategies(DualInputPlanNode join, SingleInputPlanNode reducer,
			SingleInputPlanNode combiner)
	{
		if (ShipStrategyType.BROADCAST == join.getInput1().getShipStrategy() &&
			ShipStrategyType.FORWARD == join.getInput2().getShipStrategy() &&
			ShipStrategyType.PARTITION_HASH == reducer.getInput().getShipStrategy())
		{
			// check combiner
			Assert.assertNotNull("Plan should have a combiner", combiner);
			Assert.assertEquals(ShipStrategyType.FORWARD, combiner.getInput().getShipStrategy());
			return true;
		} else {
			return false;
		}
	}
	
	private boolean checkRepartitionShipStrategies(DualInputPlanNode join, SingleInputPlanNode reducer,
			SingleInputPlanNode combiner)
	{
		if (ShipStrategyType.PARTITION_HASH == join.getInput1().getShipStrategy() &&
			ShipStrategyType.PARTITION_HASH == join.getInput2().getShipStrategy() &&
			ShipStrategyType.FORWARD == reducer.getInput().getShipStrategy())
		{
			// check combiner
			Assert.assertNull("Plan should not have a combiner", combiner);
			return true;
		} else {
			return false;
		}
	}
	
	private boolean checkHashJoinStrategies(DualInputPlanNode join, SingleInputPlanNode reducer, boolean buildFirst) {
		if ( (buildFirst && DriverStrategy.HYBRIDHASH_BUILD_FIRST == join.getDriverStrategy()) ||
			 (!buildFirst && DriverStrategy.HYBRIDHASH_BUILD_SECOND == join.getDriverStrategy()) ) 
		{
			// driver keys
			Assert.assertEquals(set0, join.getKeysForInput1());
			Assert.assertEquals(set0, join.getKeysForInput2());
			
			// local strategies
			Assert.assertEquals(LocalStrategy.NONE, join.getInput1().getLocalStrategy());
			Assert.assertEquals(LocalStrategy.NONE, join.getInput2().getLocalStrategy());
			Assert.assertEquals(LocalStrategy.COMBININGSORT, reducer.getInput().getLocalStrategy());
			
			// local strategy keys
			Assert.assertEquals(set01, reducer.getInput().getLocalStrategyKeys());
			Assert.assertEquals(set01, reducer.getKeys());
			Assert.assertTrue(Arrays.equals(reducer.getInput().getLocalStrategySortOrder(), reducer.getSortOrders()));
			return true;
		} else {
			return false;
		}
	}
	
	private boolean checkBroadcastMergeJoin(DualInputPlanNode join, SingleInputPlanNode reducer) {
		if (DriverStrategy.MERGE == join.getDriverStrategy()) {
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
			Assert.assertEquals(set01, reducer.getKeys());
			Assert.assertTrue(Arrays.equals(reducer.getInput().getLocalStrategySortOrder(), reducer.getSortOrders()));
			return true;
		} else {
			return false;
		}
	}
	
	private boolean checkRepartitionMergeJoin(DualInputPlanNode join, SingleInputPlanNode reducer) {
		if (DriverStrategy.MERGE == join.getDriverStrategy()) {
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
			Assert.assertEquals(set01, reducer.getKeys());
			Assert.assertTrue(Arrays.equals(join.getInput1().getLocalStrategySortOrder(), reducer.getSortOrders()));
			return true;
		} else {
			return false;
		}
	}
}
