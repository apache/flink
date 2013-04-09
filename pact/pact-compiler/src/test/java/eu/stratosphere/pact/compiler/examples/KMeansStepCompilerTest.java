/***********************************************************************************************************************
 *
 * Copyright (C) 2012 by the Stratosphere project (http://stratosphere.eu)
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
package eu.stratosphere.pact.compiler.examples;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.pact.common.contract.FileDataSource;
import eu.stratosphere.pact.common.plan.Plan;
import eu.stratosphere.pact.common.util.FieldList;
import eu.stratosphere.pact.compiler.CompilerTestBase;
import eu.stratosphere.pact.compiler.plan.candidate.DualInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.OptimizedPlan;
import eu.stratosphere.pact.compiler.plan.candidate.SingleInputPlanNode;
import eu.stratosphere.pact.compiler.plan.candidate.SinkPlanNode;
import eu.stratosphere.pact.example.datamining.KMeansIteration;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;

/**
 * Tests a K-Means Step under various input conditions.
 */
public class KMeansStepCompilerTest extends CompilerTestBase {
	
	private static final String DATAPOINTS = "Data Points";
	private static final String CENTERS = "Centers";
	
	private final FieldList set0 = new FieldList(0);
	
	// ------------------------------------------------------------------------
	
	
	/**
	 * Verifies that a robust block-nested-loops join is used in the absence of statistics.
	 * Since the data points have a unique id, the reducer should be chained.
	 */
	@Test
	public void testQueryNoStatisticsChainedReducer() {
		try {
			KMeansIteration job = new KMeansIteration();
			Plan p = job.getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, IN_FILE, OUT_FILE);
			
			// compile
			final OptimizedPlan plan = compileNoStats(p);
			final OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(plan);
			
			// get the nodes from the final plan
			final SinkPlanNode sink = or.getNode("New Center Positions");
			final SingleInputPlanNode reducer2 = or.getNode("Recompute Center Positions");
			final SingleInputPlanNode combiner2 = (SingleInputPlanNode) reducer2.getPredecessor(); 
			final SingleInputPlanNode reducer1 = or.getNode("Find Nearest Centers");
			final SingleInputPlanNode combiner1 = reducer1.getPredecessor() instanceof SingleInputPlanNode ?
				(SingleInputPlanNode) reducer1.getPredecessor() : null;
				
			final DualInputPlanNode cross = or.getNode("Compute Distances");
			
			// verify the optimizer choices
			checkStandardStrategies(reducer1, combiner1, reducer2, combiner2, sink);
			checkBroadCastSide(cross, false);
			checkBlockNLChainedReducer(cross, reducer1, combiner1, true, true);
			
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	/**
	 * Verifies that a robust block-nested-loops join is used in the absence of statistics.
	 * The first reducer should be chained, as the partitioning ought to be pushed before the cross.
	 */
	@Test
	public void testQueryNoStatisticsNonChainedReducer() {
		try {
			KMeansIteration job = new KMeansIteration();
			Plan p = job.getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, IN_FILE, OUT_FILE);
			
			// clear uniqueness hints
			ContractResolver cr = getContractResolver(p);
			FileDataSource pointsSource = cr.getNode(DATAPOINTS);
			FileDataSource centersSource = cr.getNode(CENTERS);
			pointsSource.getCompilerHints().clearUniqueFields();
			centersSource.getCompilerHints().clearUniqueFields();
			
			// compile
			final OptimizedPlan plan = compileNoStats(p);
			final OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(plan);
			
			// get the nodes from the final plan
			final SinkPlanNode sink = or.getNode("New Center Positions");
			final SingleInputPlanNode reducer2 = or.getNode("Recompute Center Positions");
			final SingleInputPlanNode combiner2 = (SingleInputPlanNode) reducer2.getPredecessor(); 
			final SingleInputPlanNode reducer1 = or.getNode("Find Nearest Centers");
			final SingleInputPlanNode combiner1 = reducer1.getPredecessor() instanceof SingleInputPlanNode ?
				(SingleInputPlanNode) reducer1.getPredecessor() : null;
				
			final DualInputPlanNode cross = or.getNode("Compute Distances");
			
			// verify the optimizer choices
			checkStandardStrategies(reducer1, combiner1, reducer2, combiner2, sink);
			checkBlockNLChainedReducer(cross, reducer1, combiner1, true, true);
			
		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}
	
	/**
	 * Tests the query with statistics that push for a broadcast of the centers and a
	 * chained reducer receiving pre-grouped records from the streamed-nested loops cross.
	 */
	@Test
	public void testQueryBCCentersStreamedReducer() {
		testQueryGeneric(100l*1024*1024*1024, 1024*1024, true, true);
	}
	
	/**
	 * Tests the query with statistics that push for a broadcast of the centers and a
	 * chained reducer after a block-nested-loops cross.
	 */
	@Test
	public void testQueryBCCentersBlockNLChainedReducer() {
		testQueryGeneric(100l*1024*1024*1024, 10l*1024*1024*1024, true, false);
	}
	
	/**
	 * Tests the query with statistics that push for a broadcast of the data points and a
	 * chained reducer after a block-nested-loops cross.
	 */
	@Test
	public void testQueryBCPointsBlockNLChainedReducer() {
		testQueryGeneric(1024*1024, 100l*1024*1024*1024, false, false);
	}
	
	// ------------------------------------------------------------------------
		
	private void testQueryGeneric(long dataPointsSize, long centersSize, boolean bcCenters, boolean streamedNL) {
		try {
			KMeansIteration job = new KMeansIteration();
			Plan p = job.getPlan(DEFAULT_PARALLELISM_STRING, IN_FILE, IN_FILE, OUT_FILE);
			
			// set statistics
			ContractResolver cr = getContractResolver(p);
			FileDataSource pointsSource = cr.getNode(DATAPOINTS);
			FileDataSource centersSource = cr.getNode(CENTERS);
			setSourceStatistics(pointsSource, dataPointsSize, 30f);
			setSourceStatistics(centersSource, centersSize, 30f);
			
			// compile
			final OptimizedPlan plan = compileWithStats(p);
			final OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(plan);
			
			// get the nodes from the final plan
			final SinkPlanNode sink = or.getNode("New Center Positions");
			final SingleInputPlanNode reducer2 = or.getNode("Recompute Center Positions");
			final SingleInputPlanNode combiner2 = (SingleInputPlanNode) reducer2.getPredecessor(); 
			final SingleInputPlanNode reducer1 = or.getNode("Find Nearest Centers");
			final SingleInputPlanNode combiner1 = reducer1.getPredecessor() instanceof SingleInputPlanNode ?
				(SingleInputPlanNode) reducer1.getPredecessor() : null;
				
			final DualInputPlanNode cross = or.getNode("Compute Distances");
			
			checkStandardStrategies(reducer1, combiner1, reducer2, combiner2, sink);
			checkBroadCastSide(cross, !bcCenters);
			if (streamedNL) {
				checkStreamedReducer(cross, reducer1, combiner1);
			} else {
				checkBlockNLChainedReducer(cross, reducer1, combiner1, true, true);
			}

		} catch (Exception e) {
			e.printStackTrace();
			Assert.fail(e.getMessage());
		}
	}

	// ------------------------------------------------------------------------
	//  Checks for special conditions
	// ------------------------------------------------------------------------
	
	private void checkStandardStrategies(
			SingleInputPlanNode nearestCenterReducer, SingleInputPlanNode nearestCenterCombiner,
			SingleInputPlanNode newCenterReducer, SingleInputPlanNode newCenterCombiner, SinkPlanNode sink)
	{
		// check that the new centers combiner is always there
		Assert.assertNotNull(newCenterCombiner);
		
		// check ship strategies that are always fix
		Assert.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, newCenterReducer.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.FORWARD, newCenterCombiner.getInput().getShipStrategy());
		if (nearestCenterCombiner != null) {
			Assert.assertEquals(ShipStrategyType.PARTITION_HASH, nearestCenterReducer.getInput().getShipStrategy());
			Assert.assertEquals(ShipStrategyType.FORWARD, nearestCenterCombiner.getInput().getShipStrategy());
		}
		
		// check the driver strategies that are always fix
		Assert.assertEquals(DriverStrategy.NONE, sink.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.SORTED_GROUP, newCenterReducer.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.SORTED_GROUP, nearestCenterReducer.getDriverStrategy());
		Assert.assertEquals(DriverStrategy.PARTIAL_GROUP, newCenterCombiner.getDriverStrategy());
		if (nearestCenterCombiner != null) {
			Assert.assertEquals(DriverStrategy.PARTIAL_GROUP, nearestCenterCombiner.getDriverStrategy());
		}
		
		// check the local strategies and local strategy keys
		Assert.assertEquals(LocalStrategy.NONE, newCenterCombiner.getInput().getLocalStrategy());
		Assert.assertEquals(set0, newCenterCombiner.getKeys());
		Assert.assertNull(newCenterCombiner.getInput().getLocalStrategyKeys());
		Assert.assertNull(newCenterCombiner.getInput().getLocalStrategySortOrder());
		
		Assert.assertEquals(LocalStrategy.COMBININGSORT, newCenterReducer.getInput().getLocalStrategy());
		Assert.assertEquals(set0, newCenterReducer.getKeys());
		Assert.assertEquals(set0, newCenterReducer.getInput().getLocalStrategyKeys());
		Assert.assertTrue(Arrays.equals(newCenterReducer.getInput().getLocalStrategySortOrder(), newCenterReducer.getSortOrders()));
		
		if (nearestCenterCombiner != null) {
			Assert.assertEquals(LocalStrategy.NONE, nearestCenterCombiner.getInput().getLocalStrategy());
			Assert.assertEquals(set0, nearestCenterCombiner.getKeys());
			Assert.assertNull(nearestCenterCombiner.getInput().getLocalStrategyKeys());
			Assert.assertNull(nearestCenterCombiner.getInput().getLocalStrategySortOrder());
			
			Assert.assertEquals(LocalStrategy.COMBININGSORT, nearestCenterReducer.getInput().getLocalStrategy());
			Assert.assertEquals(set0, nearestCenterReducer.getKeys());
			Assert.assertEquals(set0, nearestCenterReducer.getInput().getLocalStrategyKeys());
			Assert.assertTrue(Arrays.equals(nearestCenterReducer.getInput().getLocalStrategySortOrder(), nearestCenterReducer.getSortOrders()));
		}
	}
	
	private void checkBroadCastSide(DualInputPlanNode cross, boolean bcFirst) {
		if (bcFirst) {
			Assert.assertEquals(ShipStrategyType.BROADCAST, cross.getInput1().getShipStrategy());
			Assert.assertEquals(ShipStrategyType.FORWARD, cross.getInput2().getShipStrategy());
		} else {
			Assert.assertEquals(ShipStrategyType.FORWARD, cross.getInput1().getShipStrategy());
			Assert.assertEquals(ShipStrategyType.BROADCAST, cross.getInput2().getShipStrategy());
		}
	}
	
	private void checkBlockNLChainedReducer(DualInputPlanNode cross, SingleInputPlanNode reducer,
			SingleInputPlanNode combiner, boolean outerFirstOkay, boolean outerSecondOkay)
	{
		// check the cross drivers
		Assert.assertTrue(
			(cross.getDriverStrategy() == DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_FIRST && outerFirstOkay) ||
			(cross.getDriverStrategy() == DriverStrategy.NESTEDLOOP_BLOCKED_OUTER_SECOND && outerSecondOkay) );
		
		// check that the reducer is chained
		Assert.assertNull(combiner);
		Assert.assertEquals(ShipStrategyType.FORWARD, reducer.getInput().getShipStrategy());
	}
	
	private void checkStreamedReducer(DualInputPlanNode cross, SingleInputPlanNode reducer,
			SingleInputPlanNode combiner)
	{
		// check the cross drivers
		Assert.assertTrue(cross.getDriverStrategy() == DriverStrategy.NESTEDLOOP_STREAMED_OUTER_FIRST);
		
		// check that the reducer is streamed
		Assert.assertNull(combiner);
		Assert.assertEquals(ShipStrategyType.FORWARD, reducer.getInput().getShipStrategy());
		Assert.assertEquals(LocalStrategy.NONE, reducer.getInput().getLocalStrategy());
	}
}
