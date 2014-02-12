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
package eu.stratosphere.test.compiler.iterations;

import java.util.Arrays;

import org.junit.Assert;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.api.java.record.operators.CrossOperator;
import eu.stratosphere.compiler.PactCompiler;
import eu.stratosphere.compiler.plan.BulkIterationPlanNode;
import eu.stratosphere.compiler.plan.DualInputPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plan.SinkPlanNode;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.example.java.record.kmeans.KMeansIterative;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.test.compiler.CompilerTestBase;


/**
 *
 */
public class IterativeKMeansTest extends CompilerTestBase {
	
	private static final String DATAPOINTS = "Data Points";
	private static final String CENTERS = "Centers";
	
	private static final String ITERATION_NAME = "K-Means Loop";
	
	private static final String CROSS_NAME = "Compute Distances";
	private static final String NEAREST_CENTER_REDUCER = "Find Nearest Centers";
	private static final String RECOMPUTE_CENTERS_REDUCER = "Recompute Center Positions";
	
	private static final String SINK = "Cluster Positions";
	
	private final FieldList set0 = new FieldList(0);
	
	// --------------------------------------------------------------------------------------------
	//  K-Means (Bulk Iteration)
	// --------------------------------------------------------------------------------------------
	
//	@Test
	public void testCompileKMeansWithStats() {
		
		KMeansIterative kmi = new KMeansIterative();
		Plan p = kmi.getPlan(String.valueOf(DEFAULT_PARALLELISM),
				IN_FILE, IN_FILE, OUT_FILE, String.valueOf(20));
		
		// set the statistics
		ContractResolver cr = getContractResolver(p);
		FileDataSource pointsSource = cr.getNode(DATAPOINTS);
		FileDataSource centersSource = cr.getNode(CENTERS);
		setSourceStatistics(pointsSource, 100l*1024*1024*1024, 32f);
		setSourceStatistics(centersSource, 1024*1024, 32f);
		
		OptimizedPlan plan = compileWithStats(p);
		OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(plan);
		
		final SinkPlanNode sink = or.getNode(SINK);
		final BulkIterationPlanNode iter = or.getNode(ITERATION_NAME);
		
		final SingleInputPlanNode newCenterReducer = or.getNode(RECOMPUTE_CENTERS_REDUCER);
		final SingleInputPlanNode newCenterCombiner = (SingleInputPlanNode) newCenterReducer.getPredecessor(); 
		final SingleInputPlanNode nearestCenterReducer = or.getNode(NEAREST_CENTER_REDUCER);
		final SingleInputPlanNode nearestCenterCombiner = nearestCenterReducer.getPredecessor() instanceof SingleInputPlanNode ?
				(SingleInputPlanNode) nearestCenterReducer.getPredecessor() : null;
						
		final DualInputPlanNode cross = or.getNode(CROSS_NAME);
		
		checkIterNodeAndSink(iter, sink);
		checkStandardStrategies(nearestCenterReducer, nearestCenterCombiner, newCenterReducer, newCenterCombiner);
		
		// make sure that the partitioning is pushed down
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, cross.getInput1().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.BROADCAST, cross.getInput2().getShipStrategy());
		Assert.assertTrue(cross.getInput1().getTempMode().isCached());
		
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		jobGen.compileJobGraph(plan);
	}
	
//	@Test
	public void testCompileKMeansIterationForwardCenters() {
		
		KMeansIterative kmi = new KMeansIterative();
		Plan p = kmi.getPlan(String.valueOf(DEFAULT_PARALLELISM),
				IN_FILE, IN_FILE, OUT_FILE, String.valueOf(20));
		
		// parameterize the cross strategies
		ContractResolver cr = getContractResolver(p);
		CrossOperator crossNode = cr.getNode(CROSS_NAME);
		crossNode.setParameter(PactCompiler.HINT_SHIP_STRATEGY_SECOND_INPUT, PactCompiler.HINT_SHIP_STRATEGY_FORWARD);
		
		OptimizedPlan plan = compileNoStats(p);
		OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(plan);
		
		final SinkPlanNode sink = or.getNode(SINK);
		final BulkIterationPlanNode iter = or.getNode(ITERATION_NAME);
		
		final SingleInputPlanNode newCenterReducer = or.getNode(RECOMPUTE_CENTERS_REDUCER);
		final SingleInputPlanNode newCenterCombiner = (SingleInputPlanNode) newCenterReducer.getPredecessor(); 
		final SingleInputPlanNode nearestCenterReducer = or.getNode(NEAREST_CENTER_REDUCER);
		final SingleInputPlanNode nearestCenterCombiner = nearestCenterReducer.getPredecessor() instanceof SingleInputPlanNode ?
				(SingleInputPlanNode) nearestCenterReducer.getPredecessor() : null;
						
		final DualInputPlanNode cross = or.getNode(CROSS_NAME);
		
		checkIterNodeAndSink(iter, sink);
		checkStandardStrategies(nearestCenterReducer, nearestCenterCombiner, newCenterReducer, newCenterCombiner);
		
		// make sure that the partitioning is pushed down
		Assert.assertEquals(ShipStrategyType.BROADCAST, cross.getInput1().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.FORWARD, cross.getInput2().getShipStrategy());
		
		NepheleJobGraphGenerator jobGen = new NepheleJobGraphGenerator();
		jobGen.compileJobGraph(plan);
	}
	
	// --------------------------------------------------------------------------------------------
	//  Check methods for individual cases
	// --------------------------------------------------------------------------------------------
	
	private void checkIterNodeAndSink(BulkIterationPlanNode iterationNode, SinkPlanNode sink) {
		Assert.assertEquals(ShipStrategyType.PARTITION_RANDOM, iterationNode.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
		Assert.assertEquals(DriverStrategy.NONE, sink.getDriverStrategy());
	}
	
	private void checkStandardStrategies(
			SingleInputPlanNode nearestCenterReducer, SingleInputPlanNode nearestCenterCombiner,
			SingleInputPlanNode newCenterReducer, SingleInputPlanNode newCenterCombiner)
	{
		// check that the new centers combiner is always there
		Assert.assertNotNull(newCenterCombiner);
		
		// check ship strategies that are always fix
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, newCenterReducer.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.FORWARD, newCenterCombiner.getInput().getShipStrategy());
		if (nearestCenterCombiner != null) {
			Assert.assertEquals(ShipStrategyType.PARTITION_HASH, nearestCenterReducer.getInput().getShipStrategy());
			Assert.assertEquals(ShipStrategyType.FORWARD, nearestCenterCombiner.getInput().getShipStrategy());
		}
		
		// check the driver strategies that are always fix
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
}
