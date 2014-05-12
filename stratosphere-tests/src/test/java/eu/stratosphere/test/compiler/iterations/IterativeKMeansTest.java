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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

import eu.stratosphere.api.common.Plan;
import eu.stratosphere.api.common.operators.FileDataSource;
import eu.stratosphere.api.common.operators.util.FieldList;
import eu.stratosphere.compiler.plan.BulkIterationPlanNode;
import eu.stratosphere.compiler.plan.OptimizedPlan;
import eu.stratosphere.compiler.plan.SingleInputPlanNode;
import eu.stratosphere.compiler.plan.SinkPlanNode;
import eu.stratosphere.compiler.plantranslate.NepheleJobGraphGenerator;
import eu.stratosphere.pact.runtime.shipping.ShipStrategyType;
import eu.stratosphere.pact.runtime.task.DriverStrategy;
import eu.stratosphere.pact.runtime.task.util.LocalStrategy;
import eu.stratosphere.test.compiler.CompilerTestBase;
import eu.stratosphere.test.recordJobs.kmeans.KMeans;


public class IterativeKMeansTest extends CompilerTestBase {
	
	private static final String DATAPOINTS = "Data Points";
	private static final String CENTERS = "Centers";
	
	private static final String MAPPER_NAME = "Find Nearest Centers";
	private static final String REDUCER_NAME = "Recompute Center Positions";
	
	private static final String ITERATION_NAME = "k-means loop";
	
	private static final String SINK = "New Center Positions";
	
	private final FieldList set0 = new FieldList(0);
	
	// --------------------------------------------------------------------------------------------
	//  K-Means (Bulk Iteration)
	// --------------------------------------------------------------------------------------------
	
	@Test
	public void testCompileKMeansSingleStepWithStats() {
		
		KMeans kmi = new KMeans();
		Plan p = kmi.getPlan(String.valueOf(DEFAULT_PARALLELISM), IN_FILE, IN_FILE, OUT_FILE, String.valueOf(20));
		
		// set the statistics
		ContractResolver cr = getContractResolver(p);
		FileDataSource pointsSource = cr.getNode(DATAPOINTS);
		FileDataSource centersSource = cr.getNode(CENTERS);
		setSourceStatistics(pointsSource, 100l*1024*1024*1024, 32f);
		setSourceStatistics(centersSource, 1024*1024, 32f);
		
		OptimizedPlan plan = compileWithStats(p);
		checkPlan(plan);
		
		new NepheleJobGraphGenerator().compileJobGraph(plan);
	}

	@Test
	public void testCompileKMeansSingleStepWithOutStats() {
		
		KMeans kmi = new KMeans();
		Plan p = kmi.getPlan(String.valueOf(DEFAULT_PARALLELISM), IN_FILE, IN_FILE, OUT_FILE, String.valueOf(20));
		
		OptimizedPlan plan = compileNoStats(p);
		checkPlan(plan);
		
		new NepheleJobGraphGenerator().compileJobGraph(plan);
	}
	
	private void checkPlan(OptimizedPlan plan) {
		
		OptimizerPlanNodeResolver or = getOptimizerPlanNodeResolver(plan);
		
		final SinkPlanNode sink = or.getNode(SINK);
		final SingleInputPlanNode reducer = or.getNode(REDUCER_NAME);
		final SingleInputPlanNode combiner = (SingleInputPlanNode) reducer.getPredecessor();
		final SingleInputPlanNode mapper = or.getNode(MAPPER_NAME);
		
		final BulkIterationPlanNode iter = or.getNode(ITERATION_NAME);
		
		// -------------------- outside the loop -----------------------
		
		// check the sink
		assertEquals(ShipStrategyType.FORWARD, sink.getInput().getShipStrategy());
		assertEquals(LocalStrategy.NONE, sink.getInput().getLocalStrategy());
		
		// check the iteration
		assertEquals(ShipStrategyType.FORWARD, iter.getInput().getShipStrategy());
		assertEquals(LocalStrategy.NONE, iter.getInput().getLocalStrategy());
		
		
		// -------------------- inside the loop -----------------------
		
		// check the mapper
		assertEquals(1, mapper.getBroadcastInputs().size());
		assertEquals(ShipStrategyType.FORWARD, mapper.getInput().getShipStrategy());
		assertEquals(ShipStrategyType.BROADCAST, mapper.getBroadcastInputs().get(0).getShipStrategy());
		assertFalse(mapper.getInput().isOnDynamicPath());
		assertTrue(mapper.getBroadcastInputs().get(0).isOnDynamicPath());
		assertTrue(mapper.getInput().getTempMode().isCached());
		
		assertEquals(LocalStrategy.NONE, mapper.getInput().getLocalStrategy());
		assertEquals(LocalStrategy.NONE, mapper.getBroadcastInputs().get(0).getLocalStrategy());
		
		assertEquals(DriverStrategy.COLLECTOR_MAP, mapper.getDriverStrategy());
		
		assertNull(mapper.getInput().getLocalStrategyKeys());
		assertNull(mapper.getInput().getLocalStrategySortOrder());
		assertNull(mapper.getBroadcastInputs().get(0).getLocalStrategyKeys());
		assertNull(mapper.getBroadcastInputs().get(0).getLocalStrategySortOrder());
		
		// check the combiner
		Assert.assertNotNull(combiner);
		assertEquals(ShipStrategyType.FORWARD, combiner.getInput().getShipStrategy());
		assertTrue(combiner.getInput().isOnDynamicPath());
		
		assertEquals(LocalStrategy.NONE, combiner.getInput().getLocalStrategy());
		assertEquals(DriverStrategy.SORTED_GROUP_COMBINE, combiner.getDriverStrategy());
		assertNull(combiner.getInput().getLocalStrategyKeys());
		assertNull(combiner.getInput().getLocalStrategySortOrder());
		assertEquals(set0, combiner.getKeys());
		
		// check the reducer
		assertEquals(ShipStrategyType.PARTITION_HASH, reducer.getInput().getShipStrategy());
		assertTrue(reducer.getInput().isOnDynamicPath());
		assertEquals(LocalStrategy.COMBININGSORT, reducer.getInput().getLocalStrategy());
		assertEquals(DriverStrategy.SORTED_GROUP_REDUCE, reducer.getDriverStrategy());
		assertEquals(set0, reducer.getKeys());
		assertEquals(set0, reducer.getInput().getLocalStrategyKeys());
		assertTrue(Arrays.equals(reducer.getInput().getLocalStrategySortOrder(), reducer.getSortOrders()));
	}
}
