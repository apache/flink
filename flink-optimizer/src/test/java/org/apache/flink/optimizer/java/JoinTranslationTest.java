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

package org.apache.flink.optimizer.java;

import static org.junit.Assert.*;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.GenericDataSourceBase;
import org.apache.flink.api.common.operators.Operator;
import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.util.Visitor;
import org.junit.Test;

@SuppressWarnings("serial")
public class JoinTranslationTest extends CompilerTestBase {

	@Test
	public void testBroadcastHashFirstTest() {
		try {
			DualInputPlanNode node = createPlanAndGetJoinNode(JoinHint.BROADCAST_HASH_FIRST);
			assertEquals(ShipStrategyType.BROADCAST, node.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, node.getInput2().getShipStrategy());
			assertEquals(DriverStrategy.HYBRIDHASH_BUILD_FIRST, node.getDriverStrategy());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + ": " + e.getMessage());
		}
	}
	
	@Test
	public void testBroadcastHashSecondTest() {
		try {
			DualInputPlanNode node = createPlanAndGetJoinNode(JoinHint.BROADCAST_HASH_SECOND);
			assertEquals(ShipStrategyType.FORWARD, node.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.BROADCAST, node.getInput2().getShipStrategy());
			assertEquals(DriverStrategy.HYBRIDHASH_BUILD_SECOND, node.getDriverStrategy());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + ": " + e.getMessage());
		}
	}
	
	@Test
	public void testPartitionHashFirstTest() {
		try {
			DualInputPlanNode node = createPlanAndGetJoinNode(JoinHint.REPARTITION_HASH_FIRST);
			assertEquals(ShipStrategyType.PARTITION_HASH, node.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.PARTITION_HASH, node.getInput2().getShipStrategy());
			assertEquals(DriverStrategy.HYBRIDHASH_BUILD_FIRST, node.getDriverStrategy());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + ": " + e.getMessage());
		}
	}
	
	@Test
	public void testPartitionHashSecondTest() {
		try {
			DualInputPlanNode node = createPlanAndGetJoinNode(JoinHint.REPARTITION_HASH_SECOND);
			assertEquals(ShipStrategyType.PARTITION_HASH, node.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.PARTITION_HASH, node.getInput2().getShipStrategy());
			assertEquals(DriverStrategy.HYBRIDHASH_BUILD_SECOND, node.getDriverStrategy());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + ": " + e.getMessage());
		}
	}
	
	@Test
	public void testPartitionSortMergeTest() {
		try {
			DualInputPlanNode node = createPlanAndGetJoinNode(JoinHint.REPARTITION_SORT_MERGE);
			assertEquals(ShipStrategyType.PARTITION_HASH, node.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.PARTITION_HASH, node.getInput2().getShipStrategy());
			assertEquals(DriverStrategy.MERGE, node.getDriverStrategy());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + ": " + e.getMessage());
		}
	}
	
	@Test
	public void testOptimizerChoosesTest() {
		try {
			DualInputPlanNode node = createPlanAndGetJoinNode(JoinHint.OPTIMIZER_CHOOSES);
			assertEquals(ShipStrategyType.PARTITION_HASH, node.getInput1().getShipStrategy());
			assertEquals(ShipStrategyType.PARTITION_HASH, node.getInput2().getShipStrategy());
			assertTrue(DriverStrategy.HYBRIDHASH_BUILD_FIRST == node.getDriverStrategy() ||
					DriverStrategy.HYBRIDHASH_BUILD_SECOND == node.getDriverStrategy());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getClass().getSimpleName() + ": " + e.getMessage());
		}
	}
	
	
	private DualInputPlanNode createPlanAndGetJoinNode(JoinHint hint) {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
		DataSet<Long> i1 = env.generateSequence(1, 1000);
		DataSet<Long> i2 = env.generateSequence(1, 1000);
		
		i1.join(i2, hint).where(new IdentityKeySelector<Long>()).equalTo(new IdentityKeySelector<Long>()).output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
		
		Plan plan = env.createProgramPlan();
		
		// set statistics to the sources
		plan.accept(new Visitor<Operator<?>>() {
			@Override
			public boolean preVisit(Operator<?> visitable) {
				if (visitable instanceof GenericDataSourceBase) {
					GenericDataSourceBase<?, ?> source = (GenericDataSourceBase<?, ?>) visitable;
					setSourceStatistics(source, 10000000, 1000);
				}
				
				return true;
			}
			
			@Override
			public void postVisit(Operator<?> visitable) {}
		});
		
		OptimizedPlan op = compileWithStats(plan);
		
		return (DualInputPlanNode) ((SinkPlanNode) op.getDataSinks().iterator().next()).getInput().getSource();
	}
	
	
	
	private static final class IdentityKeySelector<T> implements KeySelector<T, T> {
		
		@Override
		public T getKey(T value) {
			return value;
		}
	}
}
