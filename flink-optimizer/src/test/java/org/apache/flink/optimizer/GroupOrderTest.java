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

import static org.junit.Assert.fail;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.record.operators.CoGroupOperator;
import org.apache.flink.api.java.record.operators.FileDataSink;
import org.apache.flink.api.java.record.operators.FileDataSource;
import org.apache.flink.api.java.record.operators.ReduceOperator;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.util.DummyCoGroupStub;
import org.apache.flink.optimizer.util.DummyInputFormat;
import org.apache.flink.optimizer.util.DummyOutputFormat;
import org.apache.flink.optimizer.util.IdentityReduce;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.types.DoubleValue;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.StringValue;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test case has been created to validate that correct strategies are used if orders within groups are
 * requested.
 */
@SuppressWarnings({"serial", "deprecation"})
public class GroupOrderTest extends CompilerTestBase {

	@Test
	public void testReduceWithGroupOrder() {
		// construct the plan
		FileDataSource source = new FileDataSource(new DummyInputFormat(), IN_FILE, "Source");
		
		ReduceOperator reduce = ReduceOperator.builder(new IdentityReduce()).keyField(IntValue.class, 2).name("Reduce").input(source).build();
		Ordering groupOrder = new Ordering(5, StringValue.class, Order.DESCENDING);
		reduce.setGroupOrder(groupOrder);
		
		FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, reduce, "Sink");
		
		
		Plan plan = new Plan(sink, "Test Temp Task");
		plan.setDefaultParallelism(DEFAULT_PARALLELISM);
		
		OptimizedPlan oPlan;
		try {
			oPlan = compileNoStats(plan);
		} catch(CompilerException ce) {
			ce.printStackTrace();
			fail("The pact compiler is unable to compile this plan correctly.");
			return; // silence the compiler
		}
		
		OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
		SinkPlanNode sinkNode = resolver.getNode("Sink");
		SingleInputPlanNode reducer = resolver.getNode("Reduce");
		
		// verify the strategies
		Assert.assertEquals(ShipStrategyType.FORWARD, sinkNode.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, reducer.getInput().getShipStrategy());
		
		Channel c = reducer.getInput();
		Assert.assertEquals(LocalStrategy.SORT, c.getLocalStrategy());
		
		FieldList ship = new FieldList(2);
		FieldList local = new FieldList(2, 5);
		Assert.assertEquals(ship, c.getShipStrategyKeys());
		Assert.assertEquals(local, c.getLocalStrategyKeys());
		Assert.assertTrue(c.getLocalStrategySortOrder()[0] == reducer.getSortOrders(0)[0]);
		
		// check that we indeed sort descending
		Assert.assertTrue(c.getLocalStrategySortOrder()[1] == groupOrder.getFieldSortDirections()[0]);
	}
	
	@Test
	public void testCoGroupWithGroupOrder() {
		// construct the plan
		FileDataSource source1 = new FileDataSource(new DummyInputFormat(), IN_FILE, "Source1");
		FileDataSource source2 = new FileDataSource(new DummyInputFormat(), IN_FILE, "Source2");
		
		CoGroupOperator coGroup = CoGroupOperator.builder(new DummyCoGroupStub(), IntValue.class, 3, 6)
				.keyField(LongValue.class, 0, 0)
				.name("CoGroup").input1(source1).input2(source2).build();
		
		Ordering groupOrder1 = new Ordering(5, StringValue.class, Order.DESCENDING);
		Ordering groupOrder2 = new Ordering(1, StringValue.class, Order.DESCENDING);
		groupOrder2.appendOrdering(4, DoubleValue.class, Order.ASCENDING);
		coGroup.setGroupOrderForInputOne(groupOrder1);
		coGroup.setGroupOrderForInputTwo(groupOrder2);
		
		FileDataSink sink = new FileDataSink(new DummyOutputFormat(), OUT_FILE, coGroup, "Sink");
		
		Plan plan = new Plan(sink, "Reduce Group Order Test");
		plan.setDefaultParallelism(DEFAULT_PARALLELISM);
		
		OptimizedPlan oPlan;
		try {
			oPlan = compileNoStats(plan);
		} catch(CompilerException ce) {
			ce.printStackTrace();
			fail("The pact compiler is unable to compile this plan correctly.");
			return; // silence the compiler
		}
		
		OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(oPlan);
		SinkPlanNode sinkNode = resolver.getNode("Sink");
		DualInputPlanNode coGroupNode = resolver.getNode("CoGroup");
		
		// verify the strategies
		Assert.assertEquals(ShipStrategyType.FORWARD, sinkNode.getInput().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, coGroupNode.getInput1().getShipStrategy());
		Assert.assertEquals(ShipStrategyType.PARTITION_HASH, coGroupNode.getInput2().getShipStrategy());
		
		Channel c1 = coGroupNode.getInput1();
		Channel c2 = coGroupNode.getInput2();
		
		Assert.assertEquals(LocalStrategy.SORT, c1.getLocalStrategy());
		Assert.assertEquals(LocalStrategy.SORT, c2.getLocalStrategy());
		
		FieldList ship1 = new FieldList(new int[] {3, 0});
		FieldList ship2 = new FieldList(new int[] {6, 0});
		
		FieldList local1 = new FieldList(new int[] {3, 0, 5});
		FieldList local2 = new FieldList(new int[] {6, 0, 1, 4});
		
		Assert.assertEquals(ship1, c1.getShipStrategyKeys());
		Assert.assertEquals(ship2, c2.getShipStrategyKeys());
		Assert.assertEquals(local1, c1.getLocalStrategyKeys());
		Assert.assertEquals(local2, c2.getLocalStrategyKeys());
		
		Assert.assertTrue(c1.getLocalStrategySortOrder()[0] == coGroupNode.getSortOrders()[0]);
		Assert.assertTrue(c1.getLocalStrategySortOrder()[1] == coGroupNode.getSortOrders()[1]);
		Assert.assertTrue(c2.getLocalStrategySortOrder()[0] == coGroupNode.getSortOrders()[0]);
		Assert.assertTrue(c2.getLocalStrategySortOrder()[1] == coGroupNode.getSortOrders()[1]);
		
		// check that the local group orderings are correct
		Assert.assertTrue(c1.getLocalStrategySortOrder()[2] == groupOrder1.getFieldSortDirections()[0]);
		Assert.assertTrue(c2.getLocalStrategySortOrder()[2] == groupOrder2.getFieldSortDirections()[0]);
		Assert.assertTrue(c2.getLocalStrategySortOrder()[3] == groupOrder2.getFieldSortDirections()[1]);
	}
}
