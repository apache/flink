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
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.testfunctions.IdentityCoGrouper;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducer;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.apache.flink.runtime.operators.util.LocalStrategy;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 * This test case has been created to validate that correct strategies are used if orders within groups are
 * requested.
 */
@SuppressWarnings({"serial"})
public class GroupOrderTest extends CompilerTestBase {

	@Test
	public void testReduceWithGroupOrder() {
		// construct the plan
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);
		DataSet<Tuple4<Long, Long, Long, Long>> set1 = env.readCsvFile("/tmp/fake.csv")
				.types(Long.class, Long.class, Long.class, Long.class);

		set1.groupBy(1).sortGroup(3, Order.DESCENDING)
				.reduceGroup(new IdentityGroupReducer<Tuple4<Long, Long, Long, Long>>()).name("Reduce")
				.output(new DiscardingOutputFormat<Tuple4<Long, Long, Long, Long>>()).name("Sink");

		Plan plan = env.createProgramPlan();
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
		
		FieldList ship = new FieldList(1);
		FieldList local = new FieldList(1, 3);
		Assert.assertEquals(ship, c.getShipStrategyKeys());
		Assert.assertEquals(local, c.getLocalStrategyKeys());
		Assert.assertTrue(c.getLocalStrategySortOrder()[0] == reducer.getSortOrders(0)[0]);
		
		// check that we indeed sort descending
		Assert.assertEquals(false, c.getLocalStrategySortOrder()[1]);
	}
	
	@Test
	public void testCoGroupWithGroupOrder() {
		// construct the plan
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);
		DataSet<Tuple7<Long, Long, Long, Long, Long, Long, Long>> set1 = env.readCsvFile("/tmp/fake1.csv")
				.types(Long.class, Long.class, Long.class, Long.class, Long.class, Long.class, Long.class);
		DataSet<Tuple7<Long, Long, Long, Long, Long, Long, Long>> set2 = env.readCsvFile("/tmp/fake2.csv")
				.types(Long.class, Long.class, Long.class, Long.class, Long.class, Long.class, Long.class);

		set1.coGroup(set2).where(3,0).equalTo(6,0)
				.sortFirstGroup(5, Order.DESCENDING)
				.sortSecondGroup(1, Order.DESCENDING).sortSecondGroup(4, Order.ASCENDING)
				.with(new IdentityCoGrouper<Tuple7<Long, Long, Long, Long, Long, Long, Long>>()).name("CoGroup")
				.output(new DiscardingOutputFormat<Tuple7<Long, Long, Long, Long, Long, Long, Long>>()).name("Sink");

		Plan plan = env.createProgramPlan();
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
		
		FieldList ship1 = new FieldList(3, 0);
		FieldList ship2 = new FieldList(6, 0);
		
		FieldList local1 = new FieldList(3, 0, 5);
		FieldList local2 = new FieldList(6, 0, 1, 4);
		
		Assert.assertEquals(ship1, c1.getShipStrategyKeys());
		Assert.assertEquals(ship2, c2.getShipStrategyKeys());
		Assert.assertEquals(local1, c1.getLocalStrategyKeys());
		Assert.assertEquals(local2, c2.getLocalStrategyKeys());
		
		Assert.assertTrue(c1.getLocalStrategySortOrder()[0] == coGroupNode.getSortOrders()[0]);
		Assert.assertTrue(c1.getLocalStrategySortOrder()[1] == coGroupNode.getSortOrders()[1]);
		Assert.assertTrue(c2.getLocalStrategySortOrder()[0] == coGroupNode.getSortOrders()[0]);
		Assert.assertTrue(c2.getLocalStrategySortOrder()[1] == coGroupNode.getSortOrders()[1]);
		
		// check that the local group orderings are correct
		Assert.assertEquals(false, c1.getLocalStrategySortOrder()[2]);
		Assert.assertEquals(false, c2.getLocalStrategySortOrder()[2]);
		Assert.assertEquals(true, c2.getLocalStrategySortOrder()[3]);
	}
}
