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

import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.operators.Ordering;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.operators.util.FieldList;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.common.Plan;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.dataproperties.PartitioningProperty;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.NAryUnionPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.plantranslate.JobGraphGenerator;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducer;
import org.apache.flink.optimizer.testfunctions.IdentityMapper;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.operators.DriverStrategy;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

@SuppressWarnings("serial")
public class UnionReplacementTest extends CompilerTestBase {

	@Test
	public void testUnionReplacement() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			DataSet<String> input1 = env.fromElements("test1");
			DataSet<String> input2 = env.fromElements("test2");
	
			DataSet<String> union = input1.union(input2);
	
			union.output(new DiscardingOutputFormat<String>());
			union.output(new DiscardingOutputFormat<String>());
	
			Plan plan = env.createProgramPlan();
			OptimizedPlan oPlan = compileNoStats(plan);
			JobGraphGenerator jobGen = new JobGraphGenerator();
			jobGen.compileJobGraph(oPlan);
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 *
	 * Test for FLINK-2662.
	 *
	 * Checks that a plan with an union with two outputs is correctly translated.
	 * The program can be illustrated as follows:
	 *
	 * Src1 ----------------\
	 *                       >-> Union123 -> GroupBy(0) -> Sum -> Output
	 * Src2 -\              /
	 *        >-> Union23--<
	 * Src3 -/              \
	 *                       >-> Union234 -> GroupBy(1) -> Sum -> Output
	 * Src4 ----------------/
	 *
	 * The fix for FLINK-2662 translates the union with two output (Union-23) into two separate
	 * unions (Union-23_1 and Union-23_2) with one output each. Due to this change, the interesting
	 * partitioning properties for GroupBy(0) and GroupBy(1) are pushed through Union-23_1 and
	 * Union-23_2 and do not interfere with each other (which would be the case if Union-23 would
	 * be a single operator with two outputs).
	 *
	 */
	@Test
	public void testUnionWithTwoOutputs() throws Exception {

		// -----------------------------------------------------------------------------------------
		// Build test program
		// -----------------------------------------------------------------------------------------

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);

		DataSet<Tuple2<Long, Long>> src1 = env.fromElements(new Tuple2<>(0L, 0L));
		DataSet<Tuple2<Long, Long>> src2 = env.fromElements(new Tuple2<>(0L, 0L));
		DataSet<Tuple2<Long, Long>> src3 = env.fromElements(new Tuple2<>(0L, 0L));
		DataSet<Tuple2<Long, Long>> src4 = env.fromElements(new Tuple2<>(0L, 0L));

		DataSet<Tuple2<Long, Long>> union23 = src2.union(src3);
		DataSet<Tuple2<Long, Long>> union123 = src1.union(union23);
		DataSet<Tuple2<Long, Long>> union234 = src4.union(union23);

		union123.groupBy(0).sum(1).name("1").output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
		union234.groupBy(1).sum(0).name("2").output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

		// -----------------------------------------------------------------------------------------
		// Verify optimized plan
		// -----------------------------------------------------------------------------------------

		OptimizedPlan optimizedPlan = compileNoStats(env.createProgramPlan());

		OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(optimizedPlan);

		SingleInputPlanNode groupRed1 = resolver.getNode("1");
		SingleInputPlanNode groupRed2 = resolver.getNode("2");

		// check partitioning is correct
		assertTrue("Reduce input should be partitioned on 0.",
			groupRed1.getInput().getGlobalProperties().getPartitioningFields().isExactMatch(new FieldList(0)));
		assertTrue("Reduce input should be partitioned on 1.",
			groupRed2.getInput().getGlobalProperties().getPartitioningFields().isExactMatch(new FieldList(1)));

		// check group reduce inputs are n-ary unions with three inputs
		assertTrue("Reduce input should be n-ary union with three inputs.",
			groupRed1.getInput().getSource() instanceof NAryUnionPlanNode &&
				((NAryUnionPlanNode) groupRed1.getInput().getSource()).getListOfInputs().size() == 3);
		assertTrue("Reduce input should be n-ary union with three inputs.",
			groupRed2.getInput().getSource() instanceof NAryUnionPlanNode &&
				((NAryUnionPlanNode) groupRed2.getInput().getSource()).getListOfInputs().size() == 3);

		// check channel from union to group reduce is forwarding
		assertTrue("Channel between union and group reduce should be forwarding",
			groupRed1.getInput().getShipStrategy().equals(ShipStrategyType.FORWARD));
		assertTrue("Channel between union and group reduce should be forwarding",
			groupRed2.getInput().getShipStrategy().equals(ShipStrategyType.FORWARD));

		// check that all inputs of unions are hash partitioned
		List<Channel> union123In = ((NAryUnionPlanNode) groupRed1.getInput().getSource()).getListOfInputs();
		for(Channel i : union123In) {
			assertTrue("Union input channel should hash partition on 0",
				i.getShipStrategy().equals(ShipStrategyType.PARTITION_HASH) &&
					i.getShipStrategyKeys().isExactMatch(new FieldList(0)));
		}
		List<Channel> union234In = ((NAryUnionPlanNode) groupRed2.getInput().getSource()).getListOfInputs();
		for(Channel i : union234In) {
			assertTrue("Union input channel should hash partition on 0",
				i.getShipStrategy().equals(ShipStrategyType.PARTITION_HASH) &&
					i.getShipStrategyKeys().isExactMatch(new FieldList(1)));
		}

	}

	/**
	 *
	 * Checks that a plan with consecutive UNIONs followed by PartitionByHash is correctly translated.
	 *
	 * The program can be illustrated as follows:
	 *
	 * Src1 -\
	 *        >-> Union12--<
	 * Src2 -/              \
	 *                       >-> Union123 -> PartitionByHash -> Output
	 * Src3 ----------------/
	 *
	 * In the resulting plan, the hash partitioning (ShippingStrategy.PARTITION_HASH) must be
	 * pushed to the inputs of the unions (Src1, Src2, Src3).
	 *
	 */
	@Test
	public void testConsecutiveUnionsWithHashPartitioning() throws Exception {

		// -----------------------------------------------------------------------------------------
		// Build test program
		// -----------------------------------------------------------------------------------------

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);

		DataSet<Tuple2<Long, Long>> src1 = env.fromElements(new Tuple2<>(0L, 0L));
		DataSet<Tuple2<Long, Long>> src2 = env.fromElements(new Tuple2<>(0L, 0L));
		DataSet<Tuple2<Long, Long>> src3 = env.fromElements(new Tuple2<>(0L, 0L));

		DataSet<Tuple2<Long, Long>> union12 = src1.union(src2);
		DataSet<Tuple2<Long, Long>> union123 = union12.union(src3);

		union123.partitionByHash(1).output(new DiscardingOutputFormat<Tuple2<Long, Long>>()).name("out");

		// -----------------------------------------------------------------------------------------
		// Verify optimized plan
		// -----------------------------------------------------------------------------------------

		OptimizedPlan optimizedPlan = compileNoStats(env.createProgramPlan());

		OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(optimizedPlan);

		SingleInputPlanNode sink = resolver.getNode("out");

		// check partitioning is correct
		assertEquals("Sink input should be hash partitioned.",
			PartitioningProperty.HASH_PARTITIONED, sink.getInput().getGlobalProperties().getPartitioning());
		assertEquals("Sink input should be hash partitioned on 1.",
			new FieldList(1), sink.getInput().getGlobalProperties().getPartitioningFields());

		SingleInputPlanNode partitioner = (SingleInputPlanNode)sink.getInput().getSource();
		assertTrue(partitioner.getDriverStrategy() == DriverStrategy.UNARY_NO_OP);
		assertEquals("Partitioner input should be hash partitioned.",
			PartitioningProperty.HASH_PARTITIONED, partitioner.getInput().getGlobalProperties().getPartitioning());
		assertEquals("Partitioner input should be hash partitioned on 1.",
			new FieldList(1), partitioner.getInput().getGlobalProperties().getPartitioningFields());
		assertEquals("Partitioner input channel should be forwarding",
			ShipStrategyType.FORWARD, partitioner.getInput().getShipStrategy());

		NAryUnionPlanNode union = (NAryUnionPlanNode)partitioner.getInput().getSource();
		// all union inputs should be hash partitioned
		for (Channel c : union.getInputs()) {
			assertEquals("Union input should be hash partitioned",
				PartitioningProperty.HASH_PARTITIONED, c.getGlobalProperties().getPartitioning());
			assertEquals("Union input channel should be hash partitioning",
				ShipStrategyType.PARTITION_HASH, c.getShipStrategy());
			assertTrue("Union input should be data source",
				c.getSource() instanceof SourcePlanNode);
		}
	}

	/**
	 *
	 * Checks that a plan with consecutive UNIONs followed by REBALANCE is correctly translated.
	 *
	 * The program can be illustrated as follows:
	 *
	 * Src1 -\
	 *        >-> Union12--<
	 * Src2 -/              \
	 *                       >-> Union123 -> Rebalance -> Output
	 * Src3 ----------------/
	 *
	 * In the resulting plan, the Rebalance (ShippingStrategy.PARTITION_FORCED_REBALANCE) must be
	 * pushed to the inputs of the unions (Src1, Src2, Src3).
	 *
	 */
	@Test
	public void testConsecutiveUnionsWithRebalance() throws Exception {

		// -----------------------------------------------------------------------------------------
		// Build test program
		// -----------------------------------------------------------------------------------------

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);

		DataSet<Tuple2<Long, Long>> src1 = env.fromElements(new Tuple2<>(0L, 0L));
		DataSet<Tuple2<Long, Long>> src2 = env.fromElements(new Tuple2<>(0L, 0L));
		DataSet<Tuple2<Long, Long>> src3 = env.fromElements(new Tuple2<>(0L, 0L));

		DataSet<Tuple2<Long, Long>> union12 = src1.union(src2);
		DataSet<Tuple2<Long, Long>> union123 = union12.union(src3);

		union123.rebalance().output(new DiscardingOutputFormat<Tuple2<Long, Long>>()).name("out");

		// -----------------------------------------------------------------------------------------
		// Verify optimized plan
		// -----------------------------------------------------------------------------------------

		OptimizedPlan optimizedPlan = compileNoStats(env.createProgramPlan());

		OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(optimizedPlan);

		SingleInputPlanNode sink = resolver.getNode("out");

		// check partitioning is correct
		assertEquals("Sink input should be force rebalanced.",
			PartitioningProperty.FORCED_REBALANCED, sink.getInput().getGlobalProperties().getPartitioning());

		SingleInputPlanNode partitioner = (SingleInputPlanNode)sink.getInput().getSource();
		assertTrue(partitioner.getDriverStrategy() == DriverStrategy.UNARY_NO_OP);
		assertEquals("Partitioner input should be force rebalanced.",
			PartitioningProperty.FORCED_REBALANCED, partitioner.getInput().getGlobalProperties().getPartitioning());
		assertEquals("Partitioner input channel should be forwarding",
			ShipStrategyType.FORWARD, partitioner.getInput().getShipStrategy());

		NAryUnionPlanNode union = (NAryUnionPlanNode)partitioner.getInput().getSource();
		// all union inputs should be force rebalanced
		for (Channel c : union.getInputs()) {
			assertEquals("Union input should be force rebalanced",
				PartitioningProperty.FORCED_REBALANCED, c.getGlobalProperties().getPartitioning());
			assertEquals("Union input channel should be rebalancing",
				ShipStrategyType.PARTITION_FORCED_REBALANCE, c.getShipStrategy());
			assertTrue("Union input should be data source",
				c.getSource() instanceof SourcePlanNode);
		}
	}

	/**
	 *
	 * Checks that a plan with consecutive UNIONs followed by PARTITION_RANGE is correctly translated.
	 *
	 * The program can be illustrated as follows:
	 *
	 * Src1 -\
	 *        >-> Union12--<
	 * Src2 -/              \
	 *                       >-> Union123 -> PartitionByRange -> Output
	 * Src3 ----------------/
	 *
	 * In the resulting plan, the range partitioning must be
	 * pushed to the inputs of the unions (Src1, Src2, Src3).
	 *
	 */
	@Test
	public void testConsecutiveUnionsWithRangePartitioning() throws Exception {

		// -----------------------------------------------------------------------------------------
		// Build test program
		// -----------------------------------------------------------------------------------------

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);

		DataSet<Tuple2<Long, Long>> src1 = env.fromElements(new Tuple2<>(0L, 0L));
		DataSet<Tuple2<Long, Long>> src2 = env.fromElements(new Tuple2<>(0L, 0L));
		DataSet<Tuple2<Long, Long>> src3 = env.fromElements(new Tuple2<>(0L, 0L));

		DataSet<Tuple2<Long, Long>> union12 = src1.union(src2);
		DataSet<Tuple2<Long, Long>> union123 = union12.union(src3);

		union123.partitionByRange(1).output(new DiscardingOutputFormat<Tuple2<Long, Long>>()).name("out");

		// -----------------------------------------------------------------------------------------
		// Verify optimized plan
		// -----------------------------------------------------------------------------------------

		OptimizedPlan optimizedPlan = compileNoStats(env.createProgramPlan());

		OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(optimizedPlan);

		SingleInputPlanNode sink = resolver.getNode("out");

		// check partitioning is correct
		assertEquals("Sink input should be range partitioned.",
			PartitioningProperty.RANGE_PARTITIONED, sink.getInput().getGlobalProperties().getPartitioning());
		assertEquals("Sink input should be range partitioned on 1",
			new Ordering(1, null, Order.ASCENDING), sink.getInput().getGlobalProperties().getPartitioningOrdering());

		SingleInputPlanNode partitioner = (SingleInputPlanNode)sink.getInput().getSource();
		assertTrue(partitioner.getDriverStrategy() == DriverStrategy.UNARY_NO_OP);
		assertEquals("Partitioner input should be range partitioned.",
			PartitioningProperty.RANGE_PARTITIONED, partitioner.getInput().getGlobalProperties().getPartitioning());
		assertEquals("Partitioner input should be range partitioned on 1",
			new Ordering(1, null, Order.ASCENDING), partitioner.getInput().getGlobalProperties().getPartitioningOrdering());
		assertEquals("Partitioner input channel should be forwarding",
			ShipStrategyType.FORWARD, partitioner.getInput().getShipStrategy());

		NAryUnionPlanNode union = (NAryUnionPlanNode)partitioner.getInput().getSource();
		// all union inputs should be range partitioned
		for (Channel c : union.getInputs()) {
			assertEquals("Union input should be range partitioned",
				PartitioningProperty.RANGE_PARTITIONED, c.getGlobalProperties().getPartitioning());
			assertEquals("Union input channel should be forwarded",
				ShipStrategyType.FORWARD, c.getShipStrategy());
			// range partitioning is executed as custom partitioning with prior sampling
			SingleInputPlanNode partitionMap = (SingleInputPlanNode)c.getSource();
			assertEquals(DriverStrategy.MAP, partitionMap.getDriverStrategy());
			assertEquals(ShipStrategyType.PARTITION_CUSTOM, partitionMap.getInput().getShipStrategy());
		}
	}

	/**
	 *
	 * Checks that a plan with consecutive UNIONs followed by broadcast-fwd JOIN is correctly translated.
	 *
	 * The program can be illustrated as follows:
	 *
	 * Src1 -\
	 *        >-> Union12--<
	 * Src2 -/              \
	 *                       >-> Union123 --> bc-fwd-Join -> Output
	 * Src3 ----------------/             /
	 *                                   /
	 * Src4 ----------------------------/
	 *
	 * In the resulting plan, the broadcasting must be
	 * pushed to the inputs of the unions (Src1, Src2, Src3).
	 *
	 */
	@Test
	public void testConsecutiveUnionsWithBroadcast() throws Exception {

		// -----------------------------------------------------------------------------------------
		// Build test program
		// -----------------------------------------------------------------------------------------

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);

		DataSet<Tuple2<Long, Long>> src1 = env.fromElements(new Tuple2<>(0L, 0L));
		DataSet<Tuple2<Long, Long>> src2 = env.fromElements(new Tuple2<>(0L, 0L));
		DataSet<Tuple2<Long, Long>> src3 = env.fromElements(new Tuple2<>(0L, 0L));
		DataSet<Tuple2<Long, Long>> src4 = env.fromElements(new Tuple2<>(0L, 0L));

		DataSet<Tuple2<Long, Long>> union12 = src1.union(src2);
		DataSet<Tuple2<Long, Long>> union123 = union12.union(src3);
		union123.join(src4, JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST)
			.where(0).equalTo(0).name("join")
			.output(new DiscardingOutputFormat<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>>()).name("out");

		// -----------------------------------------------------------------------------------------
		// Verify optimized plan
		// -----------------------------------------------------------------------------------------

		OptimizedPlan optimizedPlan = compileNoStats(env.createProgramPlan());

		OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(optimizedPlan);

		DualInputPlanNode join = resolver.getNode("join");

		// check input of join is broadcast
		assertEquals("First join input should be fully replicated.",
			PartitioningProperty.FULL_REPLICATION, join.getInput1().getGlobalProperties().getPartitioning());

		NAryUnionPlanNode union = (NAryUnionPlanNode)join.getInput1().getSource();
		// check that all union inputs are broadcast
		for (Channel c : union.getInputs()) {
			assertEquals("Union input should be fully replicated",
				PartitioningProperty.FULL_REPLICATION, c.getGlobalProperties().getPartitioning());
			assertEquals("Union input channel should be broadcasting",
				ShipStrategyType.BROADCAST, c.getShipStrategy());
		}
	}

	/**
	 * Tests that a the outgoing connection of a Union node is FORWARD.
	 * See FLINK-9031 for a bug report.
	 *
	 * The issue is quite hard to reproduce as the plan choice seems to depend on the enumeration
	 * order due to lack of plan costs. This test is a smaller variant of the job that was reported
	 * to fail.
	 *
	 *       /-\           /- PreFilter1 -\-/- Union - PostFilter1 - Reducer1 -\
	 * Src -<   >- Union -<                X                                    >- Union - Out
	 *       \-/           \- PreFilter2 -/-\- Union - PostFilter2 - Reducer2 -/
	 */
	@Test
	public void testUnionForwardOutput() throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);

		DataSet<Tuple2<Long, Long>> src1 = env.fromElements(new Tuple2<>(0L, 0L));

		DataSet<Tuple2<Long, Long>> u1 = src1.union(src1)
			.map(new IdentityMapper<>());

		DataSet<Tuple2<Long, Long>> s1 = u1
			.filter(x -> true).name("preFilter1");
		DataSet<Tuple2<Long, Long>> s2 = u1
			.filter(x -> true).name("preFilter2");

		DataSet<Tuple2<Long, Long>> reduced1 = s1
			.union(s2)
			.filter(x -> true).name("postFilter1")
			.groupBy(0)
			.reduceGroup(new IdentityGroupReducer<>()).name("reducer1");
		DataSet<Tuple2<Long, Long>> reduced2 = s1
			.union(s2)
			.filter(x -> true).name("postFilter2")
			.groupBy(1)
			.reduceGroup(new IdentityGroupReducer<>()).name("reducer2");

		reduced1
			.union(reduced2)
			.output(new DiscardingOutputFormat<>());

		// -----------------------------------------------------------------------------------------
		// Verify optimized plan
		// -----------------------------------------------------------------------------------------

		OptimizedPlan optimizedPlan = compileNoStats(env.createProgramPlan());

		OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(optimizedPlan);

		SingleInputPlanNode unionOut1 = resolver.getNode("postFilter1");
		SingleInputPlanNode unionOut2 = resolver.getNode("postFilter2");

		assertEquals(ShipStrategyType.FORWARD, unionOut1.getInput().getShipStrategy());
		assertEquals(ShipStrategyType.FORWARD, unionOut2.getInput().getShipStrategy());
	}

	/**
	 * Test the input and output shipping strategies for union operators with input and output
	 * operators with different parallelisms.
	 *
	 * Src1 - Map(fullP) -\-/- Union - Map(fullP) - Out
	 *                     X
	 * Src2 - Map(halfP) -/-\- Union - Map(halfP) - Out
	 *
	 * The union operator must always have the same parallelism as its successor and connect to it
	 * with a FORWARD strategy.
	 * In this program, the input connections for union should be FORWARD for parallelism-preserving
	 * connections and PARTITION_RANDOM for parallelism-changing connections.
	 *
	 */
	@Test
	public void testUnionInputOutputDifferentDOP() throws Exception {

		int fullDop = DEFAULT_PARALLELISM;
		int halfDop = DEFAULT_PARALLELISM / 2;

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(DEFAULT_PARALLELISM);

		DataSet<Tuple2<Long, Long>> in1 = env.fromElements(new Tuple2<>(0L, 0L))
			.map(new IdentityMapper<>()).setParallelism(fullDop).name("inDopFull");
		DataSet<Tuple2<Long, Long>> in2 = env.fromElements(new Tuple2<>(0L, 0L))
			.map(new IdentityMapper<>()).setParallelism(halfDop).name("inDopHalf");

		DataSet<Tuple2<Long, Long>> union = in1.union(in2);

		DataSet<Tuple2<Long, Long>> dopFullMap = union
			.map(new IdentityMapper<>()).setParallelism(fullDop).name("outDopFull");
		DataSet<Tuple2<Long, Long>> dopHalfMap = union
			.map(new IdentityMapper<>()).setParallelism(halfDop).name("outDopHalf");

		dopFullMap.output(new DiscardingOutputFormat<>());
		dopHalfMap.output(new DiscardingOutputFormat<>());

		// -----------------------------------------------------------------------------------------
		// Verify optimized plan
		// -----------------------------------------------------------------------------------------

		OptimizedPlan optimizedPlan = compileNoStats(env.createProgramPlan());

		OptimizerPlanNodeResolver resolver = getOptimizerPlanNodeResolver(optimizedPlan);

		SingleInputPlanNode inDopFull = resolver.getNode("inDopFull");
		SingleInputPlanNode inDopHalf = resolver.getNode("inDopHalf");
		SingleInputPlanNode outDopFull = resolver.getNode("outDopFull");
		SingleInputPlanNode outDopHalf = resolver.getNode("outDopHalf");
		NAryUnionPlanNode unionDopFull = (NAryUnionPlanNode) outDopFull.getInput().getSource();
		NAryUnionPlanNode unionDopHalf = (NAryUnionPlanNode) outDopHalf.getInput().getSource();

		// check in map nodes
		assertEquals(2, inDopFull.getOutgoingChannels().size());
		assertEquals(2, inDopHalf.getOutgoingChannels().size());
		assertEquals(fullDop, inDopFull.getParallelism());
		assertEquals(halfDop, inDopHalf.getParallelism());

		// check union nodes
		assertEquals(fullDop, unionDopFull.getParallelism());
		assertEquals(halfDop, unionDopHalf.getParallelism());

		// check out map nodes
		assertEquals(fullDop, outDopFull.getParallelism());
		assertEquals(halfDop, outDopHalf.getParallelism());

		// check Union -> outMap ship strategies
		assertEquals(ShipStrategyType.FORWARD, outDopHalf.getInput().getShipStrategy());
		assertEquals(ShipStrategyType.FORWARD, outDopFull.getInput().getShipStrategy());

		// check inMap -> Union ship strategies
		Channel fullFull;
		Channel fullHalf;
		Channel halfFull;
		Channel halfHalf;

		if (inDopFull.getOutgoingChannels().get(0).getTarget() == unionDopFull) {
			fullFull = inDopFull.getOutgoingChannels().get(0);
			fullHalf = inDopFull.getOutgoingChannels().get(1);
		} else {
			fullFull = inDopFull.getOutgoingChannels().get(1);
			fullHalf = inDopFull.getOutgoingChannels().get(0);
		}
		if (inDopHalf.getOutgoingChannels().get(0).getTarget() == unionDopFull) {
			halfFull = inDopHalf.getOutgoingChannels().get(0);
			halfHalf = inDopHalf.getOutgoingChannels().get(1);
		} else {
			halfFull = inDopHalf.getOutgoingChannels().get(1);
			halfHalf = inDopHalf.getOutgoingChannels().get(0);
		}

		assertEquals(ShipStrategyType.FORWARD, fullFull.getShipStrategy());
		assertEquals(ShipStrategyType.FORWARD, halfHalf.getShipStrategy());
		assertEquals(ShipStrategyType.PARTITION_RANDOM, fullHalf.getShipStrategy());
		assertEquals(ShipStrategyType.PARTITION_RANDOM, halfFull.getShipStrategy());
	}

}
