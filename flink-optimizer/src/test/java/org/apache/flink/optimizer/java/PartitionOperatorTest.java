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

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.operators.PartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.Channel;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.plan.SourcePlanNode;
import org.apache.flink.optimizer.testfunctions.IdentityGroupReducerCombinable;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.operators.shipping.ShipStrategyType;
import org.junit.Test;

@SuppressWarnings("serial")
public class PartitionOperatorTest extends CompilerTestBase {

	@Test
	public void testPartitionCustomOperatorPreservesFields() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			
			DataSet<Tuple2<Long, Long>> data = env.fromCollection(Collections.singleton(new Tuple2<>(0L, 0L)));
			
			data.partitionCustom(new Partitioner<Long>() {
					public int partition(Long key, int numPartitions) { return key.intValue(); }
				}, 1)
				.groupBy(1)
				.reduceGroup(new IdentityGroupReducerCombinable<Tuple2<Long, Long>>())
				.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			
			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);
			
			SinkPlanNode sink = op.getDataSinks().iterator().next();
			SingleInputPlanNode reducer = (SingleInputPlanNode) sink.getInput().getSource();
			SingleInputPlanNode partitioner = (SingleInputPlanNode) reducer.getInput().getSource();

			assertEquals(ShipStrategyType.FORWARD, reducer.getInput().getShipStrategy());
			assertEquals(ShipStrategyType.PARTITION_CUSTOM, partitioner.getInput().getShipStrategy());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRangePartitionOperatorPreservesFields() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<Tuple2<Long, Long>> data = env.fromCollection(Collections.singleton(new Tuple2<>(0L, 0L)));

			data.partitionByRange(1)
				.groupBy(1)
				.reduceGroup(new IdentityGroupReducerCombinable<Tuple2<Long,Long>>())
				.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);

			SinkPlanNode sink = op.getDataSinks().iterator().next();
			SingleInputPlanNode reducer = (SingleInputPlanNode) sink.getInput().getSource();
			SingleInputPlanNode partitionNode = (SingleInputPlanNode)reducer.getInput().getSource();
			SingleInputPlanNode partitionIDRemover = (SingleInputPlanNode) partitionNode.getInput().getSource();

			assertEquals(ShipStrategyType.FORWARD, reducer.getInput().getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, partitionNode.getInput().getShipStrategy());
			assertEquals(ShipStrategyType.PARTITION_CUSTOM, partitionIDRemover.getInput().getShipStrategy());

			SourcePlanNode sourcePlanNode = op.getDataSources().iterator().next();
			List<Channel> sourceOutgoingChannels = sourcePlanNode.getOutgoingChannels();
			assertEquals(2, sourceOutgoingChannels.size());
			assertEquals(ShipStrategyType.FORWARD, sourceOutgoingChannels.get(0).getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, sourceOutgoingChannels.get(1).getShipStrategy());
			assertEquals(DataExchangeMode.PIPELINED, sourceOutgoingChannels.get(0).getDataExchangeMode());
			assertEquals(DataExchangeMode.BATCH, sourceOutgoingChannels.get(1).getDataExchangeMode());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	@Test
	public void testRangePartitionOperatorPreservesFields2() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<Tuple2<Long, Long>> data = env.fromCollection(Collections.singleton(new Tuple2<>(0L, 0L)));

			PartitionOperator<Tuple2<Long, Long>> rangePartitioned = data.partitionByRange(1);

			rangePartitioned
				.groupBy(1)
				.reduceGroup(new IdentityGroupReducerCombinable<Tuple2<Long,Long>>())
				.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

			data
				.groupBy(0)
				.aggregate(Aggregations.SUM, 1)
				.map(new MapFunction<Tuple2<Long, Long>, Long>() {
					@Override
					public Long map(Tuple2<Long, Long> value) throws Exception {
						return value.f1;
					}
				})
				.output(new DiscardingOutputFormat<Long>());

			rangePartitioned
				.filter(new FilterFunction<Tuple2<Long, Long>>() {
					@Override
					public boolean filter(Tuple2<Long, Long> value) throws Exception {
						return value.f0 % 2 == 0;
					}
				})
				.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());


			Plan p = env.createProgramPlan();
			OptimizedPlan op = compileNoStats(p);

			SinkPlanNode sink = op.getDataSinks().iterator().next();
			SingleInputPlanNode reducer = (SingleInputPlanNode) sink.getInput().getSource();
			SingleInputPlanNode partitionNode = (SingleInputPlanNode)reducer.getInput().getSource();
			SingleInputPlanNode partitionIDRemover = (SingleInputPlanNode) partitionNode.getInput().getSource();

			assertEquals(ShipStrategyType.FORWARD, reducer.getInput().getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, partitionNode.getInput().getShipStrategy());
			assertEquals(ShipStrategyType.PARTITION_CUSTOM, partitionIDRemover.getInput().getShipStrategy());

			SourcePlanNode sourcePlanNode = op.getDataSources().iterator().next();
			List<Channel> sourceOutgoingChannels = sourcePlanNode.getOutgoingChannels();
			assertEquals(3, sourceOutgoingChannels.size());
			assertEquals(ShipStrategyType.FORWARD, sourceOutgoingChannels.get(0).getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, sourceOutgoingChannels.get(1).getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, sourceOutgoingChannels.get(2).getShipStrategy());
			assertEquals(DataExchangeMode.PIPELINED, sourceOutgoingChannels.get(0).getDataExchangeMode());
			assertEquals(DataExchangeMode.PIPELINED, sourceOutgoingChannels.get(1).getDataExchangeMode());
			assertEquals(DataExchangeMode.BATCH, sourceOutgoingChannels.get(2).getDataExchangeMode());

			List<Channel> partitionOutputChannels = partitionNode.getOutgoingChannels();
			assertEquals(2, partitionOutputChannels.size());
			assertEquals(ShipStrategyType.FORWARD, partitionOutputChannels.get(0).getShipStrategy());
			assertEquals(ShipStrategyType.FORWARD, partitionOutputChannels.get(1).getShipStrategy());
			assertEquals(DataExchangeMode.PIPELINED, partitionOutputChannels.get(0).getDataExchangeMode());
			assertEquals(DataExchangeMode.PIPELINED, partitionOutputChannels.get(1).getDataExchangeMode());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}
}
