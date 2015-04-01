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

package org.apache.flink.optimizer.dataexchange;

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.plan.DualInputPlanNode;
import org.apache.flink.optimizer.plan.OptimizedPlan;
import org.apache.flink.optimizer.plan.SingleInputPlanNode;
import org.apache.flink.optimizer.plan.SinkPlanNode;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This test checks the correct assignment of the DataExchangeMode to
 * connections for programs that branch, but do not re-join the branches.
 *
 * <pre>
 *                      /---> (filter) -> (sink)
 *                     /
 *                    /
 * (source) -> (map) -----------------\
 *                    \               (join) -> (sink)
 *                     \   (source) --/
 *                      \
 *                       \
 *                        \-> (sink)
 * </pre>
 */
@SuppressWarnings({"serial", "unchecked"})
public class DataExchangeModeOpenBranchingTest extends CompilerTestBase {

	@Test
	public void testPipelinedForced() {
		// PIPELINED_FORCED should result in pipelining all the way
		verifyBranchigPlan(ExecutionMode.PIPELINED_FORCED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED);
	}

	@Test
	public void testPipelined() {
		// PIPELINED should result in pipelining all the way
		verifyBranchigPlan(ExecutionMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED);
	}

	@Test
	public void testBatch() {
		// BATCH should result in batching the shuffle all the way
		verifyBranchigPlan(ExecutionMode.BATCH,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.BATCH,
				DataExchangeMode.BATCH, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED);
	}

	@Test
	public void testBatchForced() {
		// BATCH_FORCED should result in batching all the way
		verifyBranchigPlan(ExecutionMode.BATCH_FORCED,
				DataExchangeMode.BATCH, DataExchangeMode.BATCH,
				DataExchangeMode.BATCH, DataExchangeMode.BATCH,
				DataExchangeMode.BATCH, DataExchangeMode.BATCH,
				DataExchangeMode.BATCH);
	}

	private void verifyBranchigPlan(ExecutionMode execMode,
									DataExchangeMode toMap,
									DataExchangeMode toFilter,
									DataExchangeMode toFilterSink,
									DataExchangeMode toJoin1,
									DataExchangeMode toJoin2,
									DataExchangeMode toJoinSink,
									DataExchangeMode toDirectSink)
	{
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.getConfig().setExecutionMode(execMode);

			DataSet<Tuple2<Long, Long>> data = env.generateSequence(1, 100000)
					.map(new MapFunction<Long, Tuple2<Long, Long>>() {
						@Override
						public Tuple2<Long, Long> map(Long value) {
							return new Tuple2<Long, Long>(value, value);
						}
					});

			// output 1
			data
					.filter(new FilterFunction<Tuple2<Long, Long>>() {
						@Override
						public boolean filter(Tuple2<Long, Long> value) {
							return false;
						}
					})
					.output(new DiscardingOutputFormat<Tuple2<Long, Long>>()).name("sink1");

			// output 2 does a join before a join
			data
					.join(env.fromElements(new Tuple2<Long, Long>(1L, 2L)))
					.where(1)
					.equalTo(0)
					.output(new DiscardingOutputFormat<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>>()).name("sink2");

			// output 3 is direct
			data
					.output(new DiscardingOutputFormat<Tuple2<Long, Long>>()).name("sink3");

			OptimizedPlan optPlan = compileNoStats(env.createProgramPlan());

			SinkPlanNode filterSink = findSink(optPlan.getDataSinks(), "sink1");
			SinkPlanNode joinSink = findSink(optPlan.getDataSinks(), "sink2");
			SinkPlanNode directSink = findSink(optPlan.getDataSinks(), "sink3");

			SingleInputPlanNode filterNode = (SingleInputPlanNode) filterSink.getPredecessor();
			SingleInputPlanNode mapNode = (SingleInputPlanNode) filterNode.getPredecessor();

			DualInputPlanNode joinNode = (DualInputPlanNode) joinSink.getPredecessor();
			assertEquals(mapNode, joinNode.getInput1().getSource());

			assertEquals(mapNode, directSink.getPredecessor());

			assertEquals(toFilterSink, filterSink.getInput().getDataExchangeMode());
			assertEquals(toJoinSink, joinSink.getInput().getDataExchangeMode());
			assertEquals(toDirectSink, directSink.getInput().getDataExchangeMode());

			assertEquals(toMap, mapNode.getInput().getDataExchangeMode());
			assertEquals(toFilter, filterNode.getInput().getDataExchangeMode());

			assertEquals(toJoin1, joinNode.getInput1().getDataExchangeMode());
			assertEquals(toJoin2, joinNode.getInput2().getDataExchangeMode());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private SinkPlanNode findSink(Collection<SinkPlanNode> collection, String name) {
		for (SinkPlanNode node : collection) {
			String nodeName = node.getOptimizerNode().getOperator().getName();
			if (nodeName != null && nodeName.equals(name)) {
				return node;
			}
		}

		throw new IllegalArgumentException("No node with that name was found.");
	}
}
