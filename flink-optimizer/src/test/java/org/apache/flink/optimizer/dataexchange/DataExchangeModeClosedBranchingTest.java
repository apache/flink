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
import org.apache.flink.optimizer.testfunctions.DummyCoGroupFunction;
import org.apache.flink.optimizer.testfunctions.DummyFlatJoinFunction;
import org.apache.flink.optimizer.testfunctions.IdentityFlatMapper;
import org.apache.flink.optimizer.testfunctions.SelectOneReducer;
import org.apache.flink.optimizer.testfunctions.Top1GroupReducer;
import org.apache.flink.optimizer.util.CompilerTestBase;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * This test checks the correct assignment of the DataExchangeMode to
 * connections for programs that branch, and re-join those branches.
 *
 * <pre>
 *                                         /-> (sink)
 *                                        /
 *                         /-> (reduce) -+          /-> (flatmap) -> (sink)
 *                        /               \        /
 *     (source) -> (map) -                (join) -+-----\
 *                        \               /              \
 *                         \-> (filter) -+                \
 *                                       \                (co group) -> (sink)
 *                                        \                /
 *                                         \-> (reduce) - /
 * </pre>
 */
@SuppressWarnings("serial")
public class DataExchangeModeClosedBranchingTest extends CompilerTestBase {

	@Test
	public void testPipelinedForced() {
		// PIPELINED_FORCED should result in pipelining all the way
		verifyBranchingJoiningPlan(ExecutionMode.PIPELINED_FORCED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED,
				DataExchangeMode.PIPELINED, DataExchangeMode.PIPELINED);
	}

	@Test
	public void testPipelined() {
		// PIPELINED should result in pipelining all the way
		verifyBranchingJoiningPlan(ExecutionMode.PIPELINED,
				DataExchangeMode.PIPELINED,   // to map
				DataExchangeMode.PIPELINED,   // to combiner connections are pipelined
				DataExchangeMode.BATCH,       // to reduce
				DataExchangeMode.BATCH,       // to filter
				DataExchangeMode.PIPELINED,   // to sink after reduce
				DataExchangeMode.PIPELINED,   // to join (first input)
				DataExchangeMode.BATCH,       // to join (second input)
				DataExchangeMode.PIPELINED,   // combiner connections are pipelined
				DataExchangeMode.BATCH,       // to other reducer
				DataExchangeMode.PIPELINED,   // to flatMap
				DataExchangeMode.PIPELINED,   // to sink after flatMap
				DataExchangeMode.PIPELINED,   // to coGroup (first input)
				DataExchangeMode.PIPELINED,   // to coGroup (second input)
				DataExchangeMode.PIPELINED    // to sink after coGroup
		);
	}

	@Test
	public void testBatch() {
		// BATCH should result in batching the shuffle all the way
		verifyBranchingJoiningPlan(ExecutionMode.BATCH,
				DataExchangeMode.PIPELINED,   // to map
				DataExchangeMode.PIPELINED,   // to combiner connections are pipelined
				DataExchangeMode.BATCH,       // to reduce
				DataExchangeMode.BATCH,       // to filter
				DataExchangeMode.PIPELINED,   // to sink after reduce
				DataExchangeMode.BATCH,       // to join (first input)
				DataExchangeMode.BATCH,       // to join (second input)
				DataExchangeMode.PIPELINED,   // combiner connections are pipelined
				DataExchangeMode.BATCH,       // to other reducer
				DataExchangeMode.PIPELINED,   // to flatMap
				DataExchangeMode.PIPELINED,   // to sink after flatMap
				DataExchangeMode.BATCH,       // to coGroup (first input)
				DataExchangeMode.BATCH,       // to coGroup (second input)
				DataExchangeMode.PIPELINED    // to sink after coGroup
		);
	}

	@Test
	public void testBatchForced() {
		// BATCH_FORCED should result in batching all the way
		verifyBranchingJoiningPlan(ExecutionMode.BATCH_FORCED,
				DataExchangeMode.BATCH,       // to map
				DataExchangeMode.PIPELINED,   // to combiner connections are pipelined
				DataExchangeMode.BATCH,       // to reduce
				DataExchangeMode.BATCH,       // to filter
				DataExchangeMode.BATCH,       // to sink after reduce
				DataExchangeMode.BATCH,       // to join (first input)
				DataExchangeMode.BATCH,       // to join (second input)
				DataExchangeMode.PIPELINED,   // combiner connections are pipelined
				DataExchangeMode.BATCH,       // to other reducer
				DataExchangeMode.BATCH,       // to flatMap
				DataExchangeMode.BATCH,       // to sink after flatMap
				DataExchangeMode.BATCH,       // to coGroup (first input)
				DataExchangeMode.BATCH,       // to coGroup (second input)
				DataExchangeMode.BATCH        // to sink after coGroup
		);
	}

	private void verifyBranchingJoiningPlan(ExecutionMode execMode,
											DataExchangeMode toMap,
											DataExchangeMode toReduceCombiner,
											DataExchangeMode toReduce,
											DataExchangeMode toFilter,
											DataExchangeMode toReduceSink,
											DataExchangeMode toJoin1,
											DataExchangeMode toJoin2,
											DataExchangeMode toOtherReduceCombiner,
											DataExchangeMode toOtherReduce,
											DataExchangeMode toFlatMap,
											DataExchangeMode toFlatMapSink,
											DataExchangeMode toCoGroup1,
											DataExchangeMode toCoGroup2,
											DataExchangeMode toCoGroupSink)
	{
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			env.getConfig().setExecutionMode(execMode);

			// set parallelism to 2 to avoid chaining with source in case when available processors is 1.
			env.setParallelism(2);

			DataSet<Tuple2<Long, Long>> data = env.fromElements(33L, 44L)
					.map(new MapFunction<Long, Tuple2<Long, Long>>() {
						@Override
						public Tuple2<Long, Long> map(Long value) {
							return new Tuple2<Long, Long>(value, value);
						}
					});

			DataSet<Tuple2<Long, Long>> reduced = data.groupBy(0).reduce(new SelectOneReducer<Tuple2<Long, Long>>());
			reduced.output(new DiscardingOutputFormat<Tuple2<Long, Long>>()).name("reduceSink");

			DataSet<Tuple2<Long, Long>> filtered = data.filter(new FilterFunction<Tuple2<Long, Long>>() {
				@Override
				public boolean filter(Tuple2<Long, Long> value) throws Exception {
					return false;
				}
			});

			DataSet<Tuple2<Long, Long>> joined = reduced.join(filtered)
					.where(1).equalTo(1)
					.with(new DummyFlatJoinFunction<Tuple2<Long, Long>>());

			joined.flatMap(new IdentityFlatMapper<Tuple2<Long, Long>>())
					.output(new DiscardingOutputFormat<Tuple2<Long, Long>>()).name("flatMapSink");

			joined.coGroup(filtered.groupBy(1).reduceGroup(new Top1GroupReducer<Tuple2<Long, Long>>()))
					.where(0).equalTo(0)
					.with(new DummyCoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>())
					.output(new DiscardingOutputFormat<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>>()).name("cgSink");


			OptimizedPlan optPlan = compileNoStats(env.createProgramPlan());

			SinkPlanNode reduceSink = findSink(optPlan.getDataSinks(), "reduceSink");
			SinkPlanNode flatMapSink = findSink(optPlan.getDataSinks(), "flatMapSink");
			SinkPlanNode cgSink = findSink(optPlan.getDataSinks(), "cgSink");

			DualInputPlanNode coGroupNode = (DualInputPlanNode) cgSink.getPredecessor();

			DualInputPlanNode joinNode = (DualInputPlanNode) coGroupNode.getInput1().getSource();
			SingleInputPlanNode otherReduceNode = (SingleInputPlanNode) coGroupNode.getInput2().getSource();
			SingleInputPlanNode otherReduceCombinerNode = (SingleInputPlanNode) otherReduceNode.getPredecessor();

			SingleInputPlanNode reduceNode = (SingleInputPlanNode) joinNode.getInput1().getSource();
			SingleInputPlanNode reduceCombinerNode = (SingleInputPlanNode) reduceNode.getPredecessor();
			assertEquals(reduceNode, reduceSink.getPredecessor());

			SingleInputPlanNode filterNode = (SingleInputPlanNode) joinNode.getInput2().getSource();
			assertEquals(filterNode, otherReduceCombinerNode.getPredecessor());

			SingleInputPlanNode mapNode = (SingleInputPlanNode) filterNode.getPredecessor();
			assertEquals(mapNode, reduceCombinerNode.getPredecessor());

			SingleInputPlanNode flatMapNode = (SingleInputPlanNode) flatMapSink.getPredecessor();
			assertEquals(joinNode, flatMapNode.getPredecessor());

			// verify the data exchange modes

			assertEquals(toReduceSink, reduceSink.getInput().getDataExchangeMode());
			assertEquals(toFlatMapSink, flatMapSink.getInput().getDataExchangeMode());
			assertEquals(toCoGroupSink, cgSink.getInput().getDataExchangeMode());

			assertEquals(toCoGroup1, coGroupNode.getInput1().getDataExchangeMode());
			assertEquals(toCoGroup2, coGroupNode.getInput2().getDataExchangeMode());

			assertEquals(toJoin1, joinNode.getInput1().getDataExchangeMode());
			assertEquals(toJoin2, joinNode.getInput2().getDataExchangeMode());

			assertEquals(toOtherReduce, otherReduceNode.getInput().getDataExchangeMode());
			assertEquals(toOtherReduceCombiner, otherReduceCombinerNode.getInput().getDataExchangeMode());

			assertEquals(toFlatMap, flatMapNode.getInput().getDataExchangeMode());

			assertEquals(toFilter, filterNode.getInput().getDataExchangeMode());
			assertEquals(toReduce, reduceNode.getInput().getDataExchangeMode());
			assertEquals(toReduceCombiner, reduceCombinerNode.getInput().getDataExchangeMode());

			assertEquals(toMap, mapNode.getInput().getDataExchangeMode());
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
