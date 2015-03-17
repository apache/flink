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

import org.apache.flink.api.common.Plan;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.DiscardingOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.optimizer.dag.DataSinkNode;
import org.apache.flink.optimizer.dag.OptimizerNode;
import org.apache.flink.optimizer.dag.SingleInputNode;
import org.apache.flink.optimizer.dag.SinkJoiner;
import org.apache.flink.optimizer.dag.TwoInputNode;
import org.apache.flink.optimizer.testfunctions.DummyCoGroupFunction;
import org.apache.flink.optimizer.testfunctions.DummyFlatJoinFunction;
import org.apache.flink.optimizer.testfunctions.IdentityFlatMapper;
import org.apache.flink.optimizer.testfunctions.IdentityKeyExtractor;
import org.apache.flink.optimizer.testfunctions.SelectOneReducer;
import org.apache.flink.optimizer.testfunctions.Top1GroupReducer;
import org.apache.flink.optimizer.traversals.BranchesVisitor;
import org.apache.flink.optimizer.traversals.GraphCreatingVisitor;
import org.apache.flink.optimizer.traversals.IdAndEstimatesVisitor;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

/**
 * This test checks whether connections are correctly marked as pipelined breaking.
 */
@SuppressWarnings("serial")
public class PipelineBreakingTest {

	/**
	 * Tests that no pipeline breakers are inserted into a simple forward
	 * pipeline.
	 *
	 * <pre>
	 *     (source) -> (map) -> (filter) -> (groupBy / reduce)
	 * </pre>
	 */
	@Test
	public void testSimpleForwardPlan() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<String> dataSet = env.readTextFile("/never/accessed");
			dataSet
				.map(new MapFunction<String, Integer>() {
					@Override
					public Integer map(String value) {
						return 0;
					}
				})
				.filter(new FilterFunction<Integer>() {
					@Override
					public boolean filter(Integer value) {
						return false;
					}
				})
				.groupBy(new IdentityKeyExtractor<Integer>())
				.reduceGroup(new Top1GroupReducer<Integer>())
				.output(new DiscardingOutputFormat<Integer>());

			DataSinkNode sinkNode = convertPlan(env.createProgramPlan()).get(0);

			SingleInputNode reduceNode = (SingleInputNode) sinkNode.getPredecessorNode();
			SingleInputNode keyExtractorNode = (SingleInputNode) reduceNode.getPredecessorNode();

			SingleInputNode filterNode = (SingleInputNode) keyExtractorNode.getPredecessorNode();
			SingleInputNode mapNode = (SingleInputNode) filterNode.getPredecessorNode();

			assertFalse(sinkNode.getInputConnection().isBreakingPipeline());
			assertFalse(reduceNode.getIncomingConnection().isBreakingPipeline());
			assertFalse(keyExtractorNode.getIncomingConnection().isBreakingPipeline());
			assertFalse(filterNode.getIncomingConnection().isBreakingPipeline());
			assertFalse(mapNode.getIncomingConnection().isBreakingPipeline());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests that branching plans, where the branches are not re-joined,
	 * do not place pipeline breakers.
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
	@Test
	public void testBranchingPlanNotReJoined() {
		try {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<Integer> data = env.readTextFile("/never/accessed")
				.map(new MapFunction<String, Integer>() {
					@Override
					public Integer map(String value) {
						return 0;
					}
				});

			// output 1
			data
				.filter(new FilterFunction<Integer>() {
					@Override
					public boolean filter(Integer value) {
						return false;
					}
				})
				.output(new DiscardingOutputFormat<Integer>());

			// output 2 does a join before a join
			data
				.join(env.fromElements(1, 2, 3, 4))
					.where(new IdentityKeyExtractor<Integer>())
					.equalTo(new IdentityKeyExtractor<Integer>())
				.output(new DiscardingOutputFormat<Tuple2<Integer, Integer>>());

			// output 3 is direct
			data
				.output(new DiscardingOutputFormat<Integer>());

			List<DataSinkNode> sinks = convertPlan(env.createProgramPlan());

			// gather the optimizer DAG nodes

			DataSinkNode sinkAfterFilter = sinks.get(0);
			DataSinkNode sinkAfterJoin = sinks.get(1);
			DataSinkNode sinkDirect = sinks.get(2);

			SingleInputNode filterNode = (SingleInputNode) sinkAfterFilter.getPredecessorNode();
			SingleInputNode mapNode = (SingleInputNode) filterNode.getPredecessorNode();

			TwoInputNode joinNode = (TwoInputNode) sinkAfterJoin.getPredecessorNode();
			SingleInputNode joinInput = (SingleInputNode) joinNode.getSecondPredecessorNode();

			// verify the non-pipeline breaking status

			assertFalse(sinkAfterFilter.getInputConnection().isBreakingPipeline());
			assertFalse(sinkAfterJoin.getInputConnection().isBreakingPipeline());
			assertFalse(sinkDirect.getInputConnection().isBreakingPipeline());

			assertFalse(filterNode.getIncomingConnection().isBreakingPipeline());
			assertFalse(mapNode.getIncomingConnection().isBreakingPipeline());

			assertFalse(joinNode.getFirstIncomingConnection().isBreakingPipeline());
			assertFalse(joinNode.getSecondIncomingConnection().isBreakingPipeline());
			assertFalse(joinInput.getIncomingConnection().isBreakingPipeline());

			// some other sanity checks on the plan construction (cannot hurt)

			assertEquals(mapNode, ((SingleInputNode) joinNode.getFirstPredecessorNode()).getPredecessorNode());
			assertEquals(mapNode, sinkDirect.getPredecessorNode());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	/**
	 * Tests that branches that are re-joined have place pipeline breakers.
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
	@Test
	public void testReJoinedBranches() {
		try {
			// build a test program

			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

			DataSet<Tuple2<Long, Long>> data = env.fromElements(33L, 44L)
					.map(new MapFunction<Long, Tuple2<Long, Long>>() {
						@Override
						public Tuple2<Long, Long> map(Long value) {
							return new Tuple2<Long, Long>(value, value);
						}
					});

			DataSet<Tuple2<Long, Long>> reduced = data.groupBy(0).reduce(new SelectOneReducer<Tuple2<Long, Long>>());
			reduced.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());
			
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
					.output(new DiscardingOutputFormat<Tuple2<Long, Long>>());

			joined.coGroup(filtered.groupBy(1).reduceGroup(new Top1GroupReducer<Tuple2<Long, Long>>()))
					.where(0).equalTo(0)
					.with(new DummyCoGroupFunction<Tuple2<Long, Long>, Tuple2<Long, Long>>())
					.output(new DiscardingOutputFormat<Tuple2<Tuple2<Long, Long>, Tuple2<Long, Long>>>());

			List<DataSinkNode> sinks = convertPlan(env.createProgramPlan());

			// gather the optimizer DAG nodes

			DataSinkNode sinkAfterReduce = sinks.get(0);
			DataSinkNode sinkAfterFlatMap = sinks.get(1);
			DataSinkNode sinkAfterCoGroup = sinks.get(2);

			SingleInputNode reduceNode = (SingleInputNode) sinkAfterReduce.getPredecessorNode();
			SingleInputNode mapNode = (SingleInputNode) reduceNode.getPredecessorNode();

			SingleInputNode flatMapNode = (SingleInputNode) sinkAfterFlatMap.getPredecessorNode();
			TwoInputNode joinNode = (TwoInputNode) flatMapNode.getPredecessorNode();
			SingleInputNode filterNode = (SingleInputNode) joinNode.getSecondPredecessorNode();

			TwoInputNode coGroupNode = (TwoInputNode) sinkAfterCoGroup.getPredecessorNode();
			SingleInputNode otherReduceNode = (SingleInputNode) coGroupNode.getSecondPredecessorNode();

			// test sanity checks (that we constructed the DAG correctly)

			assertEquals(reduceNode, joinNode.getFirstPredecessorNode());
			assertEquals(mapNode, filterNode.getPredecessorNode());
			assertEquals(joinNode, coGroupNode.getFirstPredecessorNode());
			assertEquals(filterNode, otherReduceNode.getPredecessorNode());

			// verify the pipeline breaking status

			assertFalse(sinkAfterReduce.getInputConnection().isBreakingPipeline());
			assertFalse(sinkAfterFlatMap.getInputConnection().isBreakingPipeline());
			assertFalse(sinkAfterCoGroup.getInputConnection().isBreakingPipeline());

			assertFalse(mapNode.getIncomingConnection().isBreakingPipeline());
			assertFalse(flatMapNode.getIncomingConnection().isBreakingPipeline());
			assertFalse(joinNode.getFirstIncomingConnection().isBreakingPipeline());
			assertFalse(coGroupNode.getFirstIncomingConnection().isBreakingPipeline());
			assertFalse(coGroupNode.getSecondIncomingConnection().isBreakingPipeline());

			// these should be pipeline breakers
			assertTrue(reduceNode.getIncomingConnection().isBreakingPipeline());
			assertTrue(filterNode.getIncomingConnection().isBreakingPipeline());
			assertTrue(otherReduceNode.getIncomingConnection().isBreakingPipeline());
			assertTrue(joinNode.getSecondIncomingConnection().isBreakingPipeline());
		}
		catch (Exception e) {
			e.printStackTrace();
			fail(e.getMessage());
		}
	}

	private static List<DataSinkNode> convertPlan(Plan p) {
		GraphCreatingVisitor dagCreator =
				new GraphCreatingVisitor(17, p.getExecutionConfig().getExecutionMode());

		// create the DAG
		p.accept(dagCreator);
		List<DataSinkNode> sinks = dagCreator.getSinks();

		// build a single root and run the branch tracking logic
		OptimizerNode rootNode;
		if (sinks.size() == 1) {
			rootNode = sinks.get(0);
		}
		else {
			Iterator<DataSinkNode> iter = sinks.iterator();
			rootNode = iter.next();

			while (iter.hasNext()) {
				rootNode = new SinkJoiner(rootNode, iter.next());
			}
		}
		rootNode.accept(new IdAndEstimatesVisitor(null));
		rootNode.accept(new BranchesVisitor());

		return sinks;
	}
}
