/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunctionV2;
import org.apache.flink.streaming.api.functions.source.SourceRecord;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;

import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;

/**
 * Tests for generating acyclic JobGraph.
 */
public class GeneratingAcyclicJobGraphTest {

	@Test
	public void testBasicGraph() throws Exception {
		// case:
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			DataStreamSink<String> sink1 = map1.addSink(new NoOpSinkFunction()).name("sink1");

			testBase(env, new Integer[][]{
					new Integer[]{source1.getId(), map1.getId(), sink1.getId()}});
		}

		// case:
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
			DataStream<String> map1 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map1");
			DataStreamSink<String> sink1 = map1.addSink(new NoOpSinkFunction()).name("sink1");
			DataStreamSink<String> sink2 = map1.addSink(new NoOpSinkFunction()).name("sink2");

			testBase(env, new Integer[][]{
					new Integer[]{source1.getId(), source2.getId(), map1.getId(), sink1.getId(), sink2.getId()}
			});
		}

		// case: triangle
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> map1 = filter1.connect(source1).map(new NoOpCoMapFunction()).name("map1");
			DataStreamSink<String> sink1 = map1.addSink(new NoOpSinkFunction()).name("sink1");

			testBase(env, new Integer[][]{
					new Integer[]{source1.getId(), filter1.getId(), map1.getId(), sink1.getId()}
			});
		}

		// case: double-crossing
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
			DataStream<String> map1 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map1");
			DataStream<String> map2 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map2");
			DataStreamSink<String> sink1 = map1.addSink(new NoOpSinkFunction()).name("sink1");
			DataStreamSink<String> sink2 = map2.addSink(new NoOpSinkFunction()).name("sink2");

			testBase(env, new Integer[][]{
					new Integer[]{source1.getId(), source2.getId(), map1.getId(), map2.getId(), sink1.getId(), sink2.getId()}
			});
		}
	}

	@Test
	public void testTriangleTopology() throws Exception {
		// case group (same result):
		{
			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> filter1 = source1.rescale().filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> map1 = filter1.connect(source1).map(new NoOpCoMapFunction()).name("map1");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId()},
						new Integer[]{filter1.getId(), map1.getId()}
				});
			}

			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> filter1 = source1.rescale().filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> map1 = filter1.connect(source1.rescale()).map(new NoOpCoMapFunction()).name("map1");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId()},
						new Integer[]{filter1.getId(), map1.getId()}
				});
			}
		}

		// case group (same result):
		{
			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> map1 = filter1.rescale().connect(source1).map(new NoOpCoMapFunction()).name("map1");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId(), filter1.getId()},
						new Integer[]{map1.getId()}
				});
			}

			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> map1 = filter1.connect(source1.rescale()).map(new NoOpCoMapFunction()).name("map1");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId(), filter1.getId()},
						new Integer[]{map1.getId()}
				});
			}

			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> map1 = filter1.rescale().connect(source1.rescale()).map(new NoOpCoMapFunction()).name("map1");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId(), filter1.getId()},
						new Integer[]{map1.getId()}
				});
			}
		}

		// case group (same result):
		{
			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> filter1 = source1.rescale().filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> map1 = filter1.rescale().connect(source1).map(new NoOpCoMapFunction()).name("map1");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId()},
						new Integer[]{filter1.getId()},
						new Integer[]{map1.getId()}
				});
			}

			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> filter1 = source1.rescale().filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> map1 = filter1.rescale().connect(source1.rescale()).map(new NoOpCoMapFunction()).name("map1");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId()},
						new Integer[]{filter1.getId()},
						new Integer[]{map1.getId()}
				});
			}
		}
	}

	@Test
	public void testDoubleCrossingTopology() throws Exception {
		// case group (same result):
		{
			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.rescale().connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId()},
						new Integer[]{source2.getId(), map1.getId(), map2.getId()}
				});
			}

			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.rescale().connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.rescale().connect(source2).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId()},
						new Integer[]{source2.getId(), map1.getId(), map2.getId()}
				});
			}
		}

		// case group (same result):
		{
			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId(), map1.getId(), map2.getId()},
						new Integer[]{source2.getId()}
				});
			}

			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId(), map1.getId(), map2.getId()},
						new Integer[]{source2.getId()}
				});
			}
		}

		// case group (same result):
		{
			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.rescale().connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId(), source2.getId(), map2.getId()},
						new Integer[]{map1.getId()}
				});
			}
		}

		// case group (same result):
		{
			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.rescale().connect(source2).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId(), source2.getId(), map1.getId()},
						new Integer[]{map2.getId()}
				});
			}

			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId(), source2.getId(), map1.getId()},
						new Integer[]{map2.getId()}
				});
			}

			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.rescale().connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId(), source2.getId(), map1.getId()},
						new Integer[]{map2.getId()}
				});
			}
		}

		// case group (same result):
		{
			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.rescale().connect(source2).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId()},
						new Integer[]{source2.getId(), map1.getId()},
						new Integer[]{map2.getId()}
				});
			}
		}

		// case group (same result):
		{
			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.rescale().connect(source2).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId(), map1.getId()},
						new Integer[]{source2.getId()},
						new Integer[]{map2.getId()}
				});
			}

			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.rescale().connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId(), map1.getId()},
						new Integer[]{source2.getId()},
						new Integer[]{map2.getId()}
				});
			}
		}

		// case group (same result):
		{
			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.rescale().connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.rescale().connect(source2).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId()},
						new Integer[]{source2.getId(), map2.getId()},
						new Integer[]{map1.getId()}
				});
			}
		}

		// case group (same result):
		{
			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.rescale().connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId(), map2.getId()},
						new Integer[]{source2.getId()},
						new Integer[]{map1.getId()}
				});
			}
		}

		// case group (same result):
		{
			// case:
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.rescale().connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map1");
				DataStream<String> map2 = source1.rescale().connect(source2.rescale()).map(new NoOpCoMapFunction()).name("map2");

				testBase(env, new Integer[][]{
						new Integer[]{source1.getId()},
						new Integer[]{source2.getId()},
						new Integer[]{map1.getId()},
						new Integer[]{map2.getId()}
				});
			}
		}
	}

	private static StreamExecutionEnvironment createEnv() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend((StateBackend) new FsStateBackend(GeneratingAcyclicJobGraphTest.class.getResource("/").toURI()));
		env.setMultiHeadChainMode(true);

		return env;
	}

	private static void testBase(StreamExecutionEnvironment env, Integer[][] expect) {
		StreamGraph streamGraph = env.getStreamGraph();
		for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
			pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
		}

		List<Integer> sortedSourceIDs = streamGraph.getSourceIDs().stream()
				.sorted(
						Comparator.comparing((Integer id) -> {
							StreamOperator<?> operator = streamGraph.getStreamNode(id).getOperator();
							return operator == null || operator instanceof StreamSource ? 0 : 1;
						}).thenComparingInt(id -> id))
				.collect(Collectors.toList());

		List<StreamingJobGraphGenerator.ChainingStreamNode> sortedChainingNodes = StreamingJobGraphGenerator.sortTopologicalNodes(streamGraph, sortedSourceIDs);

		Map<Integer, StreamingJobGraphGenerator.ChainingStreamNode> chainingNodeMap = sortedChainingNodes.stream()
				.collect(Collectors.toMap(StreamingJobGraphGenerator.ChainingStreamNode::getNodeId, (o) -> o));

		StreamingJobGraphGenerator.splitUpInitialChains(chainingNodeMap, sortedChainingNodes, streamGraph);
		StreamingJobGraphGenerator.breakOffChainForAcyclicJobGraph(chainingNodeMap, sortedChainingNodes, streamGraph);

		Map<Integer, Set<Integer>> chainMap = new HashMap<>();
		for (StreamingJobGraphGenerator.ChainingStreamNode chainingNode : chainingNodeMap.values()) {
			Integer nodeId = chainingNode.getNodeId();
			Integer coarsenedId = chainingNode.getCoarsenedId();

			Set<Integer> chainedNodes = chainMap.computeIfAbsent(coarsenedId, HashSet::new);

			StreamNode node = streamGraph.getStreamNode(nodeId);
			for (StreamEdge edge : node.getInEdges()) {
				Integer sourceId = edge.getSourceId();
				if (chainingNode.isChainTo(sourceId)) {
					chainedNodes.add(sourceId);
				}
			}

			chainedNodes.add(nodeId);
		}

		Integer[][] result = new Integer[chainMap.size()][];
		int index = 0;
		for (Set<Integer> chain : chainMap.values()) {
			Integer[] ids = chain.toArray(new Integer[0]);
			Arrays.sort(ids);

			result[index++] = ids;
		}
		Arrays.sort(result, Comparator.comparingInt(o -> o[0]));

		assertArrayEquals(expect, result);
	}

	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private static class NoOpSourceFunctionV2 implements ParallelSourceFunctionV2<String> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean isFinished() {
			return false;
		}

		@Override
		public SourceRecord<String> next() throws Exception {
			return null;
		}

		@Override
		public void cancel() {

		}
	}

	private static class NoOpSinkFunction implements SinkFunction<String> {

		private static final long serialVersionUID = 1L;

	}

	private static class NoOpMapFunction implements MapFunction<String, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public String map(String value) throws Exception {
			return value;
		}
	}

	private static class NoOpCoMapFunction implements CoMapFunction<String, String, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public String map1(String value) {
			return value;
		}

		@Override
		public String map2(String value) {
			return value;
		}
	}

	private static class NoOpFilterFunction implements FilterFunction<String> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(String value) throws Exception {
			return true;
		}
	}
}
