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

import org.apache.flink.api.common.ExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.io.SerializedInputFormat;
import org.apache.flink.api.common.io.SerializedOutputFormat;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.FormatUtil.FormatType;
import org.apache.flink.runtime.jobgraph.FormatUtil.MultiFormatStub;
import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.MultiInputOutputFormatVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.ScheduleMode;
import org.apache.flink.runtime.jobgraph.tasks.AbstractInvokable;
import org.apache.flink.runtime.jobgraph.tasks.JobCheckpointingSettings;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.operators.util.TaskConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunctionV2;
import org.apache.flink.streaming.api.functions.source.SourceRecord;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.ChainingStreamNode;
import org.apache.flink.streaming.api.operators.AbstractOneInputSubstituteStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTaskV2;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfig;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfigCache;
import org.apache.flink.streaming.runtime.tasks.StreamTaskConfigSnapshot;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.types.Pair;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.SequenceGenerator;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.createJobGraph;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.setDepthFirstNumber;
import static org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator.sortTopologicalNodes;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StreamingJobGraphGenerator}.
 */
@SuppressWarnings("serial")
public class StreamingJobGraphGeneratorTest extends TestLogger {

	/**
	 * Tests the following topology.
	 *
	 * <p><pre>
	 *     Source1 -> Map0 -> Map1 ->            -+
	 *                |                           |
	 *                |_   -> Map2 -+             | -> Process1 -> Sink1
	 *                              | -> Filter1 -+
	 *     Source2 -> Map3         -+             | -> Map5 -> Sink2
	 *                 |_  ->                    -+
	 *                 |_  -> Map4 ->            -> Filter2 -> Sink3
	 * </pre>
	 */
	@Test
	public void testTopologicalAndDepthFirstOrder() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));
		env.setMultiHeadChainMode(true);

		ClassLoader cl = getClass().getClassLoader();

		DataStream<Integer> source1 = env.addSource(new NoOpSourceFunction()).name("source1");
		DataStream<Integer> map0 = source1.map(new NoOpMapFunction()).name("map0");
		DataStream<Integer> map1 = map0.map(new NoOpMapFunction()).name("map1");
		DataStream<Integer> map2 = map0.map(new NoOpMapFunction()).name("map2");

		DataStream<Integer> source2 = env.addSource(new NoOpSourceFunction()).name("source2");
		DataStream<Integer> map3 = source2.map(new NoOpMapFunction()).name("map3");
		DataStream<Integer> filter1 = map2.connect(map3).process(new NoOpCoProcessFuntion()).name("filter1");
		DataStream<Integer> map4 = map3.map(new NoOpMapFunction()).name("map4");

		DataStream<Integer> process1 = filter1.connect(map1)
				.process(new NoOpCoProcessFuntion()).name("process1");
		DataStreamSink<Integer> sink1 = process1.addSink(new NoOpSinkFunction()).name("sink1");

		DataStream<Integer> map5 = filter1.connect(map3)
				.map(new NoOpCoMapFunction()).name("map5");
		DataStreamSink<Integer> sink2 = map5.addSink(new NoOpSinkFunction()).name("sink2");

		DataStream<Integer> filter2 = map4.filter(new NoOpFilterFunction()).name("filter2");
		DataStreamSink<Integer> sink3 = filter2.addSink(new NoOpSinkFunction()).name("sink3");

		StreamGraph streamGraph = env.getStreamGraph();

		// sort nodes in a topological order
		List<ChainingStreamNode> topologicalOrderChainingNodes = sortTopologicalNodes(
				streamGraph, Lists.newArrayList(source1.getId(), source2.getId()));

		// sort nodes in the depth-first order
		final Map<Integer, ChainingStreamNode> chainingNodeMap = topologicalOrderChainingNodes.stream()
				.collect(Collectors.toMap(ChainingStreamNode::getNodeId, (o) -> o));
		final SequenceGenerator depthFirstSequenceGenerator = new SequenceGenerator();
		final Set<Integer> visitedNodeSet = new HashSet<>();
		for (Integer sourceNodeId : new Integer[] {source1.getId(), source2.getId()}) {
			setDepthFirstNumber(sourceNodeId, chainingNodeMap, streamGraph, depthFirstSequenceGenerator, visitedNodeSet);
		}

		// verify topological order
		{
			List<Integer> result = topologicalOrderChainingNodes.stream().mapToInt(o -> o.getNodeId())
					.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

			assertArrayEquals(
					Lists.newArrayList(
							source1.getId(), source2.getId(), map0.getId(), map3.getId(),
							map1.getId(), map2.getId(), map4.getId(),
							filter1.getId(), filter2.getId(),
							process1.getId(), map5.getId(), sink3.getId(),
							sink1.getId(), sink2.getId()
					).toArray(new Integer[0]), result.toArray(new Integer[0]));
		}

		// verify depth-first order
		{
			List<Integer> result = chainingNodeMap.keySet().stream()
					.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);

			result.sort(Comparator.comparingInt((Integer id) -> chainingNodeMap.get(id).getDepthFirstNumber()));
			assertArrayEquals(
					Lists.newArrayList(
							sink1.getId(), process1.getId(), map1.getId(),
							sink2.getId(), map5.getId(), filter1.getId(), map2.getId(),
							map0.getId(), source1.getId(),
							sink3.getId(), filter2.getId(), map4.getId(), map3.getId(), source2.getId()
					).toArray(new Integer[0]), result.toArray(new Integer[0]));
		}
	}

	/**
	 * Tests basic chaining logic using the following topology.
	 *
	 * <pre>
	 *     fromElements -> CHAIN(Map1 -> Filter1) -+
	 *                          ( |             )  | -> CHAIN(Map2 -> Print1)
	 *                          ( |_  -> Filter2) -+
	 *                                      |_ -> CHAIN(Print2)
	 * </pre>
	 */
	@Test
	public void testBasicChaining() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(12345);
		env.setBufferTimeout(54321L);
		env.enableCheckpointing(10, CheckpointingMode.AT_LEAST_ONCE);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.registerCachedFile("file:///testCachedFile", "test");
		env.setStateBackend((StateBackend) new FsStateBackend(this.getClass().getResource("/").toURI()));

		ClassLoader cl = getClass().getClassLoader();

		DataStream<Integer> sourceMap = env.fromElements(1, 2, 3).name("source1")
			.map((value) -> value).name("map1").setParallelism(66).setConfigItem("test_key1", "test_value1");

		DataStream<Integer> filter1 = sourceMap.filter((value) -> false).name("filter1").setParallelism(66);
		DataStream<Integer> filter2 = sourceMap.filter((value) -> false).name("filter2").setParallelism(66);

		filter1.connect(filter2)
			.map(new NoOpCoMapFunction()).name("map2").setParallelism(66)
			.print().name("print1").setParallelism(66);
		filter2.print().name("print2").setParallelism(77);

		StreamGraph streamGraph = env.getStreamGraph();
		Map<String, StreamNode> streamNodeMap = new HashMap<>();
		for (StreamNode node : streamGraph.getStreamNodes()) {
			streamNodeMap.put(node.getOperatorName(), node);
		}
		streamNodeMap.get("Source: source1").setInputFormat(new SerializedInputFormat());
		streamNodeMap.get("filter1").setOutputFormat(new SerializedOutputFormat());
		streamNodeMap.get("filter2").setOutputFormat(new SerializedOutputFormat());

		JobGraph jobGraph = createJobGraph(streamGraph);
		assertEquals(12345, jobGraph.getSerializedExecutionConfig().deserializeValue(getClass().getClassLoader()).getParallelism());
		assertEquals(ScheduleMode.EAGER,
			ScheduleMode.valueOf(jobGraph.getSchedulingConfiguration().getString(ScheduleMode.class.getName(), null)));

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(4, verticesSorted.size());

		// Source0 vertex
		{
			JobVertex sourceVertex = verticesSorted.get(0);

			assertEquals(ResultPartitionType.PIPELINED, sourceVertex.getProducedDataSets().get(0).getResultType());
			assertEquals(MultiInputOutputFormatVertex.class, sourceVertex.getClass());

			MultiFormatStub formatStub = new MultiFormatStub(new TaskConfig(sourceVertex.getConfiguration()), cl);
			Iterator<Pair<OperatorID, InputFormat>> inputFormatIterator = formatStub.getFormat(FormatType.INPUT);
			assertEquals(SerializedInputFormat.class, inputFormatIterator.next().getValue().getClass());
			assertFalse(inputFormatIterator.hasNext());
			assertFalse(formatStub.getFormat(FormatType.OUTPUT).hasNext());

			StreamTaskConfigSnapshot sourceTaskConfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(sourceVertex.getConfiguration()), cl);
			verifyVertex(
				sourceVertex,
				TimeCharacteristic.EventTime, true, CheckpointingMode.AT_LEAST_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
				Sets.newHashSet("Source: source1"),
				Sets.newHashSet("Source: source1"),
				SourceStreamTask.class,
				streamGraph);

			StreamConfig sourceConfig = sourceTaskConfig.getChainedHeadNodeConfigs().get(0);
			assertEquals(0, sourceConfig.getCustomConfiguration(cl).keySet().size());
			verifyChainedNode("Source: source1", true, true, 0,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
				54321L,
				sourceConfig, cl, streamGraph);
		}

		// CHAIN(Map1 -> Filter[1,2]) vertex
		{
			JobVertex map1FilterVertex = verticesSorted.get(1);

			assertEquals(ResultPartitionType.PIPELINED, map1FilterVertex.getInputs().get(0).getSource().getResultType());
			assertEquals(MultiInputOutputFormatVertex.class, map1FilterVertex.getClass());

			MultiFormatStub formatStub = new MultiFormatStub(new TaskConfig(map1FilterVertex.getConfiguration()), cl);
			assertFalse(formatStub.getFormat(FormatType.INPUT).hasNext());
			Iterator<Pair<OperatorID, OutputFormat>> outputFormatIterator = formatStub.getFormat(FormatType.OUTPUT);
			assertEquals(SerializedOutputFormat.class, outputFormatIterator.next().getValue().getClass());
			assertEquals(SerializedOutputFormat.class, outputFormatIterator.next().getValue().getClass());
			assertFalse(outputFormatIterator.hasNext());

			StreamTaskConfigSnapshot mapFilterTaskConfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(map1FilterVertex.getConfiguration()), cl);
			verifyVertex(
				map1FilterVertex,
				TimeCharacteristic.EventTime, true, CheckpointingMode.AT_LEAST_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
				Lists.newArrayList(Tuple2.of("filter1", "map2"), Tuple2.of("filter2", "map2"), Tuple2.of("filter2", "Sink: print2")),
				Sets.newHashSet("map1", "filter1", "filter2"),
				Sets.newHashSet("map1"),
				OneInputStreamTask.class,
				streamGraph);

			StreamConfig mapConfig = mapFilterTaskConfig.getChainedHeadNodeConfigs().get(0);
			Configuration operatorConfig = mapConfig.getCustomConfiguration(cl);
			assertEquals(1, operatorConfig.keySet().size());
			assertEquals("test_value1", operatorConfig.getString("test_key1", null));
			verifyChainedNode("map1", true, false, 1,
				Lists.newArrayList(Tuple2.of("map1", "filter1"), Tuple2.of("map1", "filter2")),
				Collections.emptyList(),
				54321L,
				mapConfig, cl, streamGraph);

			List<StreamEdge> mapChainedOutEdges = mapConfig.getChainedOutputs(cl);

			StreamConfig filter1Config = mapFilterTaskConfig.getChainedNodeConfigs().get(mapChainedOutEdges.get(0).getTargetId());
			assertEquals(0, filter1Config.getCustomConfiguration(cl).keySet().size());
			verifyChainedNode("filter1", false, true, 0,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("filter1", "map2")),
				54321L,
				filter1Config, cl, streamGraph);

			StreamConfig filter2Config = mapFilterTaskConfig.getChainedNodeConfigs().get(mapChainedOutEdges.get(1).getTargetId());
			assertEquals(0, filter2Config.getCustomConfiguration(cl).keySet().size());
			verifyChainedNode("filter2", false, true, 0,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("filter2", "map2"), Tuple2.of("filter2", "Sink: print2")),
				54321L,
				filter2Config, cl, streamGraph);
		}

		// CHAIN(Map2 -> Print1) vertex
		{
			JobVertex map2PrintVertex = verticesSorted.get(2);
			assertEquals(JobVertex.class, map2PrintVertex.getClass());

			StreamTaskConfigSnapshot mapPrintTaskConfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(map2PrintVertex.getConfiguration()), cl);
			verifyVertex(
				map2PrintVertex,
				TimeCharacteristic.EventTime, true, CheckpointingMode.AT_LEAST_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("filter1", "map2"), Tuple2.of("filter2", "map2")),
				Collections.emptyList(),
				Sets.newHashSet("map2", "Sink: print1"),
				Sets.newHashSet("map2"),
				TwoInputStreamTask.class,
				streamGraph);

			StreamConfig mapConfig = mapPrintTaskConfig.getChainedHeadNodeConfigs().get(0);
			assertEquals(0, mapConfig.getCustomConfiguration(cl).keySet().size());
			verifyChainedNode("map2", true, false, 2,
				Lists.newArrayList(Tuple2.of("map2", "Sink: print1")),
				Collections.emptyList(),
				54321L,
				mapConfig, cl, streamGraph);

			List<StreamEdge> mapChainedOutEdges = mapConfig.getChainedOutputs(cl);

			StreamConfig printConfig = mapPrintTaskConfig.getChainedNodeConfigs().get(mapChainedOutEdges.get(0).getTargetId());
			assertEquals(0, printConfig.getCustomConfiguration(cl).keySet().size());
			verifyChainedNode("Sink: print1", false, true, 0,
				Collections.emptyList(),
				Collections.emptyList(),
				54321L,
				printConfig, cl, streamGraph);
		}

		// Print2 vertex
		{
			JobVertex print2Vertex = verticesSorted.get(3);
			assertEquals(JobVertex.class, print2Vertex.getClass());

			StreamTaskConfigSnapshot printTaskConfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(print2Vertex.getConfiguration()), cl);
			verifyVertex(
				print2Vertex,
				TimeCharacteristic.EventTime, true, CheckpointingMode.AT_LEAST_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("filter2", "Sink: print2")),
				Collections.emptyList(),
				Sets.newHashSet("Sink: print2"),
				Sets.newHashSet("Sink: print2"),
				OneInputStreamTask.class,
				streamGraph);

			StreamConfig printConfig = printTaskConfig.getChainedHeadNodeConfigs().get(0);
			assertEquals(0, printConfig.getCustomConfiguration(cl).keySet().size());
			verifyChainedNode("Sink: print2", true, true, 1,
				Collections.emptyList(),
				Collections.emptyList(),
				54321L,
				printConfig, cl, streamGraph);
		}

		// verify slot-sharing groups
		SlotSharingGroup slotSharingGroup = verticesSorted.get(0).getSlotSharingGroup();
		assertNotNull(slotSharingGroup);
		for (JobVertex vertex : verticesSorted) {
			assertEquals(slotSharingGroup, vertex.getSlotSharingGroup());
		}
	}

	/**
	 * Tests source chaining logic using the following topology.
	 *
	 * <pre>
	 *     CHAIN(addSourceV2 -> Map1) -+
	 *                                 | -> CHAIN(Map3 -> Print1)
	 *     CHAIN(addSource -> Map2  ) -+
	 * </pre>
	 */
	@Test
	public void testSourceChaining() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(5);

		ClassLoader cl = getClass().getClassLoader();

		DataStream<Integer> map1 = env.addSourceV2(new NoOpSourceV2Function()).name("source1")
			.map((MapFunction<Integer, Integer>) value -> value).name("map1");

		DataStream<Integer> map2 = env.addSource(new NoOpSourceFunction()).name("source2")
			.map((MapFunction<Integer, Integer>) value -> value).name("map2");

		map1.connect(map2)
			.map(new NoOpCoMapFunction()).name("map3")
			.print().name("print1");

		JobGraph jobGraph = createJobGraph(env.getStreamGraph());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(3, verticesSorted.size());

		JobVertex sourceMapVertex = verticesSorted.get(0);
		assertEquals("Source: source2 -> map2", sourceMapVertex.getName());
		assertEquals(SourceStreamTask.class, sourceMapVertex.getInvokableClass(cl));

		JobVertex sourceV2MapVertex = verticesSorted.get(1);
		assertEquals("Source: source1 -> map1", sourceV2MapVertex.getName());
		assertEquals(SourceStreamTaskV2.class, sourceV2MapVertex.getInvokableClass(cl));

		JobVertex mapPrintVertex = verticesSorted.get(2);
		assertEquals("map3 -> Sink: print1", mapPrintVertex.getName());
		assertEquals(TwoInputStreamTask.class, mapPrintVertex.getInvokableClass(cl));
	}

	@Test
	public void testDisableOperatorChaining() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(5);

		// fromElements -> Map -> Print
		env.fromElements(1, 2, 3).name("source1")
			.map((MapFunction<Integer, Integer>) value -> value).name("map1")
			.print().name("print1");

		// chained
		{
			JobGraph jobGraph = createJobGraph(env.getStreamGraph());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(2, verticesSorted.size());

			JobVertex sourceVertex = verticesSorted.get(0);
			assertEquals("Source: source1", sourceVertex.getName());
			assertEquals(1, sourceVertex.getParallelism());

			JobVertex mapPrintVertex = verticesSorted.get(1);
			assertEquals("map1 -> Sink: print1", mapPrintVertex.getName());
			assertEquals(5, mapPrintVertex.getParallelism());
		}

		// disable chaining
		{
			env.disableOperatorChaining();
			JobGraph jobGraph = createJobGraph(env.getStreamGraph());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(3, verticesSorted.size());

			JobVertex sourceVertex = verticesSorted.get(0);
			JobVertex mapVertex = verticesSorted.get(1);
			JobVertex printVertex = verticesSorted.get(2);

			assertEquals("Source: source1", sourceVertex.getName());
			assertEquals("map1", mapVertex.getName());
			assertEquals("Sink: print1", printVertex.getName());

			assertEquals(1, sourceVertex.getParallelism());
			assertEquals(5, mapVertex.getParallelism());
			assertEquals(5, printVertex.getParallelism());
		}
	}

	/**
	 * Tests source chaining logic with disabled slot-sharing using the following topology.
	 *
	 * <pre>
	 *     CHAIN(addSourceV2 -> Map1) -+
	 *                                 | -> CHAIN(Map3 -> Print1)
	 *     CHAIN(addSource -> Map2  ) -+
	 * </pre>
	 */
	@Test
	public void testChainingWithDisabledSlotSharing() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(5);

		// disable slot sharing
		env.disableSlotSharing();

		ClassLoader cl = getClass().getClassLoader();

		DataStream<Integer> map1 = env.addSourceV2(new NoOpSourceV2Function()).name("source1")
				.map((MapFunction<Integer, Integer>) value -> value).name("map1");

		DataStream<Integer> map2 = env.addSource(new NoOpSourceFunction()).name("source2")
				.map((MapFunction<Integer, Integer>) value -> value).name("map2");

		map1.connect(map2)
				.map(new NoOpCoMapFunction()).name("map3")
				.print().name("print1");

		JobGraph jobGraph = createJobGraph(env.getStreamGraph());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(3, verticesSorted.size());

		JobVertex sourceMapVertex = verticesSorted.get(0);
		assertEquals("Source: source2 -> map2", sourceMapVertex.getName());
		assertEquals(SourceStreamTask.class, sourceMapVertex.getInvokableClass(cl));

		JobVertex sourceV2MapVertex = verticesSorted.get(1);
		assertEquals("Source: source1 -> map1", sourceV2MapVertex.getName());
		assertEquals(SourceStreamTaskV2.class, sourceV2MapVertex.getInvokableClass(cl));

		JobVertex mapPrintVertex = verticesSorted.get(2);
		assertEquals("map3 -> Sink: print1", mapPrintVertex.getName());
		assertEquals(TwoInputStreamTask.class, mapPrintVertex.getInvokableClass(cl));

		// verify slot-sharing groups
		for (JobVertex vertex : verticesSorted) {
			assertNull(vertex.getSlotSharingGroup());
		}
	}

	@Test
	public void testBlockingEdgeNotChained() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(5);

		// fromElements -> Map -> Print
		env.fromElements(1, 2, 3).name("source1")
			.map((MapFunction<Integer, Integer>) value -> value).name("map1")
			.print().name("print1");

		// chained
		{
			JobGraph jobGraph = createJobGraph(env.getStreamGraph());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(2, verticesSorted.size());

			JobVertex sourceVertex = verticesSorted.get(0);
			assertEquals("Source: source1", sourceVertex.getName());
			assertEquals(1, sourceVertex.getParallelism());

			JobVertex mapPrintVertex = verticesSorted.get(1);
			assertEquals("map1 -> Sink: print1", mapPrintVertex.getName());
			assertEquals(5, mapPrintVertex.getParallelism());
		}

		// set blocking edge
		{
			StreamGraph streamGraph = env.getStreamGraph();
			Map<String, StreamNode> streamNodeMap = new HashMap<>();
			for (StreamNode node : streamGraph.getStreamNodes()) {
				streamNodeMap.put(node.getOperatorName(), node);
			}
			List<StreamEdge> streamEdges = streamNodeMap.get("map1").getOutEdges();
			assertEquals(1, streamEdges.size());
			streamEdges.get(0).setDataExchangeMode(DataExchangeMode.BATCH);

			JobGraph jobGraph = createJobGraph(streamGraph);

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(3, verticesSorted.size());

			JobVertex sourceVertex = verticesSorted.get(0);
			JobVertex mapVertex = verticesSorted.get(1);
			JobVertex printVertex = verticesSorted.get(2);

			assertEquals("Source: source1", sourceVertex.getName());
			assertEquals("map1", mapVertex.getName());
			assertEquals("Sink: print1", printVertex.getName());

			assertEquals(1, sourceVertex.getParallelism());
			assertEquals(5, mapVertex.getParallelism());
			assertEquals(5, printVertex.getParallelism());

			assertEquals(1, mapVertex.getProducedDataSets().size());
			assertEquals(ResultPartitionType.BLOCKING, mapVertex.getProducedDataSets().get(0).getResultType());
		}
	}

	@Test
	public void testAutoDataExchangeEdgeChained() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(5);

		// fromElements -> Map -> Print
		env.fromElements(1, 2, 3).name("source1")
				.map((MapFunction<Integer, Integer>) value -> value).name("map1")
				.print().name("print1");

		// set BATCH execution mode
		{
			env.getConfig().setExecutionMode(ExecutionMode.BATCH);
			StreamGraph streamGraph = env.getStreamGraph();
			for (StreamNode node : streamGraph.getStreamNodes()) {
				for (StreamEdge inEdge : node.getInEdges()) {
					inEdge.setDataExchangeMode(DataExchangeMode.AUTO);
				}
			}
			JobGraph jobGraph = createJobGraph(streamGraph);

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(2, verticesSorted.size());

			JobVertex sourceVertex = verticesSorted.get(0);
			assertEquals("Source: source1", sourceVertex.getName());
			assertEquals(1, sourceVertex.getParallelism());

			JobVertex mapPrintVertex = verticesSorted.get(1);
			assertEquals("map1 -> Sink: print1", mapPrintVertex.getName());
			assertEquals(5, mapPrintVertex.getParallelism());

			assertEquals(1, sourceVertex.getProducedDataSets().size());
			assertEquals(ResultPartitionType.BLOCKING, sourceVertex.getProducedDataSets().get(0).getResultType());
		}

		// set PIPELINED execution mode
		{
			env.getConfig().setExecutionMode(ExecutionMode.PIPELINED);
			StreamGraph streamGraph = env.getStreamGraph();
			for (StreamNode node : streamGraph.getStreamNodes()) {
				for (StreamEdge inEdge : node.getInEdges()) {
					inEdge.setDataExchangeMode(DataExchangeMode.AUTO);
				}
			}
			JobGraph jobGraph = createJobGraph(streamGraph);

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(2, verticesSorted.size());

			JobVertex sourceVertex = verticesSorted.get(0);
			assertEquals("Source: source1", sourceVertex.getName());
			assertEquals(1, sourceVertex.getParallelism());

			JobVertex mapPrintVertex = verticesSorted.get(1);
			assertEquals("map1 -> Sink: print1", mapPrintVertex.getName());
			assertEquals(5, mapPrintVertex.getParallelism());

			assertEquals(1, sourceVertex.getProducedDataSets().size());
			assertEquals(ResultPartitionType.PIPELINED, sourceVertex.getProducedDataSets().get(0).getResultType());
		}
	}

	@Test
	public void testSubstituteStreamOperatorChaining() {
		// chained
		{
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(5);

			// fromElements -> Map -> Print
			TestSubstituteStreamOperator substituteOperator = new TestSubstituteStreamOperator<>(
				new StreamMap<>((MapFunction<Integer, Integer>) value -> value));
			substituteOperator.setChainingStrategy(ChainingStrategy.ALWAYS);

			env.fromElements(1, 2, 3).name("source1")
				.map((MapFunction<Integer, Integer>) value -> value).name("map1")
				.transform("operator1",
					BasicTypeInfo.INT_TYPE_INFO,
					substituteOperator)
				.print().name("print1");

			JobGraph jobGraph = createJobGraph(env.getStreamGraph());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(2, verticesSorted.size());
		}

		// not chained
		{
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setParallelism(5);

			// fromElements -> Map -> Print
			TestSubstituteStreamOperator substituteOperator = new TestSubstituteStreamOperator<>(
				new StreamMap<>((MapFunction<Integer, Integer>) value -> value));
			substituteOperator.setChainingStrategy(ChainingStrategy.HEAD);

			env.fromElements(1, 2, 3).name("source1")
				.map((MapFunction<Integer, Integer>) value -> value).name("map1")
				.transform("operator1",
					BasicTypeInfo.INT_TYPE_INFO,
					substituteOperator)
				.print().name("print1");

			JobGraph jobGraph = createJobGraph(env.getStreamGraph());

			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(3, verticesSorted.size());
		}
	}

	@Test
	public void testParallelismOneNotChained() {

		// --------- the program ---------

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Tuple2<String, String>> input = env
				.fromElements("a", "b", "c", "d", "e", "f")
				.map(new MapFunction<String, Tuple2<String, String>>() {

					@Override
					public Tuple2<String, String> map(String value) {
						return new Tuple2<>(value, value);
					}
				});

		DataStream<Tuple2<String, String>> result = input
				.keyBy(0)
				.map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

					@Override
					public Tuple2<String, String> map(Tuple2<String, String> value) {
						return value;
					}
				});

		result.addSink(new SinkFunction<Tuple2<String, String>>() {

			@Override
			public void invoke(Tuple2<String, String> value) {}
		});

		// --------- the job graph ---------

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setJobName("test job");
		JobGraph jobGraph = streamGraph.getJobGraph();
		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

		assertEquals(2, jobGraph.getNumberOfVertices());
		assertEquals(1, verticesSorted.get(0).getParallelism());
		assertEquals(1, verticesSorted.get(1).getParallelism());

		JobVertex sourceVertex = verticesSorted.get(0);
		JobVertex mapSinkVertex = verticesSorted.get(1);

		assertEquals(ResultPartitionType.PIPELINED, sourceVertex.getProducedDataSets().get(0).getResultType());
		assertEquals(ResultPartitionType.PIPELINED, mapSinkVertex.getInputs().get(0).getSource().getResultType());
	}

	@Test
	public void testParallelismOneChained() {

		// --------- the program ---------

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		DataStream<Tuple2<String, String>> input = env
			.fromElements("a", "b", "c", "d", "e", "f")
			.map(new MapFunction<String, Tuple2<String, String>>() {

				@Override
				public Tuple2<String, String> map(String value) {
					return new Tuple2<>(value, value);
				}
			});

		DataStream<Tuple2<String, String>> result = input
			.keyBy(0)
			.map(new MapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

				@Override
				public Tuple2<String, String> map(Tuple2<String, String> value) {
					return value;
				}
			});

		result.addSink(new SinkFunction<Tuple2<String, String>>() {

			@Override
			public void invoke(Tuple2<String, String> value) {}
		}).setParallelism(2);

		// --------- the job graph ---------

		StreamGraph streamGraph = env.getStreamGraph();
		streamGraph.setJobName("test job");
		streamGraph.setChainEagerlyEnabled(true);
		JobGraph jobGraph = streamGraph.getJobGraph();
		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

		assertEquals(2, jobGraph.getNumberOfVertices());
		assertEquals(1, verticesSorted.get(0).getParallelism());
		assertEquals(2, verticesSorted.get(1).getParallelism());

		JobVertex sourceVertex = verticesSorted.get(0);
		JobVertex sinkVertex = verticesSorted.get(1);

		assertEquals(ResultPartitionType.PIPELINED, sourceVertex.getProducedDataSets().get(0).getResultType());
		assertEquals(ResultPartitionType.PIPELINED, sinkVertex.getInputs().get(0).getSource().getResultType());
	}

	/**
	 * Tests that disabled checkpointing sets the checkpointing interval to Long.MAX_VALUE.
	 */
	@Test
	public void testDisabledCheckpointing() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamGraph streamGraph = new StreamGraph(env.getConfig(),
			env.getCheckpointConfig(),
			env.getParallelism(),
			env.getBufferTimeout(),
			DataPartitionerType.REBALANCE);
		assertFalse("Checkpointing enabled", streamGraph.getCheckpointConfig().isCheckpointingEnabled());

		JobGraph jobGraph = createJobGraph(streamGraph);

		JobCheckpointingSettings snapshottingSettings = jobGraph.getCheckpointingSettings();
		assertEquals(Long.MAX_VALUE, snapshottingSettings.getCheckpointCoordinatorConfiguration().getCheckpointInterval());
	}

	/**
	 * Verifies that the chain start/end is correctly set.
	 */
	@Test
	public void testChainStartEndSetting() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		// fromElements -> CHAIN(Map -> Print)
		env.fromElements(1, 2, 3).setParallelism(1)
			.map(new MapFunction<Integer, Integer>() {
				@Override
				public Integer map(Integer value) throws Exception {
					return value;
				}
			}).setParallelism(2)
			.print().setParallelism(2);
		JobGraph jobGraph = createJobGraph(env.getStreamGraph());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(2, verticesSorted.size());

		JobVertex sourceVertex = verticesSorted.get(0);
		JobVertex mapPrintVertex = verticesSorted.get(1);

		assertEquals(ResultPartitionType.PIPELINED, sourceVertex.getProducedDataSets().get(0).getResultType());
		assertEquals(ResultPartitionType.PIPELINED, mapPrintVertex.getInputs().get(0).getSource().getResultType());

		ClassLoader cl = getClass().getClassLoader();

		StreamTaskConfigSnapshot sourceTaskConfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(sourceVertex.getConfiguration()), cl);
		StreamTaskConfigSnapshot mapPrintTaskConfig = StreamTaskConfigCache.deserializeFrom(new StreamTaskConfig(mapPrintVertex.getConfiguration()), cl);

		StreamConfig sourceConfig = sourceTaskConfig.getChainedHeadNodeConfigs().get(0);
		StreamConfig mapConfig = mapPrintTaskConfig.getChainedHeadNodeConfigs().get(0);
		List<StreamEdge> mapOutEdges = mapConfig.getChainedOutputs(cl);
		StreamConfig printConfig = mapPrintTaskConfig.getChainedNodeConfigs().get(mapOutEdges.get(0).getTargetId());

		assertTrue(sourceConfig.isChainStart());
		assertTrue(sourceConfig.isChainEnd());

		assertTrue(mapConfig.isChainStart());
		assertFalse(mapConfig.isChainEnd());

		assertFalse(printConfig.isChainStart());
		assertTrue(printConfig.isChainEnd());
	}

	/**
	 * Verifies that the resources are merged correctly for chained operators (covers source and sink cases)
	 * when generating job graph.
	 */
	@Test
	public void testResourcesForChainedSourceSink() throws Exception {
		ResourceSpec resource1 = ResourceSpec.newBuilder().setCpuCores(0.1).setHeapMemoryInMB(100).build();
		ResourceSpec resource2 = ResourceSpec.newBuilder().setCpuCores(0.2).setHeapMemoryInMB(200).build();
		ResourceSpec resource3 = ResourceSpec.newBuilder().setCpuCores(0.3).setHeapMemoryInMB(300).build();
		ResourceSpec resource4 = ResourceSpec.newBuilder().setCpuCores(0.4).setHeapMemoryInMB(400).build();
		ResourceSpec resource5 = ResourceSpec.newBuilder().setCpuCores(0.5).setHeapMemoryInMB(500).build();

		Method opMethod = SingleOutputStreamOperator.class.getDeclaredMethod("setResources", ResourceSpec.class);
		opMethod.setAccessible(true);

		Method sinkMethod = DataStreamSink.class.getDeclaredMethod("setResources", ResourceSpec.class);
		sinkMethod.setAccessible(true);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Tuple2<Integer, Integer>> source = env.addSource(new ParallelSourceFunction<Tuple2<Integer, Integer>>() {
			@Override
			public void run(SourceContext<Tuple2<Integer, Integer>> ctx) throws Exception {
			}

			@Override
			public void cancel() {
			}
		});
		opMethod.invoke(source, resource1);

		DataStream<Tuple2<Integer, Integer>> map = source.map(new MapFunction<Tuple2<Integer, Integer>, Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
				return value;
			}
		});
		opMethod.invoke(map, resource2);

		// CHAIN(Source -> Map -> Filter)
		DataStream<Tuple2<Integer, Integer>> filter = map.filter(new FilterFunction<Tuple2<Integer, Integer>>() {
			@Override
			public boolean filter(Tuple2<Integer, Integer> value) throws Exception {
				return false;
			}
		});
		opMethod.invoke(filter, resource3);

		DataStream<Tuple2<Integer, Integer>> reduce = filter.keyBy(0).reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
			@Override
			public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {
				return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
			}
		});
		opMethod.invoke(reduce, resource4);

		DataStreamSink<Tuple2<Integer, Integer>> sink = reduce.addSink(new SinkFunction<Tuple2<Integer, Integer>>() {
			@Override
			public void invoke(Tuple2<Integer, Integer> value) throws Exception {
			}
		});
		sinkMethod.invoke(sink, resource5);

		JobGraph jobGraph = createJobGraph(env.getStreamGraph());

		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(2, verticesSorted.size());

		JobVertex sourceMapFilterVertex = verticesSorted.get(0);
		JobVertex reduceSinkVertex = verticesSorted.get(1);

		assertTrue(sourceMapFilterVertex.getMinResources().equals(resource1.merge(resource2).merge(resource3)));
		assertTrue(reduceSinkVertex.getPreferredResources().equals(resource4.merge(resource5)));
	}

	/**
	 * Verifies that the resources are merged correctly for chained operators (covers middle chaining and iteration cases)
	 * when generating job graph.
	 */
	@Test
	public void testResourcesForIteration() throws Exception {
		ResourceSpec resource1 = ResourceSpec.newBuilder().setCpuCores(0.1).setHeapMemoryInMB(100).build();
		ResourceSpec resource2 = ResourceSpec.newBuilder().setCpuCores(0.2).setHeapMemoryInMB(200).build();
		ResourceSpec resource3 = ResourceSpec.newBuilder().setCpuCores(0.3).setHeapMemoryInMB(300).build();
		ResourceSpec resource4 = ResourceSpec.newBuilder().setCpuCores(0.4).setHeapMemoryInMB(400).build();
		ResourceSpec resource5 = ResourceSpec.newBuilder().setCpuCores(0.5).setHeapMemoryInMB(500).build();

		Method opMethod = SingleOutputStreamOperator.class.getDeclaredMethod("setResources", ResourceSpec.class);
		opMethod.setAccessible(true);

		Method sinkMethod = DataStreamSink.class.getDeclaredMethod("setResources", ResourceSpec.class);
		sinkMethod.setAccessible(true);

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source = env.addSource(new ParallelSourceFunction<Integer>() {
			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
			}

			@Override
			public void cancel() {
			}
		}).name("test_source");
		opMethod.invoke(source, resource1);

		IterativeStream<Integer> iteration = source.iterate(3000);
		opMethod.invoke(iteration, resource2);

		DataStream<Integer> flatMap = iteration.flatMap(new FlatMapFunction<Integer, Integer>() {
			@Override
			public void flatMap(Integer value, Collector<Integer> out) throws Exception {
				out.collect(value);
			}
		}).name("test_flatMap");
		opMethod.invoke(flatMap, resource3);

		// CHAIN(flatMap -> Filter)
		DataStream<Integer> increment = flatMap.filter(new FilterFunction<Integer>() {
			@Override
			public boolean filter(Integer value) throws Exception {
				return false;
			}
		}).name("test_filter");
		opMethod.invoke(increment, resource4);

		DataStreamSink<Integer> sink = iteration.closeWith(increment).addSink(new SinkFunction<Integer>() {
			@Override
			public void invoke(Integer value) throws Exception {
			}
		}).disableChaining().name("test_sink");
		sinkMethod.invoke(sink, resource5);

		JobGraph jobGraph = createJobGraph(env.getStreamGraph());

		for (JobVertex jobVertex : jobGraph.getVertices()) {
			if (jobVertex.getName().contains("test_source")) {
				assertTrue(jobVertex.getMinResources().equals(resource1));
			} else if (jobVertex.getName().contains("Iteration_Source")) {
				assertTrue(jobVertex.getPreferredResources().equals(resource2));
			} else if (jobVertex.getName().contains("test_flatMap")) {
				assertTrue(jobVertex.getMinResources().equals(resource3.merge(resource4)));
			} else if (jobVertex.getName().contains("Iteration_Tail")) {
				assertTrue(jobVertex.getPreferredResources().equals(ResourceSpec.DEFAULT));
			} else if (jobVertex.getName().contains("test_sink")) {
				assertTrue(jobVertex.getMinResources().equals(resource5));
			}
		}
	}

	/**
	 * Verifies whether generated JobEdge ResultPartitionType is as correctly set with configured
	 * StreamEdge DataExchangeMode and Job ExecutionMode=PIPELINED.
	 */
	@Test
	public void testEdgeResultPartitionTypeAssignmentInPipelinedMode() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setExecutionMode(ExecutionMode.PIPELINED);

		DataStream<Integer> sourceMap = env.fromElements(1, 2, 3).name("source1")
			.map((value) -> value).name("map1").setParallelism(10);

		DataStream<Integer> filter1 = sourceMap.filter((value) -> false).name("filter1").setParallelism(2);
		DataStream<Integer> filter2 = sourceMap.filter((value) -> false).name("filter2").setParallelism(3);

		filter1.connect(filter2).map(new CoMapFunction<Integer, Integer, Integer>() {
			@Override
			public Integer map1(Integer value) {
				return value;
			}

			@Override
			public Integer map2(Integer value) {
				return value;
			}
		}).name("map2").setParallelism(4).print().name("print1").setParallelism(5);
		filter2.print().name("print2").setParallelism(6);

		StreamGraph streamGraph = env.getStreamGraph();
		for (StreamNode node : streamGraph.getStreamNodes()) {
			if (node.getOutEdges().size() > 0) {
				node.getOutEdges().get(0).setDataExchangeMode(DataExchangeMode.BATCH);
				break;
			}
		}

		JobGraph jobGraph = streamGraph.getJobGraph();
		int pipelinedEdges = 0;
		int blockingEdges = 0;
		int edges = 0;
		for (JobVertex vertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
			for (IntermediateDataSet dataSet : vertex.getProducedDataSets()) {
				edges++;
				if (dataSet.getResultType() == ResultPartitionType.PIPELINED) {
					pipelinedEdges++;
				} else if (dataSet.getResultType() == ResultPartitionType.BLOCKING) {
					blockingEdges++;
				}
			}
		}
		assertEquals(edges, pipelinedEdges + blockingEdges);
		assertEquals(1, blockingEdges);
	}

	/**
	 * Verifies whether generated JobEdge ResultPartitionType is as correctly set with configured
	 * StreamEdge DataExchangeMode and Job ExecutionMode=BATCH.
	 */
	@Test
	public void testEdgeResultPartitionTypeAssignmentInBatchMode() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setExecutionMode(ExecutionMode.BATCH);

		DataStream<Integer> sourceMap = env.fromElements(1, 2, 3).name("source1")
			.map((value) -> value).name("map1").setParallelism(10);

		DataStream<Integer> filter1 = sourceMap.filter((value) -> false).name("filter1").setParallelism(2);
		DataStream<Integer> filter2 = sourceMap.filter((value) -> false).name("filter2").setParallelism(3);

		filter1.connect(filter2).map(new CoMapFunction<Integer, Integer, Integer>() {
			@Override
			public Integer map1(Integer value) {
				return value;
			}

			@Override
			public Integer map2(Integer value) {
				return value;
			}
		}).name("map2").setParallelism(4).print().name("print1").setParallelism(5);
		filter2.print().name("print2").setParallelism(6);

		StreamGraph streamGraph = env.getStreamGraph();
		for (StreamNode node : streamGraph.getStreamNodes()) {
			if (node.getOutEdges().size() > 0) {
				node.getOutEdges().get(0).setDataExchangeMode(DataExchangeMode.PIPELINED);
				break;
			}
		}

		JobGraph jobGraph = streamGraph.getJobGraph();
		int pipelinedEdges = 0;
		int blockingEdges = 0;
		int edges = 0;
		for (JobVertex vertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
			for (IntermediateDataSet dataSet : vertex.getProducedDataSets()) {
				edges++;
				if (dataSet.getResultType() == ResultPartitionType.PIPELINED) {
					pipelinedEdges++;
				} else if (dataSet.getResultType() == ResultPartitionType.BLOCKING) {
					blockingEdges++;
				}
			}
		}
		assertEquals(edges, pipelinedEdges + blockingEdges);
		assertEquals(1, pipelinedEdges);
	}

	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private static class TestSubstituteStreamOperator<IN, OUT> implements AbstractOneInputSubstituteStreamOperator<IN, OUT> {

		private ChainingStrategy chainingStrategy = ChainingStrategy.ALWAYS;
		private final OneInputStreamOperator<IN, OUT> actualStreamOperator;

		TestSubstituteStreamOperator(OneInputStreamOperator<IN, OUT> actualStreamOperator) {
			this.actualStreamOperator = actualStreamOperator;
		}

		@Override
		public StreamOperator<OUT> getActualStreamOperator(ClassLoader cl) {
			return actualStreamOperator;
		}

		@Override
		public void setChainingStrategy(ChainingStrategy chainingStrategy) {
			this.chainingStrategy = chainingStrategy;
			this.actualStreamOperator.setChainingStrategy(chainingStrategy);
		}

		@Override
		public boolean requireState() {
			return actualStreamOperator.requireState();
		}

		@Override
		public ChainingStrategy getChainingStrategy() {
			return chainingStrategy;
		}

		@Override
		public void endInput() throws Exception {
			actualStreamOperator.endInput();
		}
	}

	static void verifyVertex(
			JobVertex vertex,
			TimeCharacteristic expectedTimeChar,
			boolean expectedCheckpointingEnabled,
			CheckpointingMode expectedCheckpointMode,
			Class<?> expectedStateBackend,
			List<Tuple2<String, String>> expectedInEdges,
			List<Tuple2<String, String>> expectedOutEdges,
			Set<String> expectedChainedNodes,
			Set<String> expectedHeadNodes,
			Class<? extends AbstractInvokable> expectedInvokableclass,
			final StreamGraph streamGraph) {

		ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		StreamTaskConfigSnapshot vertexConfig = StreamTaskConfigCache.deserializeFrom(
				new StreamTaskConfig(vertex.getConfiguration()), classLoader);

		assertEquals(expectedInEdges.size(), vertex.getInputs().size());
		assertEquals(expectedInvokableclass, vertex.getInvokableClass(classLoader));

		assertEquals(expectedTimeChar, vertexConfig.getTimeCharacteristic());
		assertEquals(expectedCheckpointingEnabled, vertexConfig.isCheckpointingEnabled());
		assertEquals(expectedCheckpointMode, vertexConfig.getCheckpointMode());
		assertEquals(expectedStateBackend, vertexConfig.getStateBackend().getClass());
		assertEquals(expectedInEdges.size(), vertexConfig.getInStreamEdgesOfChain().size());
		assertEquals(expectedOutEdges.size(), vertexConfig.getOutStreamEdgesOfChain().size());
		assertEquals(expectedChainedNodes.size(), vertexConfig.getChainedNodeConfigs().size());
		assertEquals(expectedHeadNodes.size(), vertexConfig.getChainedHeadNodeIds().size());
		assertEquals(expectedHeadNodes.size(), vertexConfig.getChainedHeadNodeConfigs().size());

		List<String> expectedInEdgesKeys = expectedInEdges.stream().map((tuple) -> tuple.f0 + "|" + tuple.f1)
				.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
		for (int i = 0; i < vertexConfig.getInStreamEdgesOfChain().size(); i++) {
			StreamEdge edge = vertexConfig.getInStreamEdgesOfChain().get(i);
			assertEquals(expectedInEdgesKeys.get(i),
					streamGraph.getStreamNode(edge.getSourceId()).getOperatorName() + "|" + streamGraph.getStreamNode(edge.getTargetId()).getOperatorName());
		}

		List<String> expectedOutEdgesKeys = expectedOutEdges.stream().map((tuple) -> tuple.f0 + "|" + tuple.f1)
				.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
		for (int i = 0; i < vertexConfig.getOutStreamEdgesOfChain().size(); i++) {
			StreamEdge edge = vertexConfig.getOutStreamEdgesOfChain().get(i);
			assertEquals(expectedOutEdgesKeys.get(i),
					streamGraph.getStreamNode(edge.getSourceId()).getOperatorName() + "|" + streamGraph.getStreamNode(edge.getTargetId()).getOperatorName());
		}

		for (Map.Entry<Integer, StreamConfig> entry: vertexConfig.getChainedNodeConfigs().entrySet()) {
			assertTrue(String.format("The chained node '%s' is not in the expected list.", entry.getValue().getOperatorName()),
					expectedChainedNodes.contains(entry.getValue().getOperatorName()));
		}

		for (StreamConfig headNodeConfig : vertexConfig.getChainedHeadNodeConfigs()) {
			assertTrue(String.format("The chained head node '%s' is not in the expected list.", headNodeConfig.getOperatorName()),
					expectedHeadNodes.contains(headNodeConfig.getOperatorName()));
		}
	}

	private static void verifyChainedNode(
		String operatorName,
		boolean expectedChainStart,
		boolean expectedChainEnd,
		int expectedNumInputs,
		List<Tuple2<String, String>> expectedChainedOutputs,
		List<Tuple2<String, String>> expectedNonChainedOutputs,
		long expectedBufferTimeout,
		final StreamConfig nodeConfig,
		final ClassLoader classLoader,
		final StreamGraph streamGraph) {

		final List<StreamEdge> chainedOutEdges = nodeConfig.getChainedOutputs(classLoader);
		final List<StreamEdge> nonChainedOutEdges = nodeConfig.getNonChainedOutputs(classLoader);

		assertEquals(operatorName, nodeConfig.getOperatorName());
		assertEquals(expectedChainStart, nodeConfig.isChainStart());
		assertEquals(expectedChainEnd, nodeConfig.isChainEnd());
		assertEquals(expectedNumInputs, nodeConfig.getNumberOfInputs());
		assertEquals(expectedNonChainedOutputs.size(), nodeConfig.getNumberOfOutputs());
		assertEquals(expectedChainedOutputs.size(), nodeConfig.getChainedOutputs(classLoader).size());
		assertEquals(expectedNonChainedOutputs.size(), nodeConfig.getNonChainedOutputs(classLoader).size());

		List<String> expectedChainedOutEdgesKeys = expectedChainedOutputs.stream().map((tuple) -> tuple.f0 + "|" + tuple.f1)
			.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
		for (int i = 0; i < chainedOutEdges.size(); i++) {
			StreamEdge edge = chainedOutEdges.get(i);
			assertEquals(expectedChainedOutEdgesKeys.get(i),
					streamGraph.getStreamNode(edge.getSourceId()).getOperatorName() + "|" + streamGraph.getStreamNode(edge.getTargetId()).getOperatorName());
		}

		List<String> expectedNonChainedOutEdgesKeys = expectedNonChainedOutputs.stream().map((tuple) -> tuple.f0 + "|" + tuple.f1)
			.collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
		for (int i = 0; i < nonChainedOutEdges.size(); i++) {
			StreamEdge edge = nonChainedOutEdges.get(i);
			assertEquals(expectedNonChainedOutEdgesKeys.get(i),
					streamGraph.getStreamNode(edge.getSourceId()).getOperatorName() + "|" + streamGraph.getStreamNode(edge.getTargetId()).getOperatorName());
		}

		assertEquals(expectedBufferTimeout, nodeConfig.getBufferTimeout());
	}

	// --------------------------------------------------------------------------------

	private static class NoOpSourceFunction implements ParallelSourceFunction<Integer> {

		private static final long serialVersionUID = -6459224792698512633L;

		@Override
		public void run(SourceContext<Integer> ctx) throws Exception {
		}

		@Override
		public void cancel() {
		}
	}

	private static class NoOpSourceV2Function implements ParallelSourceFunctionV2<Integer> {

		private static final long serialVersionUID = -6459224792698512633L;

		@Override
		public boolean isFinished() {
			return true;
		}

		@Override
		public SourceRecord<Integer> next() throws Exception {
			return null;
		}

		@Override
		public void cancel() {
		}
	}

	private static class NoOpSinkFunction implements SinkFunction<Integer> {

	}

	private static class NoOpMapFunction implements MapFunction<Integer, Integer> {

		@Override
		public Integer map(Integer value) throws Exception {
			return value;
		}
	}

	private static class NoOpCoMapFunction implements CoMapFunction<Integer, Integer, Integer> {

		@Override
		public Integer map1(Integer value) {
			return value;
		}

		@Override
		public Integer map2(Integer value) {
			return value;
		}
	}

	private static class NoOpCoProcessFuntion extends CoProcessFunction<Integer, Integer, Integer> {

		@Override
		public void processElement1(Integer value, Context ctx, Collector<Integer> out) {

		}

		@Override
		public void processElement2(Integer value, Context ctx, Collector<Integer> out) {

		}
	}

	private static class NoOpFilterFunction implements FilterFunction<Integer> {

		@Override
		public boolean filter(Integer value) throws Exception {
			return true;
		}
	}
}
