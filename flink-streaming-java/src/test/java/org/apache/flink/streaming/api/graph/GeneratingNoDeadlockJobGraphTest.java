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
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.io.network.DataExchangeMode;
import org.apache.flink.runtime.operators.DamBehavior;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunctionV2;
import org.apache.flink.streaming.api.functions.source.SourceRecord;
import org.apache.flink.streaming.api.graph.StreamNode.ReadPriority;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation.ReadOrder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for generating no deadlock JobGraph.
 */
public class GeneratingNoDeadlockJobGraphTest {

	/**
	 * Tests for inferring ReadPriority.
	 */
	public static class InferringReadPriorityTest {

		@Test
		public void testBlockingEdge() throws Exception {
			// case
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
				DataStream<String> process1 = map1.process(new NoOpProcessFuntion()).name("process1");
				DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> process2 = process1.connect(filter1).process(new NoOpCoProcessFuntion()).name("process2");
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
				DataStreamSink<String> sink1 = process2.addSink(new NoOpSinkFunction()).name("sink1");

				testBase(env,
						Arrays.asList(
								new TestEdge(map1.getId(), process1.getId()).setDataExchangeMode(DataExchangeMode.BATCH)),
						Arrays.asList(
								new TestEdge(process2.getId(), sink1.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(process1.getId(), process2.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(filter1.getId(), process2.getId()).setReadPriority(ReadPriority.LOWER),
								new TestEdge(map1.getId(), process1.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(source1.getId(), map1.getId()).setReadPriority(ReadPriority.LOWER),
								new TestEdge(source1.getId(), filter1.getId()).setReadPriority(ReadPriority.LOWER)
						));
			}

			// case
			{
				StreamExecutionEnvironment env = createEnv();
				env.getConfig().setExecutionMode(ExecutionMode.BATCH);

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
				DataStream<String> process1 = map1.process(new NoOpProcessFuntion()).name("process1");
				DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> process2 = process1.connect(filter1).process(new NoOpCoProcessFuntion()).name("process2");
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
				DataStreamSink<String> sink1 = process2.addSink(new NoOpSinkFunction()).name("sink1");

				testBase(env,
						Arrays.asList(
								new TestEdge(process2.getId(), sink1.getId()).setDataExchangeMode(DataExchangeMode.PIPELINED),
								new TestEdge(process1.getId(), process2.getId()).setDataExchangeMode(DataExchangeMode.PIPELINED),
								new TestEdge(filter1.getId(), process2.getId()).setDataExchangeMode(DataExchangeMode.PIPELINED),
								new TestEdge(map1.getId(), process1.getId()).setDataExchangeMode(DataExchangeMode.AUTO),
								new TestEdge(source1.getId(), map1.getId()).setDataExchangeMode(DataExchangeMode.PIPELINED),
								new TestEdge(source1.getId(), filter1.getId()).setDataExchangeMode(DataExchangeMode.PIPELINED)),
						Arrays.asList(
								new TestEdge(process2.getId(), sink1.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(process1.getId(), process2.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(filter1.getId(), process2.getId()).setReadPriority(ReadPriority.LOWER),
								new TestEdge(map1.getId(), process1.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(source1.getId(), map1.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(source1.getId(), filter1.getId()).setReadPriority(ReadPriority.LOWER)
						));
			}
		}

		@Test
		public void testOthers() throws Exception {
			// case
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

				DataStream<String> process1 = source1.connect(source2).process(new NoOpCoProcessFuntion()).name("process1");
				((TwoInputTransformation) process1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

				DataStream<String> filter1 = process1.filter(new NoOpFilterFunction()).name("filter1");
				DataStream<String> process2 = filter1.connect(process1).process(new NoOpCoProcessFuntion()).name("process2");
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

				DataStreamSink<String> sink1 = process1.addSink(new NoOpSinkFunction()).name("sink1");
				DataStreamSink<String> sink2 = process2.addSink(new NoOpSinkFunction()).name("sink2");

				testBase(env, null,
						Arrays.asList(
								new TestEdge(process2.getId(), sink2.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(filter1.getId(), process2.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(process1.getId(), process2.getId()).setReadPriority(ReadPriority.LOWER),
								new TestEdge(process1.getId(), filter1.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(process1.getId(), sink1.getId()).setReadPriority(ReadPriority.DYNAMIC),
								new TestEdge(source1.getId(), process1.getId()).setReadPriority(ReadPriority.DYNAMIC),
								new TestEdge(source1.getId(), process1.getId()).setReadPriority(ReadPriority.DYNAMIC)
						));
			}

			// case
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> source3 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source3");

				DataStream<String> map1 = source2.map(new NoOpMapFunction()).name("map1");
				DataStream<String> process1 = source1.connect(map1).process(new NoOpCoProcessFuntion()).name("process1");
				((TwoInputTransformation) process1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

				DataStream<String> process2 = process1.connect(source3).process(new NoOpCoProcessFuntion()).name("process2");
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
				DataStreamSink<String> sink1 = process2.addSink(new NoOpSinkFunction()).name("sink1");

				testBase(env, null,
						Arrays.asList(
								new TestEdge(process2.getId(), sink1.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(process1.getId(), process2.getId()).setReadPriority(ReadPriority.LOWER),
								new TestEdge(source3.getId(), process2.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(source1.getId(), process1.getId()).setReadPriority(ReadPriority.LOWER),
								new TestEdge(map1.getId(), process1.getId()).setReadPriority(ReadPriority.DYNAMIC),
								new TestEdge(source2.getId(), map1.getId()).setReadPriority(ReadPriority.DYNAMIC)
						));
			}

			// case
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> source3 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source3");
				DataStream<String> source4 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source4");

				DataStream<String> process1 = source2.connect(source3).process(new NoOpCoProcessFuntion()).name("process1");
				((TwoInputTransformation) process1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
				DataStream<String> process2 = source1.connect(process1).process(new NoOpCoProcessFuntion()).name("process2");
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

				DataStream<String> process3 = process2.connect(source4).process(new NoOpCoProcessFuntion()).name("process3");
				((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
				DataStreamSink<String> sink1 = process3.addSink(new NoOpSinkFunction()).name("sink1");

				testBase(env, null,
						Arrays.asList(
								new TestEdge(process3.getId(), sink1.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(process2.getId(), process3.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(source4.getId(), process3.getId()).setReadPriority(ReadPriority.LOWER),
								new TestEdge(source1.getId(), process2.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(process1.getId(), process2.getId()).setReadPriority(ReadPriority.DYNAMIC),
								new TestEdge(source2.getId(), process1.getId()).setReadPriority(ReadPriority.DYNAMIC),
								new TestEdge(source3.getId(), process1.getId()).setReadPriority(ReadPriority.DYNAMIC)
						));
			}

			// case
			{
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

				DataStream<String> process1 = source1.rescale().transform(
						"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
				((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

				DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
				DataStream<String> process2 = map1.connect(source2).process(new NoOpCoProcessFuntion()).name("process2");
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

				DataStream<String> process3 = process1.connect(process2).process(new NoOpCoProcessFuntion()).name("process3");
				((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

				DataStreamSink<String> sink1 = process3.addSink(new NoOpSinkFunction()).name("sink1");

				testBase(env, null,
						Arrays.asList(
								new TestEdge(process3.getId(), sink1.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(process1.getId(), process3.getId()).setReadPriority(ReadPriority.LOWER),
								new TestEdge(process2.getId(), process3.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(map1.getId(), process2.getId()).setReadPriority(ReadPriority.DYNAMIC),
								new TestEdge(source2.getId(), process2.getId()).setReadPriority(ReadPriority.HIGHER),
								new TestEdge(source1.getId(), process1.getId()).setReadPriority(ReadPriority.LOWER),
								new TestEdge(source1.getId(), map1.getId()).setReadPriority(ReadPriority.DYNAMIC)
						));
			}
		}

		public static void testBase(StreamExecutionEnvironment env,
			List<TestEdge> dataExchangeModes,
			List<TestEdge> expectedPriorities) throws Exception {

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}

			if (dataExchangeModes != null) {
				for (TestEdge testEdge : dataExchangeModes) {
					for (StreamEdge edge : streamGraph.getStreamEdges(testEdge.getSourceId(), testEdge.getTargetId())) {
						edge.setDataExchangeMode(testEdge.getDataExchangeMode());
					}
				}
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
			StreamingJobGraphGenerator.inferReadPriority(chainingNodeMap, sortedChainingNodes, streamGraph);

			for (TestEdge testEdge : expectedPriorities) {
				ReadPriority readPriority = testEdge.getReadPriority();
				Integer sourceId = testEdge.getSourceId(), targetId = testEdge.getTargetId();

				String message = String.format("[source: %s, target: %s]",
						streamGraph.getStreamNode(sourceId).getOperatorName(),
						streamGraph.getStreamNode(targetId).getOperatorName());

				assertEquals(message, readPriority, chainingNodeMap.get(targetId).getReadPriority(sourceId));
				assertTrue(message, chainingNodeMap.get(sourceId).getDownPriorityNodes(readPriority).contains(targetId));
			}
		}

		private static class TestEdge {

			private final Integer sourceId;
			private final Integer targetId;

			private DataExchangeMode dataExchangeMode;
			private ReadPriority readPriority;

			public TestEdge(Integer sourceId, Integer targetId) {
				this.sourceId = sourceId;
				this.targetId = targetId;
			}

			public Integer getSourceId() {
				return sourceId;
			}

			public Integer getTargetId() {
				return targetId;
			}

			public DataExchangeMode getDataExchangeMode() {
				return dataExchangeMode;
			}

			public TestEdge setDataExchangeMode(DataExchangeMode dataExchangeMode) {
				this.dataExchangeMode = dataExchangeMode;
				return this;
			}

			public ReadPriority getReadPriority() {
				return readPriority;
			}

			public TestEdge setReadPriority(ReadPriority readPriority) {
				this.readPriority = readPriority;
				return this;
			}
		}
	}

	@Test
	public void testBasicGraph() throws Exception {
		// case:
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			map1.addSink(new NoOpSinkFunction()).name("sink1");

			testBase(env, new Integer[][]{});
		}

		// case:
		{
			for (ReadOrder readOrder : new ReadOrder[] {
					ReadOrder.RANDOM_ORDER, ReadOrder.INPUT1_FIRST, ReadOrder.INPUT2_FIRST, ReadOrder.SPECIAL_ORDER}) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> map1 = source1.connect(source2).map(new NoOpCoMapFunction()).name("map1");
				map1.addSink(new NoOpSinkFunction()).name("sink1");
				map1.addSink(new NoOpSinkFunction()).name("sink2");

				if (readOrder != ReadOrder.RANDOM_ORDER) {
					((TwoInputTransformation) map1.getTransformation()).setReadOrderHint(readOrder);
				}

				testBase(env, readOrder.name(), new Integer[][]{});
			}
		}
	}

	@Test
	public void testUnion() throws Exception {
		// case: triangle
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> map1 = filter1.union(source1).map(new NoOpMapFunction()).name("map1");
			map1.addSink(new NoOpSinkFunction()).name("sink1");

			testBase(env, new Integer[][]{});
		}

		// case: double cross
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
			DataStream<String> map1 = source1.union(source2).map(new NoOpMapFunction()).name("map1");
			DataStream<String> map2 = source1.union(source2).map(new NoOpMapFunction()).name("map2");
			map1.addSink(new NoOpSinkFunction()).name("sink1");
			map2.addSink(new NoOpSinkFunction()).name("sink2");

			testBase(env, new Integer[][]{});
		}
	}

	@Test
	public void testTriangleTopology1() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
				ReadOrder.RANDOM_ORDER, ReadOrder.INPUT2_FIRST, ReadOrder.SPECIAL_ORDER}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			DataStream<String> process1 = map1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process2 = process1.connect(source1).process(new NoOpCoProcessFuntion()).name("process2");
			process2.addSink(new NoOpSinkFunction()).name("sink1");

			if (readOrder != ReadOrder.RANDOM_ORDER) {
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(readOrder);
			}

			switch (readOrder) {
				case RANDOM_ORDER:
					testBase(env, readOrder.name(), new Integer[][]{});
					break;
				case INPUT2_FIRST:
					testBase(env, readOrder.name(), new Integer[][]{
							new Integer[]{process1.getId(), process2.getId()}
					});
					break;
				case SPECIAL_ORDER:
					testBase(env, readOrder.name(), new Integer[][]{
							new Integer[]{process1.getId(), process2.getId()}
					});
					break;
			}
		}
	}

	@Test
	public void testTriangleTopology2() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
				ReadOrder.RANDOM_ORDER, ReadOrder.INPUT1_FIRST, ReadOrder.SPECIAL_ORDER}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			DataStream<String> process1 = map1.rescale().transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process2 = source1.connect(process1).process(new NoOpCoProcessFuntion()).name("process2");
			process2.addSink(new NoOpSinkFunction()).name("sink1");

			if (readOrder != ReadOrder.RANDOM_ORDER) {
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(readOrder);
			}

			switch (readOrder) {
				case RANDOM_ORDER:
					testBase(env, readOrder.name(), new Integer[][]{
							new Integer[]{map1.getId(), process1.getId()}
					});
					break;
				case INPUT2_FIRST:
					testBase(env, readOrder.name(), new Integer[][]{
							new Integer[]{map1.getId(), process1.getId()},
							new Integer[]{process1.getId(), process2.getId()}
					});
					break;
				case SPECIAL_ORDER:
					testBase(env, readOrder.name(), new Integer[][]{
							new Integer[]{map1.getId(), process1.getId()},
							new Integer[]{process1.getId(), process2.getId()}
					});
					break;
			}
		}
	}

	@Test
	public void testTriangleTopology3() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
				ReadOrder.RANDOM_ORDER, ReadOrder.INPUT2_FIRST, ReadOrder.SPECIAL_ORDER}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> map1 = source1.rescale().map(new NoOpMapFunction()).name("map1");
			DataStream<String> map2 = source1.map(new NoOpMapFunction()).name("map2");
			DataStream<String> process1 = map1.connect(map2).transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpTwoInputStreamOperator()).name("process1");
			((TwoInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process2 = process1.rescale().connect(source1).process(new NoOpCoProcessFuntion()).name("process2");
			process2.addSink(new NoOpSinkFunction()).name("sink1");

			if (readOrder != ReadOrder.RANDOM_ORDER) {
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(readOrder);
			}

			testBase(env, readOrder.name(), new Integer[][]{
					new Integer[]{source1.getId(), map1.getId()},
					new Integer[]{process1.getId(), process2.getId()}
			});
		}
	}

	@Test
	public void testTriangleTopology4() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
				ReadOrder.RANDOM_ORDER, ReadOrder.INPUT2_FIRST, ReadOrder.SPECIAL_ORDER}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			DataStream<String> process1 = map1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process2 = process1.rescale().connect(source1).process(new NoOpCoProcessFuntion()).name("process2");
			process2.addSink(new NoOpSinkFunction()).name("sink1");

			if (readOrder != ReadOrder.RANDOM_ORDER) {
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(readOrder);
			}

			testBase(env, readOrder.name(), new Integer[][]{
					new Integer[]{process1.getId(), process2.getId()}
			});
		}
	}

	@Test
	public void testTriangleTopology5() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
				ReadOrder.RANDOM_ORDER, ReadOrder.INPUT1_FIRST, ReadOrder.SPECIAL_ORDER}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			DataStream<String> process1 = map1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> filter1 = process1.rescale().filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> filter2 = process1.rescale().filter(new NoOpFilterFunction()).name("filter2");
			DataStream<String> map2 = filter1.connect(filter2).process(new NoOpCoProcessFuntion()).name("map1");

			DataStream<String> process2 = source1.connect(map2).process(new NoOpCoProcessFuntion()).name("process2");
			process2.addSink(new NoOpSinkFunction()).name("sink1");

			if (readOrder != ReadOrder.RANDOM_ORDER) {
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(readOrder);
			}

			testBase(env, readOrder.name(), new Integer[][]{
					new Integer[]{process1.getId(), filter1.getId()},
					new Integer[]{process1.getId(), filter2.getId()}
			});
		}
	}

	@Test
	public void testCrossTopology1() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
				ReadOrder.RANDOM_ORDER, ReadOrder.INPUT1_FIRST, ReadOrder.INPUT2_FIRST}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
			DataStream<String> process1 = source1.connect(source2).process(new NoOpCoProcessFuntion()).name("process1");
			DataStream<String> process2 = source1.connect(source2).process(new NoOpCoProcessFuntion()).name("process2");
			process1.addSink(new NoOpSinkFunction()).name("sink1");
			process2.addSink(new NoOpSinkFunction()).name("sink2");

			String msgPrefix = null;
			switch (readOrder) {
				case RANDOM_ORDER:
					msgPrefix = "ALL_RANDOM_ORDER";
					break;
				case INPUT1_FIRST:
					((TwoInputTransformation) process1.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
					((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
					msgPrefix = "ALL_INPUT1_FIRST";
					break;
				case INPUT2_FIRST:
					((TwoInputTransformation) process1.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
					((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
					msgPrefix = "ALL_INPUT2_FIRST";
					break;
			}

			testBase(env, msgPrefix, new Integer[][]{});
		}
	}

	@Test
	public void testCrossTopology2() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
			ReadOrder.INPUT1_FIRST, ReadOrder.SPECIAL_ORDER}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			DataStream<String> process1 = map1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process2 = source1.connect(source2).process(new NoOpCoProcessFuntion()).name("process2");
			DataStream<String> process3 = process1.connect(source2).process(new NoOpCoProcessFuntion()).name("process3");
			process2.addSink(new NoOpSinkFunction()).name("sink1");
			process3.addSink(new NoOpSinkFunction()).name("sink2");

			switch (readOrder) {
				case INPUT1_FIRST:
					((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

					testBase(env, "INPUT1_INPUT2_FIRST ", new Integer[][]{
							new Integer[]{process1.getId(), process3.getId()}
					});
					break;
				case SPECIAL_ORDER:
					((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);

					testBase(env, "ALL_SPECIAL_ORDER ", new Integer[][]{
							new Integer[]{process1.getId(), process3.getId()}
					});
					break;
			}
		}
	}

	@Test
	public void testCrossTopology3() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
				ReadOrder.INPUT1_FIRST, ReadOrder.SPECIAL_ORDER}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			DataStream<String> process1 = map1.rescale().transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process2 = source1.connect(source2).process(new NoOpCoProcessFuntion()).name("process2");
			DataStream<String> process3 = process1.connect(source2).process(new NoOpCoProcessFuntion()).name("process3");
			process2.addSink(new NoOpSinkFunction()).name("sink1");
			process3.addSink(new NoOpSinkFunction()).name("sink2");

			switch (readOrder) {
				case INPUT1_FIRST:
					((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

					testBase(env, "INPUT1_INPUT2_FIRST ", new Integer[][]{
							new Integer[]{map1.getId(), process1.getId()},
							new Integer[]{process1.getId(), process3.getId()}
					});
					break;
				case SPECIAL_ORDER:
					((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);

					testBase(env, "ALL_SPECIAL_ORDER ", new Integer[][]{
							new Integer[]{map1.getId(), process1.getId()},
							new Integer[]{process1.getId(), process3.getId()}
					});
					break;
			}
		}
	}

	@Test
	public void testCrossTopology4() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
				ReadOrder.INPUT1_FIRST, ReadOrder.SPECIAL_ORDER}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			DataStream<String> process1 = map1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> map2 = process1.rescale().map(new NoOpMapFunction()).name("map2");
			DataStream<String> filter1 = process1.rescale().filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> map3 = map2.connect(filter1).map(new NoOpCoMapFunction()).name("map3");

			DataStream<String> process2 = source1.connect(source2).process(new NoOpCoProcessFuntion()).name("process2");
			DataStream<String> process3 = map3.connect(source2).process(new NoOpCoProcessFuntion()).name("process3");
			process2.addSink(new NoOpSinkFunction()).name("sink1");
			process3.addSink(new NoOpSinkFunction()).name("sink2");

			switch (readOrder) {
				case INPUT1_FIRST:
					((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

					testBase(env, "INPUT1_INPUT2_FIRST ", new Integer[][]{
							new Integer[]{process1.getId(), map2.getId()},
							new Integer[]{process1.getId(), filter1.getId()}
					});
					break;
				case SPECIAL_ORDER:
					((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);

					testBase(env, "ALL_SPECIAL_ORDER ", new Integer[][]{
							new Integer[]{process1.getId(), map2.getId()},
							new Integer[]{process1.getId(), filter1.getId()}
					});
					break;
			}
		}
	}

	@Test
	public void testCrossTopology5() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
				ReadOrder.INPUT2_FIRST, ReadOrder.SPECIAL_ORDER}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
			DataStream<String> process1 = source1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process2 = source2.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process2");
			((OneInputTransformation) process2.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process3 = process1.connect(source2).process(new NoOpCoProcessFuntion()).name("process3");
			DataStream<String> process4 = source1.connect(process2).process(new NoOpCoProcessFuntion()).name("process4");
			process3.addSink(new NoOpSinkFunction()).name("sink1");
			process4.addSink(new NoOpSinkFunction()).name("sink2");

			switch (readOrder) {
				case INPUT2_FIRST:
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
					((TwoInputTransformation) process4.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

					testBase(env, "INPUT2_INPUT1_FIRST ", new Integer[][]{
							new Integer[]{process1.getId(), process3.getId()},
							new Integer[]{process2.getId(), process4.getId()}
					});
					break;
				case SPECIAL_ORDER:
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
					((TwoInputTransformation) process4.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);

					testBase(env, "ALL_SPECIAL_ORDER ", new Integer[][]{
							new Integer[]{process1.getId(), process3.getId()},
							new Integer[]{process2.getId(), process4.getId()}
					});
					break;
			}
		}
	}

	@Test
	public void testDam() throws Exception {
		// case:
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> process1 = source1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			process1.addSink(new NoOpSinkFunction()).name("sink1");

			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			testBase(env, new Integer[][]{});
		}

		// case:
		{
			for (ReadOrder readOrder : new ReadOrder[] {
					ReadOrder.RANDOM_ORDER, ReadOrder.INPUT1_FIRST, ReadOrder.INPUT2_FIRST, ReadOrder.SPECIAL_ORDER}) {
				StreamExecutionEnvironment env = createEnv();

				DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
				DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
				DataStream<String> process1 = source1.transform(
						"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
				DataStream<String> process2 = source2.transform(
						"operator2", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process2");
				DataStream<String> process3 = process1.connect(process2).process(new NoOpCoProcessFuntion()).name("process3");
				process3.addSink(new NoOpSinkFunction()).name("sink1");
				process3.addSink(new NoOpSinkFunction()).name("sink2");

				((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);
				((OneInputTransformation) process2.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

				if (readOrder != ReadOrder.RANDOM_ORDER) {
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(readOrder);
				}

				testBase(env, readOrder.name(), new Integer[][]{});
			}
		}
	}

	@Test
	public void testUnionAndTriangleTopology() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
				ReadOrder.INPUT2_FIRST, ReadOrder.SPECIAL_ORDER}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> process1 = source1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> process2 = process1.connect(source1.union(filter1)).process(new NoOpCoProcessFuntion()).name("process2");
			process2.addSink(new NoOpSinkFunction()).name("sink1");

			((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(readOrder);

			testBase(env, readOrder.name(), new Integer[][]{
					new Integer[]{process1.getId(), process2.getId()}
			});
		}
	}

	@Test
	public void testUnionAndCrossTopology() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
				ReadOrder.INPUT2_FIRST, ReadOrder.SPECIAL_ORDER}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");

			DataStream<String> map2 = map1.rescale().map(new NoOpMapFunction()).name("map2");
			DataStream<String> filter1 = map1.rescale().filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> process1 = map2.union(filter1).transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process2 = process1.connect(source2).process(new NoOpCoProcessFuntion()).name("process2");
			DataStream<String> process3 = source1.connect(source2).process(new NoOpCoProcessFuntion()).name("process3");
			process2.addSink(new NoOpSinkFunction()).name("sink1");
			process3.addSink(new NoOpSinkFunction()).name("sink2");

			switch (readOrder) {
				case INPUT1_FIRST:
					((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

					testBase(env, "INPUT2_INPUT1_FIRST ", new Integer[][]{
							new Integer[]{map1.getId(), map2.getId()},
							new Integer[]{map1.getId(), filter1.getId()},
							new Integer[]{process1.getId(), process2.getId()}
					});
					break;
				case SPECIAL_ORDER:
					((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);

					testBase(env, "ALL_SPECIAL_ORDER ", new Integer[][]{
							new Integer[]{map1.getId(), map2.getId()},
							new Integer[]{map1.getId(), filter1.getId()},
							new Integer[]{process1.getId(), process2.getId()}
					});
					break;
			}
		}
	}

	@Test
	public void testTriangleAndCrossTopology() throws Exception {
		for (ReadOrder readOrder : new ReadOrder[] {
			ReadOrder.INPUT1_FIRST, ReadOrder.SPECIAL_ORDER}) {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

			DataStream<String> process1 = source1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);
			DataStream<String> process2 = process1.transform(
					"operator2", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process2");
			((OneInputTransformation) process2.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process3 = source1.connect(process1).process(new NoOpCoProcessFuntion()).name("process3");
			DataStream<String> process4 = source2.connect(process2).process(new NoOpCoProcessFuntion()).name("process4");
			DataStream<String> process5 = source2.connect(source1).process(new NoOpCoProcessFuntion()).name("process5");

			process3.addSink(new NoOpSinkFunction()).name("sink1");
			process4.addSink(new NoOpSinkFunction()).name("sink2");
			process5.addSink(new NoOpSinkFunction()).name("sink3");

			switch (readOrder) {
				case INPUT1_FIRST:
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
					((TwoInputTransformation) process4.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
					((TwoInputTransformation) process5.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

					testBase(env, "INPUT1_FIRST ", new Integer[][]{
							new Integer[]{process1.getId(), process3.getId()},
							new Integer[]{process2.getId(), process4.getId()}
					});
					break;
				case SPECIAL_ORDER:
					((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
					((TwoInputTransformation) process4.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
					((TwoInputTransformation) process5.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);

					testBase(env, "ALL_SPECIAL_ORDER ", new Integer[][]{
							new Integer[]{process1.getId(), process3.getId()},
							new Integer[]{process2.getId(), process4.getId()}
					});
					break;
			}
		}
	}

	@Test
	public void testDamAndUnion() throws Exception {
		// case: triangle
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> process1 = source1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			DataStream<String> map1 = process1.union(source1).map(new NoOpMapFunction()).name("map1");
			map1.addSink(new NoOpSinkFunction()).name("sink1");

			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			testBase(env, new Integer[][]{});
		}

		// case: double cross
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
			DataStream<String> process1 = source1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			DataStream<String> process2 = source2.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process2");
			DataStream<String> map1 = process1.union(source2).map(new NoOpMapFunction()).name("map1");
			DataStream<String> map2 = source1.union(process2).map(new NoOpMapFunction()).name("map2");
			map1.addSink(new NoOpSinkFunction()).name("sink1");
			map2.addSink(new NoOpSinkFunction()).name("sink2");

			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);
			((OneInputTransformation) process2.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			testBase(env, new Integer[][]{});
		}
	}

	private static StreamExecutionEnvironment createEnv() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend((StateBackend) new FsStateBackend(GeneratingNoDeadlockJobGraphTest.class.getResource("/").toURI()));
		env.setMultiHeadChainMode(true);

		return env;
	}

	private static void testBase(StreamExecutionEnvironment env, Integer[][] expect) {
		testBase(env, null, expect);
	}

	private static void testBase(StreamExecutionEnvironment env, String msgPrefix, Integer[][] expect) {
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
		StreamingJobGraphGenerator.breakOffChainForNoDeadlock(chainingNodeMap, sortedChainingNodes, streamGraph);

		Set<StreamEdge> breakEdgeSet = new HashSet<>();
		for (StreamingJobGraphGenerator.ChainingStreamNode chainingNode : chainingNodeMap.values()) {
			Integer nodeId = chainingNode.getNodeId();
			StreamNode node = streamGraph.getStreamNode(nodeId);
			for (StreamEdge edge : node.getInEdges()) {
				Integer sourceId = edge.getSourceId();
				if (!chainingNode.isChainTo(sourceId)) {
					breakEdgeSet.add(edge);
				}
			}
		}

		for (int i = 0; i < expect.length; i++) {
			Integer sourceId = expect[i][0];
			Integer targetId = expect[i][1];
			assertTrue(
					String.format("%sThe edge(%s -> %s) is not broken.",
							(msgPrefix != null && !msgPrefix.isEmpty()) ? msgPrefix + ": " : "",
							streamGraph.getStreamNode(sourceId).getOperatorName(),
							streamGraph.getStreamNode(targetId).getOperatorName()),
					breakEdgeSet.contains(streamGraph.getStreamEdges(sourceId, targetId).get(0)));
		}
		assertEquals((msgPrefix != null ? msgPrefix + ":" : "") +
				"The size of broken edges is not equal.", expect.length, breakEdgeSet.size());
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

	private static class NoOpProcessFuntion extends ProcessFunction<String, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

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

	private static class NoOpCoProcessFuntion extends CoProcessFunction<String, String, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public void processElement1(String value, Context ctx, Collector<String> out) {

		}

		@Override
		public void processElement2(String value, Context ctx, Collector<String> out) {

		}
	}

	private static class NoOpOneInputStreamOperator extends AbstractStreamOperator<String>
			implements OneInputStreamOperator<String, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(StreamRecord<String> element) throws Exception {

		}

		@Override
		public void endInput() throws Exception {

		}
	}

	private static class NoOpTwoInputStreamOperator extends AbstractStreamOperator<String>
			implements TwoInputStreamOperator<String, String, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public TwoInputSelection firstInputSelection() {
			return null;
		}

		@Override
		public void endInput1() throws Exception {

		}

		@Override
		public void endInput2() throws Exception {

		}

		@Override
		public TwoInputSelection processElement2(StreamRecord<String> element) throws Exception {
			return null;
		}

		@Override
		public TwoInputSelection processElement1(StreamRecord<String> element) throws Exception {
			return null;
		}

		@Override
		public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<String>> output) {

		}
	}
}
