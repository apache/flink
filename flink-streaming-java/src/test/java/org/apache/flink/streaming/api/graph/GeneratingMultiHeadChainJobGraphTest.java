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
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.operators.DamBehavior;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunctionV2;
import org.apache.flink.streaming.api.functions.source.SourceRecord;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation.ReadOrder;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ArbitraryInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.OneInputStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTask;
import org.apache.flink.streaming.runtime.tasks.SourceStreamTaskV2;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.tasks.TwoInputStreamTask;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.api.graph.StreamingJobGraphGeneratorTest.verifyVertex;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link StreamingJobGraphGenerator}.
 */
@SuppressWarnings("serial")
public class GeneratingMultiHeadChainJobGraphTest extends TestLogger {

	/**
	 * Tests the following topology.
	 *
	 * <p><pre>
	 *     Source1 -> Filter1 -+
	 *        |                | -> (union) -> Map1 -> Sink1
	 *        |_   ->         -+
	 * </pre>
	 */
	@Test
	public void testUnionOperatorTopology1() throws Exception {
		// case: triangle
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> map1 = filter1.union(source1).map(new NoOpMapFunction()).name("map1");
			map1.addSink(new NoOpSinkFunction()).name("sink1");

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(1, jobGraph.getNumberOfVertices());

			int vertexIndex = 0;

			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Collections.emptyList(),
					Sets.newHashSet("Source: source1", "filter1", "map1", "Sink: sink1"),
					Sets.newHashSet("Source: source1"),
					ArbitraryInputStreamTask.class,
					streamGraph);
		}
	}

	/**
	 * Tests the following topology.
	 *
	 * <p><pre>
	 *     Source1 ->                                                         -+
	 *         |_ ->                       -+                                  |
	 *                                      | -> (union) -> Process1 -> Sink1  | -> (union) -> Process2 -> Sink2
	 *                               |- -> -+                                  |
	 *     Source2 -> (rescale) -> Map1 ->                                    -+
	 * </pre>
	 */
	@Test
	public void testUnionOperatorTopology2() throws Exception {
		StreamExecutionEnvironment env = createEnv();

		DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
		DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
		DataStream<String> map1 = source2.rescale().map(new NoOpMapFunction()).name("map1");

		DataStream<String> process1 = source1.union(map1).process(new NoOpProcessFuntion()).name("process1");
		DataStream<String> process2 = source1.union(map1).process(new NoOpProcessFuntion()).name("process2");
		process1.addSink(new NoOpSinkFunction()).name("sink1");
		process2.addSink(new NoOpSinkFunction()).name("sink2");

		StreamGraph streamGraph = env.getStreamGraph();
		for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
			pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
		}
		JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
		List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
		assertEquals(2, jobGraph.getNumberOfVertices());

		int vertexIndex = 0;

		// CHAIN(Source2)
		verifyVertex(
				verticesSorted.get(vertexIndex++),
				TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Collections.emptyList(),
				Lists.newArrayList(Tuple2.of("Source: source2", "map1")),
				Sets.newHashSet("Source: source2"),
				Sets.newHashSet("Source: source2"),
				SourceStreamTaskV2.class,
				streamGraph);

		// CHAIN([Source1 -> (Process1, Process2), Map1 -> (Process1, Process2), Process1 -> Sink1, Process2 -> Sink2])
		verifyVertex(
				verticesSorted.get(vertexIndex++),
				TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
				Lists.newArrayList(Tuple2.of("Source: source2", "map1")),
				Collections.emptyList(),
				Sets.newHashSet("Source: source1", "map1", "process1", "process2", "Sink: sink1", "Sink: sink2"),
				Sets.newHashSet("Source: source1", "map1"),
				ArbitraryInputStreamTask.class,
				streamGraph);
	}

	/**
	 * Tests the following topology.
	 *
	 * <p><pre>
	 *     Source1 -> Filter1 -+
	 *        |                | ->  Process1 -> Sink1
	 *        |_   ->         -+
	 * </pre>
	 */
	@Test
	public void testSimpleTwoInputOperatorTopology() throws Exception {
		// case: triangle
		{
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> process1 = filter1.connect(source1).process(new NoOpCoProcessFuntion()).name("process1");
			process1.addSink(new NoOpSinkFunction()).name("sink1");

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(1, jobGraph.getNumberOfVertices());

			int vertexIndex = 0;

			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Collections.emptyList(),
					Sets.newHashSet("Source: source1", "filter1", "process1", "Sink: sink1"),
					Sets.newHashSet("Source: source1"),
					ArbitraryInputStreamTask.class,
					streamGraph);
		}
	}

	/**
	 * Tests for the topology which include {@link ReadOrder}.
	 */
	@RunWith(Parameterized.class)
	public static class ParameterizedReadOrderTest {

		private final ReadOrder readOrder;

		public ParameterizedReadOrderTest(ReadOrder readOrder) {
			this.readOrder = readOrder;
		}

		@Parameterized.Parameters(name = "type={0}")
		public static Collection<ReadOrder> getParameters() {
			return Arrays.asList(new ReadOrder[]{
					null, ReadOrder.INPUT1_FIRST, ReadOrder.INPUT2_FIRST, ReadOrder.SPECIAL_ORDER
			});
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     Source1  -+                +- Map1
		 *               | -> Process1 -> |
		 *     Source2  -+                +- Map2
		 * </pre>
		 */
		@Test
		public void testSimpleTopology() throws Exception {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

			DataStream<String> process1 = source1.connect(source2).process(new NoOpCoProcessFuntion()).name("process1");
			process1.map(new NoOpMapFunction()).name("map1");
			process1.map(new NoOpMapFunction()).name("map2");

			if (this.readOrder != null) {
				((TwoInputTransformation) process1.getTransformation()).setReadOrderHint(readOrder);
			}

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			assertEquals(1, jobGraph.getNumberOfVertices());

			verifyVertex(
					jobGraph.getVertices().iterator().next(),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Collections.emptyList(),
					Sets.newHashSet("Source: source1", "Source: source2", "process1", "map1", "map2"),
					Sets.newHashSet("Source: source1", "Source: source2"),
					ArbitraryInputStreamTask.class,
					streamGraph);
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     RunnableSource1 -> Map1 -+
		 *                              | -> Process1 -> Map3
		 *     RunnableSource2 -> Map2 -+       |_    -> Map4
		 * </pre>
		 */
		@Test
		public void testRunnableSourceTopology() throws Exception {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSource(new NoOpSourceFunction()).name("source1");
			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");

			DataStream<String> source2 = env.addSource(new NoOpSourceFunction()).name("source2");
			DataStream<String> map2 = source2.map(new NoOpMapFunction()).name("map2");

			DataStream<String> process1 = map1.connect(map2).process(new NoOpCoProcessFuntion()).name("process1");
			process1.map(new NoOpMapFunction()).name("map3");
			process1.map(new NoOpMapFunction()).name("map4");

			if (this.readOrder != null) {
				((TwoInputTransformation) process1.getTransformation()).setReadOrderHint(this.readOrder);
			}

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(3, jobGraph.getNumberOfVertices());

			int vertexIndex = 0;

			// CHAIN(Source1 -> Map1)
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("map1", "process1")),
					Sets.newHashSet("Source: source1", "map1"),
					Sets.newHashSet("Source: source1"),
					SourceStreamTask.class,
					streamGraph);

			// CHAIN(Source2 -> Map2)
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("map2", "process1")),
					Sets.newHashSet("Source: source2", "map2"),
					Sets.newHashSet("Source: source2"),
					SourceStreamTask.class,
					streamGraph);

			// CHAIN(Process1 -> (Map3, Map4))
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("map1", "process1"), Tuple2.of("map2", "process1")),
					Collections.emptyList(),
					Sets.newHashSet("process1", "map3", "map4"),
					Sets.newHashSet("process1"),
					TwoInputStreamTask.class,
					streamGraph);
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     Source1 -> Process1(dam) -+
		 *                               | -> Process3 -> Map1
		 *     Source2 -> Process2(dam) -+         |_ -> Map2
		 * </pre>
		 */
		@Test
		public void testDamOperatorTopology() throws Exception {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
			DataStream<String> process1 = source1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			DataStream<String> process2 = source2.transform(
					"operator2", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process2");
			DataStream<String> process3 = process1.connect(process2).process(new NoOpCoProcessFuntion()).name("process3");
			process3.map(new NoOpMapFunction()).name("map1");
			process3.map(new NoOpMapFunction()).name("map2");

			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);
			((OneInputTransformation) process2.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(readOrder);

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			assertEquals(1, jobGraph.getNumberOfVertices());

			verifyVertex(
					verticesSorted.get(0),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Collections.emptyList(),
					Sets.newHashSet("Source: source1", "Source: source2", "process1", "process2", "process3",
							"map1", "map2"),
					Sets.newHashSet("Source: source1", "Source: source2"),
					ArbitraryInputStreamTask.class,
					streamGraph);
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     Source1 -> (rescale)-> Map1 -> Filter1                  -+
		 *                            |                                 | -> Process2 -> Map2
		 *                            |_ -> (rescale) -> Process1(dam) -+
		 * </pre>
		 */
		@Test
		public void testTriangleTopology() throws Exception {
			if (this.readOrder == ReadOrder.INPUT2_FIRST) {
				return;
			}

			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> map1 = source1.rescale().map(new NoOpMapFunction()).name("map1");
			DataStream<String> filter1 = map1.filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> process1 = map1.rescale().transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process2 = filter1.connect(process1).process(new NoOpCoProcessFuntion()).name("process2");
			process2.map(new NoOpMapFunction()).name("map2");

			if (readOrder != null) {
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(this.readOrder);
			}

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			if (this.readOrder != null) {
				assertEquals(4, jobGraph.getNumberOfVertices());

				int vertexIndex = 0;

				// CHAIN(Source1)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Collections.emptyList(),
						Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
						Sets.newHashSet("Source: source1"),
						Sets.newHashSet("Source: source1"),
						SourceStreamTaskV2.class,
						streamGraph);

				// CHAIN(Map1 -> Filter1)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
						Lists.newArrayList(Tuple2.of("filter1", "process2"), Tuple2.of("map1", "process1")),
						Sets.newHashSet("map1", "filter1"),
						Sets.newHashSet("map1"),
						OneInputStreamTask.class,
						streamGraph);

				// CHAIN(Process1)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("map1", "process1")),
						Lists.newArrayList(Tuple2.of("process1", "process2")),
						Sets.newHashSet("process1"),
						Sets.newHashSet("process1"),
						OneInputStreamTask.class,
						streamGraph);

				// CHAIN(Process2 -> Map2)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("filter1", "process2"), Tuple2.of("process1", "process2")),
						Collections.emptyList(),
						Sets.newHashSet("process2", "map2"),
						Sets.newHashSet("process2"),
						TwoInputStreamTask.class,
						streamGraph);
			} else {
				assertEquals(3, jobGraph.getNumberOfVertices());

				int vertexIndex = 0;

				// CHAIN(Source1)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Collections.emptyList(),
						Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
						Sets.newHashSet("Source: source1"),
						Sets.newHashSet("Source: source1"),
						SourceStreamTaskV2.class,
						streamGraph);

				// CHAIN(Map1 -> Filter1)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
						Lists.newArrayList(Tuple2.of("filter1", "process2"), Tuple2.of("map1", "process1")),
						Sets.newHashSet("map1", "filter1"),
						Sets.newHashSet("map1"),
						OneInputStreamTask.class,
						streamGraph);

				// CHAIN(Process1 -> Process2, Process2 -> Map2)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("filter1", "process2"), Tuple2.of("map1", "process1")),
						Collections.emptyList(),
						Sets.newHashSet("process1", "process2", "map2"),
						Sets.newHashSet("process1", "process2"),
						ArbitraryInputStreamTask.class,
						streamGraph);
			}
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     Source1 -> Map1 ->                                           -+
		 *                 |_ ->                    -+                       |
		 *                                           | -> Process2 -> Map2   | -> Process3 -> Map3
		 *                                    |- -> -+                       |
		 *     Source2 -> (rescale) -> Process1(dam) ->                     -+
		 * </pre>
		 */
		@Test
		public void testCrossTopology() throws Exception {
			if (this.readOrder == ReadOrder.INPUT2_FIRST) {
				return;
			}

			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			DataStream<String> process1 = source2.rescale().transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process2 = map1.connect(process1).process(new NoOpCoProcessFuntion()).name("process2");
			DataStream<String> process3 = source1.connect(source2).process(new NoOpCoProcessFuntion()).name("process3");
			process2.map(new NoOpMapFunction()).name("map2");
			process3.map(new NoOpMapFunction()).name("map3");

			if (readOrder != null) {
				switch (readOrder) {
					case INPUT1_FIRST:
						((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);
						((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);
						break;
					case SPECIAL_ORDER:
						((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
						((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);
						break;
				}
			}

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			if (this.readOrder != null) {
				assertEquals(3, jobGraph.getNumberOfVertices());

				int vertexIndex = 0;

				// CHAIN([Source1 -> Map1, (Source1, Source2) -> Process3, Process3 -> Map3])
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Collections.emptyList(),
						Lists.newArrayList(Tuple2.of("map1", "process2"), Tuple2.of("Source: source2", "process1")),
						Sets.newHashSet("Source: source1", "map1", "Source: source2", "process3", "map3"),
						Sets.newHashSet("Source: source1", "Source: source2"),
						ArbitraryInputStreamTask.class,
						streamGraph);

				// CHAIN(Process1)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("Source: source2", "process1")),
						Lists.newArrayList(Tuple2.of("process1", "process2")),
						Sets.newHashSet("process1"),
						Sets.newHashSet("process1"),
						OneInputStreamTask.class,
						streamGraph);

				// CHAIN(Process2 -> Map2)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("map1", "process2"), Tuple2.of("process1", "process2")),
						Collections.emptyList(),
						Sets.newHashSet("process2", "map2"),
						Sets.newHashSet("process2"),
						TwoInputStreamTask.class,
						streamGraph);
			} else {
				assertEquals(2, jobGraph.getNumberOfVertices());

				int vertexIndex = 0;

				// CHAIN([Source1 -> Map1, (Source1, Source2) -> Process3, Process3 -> Map3])
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Collections.emptyList(),
						Lists.newArrayList(Tuple2.of("map1", "process2"), Tuple2.of("Source: source2", "process1")),
						Sets.newHashSet("Source: source1", "map1", "Source: source2", "process3", "map3"),
						Sets.newHashSet("Source: source1", "Source: source2"),
						ArbitraryInputStreamTask.class,
						streamGraph);

				// CHAIN([Process1 -> Process2, Process2 -> Map2])
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("map1", "process2"), Tuple2.of("Source: source2", "process1")),
						Collections.emptyList(),
						Sets.newHashSet("process1", "process2", "map2"),
						Sets.newHashSet("process1", "process2"),
						ArbitraryInputStreamTask.class,
						streamGraph);
			}
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     Source1 -> Filter1 ->                                -+
		 *        |                                                  | -> Process2 -> Map3
		 *        |_  -> Process1(dam) -> (rescale) -> Map1 -> Map2 -+
		 * </pre>
		 */
		@Test
		public void testRhombusTopology() throws Exception {
			if (this.readOrder == ReadOrder.INPUT1_FIRST) {
				return;
			}

			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");

			DataStream<String> process1 = source1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> map1 = process1.rescale().map(new NoOpMapFunction()).name("map1");
			DataStream<String> map2 = map1.map(new NoOpMapFunction()).name("map2");
			DataStream<String> process2 = map2.connect(filter1).process(new NoOpCoProcessFuntion()).name("process2");
			process2.map(new NoOpMapFunction()).name("map3");

			if (readOrder != null) {
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(readOrder);
			}

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(2, jobGraph.getNumberOfVertices());

			int vertexIndex = 0;

			// CHAIN(Source1 -> (Process1, Filter1))
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("filter1", "process2"), Tuple2.of("process1", "map1")),
					Sets.newHashSet("Source: source1", "process1", "filter1"),
					Sets.newHashSet("Source: source1"),
					SourceStreamTaskV2.class,
					streamGraph);

			// CHAIN([Map1 -> Map2, Map2 -> Process2, Process2 -> Map3])
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("filter1", "process2"), Tuple2.of("process1", "map1")),
					Collections.emptyList(),
					Sets.newHashSet("map1", "map2", "process2", "map3"),
					Sets.newHashSet("map1", "process2"),
					ArbitraryInputStreamTask.class,
					streamGraph);
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     RunnableSource1 -> Filter1 -> Sink1
		 *              |           |_ -> -+
		 *              |                  | -> Map1 -> Sink2
		 *              |_ ->             -+
		 *              |_ -> Filter2 ->  -+
		 *                                 |
		 *     Source2 -+                  | -> Process1 ->       -+
		 *              | -> Map2 ->      -+       |               | -> Process2 -> Map3
		 *     Source3 -+                          |   Source4 -> -+
		 *                                         |
		 *                                         |_ -> Process3 -> Sink4
		 * </pre>
		 */
		@Test
		public void testMixSourceTopology() throws Exception {
			if (this.readOrder == null) {
				return;
			}

			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSource(new NoOpSourceFunction()).name("source1");
			DataStream<String> filter1 = source1.filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> filter2 = source1.filter(new NoOpFilterFunction()).name("filter2");

			filter1.addSink(new NoOpSinkFunction()).name("sink1");
			filter1.connect(source1).map(new NoOpCoMapFunction()).name("map1")
					.addSink(new NoOpSinkFunction()).name("sink2");

			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");
			DataStream<String> source3 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source3");
			DataStream<String> source4 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source4");

			DataStream<String> map2 = source2.connect(source3).map(new NoOpCoMapFunction()).name("map2");
			DataStream<String> process1 = filter2.connect(map2).process(new NoOpCoProcessFuntion()).name("process1");

			DataStream<String> process2 = process1.connect(source4).process(new NoOpCoProcessFuntion()).name("process2");
			process2.map(new NoOpMapFunction()).name("map3");

			process1.process(new NoOpProcessFuntion()).name("process3")
					.addSink(new NoOpSinkFunction()).name("sink4");

			((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(this.readOrder);

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(3, jobGraph.getNumberOfVertices());

			int vertexIndex = 0;

			// CHAIN([Source1 -> (Filter1, Filter2), Filter1 -> Sink1])
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("filter1", "map1"), Tuple2.of("Source: source1", "map1"), Tuple2.of("filter2", "process1")),
					Sets.newHashSet("Source: source1", "filter1", "filter2", "Sink: sink1"),
					Sets.newHashSet("Source: source1"),
					SourceStreamTask.class,
					streamGraph);

			// CHAIN(Map1 -> Sink2)
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("filter1", "map1"), Tuple2.of("Source: source1", "map1")),
					Collections.emptyList(),
					Sets.newHashSet("map1", "Sink: sink2"),
					Sets.newHashSet("map1"),
					TwoInputStreamTask.class,
					streamGraph);

			// CHAIN([(Source2, Source3) -> Map2, Map2 -> Process1, Process1 -> Process3, Process3 -> Sink4,
			//        (Process1, Source4) -> Process2, Process2 -> map3])
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("filter2", "process1")),
					Collections.emptyList(),
					Sets.newHashSet("Source: source2", "Source: source3", "map2", "process1", "process3", "Sink: sink4",
							"Source: source4", "process2", "map3"),
					Sets.newHashSet("Source: source2", "Source: source3", "Source: source4", "process1"),
					ArbitraryInputStreamTask.class,
					streamGraph);
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     Source1 -> (rescale) -> Map1 -> Filter1 -> -+
		 *                              |                  | -> (union1) ->                  -+
		 *                              |_  -> Filter2 -> -+                                  | -> Process2 -> Map2
		 *                              |                  | -> (union) -> Process1 (dam) -> -+
		 *                              |_ ->             -+
		 * </pre>
		 */
		@Test
		public void testUnionAndTriangleTopology() throws Exception {
			if (this.readOrder == ReadOrder.INPUT2_FIRST) {
				return;
			}

			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> map1 = source1.rescale().map(new NoOpMapFunction()).name("map1");
			DataStream<String> filter1 = map1.filter(new NoOpFilterFunction()).name("filter1");
			DataStream<String> filter2 = map1.filter(new NoOpFilterFunction()).name("filter2");

			DataStream<String> process1 = filter2.union(map1).transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process2 = filter1.union(filter2).connect(process1)
					.process(new NoOpCoProcessFuntion()).name("process2");
			process2.map(new NoOpMapFunction()).name("map2");

			if (this.readOrder != null) {
				((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(this.readOrder);
			}

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			if (readOrder != null) {
				assertEquals(3, jobGraph.getNumberOfVertices());

				int vertexIndex = 0;

				// CHAIN(Source1)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Collections.emptyList(),
						Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
						Sets.newHashSet("Source: source1"),
						Sets.newHashSet("Source: source1"),
						SourceStreamTaskV2.class, streamGraph);

				// CHAIN([Map1 -> (Filter1, Filter2), (Filter2, Map1) -> Process1)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
						Lists.newArrayList(Tuple2.of("filter1", "process2"), Tuple2.of("filter2", "process2"),
								Tuple2.of("process1", "process2")),
						Sets.newHashSet("map1", "filter1", "filter2", "process1"),
						Sets.newHashSet("map1"),
						ArbitraryInputStreamTask.class, streamGraph);

				// CHAIN(Process2 -> Map2)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("filter1", "process2"), Tuple2.of("filter2", "process2"),
								Tuple2.of("process1", "process2")),
						Collections.emptyList(),
						Sets.newHashSet("process2", "map2"),
						Sets.newHashSet("process2"),
						TwoInputStreamTask.class, streamGraph);
			} else {
				assertEquals(2, jobGraph.getNumberOfVertices());

				int vertexIndex = 0;

				// CHAIN(Source1)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Collections.emptyList(),
						Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
						Sets.newHashSet("Source: source1"),
						Sets.newHashSet("Source: source1"),
						SourceStreamTaskV2.class, streamGraph);

				// CHAIN([Map1 -> (Filter1, Filter2), (Filter2, Map1) -> Process1, (Filter1, Filter2, Process1) -> Process2, Process2 -> Map2)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("Source: source1", "map1")),
						Collections.emptyList(),
						Sets.newHashSet("map1", "filter1", "filter2", "process1", "process2", "map2"),
						Sets.newHashSet("map1"),
						ArbitraryInputStreamTask.class, streamGraph);
			}
		}
	}

	/**
	 * Tests for breaking off chains.
	 */
	@RunWith(Parameterized.class)
	public static class ParameterizedBreakingOffTest {
		private final int breakingOffType;

		@Parameterized.Parameters(name = "breakingOffType={0}")
		public static Collection<Integer> getParameters() {
			return Arrays.asList(new Integer[]{
				0, 1, 2, 3
			});
		}

		public ParameterizedBreakingOffTest(int breakOffChainType) {
			this.breakingOffType = breakOffChainType;
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     Source1 -> (rescale) -> Process1 (dam) -> (rescale|nop)     -+
		 *        |                                                         | ->  Process3 -> Sink1
		 *        |_  -> Map1 -> Filter1 -> (rescale|nop) ->               -+         |_ -> -+
		 *        |                                                                          | -> Process4 -> Sink2
		 *        |_ -> (rescale) -> Process2 (dam) ->                                      -+
		 * </pre>
		 */
		@Test
		public void testTwoRhombusesTopology1() throws Exception {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> process1 = source1.rescale().transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process2 = source1.rescale().transform(
					"operator2", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process2");
			((OneInputTransformation) process2.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> filter1 = source1.map(new NoOpMapFunction()).name("map1").filter(new NoOpFilterFunction()).name("filter1");

			DataStream<String> process3 = (breakingOffType == 1 || breakingOffType == 3 ? filter1.rescale() : filter1)
				.connect(breakingOffType == 2 || breakingOffType == 3 ? process1.rescale() : process1)
				.process(new NoOpCoProcessFuntion()).name("process3");
			((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

			DataStream<String> process4 = process3.connect(process2).process(new NoOpCoProcessFuntion()).name("process4");
			((TwoInputTransformation) process4.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

			process3.addSink(new NoOpSinkFunction()).name("sink1");
			process4.addSink(new NoOpSinkFunction()).name("sink2");

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(4, jobGraph.getNumberOfVertices());

			int vertexIndex = 0;

			// CHAIN([Source1 -> Map1, Map1 -> Filter1)
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("filter1", "process3"),
							Tuple2.of("Source: source1", "process1"), Tuple2.of("Source: source1", "process2")),
					Sets.newHashSet("Source: source1", "map1", "filter1"),
					Sets.newHashSet("Source: source1"),
					SourceStreamTaskV2.class,
					streamGraph);

			// CHAIN(process1)
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("Source: source1", "process1")),
					Lists.newArrayList(Tuple2.of("process1", "process3")),
					Sets.newHashSet("process1"),
					Sets.newHashSet("process1"),
					OneInputStreamTask.class,
					streamGraph);

			// CHAIN(process2)
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("Source: source1", "process2")),
					Lists.newArrayList(Tuple2.of("process2", "process4")),
					Sets.newHashSet("process2"),
					Sets.newHashSet("process2"),
					OneInputStreamTask.class,
					streamGraph);

			// CHAIN([Process3 -> (Sink1, Process4), Process4 -> Sink2])
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("process2", "process4"),
							Tuple2.of("filter1", "process3"), Tuple2.of("process1", "process3")),
					Collections.emptyList(),
					Sets.newHashSet("process3", "Sink: sink1", "process4", "Sink: sink2"),
					Sets.newHashSet("process3", "process4"),
					ArbitraryInputStreamTask.class, streamGraph);
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     Source1 -> (rescale) -> Process1(dam) -> (rescale|nop) ->       -+
		 *        |                                                             | -> Process3 -> Sink1
		 *        |_ -> Map1 ->              -+                                 |
		 *                                    | -> Process2 -> (rescale|nop) -> -+
		 *     Source2 ->                    -+
		 * </pre>
		 */
		@Test
		public void testMultiHeadRhombusTopology() throws Exception {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

			DataStream<String> process1 = source1.rescale().transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			DataStream<String> process2 = map1.connect(source2).process(new NoOpCoProcessFuntion()).name("process2");
			((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

			DataStream<String> process3 = (breakingOffType == 1 || breakingOffType == 3 ? process1.rescale() : process1)
				.connect(breakingOffType == 2 || breakingOffType == 3 ? process2.rescale() : process2)
				.process(new NoOpCoProcessFuntion()).name("process3");
			((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

			process3.addSink(new NoOpSinkFunction()).name("sink1");

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			assertEquals(3, jobGraph.getNumberOfVertices());

			int vertexIndex = 0;

			// CHAIN([Source1-> Map1, Map1 -> Process2, Source2 -> Process2])
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("process2", "process3"), Tuple2.of("Source: source1", "process1")),
					Sets.newHashSet("Source: source1", "map1", "Source: source2", "process2"),
					Sets.newHashSet("Source: source1", "Source: source2"),
					ArbitraryInputStreamTask.class, streamGraph);

			// CHAIN(Process1)
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("Source: source1", "process1")),
					Lists.newArrayList(Tuple2.of("process1", "process3")),
					Sets.newHashSet("process1"),
					Sets.newHashSet("process1"),
					OneInputStreamTask.class, streamGraph);

			// CHAIN(Process3 -> Sink1)
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("process1", "process3"), Tuple2.of("process2", "process3")),
					Collections.emptyList(),
					Sets.newHashSet("process3", "Sink: sink1"),
					Sets.newHashSet("process3"),
					TwoInputStreamTask.class, streamGraph);
		}

        /**
         * Tests the following topology.
         *
         * <p><pre>
         *     Source1 ->                                                                           -+
         *              |_ ->  Process1(dam) -> (rescale|nop)              -+                        |
         *                                                                  | -> Process3 -> Sink1   | -> Process4 ->Sink2
         *     Source2 ->  Map1 -> (rescale|nop) ->                        -+                        |
         *                  |_ -> Process2(dam)                                                     -+
         *                  |           |_ ->   -+
         *                                       | -> Process5 ->Sink3
         *                  |_ ->               -+
         *
         * </pre>
         */
		@Test
		public void testCrossAndTriangleTopology() throws Exception {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

			DataStream<String> process1 = source1.transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process1");
			((OneInputTransformation) process1.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> map1 = source2.map(new NoOpMapFunction()).name("map1");
			DataStream<String> process2 = map1.transform(
					"operator2", BasicTypeInfo.STRING_TYPE_INFO, new NoOpOneInputStreamOperator()).name("process2");
			((OneInputTransformation) process2.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);

			DataStream<String> process3 = (breakingOffType == 1 || breakingOffType == 3 ? process1.rescale() : process1)
					.connect(breakingOffType == 2 || breakingOffType == 3 ? map1.rescale() : map1)
					.process(new NoOpCoProcessFuntion()).name("process3");
			((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

			DataStream<String> process4 = source1.connect(process2).process(new NoOpCoProcessFuntion()).name("process4");
			((TwoInputTransformation) process4.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

			DataStream<String> process5 = process2.connect(map1).process(new NoOpCoProcessFuntion()).name("process5");
			((TwoInputTransformation) process5.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

			process3.addSink(new NoOpSinkFunction()).name("sink1");
			process4.addSink(new NoOpSinkFunction()).name("sink2");
			process5.addSink(new NoOpSinkFunction()).name("sink3");

			StreamGraph streamGraph = env.getStreamGraph();
			for (Tuple2<Integer, StreamOperator<?>> pair : streamGraph.getOperators()) {
				pair.f1.setChainingStrategy(ChainingStrategy.ALWAYS);
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();

			if (breakingOffType == 0 || breakingOffType == 1) {
				assertEquals(4, jobGraph.getNumberOfVertices());

				int vertexIndex = 0;

				// CHAIN(Source1 -> Process1)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Collections.emptyList(),
						Lists.newArrayList(Tuple2.of("process1", "process3"), Tuple2.of("Source: source1", "process4")),
						Sets.newHashSet("Source: source1", "process1"),
						Sets.newHashSet("Source: source1"),
						SourceStreamTaskV2.class,
						streamGraph);

				// CHAIN([Source2 -> Map1, Map1 -> (Process3, Process2), Process3 -> Sink1])
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("process1", "process3")),
						Lists.newArrayList(Tuple2.of("process2", "process4"), Tuple2.of("process2", "process5"),
								Tuple2.of("map1", "process5")),
						Sets.newHashSet("Source: source2", "map1", "process3", "process2", "Sink: sink1"),
						Sets.newHashSet("Source: source2", "process3"),
						ArbitraryInputStreamTask.class,
						streamGraph);

				// CHAIN(Process4 -> Sink2)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("Source: source1", "process4"), Tuple2.of("process2", "process4")),
						Collections.emptyList(),
						Sets.newHashSet("process4", "Sink: sink2"),
						Sets.newHashSet("process4"),
						TwoInputStreamTask.class,
						streamGraph);

				// CHAIN(Process5 -> Sink3)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("process2", "process5"), Tuple2.of("map1", "process5")),
						Collections.emptyList(),
						Sets.newHashSet("process5", "Sink: sink3"),
						Sets.newHashSet("process5"),
						TwoInputStreamTask.class,
						streamGraph);
			} else {
				assertEquals(4, jobGraph.getNumberOfVertices());

				int vertexIndex = 0;

				// CHAIN([Source2 -> Map1, Map1 -> Process2])
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Collections.emptyList(),
						Lists.newArrayList(Tuple2.of("map1", "process3"), Tuple2.of("process2", "process4"),
								Tuple2.of("process2", "process5"), Tuple2.of("map1", "process5")),
						Sets.newHashSet("Source: source2", "map1", "process2"),
						Sets.newHashSet("Source: source2"),
						SourceStreamTaskV2.class,
						streamGraph);

				// CHAIN([Source1 -> Process1, Source1 -> Process4, Process4 -> Sink2])
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("process2", "process4")),
						Lists.newArrayList(Tuple2.of("process1", "process3")),
						Sets.newHashSet("Source: source1", "process1", "process4", "Sink: sink2"),
						Sets.newHashSet("Source: source1", "process4"),
						ArbitraryInputStreamTask.class,
						streamGraph);

				// CHAIN(Process3 -> Sink1)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("process1", "process3"), Tuple2.of("map1", "process3")),
						Collections.emptyList(),
						Sets.newHashSet("process3", "Sink: sink1"),
						Sets.newHashSet("process3"),
						TwoInputStreamTask.class,
						streamGraph);

				// CHAIN(Process5 -> Sink3)
				verifyVertex(
						verticesSorted.get(vertexIndex++),
						TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
						Lists.newArrayList(Tuple2.of("process2", "process5"), Tuple2.of("map1", "process5")),
						Collections.emptyList(),
						Sets.newHashSet("process5", "Sink: sink3"),
						Sets.newHashSet("process5"),
						TwoInputStreamTask.class,
						streamGraph);
			}
		}

		/**
		 * Tests the following topology.
		 *
		 * <p><pre>
		 *     Source1 -> Map1 -> Map2 -> (rescale|nop) ->                     -+
		 *                |                                                     |
		 *                |_   -> Map3 -+                                       | -> Process2 -> Sink1
		 *                              | -> Process1(dam) -> (rescale|nop) -> -+
		 *     Source2 -> Map4         -+                                       | -> Process3 -> Sink2
		 *                 |_  -> (rescale|nop) ->                             -+
		 *                 |_  -> Map5 ->            -> Filter1 -> Sink3
		 * </pre>
		 */
		@Test
		public void testTwoRhombusesTopology2() throws Exception {
			StreamExecutionEnvironment env = createEnv();

			DataStream<String> source1 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source1");
			DataStream<String> source2 = env.addSourceV2(new NoOpSourceFunctionV2()).name("source2");

			DataStream<String> map1 = source1.map(new NoOpMapFunction()).name("map1");
			DataStream<String> map2 = map1.map(new NoOpMapFunction()).name("map2");
			DataStream<String> map3 = map1.map(new NoOpMapFunction()).name("map3");
			DataStream<String> map4 = source2.map(new NoOpMapFunction()).name("map4");

			DataStream<String> process1 = map3.connect(map4).transform(
					"operator1", BasicTypeInfo.STRING_TYPE_INFO, new NoOpTwoInputStreamOperator()).name("process1");
			TwoInputTransformation process1Transformation = ((TwoInputTransformation) process1.getTransformation());
			process1Transformation.setDamBehavior(DamBehavior.FULL_DAM);
			process1Transformation.setReadOrderHint(ReadOrder.INPUT2_FIRST);

			DataStream<String> process2 = (breakingOffType == 1 || breakingOffType == 3 ? map2.rescale() : map2)
					.connect(breakingOffType == 2 || breakingOffType == 3 ? process1.rescale() : process1)
					.process(new NoOpCoProcessFuntion()).name("process2");
			((TwoInputTransformation) process2.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

			DataStream<String> process3 = (breakingOffType == 1 || breakingOffType == 3 ? map4.rescale() : map4)
					.connect(breakingOffType == 2 || breakingOffType == 3 ? process1.rescale() : process1)
					.process(new NoOpCoProcessFuntion()).name("process3");
			((TwoInputTransformation) process3.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

			process2.addSink(new NoOpSinkFunction()).name("sink1");
			process3.addSink(new NoOpSinkFunction()).name("sink2");
			map4.map(new NoOpMapFunction()).name("map5").filter(new NoOpFilterFunction()).name("filter1")
					.addSink(new NoOpSinkFunction()).name("sink3");

			StreamGraph streamGraph = env.getStreamGraph();
			for (StreamNode node : streamGraph.getStreamNodes()) {
				if ("filter2".equals(node.getOperatorName())) {
					node.getOperator().setChainingStrategy(ChainingStrategy.HEAD);
				} else {
					node.getOperator().setChainingStrategy(ChainingStrategy.ALWAYS);
				}
			}
			JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
			List<JobVertex> verticesSorted = jobGraph.getVerticesSortedTopologicallyFromSources();
			assertEquals(3, jobGraph.getNumberOfVertices());

			int vertexIndex = 0;

			// CHAIN(...)
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Collections.emptyList(),
					Lists.newArrayList(Tuple2.of("map2", "process2"), Tuple2.of("process1", "process2"),
							Tuple2.of("map4", "process3"), Tuple2.of("process1", "process3")),
					Sets.newHashSet("Source: source1", "map1", "map2", "map3", "Source: source2", "map4", "process1",
							"map5", "filter1", "Sink: sink3"),
					Sets.newHashSet("Source: source1", "Source: source2"),
					ArbitraryInputStreamTask.class,
					streamGraph);

			// CHAIN(Process2 -> Sink1)
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("map2", "process2"), Tuple2.of("process1", "process2")),
					Collections.emptyList(),
					Sets.newHashSet("process2", "Sink: sink1"),
					Sets.newHashSet("process2"),
					TwoInputStreamTask.class,
					streamGraph);

			// CHAIN(Process3 -> Sink2)
			verifyVertex(
					verticesSorted.get(vertexIndex++),
					TimeCharacteristic.IngestionTime, true, CheckpointingMode.EXACTLY_ONCE, FsStateBackend.class,
					Lists.newArrayList(Tuple2.of("map4", "process3"), Tuple2.of("process1", "process3")),
					Collections.emptyList(),
					Sets.newHashSet("process3", "Sink: sink2"),
					Sets.newHashSet("process3"),
					TwoInputStreamTask.class,
					streamGraph);
		}
	}

	private static StreamExecutionEnvironment createEnv() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
		env.enableCheckpointing(10, CheckpointingMode.EXACTLY_ONCE);
		env.setStateBackend((StateBackend) new FsStateBackend(GeneratingMultiHeadChainJobGraphTest.class.getResource("/").toURI()));
		env.setMultiHeadChainMode(true);

		return env;
	}

	// ------------------------------------------------------------------------
	//  Test Utilities
	// ------------------------------------------------------------------------

	private static class NoOpSourceFunction implements ParallelSourceFunction<String> {

		private static final long serialVersionUID = 1L;

		@Override
		public void run(SourceContext<String> ctx) throws Exception {
		}

		@Override
		public void cancel() {
		}
	}

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

	private static class NoOpProcessFuntion extends ProcessFunction<String, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

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

	private static class NoOpFilterFunction implements FilterFunction<String> {

		private static final long serialVersionUID = 1L;

		@Override
		public boolean filter(String value) throws Exception {
			return true;
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
