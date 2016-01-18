/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.EvenOddOutputSelector;
import org.apache.flink.streaming.util.NoOpIntMap;
import org.apache.flink.streaming.util.NoOpSink;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link StreamGraphGenerator}. This only tests correct translation of split/select,
 * union, partitioning since the other translation routines are tested already in operation
 * specific tests, for example in {@link org.apache.flink.streaming.api.IterateTest} for
 * iterations.
 */
public class StreamGraphGeneratorTest {

	/**
	 * This tests whether virtual Transformations behave correctly.
	 *
	 * <p>
	 * Verifies that partitioning, output selector, selected names are correctly set in the
	 * StreamGraph when they are intermixed.
	 */
	@Test
	public void testVirtualTransformations() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source = env.fromElements(1, 10);

		DataStream<Integer> rebalanceMap = source.rebalance().map(new NoOpIntMap());

		// verify that only the partitioning that was set last is used
		DataStream<Integer> broadcastMap = rebalanceMap
				.forward()
				.global()
				.broadcast()
				.map(new NoOpIntMap());

		broadcastMap.addSink(new NoOpSink<Integer>());

		// verify that partitioning is preserved across union and split/select
		EvenOddOutputSelector selector1 = new EvenOddOutputSelector();
		EvenOddOutputSelector selector2 = new EvenOddOutputSelector();
		EvenOddOutputSelector selector3 = new EvenOddOutputSelector();

		DataStream<Integer> map1Operator = rebalanceMap
				.map(new NoOpIntMap());

		DataStream<Integer> map1 = map1Operator
				.broadcast()
				.split(selector1)
				.select("even");

		DataStream<Integer> map2Operator = rebalanceMap
				.map(new NoOpIntMap());

		DataStream<Integer> map2 = map2Operator
				.split(selector2)
				.select("odd")
				.global();

		DataStream<Integer> map3Operator = rebalanceMap
				.map(new NoOpIntMap());

		DataStream<Integer> map3 = map3Operator
				.global()
				.split(selector3)
				.select("even")
				.shuffle();


		SingleOutputStreamOperator<Integer, ?> unionedMap = map1.union(map2).union(map3)
				.map(new NoOpIntMap());

		unionedMap.addSink(new NoOpSink<Integer>());

		StreamGraph graph = env.getStreamGraph();

		// rebalanceMap
		assertTrue(graph.getStreamNode(rebalanceMap.getId()).getInEdges().get(0).getPartitioner() instanceof RebalancePartitioner);

		// verify that only last partitioning takes precedence
		assertTrue(graph.getStreamNode(broadcastMap.getId()).getInEdges().get(0).getPartitioner() instanceof BroadcastPartitioner);
		assertEquals(rebalanceMap.getId(), graph.getStreamNode(broadcastMap.getId()).getInEdges().get(0).getSourceVertex().getId());

		// verify that partitioning in unions is preserved and that it works across split/select
		assertTrue(graph.getStreamNode(map1Operator.getId()).getOutEdges().get(0).getPartitioner() instanceof BroadcastPartitioner);
		assertTrue(graph.getStreamNode(map1Operator.getId()).getOutEdges().get(0).getSelectedNames().get(0).equals("even"));
		assertTrue(graph.getStreamNode(map1Operator.getId()).getOutputSelectors().contains(selector1));

		assertTrue(graph.getStreamNode(map2Operator.getId()).getOutEdges().get(0).getPartitioner() instanceof GlobalPartitioner);
		assertTrue(graph.getStreamNode(map2Operator.getId()).getOutEdges().get(0).getSelectedNames().get(0).equals("odd"));
		assertTrue(graph.getStreamNode(map2Operator.getId()).getOutputSelectors().contains(selector2));

		assertTrue(graph.getStreamNode(map3Operator.getId()).getOutEdges().get(0).getPartitioner() instanceof ShufflePartitioner);
		assertTrue(graph.getStreamNode(map3Operator.getId()).getOutEdges().get(0).getSelectedNames().get(0).equals("even"));
		assertTrue(graph.getStreamNode(map3Operator.getId()).getOutputSelectors().contains(selector3));
	}

	/**
	 * This tests whether virtual Transformations behave correctly.
	 *
	 * Checks whether output selector, partitioning works correctly when applied on a union.
	 */
	@Test
	public void testVirtualTransformations2() throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source = env.fromElements(1, 10);

		DataStream<Integer> rebalanceMap = source.rebalance().map(new NoOpIntMap());

		DataStream<Integer> map1 = rebalanceMap
				.map(new NoOpIntMap());

		DataStream<Integer> map2 = rebalanceMap
				.map(new NoOpIntMap());

		DataStream<Integer> map3 = rebalanceMap
				.map(new NoOpIntMap());

		EvenOddOutputSelector selector = new EvenOddOutputSelector();

		SingleOutputStreamOperator<Integer, ?> unionedMap = map1.union(map2).union(map3)
				.broadcast()
				.split(selector)
				.select("foo")
				.map(new NoOpIntMap());

		unionedMap.addSink(new NoOpSink<Integer>());

		StreamGraph graph = env.getStreamGraph();

		// verify that the properties are correctly set on all input operators
		assertTrue(graph.getStreamNode(map1.getId()).getOutEdges().get(0).getPartitioner() instanceof BroadcastPartitioner);
		assertTrue(graph.getStreamNode(map1.getId()).getOutEdges().get(0).getSelectedNames().get(0).equals("foo"));
		assertTrue(graph.getStreamNode(map1.getId()).getOutputSelectors().contains(selector));

		assertTrue(graph.getStreamNode(map2.getId()).getOutEdges().get(0).getPartitioner() instanceof BroadcastPartitioner);
		assertTrue(graph.getStreamNode(map2.getId()).getOutEdges().get(0).getSelectedNames().get(0).equals("foo"));
		assertTrue(graph.getStreamNode(map2.getId()).getOutputSelectors().contains(selector));

		assertTrue(graph.getStreamNode(map3.getId()).getOutEdges().get(0).getPartitioner() instanceof BroadcastPartitioner);
		assertTrue(graph.getStreamNode(map3.getId()).getOutEdges().get(0).getSelectedNames().get(0).equals("foo"));
		assertTrue(graph.getStreamNode(map3.getId()).getOutputSelectors().contains(selector));

	}

	/**
	 * Test whether an {@link OutputTypeConfigurable} implementation gets called with the correct
	 * output type. In this test case the output type must be BasicTypeInfo.INT_TYPE_INFO.
	 *
	 * @throws Exception
	 */
	@Test
	public void testOutputTypeConfigurationWithOneInputTransformation() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source = env.fromElements(1, 10);

		OutputTypeConfigurableOperationWithOneInput outputTypeConfigurableOperation = new OutputTypeConfigurableOperationWithOneInput();

		DataStream<Integer> result = source.transform(
			"Single input and output type configurable operation",
			BasicTypeInfo.INT_TYPE_INFO,
			outputTypeConfigurableOperation);

		result.addSink(new NoOpSink<Integer>());

		StreamGraph graph = env.getStreamGraph();

		assertEquals(BasicTypeInfo.INT_TYPE_INFO, outputTypeConfigurableOperation.getTypeInformation());
	}

	@Test
	public void testOutputTypeConfigurationWithTwoInputTransformation() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source1 = env.fromElements(1, 10);
		DataStream<Integer> source2 = env.fromElements(2, 11);

		ConnectedStreams<Integer, Integer> connectedSource = source1.connect(source2);

		OutputTypeConfigurableOperationWithTwoInputs outputTypeConfigurableOperation = new OutputTypeConfigurableOperationWithTwoInputs();

		DataStream<Integer> result = connectedSource.transform(
				"Two input and output type configurable operation",
				BasicTypeInfo.INT_TYPE_INFO,
				outputTypeConfigurableOperation);

		result.addSink(new NoOpSink<Integer>());

		StreamGraph graph = env.getStreamGraph();

		assertEquals(BasicTypeInfo.INT_TYPE_INFO, outputTypeConfigurableOperation.getTypeInformation());
	}

	private static class OutputTypeConfigurableOperationWithTwoInputs
			extends AbstractStreamOperator<Integer>
			implements TwoInputStreamOperator<Integer, Integer, Integer>, OutputTypeConfigurable<Integer> {

		TypeInformation<Integer> tpeInformation;

		public TypeInformation<Integer> getTypeInformation() {
			return tpeInformation;
		}

		@Override
		public void setOutputType(TypeInformation<Integer> outTypeInfo, ExecutionConfig executionConfig) {
			tpeInformation = outTypeInfo;
		}

		@Override
		public void processElement1(StreamRecord element) throws Exception {
			output.collect(element);
		}

		@Override
		public void processElement2(StreamRecord element) throws Exception {
			output.collect(element);
		}

		@Override
		public void processWatermark1(Watermark mark) throws Exception {}

		@Override
		public void processWatermark2(Watermark mark) throws Exception {}

		@Override
		public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Integer>> output) {}
	}

	private static class OutputTypeConfigurableOperationWithOneInput
			extends AbstractStreamOperator<Integer>
			implements OneInputStreamOperator<Integer, Integer>, OutputTypeConfigurable<Integer> {

		TypeInformation<Integer> tpeInformation;

		public TypeInformation<Integer> getTypeInformation() {
			return tpeInformation;
		}

		@Override
		public void processElement(StreamRecord<Integer> element) {
			output.collect(element);
		}

		@Override
		public void processWatermark(Watermark mark) {}

		@Override
		public void setOutputType(TypeInformation<Integer> outTypeInfo, ExecutionConfig executionConfig) {
			tpeInformation = outTypeInfo;
		}
	}

}
