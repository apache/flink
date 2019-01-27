/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.operators.DamBehavior;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironmentFactory;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.TwoInputSelection;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.SideOutputTransformation;
import org.apache.flink.streaming.api.transformations.StreamTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.RescalePartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.EvenOddOutputSelector;
import org.apache.flink.streaming.util.NoOpIntMap;
import org.apache.flink.util.OutputTag;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.streaming.api.graph.StreamNode.ReadPriority;
import static org.apache.flink.streaming.api.transformations.TwoInputTransformation.ReadOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.mockito.PowerMockito.when;

/**
 * Tests for {@link StreamGraphGenerator}. This only tests correct translation of split/select,
 * union, partitioning since the other translation routines are tested already in operation
 * specific tests.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(GlobalConfiguration.class)
public class StreamGraphGeneratorTest {

	@Test
	public void testBufferTimeout() {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		env.setBufferTimeout(77); // set timeout to some recognizable number

		env
			.fromElements(1, 2, 3, 4, 5)

			.map(value -> value)
				.setBufferTimeout(-1)
				.name("A")
			.map(value -> value)
				.setBufferTimeout(0)
				.name("B")
			.map(value -> value)
				.setBufferTimeout(12)
				.name("C")
			.map(value -> value)
				.name("D");

		final StreamGraph sg = env.getStreamGraph();
		for (StreamNode node : sg.getStreamNodes()) {
			switch (node.getOperatorName()) {

				case "A":
					assertEquals(77L, node.getBufferTimeout().longValue());
					break;
				case "B":
					assertEquals(0L, node.getBufferTimeout().longValue());
					break;
				case "C":
					assertEquals(12L, node.getBufferTimeout().longValue());
					break;
				case "D":
					assertEquals(77L, node.getBufferTimeout().longValue());
					break;
				default:
					assertTrue(node.getOperator() instanceof StreamSource);
			}
		}
	}

	/**
	 * This tests whether virtual Transformations behave correctly.
	 *
	 * <p>Verifies that partitioning, output selector, selected names are correctly set in the
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

		broadcastMap.addSink(new DiscardingSink<Integer>());

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

		SingleOutputStreamOperator<Integer> unionedMap = map1.union(map2).union(map3)
				.map(new NoOpIntMap());

		unionedMap.addSink(new DiscardingSink<Integer>());

		StreamGraph graph = env.getStreamGraph();

		// rebalanceMap
		assertTrue(graph.getStreamNode(rebalanceMap.getId()).getInEdges().get(0).getPartitioner() instanceof RebalancePartitioner);

		// verify that only last partitioning takes precedence
		assertTrue(graph.getStreamNode(broadcastMap.getId()).getInEdges().get(0).getPartitioner() instanceof BroadcastPartitioner);
		assertEquals(rebalanceMap.getId(), graph.getStreamNode(broadcastMap.getId()).getInEdges().get(0).getSourceId());

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
	 * <p>Checks whether output selector, partitioning works correctly when applied on a union.
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

		SingleOutputStreamOperator<Integer> unionedMap = map1.union(map2).union(map3)
				.broadcast()
				.split(selector)
				.select("foo")
				.map(new NoOpIntMap());

		unionedMap.addSink(new DiscardingSink<Integer>());

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

		result.addSink(new DiscardingSink<Integer>());

		env.getStreamGraph();

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

		result.addSink(new DiscardingSink<Integer>());

		env.getStreamGraph();

		assertEquals(BasicTypeInfo.INT_TYPE_INFO, outputTypeConfigurableOperation.getTypeInformation());
	}

	/**
	 * Tests that the KeyGroupStreamPartitioner are properly set up with the correct value of
	 * maximum parallelism.
	 */
	@Test
	public void testSetupOfKeyGroupPartitioner() {
		int maxParallelism = 42;
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setMaxParallelism(maxParallelism);

		DataStream<Integer> source = env.fromElements(1, 2, 3);

		DataStream<Integer> keyedResult = source.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 9205556348021992189L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}).map(new NoOpIntMap());

		keyedResult.addSink(new DiscardingSink<Integer>());

		StreamGraph graph = env.getStreamGraph();

		StreamNode keyedResultNode = graph.getStreamNode(keyedResult.getId());

		StreamPartitioner<?> streamPartitioner = keyedResultNode.getInEdges().get(0).getPartitioner();
	}

	/**
	 * Tests that the global and operator-wide max parallelism setting is respected.
	 */
	@Test
	public void testMaxParallelismForwarding() {
		int globalMaxParallelism = 42;
		int keyedResult2MaxParallelism = 17;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.getConfig().setMaxParallelism(globalMaxParallelism);

		DataStream<Integer> source = env.fromElements(1, 2, 3);

		DataStream<Integer> keyedResult1 = source.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 9205556348021992189L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}).map(new NoOpIntMap());

		DataStream<Integer> keyedResult2 = keyedResult1.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1250168178707154838L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}).map(new NoOpIntMap()).setMaxParallelism(keyedResult2MaxParallelism);

		keyedResult2.addSink(new DiscardingSink<Integer>());

		StreamGraph graph = env.getStreamGraph();

		StreamNode keyedResult1Node = graph.getStreamNode(keyedResult1.getId());
		StreamNode keyedResult2Node = graph.getStreamNode(keyedResult2.getId());

		assertEquals(globalMaxParallelism, keyedResult1Node.getMaxParallelism());
		assertEquals(keyedResult2MaxParallelism, keyedResult2Node.getMaxParallelism());
	}

	/**
	 * Tests that the max parallelism is automatically set to the parallelism if it has not been
	 * specified.
	 */
	@Test
	public void testAutoMaxParallelism() {
		int globalParallelism = 42;
		int mapParallelism = 17;
		int maxParallelism = 21;
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(globalParallelism);

		DataStream<Integer> source = env.fromElements(1, 2, 3);

		DataStream<Integer> keyedResult1 = source.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 9205556348021992189L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}).map(new NoOpIntMap());

		DataStream<Integer> keyedResult2 = keyedResult1.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1250168178707154838L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}).map(new NoOpIntMap()).setParallelism(mapParallelism);

		DataStream<Integer> keyedResult3 = keyedResult2.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1250168178707154838L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}).map(new NoOpIntMap()).setMaxParallelism(maxParallelism);

		DataStream<Integer> keyedResult4 = keyedResult3.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1250168178707154838L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		}).map(new NoOpIntMap()).setMaxParallelism(maxParallelism).setParallelism(mapParallelism);

		keyedResult4.addSink(new DiscardingSink<Integer>());

		StreamGraph graph = env.getStreamGraph();

		StreamNode keyedResult3Node = graph.getStreamNode(keyedResult3.getId());
		StreamNode keyedResult4Node = graph.getStreamNode(keyedResult4.getId());

		assertEquals(maxParallelism, keyedResult3Node.getMaxParallelism());
		assertEquals(maxParallelism, keyedResult4Node.getMaxParallelism());
	}

	/**
	 * Tests that the max parallelism is properly set for connected
	 * streams.
	 */
	@Test
	public void testMaxParallelismWithConnectedKeyedStream() {
		int maxParallelism = 42;

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Integer> input1 = env.fromElements(1, 2, 3, 4).setMaxParallelism(128);
		DataStream<Integer> input2 = env.fromElements(1, 2, 3, 4).setMaxParallelism(129);

		env.getConfig().setMaxParallelism(maxParallelism);

		DataStream<Integer> keyedResult = input1.connect(input2).keyBy(
			new KeySelector<Integer, Integer>() {
				private static final long serialVersionUID = -6908614081449363419L;

				@Override
				public Integer getKey(Integer value) throws Exception {
					return value;
				}
			},
			new KeySelector<Integer, Integer>() {
				private static final long serialVersionUID = 3195683453223164931L;

				@Override
				public Integer getKey(Integer value) throws Exception {
					return value;
				}
			}).map(new NoOpIntCoMap());

		keyedResult.addSink(new DiscardingSink<Integer>());

		StreamGraph graph = env.getStreamGraph();

		StreamNode keyedResultNode = graph.getStreamNode(keyedResult.getId());

		StreamPartitioner<?> streamPartitioner1 = keyedResultNode.getInEdges().get(0).getPartitioner();
		StreamPartitioner<?> streamPartitioner2 = keyedResultNode.getInEdges().get(1).getPartitioner();
	}

	/**
	 * Tests that the json generated by JSONGenerator shall meet with 2 requirements:
	 * 1. sink nodes are at the back
	 * 2. if both two nodes are sink nodes or neither of them is sink node, then sort by its id.
	 */
	@Test
	public void testSinkIdComparison() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Integer> source = env.fromElements(1, 2, 3);
		for (int i = 0; i < 32; i++) {
			if (i % 2 == 0) {
				source.addSink(new SinkFunction<Integer>() {
					@Override
					public void invoke(Integer value) throws Exception {}
				});
			} else {
				source.map(x -> x + 1);
			}
		}
		// IllegalArgumentException will be thrown without FLINK-9216
		env.getStreamGraph().getStreamingPlanAsJSON();
	}

	@Test
	public void testDefaultDataPartitionerType() throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		DataStream<Integer> map = env.fromElements(1, 10)
			.map(new NoOpIntMap()).setParallelism(2);

		StreamGraph graph = env.getStreamGraph();
		assertTrue(graph.getStreamNode(map.getId()).getInEdges().get(0).getPartitioner() instanceof RebalancePartitioner);

		// set the default partitioner type to RESCALE
		PowerMockito.mockStatic(GlobalConfiguration.class);
		Configuration configuration = new Configuration();
		configuration.setString(CoreOptions.DEFAULT_PARTITIONER, DataPartitionerType.RESCALE.toString());
		when(GlobalConfiguration.loadConfiguration()).thenReturn(configuration);

		graph = env.getStreamGraph();
		assertTrue(graph.getStreamNode(map.getId()).getInEdges().get(0).getPartitioner() instanceof RescalePartitioner);
	}

	/**
	 * Tests that the custom configuration is properly set.
	 */
	@Test
	public void testCustomConfiguration() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		final String jobConfigKey = "job:test-key";
		final String jobConfigValue = "job:test-value";

		final String operatorConfigKey1 = "op:test-key1";
		final String operatorConfigValue1 = "op:test-value1";
		final ConfigOption operatorConfigKey2 = ConfigOptions.key("op:test-key2").noDefaultValue();
		final String operatorConfigValue2 = "op:test-value2";

		env.getCustomConfiguration().setString(jobConfigKey, jobConfigValue);

		DataStream<Integer> source = env.fromElements(1, 2, 3)
			.setConfigItem(operatorConfigKey1, operatorConfigValue1);

		DataStream<Integer> keyedResult = source.keyBy(new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 9205556348021992189L;

			@Override
			public Integer getKey(Integer value) {
				return value;
			}
		}).map(new NoOpIntMap()).name("map1");

		DataStreamSink<Integer> sink = keyedResult.addSink(new DiscardingSink<>())
			.setConfigItem(operatorConfigKey2, operatorConfigValue2);

		StreamGraph streamGraph = env.getStreamGraph();
		Map<String, StreamNode> streamNodeMap = new HashMap<>();
		for (StreamNode node : streamGraph.getStreamNodes()) {
			streamNodeMap.put(node.getOperatorName(), node);
		}

		Configuration customConfiguration = streamGraph.getCustomConfiguration();
		assertTrue(customConfiguration.containsKey(jobConfigKey));
		assertEquals(jobConfigValue, customConfiguration.getString(jobConfigKey, null));

		{
			StreamNode sourceNode = streamGraph.getStreamNode(source.getId());
			Configuration sourceConfiguration = sourceNode.getCustomConfiguration();
			assertTrue(sourceConfiguration.containsKey(operatorConfigKey1));
			assertEquals(operatorConfigValue1, sourceConfiguration.getString(operatorConfigKey1, null));
		}

		{
			StreamNode map1Node = streamNodeMap.get("map1");
			Configuration map1Configuration = map1Node.getCustomConfiguration();
			assertEquals(0, map1Configuration.keySet().size());
		}

		{
			StreamNode sinkNode = streamGraph.getStreamNode(sink.getId());
			Configuration sinkConfiguration = sinkNode.getCustomConfiguration();
			assertTrue(sinkConfiguration.contains(operatorConfigKey2));
			assertEquals(operatorConfigValue2, sinkConfiguration.getString(operatorConfigKey2, null));
		}
	}

	@Test
	public void testDamBehavior() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source = env.fromElements(1, 10);

		DataStream<Integer> map1 = source.map(new NoOpIntMap());
		((OneInputTransformation) map1.getTransformation()).setDamBehavior(DamBehavior.MATERIALIZING);
		DataStream<Integer> filter1 = map1.filter(new NoOpIntFilter());

		DataStream<Integer> sideOutput = ((SingleOutputStreamOperator<Integer>) map1)
				.getSideOutput(new OutputTag<Integer>("test tag"){});
		((SideOutputTransformation) sideOutput.getTransformation()).setDamBehavior(DamBehavior.FULL_DAM);
		DataStream<Integer> filter2 = sideOutput.filter(new NoOpIntFilter());

		DataStream<Integer> map2 = filter1.connect(filter2).map(new NoOpIntCoMap());
		((TwoInputTransformation) map2.getTransformation()).setDamBehavior(DamBehavior.MATERIALIZING);
		SplitStream<Integer> spit1 = map2.split((o) -> null);
		DataStreamSink<Integer> sink1 = spit1.select("select1").addSink(new DiscardingSink<>());
		DataStreamSink<Integer> sink2 = spit1.select("select2").addSink(new DiscardingSink<>());

		StreamGraph graph = env.getStreamGraph();

		assertEquals(DamBehavior.PIPELINED, graph.getStreamEdges(source.getId(), map1.getId()).get(0).getDamBehavior());
		assertEquals(DamBehavior.MATERIALIZING, graph.getStreamEdges(map1.getId(), filter1.getId()).get(0).getDamBehavior());
		assertEquals(DamBehavior.FULL_DAM, graph.getStreamEdges(map1.getId(), filter2.getId()).get(0).getDamBehavior());
		assertEquals(DamBehavior.MATERIALIZING, graph.getStreamEdges(map2.getId(), sink1.getId()).get(0).getDamBehavior());
		assertEquals(DamBehavior.MATERIALIZING, graph.getStreamEdges(map2.getId(), sink1.getId()).get(0).getDamBehavior());
	}

	@Test
	public void testReadPriorityHint() {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<Integer> source1 = env.fromElements(1, 10);
		DataStream<Integer> source2 = env.fromElements(10, 20);

		DataStream<Integer> map1 = source1.union(source2).map(new NoOpIntMap());
		DataStream<Integer> filter1 = source2.filter(new NoOpIntFilter());

		DataStream<Integer> map2 = map1.rescale().union(source2.rescale()).connect(filter1.rescale()).map(new NoOpIntCoMap());
		((TwoInputTransformation) map2.getTransformation()).setReadOrderHint(ReadOrder.INPUT2_FIRST);

		DataStream<Integer> map3 = source1.union(map1).connect(map2).map(new NoOpIntCoMap());
		((TwoInputTransformation) map3.getTransformation()).setReadOrderHint(ReadOrder.INPUT1_FIRST);

		DataStream<Integer> map4 = map3.connect(map2).map(new NoOpIntCoMap());
		((TwoInputTransformation) map4.getTransformation()).setReadOrderHint(ReadOrder.SPECIAL_ORDER);

		DataStream<Integer> map5 = map4.connect(filter1).map(new NoOpIntCoMap());
		DataStreamSink<Integer> sink1 = map5.addSink(new DiscardingSink<>());

		StreamGraph graph = env.getStreamGraph();

		StreamNode map1Node = graph.getStreamNode(map1.getId());
		assertNull(map1Node.getReadPriorityHint(graph.getStreamEdges(source1.getId(), map1.getId()).get(0)));
		assertNull(map1Node.getReadPriorityHint(graph.getStreamEdges(source2.getId(), map1.getId()).get(0)));

		StreamNode filter1Node = graph.getStreamNode(filter1.getId());
		assertNull(filter1Node.getReadPriorityHint(graph.getStreamEdges(source2.getId(), filter1.getId()).get(0)));

		StreamNode map2Node = graph.getStreamNode(map2.getId());
		assertEquals(ReadPriority.LOWER, map2Node.getReadPriorityHint(graph.getStreamEdges(map1.getId(), map2.getId()).get(0)));
		assertEquals(ReadPriority.LOWER, map2Node.getReadPriorityHint(graph.getStreamEdges(source2.getId(), map2.getId()).get(0)));
		assertEquals(ReadPriority.HIGHER, map2Node.getReadPriorityHint(graph.getStreamEdges(filter1.getId(), map2.getId()).get(0)));

		StreamNode map3Node = graph.getStreamNode(map3.getId());
		assertEquals(ReadPriority.HIGHER, map3Node.getReadPriorityHint(graph.getStreamEdges(source1.getId(), map3.getId()).get(0)));
		assertEquals(ReadPriority.HIGHER, map3Node.getReadPriorityHint(graph.getStreamEdges(map1.getId(), map3.getId()).get(0)));
		assertEquals(ReadPriority.LOWER, map3Node.getReadPriorityHint(graph.getStreamEdges(map2.getId(), map3.getId()).get(0)));

		StreamNode map4Node = graph.getStreamNode(map4.getId());
		assertEquals(ReadPriority.DYNAMIC, map4Node.getReadPriorityHint(graph.getStreamEdges(map3.getId(), map4.getId()).get(0)));
		assertEquals(ReadPriority.DYNAMIC, map4Node.getReadPriorityHint(graph.getStreamEdges(map2.getId(), map4.getId()).get(0)));

		StreamNode map5Node = graph.getStreamNode(map5.getId());
		assertNull(map5Node.getReadPriorityHint(graph.getStreamEdges(map4.getId(), map5.getId()).get(0)));
		assertNull(map5Node.getReadPriorityHint(graph.getStreamEdges(filter1.getId(), map5.getId()).get(0)));

		StreamNode sink1Node = graph.getStreamNode(sink1.getId());
		assertNull(sink1Node.getReadPriorityHint(graph.getStreamEdges(map5.getId(), sink1.getId()).get(0)));
	}

	@Test
	public void testSlotSharingEnabled() {
		// case: unbounded data stream
		{
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> filter1 = source1.filter(new NoOpIntFilter()).slotSharingGroup("slotSharingGroup1");
			DataStream<Integer> filter2 = source1.filter(new NoOpIntFilter()).slotSharingGroup("slotSharingGroup1");
			DataStream<Integer> process1 = filter1.connect(filter2).map(new NoOpIntCoMap());
			DataStream<Integer> map1 = process1.map(new NoOpIntMap()).slotSharingGroup("slotSharingGroup2");

			IterativeStream<Integer> iter1 = map1.iterate();
			DataStream<Integer> map2 = iter1.map(new NoOpIntMap());
			iter1.closeWith(map2);

			DataStreamSink<Integer> sink1 = map2.addSink(new NoOpSinkFunction());

			StreamGraph graph = env.getStreamGraph();
			assertEquals("default", graph.getStreamNode(source1.getId()).getSlotSharingGroup());
			assertEquals("slotSharingGroup1", graph.getStreamNode(filter1.getId()).getSlotSharingGroup());
			assertEquals("slotSharingGroup1", graph.getStreamNode(filter2.getId()).getSlotSharingGroup());
			assertEquals("slotSharingGroup1", graph.getStreamNode(process1.getId()).getSlotSharingGroup());
			assertEquals("slotSharingGroup2", graph.getStreamNode(map1.getId()).getSlotSharingGroup());
			assertEquals("default", graph.getStreamNode(map2.getId()).getSlotSharingGroup());
			assertEquals("default", graph.getStreamNode(sink1.getId()).getSlotSharingGroup());

			for (Tuple2<StreamNode, StreamNode> iterPair : graph.getIterationSourceSinkPairs()) {
				assertEquals("default", iterPair.f0.getSlotSharingGroup());
				assertEquals("default", iterPair.f1.getSlotSharingGroup());
			}
		}

		// case: bounded data stream
		{
			TestStreamEnvironment.setAsContext();

			TestStreamEnvironment env = (TestStreamEnvironment) StreamExecutionEnvironment.getExecutionEnvironment();

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> filter1 = source1.filter(new NoOpIntFilter()).slotSharingGroup("slotSharingGroup1");
			DataStream<Integer> filter2 = source1.filter(new NoOpIntFilter()).slotSharingGroup("slotSharingGroup1");
			DataStream<Integer> process1 = filter1.connect(filter2).map(new NoOpIntCoMap());
			DataStream<Integer> map1 = process1.map(new NoOpIntMap()).slotSharingGroup("slotSharingGroup2");
			DataStreamSink<Integer> sink1 = map1.addSink(new NoOpSinkFunction());

			StreamGraph graph = StreamGraphGenerator.generate(StreamGraphGenerator.Context.buildBatchProperties(env),
					env.getTransformations());
			assertEquals("default", graph.getStreamNode(source1.getId()).getSlotSharingGroup());
			assertEquals("slotSharingGroup1", graph.getStreamNode(filter1.getId()).getSlotSharingGroup());
			assertEquals("slotSharingGroup1", graph.getStreamNode(filter2.getId()).getSlotSharingGroup());
			assertEquals("slotSharingGroup1", graph.getStreamNode(process1.getId()).getSlotSharingGroup());
			assertEquals("slotSharingGroup2", graph.getStreamNode(map1.getId()).getSlotSharingGroup());
			assertEquals("slotSharingGroup2", graph.getStreamNode(sink1.getId()).getSlotSharingGroup());
		}
	}

	@Test
	public void testSlotSharingDisabled() {
		// case: unbounded data stream
		{
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.disableSlotSharing();

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> filter1 = source1.filter(new NoOpIntFilter());
			DataStream<Integer> filter2 = source1.filter(new NoOpIntFilter());
			DataStream<Integer> process1 = filter1.connect(filter2).map(new NoOpIntCoMap());
			DataStream<Integer> map1 = process1.map(new NoOpIntMap());

			IterativeStream<Integer> iter1 = map1.iterate();
			DataStream<Integer> map2 = iter1.map(new NoOpIntMap());
			iter1.closeWith(map2);

			DataStreamSink<Integer> sink1 = map2.addSink(new NoOpSinkFunction());

			StreamGraph graph = env.getStreamGraph();
			assertNull(graph.getStreamNode(source1.getId()).getSlotSharingGroup());
			assertNull(graph.getStreamNode(filter1.getId()).getSlotSharingGroup());
			assertNull(graph.getStreamNode(filter2.getId()).getSlotSharingGroup());
			assertNull(graph.getStreamNode(process1.getId()).getSlotSharingGroup());
			assertNull(graph.getStreamNode(map1.getId()).getSlotSharingGroup());
			assertNull(graph.getStreamNode(map2.getId()).getSlotSharingGroup());
			assertNull(graph.getStreamNode(sink1.getId()).getSlotSharingGroup());

			for (Tuple2<StreamNode, StreamNode> iterPair : graph.getIterationSourceSinkPairs()) {
				assertEquals("default", iterPair.f0.getSlotSharingGroup());
				assertEquals("default", iterPair.f1.getSlotSharingGroup());
			}
		}

		// case: bounded data stream
		{
			TestStreamEnvironment.setAsContext();

			TestStreamEnvironment env = (TestStreamEnvironment) StreamExecutionEnvironment.getExecutionEnvironment();
			env.disableSlotSharing();

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> filter1 = source1.filter(new NoOpIntFilter());
			DataStream<Integer> filter2 = source1.filter(new NoOpIntFilter());
			DataStream<Integer> process1 = filter1.connect(filter2).map(new NoOpIntCoMap());
			DataStreamSink<Integer> sink1 = process1.addSink(new NoOpSinkFunction());

			StreamGraph graph = StreamGraphGenerator.generate(StreamGraphGenerator.Context.buildBatchProperties(env),
					env.getTransformations());
			assertNull(graph.getStreamNode(source1.getId()).getSlotSharingGroup());
			assertNull(graph.getStreamNode(filter1.getId()).getSlotSharingGroup());
			assertNull(graph.getStreamNode(filter2.getId()).getSlotSharingGroup());
			assertNull(graph.getStreamNode(process1.getId()).getSlotSharingGroup());
			assertNull(graph.getStreamNode(sink1.getId()).getSlotSharingGroup());
		}
	}

	@Test
	public void testDefaultResourcesForUnboundedDataStream() {
		// case
		{
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> map1 = source1.map(new NoOpIntMap());
			DataStreamSink<Integer> sink1 = map1.addSink(new NoOpSinkFunction());

			StreamGraph graph = env.getStreamGraph();
			assertEquals(ResourceSpec.DEFAULT, graph.getStreamNode(source1.getId()).getMinResources());
			assertEquals(ResourceSpec.DEFAULT, graph.getStreamNode(map1.getId()).getMinResources());
			assertEquals(ResourceSpec.DEFAULT, graph.getStreamNode(sink1.getId()).getMinResources());
			for (StreamNode node : graph.getStreamNodes()) {
				assertEquals(node.getMinResources(), node.getPreferredResources());
			}
		}

		// case
		{
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			ResourceSpec defaultResources = new ResourceSpec.Builder().setCpuCores(12345).build();
			env.setDefaultResources(defaultResources);

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> map1 = source1.map(new NoOpIntMap());
			DataStreamSink<Integer> sink1 = map1.addSink(new NoOpSinkFunction());

			StreamGraph graph = env.getStreamGraph();
			assertEquals(defaultResources, graph.getStreamNode(source1.getId()).getMinResources());
			assertEquals(defaultResources, graph.getStreamNode(map1.getId()).getMinResources());
			assertEquals(defaultResources, graph.getStreamNode(sink1.getId()).getMinResources());
			for (StreamNode node : graph.getStreamNodes()) {
				assertEquals(node.getMinResources(), node.getPreferredResources());
			}
		}

		// case
		{
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

			Configuration globalConf = GlobalConfiguration.loadConfiguration();
			ResourceSpec globalDefaultResources = new ResourceSpec.Builder()
					.setCpuCores(globalConf.getDouble(CoreOptions.DEFAULT_RESOURCE_CPU_CORES))
					.setHeapMemoryInMB(globalConf.getInteger(CoreOptions.DEFAULT_RESOURCE_HEAP_MEMORY))
					.build();

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> map1 = source1.map(new NoOpIntMap());
			DataStreamSink<Integer> sink1 = map1.addSink(new NoOpSinkFunction());

			ResourceSpec map1Resources = new ResourceSpec.Builder().setCpuCores(9876).build();
			((SingleOutputStreamOperator<Integer>) map1).setResources(map1Resources);

			StreamGraph graph = env.getStreamGraph();
			assertEquals(globalDefaultResources, graph.getStreamNode(source1.getId()).getMinResources());
			assertEquals(map1Resources, graph.getStreamNode(map1.getId()).getMinResources());
			assertEquals(globalDefaultResources, graph.getStreamNode(sink1.getId()).getMinResources());
			for (StreamNode node : graph.getStreamNodes()) {
				assertEquals(node.getMinResources(), node.getPreferredResources());
			}
		}

		// case
		{
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			ResourceSpec defaultResources = new ResourceSpec.Builder().setCpuCores(12345).build();
			env.setDefaultResources(defaultResources);

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> map1 = source1.map(new NoOpIntMap());
			DataStreamSink<Integer> sink1 = map1.addSink(new NoOpSinkFunction());

			ResourceSpec map1Resources = new ResourceSpec.Builder().setCpuCores(9876).build();
			((SingleOutputStreamOperator<Integer>) map1).setResources(map1Resources);

			StreamGraph graph = env.getStreamGraph();
			assertEquals(defaultResources, graph.getStreamNode(source1.getId()).getMinResources());
			assertEquals(map1Resources, graph.getStreamNode(map1.getId()).getMinResources());
			assertEquals(defaultResources, graph.getStreamNode(sink1.getId()).getMinResources());
			for (StreamNode node : graph.getStreamNodes()) {
				assertEquals(node.getMinResources(), node.getPreferredResources());
			}
		}

		// case
		{
			StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
			env.setDefaultResources(ResourceSpec.DEFAULT);

			Configuration globalConf = GlobalConfiguration.loadConfiguration();
			ResourceSpec globalDefaultResources = new ResourceSpec.Builder()
					.setCpuCores(globalConf.getDouble(CoreOptions.DEFAULT_RESOURCE_CPU_CORES))
					.setHeapMemoryInMB(globalConf.getInteger(CoreOptions.DEFAULT_RESOURCE_HEAP_MEMORY))
					.build();

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> map1 = source1.map(new NoOpIntMap());
			DataStreamSink<Integer> sink1 = map1.addSink(new NoOpSinkFunction());

			ResourceSpec map1Resources = new ResourceSpec.Builder().setCpuCores(9876).build();
			((SingleOutputStreamOperator<Integer>) map1).setResources(map1Resources);

			StreamGraph graph = env.getStreamGraph();
			assertEquals(globalDefaultResources, graph.getStreamNode(source1.getId()).getMinResources());
			assertEquals(map1Resources, graph.getStreamNode(map1.getId()).getMinResources());
			assertEquals(globalDefaultResources, graph.getStreamNode(sink1.getId()).getMinResources());
			for (StreamNode node : graph.getStreamNodes()) {
				assertEquals(node.getMinResources(), node.getPreferredResources());
			}
		}
	}

	@Test
	public void testDefaultResourcesForBoundedStream() {
		TestStreamEnvironment.setAsContext();

		// case
		{
			TestStreamEnvironment env = (TestStreamEnvironment) StreamExecutionEnvironment.getExecutionEnvironment();

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> map1 = source1.map(new NoOpIntMap());
			DataStreamSink<Integer> sink1 = map1.addSink(new NoOpSinkFunction());

			StreamGraph graph = StreamGraphGenerator.generate(StreamGraphGenerator.Context.buildBatchProperties(env),
					env.getTransformations());
			assertEquals(ResourceSpec.DEFAULT, graph.getStreamNode(source1.getId()).getMinResources());
			assertEquals(ResourceSpec.DEFAULT, graph.getStreamNode(map1.getId()).getMinResources());
			assertEquals(ResourceSpec.DEFAULT, graph.getStreamNode(sink1.getId()).getMinResources());
			for (StreamNode node : graph.getStreamNodes()) {
				assertEquals(node.getMinResources(), node.getPreferredResources());
			}
		}

		// case
		{
			TestStreamEnvironment env = (TestStreamEnvironment) StreamExecutionEnvironment.getExecutionEnvironment();
			ResourceSpec defaultResources = new ResourceSpec.Builder().setCpuCores(12345).build();
			env.setDefaultResources(defaultResources);

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> map1 = source1.map(new NoOpIntMap());
			DataStreamSink<Integer> sink1 = map1.addSink(new NoOpSinkFunction());

			StreamGraph graph = StreamGraphGenerator.generate(StreamGraphGenerator.Context.buildBatchProperties(env),
					env.getTransformations());
			assertEquals(defaultResources, graph.getStreamNode(source1.getId()).getMinResources());
			assertEquals(defaultResources, graph.getStreamNode(map1.getId()).getMinResources());
			assertEquals(defaultResources, graph.getStreamNode(sink1.getId()).getMinResources());
			for (StreamNode node : graph.getStreamNodes()) {
				assertEquals(node.getMinResources(), node.getPreferredResources());
			}
		}

		// case
		{
			TestStreamEnvironment env = (TestStreamEnvironment) StreamExecutionEnvironment.getExecutionEnvironment();

			Configuration globalConf = GlobalConfiguration.loadConfiguration();
			ResourceSpec globalDefaultResources = new ResourceSpec.Builder()
					.setCpuCores(globalConf.getDouble(CoreOptions.DEFAULT_RESOURCE_CPU_CORES))
					.setHeapMemoryInMB(globalConf.getInteger(CoreOptions.DEFAULT_RESOURCE_HEAP_MEMORY))
					.build();

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> map1 = source1.map(new NoOpIntMap());
			DataStreamSink<Integer> sink1 = map1.addSink(new NoOpSinkFunction());

			ResourceSpec map1Resources = new ResourceSpec.Builder().setCpuCores(9876).build();
			((SingleOutputStreamOperator<Integer>) map1).setResources(map1Resources);

			StreamGraph graph = StreamGraphGenerator.generate(StreamGraphGenerator.Context.buildBatchProperties(env),
					env.getTransformations());
			assertEquals(globalDefaultResources, graph.getStreamNode(source1.getId()).getMinResources());
			assertEquals(map1Resources, graph.getStreamNode(map1.getId()).getMinResources());
			assertEquals(globalDefaultResources, graph.getStreamNode(sink1.getId()).getMinResources());
			for (StreamNode node : graph.getStreamNodes()) {
				assertEquals(node.getMinResources(), node.getPreferredResources());
			}
		}

		// case
		{
			TestStreamEnvironment env = (TestStreamEnvironment) StreamExecutionEnvironment.getExecutionEnvironment();
			ResourceSpec defaultResources = new ResourceSpec.Builder().setCpuCores(12345).build();
			env.setDefaultResources(defaultResources);

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> map1 = source1.map(new NoOpIntMap());
			DataStreamSink<Integer> sink1 = map1.addSink(new NoOpSinkFunction());

			ResourceSpec map1Resources = new ResourceSpec.Builder().setCpuCores(9876).build();
			((SingleOutputStreamOperator<Integer>) map1).setResources(map1Resources);

			StreamGraph graph = StreamGraphGenerator.generate(StreamGraphGenerator.Context.buildBatchProperties(env),
					env.getTransformations());
			assertEquals(defaultResources, graph.getStreamNode(source1.getId()).getMinResources());
			assertEquals(map1Resources, graph.getStreamNode(map1.getId()).getMinResources());
			assertEquals(defaultResources, graph.getStreamNode(sink1.getId()).getMinResources());
			for (StreamNode node : graph.getStreamNodes()) {
				assertEquals(node.getMinResources(), node.getPreferredResources());
			}
		}

		// case
		{
			TestStreamEnvironment env = (TestStreamEnvironment) StreamExecutionEnvironment.getExecutionEnvironment();
			env.setDefaultResources(ResourceSpec.DEFAULT);

			Configuration globalConf = GlobalConfiguration.loadConfiguration();
			ResourceSpec globalDefaultResources = new ResourceSpec.Builder()
					.setCpuCores(globalConf.getDouble(CoreOptions.DEFAULT_RESOURCE_CPU_CORES))
					.setHeapMemoryInMB(globalConf.getInteger(CoreOptions.DEFAULT_RESOURCE_HEAP_MEMORY))
					.build();

			DataStream<Integer> source1 = env.fromElements(1, 10);
			DataStream<Integer> map1 = source1.map(new NoOpIntMap());
			DataStreamSink<Integer> sink1 = map1.addSink(new NoOpSinkFunction());

			ResourceSpec map1Resources = new ResourceSpec.Builder().setCpuCores(9876).build();
			((SingleOutputStreamOperator<Integer>) map1).setResources(map1Resources);

			StreamGraph graph = StreamGraphGenerator.generate(StreamGraphGenerator.Context.buildBatchProperties(env),
					env.getTransformations());
			assertEquals(globalDefaultResources, graph.getStreamNode(source1.getId()).getMinResources());
			assertEquals(map1Resources, graph.getStreamNode(map1.getId()).getMinResources());
			assertEquals(globalDefaultResources, graph.getStreamNode(sink1.getId()).getMinResources());
			for (StreamNode node : graph.getStreamNodes()) {
				assertEquals(node.getMinResources(), node.getPreferredResources());
			}
		}
	}

	private static class OutputTypeConfigurableOperationWithTwoInputs
			extends AbstractStreamOperator<Integer>
			implements TwoInputStreamOperator<Integer, Integer, Integer>, OutputTypeConfigurable<Integer> {
		private static final long serialVersionUID = 1L;

		TypeInformation<Integer> tpeInformation;

		public TypeInformation<Integer> getTypeInformation() {
			return tpeInformation;
		}

		@Override
		public void setOutputType(TypeInformation<Integer> outTypeInfo, ExecutionConfig executionConfig) {
			tpeInformation = outTypeInfo;
		}

		@Override
		public TwoInputSelection firstInputSelection() {
			return TwoInputSelection.ANY;
		}

		@Override
		public TwoInputSelection processElement1(StreamRecord<Integer> element) throws Exception {
			output.collect(element);
			return TwoInputSelection.ANY;
		}

		@Override
		public TwoInputSelection processElement2(StreamRecord<Integer> element) throws Exception {
			output.collect(element);
			return TwoInputSelection.ANY;
		}

		@Override
		public void processWatermark1(Watermark mark) throws Exception {}

		@Override
		public void processWatermark2(Watermark mark) throws Exception {}

		@Override
		public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {
			// ignore
		}

		@Override
		public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {
			// ignore
		}

		@Override
		public void endInput1() throws Exception {

		}

		@Override
		public void endInput2() throws Exception {

		}

		@Override
		public void setup(StreamTask<?, ?> containingTask, StreamConfig config, Output<StreamRecord<Integer>> output) {}
	}

	private static class OutputTypeConfigurableOperationWithOneInput
			extends AbstractStreamOperator<Integer>
			implements OneInputStreamOperator<Integer, Integer>, OutputTypeConfigurable<Integer> {
		private static final long serialVersionUID = 1L;

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
		public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {

		}

		@Override
		public void endInput() throws Exception {

		}

		@Override
		public void setOutputType(TypeInformation<Integer> outTypeInfo, ExecutionConfig executionConfig) {
			tpeInformation = outTypeInfo;
		}
	}

	static class NoOpIntCoMap implements CoMapFunction<Integer, Integer, Integer> {
		private static final long serialVersionUID = 1886595528149124270L;

		public Integer map1(Integer value) throws Exception {
			return value;
		}

		public Integer map2(Integer value) throws Exception {
			return value;
		}

	}

	static class NoOpIntFilter implements FilterFunction<Integer> {

		@Override
		public boolean filter(Integer value) throws Exception {
			return false;
		}
	}

	static class NoOpSinkFunction implements SinkFunction<Integer> {

	}

	static class TestStreamEnvironment extends StreamExecutionEnvironment {

		private TestStreamEnvironment() {

		}

		public List<StreamTransformation<?>> getTransformations() {
			return this.transformations;
		}

		public static void setAsContext() {

			StreamExecutionEnvironmentFactory factory = new StreamExecutionEnvironmentFactory() {
				@Override
				public StreamExecutionEnvironment createExecutionEnvironment() {
					return new TestStreamEnvironment();
				}
			};

			initializeContextEnvironment(factory);
		}

		@Override
		public JobExecutionResult execute(StreamGraph streamGraph) throws Exception {
			return null;
		}

		@Override
		protected JobSubmissionResult executeInternal(String jobName, boolean detached, SavepointRestoreSettings savepointRestoreSettings) throws Exception {
			return null;
		}

		@Override
		public String triggerSavepoint(String jobId, String path) throws Exception {
			return null;
		}
	}
}
