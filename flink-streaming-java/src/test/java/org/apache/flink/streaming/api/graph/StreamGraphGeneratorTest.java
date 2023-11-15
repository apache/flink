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
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.Function;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.CachedDataStream;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.StreamSource;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.CacheTransformation;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;
import org.apache.flink.streaming.runtime.partitioner.GlobalPartitioner;
import org.apache.flink.streaming.runtime.partitioner.RebalancePartitioner;
import org.apache.flink.streaming.runtime.partitioner.ShufflePartitioner;
import org.apache.flink.streaming.runtime.partitioner.StreamPartitioner;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.runtime.translators.CacheTransformationTranslator;
import org.apache.flink.streaming.util.NoOpIntMap;
import org.apache.flink.streaming.util.TestExpandingSink;
import org.apache.flink.util.AbstractID;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.TestLogger;

import org.assertj.core.api.Assertions;
import org.hamcrest.Description;
import org.hamcrest.FeatureMatcher;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;

/**
 * Tests for {@link StreamGraphGenerator}. This only tests correct translation of split/select,
 * union, partitioning since the other translation routines are tested already in operation specific
 * tests.
 */
@SuppressWarnings("serial")
public class StreamGraphGeneratorTest extends TestLogger {

    @Test
    public void generatorForwardsSavepointRestoreSettings() {
        StreamGraphGenerator streamGraphGenerator =
                new StreamGraphGenerator(
                        Collections.emptyList(), new ExecutionConfig(), new CheckpointConfig());

        streamGraphGenerator.setSavepointRestoreSettings(SavepointRestoreSettings.forPath("hello"));

        StreamGraph streamGraph = streamGraphGenerator.generate();
        assertThat(streamGraph.getSavepointRestoreSettings().getRestorePath(), is("hello"));
    }

    @Test
    public void testBufferTimeout() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setBufferTimeout(77); // set timeout to some recognizable number

        env.fromElements(1, 2, 3, 4, 5)
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
                    Assertions.assertThat(77L).isEqualTo(node.getBufferTimeout());
                    break;
                case "B":
                    Assertions.assertThat(node.getBufferTimeout()).isEqualTo(0L);
                    break;
                case "C":
                    Assertions.assertThat(node.getBufferTimeout()).isEqualTo(12L);
                    break;
                case "D":
                    Assertions.assertThat(node.getBufferTimeout()).isEqualTo(77L);
                    break;
                default:
                    Assertions.assertThat(node.getOperator()).isInstanceOf(StreamSource.class);
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
        DataStream<Integer> broadcastMap =
                rebalanceMap.forward().global().broadcast().map(new NoOpIntMap());

        broadcastMap.sinkTo(new DiscardingSink<>());

        DataStream<Integer> broadcastOperator =
                rebalanceMap.map(new NoOpIntMap()).name("broadcast");

        DataStream<Integer> map1 = broadcastOperator.broadcast();

        DataStream<Integer> globalOperator = rebalanceMap.map(new NoOpIntMap()).name("global");

        DataStream<Integer> map2 = globalOperator.global();

        DataStream<Integer> shuffleOperator = rebalanceMap.map(new NoOpIntMap()).name("shuffle");

        DataStream<Integer> map3 = shuffleOperator.shuffle();

        SingleOutputStreamOperator<Integer> unionedMap =
                map1.union(map2).union(map3).map(new NoOpIntMap()).name("union");

        unionedMap.sinkTo(new DiscardingSink<>());

        StreamGraph graph = env.getStreamGraph();

        // rebalanceMap
        Assertions.assertThat(
                        graph.getStreamNode(rebalanceMap.getId())
                                .getInEdges()
                                .get(0)
                                .getPartitioner())
                .isInstanceOf(RebalancePartitioner.class);

        // verify that only last partitioning takes precedence
        Assertions.assertThat(
                        graph.getStreamNode(broadcastMap.getId())
                                .getInEdges()
                                .get(0)
                                .getPartitioner())
                .isInstanceOf(BroadcastPartitioner.class);
        Assertions.assertThat(
                        graph.getSourceVertex(
                                        graph.getStreamNode(broadcastMap.getId())
                                                .getInEdges()
                                                .get(0))
                                .getId())
                .isEqualTo(rebalanceMap.getId());

        // verify that partitioning in unions is preserved
        Assertions.assertThat(
                        graph.getStreamNode(broadcastOperator.getId())
                                .getOutEdges()
                                .get(0)
                                .getPartitioner())
                .isInstanceOf(BroadcastPartitioner.class);
        Assertions.assertThat(
                        graph.getStreamNode(globalOperator.getId())
                                .getOutEdges()
                                .get(0)
                                .getPartitioner())
                .isInstanceOf(GlobalPartitioner.class);
        Assertions.assertThat(
                        graph.getStreamNode(shuffleOperator.getId())
                                .getOutEdges()
                                .get(0)
                                .getPartitioner())
                .isInstanceOf(ShufflePartitioner.class);
    }

    @Test
    public void testOutputTypeConfigurationWithUdfStreamOperator() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        OutputTypeConfigurableFunction<Integer> function = new OutputTypeConfigurableFunction<>();

        DataStream<Integer> source = env.fromElements(1, 10);

        NoOpUdfOperator<Integer> udfOperator = new NoOpUdfOperator<>(function);

        source.transform("no-op udf operator", BasicTypeInfo.INT_TYPE_INFO, udfOperator)
                .sinkTo(new DiscardingSink<>());

        env.getStreamGraph();

        Assertions.assertThat(udfOperator).isInstanceOf(AbstractUdfStreamOperator.class);
        Assertions.assertThat(function.getTypeInformation()).isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
    }

    /**
     * Test whether an {@link OutputTypeConfigurable} implementation gets called with the correct
     * output type. In this test case the output type must be BasicTypeInfo.INT_TYPE_INFO.
     */
    @Test
    public void testOutputTypeConfigurationWithOneInputTransformation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> source = env.fromElements(1, 10);

        OutputTypeConfigurableOperationWithOneInput outputTypeConfigurableOperation =
                new OutputTypeConfigurableOperationWithOneInput();

        DataStream<Integer> result =
                source.transform(
                        "Single input and output type configurable operation",
                        BasicTypeInfo.INT_TYPE_INFO,
                        outputTypeConfigurableOperation);

        result.sinkTo(new DiscardingSink<>());

        env.getStreamGraph();

        Assertions.assertThat(outputTypeConfigurableOperation.getTypeInformation())
                .isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
    }

    @Test
    public void testOutputTypeConfigurationWithTwoInputTransformation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> source1 = env.fromElements(1, 10);
        DataStream<Integer> source2 = env.fromElements(2, 11);

        ConnectedStreams<Integer, Integer> connectedSource = source1.connect(source2);

        OutputTypeConfigurableOperationWithTwoInputs outputTypeConfigurableOperation =
                new OutputTypeConfigurableOperationWithTwoInputs();

        DataStream<Integer> result =
                connectedSource.transform(
                        "Two input and output type configurable operation",
                        BasicTypeInfo.INT_TYPE_INFO,
                        outputTypeConfigurableOperation);

        result.sinkTo(new DiscardingSink<>());

        env.getStreamGraph();

        Assertions.assertThat(outputTypeConfigurableOperation.getTypeInformation())
                .isEqualTo(BasicTypeInfo.INT_TYPE_INFO);
    }

    @Test
    public void testMultipleInputTransformation() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> source1 = env.fromElements(1, 10);
        DataStream<Long> source2 = env.fromElements(2L, 11L);
        DataStream<String> source3 = env.fromElements("42", "44");

        MultipleInputTransformation<String> transform =
                new MultipleInputTransformation<String>(
                        "My Operator",
                        new MultipleInputOperatorFactory(),
                        BasicTypeInfo.STRING_TYPE_INFO,
                        3);

        env.addOperator(
                transform
                        .addInput(source1.getTransformation())
                        .addInput(source2.getTransformation())
                        .addInput(source3.getTransformation()));

        StreamGraph streamGraph = env.getStreamGraph();
        Assertions.assertThat(streamGraph.getStreamNodes().size()).isEqualTo(4);

        Assertions.assertThat(streamGraph.getStreamEdges(source1.getId(), transform.getId()).size())
                .isEqualTo(1);
        Assertions.assertThat(streamGraph.getStreamEdges(source2.getId(), transform.getId()).size())
                .isEqualTo(1);
        Assertions.assertThat(streamGraph.getStreamEdges(source3.getId(), transform.getId()).size())
                .isEqualTo(1);
        Assertions.assertThat(streamGraph.getStreamEdges(source1.getId()).size()).isEqualTo(1);
        Assertions.assertThat(streamGraph.getStreamEdges(source2.getId()).size()).isEqualTo(1);
        Assertions.assertThat(streamGraph.getStreamEdges(source3.getId()).size()).isEqualTo(1);
        Assertions.assertThat(streamGraph.getStreamEdges(transform.getId()).size()).isEqualTo(0);
    }

    @Test
    public void testUnalignedCheckpointDisabledOnPointwise() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(42);

        DataStream<Long> source1 = env.fromSequence(1L, 10L);
        DataStream<Long> map1 = source1.forward().map(l -> l);
        DataStream<Long> source2 = env.fromSequence(2L, 11L);
        DataStream<Long> map2 = source2.shuffle().map(l -> l);

        final MapStateDescriptor<Long, Long> descriptor =
                new MapStateDescriptor<>(
                        "broadcast", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);
        final BroadcastStream<Long> broadcast = map1.broadcast(descriptor);
        final SingleOutputStreamOperator<Long> joined =
                map2.connect(broadcast)
                        .process(
                                new BroadcastProcessFunction<Long, Long, Long>() {
                                    @Override
                                    public void processElement(
                                            Long value, ReadOnlyContext ctx, Collector<Long> out) {}

                                    @Override
                                    public void processBroadcastElement(
                                            Long value, Context ctx, Collector<Long> out) {}
                                });

        DataStream<Long> map3 = joined.shuffle().map(l -> l);
        DataStream<Long> map4 = map3.rescale().map(l -> l).setParallelism(1337);

        StreamGraph streamGraph = env.getStreamGraph();
        Assertions.assertThat(streamGraph.getStreamNodes().size()).isEqualTo(7);

        // forward
        assertThat(edge(streamGraph, source1, map1), supportsUnalignedCheckpoints(false));
        // shuffle
        assertThat(edge(streamGraph, source2, map2), supportsUnalignedCheckpoints(true));
        // broadcast, but other channel is forwarded
        assertThat(edge(streamGraph, map1, joined), supportsUnalignedCheckpoints(false));
        // forward
        assertThat(edge(streamGraph, map2, joined), supportsUnalignedCheckpoints(false));
        // shuffle
        assertThat(edge(streamGraph, joined, map3), supportsUnalignedCheckpoints(true));
        // rescale
        assertThat(edge(streamGraph, map3, map4), supportsUnalignedCheckpoints(false));
    }

    @Test
    public void testUnalignedCheckpointDisabledOnBroadcast() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(42);

        DataStream<Long> source1 = env.fromSequence(1L, 10L);
        DataStream<Long> map1 = source1.broadcast().map(l -> l);
        DataStream<Long> source2 = env.fromSequence(2L, 11L);
        DataStream<Long> keyed = source2.keyBy(r -> 0L);

        final MapStateDescriptor<Long, Long> descriptor =
                new MapStateDescriptor<>(
                        "broadcast", BasicTypeInfo.LONG_TYPE_INFO, BasicTypeInfo.LONG_TYPE_INFO);
        final BroadcastStream<Long> broadcast = map1.broadcast(descriptor);
        final SingleOutputStreamOperator<Long> joined =
                keyed.connect(broadcast)
                        .process(
                                new KeyedBroadcastProcessFunction<Long, Long, Long, Long>() {
                                    @Override
                                    public void processElement(
                                            Long value, ReadOnlyContext ctx, Collector<Long> out) {}

                                    @Override
                                    public void processBroadcastElement(
                                            Long value, Context ctx, Collector<Long> out) {}
                                });

        StreamGraph streamGraph = env.getStreamGraph();
        Assertions.assertThat(streamGraph.getStreamNodes().size()).isEqualTo(4);

        // single broadcast
        assertThat(edge(streamGraph, source1, map1), supportsUnalignedCheckpoints(false));
        // keyed, connected with broadcast
        assertThat(edge(streamGraph, source2, joined), supportsUnalignedCheckpoints(false));
        // broadcast, connected with keyed
        assertThat(edge(streamGraph, map1, joined), supportsUnalignedCheckpoints(false));
    }

    private static StreamEdge edge(
            StreamGraph streamGraph, DataStream<Long> op1, DataStream<Long> op2) {
        List<StreamEdge> streamEdges = streamGraph.getStreamEdges(op1.getId(), op2.getId());
        assertThat(streamEdges, iterableWithSize(1));
        return streamEdges.get(0);
    }

    private static Matcher<StreamEdge> supportsUnalignedCheckpoints(boolean enabled) {
        return new FeatureMatcher<StreamEdge, Boolean>(
                equalTo(enabled),
                "supports unaligned checkpoint",
                "supports unaligned checkpoint") {
            @Override
            protected Boolean featureValueOf(StreamEdge actual) {
                return actual.supportsUnalignedCheckpoints();
            }
        };
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

        DataStream<Integer> keyedResult = source.keyBy(value -> value).map(new NoOpIntMap());

        keyedResult.sinkTo(new DiscardingSink<>());

        StreamGraph graph = env.getStreamGraph();

        StreamNode keyedResultNode = graph.getStreamNode(keyedResult.getId());

        StreamPartitioner<?> streamPartitioner =
                keyedResultNode.getInEdges().get(0).getPartitioner();
    }

    /** Tests that the global and operator-wide max parallelism setting is respected. */
    @Test
    public void testMaxParallelismForwarding() {
        int globalMaxParallelism = 42;
        int keyedResult2MaxParallelism = 17;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setMaxParallelism(globalMaxParallelism);

        DataStream<Integer> source = env.fromElements(1, 2, 3);

        DataStream<Integer> keyedResult1 = source.keyBy(value -> value).map(new NoOpIntMap());

        DataStream<Integer> keyedResult2 =
                keyedResult1
                        .keyBy(value -> value)
                        .map(new NoOpIntMap())
                        .setMaxParallelism(keyedResult2MaxParallelism);

        keyedResult2.sinkTo(new DiscardingSink<>());

        StreamGraph graph = env.getStreamGraph();

        StreamNode keyedResult1Node = graph.getStreamNode(keyedResult1.getId());
        StreamNode keyedResult2Node = graph.getStreamNode(keyedResult2.getId());

        Assertions.assertThat(keyedResult1Node.getMaxParallelism()).isEqualTo(globalMaxParallelism);
        Assertions.assertThat(keyedResult2Node.getMaxParallelism())
                .isEqualTo(keyedResult2MaxParallelism);
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

        DataStream<Integer> keyedResult1 = source.keyBy(value -> value).map(new NoOpIntMap());

        DataStream<Integer> keyedResult2 =
                keyedResult1
                        .keyBy(value -> value)
                        .map(new NoOpIntMap())
                        .setParallelism(mapParallelism);

        DataStream<Integer> keyedResult3 =
                keyedResult2
                        .keyBy(value -> value)
                        .map(new NoOpIntMap())
                        .setMaxParallelism(maxParallelism);

        DataStream<Integer> keyedResult4 =
                keyedResult3
                        .keyBy(value -> value)
                        .map(new NoOpIntMap())
                        .setMaxParallelism(maxParallelism)
                        .setParallelism(mapParallelism);

        keyedResult4.sinkTo(new DiscardingSink<>());

        StreamGraph graph = env.getStreamGraph();

        StreamNode keyedResult3Node = graph.getStreamNode(keyedResult3.getId());
        StreamNode keyedResult4Node = graph.getStreamNode(keyedResult4.getId());

        Assertions.assertThat(keyedResult3Node.getMaxParallelism()).isEqualTo(maxParallelism);
        Assertions.assertThat(keyedResult4Node.getMaxParallelism()).isEqualTo(maxParallelism);
    }

    /** Tests that the max parallelism is properly set for connected streams. */
    @Test
    public void testMaxParallelismWithConnectedKeyedStream() {
        int maxParallelism = 42;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> input1 = env.fromElements(1, 2, 3, 4).setMaxParallelism(128);
        DataStream<Integer> input2 = env.fromElements(1, 2, 3, 4).setMaxParallelism(129);

        env.getConfig().setMaxParallelism(maxParallelism);

        DataStream<Integer> keyedResult =
                input1.connect(input2)
                        .keyBy(value -> value, value -> value)
                        .map(new NoOpIntCoMap());

        keyedResult.sinkTo(new DiscardingSink<>());

        StreamGraph graph = env.getStreamGraph();

        StreamNode keyedResultNode = graph.getStreamNode(keyedResult.getId());

        StreamPartitioner<?> streamPartitioner1 =
                keyedResultNode.getInEdges().get(0).getPartitioner();
        StreamPartitioner<?> streamPartitioner2 =
                keyedResultNode.getInEdges().get(1).getPartitioner();
    }

    /**
     * Tests that the json generated by JSONGenerator shall meet with 2 requirements: 1. sink nodes
     * are at the back 2. if both two nodes are sink nodes or neither of them is sink node, then
     * sort by its id.
     */
    @Test
    public void testSinkIdComparison() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> source = env.fromElements(1, 2, 3);
        for (int i = 0; i < 32; i++) {
            if (i % 2 == 0) {
                source.addSink(
                        new SinkFunction<Integer>() {
                            @Override
                            public void invoke(Integer value, Context ctx) throws Exception {}
                        });
            } else {
                source.map(x -> x + 1);
            }
        }
        // IllegalArgumentException will be thrown without FLINK-9216
        env.getStreamGraph().getStreamingPlanAsJSON();
    }

    /** Test iteration job, check slot sharing group and co-location group. */
    @Test
    public void testIteration() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> source = env.fromElements(1, 2, 3).name("source");
        IterativeStream<Integer> iteration = source.iterate(3000);
        iteration.name("iteration").setParallelism(2);
        DataStream<Integer> map = iteration.map(x -> x + 1).name("map").setParallelism(2);
        DataStream<Integer> filter = map.filter((x) -> false).name("filter").setParallelism(2);
        iteration.closeWith(filter).print();

        final ResourceSpec resources = ResourceSpec.newBuilder(1.0, 100).build();
        iteration.getTransformation().setResources(resources, resources);

        StreamGraph streamGraph = env.getStreamGraph();
        for (Tuple2<StreamNode, StreamNode> iterationPair :
                streamGraph.getIterationSourceSinkPairs()) {
            Assertions.assertThat(iterationPair.f0.getCoLocationGroup()).isNotNull();
            Assertions.assertThat(iterationPair.f1.getCoLocationGroup())
                    .isEqualTo(iterationPair.f0.getCoLocationGroup());

            Assertions.assertThat(iterationPair.f0.getSlotSharingGroup())
                    .isEqualTo(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP);
            Assertions.assertThat(iterationPair.f1.getSlotSharingGroup())
                    .isEqualTo(iterationPair.f0.getSlotSharingGroup());

            final ResourceSpec sourceMinResources = iterationPair.f0.getMinResources();
            final ResourceSpec sinkMinResources = iterationPair.f1.getMinResources();
            final ResourceSpec iterationResources = sourceMinResources.merge(sinkMinResources);
            assertThat(iterationResources, equalsResourceSpec(resources));
        }
    }

    private Matcher<ResourceSpec> equalsResourceSpec(ResourceSpec resources) {
        return new EqualsResourceSpecMatcher(resources);
    }

    /** Test slot sharing is enabled. */
    @Test
    public void testEnableSlotSharing() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Integer> sourceDataStream = env.fromElements(1, 2, 3);
        DataStream<Integer> mapDataStream = sourceDataStream.map(x -> x + 1);

        final List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(sourceDataStream.getTransformation());
        transformations.add(mapDataStream.getTransformation());

        // all stream nodes share default group by default
        StreamGraph streamGraph =
                new StreamGraphGenerator(
                                transformations, env.getConfig(), env.getCheckpointConfig())
                        .generate();

        Collection<StreamNode> streamNodes = streamGraph.getStreamNodes();
        for (StreamNode streamNode : streamNodes) {
            Assertions.assertThat(streamNode.getSlotSharingGroup())
                    .isEqualTo(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP);
        }
    }

    @Test
    public void testSetManagedMemoryWeight() {
        final int weight = 123;
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Integer> source = env.fromElements(1, 2, 3).name("source");
        source.getTransformation()
                .declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase.OPERATOR, weight);
        source.print().name("sink");

        final StreamGraph streamGraph = env.getStreamGraph();
        for (StreamNode streamNode : streamGraph.getStreamNodes()) {
            if (streamNode.getOperatorName().contains("source")) {
                assertThat(
                        streamNode
                                .getManagedMemoryOperatorScopeUseCaseWeights()
                                .get(ManagedMemoryUseCase.OPERATOR),
                        is(weight));
            } else {
                assertThat(streamNode.getManagedMemoryOperatorScopeUseCaseWeights().size(), is(0));
            }
        }
    }

    @Test
    public void testSetSlotSharingResource() {
        final String slotSharingGroup1 = "a";
        final String slotSharingGroup2 = "b";
        final ResourceProfile resourceProfile1 = ResourceProfile.fromResources(1, 10);
        final ResourceProfile resourceProfile2 = ResourceProfile.fromResources(2, 20);
        final ResourceProfile resourceProfile3 = ResourceProfile.fromResources(3, 30);
        final Map<String, ResourceProfile> slotSharingGroupResource = new HashMap<>();
        slotSharingGroupResource.put(slotSharingGroup1, resourceProfile1);
        slotSharingGroupResource.put(slotSharingGroup2, resourceProfile2);
        slotSharingGroupResource.put(
                StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP, resourceProfile3);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final DataStream<Integer> sourceDataStream =
                env.fromElements(1, 2, 3).slotSharingGroup(slotSharingGroup1);
        final DataStream<Integer> mapDataStream1 =
                sourceDataStream.map(x -> x + 1).slotSharingGroup(slotSharingGroup2);
        final DataStream<Integer> mapDataStream2 = mapDataStream1.map(x -> x * 2);

        final List<Transformation<?>> transformations = new ArrayList<>();
        transformations.add(sourceDataStream.getTransformation());
        transformations.add(mapDataStream1.getTransformation());
        transformations.add(mapDataStream2.getTransformation());

        // all stream nodes share default group by default
        final StreamGraph streamGraph =
                new StreamGraphGenerator(
                                transformations, env.getConfig(), env.getCheckpointConfig())
                        .setSlotSharingGroupResource(slotSharingGroupResource)
                        .generate();

        assertThat(
                streamGraph.getSlotSharingGroupResource(slotSharingGroup1).get(),
                equalTo(resourceProfile1));
        assertThat(
                streamGraph.getSlotSharingGroupResource(slotSharingGroup2).get(),
                equalTo(resourceProfile2));
        assertThat(
                streamGraph
                        .getSlotSharingGroupResource(
                                StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                        .get(),
                equalTo(resourceProfile3));
    }

    @Test
    public void testSettingSavepointRestoreSettings() {
        Configuration config = new Configuration();
        config.set(SavepointConfigOptions.SAVEPOINT_PATH, "/tmp/savepoint");

        final StreamGraph streamGraph =
                new StreamGraphGenerator(
                                Collections.emptyList(),
                                new ExecutionConfig(),
                                new CheckpointConfig(),
                                config)
                        .generate();

        SavepointRestoreSettings savepointRestoreSettings =
                streamGraph.getSavepointRestoreSettings();
        assertThat(
                savepointRestoreSettings,
                equalTo(SavepointRestoreSettings.forPath("/tmp/savepoint")));
    }

    @Test
    public void testSettingSavepointRestoreSettingsSetterOverrides() {
        Configuration config = new Configuration();
        config.set(SavepointConfigOptions.SAVEPOINT_PATH, "/tmp/savepoint");

        StreamGraphGenerator generator =
                new StreamGraphGenerator(
                        Collections.emptyList(),
                        new ExecutionConfig(),
                        new CheckpointConfig(),
                        config);
        generator.setSavepointRestoreSettings(SavepointRestoreSettings.forPath("/tmp/savepoint1"));
        final StreamGraph streamGraph = generator.generate();

        SavepointRestoreSettings savepointRestoreSettings =
                streamGraph.getSavepointRestoreSettings();
        assertThat(
                savepointRestoreSettings,
                equalTo(SavepointRestoreSettings.forPath("/tmp/savepoint1")));
    }

    @Test
    public void testConfigureSlotSharingGroupResource() {
        final SlotSharingGroup ssg1 =
                SlotSharingGroup.newBuilder("ssg1").setCpuCores(1).setTaskHeapMemoryMB(100).build();
        final SlotSharingGroup ssg2 =
                SlotSharingGroup.newBuilder("ssg2").setCpuCores(2).setTaskHeapMemoryMB(200).build();
        final SlotSharingGroup ssg3 =
                SlotSharingGroup.newBuilder(StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                        .setCpuCores(3)
                        .setTaskHeapMemoryMB(300)
                        .build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<Integer> source = env.fromElements(1).slotSharingGroup("ssg1");
        source.map(value -> value)
                .slotSharingGroup(ssg2)
                .map(value -> value * 2)
                .map(value -> value * 3)
                .slotSharingGroup(SlotSharingGroup.newBuilder("ssg4").build())
                .map(value -> value * 4)
                .slotSharingGroup(ssg3)
                .sinkTo(new DiscardingSink<>())
                .slotSharingGroup(ssg1);

        final StreamGraph streamGraph = env.getStreamGraph();
        assertThat(
                streamGraph.getSlotSharingGroupResource("ssg1").get(),
                is(ResourceProfile.fromResources(1, 100)));
        assertThat(
                streamGraph.getSlotSharingGroupResource("ssg2").get(),
                is(ResourceProfile.fromResources(2, 200)));
        assertThat(
                streamGraph
                        .getSlotSharingGroupResource(
                                StreamGraphGenerator.DEFAULT_SLOT_SHARING_GROUP)
                        .get(),
                is(ResourceProfile.fromResources(3, 300)));
    }

    @Test
    public void testConflictSlotSharingGroup() {
        final SlotSharingGroup ssg =
                SlotSharingGroup.newBuilder("ssg").setCpuCores(1).setTaskHeapMemoryMB(100).build();
        final SlotSharingGroup ssgConflict =
                SlotSharingGroup.newBuilder("ssg").setCpuCores(2).setTaskHeapMemoryMB(200).build();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        final DataStream<Integer> source = env.fromElements(1).slotSharingGroup(ssg);
        source.map(value -> value)
                .slotSharingGroup(ssgConflict)
                .sinkTo(new DiscardingSink<>())
                .slotSharingGroup(ssgConflict);

        Assertions.assertThatThrownBy(env::getStreamGraph)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testTrackTransformationsByIdentity() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final Transformation<?> noopTransformation = env.fromSequence(1, 2).getTransformation();

        final StreamGraphGenerator generator =
                new StreamGraphGenerator(
                        Arrays.asList(
                                noopTransformation,
                                new FailingTransformation(noopTransformation.hashCode())),
                        new ExecutionConfig(),
                        new CheckpointConfig());
        assertThatThrownBy(generator::generate)
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Unknown transformation: FailingTransformation");
    }

    @Test
    public void testResetBatchExchangeModeInStreamingExecution() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Integer> sourceDataStream = env.fromElements(1, 2, 3);
        PartitionTransformation<Integer> transformation =
                new PartitionTransformation<>(
                        sourceDataStream.getTransformation(),
                        new RebalancePartitioner<>(),
                        StreamExchangeMode.BATCH);
        DataStream<Integer> partitionStream = new DataStream<>(env, transformation);
        partitionStream.map(value -> value).print();

        final StreamGraph streamGraph = env.getStreamGraph();

        final List<Integer> nodeIds =
                streamGraph.getStreamNodes().stream()
                        .map(StreamNode::getId)
                        .sorted(Integer::compare)
                        .collect(Collectors.toList());
        Assertions.assertThat(streamGraph.getStreamEdges(nodeIds.get(0), nodeIds.get(1)))
                .hasSize(1)
                .satisfies(
                        e ->
                                Assertions.assertThat(e.get(0).getExchangeMode())
                                        .isEqualTo(StreamExchangeMode.UNDEFINED));
    }

    @Test
    public void testAutoParallelismForExpandedTransformations() {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);

        DataStream<Integer> sourceDataStream = env.fromElements(1, 2, 3);
        // Parallelism is set to -1 (default parallelism identifier) to imitate the behavior of
        // the table planner. Parallelism should be set automatically after translating.
        sourceDataStream.sinkTo(new TestExpandingSink()).setParallelism(-1);

        StreamGraph graph = env.getStreamGraph();

        graph.getStreamNodes()
                .forEach(
                        node -> {
                            if (!node.getOperatorName().startsWith("Source")) {
                                Assertions.assertThat(node.getParallelism()).isEqualTo(2);
                            }
                        });
    }

    @Test
    public void testCacheTransformation() {
        final TestingStreamExecutionEnvironment env = new TestingStreamExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStream<Integer> source = env.fromElements(1, 2, 3);
        final int upstreamParallelism = 3;
        CachedDataStream<Integer> cachedStream =
                source.keyBy(i -> i)
                        .reduce(Integer::sum)
                        .setParallelism(upstreamParallelism)
                        .cache();
        Assertions.assertThat(cachedStream.getTransformation())
                .isInstanceOf(CacheTransformation.class);
        CacheTransformation<Integer> cacheTransformation =
                (CacheTransformation<Integer>) cachedStream.getTransformation();

        cachedStream.print();
        final StreamGraph streamGraph = env.getStreamGraph();

        verifyCacheProduceNode(upstreamParallelism, cacheTransformation, streamGraph, null);

        env.addCompletedClusterDatasetIds(cacheTransformation.getDatasetId());
        cachedStream.print();

        verifyCacheConsumeNode(env, upstreamParallelism, cacheTransformation);
    }

    @Test
    public void testCacheSideOutput() {
        final TestingStreamExecutionEnvironment env = new TestingStreamExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        final int upstreamParallelism = 2;
        SingleOutputStreamOperator<Integer> stream =
                env.fromElements(1, 2, 3).map(i -> i).setParallelism(upstreamParallelism);
        final DataStream<Integer> sideOutputCache =
                stream.getSideOutput(new OutputTag<Integer>("1") {}).cache();
        Assertions.assertThat(sideOutputCache.getTransformation())
                .isInstanceOf(CacheTransformation.class);
        final CacheTransformation<Integer> cacheTransformation =
                (CacheTransformation<Integer>) sideOutputCache.getTransformation();
        sideOutputCache.print();

        final StreamGraph streamGraph = env.getStreamGraph();
        verifyCacheProduceNode(upstreamParallelism, cacheTransformation, streamGraph, "1");

        env.addCompletedClusterDatasetIds(cacheTransformation.getDatasetId());
        sideOutputCache.print();

        verifyCacheConsumeNode(env, upstreamParallelism, cacheTransformation);
    }

    private void verifyCacheProduceNode(
            int upstreamParallelism,
            CacheTransformation<Integer> cacheTransformation,
            StreamGraph streamGraph,
            String expectedTagId) {
        Assertions.assertThat(streamGraph.getStreamNodes())
                .anyMatch(
                        node -> {
                            if (!CacheTransformationTranslator.CACHE_PRODUCER_OPERATOR_NAME.equals(
                                    node.getOperatorName())) {
                                return false;
                            }

                            Assertions.assertThat(node.getParallelism())
                                    .isEqualTo(upstreamParallelism);
                            Assertions.assertThat(node.getInEdges().size()).isEqualTo(1);
                            final StreamEdge inEdge = node.getInEdges().get(0);
                            Assertions.assertThat(inEdge.getPartitioner())
                                    .isInstanceOf(ForwardPartitioner.class);
                            if (expectedTagId != null) {
                                Assertions.assertThat(inEdge.getOutputTag().getId())
                                        .isEqualTo(expectedTagId);
                            }

                            Assertions.assertThat(inEdge.getIntermediateDatasetIdToProduce())
                                    .isNotNull();
                            Assertions.assertThat(
                                            new AbstractID(
                                                    inEdge.getIntermediateDatasetIdToProduce()))
                                    .isEqualTo(cacheTransformation.getDatasetId());
                            return true;
                        });
    }

    private void verifyCacheConsumeNode(
            StreamExecutionEnvironment env,
            int upstreamParallelism,
            CacheTransformation<Integer> cacheTransformation) {
        Assertions.assertThat(env.getStreamGraph().getStreamNodes())
                .anyMatch(
                        node -> {
                            if (!CacheTransformationTranslator.CACHE_CONSUMER_OPERATOR_NAME.equals(
                                    node.getOperatorName())) {
                                return false;
                            }

                            Assertions.assertThat(node.getParallelism())
                                    .isEqualTo(upstreamParallelism);
                            Assertions.assertThat(new AbstractID(node.getConsumeClusterDatasetId()))
                                    .isEqualTo(cacheTransformation.getDatasetId());
                            return true;
                        });
    }

    private static class FailingTransformation extends Transformation<String> {
        private final int hashCode;

        FailingTransformation(int hashCode) {
            super("FailingTransformation", BasicTypeInfo.STRING_TYPE_INFO, 1);
            this.hashCode = hashCode;
        }

        @Override
        public List<Transformation<?>> getTransitivePredecessors() {
            return Collections.emptyList();
        }

        @Override
        public List<Transformation<?>> getInputs() {
            return Collections.emptyList();
        }

        // Overwrite equal to test transformation based on identity
        @Override
        public boolean equals(Object o) {
            return true;
        }

        // Overwrite hashCode to test transformation based on identity
        @Override
        public int hashCode() {
            return hashCode;
        }
    }

    private static class OutputTypeConfigurableFunction<T>
            implements OutputTypeConfigurable<T>, Function {
        private TypeInformation<T> typeInformation;

        public TypeInformation<T> getTypeInformation() {
            return typeInformation;
        }

        @Override
        public void setOutputType(TypeInformation<T> outTypeInfo, ExecutionConfig executionConfig) {
            typeInformation = outTypeInfo;
        }
    }

    static class NoOpUdfOperator<T> extends AbstractUdfStreamOperator<T, Function>
            implements OneInputStreamOperator<T, T> {
        NoOpUdfOperator(Function function) {
            super(function);
        }

        @Override
        public void processElement(StreamRecord<T> element) throws Exception {
            output.collect(element);
        }
    }

    static class OutputTypeConfigurableOperationWithTwoInputs
            extends AbstractStreamOperator<Integer>
            implements TwoInputStreamOperator<Integer, Integer, Integer>,
                    OutputTypeConfigurable<Integer> {
        private static final long serialVersionUID = 1L;

        TypeInformation<Integer> tpeInformation;

        public TypeInformation<Integer> getTypeInformation() {
            return tpeInformation;
        }

        @Override
        public void setOutputType(
                TypeInformation<Integer> outTypeInfo, ExecutionConfig executionConfig) {
            tpeInformation = outTypeInfo;
        }

        @Override
        public void processElement1(StreamRecord<Integer> element) throws Exception {
            output.collect(element);
        }

        @Override
        public void processElement2(StreamRecord<Integer> element) throws Exception {
            output.collect(element);
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
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<Integer>> output) {}
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
        public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {}

        @Override
        public void setOutputType(
                TypeInformation<Integer> outTypeInfo, ExecutionConfig executionConfig) {
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

    private static class EqualsResourceSpecMatcher extends TypeSafeMatcher<ResourceSpec> {
        private final ResourceSpec resources;

        EqualsResourceSpecMatcher(ResourceSpec resources) {
            this.resources = resources;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("expected resource spec ").appendValue(resources);
        }

        @Override
        protected boolean matchesSafely(ResourceSpec item) {
            return resources.lessThanOrEqual(item) && item.lessThanOrEqual(resources);
        }
    }

    private static class MultipleInputOperatorFactory implements StreamOperatorFactory<String> {
        @Override
        public <T extends StreamOperator<String>> T createStreamOperator(
                StreamOperatorParameters<String> parameters) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setChainingStrategy(ChainingStrategy strategy) {
            throw new UnsupportedOperationException();
        }

        @Override
        public ChainingStrategy getChainingStrategy() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestingStreamExecutionEnvironment extends StreamExecutionEnvironment {
        Set<AbstractID> completedClusterDatasetIds = new HashSet<>();

        public void addCompletedClusterDatasetIds(AbstractID id) {
            completedClusterDatasetIds.add(id);
        }

        @Override
        public Set<AbstractID> listCompletedClusterDatasets() {
            return new HashSet<>(completedClusterDatasetIds);
        }
    }
}
