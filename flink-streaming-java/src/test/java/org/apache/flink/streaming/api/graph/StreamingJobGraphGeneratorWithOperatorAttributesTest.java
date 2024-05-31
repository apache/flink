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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.co.CoProcessOperator;
import org.apache.flink.streaming.api.transformations.KeyedMultipleInputTransformation;
import org.apache.flink.util.Collector;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StreamingJobGraphGenerator} with internal sorter. */
public class StreamingJobGraphGeneratorWithOperatorAttributesTest {
    @Test
    void testOutputOnlyAfterEndOfStream() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());

        final DataStream<Integer> source = env.fromData(1, 2, 3).name("source");
        source.keyBy(x -> x)
                .transform(
                        "transform",
                        Types.INT,
                        new StreamOperatorWithConfigurableOperatorAttributes<>(
                                x -> x,
                                new OperatorAttributesBuilder()
                                        .setOutputOnlyAfterEndOfStream(true)
                                        .build()))
                .map(x -> x)
                .sinkTo(new DiscardingSink<>())
                .disableChaining()
                .name("sink");

        final StreamGraph streamGraph = env.getStreamGraph(false);
        Map<String, StreamNode> nodeMap = new HashMap<>();
        for (StreamNode node : streamGraph.getStreamNodes()) {
            nodeMap.put(node.getOperatorName(), node);
        }
        assertThat(nodeMap).hasSize(4);
        assertThat(nodeMap.get("Source: source").isOutputOnlyAfterEndOfStream()).isFalse();
        assertThat(nodeMap.get("transform").isOutputOnlyAfterEndOfStream()).isTrue();
        assertThat(nodeMap.get("Map").isOutputOnlyAfterEndOfStream()).isFalse();
        assertThat(nodeMap.get("sink: Writer").isOutputOnlyAfterEndOfStream()).isFalse();
        assertManagedMemoryWeightsSize(nodeMap.get("Source: source"), 0);
        assertManagedMemoryWeightsSize(nodeMap.get("transform"), 1);
        assertManagedMemoryWeightsSize(nodeMap.get("Map"), 0);
        assertManagedMemoryWeightsSize(nodeMap.get("sink: Writer"), 0);

        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
        Map<String, JobVertex> vertexMap = new HashMap<>();
        for (JobVertex vertex : jobGraph.getVertices()) {
            vertexMap.put(vertex.getName(), vertex);
        }
        assertThat(vertexMap).hasSize(3);
        assertHasOutputPartitionType(
                vertexMap.get("Source: source"), ResultPartitionType.PIPELINED_BOUNDED);
        assertHasOutputPartitionType(
                vertexMap.get("transform -> Map"), ResultPartitionType.BLOCKING);
        assertThat(vertexMap.get("Source: source").isAnyOutputBlocking()).isFalse();
        assertThat(vertexMap.get("transform -> Map").isAnyOutputBlocking()).isTrue();
        assertThat(vertexMap.get("sink: Writer").isAnyOutputBlocking()).isFalse();

        env.disableOperatorChaining();
        jobGraph = StreamingJobGraphGenerator.createJobGraph(env.getStreamGraph(false));
        vertexMap = new HashMap<>();
        for (JobVertex vertex : jobGraph.getVertices()) {
            vertexMap.put(vertex.getName(), vertex);
        }
        assertThat(vertexMap).hasSize(4);
        assertHasOutputPartitionType(
                vertexMap.get("Source: source"), ResultPartitionType.PIPELINED_BOUNDED);
        assertHasOutputPartitionType(vertexMap.get("transform"), ResultPartitionType.BLOCKING);
        assertHasOutputPartitionType(vertexMap.get("Map"), ResultPartitionType.PIPELINED_BOUNDED);
        assertThat(vertexMap.get("Source: source").isAnyOutputBlocking()).isFalse();
        assertThat(vertexMap.get("transform").isAnyOutputBlocking()).isTrue();
        assertThat(vertexMap.get("Map").isAnyOutputBlocking()).isFalse();
        assertThat(vertexMap.get("sink: Writer").isAnyOutputBlocking()).isFalse();
    }

    @Test
    void testApplyBatchExecutionSettingsOnTwoInputOperator() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());

        final DataStream<Integer> source1 = env.fromData(1, 2, 3).name("source1");
        final DataStream<Integer> source2 = env.fromData(1, 2, 3).name("source2");
        source1.keyBy(x -> x)
                .connect(source2.keyBy(x -> x))
                .transform(
                        "transform",
                        Types.INT,
                        new TwoInputStreamOperatorWithConfigurableOperatorAttributes<>(
                                new OperatorAttributesBuilder()
                                        .setOutputOnlyAfterEndOfStream(true)
                                        .build()))
                .sinkTo(new DiscardingSink<>())
                .name("sink");

        final StreamGraph streamGraph = env.getStreamGraph(false);
        Map<String, StreamNode> nodeMap = new HashMap<>();
        for (StreamNode node : streamGraph.getStreamNodes()) {
            nodeMap.put(node.getOperatorName(), node);
        }
        assertThat(nodeMap).hasSize(4);
        assertManagedMemoryWeightsSize(nodeMap.get("Source: source1"), 0);
        assertManagedMemoryWeightsSize(nodeMap.get("Source: source2"), 0);
        assertManagedMemoryWeightsSize(nodeMap.get("transform"), 1);
        assertManagedMemoryWeightsSize(nodeMap.get("sink: Writer"), 0);
    }

    private static void assertManagedMemoryWeightsSize(StreamNode node, int weightSize) {
        assertThat(node.getManagedMemoryOperatorScopeUseCaseWeights()).hasSize(weightSize);
    }

    @Test
    void testOneInputOperatorWithInternalSorterSupported() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());

        final DataStream<Integer> source1 = env.fromData(1, 2, 3).name("source1");
        source1.keyBy(x -> x)
                .transform(
                        "internalSorter",
                        Types.INT,
                        new StreamOperatorWithConfigurableOperatorAttributes<>(
                                (MapFunction<Integer, Integer>) value -> value,
                                new OperatorAttributesBuilder()
                                        .setOutputOnlyAfterEndOfStream(true)
                                        .setInternalSorterSupported(true)
                                        .build()))
                .keyBy(x -> x)
                .transform(
                        "noInternalSorter",
                        Types.INT,
                        new StreamOperatorWithConfigurableOperatorAttributes<>(
                                (MapFunction<Integer, Integer>) value -> value,
                                new OperatorAttributesBuilder()
                                        .setOutputOnlyAfterEndOfStream(true)
                                        .build()))
                .sinkTo(new DiscardingSink<>())
                .name("sink");

        final StreamGraph streamGraph = env.getStreamGraph(false);
        Map<String, StreamNode> nodeMap = new HashMap<>();
        for (StreamNode node : streamGraph.getStreamNodes()) {
            nodeMap.put(node.getOperatorName(), node);
        }

        assertThat(nodeMap.get("internalSorter").getInputRequirements()).isEmpty();
        assertThat(nodeMap.get("noInternalSorter").getInputRequirements().get(0))
                .isEqualTo(StreamConfig.InputRequirement.SORTED);
    }

    @Test
    void testTwoInputOperatorWithInternalSorterSupported() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());

        final DataStream<Integer> source1 = env.fromData(1, 2, 3).name("source1");
        final DataStream<Integer> source2 = env.fromData(1, 2, 3).name("source2");
        source1.keyBy(x -> x)
                .connect(source2.keyBy(x -> x))
                .transform(
                        "internalSorter",
                        Types.INT,
                        new TwoInputStreamOperatorWithConfigurableOperatorAttributes<>(
                                new OperatorAttributesBuilder()
                                        .setOutputOnlyAfterEndOfStream(true)
                                        .setInternalSorterSupported(true)
                                        .build()))
                .keyBy(x -> x)
                .connect(source2.keyBy(x -> x))
                .transform(
                        "noInternalSorter",
                        Types.INT,
                        new TwoInputStreamOperatorWithConfigurableOperatorAttributes<>(
                                new OperatorAttributesBuilder()
                                        .setOutputOnlyAfterEndOfStream(true)
                                        .build()))
                .sinkTo(new DiscardingSink<>())
                .name("sink");

        final StreamGraph streamGraph = env.getStreamGraph(false);
        Map<String, StreamNode> nodeMap = new HashMap<>();
        for (StreamNode node : streamGraph.getStreamNodes()) {
            nodeMap.put(node.getOperatorName(), node);
        }

        assertThat(nodeMap.get("internalSorter").getInputRequirements()).isEmpty();
        assertThat(nodeMap.get("noInternalSorter").getInputRequirements().get(0))
                .isEqualTo(StreamConfig.InputRequirement.SORTED);
        assertThat(nodeMap.get("noInternalSorter").getInputRequirements().get(1))
                .isEqualTo(StreamConfig.InputRequirement.SORTED);
    }

    @Test
    void testMultipleInputOperatorWithInternalSorterSupported() {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(new Configuration());

        final DataStream<Integer> source1 = env.fromData(1, 2, 3).name("source1");
        final DataStream<Integer> source2 = env.fromData(1, 2, 3).name("source2");

        KeyedMultipleInputTransformation<Integer> transform =
                new KeyedMultipleInputTransformation<>(
                        "internalSorter",
                        new OperatorAttributesConfigurableOperatorFactory<>(
                                new OperatorAttributesBuilder()
                                        .setOutputOnlyAfterEndOfStream(true)
                                        .setInternalSorterSupported(true)
                                        .build()),
                        BasicTypeInfo.INT_TYPE_INFO,
                        3,
                        BasicTypeInfo.INT_TYPE_INFO);

        transform.addInput(source1.keyBy(x -> x).getTransformation(), x -> x);
        transform.addInput(source2.getTransformation(), null);

        KeyedMultipleInputTransformation<Integer> transform2 =
                new KeyedMultipleInputTransformation<>(
                        "noInternalSorter",
                        new OperatorAttributesConfigurableOperatorFactory<>(
                                new OperatorAttributesBuilder()
                                        .setOutputOnlyAfterEndOfStream(true)
                                        .build()),
                        BasicTypeInfo.INT_TYPE_INFO,
                        3,
                        BasicTypeInfo.INT_TYPE_INFO);

        transform2.addInput(transform, null);
        transform2.addInput(source2.keyBy(x -> x).getTransformation(), x -> x);

        new DataStream<>(env, transform2).sinkTo(new DiscardingSink<>());

        final StreamGraph streamGraph = env.getStreamGraph(false);
        Map<String, StreamNode> nodeMap = new HashMap<>();
        for (StreamNode node : streamGraph.getStreamNodes()) {
            nodeMap.put(node.getOperatorName(), node);
        }

        assertThat(nodeMap.get("internalSorter").getInputRequirements()).isEmpty();
        assertThat(nodeMap.get("noInternalSorter").getInputRequirements().get(0))
                .isEqualTo(StreamConfig.InputRequirement.PASS_THROUGH);
        assertThat(nodeMap.get("noInternalSorter").getInputRequirements().get(1))
                .isEqualTo(StreamConfig.InputRequirement.SORTED);
    }

    private static class StreamOperatorWithConfigurableOperatorAttributes<IN, OUT>
            extends StreamMap<IN, OUT> {
        private final OperatorAttributes attributes;

        public StreamOperatorWithConfigurableOperatorAttributes(
                MapFunction<IN, OUT> mapper, OperatorAttributes attributes) {
            super(mapper);
            this.attributes = attributes;
        }

        @Override
        public OperatorAttributes getOperatorAttributes() {
            return attributes;
        }
    }

    private static class TwoInputStreamOperatorWithConfigurableOperatorAttributes<IN1, IN2, OUT>
            extends CoProcessOperator<IN1, IN2, OUT> {
        private final OperatorAttributes attributes;

        public TwoInputStreamOperatorWithConfigurableOperatorAttributes(
                OperatorAttributes attributes) {
            super(new NoOpCoProcessFunction<>());
            this.attributes = attributes;
        }

        @Override
        public OperatorAttributes getOperatorAttributes() {
            return attributes;
        }
    }

    private static class OperatorAttributesConfigurableOperatorFactory<OUT>
            implements StreamOperatorFactory<OUT> {

        private final OperatorAttributes operatorAttributes;

        public OperatorAttributesConfigurableOperatorFactory(
                OperatorAttributes operatorAttributes) {
            this.operatorAttributes = operatorAttributes;
        }

        @Override
        public <T extends StreamOperator<OUT>> T createStreamOperator(
                StreamOperatorParameters<OUT> parameters) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setChainingStrategy(ChainingStrategy strategy) {}

        @Override
        public ChainingStrategy getChainingStrategy() {
            return ChainingStrategy.DEFAULT_CHAINING_STRATEGY;
        }

        @Override
        public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
            return StreamMap.class;
        }

        @Override
        public OperatorAttributes getOperatorAttributes() {
            return operatorAttributes;
        }
    }

    private static class NoOpCoProcessFunction<IN1, IN2, OUT>
            extends CoProcessFunction<IN1, IN2, OUT> {
        @Override
        public void processElement1(
                IN1 value, CoProcessFunction<IN1, IN2, OUT>.Context ctx, Collector<OUT> out) {}

        @Override
        public void processElement2(
                IN2 value, CoProcessFunction<IN1, IN2, OUT>.Context ctx, Collector<OUT> out) {}
    }

    private void assertHasOutputPartitionType(
            JobVertex jobVertex, ResultPartitionType partitionType) {
        assertThat(jobVertex.getProducedDataSets().get(0).getResultType()).isEqualTo(partitionType);
    }
}
