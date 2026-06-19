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

package org.apache.flink.datastream.impl.attribute;

import org.apache.flink.api.connector.dsv2.DataStreamV2SourceUtils;
import org.apache.flink.api.connector.dsv2.WrappedSink;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.datastream.api.ExecutionEnvironment;
import org.apache.flink.datastream.api.attribute.NoOutputUntilEndOfInput;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.api.stream.NonKeyedPartitionStream;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.runtime.io.network.partition.ResultPartitionType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StreamingJobGraphGenerator} with different attributes. */
class StreamingJobGraphGeneratorWithAttributeTest {

    @Test
    void testNoOutputUntilEndOfInputWithOperatorChainCase1() throws Exception {
        ExecutionEnvironmentImpl env =
                (ExecutionEnvironmentImpl) ExecutionEnvironment.getInstance();
        NonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(Arrays.asList(1, 2, 3)), "test-source");
        source.keyBy(x -> x)
                .process(new NoOutputUntilEndOfInputMapTask())
                .withParallelism(2)
                .process(new TestMapTask())
                .withParallelism(2)
                .toSink(new WrappedSink<>(new DiscardingSink<>()))
                .withParallelism(3);
        StreamGraph streamGraph = env.getStreamGraph();
        Map<String, StreamNode> nodeMap = new HashMap<>();
        for (StreamNode node : streamGraph.getStreamNodes()) {
            nodeMap.put(node.getOperatorName(), node);
        }
        assertThat(nodeMap).hasSize(4);
        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
        Map<String, JobVertex> vertexMap = new HashMap<>();
        for (JobVertex vertex : jobGraph.getVertices()) {
            vertexMap.put(vertex.getName(), vertex);
        }
        assertThat(vertexMap).hasSize(3);
        assertHasOutputPartitionType(
                vertexMap.get("Source: Collection Source"), ResultPartitionType.PIPELINED_BOUNDED);
        assertHasOutputPartitionType(
                vertexMap.get("KeyedProcess -> Process"), ResultPartitionType.BLOCKING);
        assertThat(vertexMap.get("Source: Collection Source").isAnyOutputBlocking()).isFalse();
        assertThat(vertexMap.get("KeyedProcess -> Process").isAnyOutputBlocking()).isTrue();
        assertThat(vertexMap.get("Sink: Writer").isAnyOutputBlocking()).isFalse();
    }

    @Test
    void testPropagateAlongOperatorChain() throws Exception {
        ExecutionEnvironmentImpl env =
                (ExecutionEnvironmentImpl) ExecutionEnvironment.getInstance();
        NonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(Arrays.asList(1, 2, 3)), "test-source");
        source.keyBy(x -> x)
                .process(new TestMapTask())
                .withParallelism(2)
                .process(new NoOutputUntilEndOfInputMapTask())
                .withParallelism(2)
                .toSink(new WrappedSink<>(new DiscardingSink<>()))
                .withParallelism(3);
        StreamGraph streamGraph = env.getStreamGraph();
        Map<String, StreamNode> nodeMap = new HashMap<>();
        for (StreamNode node : streamGraph.getStreamNodes()) {
            nodeMap.put(node.getOperatorName(), node);
        }
        assertThat(nodeMap).hasSize(4);
        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
        Map<String, JobVertex> vertexMap = new HashMap<>();
        for (JobVertex vertex : jobGraph.getVertices()) {
            vertexMap.put(vertex.getName(), vertex);
        }
        assertThat(vertexMap).hasSize(3);
        assertHasOutputPartitionType(
                vertexMap.get("Source: Collection Source"), ResultPartitionType.PIPELINED_BOUNDED);
        assertHasOutputPartitionType(
                vertexMap.get("KeyedProcess -> Process"), ResultPartitionType.BLOCKING);
        assertThat(vertexMap.get("Source: Collection Source").isAnyOutputBlocking()).isFalse();
        assertThat(vertexMap.get("KeyedProcess -> Process").isAnyOutputBlocking()).isTrue();
        assertThat(vertexMap.get("Sink: Writer").isAnyOutputBlocking()).isFalse();
    }

    @Test
    void testTwoOutput() throws Exception {
        ExecutionEnvironmentImpl env =
                (ExecutionEnvironmentImpl) ExecutionEnvironment.getInstance();
        NonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(Arrays.asList(1, 2, 3)), "test-source");
        NonKeyedPartitionStream.ProcessConfigurableAndTwoNonKeyedPartitionStream<Integer, Integer>
                twoOutputStream =
                        source.process(new TestMapTask())
                                .withParallelism(2)
                                .process(new TestTwoOutputProcessFunction())
                                .withParallelism(2);
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer> firstStream =
                twoOutputStream.getFirst();
        NonKeyedPartitionStream.ProcessConfigurableAndNonKeyedPartitionStream<Integer>
                secondStream = twoOutputStream.getSecond();
        firstStream
                .process(new NoOutputUntilEndOfInputMapTask())
                .withParallelism(2)
                .toSink(new WrappedSink<>(new DiscardingSink<>()))
                .withParallelism(3);
        secondStream
                .process(new TestMapTask())
                .withParallelism(2)
                .toSink(new WrappedSink<>(new DiscardingSink<>()))
                .withParallelism(3);
        StreamGraph streamGraph = env.getStreamGraph();
        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
        Map<String, JobVertex> vertexMap = new HashMap<>();
        for (JobVertex vertex : jobGraph.getVertices()) {
            vertexMap.put(vertex.getName(), vertex);
        }
        assertThat(vertexMap).hasSize(3);
        assertHasOutputPartitionType(
                vertexMap.get("Process -> Two-Output-Operator -> (Process, Process)"),
                ResultPartitionType.BLOCKING);
        assertThat(
                        vertexMap
                                .get("Process -> Two-Output-Operator -> (Process, Process)")
                                .isAnyOutputBlocking())
                .isTrue();
    }

    @Test
    void testWithoutOperatorChain() throws Exception {
        ExecutionEnvironmentImpl env =
                (ExecutionEnvironmentImpl) ExecutionEnvironment.getInstance();
        env.getConfiguration().set(PipelineOptions.OPERATOR_CHAINING, false);
        NonKeyedPartitionStream<Integer> source =
                env.fromSource(
                        DataStreamV2SourceUtils.fromData(Arrays.asList(1, 2, 3)), "test-source");
        source.keyBy(x -> x)
                .process(new NoOutputUntilEndOfInputMapTask())
                .withParallelism(2)
                .process(new TestMapTask())
                .withParallelism(2)
                .toSink(new WrappedSink<>(new DiscardingSink<>()))
                .withParallelism(3);
        StreamGraph streamGraph = env.getStreamGraph();
        Map<String, StreamNode> nodeMap = new HashMap<>();
        for (StreamNode node : streamGraph.getStreamNodes()) {
            nodeMap.put(node.getOperatorName(), node);
        }
        assertThat(nodeMap).hasSize(4);
        JobGraph jobGraph = StreamingJobGraphGenerator.createJobGraph(streamGraph);
        Map<String, JobVertex> vertexMap = new HashMap<>();
        for (JobVertex vertex : jobGraph.getVertices()) {
            vertexMap.put(vertex.getName(), vertex);
        }
        assertThat(vertexMap).hasSize(4);
        assertHasOutputPartitionType(
                vertexMap.get("Source: Collection Source"), ResultPartitionType.PIPELINED_BOUNDED);
        assertHasOutputPartitionType(vertexMap.get("KeyedProcess"), ResultPartitionType.BLOCKING);
        assertHasOutputPartitionType(
                vertexMap.get("Process"), ResultPartitionType.PIPELINED_BOUNDED);
        assertThat(vertexMap.get("Source: Collection Source").isAnyOutputBlocking()).isFalse();
        assertThat(vertexMap.get("KeyedProcess").isAnyOutputBlocking()).isTrue();
        assertThat(vertexMap.get("Process").isAnyOutputBlocking()).isFalse();
        assertThat(vertexMap.get("Sink: Writer").isAnyOutputBlocking()).isFalse();
    }

    private void assertHasOutputPartitionType(
            JobVertex jobVertex, ResultPartitionType partitionType) {
        assertThat(jobVertex.getProducedDataSets().get(0).getResultType()).isEqualTo(partitionType);
    }

    @NoOutputUntilEndOfInput
    private static class NoOutputUntilEndOfInputMapTask
            implements OneInputStreamProcessFunction<Integer, Integer> {

        @Override
        public void processRecord(
                Integer record, Collector<Integer> output, PartitionedContext<Integer> ctx) {
            output.collect(record + 1);
        }
    }

    private static class TestMapTask implements OneInputStreamProcessFunction<Integer, Integer> {

        @Override
        public void processRecord(
                Integer record, Collector<Integer> output, PartitionedContext<Integer> ctx) {
            if (record != 2) {
                output.collect(record + 1);
            }
        }
    }

    /** The test {@link TwoOutputStreamProcessFunction}. */
    private static class TestTwoOutputProcessFunction
            implements TwoOutputStreamProcessFunction<Integer, Integer, Integer> {

        @Override
        public void processRecord(
                Integer record,
                Collector<Integer> output1,
                Collector<Integer> output2,
                TwoOutputPartitionedContext<Integer, Integer> ctx) {
            output1.collect(record + 1);
            output2.collect(record - 1);
        }
    }
}
