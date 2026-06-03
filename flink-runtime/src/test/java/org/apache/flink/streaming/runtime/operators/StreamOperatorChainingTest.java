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

package org.apache.flink.streaming.runtime.operators;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.io.AvailabilityProvider;
import org.apache.flink.runtime.io.network.api.writer.AvailabilityTestResultPartitionWriter;
import org.apache.flink.runtime.io.network.api.writer.RecordWriterDelegate;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.runtime.plugable.SerializationDelegate;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.async.AsyncWaitOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.RegularOperatorChain;
import org.apache.flink.streaming.runtime.tasks.StreamOperatorWrapper;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.api.operators.StreamOperatorUtils.setupStreamOperator;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for stream operator chaining behaviour. */
@SuppressWarnings("serial")
class StreamOperatorChainingTest {

    // We have to use static fields because the sink functions will go through serialization
    private static List<String> sink1Results;
    private static List<String> sink2Results;
    private static List<String> sink3Results;

    @Test
    void testMultiChainingWithObjectReuse() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        testMultiChaining(env);
    }

    @Test
    void testMultiChainingWithoutObjectReuse() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableObjectReuse();

        testMultiChaining(env);
    }

    /** Verify that multi-chaining works. */
    private void testMultiChaining(StreamExecutionEnvironment env) throws Exception {

        // set parallelism to 2 to avoid chaining with source in case when available processors is
        // 1.
        env.setParallelism(2);

        // the actual elements will not be used
        DataStream<Integer> input = env.fromData(1, 2, 3);

        sink1Results = new ArrayList<>();
        sink2Results = new ArrayList<>();

        input = input.map(value -> value);

        input.map(value -> "First: " + value)
                .addSink(
                        new SinkFunction<String>() {

                            @Override
                            public void invoke(String value, Context ctx) throws Exception {
                                sink1Results.add(value);
                            }
                        });

        input.map(value -> "Second: " + value)
                .addSink(
                        new SinkFunction<String>() {

                            @Override
                            public void invoke(String value, Context ctx) throws Exception {
                                sink2Results.add(value);
                            }
                        });

        // be build our own StreamTask and OperatorChain
        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        assertThat(jobGraph.getVerticesSortedTopologicallyFromSources()).hasSize(2);

        JobVertex chainedVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);

        Configuration configuration = chainedVertex.getConfiguration();

        StreamConfig streamConfig = new StreamConfig(configuration);

        StreamMap<Integer, Integer> headOperator =
                streamConfig.getStreamOperator(Thread.currentThread().getContextClassLoader());

        try (MockEnvironment environment = createMockEnvironment(chainedVertex.getName())) {
            StreamTask<Integer, StreamMap<Integer, Integer>> mockTask =
                    createMockTask(streamConfig, environment);
            OperatorChain<Integer, StreamMap<Integer, Integer>> operatorChain =
                    createOperatorChain(streamConfig, environment, mockTask);

            setupStreamOperator(
                    headOperator, mockTask, streamConfig, operatorChain.getMainOperatorOutput());

            operatorChain.initializeStateAndOpenOperators(null);

            headOperator.processElement(new StreamRecord<>(1));
            headOperator.processElement(new StreamRecord<>(2));
            headOperator.processElement(new StreamRecord<>(3));

            assertThat(sink1Results).containsExactly("First: 1", "First: 2", "First: 3");
            assertThat(sink2Results).containsExactly("Second: 1", "Second: 2", "Second: 3");
        }
    }

    private MockEnvironment createMockEnvironment(String taskName) {
        return new MockEnvironmentBuilder()
                .setTaskName(taskName)
                .setManagedMemorySize(3 * 1024 * 1024)
                .setInputSplitProvider(new MockInputSplitProvider())
                .setBufferSize(1024)
                .build();
    }

    @Test
    void testMultiChainingWithSplitWithObjectReuse() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        testMultiChainingWithSplit(env);
    }

    @Test
    void testMultiChainingWithSplitWithoutObjectReuse() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().disableObjectReuse();

        testMultiChainingWithSplit(env);
    }

    /** Verify that multi-chaining works with object reuse enabled. */
    private void testMultiChainingWithSplit(StreamExecutionEnvironment env) throws Exception {

        // set parallelism to 2 to avoid chaining with source in case when available processors is
        // 1.
        env.setParallelism(2);

        // the actual elements will not be used
        DataStream<Integer> input = env.fromData(1, 2, 3);

        sink1Results = new ArrayList<>();
        sink2Results = new ArrayList<>();
        sink3Results = new ArrayList<>();

        input = input.map(value -> value);

        OutputTag<Integer> oneOutput = new OutputTag<Integer>("one") {};
        OutputTag<Integer> otherOutput = new OutputTag<Integer>("other") {};
        SingleOutputStreamOperator<Object> split =
                input.process(
                        new ProcessFunction<Integer, Object>() {
                            private static final long serialVersionUID = 1L;

                            @Override
                            public void processElement(
                                    Integer value, Context ctx, Collector<Object> out)
                                    throws Exception {
                                if (value.equals(1)) {
                                    ctx.output(oneOutput, value);
                                } else {
                                    ctx.output(otherOutput, value);
                                }
                            }
                        });

        split.getSideOutput(oneOutput)
                .map(value -> "First 1: " + value)
                .addSink(
                        new SinkFunction<String>() {

                            @Override
                            public void invoke(String value, Context ctx) throws Exception {
                                sink1Results.add(value);
                            }
                        });

        split.getSideOutput(oneOutput)
                .map(value -> "First 2: " + value)
                .addSink(
                        new SinkFunction<String>() {

                            @Override
                            public void invoke(String value, Context ctx) throws Exception {
                                sink2Results.add(value);
                            }
                        });

        split.getSideOutput(otherOutput)
                .map(value -> "Second: " + value)
                .addSink(
                        new SinkFunction<String>() {

                            @Override
                            public void invoke(String value, Context ctx) throws Exception {
                                sink3Results.add(value);
                            }
                        });

        // be build our own StreamTask and OperatorChain
        JobGraph jobGraph = env.getStreamGraph().getJobGraph();

        assertThat(jobGraph.getVerticesSortedTopologicallyFromSources()).hasSize(2);

        JobVertex chainedVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);

        Configuration configuration = chainedVertex.getConfiguration();

        StreamConfig streamConfig = new StreamConfig(configuration);

        StreamMap<Integer, Integer> headOperator =
                streamConfig.getStreamOperator(Thread.currentThread().getContextClassLoader());

        try (MockEnvironment environment = createMockEnvironment(chainedVertex.getName())) {
            StreamTask<Integer, StreamMap<Integer, Integer>> mockTask =
                    createMockTask(streamConfig, environment);
            OperatorChain<Integer, StreamMap<Integer, Integer>> operatorChain =
                    createOperatorChain(streamConfig, environment, mockTask);

            setupStreamOperator(
                    headOperator, mockTask, streamConfig, operatorChain.getMainOperatorOutput());

            operatorChain.initializeStateAndOpenOperators(null);

            headOperator.processElement(new StreamRecord<>(1));
            headOperator.processElement(new StreamRecord<>(2));
            headOperator.processElement(new StreamRecord<>(3));

            assertThat(sink1Results).containsExactly("First 1: 1");
            assertThat(sink2Results).containsExactly("First 2: 1");
            assertThat(sink3Results).containsExactly("Second: 2", "Second: 3");
        }
    }

    /**
     * Exercises the head-async branch of {@link OperatorChain}: when the main operator is an {@link
     * AsyncWaitOperator}, the chain availability provider must be the head async operator itself
     * (the head represents the entire chain to {@link StreamTask}).
     */
    @Test
    void testChainAvailabilityProviderForHeadAsyncOperator() throws Exception {
        JobVertex chainedVertex = buildChainedVertexWithAsyncOperator(true);

        StreamConfig streamConfig = new StreamConfig(chainedVertex.getConfiguration());

        try (MockEnvironment environment = createMockEnvironment(chainedVertex.getName())) {
            environment.addOutputs(
                    Collections.singletonList(new AvailabilityTestResultPartitionWriter(true)));
            StreamTask<Integer, AsyncWaitOperator<Integer, Integer>> mockTask =
                    createMockTask(streamConfig, environment);
            OperatorChain<Integer, AsyncWaitOperator<Integer, Integer>> operatorChain =
                    createOperatorChain(streamConfig, environment, mockTask);

            AvailabilityProvider chainAvailability =
                    operatorChain.getOperatorChainAvailabilityProvider();
            assertThat(chainAvailability)
                    .as("Head async operator should represent chain availability")
                    .isSameAs(operatorChain.getMainOperator());
            assertThat(chainAvailability.isAvailable())
                    .as("Empty async queue is initially available")
                    .isTrue();
        }
    }

    /**
     * Exercises the chained-async branch of {@link OperatorChain}: when an {@link
     * AsyncWaitOperator} sits behind the head, it replaces the downstream leaves in the parent's
     * collector, so the chain availability provider must be that chained async operator.
     */
    @Test
    void testChainAvailabilityProviderForChainedAsyncOperator() throws Exception {
        JobVertex chainedVertex = buildChainedVertexWithAsyncOperator(false);

        StreamConfig streamConfig = new StreamConfig(chainedVertex.getConfiguration());

        try (MockEnvironment environment = createMockEnvironment(chainedVertex.getName())) {
            environment.addOutputs(
                    Collections.singletonList(new AvailabilityTestResultPartitionWriter(true)));
            StreamTask<Integer, StreamMap<Integer, Integer>> mockTask =
                    createMockTask(streamConfig, environment);
            OperatorChain<Integer, StreamMap<Integer, Integer>> operatorChain =
                    createOperatorChain(streamConfig, environment, mockTask);

            AsyncWaitOperator<?, ?> chainedAsync = findSingleAsyncOperator(operatorChain);
            AvailabilityProvider chainAvailability =
                    operatorChain.getOperatorChainAvailabilityProvider();
            assertThat(chainAvailability)
                    .as(
                            "Chained async operator should replace downstream leaves and act as"
                                    + " chain availability boundary")
                    .isSameAs(chainedAsync);
            assertThat(chainAvailability)
                    .as("Head operator (not the async one) is not the chain availability provider")
                    .isNotSameAs(operatorChain.getMainOperator());
            assertThat(chainAvailability.isAvailable())
                    .as("Empty async queue is initially available")
                    .isTrue();
        }
    }

    private static JobVertex buildChainedVertexWithAsyncOperator(boolean asyncAtHead) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // parallelism 2 keeps the source in its own vertex.
        env.setParallelism(2);

        DataStream<Integer> input = env.fromData(1, 2, 3);

        if (asyncAtHead) {
            // Vertex layout: [AsyncWait (head) -> Map] -> network -> [Sink]
            input =
                    AsyncDataStream.orderedWait(
                            input, new NoOpAsyncFunction(), 1000L, TimeUnit.MILLISECONDS, 4);
            input = input.map(value -> value);
        } else {
            // Vertex layout: [Map (head) -> AsyncWait] -> network -> [Sink]
            input = input.map(value -> value);
            input =
                    AsyncDataStream.orderedWait(
                            input, new NoOpAsyncFunction(), 1000L, TimeUnit.MILLISECONDS, 4);
        }

        // startNewChain forces the sink into its own vertex, so the chained vertex under test ends
        // with a network RecordWriter as its leaf.
        input.map(value -> value).startNewChain().sinkTo(new DiscardingSink<>());

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        assertThat(jobGraph.getVerticesSortedTopologicallyFromSources()).hasSize(3);
        return jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
    }

    private static AsyncWaitOperator<?, ?> findSingleAsyncOperator(OperatorChain<?, ?> chain) {
        AsyncWaitOperator<?, ?> found = null;
        for (StreamOperatorWrapper<?, ?> wrapper : chain.getAllOperators()) {
            if (wrapper.getStreamOperator() instanceof AsyncWaitOperator) {
                assertThat(found).as("Expected exactly one AsyncWaitOperator in chain").isNull();
                found = (AsyncWaitOperator<?, ?>) wrapper.getStreamOperator();
            }
        }
        assertThat(found).as("Expected an AsyncWaitOperator in chain").isNotNull();
        return found;
    }

    private static class NoOpAsyncFunction implements AsyncFunction<Integer, Integer> {
        private static final long serialVersionUID = 1L;

        @Override
        public void asyncInvoke(Integer input, ResultFuture<Integer> resultFuture) {
            // Tests only inspect chain wiring; the function is never invoked.
        }
    }

    private <IN, OT extends StreamOperator<IN>> OperatorChain<IN, OT> createOperatorChain(
            StreamConfig streamConfig, Environment environment, StreamTask<IN, OT> task) {
        return new TestOperatorChain<>(
                task, StreamTask.createRecordWriterDelegate(streamConfig, environment));
    }

    private <IN, OT extends StreamOperator<IN>> StreamTask<IN, OT> createMockTask(
            StreamConfig streamConfig, Environment environment) throws Exception {

        //noinspection unchecked
        return new MockStreamTaskBuilder(environment)
                .setConfig(streamConfig)
                .setExecutionConfig(new ExecutionConfig().enableObjectReuse())
                .build();
    }

    private static class TestOperatorChain<IN, OUT extends StreamOperator<IN>>
            extends RegularOperatorChain<IN, OUT> {
        public TestOperatorChain(
                StreamTask<IN, OUT> task,
                RecordWriterDelegate<SerializationDelegate<StreamRecord<IN>>>
                        recordWriterDelegate) {
            super(task, recordWriterDelegate);
        }

        @Override
        public void initializeStateAndOpenOperators(
                StreamTaskStateInitializer streamTaskStateInitializer) throws Exception {
            for (StreamOperatorWrapper<?, ?> operatorWrapper : getAllOperators(true)) {
                StreamOperator<?> operator = operatorWrapper.getStreamOperator();
                operator.open();
            }
        }
    }
}
