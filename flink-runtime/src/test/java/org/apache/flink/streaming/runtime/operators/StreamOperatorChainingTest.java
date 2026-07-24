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
import org.apache.flink.api.common.functions.MapFunction;
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
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.SupportsSoftBackpressure;
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
import java.util.concurrent.CompletableFuture;

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
     * Exercises the head branch of {@link OperatorChain}: when the main operator implements {@link
     * SupportsSoftBackpressure}, the chain availability provider must be the head operator itself.
     */
    @Test
    void testChainAvailabilityProviderForHeadSoftBackpressureOperator() throws Exception {
        JobVertex chainedVertex = buildChainedVertexWithSoftBackpressureOperator(true);

        StreamConfig streamConfig = new StreamConfig(chainedVertex.getConfiguration());

        try (MockEnvironment environment = createMockEnvironment(chainedVertex.getName())) {
            environment.addOutputs(
                    Collections.singletonList(new AvailabilityTestResultPartitionWriter(true)));
            StreamTask<Integer, SoftBackpressureMapOperator<Integer, Integer>> mockTask =
                    createMockTask(streamConfig, environment);
            OperatorChain<Integer, SoftBackpressureMapOperator<Integer, Integer>> operatorChain =
                    createOperatorChain(streamConfig, environment, mockTask);

            AvailabilityProvider chainAvailability =
                    operatorChain.getOperatorChainAvailabilityProvider();
            assertThat(chainAvailability)
                    .as("Head soft-backpressure operator should represent chain" + " availability")
                    .isSameAs(operatorChain.getMainOperator());
            assertThat(chainAvailability.isAvailable()).as("Operator starts available").isTrue();
        }
    }

    /**
     * Exercises the chained branch of {@link OperatorChain}: when a {@link
     * SupportsSoftBackpressure} operator sits behind the head, it replaces the downstream leaves in
     * the parent's collector, so the chain availability provider must be that chained operator.
     */
    @Test
    void testChainAvailabilityProviderForChainedSoftBackpressureOperator() throws Exception {
        JobVertex chainedVertex = buildChainedVertexWithSoftBackpressureOperator(false);

        StreamConfig streamConfig = new StreamConfig(chainedVertex.getConfiguration());

        try (MockEnvironment environment = createMockEnvironment(chainedVertex.getName())) {
            environment.addOutputs(
                    Collections.singletonList(new AvailabilityTestResultPartitionWriter(true)));
            StreamTask<Integer, StreamMap<Integer, Integer>> mockTask =
                    createMockTask(streamConfig, environment);
            OperatorChain<Integer, StreamMap<Integer, Integer>> operatorChain =
                    createOperatorChain(streamConfig, environment, mockTask);

            SupportsSoftBackpressure chainedSbp = findSingleSoftBackpressureOperator(operatorChain);
            AvailabilityProvider chainAvailability =
                    operatorChain.getOperatorChainAvailabilityProvider();
            assertThat(chainAvailability)
                    .as(
                            "Chained soft-backpressure operator should replace downstream"
                                    + " leaves and act as chain availability boundary")
                    .isSameAs(chainedSbp);
            assertThat(chainAvailability)
                    .as(
                            "Head operator (not the soft-backpressure one) is not the chain"
                                    + " availability provider")
                    .isNotSameAs(operatorChain.getMainOperator());
            assertThat(chainAvailability.isAvailable()).as("Operator starts available").isTrue();
        }
    }

    private static JobVertex buildChainedVertexWithSoftBackpressureOperator(
            boolean softBackpressureAtHead) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStream<Integer> input = env.fromData(1, 2, 3);

        if (softBackpressureAtHead) {
            input =
                    input.transform(
                            "SoftBackpressureMap",
                            input.getType(),
                            new SoftBackpressureMapOperator<>(value -> value));
            input = input.map(value -> value);
        } else {
            input = input.map(value -> value);
            input =
                    input.transform(
                            "SoftBackpressureMap",
                            input.getType(),
                            new SoftBackpressureMapOperator<>(value -> value));
        }

        input.map(value -> value)
                .startNewChain()
                .addSink(
                        new SinkFunction<Integer>() {
                            @Override
                            public void invoke(Integer value, Context ctx) {}
                        });

        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        assertThat(jobGraph.getVerticesSortedTopologicallyFromSources()).hasSize(3);
        return jobGraph.getVerticesSortedTopologicallyFromSources().get(1);
    }

    private static SupportsSoftBackpressure findSingleSoftBackpressureOperator(
            OperatorChain<?, ?> chain) {
        SupportsSoftBackpressure found = null;
        for (StreamOperatorWrapper<?, ?> wrapper : chain.getAllOperators()) {
            if (wrapper.getStreamOperator() instanceof SupportsSoftBackpressure) {
                assertThat(found)
                        .as("Expected exactly one SupportsSoftBackpressure operator in chain")
                        .isNull();
                found = (SupportsSoftBackpressure) wrapper.getStreamOperator();
            }
        }
        assertThat(found).as("Expected a SupportsSoftBackpressure operator in chain").isNotNull();
        return found;
    }

    /**
     * A {@link StreamMap} that also implements {@link SupportsSoftBackpressure} for testing
     * OperatorChain availability-provider wiring without depending on flink-streaming-java
     * operators.
     */
    private static class SoftBackpressureMapOperator<IN, OUT> extends StreamMap<IN, OUT>
            implements SupportsSoftBackpressure {

        private static final long serialVersionUID = 1L;

        SoftBackpressureMapOperator(MapFunction<IN, OUT> mapper) {
            super(mapper);
        }

        @Override
        public void setDownstreamAvailabilityProvider(AvailabilityProvider provider) {}

        @Override
        public CompletableFuture<?> getAvailableFuture() {
            return AvailabilityProvider.AVAILABLE;
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
