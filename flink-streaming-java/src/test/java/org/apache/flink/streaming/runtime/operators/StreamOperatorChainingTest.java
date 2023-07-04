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
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.execution.Environment;
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
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OperatorAttributes;
import org.apache.flink.streaming.api.operators.OperatorAttributesBuilder;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.RegularOperatorChain;
import org.apache.flink.streaming.runtime.tasks.StreamOperatorWrapper;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

/** Tests for stream operator chaining behaviour. */
@SuppressWarnings("serial")
public class StreamOperatorChainingTest {

    // We have to use static fields because the sink functions will go through serialization
    private static List<String> sink1Results;
    private static List<String> sink2Results;
    private static List<String> sink3Results;

    @Test
    public void testMultiChainingWithObjectReuse() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        testMultiChaining(env);
    }

    @Test
    public void testMultiChainingWithoutObjectReuse() throws Exception {
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
        DataStream<Integer> input = env.fromElements(1, 2, 3);

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

        Assert.assertEquals(2, jobGraph.getVerticesSortedTopologicallyFromSources().size());

        JobVertex chainedVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);

        Configuration configuration = chainedVertex.getConfiguration();

        StreamConfig streamConfig = new StreamConfig(configuration);

        StreamMap<Integer, Integer> headOperator =
                streamConfig.getStreamOperator(Thread.currentThread().getContextClassLoader());

        try (MockEnvironment environment =
                createMockEnvironment(chainedVertex.getName(), env.getConfig())) {
            StreamTask<Integer, StreamMap<Integer, Integer>> mockTask =
                    createMockTask(streamConfig, environment);
            OperatorChain<Integer, StreamMap<Integer, Integer>> operatorChain =
                    createOperatorChain(streamConfig, environment, mockTask);

            headOperator.setup(mockTask, streamConfig, operatorChain.getMainOperatorOutput());

            operatorChain.initializeStateAndOpenOperators(null);

            headOperator.processElement(new StreamRecord<>(1));
            headOperator.processElement(new StreamRecord<>(2));
            headOperator.processElement(new StreamRecord<>(3));

            assertThat(sink1Results, contains("First: 1", "First: 2", "First: 3"));
            assertThat(sink2Results, contains("Second: 1", "Second: 2", "Second: 3"));
        }
    }

    private MockEnvironment createMockEnvironment(
            String taskName, ExecutionConfig executionConfig) {
        return new MockEnvironmentBuilder()
                .setTaskName(taskName)
                .setExecutionConfig(executionConfig)
                .setManagedMemorySize(3 * 1024 * 1024)
                .setInputSplitProvider(new MockInputSplitProvider())
                .setBufferSize(1024)
                .build();
    }

    @Test
    public void testMultiChainingWithSplitWithObjectReuse() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().enableObjectReuse();

        testMultiChainingWithSplit(env);
    }

    @Test
    public void testMultiChainingWithSplitWithoutObjectReuse() throws Exception {
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
        DataStream<Integer> input = env.fromElements(1, 2, 3);

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

        Assert.assertEquals(2, jobGraph.getVerticesSortedTopologicallyFromSources().size());

        JobVertex chainedVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);

        Configuration configuration = chainedVertex.getConfiguration();

        StreamConfig streamConfig = new StreamConfig(configuration);

        StreamMap<Integer, Integer> headOperator =
                streamConfig.getStreamOperator(Thread.currentThread().getContextClassLoader());

        try (MockEnvironment environment =
                createMockEnvironment(chainedVertex.getName(), env.getConfig())) {
            StreamTask<Integer, StreamMap<Integer, Integer>> mockTask =
                    createMockTask(streamConfig, environment);
            OperatorChain<Integer, StreamMap<Integer, Integer>> operatorChain =
                    createOperatorChain(streamConfig, environment, mockTask);

            headOperator.setup(mockTask, streamConfig, operatorChain.getMainOperatorOutput());

            operatorChain.initializeStateAndOpenOperators(null);

            headOperator.processElement(new StreamRecord<>(1));
            headOperator.processElement(new StreamRecord<>(2));
            headOperator.processElement(new StreamRecord<>(3));

            assertThat(sink1Results, contains("First 1: 1"));
            assertThat(sink2Results, contains("First 2: 1"));
            assertThat(sink3Results, contains("Second: 2", "Second: 3"));
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
                .setExecutionConfig(environment.getExecutionConfig())
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

    @Test
    public void testChainOperatorObjectReuseCompliant() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set parallelism to 2 to avoid chaining with source in case when available processors is
        // 1.
        env.setParallelism(2);

        // the actual elements will not be used
        DataStream<Record> input = env.fromElements(Record.of("a"));

        // op1 -object-reuse-> op2 -deep-copy-> op3
        final TestOperator op1 = new TestOperator("op1", false);
        input = input.transform("op1", Types.POJO(Record.class), op1);
        final TestOperator op2 = new TestOperator("op2", true);
        input = input.transform("op2", Types.POJO(Record.class), op2);
        final TestOperator op3 = new TestOperator("op3", false);
        input = input.transform("op3", Types.POJO(Record.class), op3);

        input.addSink(new DiscardingSink<>());

        List<Record> inputs = Arrays.asList(Record.of("a"), Record.of("b"));
        runJob(env, inputs);

        assertThat(op1.getInputs().stream().map(r -> r.value).collect(Collectors.toList()))
                .containsExactly("a", "b");
        assertThat(op1.getOutputs()).containsExactlyElementsOf(op2.getInputs());
        assertThat(op2.getInputs().stream().map(r -> r.value).collect(Collectors.toList()))
                .containsExactly("a-op1", "b-op1");
        assertThat(op2.getOutputs()).doesNotContainAnyElementsOf(op3.getInputs());
        assertThat(op3.getInputs().stream().map(r -> r.value).collect(Collectors.toList()))
                .containsExactly("a-op1-op2", "b-op1-op2");
    }

    @Test
    public void testMultiChainOperatorObjectReuseCompliant() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // set parallelism to 2 to avoid chaining with source in case when available processors is
        // 1.
        env.setParallelism(2);

        // the actual elements will not be used
        DataStream<Record> input = env.fromElements(Record.of("a"));

        // op1 -object-reuse-> op2
        //     -object-reuse-> op3 -deep-copy-> op4
        //                         -deep-copy-> op5
        final TestOperator op1 = new TestOperator("op1", false);
        input = input.transform("op1", Types.POJO(Record.class), op1);
        final TestOperator op2 = new TestOperator("op2", false);
        input.transform("op2", Types.POJO(Record.class), op2).addSink(new DiscardingSink<>());
        final TestOperator op3 = new TestOperator("op3", true);
        input = input.transform("op3", Types.POJO(Record.class), op3);
        final TestOperator op4 = new TestOperator("op4", false);
        input.transform("op4", Types.POJO(Record.class), op4).addSink(new DiscardingSink<>());
        final TestOperator op5 = new TestOperator("op5", false);
        input.transform("op5", Types.POJO(Record.class), op5).addSink(new DiscardingSink<>());

        List<Record> inputs = Arrays.asList(Record.of("a"), Record.of("b"));
        runJob(env, inputs);

        assertThat(op1.getInputs().stream().map(r -> r.value).collect(Collectors.toList()))
                .containsExactly("a", "b");
        assertThat(op1.getOutputs()).containsExactlyElementsOf(op2.getInputs());
        assertThat(op2.getInputs()).containsExactlyElementsOf(op3.getInputs());
        assertThat(op2.getInputs().stream().map(r -> r.value).collect(Collectors.toList()))
                .containsExactly("a-op1", "b-op1");

        assertThat(op3.getOutputs()).doesNotContainAnyElementsOf(op4.getInputs());
        assertThat(op3.getOutputs()).doesNotContainAnyElementsOf(op5.getInputs());
        assertThat(op4.getInputs()).doesNotContainAnyElementsOf(op5.getInputs());
        assertThat(op4.getInputs().stream().map(r -> r.value).collect(Collectors.toList()))
                .containsExactly("a-op1-op3", "b-op1-op3");
        assertThat(op5.getInputs().stream().map(r -> r.value).collect(Collectors.toList()))
                .containsExactly("a-op1-op3", "b-op1-op3");
    }

    private void runJob(StreamExecutionEnvironment env, List<Record> inputs) throws Exception {
        final JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        Assert.assertEquals(2, jobGraph.getVerticesSortedTopologicallyFromSources().size());

        JobVertex chainedVertex = jobGraph.getVerticesSortedTopologicallyFromSources().get(1);

        Configuration configuration = chainedVertex.getConfiguration();

        StreamConfig streamConfig = new StreamConfig(configuration);

        TestOperator headOperator =
                streamConfig.getStreamOperator(Thread.currentThread().getContextClassLoader());

        try (MockEnvironment environment =
                createMockEnvironment(chainedVertex.getName(), env.getConfig())) {
            StreamTask<Record, TestOperator> mockTask = createMockTask(streamConfig, environment);
            OperatorChain<Record, TestOperator> operatorChain =
                    createOperatorChain(streamConfig, environment, mockTask);

            headOperator.setup(mockTask, streamConfig, operatorChain.getMainOperatorOutput());

            operatorChain.initializeStateAndOpenOperators(null);

            for (Record input : inputs) {
                headOperator.processElement(new StreamRecord<>(input));
            }
        }
    }

    private static class TestOperator extends AbstractStreamOperator<Record>
            implements OneInputStreamOperator<Record, Record> {

        private static final Map<String, List<Record>> inputsMap = new HashMap<>();
        private static final Map<String, List<Record>> outputsMap = new HashMap<>();

        private final String operatorName;
        private final boolean outputStreamRecordValueStored;
        private final List<OutputTag<Record>> outputTags;

        public TestOperator(String operatorName, boolean outputStreamRecordValueStored) {
            this(operatorName, outputStreamRecordValueStored, Collections.emptyList());
        }

        public TestOperator(
                String operatorName,
                boolean outputStreamRecordValueStored,
                List<OutputTag<Record>> outputTags) {
            inputsMap.put(operatorName, new ArrayList<>());
            outputsMap.put(operatorName, new ArrayList<>());
            this.operatorName = operatorName;
            this.outputStreamRecordValueStored = outputStreamRecordValueStored;
            this.outputTags = outputTags;
            this.chainingStrategy = ChainingStrategy.ALWAYS;
        }

        @Override
        public void processElement(StreamRecord<Record> element) throws Exception {
            final Record input = element.getValue();
            getInputs().add(input);

            final Record output = Record.copy(input);
            output.value += "-";
            output.value += operatorName;
            getOutputs().add(output);

            final StreamRecord<Record> outputStreamRecord = element.copy(output);
            this.output.collect(outputStreamRecord);

            for (OutputTag<Record> outputTag : outputTags) {
                this.output.collect(outputTag, outputStreamRecord);
            }
        }

        public List<Record> getInputs() {
            return inputsMap.get(operatorName);
        }

        public List<Record> getOutputs() {
            return outputsMap.get(operatorName);
        }

        @Override
        public OperatorAttributes getOperatorAttributes() {
            return new OperatorAttributesBuilder()
                    .setOutputStreamRecordValueStored(this.outputStreamRecordValueStored)
                    .build();
        }
    }

    /** Record class for testing. */
    public static class Record {
        public String value;

        public Record() {}

        public Record(String value) {
            this.value = value;
        }

        public static Record of(String value) {
            return new Record(value);
        }

        public static Record copy(Record r) {
            return Record.of(r.value);
        }
    }
}
