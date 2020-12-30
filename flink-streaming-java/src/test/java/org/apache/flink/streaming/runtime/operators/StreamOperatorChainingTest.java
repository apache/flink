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
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.operators.testutils.MockEnvironment;
import org.apache.flink.runtime.operators.testutils.MockEnvironmentBuilder;
import org.apache.flink.runtime.operators.testutils.MockInputSplitProvider;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.operators.StreamMap;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorChain;
import org.apache.flink.streaming.runtime.tasks.StreamOperatorWrapper;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.streaming.util.MockStreamTaskBuilder;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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

        Assert.assertTrue(jobGraph.getVerticesSortedTopologicallyFromSources().size() == 2);

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

            headOperator.setup(mockTask, streamConfig, operatorChain.getMainOperatorOutput());

            for (StreamOperatorWrapper<?, ?> operatorWrapper :
                    operatorChain.getAllOperators(true)) {
                operatorWrapper.getStreamOperator().open();
            }

            headOperator.processElement(new StreamRecord<>(1));
            headOperator.processElement(new StreamRecord<>(2));
            headOperator.processElement(new StreamRecord<>(3));

            assertThat(sink1Results, contains("First: 1", "First: 2", "First: 3"));
            assertThat(sink2Results, contains("Second: 1", "Second: 2", "Second: 3"));
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

        Assert.assertTrue(jobGraph.getVerticesSortedTopologicallyFromSources().size() == 2);

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

            headOperator.setup(mockTask, streamConfig, operatorChain.getMainOperatorOutput());

            for (StreamOperatorWrapper<?, ?> operatorWrapper :
                    operatorChain.getAllOperators(true)) {
                operatorWrapper.getStreamOperator().open();
            }

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
        return new OperatorChain<>(
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
}
