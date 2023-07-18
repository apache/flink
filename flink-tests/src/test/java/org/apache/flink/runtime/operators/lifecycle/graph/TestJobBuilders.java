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

package org.apache.flink.runtime.operators.lifecycle.graph;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.operators.lifecycle.TestJobWithDescription;
import org.apache.flink.runtime.operators.lifecycle.command.TestCommandDispatcher;
import org.apache.flink.runtime.operators.lifecycle.event.TestEventQueue;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.MultipleConnectedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.util.function.ThrowingConsumer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.apache.flink.api.common.restartstrategy.RestartStrategies.noRestart;
import static org.apache.flink.configuration.JobManagerOptions.EXECUTION_FAILOVER_STRATEGY;

/** Helper to build {@link TestJobWithDescription}. */
public class TestJobBuilders {

    /** {@link TestJobWithDescription} builder. */
    @FunctionalInterface
    public interface TestingGraphBuilder {
        TestJobWithDescription build(
                SharedObjects shared,
                ThrowingConsumer<Configuration, Exception> modifyConfig,
                ThrowingConsumer<StreamExecutionEnvironment, Exception> modifyEnvironment)
                throws Exception;
    }

    private TestJobBuilders() {}

    public static final TestingGraphBuilder SIMPLE_GRAPH_BUILDER =
            new TestingGraphBuilder() {
                @Override
                public TestJobWithDescription build(
                        SharedObjects shared,
                        ThrowingConsumer<Configuration, Exception> confConsumer,
                        ThrowingConsumer<StreamExecutionEnvironment, Exception> envConsumer)
                        throws Exception {

                    TestEventQueue eventQueue = TestEventQueue.createShared(shared);
                    TestCommandDispatcher commandQueue = TestCommandDispatcher.createShared(shared);
                    StreamExecutionEnvironment env = prepareEnv(confConsumer, envConsumer);

                    // using hashes so that operators emit identifiable events
                    String unitedSourceLeft = OP_ID_HASH_PREFIX + "1";
                    String mapForward = OP_ID_HASH_PREFIX + "5";

                    DataStream<TestDataElement> src =
                            env.addSource(
                                            new TestEventSource(
                                                    unitedSourceLeft, eventQueue, commandQueue))
                                    .setUidHash(unitedSourceLeft)
                                    .assignTimestampsAndWatermarks(createWmAssigner());

                    SingleOutputStreamOperator<TestDataElement> forwardTransform =
                            src.transform(
                                            "transform-1-forward",
                                            TypeInformation.of(TestDataElement.class),
                                            new OneInputTestStreamOperatorFactory(
                                                    mapForward, eventQueue, commandQueue))
                                    .setUidHash(mapForward);

                    forwardTransform.sinkTo(new DiscardingSink<>());

                    Map<String, Integer> operatorsNumberOfInputs = new HashMap<>();
                    operatorsNumberOfInputs.put(mapForward, 1);

                    return new TestJobWithDescription(
                            env.getStreamGraph().getJobGraph(),
                            singleton(unitedSourceLeft),
                            new HashSet<>(singletonList(mapForward)),
                            new HashSet<>(asList(unitedSourceLeft, mapForward)),
                            operatorsNumberOfInputs,
                            eventQueue,
                            commandQueue);
                }

                @Override
                public String toString() {
                    return "simple graph";
                }
            };

    public static final TestingGraphBuilder COMPLEX_GRAPH_BUILDER =
            new TestingGraphBuilder() {
                @Override
                public TestJobWithDescription build(
                        SharedObjects shared,
                        ThrowingConsumer<Configuration, Exception> confConsumer,
                        ThrowingConsumer<StreamExecutionEnvironment, Exception> envConsumer)
                        throws Exception {

                    TestEventQueue eventQueue = TestEventQueue.createShared(shared);
                    TestCommandDispatcher commandQueue = TestCommandDispatcher.createShared(shared);

                    StreamExecutionEnvironment env = prepareEnv(confConsumer, envConsumer);

                    // using hashes so that operators emit identifiable events
                    String unitedSourceLeft = OP_ID_HASH_PREFIX + "1";
                    String unitedSourceRight = OP_ID_HASH_PREFIX + "2";
                    String connectedSource = OP_ID_HASH_PREFIX + "3";
                    String multiSource = OP_ID_HASH_PREFIX + "4";
                    String mapForward = OP_ID_HASH_PREFIX + "5";
                    String mapKeyed = OP_ID_HASH_PREFIX + "6";
                    String mapTwoInput = OP_ID_HASH_PREFIX + "7";
                    String multipleInput = OP_ID_HASH_PREFIX + "8";

                    // todo: FLIP-27 sources
                    // todo: chain sources
                    DataStream<TestDataElement> unitedSources =
                            env.addSource(
                                            new TestEventSource(
                                                    unitedSourceLeft, eventQueue, commandQueue))
                                    .setUidHash(unitedSourceLeft)
                                    .assignTimestampsAndWatermarks(createWmAssigner())
                                    .union(
                                            env.addSource(
                                                            new TestEventSource(
                                                                    unitedSourceRight,
                                                                    eventQueue,
                                                                    commandQueue))
                                                    .setUidHash(unitedSourceRight)
                                                    .assignTimestampsAndWatermarks(
                                                            createWmAssigner()));
                    SingleOutputStreamOperator<TestDataElement> sideSource =
                            env.addSource(
                                            new TestEventSource(
                                                    multiSource, eventQueue, commandQueue))
                                    .setUidHash(multiSource)
                                    .assignTimestampsAndWatermarks(createWmAssigner());

                    DataStream<?>[] inputs = new DataStream[] {unitedSources, sideSource};
                    final MultipleInputTransformation<TestDataElement> multipleInputsTransform =
                            new MultipleInputTransformation<>(
                                    "MultipleInputOperator",
                                    new MultiInputTestOperatorFactory(
                                            inputs.length, eventQueue, multipleInput),
                                    TypeInformation.of(TestDataElement.class),
                                    env.getParallelism());
                    for (DataStream<?> input : inputs) {
                        multipleInputsTransform.addInput(input.getTransformation());
                    }
                    multipleInputsTransform.setChainingStrategy(ChainingStrategy.HEAD_WITH_SOURCES);
                    env.addOperator(multipleInputsTransform);

                    SingleOutputStreamOperator<TestDataElement> multipleSources =
                            new MultipleConnectedStreams(env)
                                    .transform(multipleInputsTransform)
                                    .setUidHash(multiSource);

                    SingleOutputStreamOperator<TestDataElement> forwardTransform =
                            multipleSources
                                    .startNewChain()
                                    .transform(
                                            "transform-1-forward",
                                            TypeInformation.of(TestDataElement.class),
                                            new OneInputTestStreamOperatorFactory(
                                                    mapForward, eventQueue, commandQueue))
                                    .setUidHash(mapForward);

                    SingleOutputStreamOperator<TestDataElement> keyedTransform =
                            forwardTransform
                                    .startNewChain()
                                    // distribute the load evenly but keep the number of keys
                                    // manageable to not overload state backends
                                    .keyBy(e -> e.seq % 1000)
                                    .transform(
                                            "transform-2-keyed",
                                            TypeInformation.of(TestDataElement.class),
                                            new OneInputTestStreamOperatorFactory(
                                                    mapKeyed, eventQueue, commandQueue))
                                    .setUidHash(mapKeyed);

                    SingleOutputStreamOperator<TestDataElement> twoInputTransform =
                            keyedTransform
                                    .startNewChain()
                                    .connect(
                                            env.addSource(
                                                            new TestEventSource(
                                                                    connectedSource,
                                                                    eventQueue,
                                                                    commandQueue))
                                                    .setUidHash(connectedSource))
                                    .transform(
                                            "transform-3-two-input",
                                            TypeInformation.of(TestDataElement.class),
                                            new TwoInputTestStreamOperator(mapTwoInput, eventQueue))
                                    .setUidHash(mapTwoInput);

                    twoInputTransform.sinkTo(new DiscardingSink<>());

                    Map<String, Integer> operatorsNumberOfInputs = new HashMap<>();
                    operatorsNumberOfInputs.put(mapForward, 1);
                    operatorsNumberOfInputs.put(mapKeyed, 1);
                    operatorsNumberOfInputs.put(mapTwoInput, 2);
                    operatorsNumberOfInputs.put(multipleInput, 2);

                    return new TestJobWithDescription(
                            env.getStreamGraph().getJobGraph(),
                            new HashSet<>(
                                    asList(unitedSourceLeft, unitedSourceRight, connectedSource)),
                            new HashSet<>(asList(mapForward, mapKeyed, mapTwoInput, multipleInput)),
                            new HashSet<>(
                                    asList(
                                            unitedSourceLeft,
                                            unitedSourceRight,
                                            connectedSource,
                                            mapForward,
                                            mapKeyed,
                                            mapTwoInput,
                                            multipleInput)),
                            operatorsNumberOfInputs,
                            eventQueue,
                            commandQueue);
                }

                @Override
                public String toString() {
                    return "complex graph";
                }
            };

    private static StreamExecutionEnvironment prepareEnv(
            ThrowingConsumer<Configuration, Exception> confConsumer,
            ThrowingConsumer<StreamExecutionEnvironment, Exception> envConsumer)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(EXECUTION_FAILOVER_STRATEGY, "full");
        confConsumer.accept(configuration);
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(4);
        env.setRestartStrategy(noRestart());
        env.enableCheckpointing(200); // shouldn't matter
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setAutoWatermarkInterval(50);
        envConsumer.accept(env);
        return env;
    }

    private static final String OP_ID_HASH_PREFIX = "0000000000000000000000000000000";

    private static WatermarkStrategy<TestDataElement> createWmAssigner() {
        return WatermarkStrategy.forGenerator(
                        ctx ->
                                new WatermarkGenerator<TestDataElement>() {
                                    private Watermark watermark = new Watermark(Long.MIN_VALUE);

                                    @Override
                                    public void onEvent(
                                            TestDataElement event,
                                            long eventTimestamp,
                                            WatermarkOutput output) {
                                        this.watermark = new Watermark(eventTimestamp);
                                    }

                                    @Override
                                    public void onPeriodicEmit(WatermarkOutput output) {
                                        output.emitWatermark(watermark);
                                    }
                                })
                .withTimestampAssigner(
                        (SerializableTimestampAssigner<TestDataElement>)
                                (element, recordTimestamp) -> element.seq);
    }
}
