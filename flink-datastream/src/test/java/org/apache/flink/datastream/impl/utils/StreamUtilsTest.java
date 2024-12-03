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

package org.apache.flink.datastream.impl.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.MissingTypeInfo;
import org.apache.flink.datastream.api.common.Collector;
import org.apache.flink.datastream.api.context.NonPartitionedContext;
import org.apache.flink.datastream.api.context.PartitionedContext;
import org.apache.flink.datastream.api.context.TwoOutputPartitionedContext;
import org.apache.flink.datastream.api.function.OneInputStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoInputNonBroadcastStreamProcessFunction;
import org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction;
import org.apache.flink.datastream.impl.ExecutionEnvironmentImpl;
import org.apache.flink.datastream.impl.TestingTransformation;
import org.apache.flink.datastream.impl.operators.ProcessOperator;
import org.apache.flink.datastream.impl.operators.TwoInputNonBroadcastProcessOperator;
import org.apache.flink.datastream.impl.stream.NonKeyedPartitionStreamImpl;
import org.apache.flink.datastream.impl.stream.StreamTestUtils;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link StreamUtils}. */
class StreamUtilsTest {
    @Test
    void testGetOneInputOutputType() {
        TypeInformation<Long> outputType =
                StreamUtils.getOutputTypeForOneInputProcessFunction(
                        new OneInputStreamProcessFunction<Integer, Long>() {
                            @Override
                            public void processRecord(
                                    Integer record, Collector<Long> output, PartitionedContext ctx)
                                    throws Exception {
                                // ignore
                            }
                        },
                        Types.INT);
        assertThat(outputType).isEqualTo(Types.LONG);
    }

    @Test
    void testLambdaMissingType() {
        TypeInformation<Long> outputType =
                StreamUtils.getOutputTypeForOneInputProcessFunction(
                        (OneInputStreamProcessFunction<Integer, Long>)
                                (record, output, ctx) -> {
                                    // ignore
                                },
                        Types.INT);
        assertThat(outputType).isInstanceOf(MissingTypeInfo.class);
    }

    @Test
    void testGetTwoInputOutputType() {
        TypeInformation<String> outputType =
                StreamUtils.getOutputTypeForTwoInputNonBroadcastProcessFunction(
                        new TwoInputNonBroadcastStreamProcessFunction<Integer, Long, String>() {
                            @Override
                            public void processRecordFromFirstInput(
                                    Integer record,
                                    Collector<String> output,
                                    PartitionedContext ctx)
                                    throws Exception {
                                // ignore
                            }

                            @Override
                            public void processRecordFromSecondInput(
                                    Long record, Collector<String> output, PartitionedContext ctx)
                                    throws Exception {
                                // ignore
                            }
                        },
                        Types.INT,
                        Types.LONG);
        assertThat(outputType).isEqualTo(Types.STRING);

        outputType =
                StreamUtils.getOutputTypeForTwoInputBroadcastProcessFunction(
                        new TwoInputBroadcastStreamProcessFunction<Integer, Long, String>() {
                            @Override
                            public void processRecordFromNonBroadcastInput(
                                    Integer record,
                                    Collector<String> output,
                                    PartitionedContext ctx)
                                    throws Exception {
                                // ignore
                            }

                            @Override
                            public void processRecordFromBroadcastInput(
                                    Long record, NonPartitionedContext<String> ctx)
                                    throws Exception {
                                // ignore
                            }
                        },
                        Types.INT,
                        Types.LONG);
        assertThat(outputType).isEqualTo(Types.STRING);
    }

    @Test
    void testTwoOutputType() {
        Tuple2<TypeInformation<Long>, TypeInformation<String>> outputType =
                StreamUtils.getOutputTypesForTwoOutputProcessFunction(
                        new TwoOutputStreamProcessFunction<Integer, Long, String>() {
                            @Override
                            public void processRecord(
                                    Integer record,
                                    Collector<Long> output1,
                                    Collector<String> output2,
                                    TwoOutputPartitionedContext ctx)
                                    throws Exception {
                                // ignore
                            }
                        },
                        Types.INT);
        assertThat(outputType.f0).isEqualTo(Types.LONG);
        assertThat(outputType.f1).isEqualTo(Types.STRING);
    }

    @Test
    void testGetOneInputTransformation() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        ProcessOperator<Integer, Long> operator =
                new ProcessOperator<>(new StreamTestUtils.NoOpOneInputStreamProcessFunction());
        OneInputTransformation<Integer, Long> transformation =
                StreamUtils.getOneInputTransformation(
                        "op",
                        new NonKeyedPartitionStreamImpl<>(
                                env, new TestingTransformation<>("t", Types.INT, 1)),
                        Types.LONG,
                        operator);
        assertThat(transformation.getOperator()).isEqualTo(operator);
        assertThat(transformation.getOutputType()).isEqualTo(Types.LONG);
        assertThat(transformation.getStateKeySelector()).isNull();
    }

    @Test
    void testGetOneInputKeyedTransformation() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        ProcessOperator<Integer, Long> operator =
                new ProcessOperator<>(new StreamTestUtils.NoOpOneInputStreamProcessFunction());
        OneInputTransformation<Integer, Long> transformation =
                StreamUtils.getOneInputKeyedTransformation(
                        "op",
                        new NonKeyedPartitionStreamImpl<>(
                                env, new TestingTransformation<>("t", Types.INT, 1)),
                        Types.LONG,
                        operator,
                        (KeySelector<Integer, Integer>) value -> value,
                        Types.INT);
        assertThat(transformation.getOperator()).isEqualTo(operator);
        assertThat(transformation.getOutputType()).isEqualTo(Types.LONG);
        assertThat(transformation.getStateKeySelector()).isNotNull();
    }

    @Test
    void testGetTwoInputTransformation() throws Exception {
        ExecutionEnvironmentImpl env = StreamTestUtils.getEnv();
        TwoInputNonBroadcastProcessOperator<Integer, Long, Long> operator =
                new TwoInputNonBroadcastProcessOperator<>(
                        new StreamTestUtils.NoOpTwoInputNonBroadcastStreamProcessFunction());
        TwoInputTransformation<Integer, Long, Long> transformation =
                StreamUtils.getTwoInputTransformation(
                        "op",
                        new NonKeyedPartitionStreamImpl<>(
                                env, new TestingTransformation<>("t1", Types.INT, 1)),
                        new NonKeyedPartitionStreamImpl<>(
                                env, new TestingTransformation<>("t2", Types.LONG, 1)),
                        Types.LONG,
                        operator);
        assertThat(transformation.getOperator()).isEqualTo(operator);
        assertThat(transformation.getOutputType()).isEqualTo(Types.LONG);
        assertThat(transformation.getStateKeySelector1()).isNull();
        assertThat(transformation.getStateKeySelector2()).isNull();
    }
}
