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

package org.apache.flink.asyncprocessing.operators;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.declare.ContextVariable;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationException;
import org.apache.flink.runtime.asyncprocessing.declare.NamedCallback;
import org.apache.flink.runtime.asyncprocessing.declare.NamedConsumer;
import org.apache.flink.runtime.asyncprocessing.declare.NamedFunction;
import org.apache.flink.runtime.asyncprocessing.functions.DeclaringAsyncKeyedProcessFunction;
import org.apache.flink.runtime.asyncprocessing.operators.AsyncKeyedProcessOperator;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.asyncprocessing.AsyncKeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AsyncKeyedProcessOperator}. */
public class AsyncKeyedProcessOperatorTest {

    @ParameterizedTest(name = "Chain declaration = {0}")
    @ValueSource(booleans = {false, true})
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testNormalProcessor(boolean chained) throws Exception {
        TestDeclarationFunctionBase function =
                chained ? new TestChainDeclarationFunction() : new TestNormalDeclarationFunction();
        AsyncKeyedProcessOperator<Integer, Tuple2<Integer, String>, String> testOperator =
                new AsyncKeyedProcessOperator<>(function);
        ArrayList<StreamRecord<String>> expectedOutput = new ArrayList<>();
        try (AsyncKeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness =
                        AsyncKeyedOneInputStreamOperatorTestHarness.create(
                                testOperator, (e) -> e.f0, TypeInformation.of(Integer.class))) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(Tuple2.of(5, "5")));
            expectedOutput.add(new StreamRecord<>("11"));
            assertThat(function.getValue()).isEqualTo(11);
            testHarness.processElement(new StreamRecord<>(Tuple2.of(6, "6")));
            expectedOutput.add(new StreamRecord<>("24"));
            assertThat(function.getValue()).isEqualTo(24);
            assertThat(testHarness.getOutput()).containsExactly(expectedOutput.toArray());
        }
    }

    @Test
    @SuppressWarnings({"rawtypes", "unchecked"})
    public void testTimerProcessor() throws Exception {
        TestDeclarationFunctionBase function = new TestTimerDeclarationFunction();
        AsyncKeyedProcessOperator<Integer, Tuple2<Integer, String>, String> testOperator =
                new AsyncKeyedProcessOperator<>(function);
        ArrayList<Object> expectedOutput = new ArrayList<>();
        try (AsyncKeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness =
                        AsyncKeyedOneInputStreamOperatorTestHarness.create(
                                testOperator, (e) -> e.f0, TypeInformation.of(Integer.class))) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(Tuple2.of(5, "5")));
            testHarness.processElement(new StreamRecord<>(Tuple2.of(6, "5")));
            assertThat(function.getValue()).isEqualTo(0);
            testHarness.processWatermark(5L);
            expectedOutput.add(new StreamRecord<>("11", 5L));
            expectedOutput.add(new Watermark(5L));
            assertThat(function.getValue()).isEqualTo(11);
            testHarness.processWatermark(6L);
            expectedOutput.add(new StreamRecord<>("24", 6L));
            expectedOutput.add(new Watermark(6L));
            assertThat(function.getValue()).isEqualTo(24);
            assertThat(testHarness.getOutput()).containsExactly(expectedOutput.toArray());
        }
    }

    @Test
    public void testNoDeclaredFunction() throws Exception {
        TestNotDeclarationFunction function = new TestNotDeclarationFunction();
        AsyncKeyedProcessOperator<Integer, Tuple2<Integer, String>, String> testOperator =
                new AsyncKeyedProcessOperator<>(function);
        ArrayList<Object> expectedOutput = new ArrayList<>();
        try (AsyncKeyedOneInputStreamOperatorTestHarness<Integer, Tuple2<Integer, String>, String>
                testHarness =
                        AsyncKeyedOneInputStreamOperatorTestHarness.create(
                                testOperator, (e) -> e.f0, TypeInformation.of(Integer.class))) {
            testHarness.open();
            testHarness.processElement(new StreamRecord<>(Tuple2.of(5, "5")));
            testHarness.processElement(new StreamRecord<>(Tuple2.of(6, "5")));
            assertThat(function.getValue()).isEqualTo(0);
            testHarness.processWatermark(5L);
            expectedOutput.add(new StreamRecord<>("11", 5L));
            expectedOutput.add(new Watermark(5L));
            assertThat(function.getValue()).isEqualTo(11);
            testHarness.processWatermark(6L);
            expectedOutput.add(new StreamRecord<>("24", 6L));
            expectedOutput.add(new Watermark(6L));
            assertThat(function.getValue()).isEqualTo(24);
            assertThat(testHarness.getOutput()).containsExactly(expectedOutput.toArray());
        }
    }

    private abstract static class TestDeclarationFunctionBase
            extends DeclaringAsyncKeyedProcessFunction<Integer, Tuple2<Integer, String>, String> {
        final AtomicInteger value = new AtomicInteger(0);

        public int getValue() {
            return value.get();
        }
    }

    private static class TestNormalDeclarationFunction extends TestDeclarationFunctionBase {

        @Override
        public ThrowingConsumer<Tuple2<Integer, String>, Exception> declareProcess(
                DeclarationContext context, Context ctx, Collector<String> out)
                throws DeclarationException {
            ContextVariable<Integer> inputValue = context.declareVariable(null);
            NamedFunction<Void, StateFuture<Integer>> adder =
                    context.declare(
                            "adder",
                            (i) -> {
                                return StateFutureUtils.completedFuture(value.incrementAndGet());
                            });
            NamedConsumer<Integer> doubler =
                    context.declare(
                            "doubler",
                            (v) -> {
                                value.addAndGet(inputValue.get());
                                out.collect(String.valueOf(value.get()));
                            });
            assertThat(adder).isInstanceOf(NamedCallback.class);
            assertThat(doubler).isInstanceOf(NamedCallback.class);
            return (e) -> {
                if (inputValue.get() == null) {
                    inputValue.set(e.f0);
                }
                value.addAndGet(e.f0);
                StateFutureUtils.<Void>completedVoidFuture().thenCompose(adder).thenAccept(doubler);
            };
        }
    }

    private static class TestChainDeclarationFunction extends TestDeclarationFunctionBase {

        @Override
        public ThrowingConsumer<Tuple2<Integer, String>, Exception> declareProcess(
                DeclarationContext context, Context ctx, Collector<String> out)
                throws DeclarationException {
            ContextVariable<Integer> inputValue = context.declareVariable(null);
            return context.<Tuple2<Integer, String>>declareChain()
                    .thenCompose(
                            e -> {
                                if (inputValue.get() == null) {
                                    inputValue.set(e.f0);
                                }
                                value.addAndGet(e.f0);
                                return StateFutureUtils.completedVoidFuture();
                            })
                    .thenCompose(v -> StateFutureUtils.completedFuture(value.incrementAndGet()))
                    .withName("adder")
                    .thenAccept(
                            (v) -> {
                                value.addAndGet(inputValue.get());
                                out.collect(String.valueOf(value.get()));
                            })
                    .withName("doubler")
                    .finish();
        }
    }

    private static class TestTimerDeclarationFunction extends TestDeclarationFunctionBase {

        @Override
        public ThrowingConsumer<Tuple2<Integer, String>, Exception> declareProcess(
                DeclarationContext context, Context ctx, Collector<String> out)
                throws DeclarationException {
            return context.<Tuple2<Integer, String>>declareChain()
                    .thenCompose(
                            e -> {
                                ctx.timerService().registerEventTimeTimer(e.f0);
                                return StateFutureUtils.completedVoidFuture();
                            })
                    .finish();
        }

        public ThrowingConsumer<Long, Exception> declareOnTimer(
                DeclarationContext context, OnTimerContext ctx, Collector<String> out)
                throws DeclarationException {
            ContextVariable<Integer> inputValue = context.declareVariable(null);
            return context.<Long>declareChain()
                    .thenCompose(
                            e -> {
                                if (inputValue.get() == null) {
                                    inputValue.set(e.intValue());
                                }
                                value.addAndGet(e.intValue());
                                return StateFutureUtils.completedVoidFuture();
                            })
                    .thenCompose(v -> StateFutureUtils.completedFuture(value.incrementAndGet()))
                    .withName("adder")
                    .thenAccept(
                            (v) -> {
                                value.addAndGet(inputValue.get());
                                out.collect(String.valueOf(value.get()));
                            })
                    .withName("doubler")
                    .finish();
        }
    }

    /** Equivalent to {@link TestTimerDeclarationFunction}. */
    private static class TestNotDeclarationFunction
            extends KeyedProcessFunction<Integer, Tuple2<Integer, String>, String> {
        final AtomicInteger value = new AtomicInteger(0);

        public int getValue() {
            return value.get();
        }

        @Override
        public void processElement(
                Tuple2<Integer, String> e,
                KeyedProcessFunction<Integer, Tuple2<Integer, String>, String>.Context ctx,
                Collector<String> out)
                throws Exception {
            ctx.timerService().registerEventTimeTimer(e.f0);
        }

        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws DeclarationException {
            value.addAndGet((int) timestamp);
            value.incrementAndGet();
            value.addAndGet((int) timestamp);
            out.collect(String.valueOf(value.get()));
        }
    }
}
