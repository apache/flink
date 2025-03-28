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

import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.api.common.state.v2.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.declare.ContextVariable;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationContext;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationException;
import org.apache.flink.runtime.asyncprocessing.functions.DeclaringAsyncKeyedCoProcessFunction;
import org.apache.flink.runtime.asyncprocessing.operators.co.AsyncKeyedCoProcessOperator;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.asyncprocessing.AsyncKeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.function.ThrowingConsumer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AsyncKeyedCoProcessOperator}. */
class AsyncKeyedCoProcessOperatorTest {

    @Test
    void testDeclareProcessor() throws Exception {
        TestChainDeclarationFunction function = new TestChainDeclarationFunction();
        AsyncKeyedCoProcessOperator<String, Integer, String, String> operator =
                new AsyncKeyedCoProcessOperator<>(function);

        AsyncKeyedTwoInputStreamOperatorTestHarness<String, Integer, String, String> testHarness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<>(),
                        BasicTypeInfo.STRING_TYPE_INFO);

        ArrayList<StreamRecord<String>> expectedOutput = new ArrayList<>();

        testHarness.open();
        testHarness.processElement1(new StreamRecord<>(5));
        expectedOutput.add(new StreamRecord<>("11"));
        assertThat(function.value.get()).isEqualTo(11);
        testHarness.processElement2(new StreamRecord<>("6"));
        expectedOutput.add(new StreamRecord<>("6"));
        assertThat(function.value.get()).isEqualTo(17);
        assertThat(testHarness.getOutput()).containsExactly(expectedOutput.toArray());
    }

    @Test
    void testTimestampAndWatermarkQuerying() throws Exception {

        AsyncKeyedCoProcessOperator<String, Integer, String, String> operator =
                new AsyncKeyedCoProcessOperator<>(new WatermarkQueryingProcessFunction());

        AsyncKeyedTwoInputStreamOperatorTestHarness<String, Integer, String, String> testHarness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<>(),
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processWatermark1(new Watermark(17));
        testHarness.processWatermark2(new Watermark(17));
        testHarness.processElement1(new StreamRecord<>(5, 12L));

        testHarness.processWatermark1(new Watermark(42));
        testHarness.processWatermark2(new Watermark(42));
        testHarness.processElement2(new StreamRecord<>("6", 13L));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new Watermark(17L));
        expectedOutput.add(new StreamRecord<>("5WM:17 TS:12", 12L));
        expectedOutput.add(new Watermark(42L));
        expectedOutput.add(new StreamRecord<>("6WM:42 TS:13", 13L));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    void testTimestampAndProcessingTimeQuerying() throws Exception {

        AsyncKeyedCoProcessOperator<String, Integer, String, String> operator =
                new AsyncKeyedCoProcessOperator<>(new ProcessingTimeQueryingProcessFunction());

        AsyncKeyedTwoInputStreamOperatorTestHarness<String, Integer, String, String> testHarness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<>(),
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.setProcessingTime(17);
        testHarness.processElement1(new StreamRecord<>(5));

        testHarness.setProcessingTime(42);
        testHarness.processElement2(new StreamRecord<>("6"));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>("5PT:17 TS:null"));
        expectedOutput.add(new StreamRecord<>("6PT:42 TS:null"));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    void testEventTimeTimers() throws Exception {

        AsyncKeyedCoProcessOperator<String, Integer, String, String> operator =
                new AsyncKeyedCoProcessOperator<>(new EventTimeTriggeringProcessFunction());

        AsyncKeyedTwoInputStreamOperatorTestHarness<String, Integer, String, String> testHarness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<>(),
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processElement1(new StreamRecord<>(17, 42L));
        testHarness.processElement2(new StreamRecord<>("18", 42L));

        testHarness.processWatermark1(new Watermark(5));
        testHarness.processWatermark2(new Watermark(5));

        testHarness.processWatermark1(new Watermark(6));
        testHarness.processWatermark2(new Watermark(6));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>("INPUT1:17", 42L));
        expectedOutput.add(new StreamRecord<>("INPUT2:18", 42L));
        expectedOutput.add(new StreamRecord<>("17:1777", 5L));
        expectedOutput.add(new Watermark(5L));
        expectedOutput.add(new StreamRecord<>("18:1777", 6L));
        expectedOutput.add(new Watermark(6L));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    void testProcessingTimeTimers() throws Exception {

        AsyncKeyedCoProcessOperator<String, Integer, String, String> operator =
                new AsyncKeyedCoProcessOperator<>(new ProcessingTimeTriggeringProcessFunction());

        AsyncKeyedTwoInputStreamOperatorTestHarness<String, Integer, String, String> testHarness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<>(),
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processElement1(new StreamRecord<>(17));
        testHarness.processElement2(new StreamRecord<>("18"));

        testHarness.setProcessingTime(5);
        testHarness.setProcessingTime(6);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>("INPUT1:17"));
        expectedOutput.add(new StreamRecord<>("INPUT2:18"));
        expectedOutput.add(new StreamRecord<>("1777"));
        expectedOutput.add(new StreamRecord<>("1777"));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    /** Verifies that we don't have leakage between different keys. */
    @Test
    void testEventTimeTimerWithState() throws Exception {

        AsyncKeyedCoProcessOperator<String, Integer, String, String> operator =
                new AsyncKeyedCoProcessOperator<>(new EventTimeTriggeringStatefulProcessFunction());

        AsyncKeyedTwoInputStreamOperatorTestHarness<String, Integer, String, String> testHarness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<>(),
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processWatermark1(new Watermark(1));
        testHarness.processWatermark2(new Watermark(1));
        testHarness.processElement1(new StreamRecord<>(17, 0L)); // should set timer for 6
        testHarness.processElement1(new StreamRecord<>(13, 0L)); // should set timer for 6

        testHarness.processWatermark1(new Watermark(2));
        testHarness.processWatermark2(new Watermark(2));
        testHarness.processElement1(new StreamRecord<>(13, 1L)); // should delete timer
        testHarness.processElement2(new StreamRecord<>("42", 1L)); // should set timer for 7

        testHarness.processWatermark1(new Watermark(6));
        testHarness.processWatermark2(new Watermark(6));

        testHarness.processWatermark1(new Watermark(7));
        testHarness.processWatermark2(new Watermark(7));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new Watermark(1L));
        expectedOutput.add(new StreamRecord<>("INPUT1:17", 0L));
        expectedOutput.add(new StreamRecord<>("INPUT1:13", 0L));
        expectedOutput.add(new Watermark(2L));
        expectedOutput.add(new StreamRecord<>("INPUT2:42", 1L));
        expectedOutput.add(new StreamRecord<>("STATE:17", 6L));
        expectedOutput.add(new Watermark(6L));
        expectedOutput.add(new StreamRecord<>("STATE:42", 7L));
        expectedOutput.add(new Watermark(7L));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    /** Verifies that we don't have leakage between different keys. */
    @Test
    void testProcessingTimeTimerWithState() throws Exception {

        AsyncKeyedCoProcessOperator<String, Integer, String, String> operator =
                new AsyncKeyedCoProcessOperator<>(
                        new ProcessingTimeTriggeringStatefulProcessFunction());

        AsyncKeyedTwoInputStreamOperatorTestHarness<String, Integer, String, String> testHarness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<>(),
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.setProcessingTime(1);
        testHarness.processElement1(new StreamRecord<>(17)); // should set timer for 6
        testHarness.processElement1(new StreamRecord<>(13)); // should set timer for 6

        testHarness.setProcessingTime(2);
        testHarness.processElement1(new StreamRecord<>(13)); // should delete timer again
        testHarness.processElement2(new StreamRecord<>("42")); // should set timer for 7

        testHarness.setProcessingTime(6);
        testHarness.setProcessingTime(7);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>("INPUT1:17"));
        expectedOutput.add(new StreamRecord<>("INPUT1:13"));
        expectedOutput.add(new StreamRecord<>("INPUT2:42"));
        expectedOutput.add(new StreamRecord<>("STATE:17"));
        expectedOutput.add(new StreamRecord<>("STATE:42"));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    void testSnapshotAndRestore() throws Exception {

        AsyncKeyedCoProcessOperator<String, Integer, String, String> operator =
                new AsyncKeyedCoProcessOperator<>(new BothTriggeringProcessFunction());

        AsyncKeyedTwoInputStreamOperatorTestHarness<String, Integer, String, String> testHarness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<>(),
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processElement1(new StreamRecord<>(5, 12L));
        testHarness.processElement2(new StreamRecord<>("5", 12L));

        // snapshot and restore from scratch
        OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

        testHarness.close();

        operator = new AsyncKeyedCoProcessOperator<>(new BothTriggeringProcessFunction());

        testHarness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<>(),
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.setProcessingTime(5);
        testHarness.processWatermark1(new Watermark(6));
        testHarness.processWatermark2(new Watermark(6));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>("PROC:1777"));
        expectedOutput.add(new StreamRecord<>("EVENT:1777", 6L));
        expectedOutput.add(new Watermark(6));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    @Test
    void testGetCurrentKeyFromContext() throws Exception {
        AsyncKeyedCoProcessOperator<String, Integer, String, String> operator =
                new AsyncKeyedCoProcessOperator<>(new AppendCurrentKeyProcessFunction());

        AsyncKeyedTwoInputStreamOperatorTestHarness<String, Integer, String, String> testHarness =
                AsyncKeyedTwoInputStreamOperatorTestHarness.create(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<>(),
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processElement1(new StreamRecord<>(5));
        testHarness.processElement1(new StreamRecord<>(6));
        testHarness.processElement2(new StreamRecord<>("hello"));
        testHarness.processElement2(new StreamRecord<>("world"));

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        expectedOutput.add(new StreamRecord<>("5,5"));
        expectedOutput.add(new StreamRecord<>("6,6"));
        expectedOutput.add(new StreamRecord<>("hello,hello"));
        expectedOutput.add(new StreamRecord<>("world,world"));

        TestHarnessUtil.assertOutputEquals(
                "Output was not correct.", expectedOutput, testHarness.getOutput());

        testHarness.close();
    }

    private static class IntToStringKeySelector<T> implements KeySelector<Integer, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(Integer value) throws Exception {
            return "" + value;
        }
    }

    private static class IdentityKeySelector<T> implements KeySelector<T, T> {
        private static final long serialVersionUID = 1L;

        @Override
        public T getKey(T value) throws Exception {
            return value;
        }
    }

    private static class TestChainDeclarationFunction
            extends DeclaringAsyncKeyedCoProcessFunction<String, Integer, String, String> {

        final AtomicInteger value = new AtomicInteger(0);

        @Override
        public ThrowingConsumer<Integer, Exception> declareProcess1(
                DeclarationContext context,
                KeyedCoProcessFunction<String, Integer, String, String>.Context ctx,
                Collector<String> out)
                throws DeclarationException {
            ContextVariable<Integer> inputValue = context.declareVariable(null);
            return context.<Integer>declareChain()
                    .thenCompose(
                            e -> {
                                if (inputValue.get() == null) {
                                    inputValue.set(e);
                                }
                                value.addAndGet(e);
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

        @Override
        public ThrowingConsumer<String, Exception> declareProcess2(
                DeclarationContext context,
                KeyedCoProcessFunction<String, Integer, String, String>.Context ctx,
                Collector<String> out)
                throws DeclarationException {
            return context.<String>declareChain()
                    .thenAccept(
                            v -> {
                                out.collect(v);
                                value.addAndGet(Integer.valueOf(v));
                            })
                    .withName("pass")
                    .finish();
        }
    }

    private static class WatermarkQueryingProcessFunction
            extends KeyedCoProcessFunction<String, Integer, String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement1(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect(
                    value
                            + "WM:"
                            + ctx.timerService().currentWatermark()
                            + " TS:"
                            + ctx.timestamp());
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect(
                    value
                            + "WM:"
                            + ctx.timerService().currentWatermark()
                            + " TS:"
                            + ctx.timestamp());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {}
    }

    private static class EventTimeTriggeringProcessFunction
            extends KeyedCoProcessFunction<String, Integer, String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement1(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect("INPUT1:" + value);
            ctx.timerService().registerEventTimeTimer(5);
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect("INPUT2:" + value);
            ctx.timerService().registerEventTimeTimer(6);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            assertThat(ctx.timeDomain()).isEqualTo(TimeDomain.EVENT_TIME);
            out.collect(ctx.getCurrentKey() + ":" + 1777);
        }
    }

    private static class EventTimeTriggeringStatefulProcessFunction
            extends KeyedCoProcessFunction<String, Integer, String, String> {

        private static final long serialVersionUID = 1L;

        private final ValueStateDescriptor<String> state =
                new ValueStateDescriptor<>("seen-element", StringSerializer.INSTANCE);

        @Override
        public void processElement1(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            handleValue(value, out, ctx.timerService(), 1);
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<String> out)
                throws Exception {
            handleValue(value, out, ctx.timerService(), 2);
        }

        private void handleValue(
                Object value, Collector<String> out, TimerService timerService, int channel)
                throws IOException {
            final ValueState<String> state = getRuntimeContext().getState(this.state);
            state.asyncValue()
                    .thenAccept(
                            v -> {
                                if (v == null) {
                                    state.asyncUpdate(String.valueOf(value))
                                            .thenAccept(
                                                    VO ->
                                                            out.collect(
                                                                    "INPUT" + channel + ":"
                                                                            + value));
                                    timerService.registerEventTimeTimer(
                                            timerService.currentWatermark() + 5);
                                } else {
                                    state.asyncClear();
                                    timerService.deleteEventTimeTimer(
                                            timerService.currentWatermark() + 4);
                                }
                            });
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            assertThat(ctx.timeDomain()).isEqualTo(TimeDomain.EVENT_TIME);
            getRuntimeContext()
                    .getState(state)
                    .asyncValue()
                    .thenAccept(
                            v -> {
                                out.collect("STATE:" + v);
                            });
        }
    }

    private static class ProcessingTimeTriggeringProcessFunction
            extends KeyedCoProcessFunction<String, Integer, String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement1(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect("INPUT1:" + value);
            ctx.timerService().registerProcessingTimeTimer(5);
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect("INPUT2:" + value);
            ctx.timerService().registerProcessingTimeTimer(6);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            assertThat(ctx.timeDomain()).isEqualTo(TimeDomain.PROCESSING_TIME);
            out.collect("" + 1777);
        }
    }

    private static class ProcessingTimeQueryingProcessFunction
            extends KeyedCoProcessFunction<String, Integer, String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement1(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect(
                    value
                            + "PT:"
                            + ctx.timerService().currentProcessingTime()
                            + " TS:"
                            + ctx.timestamp());
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect(
                    value
                            + "PT:"
                            + ctx.timerService().currentProcessingTime()
                            + " TS:"
                            + ctx.timestamp());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {}
    }

    private static class ProcessingTimeTriggeringStatefulProcessFunction
            extends KeyedCoProcessFunction<String, Integer, String, String> {

        private static final long serialVersionUID = 1L;

        private final ValueStateDescriptor<String> state =
                new ValueStateDescriptor<>("seen-element", StringSerializer.INSTANCE);

        @Override
        public void processElement1(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            handleValue(value, out, ctx.timerService(), 1);
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<String> out)
                throws Exception {
            handleValue(value, out, ctx.timerService(), 2);
        }

        private void handleValue(
                Object value, Collector<String> out, TimerService timerService, int channel)
                throws IOException {
            final ValueState<String> state = getRuntimeContext().getState(this.state);
            state.asyncValue()
                    .thenAccept(
                            v -> {
                                if (v == null) {
                                    state.asyncUpdate(String.valueOf(value))
                                            .thenAccept(
                                                    VO ->
                                                            out.collect(
                                                                    "INPUT" + channel + ":"
                                                                            + value));
                                    timerService.registerProcessingTimeTimer(
                                            timerService.currentProcessingTime() + 5);
                                } else {
                                    state.asyncClear();
                                    timerService.deleteProcessingTimeTimer(
                                            timerService.currentProcessingTime() + 4);
                                }
                            });
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            assertThat(ctx.timeDomain()).isEqualTo(TimeDomain.PROCESSING_TIME);
            out.collect("STATE:" + getRuntimeContext().getState(state).value());
        }
    }

    private static class BothTriggeringProcessFunction
            extends KeyedCoProcessFunction<String, Integer, String, String> {

        private static final long serialVersionUID = 1L;

        @Override
        public void processElement1(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            ctx.timerService().registerProcessingTimeTimer(3);
            ctx.timerService().registerEventTimeTimer(6);
            ctx.timerService().deleteProcessingTimeTimer(3);
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<String> out)
                throws Exception {
            ctx.timerService().registerEventTimeTimer(4);
            ctx.timerService().registerProcessingTimeTimer(5);
            ctx.timerService().deleteEventTimeTimer(4);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            if (TimeDomain.EVENT_TIME.equals(ctx.timeDomain())) {
                out.collect("EVENT:1777");
            } else {
                out.collect("PROC:1777");
            }
        }
    }

    private static class AppendCurrentKeyProcessFunction
            extends KeyedCoProcessFunction<String, Integer, String, String> {

        @Override
        public void processElement1(Integer value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect(value + "," + ctx.getCurrentKey());
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<String> out)
                throws Exception {
            out.collect(value + "," + ctx.getCurrentKey());
        }
    }
}
