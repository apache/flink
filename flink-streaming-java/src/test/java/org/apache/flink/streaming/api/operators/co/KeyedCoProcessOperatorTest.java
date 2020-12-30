/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

/** Tests {@link KeyedCoProcessOperator}. */
public class KeyedCoProcessOperatorTest extends TestLogger {

    @Test
    public void testTimestampAndWatermarkQuerying() throws Exception {

        KeyedCoProcessOperator<String, Integer, String, String> operator =
                new KeyedCoProcessOperator<>(new WatermarkQueryingProcessFunction());

        TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<String>(),
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
    public void testTimestampAndProcessingTimeQuerying() throws Exception {

        KeyedCoProcessOperator<String, Integer, String, String> operator =
                new KeyedCoProcessOperator<>(new ProcessingTimeQueryingProcessFunction());

        TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<String>(),
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
    public void testEventTimeTimers() throws Exception {

        KeyedCoProcessOperator<String, Integer, String, String> operator =
                new KeyedCoProcessOperator<>(new EventTimeTriggeringProcessFunction());

        TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<String>(),
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
    public void testProcessingTimeTimers() throws Exception {

        KeyedCoProcessOperator<String, Integer, String, String> operator =
                new KeyedCoProcessOperator<>(new ProcessingTimeTriggeringProcessFunction());

        TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<String>(),
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
    public void testEventTimeTimerWithState() throws Exception {

        KeyedCoProcessOperator<String, Integer, String, String> operator =
                new KeyedCoProcessOperator<>(new EventTimeTriggeringStatefulProcessFunction());

        TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<String>(),
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
    public void testProcessingTimeTimerWithState() throws Exception {

        KeyedCoProcessOperator<String, Integer, String, String> operator =
                new KeyedCoProcessOperator<>(new ProcessingTimeTriggeringStatefulProcessFunction());

        TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<String>(),
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
    public void testSnapshotAndRestore() throws Exception {

        KeyedCoProcessOperator<String, Integer, String, String> operator =
                new KeyedCoProcessOperator<>(new BothTriggeringProcessFunction());

        TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<String>(),
                        BasicTypeInfo.STRING_TYPE_INFO);

        testHarness.setup();
        testHarness.open();

        testHarness.processElement1(new StreamRecord<>(5, 12L));
        testHarness.processElement2(new StreamRecord<>("5", 12L));

        // snapshot and restore from scratch
        OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

        testHarness.close();

        operator = new KeyedCoProcessOperator<>(new BothTriggeringProcessFunction());

        testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<String>(),
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
    public void testGetCurrentKeyFromContext() throws Exception {
        KeyedCoProcessOperator<String, Integer, String, String> operator =
                new KeyedCoProcessOperator<>(new AppendCurrentKeyProcessFunction());

        TwoInputStreamOperatorTestHarness<Integer, String, String> testHarness =
                new KeyedTwoInputStreamOperatorTestHarness<>(
                        operator,
                        new IntToStringKeySelector<>(),
                        new IdentityKeySelector<String>(),
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

            assertEquals(TimeDomain.EVENT_TIME, ctx.timeDomain());
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
            if (state.value() == null) {
                out.collect("INPUT" + channel + ":" + value);
                state.update(String.valueOf(value));
                timerService.registerEventTimeTimer(timerService.currentWatermark() + 5);
            } else {
                state.clear();
                timerService.deleteEventTimeTimer(timerService.currentWatermark() + 4);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            assertEquals(TimeDomain.EVENT_TIME, ctx.timeDomain());
            out.collect("STATE:" + getRuntimeContext().getState(state).value());
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

            assertEquals(TimeDomain.PROCESSING_TIME, ctx.timeDomain());
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
            if (state.value() == null) {
                out.collect("INPUT" + channel + ":" + value);
                state.update(String.valueOf(value));
                timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + 5);
            } else {
                state.clear();
                timerService.deleteProcessingTimeTimer(timerService.currentProcessingTime() + 4);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out)
                throws Exception {
            assertEquals(TimeDomain.PROCESSING_TIME, ctx.timeDomain());
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
