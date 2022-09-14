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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.NullByteKeySelector;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;

import org.junit.Test;

import java.util.ArrayDeque;
import java.util.Objects;
import java.util.stream.StreamSupport;

import static org.junit.Assert.assertTrue;

/** Tests for {@link ContinuousProcessingTimeTrigger}. */
public class ContinuousProcessingTimeTriggerTest {

    private static final long NO_TIMESTAMP = Watermark.UNINITIALIZED.getTimestamp();

    private static class WindowedInteger {
        private final TimeWindow window;
        private final int value;

        public WindowedInteger(TimeWindow window, int value) {
            this.window = window;
            this.value = value;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof WindowedInteger)) {
                return false;
            }
            WindowedInteger other = (WindowedInteger) o;
            return value == other.value && Objects.equals(window, other.window);
        }

        @Override
        public int hashCode() {
            return Objects.hash(window, value);
        }

        @Override
        public String toString() {
            return "WindowedInteger{" + "window=" + window + ", value=" + value + '}';
        }
    }

    private static class IntegerSumWindowFunction
            implements WindowFunction<Integer, WindowedInteger, Byte, TimeWindow> {
        @Override
        public void apply(
                Byte key,
                TimeWindow window,
                Iterable<Integer> input,
                Collector<WindowedInteger> out)
                throws Exception {
            int sum =
                    StreamSupport.stream(input.spliterator(), false)
                            .mapToInt(Integer::intValue)
                            .sum();
            out.collect(new WindowedInteger(window, sum));
        }
    }

    /** Verify ContinuousProcessingTimeTrigger fire. */
    @Test
    public void testWindowFiring() throws Exception {
        ContinuousProcessingTimeTrigger<TimeWindow> trigger =
                ContinuousProcessingTimeTrigger.of(Time.milliseconds(5));

        assertTrue(trigger.canMerge());

        ListStateDescriptor<Integer> stateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        BasicTypeInfo.INT_TYPE_INFO.createSerializer(new ExecutionConfig()));

        WindowOperator<Byte, Integer, Iterable<Integer>, WindowedInteger, TimeWindow> operator =
                new WindowOperator<>(
                        TumblingProcessingTimeWindows.of(Time.milliseconds(10)),
                        new TimeWindow.Serializer(),
                        new NullByteKeySelector<>(),
                        BasicTypeInfo.BYTE_TYPE_INFO.createSerializer(new ExecutionConfig()),
                        stateDesc,
                        new InternalIterableWindowFunction<>(new IntegerSumWindowFunction()),
                        trigger,
                        0,
                        null);

        KeyedOneInputStreamOperatorTestHarness<Byte, Integer, WindowedInteger> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, operator.getKeySelector(), BasicTypeInfo.BYTE_TYPE_INFO);

        ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

        testHarness.open();

        // window [0, 10)
        testHarness.getProcessingTimeService().setCurrentTime(0);
        testHarness.processElement(1, NO_TIMESTAMP);

        // window [0, 10)
        testHarness.getProcessingTimeService().setCurrentTime(2);
        testHarness.processElement(2, NO_TIMESTAMP);

        // Fire window [0, 10), value is 1+2=3.
        testHarness.getProcessingTimeService().setCurrentTime(5);
        expectedOutput.add(new StreamRecord<>(new WindowedInteger(new TimeWindow(0, 10), 3), 9));
        TestHarnessUtil.assertOutputEquals(
                "Output mismatch", expectedOutput, testHarness.getOutput());

        // window [0, 10)
        testHarness.getProcessingTimeService().setCurrentTime(7);
        testHarness.processElement(3, NO_TIMESTAMP);

        // Fire window [0, 10), value is 3+3=6.
        testHarness.getProcessingTimeService().setCurrentTime(9);
        expectedOutput.add(new StreamRecord<>(new WindowedInteger(new TimeWindow(0, 10), 6), 9));
        TestHarnessUtil.assertOutputEquals(
                "Output mismatch", expectedOutput, testHarness.getOutput());

        // window [10, 20)
        testHarness.getProcessingTimeService().setCurrentTime(10);
        testHarness.processElement(3, NO_TIMESTAMP);

        // Fire window [10, 20), value is 3.
        testHarness.getProcessingTimeService().setCurrentTime(15);
        expectedOutput.add(new StreamRecord<>(new WindowedInteger(new TimeWindow(10, 20), 3), 19));
        TestHarnessUtil.assertOutputEquals(
                "Output mismatch", expectedOutput, testHarness.getOutput());

        // window [10, 20)
        testHarness.getProcessingTimeService().setCurrentTime(18);
        testHarness.processElement(3, NO_TIMESTAMP);

        // Fire window [10, 20), value is 3+3=6.
        testHarness.getProcessingTimeService().setCurrentTime(20);
        expectedOutput.add(new StreamRecord<>(new WindowedInteger(new TimeWindow(10, 20), 6), 19));
        TestHarnessUtil.assertOutputEquals(
                "Output mismatch", expectedOutput, testHarness.getOutput());
    }

    @Test
    public void testMergingWindows() throws Exception {
        ContinuousProcessingTimeTrigger<TimeWindow> trigger =
                ContinuousProcessingTimeTrigger.of(Time.milliseconds(5));

        assertTrue(trigger.canMerge());

        ListStateDescriptor<Integer> stateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        BasicTypeInfo.INT_TYPE_INFO.createSerializer(new ExecutionConfig()));

        WindowOperator<Byte, Integer, Iterable<Integer>, WindowedInteger, TimeWindow> operator =
                new WindowOperator<>(
                        ProcessingTimeSessionWindows.withGap(Time.milliseconds(10)),
                        new TimeWindow.Serializer(),
                        new NullByteKeySelector<>(),
                        BasicTypeInfo.BYTE_TYPE_INFO.createSerializer(new ExecutionConfig()),
                        stateDesc,
                        new InternalIterableWindowFunction<>(new IntegerSumWindowFunction()),
                        trigger,
                        0,
                        null);

        KeyedOneInputStreamOperatorTestHarness<Byte, Integer, WindowedInteger> testHarness =
                new KeyedOneInputStreamOperatorTestHarness<>(
                        operator, operator.getKeySelector(), BasicTypeInfo.BYTE_TYPE_INFO);

        ArrayDeque<Object> expectedOutput = new ArrayDeque<>();

        testHarness.open();

        // window [0, 10)
        testHarness.getProcessingTimeService().setCurrentTime(0);
        testHarness.processElement(1, NO_TIMESTAMP);

        // window [2, 12) ==> [0, 12)
        testHarness.getProcessingTimeService().setCurrentTime(2);
        testHarness.processElement(2, NO_TIMESTAMP);

        // Merged timer should still fire.
        testHarness.getProcessingTimeService().setCurrentTime(5);
        expectedOutput.add(new StreamRecord<>(new WindowedInteger(new TimeWindow(0, 12), 3), 11));
        TestHarnessUtil.assertOutputEquals(
                "Output mismatch", expectedOutput, testHarness.getOutput());

        // Merged window should work as normal.
        testHarness.getProcessingTimeService().setCurrentTime(9);
        TestHarnessUtil.assertOutputEquals(
                "Output mismatch", expectedOutput, testHarness.getOutput());

        testHarness.getProcessingTimeService().setCurrentTime(10);
        expectedOutput.add(new StreamRecord<>(new WindowedInteger(new TimeWindow(0, 12), 3), 11));
        TestHarnessUtil.assertOutputEquals(
                "Output mismatch", expectedOutput, testHarness.getOutput());

        // Firing on time.
        testHarness.getProcessingTimeService().setCurrentTime(15);
        expectedOutput.add(new StreamRecord<>(new WindowedInteger(new TimeWindow(0, 12), 3), 11));
        TestHarnessUtil.assertOutputEquals(
                "Output mismatch", expectedOutput, testHarness.getOutput());

        // Window is dropped already.
        testHarness.getProcessingTimeService().setCurrentTime(100);
        TestHarnessUtil.assertOutputEquals(
                "Output mismatch", expectedOutput, testHarness.getOutput());
    }
}
