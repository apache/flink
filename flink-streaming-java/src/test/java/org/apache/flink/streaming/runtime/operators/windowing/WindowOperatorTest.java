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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.DynamicEventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.DynamicProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SessionWindowTimeGapExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.ProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueProcessWindowFunction;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import org.apache.flink.shaded.guava32.com.google.common.base.Joiner;
import org.apache.flink.shaded.guava32.com.google.common.collect.Iterables;

import org.junit.jupiter.api.Test;

import java.io.Serializable;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Tests for {@link WindowOperator}. */
@SuppressWarnings("serial")
class WindowOperatorTest {

    private static final TypeInformation<Tuple2<String, Integer>> STRING_INT_TUPLE =
            TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {});

    // For counting if close() is called the correct number of times on the SumReducer
    private static AtomicInteger closeCalled = new AtomicInteger(0);

    // late arriving event OutputTag<StreamRecord<IN>>
    private static final OutputTag<Tuple2<String, Integer>> lateOutputTag =
            new OutputTag<Tuple2<String, Integer>>("late-output") {};

    private void testSlidingEventTimeWindows(
            OneInputStreamOperatorFactory<Tuple2<String, Integer>, Tuple2<String, Integer>>
                    operator)
            throws Exception {

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        testHarness.setup();
        testHarness.open();

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        // add elements out-of-order
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 999));
        expectedOutput.add(new Watermark(999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 1999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 1999));
        expectedOutput.add(new Watermark(1999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 2999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));
        expectedOutput.add(new Watermark(2999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        // do a snapshot, close and restore again
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
        testHarness.close();

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), 3999));
        expectedOutput.add(new Watermark(3999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 4999));
        expectedOutput.add(new Watermark(4999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 5999));
        expectedOutput.add(new Watermark(5999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(6999));
        testHarness.processWatermark(new Watermark(7999));
        expectedOutput.add(new Watermark(6999));
        expectedOutput.add(new Watermark(7999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSlidingEventTimeWindowsReduce() throws Exception {
        closeCalled.set(0);

        final int windowSize = 3;
        final int windowSlide = 1;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                SlidingEventTimeWindows.of(
                                        Duration.ofSeconds(windowSize),
                                        Duration.ofSeconds(windowSlide)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, TimeWindow, Tuple2<String, Integer>>()),
                                EventTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        testSlidingEventTimeWindows(operator);
    }

    @Test
    void testSlidingEventTimeWindowsApply() throws Exception {
        closeCalled.set(0);

        final int windowSize = 3;
        final int windowSlide = 1;

        ListStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                SlidingEventTimeWindows.of(
                                        Duration.ofSeconds(windowSize),
                                        Duration.ofSeconds(windowSlide)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalIterableWindowFunction<>(
                                        new RichSumReducer<TimeWindow>()),
                                EventTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        testSlidingEventTimeWindows(operator);

        // we close once in the rest...
        assertThat(closeCalled).as("Close was not called.").hasValue(2);
    }

    private void testTumblingEventTimeWindows(
            OneInputStreamOperatorFactory<Tuple2<String, Integer>, Tuple2<String, Integer>>
                    operator)
            throws Exception {
        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // add elements out-of-order
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

        testHarness.processWatermark(new Watermark(999));
        expectedOutput.add(new Watermark(999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(1999));
        expectedOutput.add(new Watermark(1999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        // do a snapshot, close and restore again
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());
        testHarness.close();

        testHarness = createTestHarness(operator);
        expectedOutput.clear();
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processWatermark(new Watermark(2999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 2999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));
        expectedOutput.add(new Watermark(2999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(3999));
        expectedOutput.add(new Watermark(3999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(4999));
        expectedOutput.add(new Watermark(4999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(5999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 5999));
        expectedOutput.add(new Watermark(5999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(6999));
        testHarness.processWatermark(new Watermark(7999));
        expectedOutput.add(new Watermark(6999));
        expectedOutput.add(new Watermark(7999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testTumblingEventTimeWindowsReduce() throws Exception {
        closeCalled.set(0);

        final int windowSize = 3;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                TumblingEventTimeWindows.of(Duration.ofSeconds(windowSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, TimeWindow, Tuple2<String, Integer>>()),
                                EventTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        testTumblingEventTimeWindows(operator);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testTumblingEventTimeWindowsApply() throws Exception {
        closeCalled.set(0);

        final int windowSize = 3;

        ListStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                TumblingEventTimeWindows.of(Duration.ofSeconds(windowSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalIterableWindowFunction<>(
                                        new RichSumReducer<TimeWindow>()),
                                EventTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        testTumblingEventTimeWindows(operator);

        // we close once in the rest...
        assertThat(closeCalled).as("Close was not called.").hasValue(2);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSessionWindows() throws Exception {
        closeCalled.set(0);

        final int sessionSize = 3;

        ListStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(sessionSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalIterableWindowFunction<>(new SessionWindowFunction()),
                                EventTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // add elements out-of-order
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 1000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3), 2500));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 1000));

        // do a snapshot, close and restore again
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());
        testHarness.close();

        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 2500));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 4), 5501));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 5), 6000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 5), 6000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 6), 6050));

        testHarness.processWatermark(new Watermark(12000));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-6", 10L, 5500L), 5499));
        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-6", 0L, 5500L), 5499));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-20", 5501L, 9050L), 9049));
        expectedOutput.add(new Watermark(12000));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 10), 15000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 20), 15000));

        testHarness.processWatermark(new Watermark(17999));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-30", 15000L, 18000L), 17999));
        expectedOutput.add(new Watermark(17999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());

        testHarness.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testSessionWindowsWithProcessFunction() throws Exception {
        closeCalled.set(0);

        final int sessionSize = 3;

        ListStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(sessionSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalIterableProcessWindowFunction<>(
                                        new SessionProcessWindowFunction()),
                                EventTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // add elements out-of-order
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 1000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3), 2500));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 1000));

        // do a snapshot, close and restore again
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());
        testHarness.close();

        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 2500));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 4), 5501));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 5), 6000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 5), 6000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 6), 6050));

        testHarness.processWatermark(new Watermark(12000));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-6", 10L, 5500L), 5499));
        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-6", 0L, 5500L), 5499));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-20", 5501L, 9050L), 9049));
        expectedOutput.add(new Watermark(12000));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 10), 15000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 20), 15000));

        testHarness.processWatermark(new Watermark(17999));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-30", 15000L, 18000L), 17999));
        expectedOutput.add(new Watermark(17999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());

        testHarness.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReduceSessionWindows() throws Exception {
        closeCalled.set(0);

        final int sessionSize = 3;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(sessionSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new ReducedSessionWindowFunction()),
                                EventTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // add elements out-of-order
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 1000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3), 2500));

        // do a snapshot, close and restore again
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
        testHarness.close();

        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 1000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 2500));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 4), 5501));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 5), 6000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 5), 6000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 6), 6050));

        testHarness.processWatermark(new Watermark(12000));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-6", 10L, 5500L), 5499));
        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-6", 0L, 5500L), 5499));
        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-20", 5501L, 9050L), 9049));
        expectedOutput.add(new Watermark(12000));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 10), 15000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 20), 15000));

        testHarness.processWatermark(new Watermark(17999));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-30", 15000L, 18000L), 17999));
        expectedOutput.add(new Watermark(17999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());

        testHarness.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testReduceSessionWindowsWithProcessFunction() throws Exception {
        closeCalled.set(0);

        final int sessionSize = 3;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(sessionSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueProcessWindowFunction<>(
                                        new ReducedProcessSessionWindowFunction()),
                                EventTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // add elements out-of-order
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 1000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3), 2500));

        // do a snapshot, close and restore again
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
        testHarness.close();

        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 1000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 2500));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 4), 5501));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 5), 6000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 5), 6000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 6), 6050));

        testHarness.processWatermark(new Watermark(12000));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-6", 10L, 5500L), 5499));
        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-6", 0L, 5500L), 5499));
        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-20", 5501L, 9050L), 9049));
        expectedOutput.add(new Watermark(12000));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 10), 15000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 20), 15000));

        testHarness.processWatermark(new Watermark(17999));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-30", 15000L, 18000L), 17999));
        expectedOutput.add(new Watermark(17999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());

        testHarness.close();
    }

    /** This tests whether merging works correctly with the CountTrigger. */
    @Test
    void testSessionWindowsWithCountTrigger() throws Exception {
        closeCalled.set(0);

        final int sessionSize = 3;

        ListStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(sessionSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalIterableWindowFunction<>(new SessionWindowFunction()),
                                PurgingTrigger.of(CountTrigger.of(4)),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // add elements out-of-order
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 1000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3), 2500));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 4), 3500));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 1000));

        // do a snapshot, close and restore again
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);
        testHarness.close();

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-10", 0L, 6500L), 6499));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());
        expectedOutput.clear();

        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 2500));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 6000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 6500));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 7000));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());

        // add an element that merges the two "key1" sessions, they should now have count 6, and
        // therefore fire
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 10), 4500));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-22", 10L, 10000L), 9999L));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());

        testHarness.close();
    }

    /** This tests whether merging works correctly with the ContinuousEventTimeTrigger. */
    @Test
    void testSessionWindowsWithContinuousEventTimeTrigger() throws Exception {
        closeCalled.set(0);

        final int sessionSize = 3;

        ListStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(sessionSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalIterableWindowFunction<>(new SessionWindowFunction()),
                                ContinuousEventTimeTrigger.of(Duration.ofSeconds(2)),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // add elements out-of-order and first trigger time is 2000
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 1500));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3), 2500));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 1000));

        // triggers emit and next trigger time is 4000
        testHarness.processWatermark(new Watermark(2500));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-1", 1500L, 4500L), 4499));
        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-6", 0L, 5500L), 5499));
        expectedOutput.add(new Watermark(2500));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 5), 4000));
        testHarness.processWatermark(new Watermark(3000));
        expectedOutput.add(new Watermark(3000));

        // do a snapshot, close and restore again
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());
        testHarness.close();

        expectedOutput.clear();
        testHarness = createTestHarness(operator);
        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 4000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 4), 3500));
        // triggers emit and next trigger time is 6000
        testHarness.processWatermark(new Watermark(4000));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-3", 1500L, 7000L), 6999));
        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-15", 0L, 7000L), 6999));
        expectedOutput.add(new Watermark(4000));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());

        testHarness.close();
    }

    /**
     * This tests a custom Session window assigner that assigns some elements to "point windows",
     * windows that have the same timestamp for start and end.
     *
     * <p>In this test, elements that have 33 as the second tuple field will be put into a point
     * window.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testPointSessions() throws Exception {
        closeCalled.set(0);

        ListStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                new PointSessionWindows(3000),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalIterableWindowFunction<>(new SessionWindowFunction()),
                                EventTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        OperatorSubtaskState snapshot;

        try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator)) {
            testHarness.open();

            // add elements out-of-order
            testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
            testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 33), 1000));

            // do a snapshot, close and restore again
            snapshot = testHarness.snapshot(0L, 0L);
        }

        try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator)) {
            testHarness.setup();
            testHarness.initializeState(snapshot);
            testHarness.open();

            testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 33), 2500));

            testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10));
            testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 1000));
            testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 33), 2500));

            testHarness.processWatermark(new Watermark(12000));

            expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-36", 10L, 4000L), 3999));
            expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-67", 0L, 3000L), 2999));
            expectedOutput.add(new Watermark(12000));

            TestHarnessUtil.assertOutputEqualsSorted(
                    "Output was not correct.",
                    expectedOutput,
                    testHarness.getOutput(),
                    new Tuple3ResultSortComparator());
        }
    }

    private static <OUT>
            OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, OUT> createTestHarness(
                    OneInputStreamOperatorFactory<Tuple2<String, Integer>, OUT> operator)
                    throws Exception {
        return new KeyedOneInputStreamOperatorTestHarness<>(
                operator, new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);
    }

    @Test
    @SuppressWarnings("unchecked")
    void testContinuousWatermarkTrigger() throws Exception {
        closeCalled.set(0);

        final int windowSize = 3;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        GlobalWindow>
                operator =
                        new WindowOperatorFactory<>(
                                GlobalWindows.create(),
                                new GlobalWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, GlobalWindow, Tuple2<String, Integer>>()),
                                ContinuousEventTimeTrigger.of(Duration.ofSeconds(windowSize)),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // The global window actually ignores these timestamps...

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0));

        // add elements out-of-order
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

        testHarness.processWatermark(new Watermark(1000));
        expectedOutput.add(new Watermark(1000));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(2000));
        expectedOutput.add(new Watermark(2000));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(3000));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), Long.MAX_VALUE));
        expectedOutput.add(new Watermark(3000));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(4000));
        expectedOutput.add(new Watermark(4000));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(5000));
        expectedOutput.add(new Watermark(5000));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(new Watermark(6000));

        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), Long.MAX_VALUE));

        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), Long.MAX_VALUE));
        expectedOutput.add(new Watermark(6000));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        // those don't have any effect...
        testHarness.processWatermark(new Watermark(7000));
        testHarness.processWatermark(new Watermark(8000));
        expectedOutput.add(new Watermark(7000));
        expectedOutput.add(new Watermark(8000));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testCountTrigger() throws Exception {
        closeCalled.set(0);

        final int windowSize = 4;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        GlobalWindow>
                operator =
                        new WindowOperatorFactory<>(
                                GlobalWindows.create(),
                                new GlobalWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, GlobalWindow, Tuple2<String, Integer>>()),
                                PurgingTrigger.of(CountTrigger.of(windowSize)),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // The global window actually ignores these timestamps...

        // add elements out-of-order
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));

        // do a snapshot, close and restore again
        OperatorSubtaskState snapshot = testHarness.snapshot(0L, 0L);

        testHarness.close();

        ConcurrentLinkedQueue<Object> outputBeforeClose = testHarness.getOutput();

        stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        operator =
                new WindowOperatorFactory<>(
                        GlobalWindows.create(),
                        new GlobalWindow.Serializer(),
                        new TupleKeySelector(),
                        BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new SerializerConfigImpl()),
                        stateDesc,
                        new InternalSingleValueWindowFunction<>(
                                new PassThroughWindowFunction<
                                        String, GlobalWindow, Tuple2<String, Integer>>()),
                        PurgingTrigger.of(CountTrigger.of(windowSize)),
                        0,
                        null /* late data output tag */);

        testHarness = createTestHarness(operator);

        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 4), Long.MAX_VALUE));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                Iterables.concat(outputBeforeClose, testHarness.getOutput()),
                new Tuple2ResultSortComparator());

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 4), Long.MAX_VALUE));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 4), Long.MAX_VALUE));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                Iterables.concat(outputBeforeClose, testHarness.getOutput()),
                new Tuple2ResultSortComparator());

        testHarness.close();
    }

    @Test
    void testEndOfStreamTrigger() throws Exception {
        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(
                                new ExecutionConfig().getSerializerConfig()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        GlobalWindow>
                operator =
                        new WindowOperatorFactory<>(
                                GlobalWindows.createWithEndOfStreamTrigger(),
                                new GlobalWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new ExecutionConfig().getSerializerConfig()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, GlobalWindow, Tuple2<String, Integer>>()),
                                GlobalWindows.createWithEndOfStreamTrigger().getDefaultTrigger(),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        // add elements out-of-order
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 20));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 0));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 999));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                Collections.EMPTY_LIST,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processWatermark(Watermark.MAX_WATERMARK);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), Long.MAX_VALUE));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), Long.MAX_VALUE));
        expectedOutput.add(Watermark.MAX_WATERMARK);

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.close();
    }

    @Test
    void testProcessingTimeTumblingWindows() throws Throwable {
        final int windowSize = 3;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                TumblingProcessingTimeWindows.of(Duration.ofSeconds(windowSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, TimeWindow, Tuple2<String, Integer>>()),
                                ProcessingTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        testHarness.setProcessingTime(3);

        // timestamp is ignored in processing time
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), Long.MAX_VALUE));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 7000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 7000));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 7000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 7000));

        testHarness.setProcessingTime(5000);

        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 2), 2999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 7000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 7000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 7000));

        testHarness.setProcessingTime(7000);

        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 5999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.close();
    }

    @Test
    void testProcessingTimeSlidingWindows() throws Throwable {
        final int windowSize = 3;
        final int windowSlide = 1;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                SlidingProcessingTimeWindows.of(
                                        Duration.ofSeconds(windowSize),
                                        Duration.ofSeconds(windowSlide)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, TimeWindow, Tuple2<String, Integer>>()),
                                ProcessingTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // timestamp is ignored in processing time
        testHarness.setProcessingTime(3);
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), Long.MAX_VALUE));

        testHarness.setProcessingTime(1000);

        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 1), 999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), Long.MAX_VALUE));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), Long.MAX_VALUE));

        testHarness.setProcessingTime(2000);

        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 1999));
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));

        testHarness.setProcessingTime(3000);

        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 2), 2999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));

        testHarness.setProcessingTime(7000);

        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 3999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 5), 3999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 5), 4999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 5999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.close();
    }

    @Test
    void testProcessingTimeSessionWindows() throws Throwable {
        final int windowGap = 3;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(windowGap)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, TimeWindow, Tuple2<String, Integer>>()),
                                ProcessingTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // timestamp is ignored in processing time
        testHarness.setProcessingTime(3);
        testHarness.processElement(
                new StreamRecord<>(new Tuple2<>("key2", 1), 1)); // Long.MAX_VALUE));

        testHarness.setProcessingTime(1000);
        testHarness.processElement(
                new StreamRecord<>(new Tuple2<>("key2", 1), 1002)); // Long.MAX_VALUE));

        testHarness.setProcessingTime(5000);

        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 3999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 5000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 5000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 5000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 5000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 5000));

        testHarness.setProcessingTime(10000);

        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 7999));
        expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 7999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        assertThat(testHarness.getOutput()).hasSameSizeAs(expectedOutput);
        for (Object elem : testHarness.getOutput()) {
            if (elem instanceof StreamRecord) {
                StreamRecord<Tuple2<String, Integer>> el =
                        (StreamRecord<Tuple2<String, Integer>>) elem;
                assertThat(expectedOutput).contains(el);
            }
        }
        testHarness.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDynamicEventTimeSessionWindows() throws Exception {
        closeCalled.set(0);

        SessionWindowTimeGapExtractor<Tuple2<String, Integer>> extractor =
                mock(SessionWindowTimeGapExtractor.class);
        when(extractor.extract(any(Tuple2.class)))
                .thenAnswer(
                        invocation -> {
                            Tuple2<String, Integer> element =
                                    (Tuple2<String, Integer>) invocation.getArguments()[0];
                            switch (element.f0) {
                                case "key1":
                                    return 3000L;
                                case "key2":
                                    switch (element.f1) {
                                        case 10:
                                            return 1000L;
                                        default:
                                            return 2000L;
                                    }
                                default:
                                    return 0L;
                            }
                        });

        ListStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                DynamicEventTimeSessionWindows.withDynamicGap(extractor),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalIterableWindowFunction<>(new SessionWindowFunction()),
                                EventTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // test different gaps for different keys
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 10));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 4), 5000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 5), 6000));

        testHarness.processWatermark(new Watermark(8999));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-3", 10L, 3010L), 3009));
        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-9", 5000L, 8000L), 7999));
        expectedOutput.add(new Watermark(8999));

        // test gap when it produces an end time before current timeout
        // the furthest timeout is respected
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 9000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 10000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 10), 10500));

        testHarness.processWatermark(new Watermark(12999));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-13", 9000L, 12000L), 11999));
        expectedOutput.add(new Watermark(12999));

        // test gap when it produces an end time after current timeout
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 10), 13000));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 10), 13500));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 14000));

        testHarness.processWatermark(new Watermark(16999));

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-21", 13000L, 16000L), 15999));
        expectedOutput.add(new Watermark(16999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());

        testHarness.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    void testDynamicProcessingTimeSessionWindows() throws Exception {
        closeCalled.set(0);

        SessionWindowTimeGapExtractor<Tuple2<String, Integer>> extractor =
                mock(SessionWindowTimeGapExtractor.class);
        when(extractor.extract(any(Tuple2.class)))
                .thenAnswer(
                        invocation -> {
                            Tuple2<String, Integer> element =
                                    (Tuple2<String, Integer>) invocation.getArguments()[0];
                            switch (element.f0) {
                                case "key1":
                                    return 3000L;
                                case "key2":
                                    switch (element.f1) {
                                        case 10:
                                            return 1000L;
                                        default:
                                            return 2000L;
                                    }
                                default:
                                    return 0L;
                            }
                        });

        ListStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                DynamicProcessingTimeSessionWindows.withDynamicGap(extractor),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalIterableWindowFunction<>(new SessionWindowFunction()),
                                ProcessingTimeTrigger.create(),
                                0,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

        testHarness.open();

        // test different gaps for different keys
        testHarness.setProcessingTime(10);
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 10));

        testHarness.setProcessingTime(5000);
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 4), 5000));
        testHarness.setProcessingTime(6000);
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 5), 6000));
        testHarness.setProcessingTime(8999);

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-3", 10L, 3010L), 3009));
        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-9", 5000L, 8000L), 7999));

        // test gap when it produces an end time before current timeout
        // the furthest timeout is respected
        testHarness.setProcessingTime(9000);
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 9000));
        testHarness.setProcessingTime(10000);
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 10000));
        testHarness.setProcessingTime(10500);
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 10), 10500));
        testHarness.setProcessingTime(10500);

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-13", 9000L, 12000L), 11999));

        // test gap when it produces an end time after current timeout
        testHarness.setProcessingTime(13000);
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 10), 13000));
        testHarness.setProcessingTime(13500);
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 10), 13500));
        testHarness.setProcessingTime(14000);
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 14000));
        testHarness.setProcessingTime(16999);

        expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-21", 13000L, 16000L), 15999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expectedOutput,
                testHarness.getOutput(),
                new Tuple3ResultSortComparator());

        testHarness.close();
    }

    @Test
    void testLateness() throws Exception {
        final int windowSize = 2;
        final long lateness = 500;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                TumblingEventTimeWindows.of(Duration.ofSeconds(windowSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, TimeWindow, Tuple2<String, Integer>>()),
                                PurgingTrigger.of(EventTimeTrigger.create()),
                                lateness,
                                lateOutputTag);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Object> lateExpected = new ConcurrentLinkedQueue<>();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 500));
        testHarness.processWatermark(new Watermark(1500));

        expected.add(new Watermark(1500));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1300));
        testHarness.processWatermark(new Watermark(2300));

        expected.add(new StreamRecord<>(new Tuple2<>("key2", 2), 1999));
        expected.add(new Watermark(2300));

        // this will not be sideoutput because window.maxTimestamp() + allowedLateness >
        // currentWatermark
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1997));
        testHarness.processWatermark(new Watermark(6000));

        // this is 1 and not 3 because the trigger fires and purges
        expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
        expected.add(new Watermark(6000));

        // this will be side output because window.maxTimestamp() + allowedLateness <
        // currentWatermark
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
        testHarness.processWatermark(new Watermark(7000));

        lateExpected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
        expected.add(new Watermark(7000));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expected,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());

        TestHarnessUtil.assertOutputEqualsSorted(
                "SideOutput was not correct.",
                lateExpected,
                (Iterable) testHarness.getSideOutput(lateOutputTag),
                new Tuple2ResultSortComparator());

        testHarness.close();
    }

    @Test
    void testCleanupTimeOverflow() throws Exception {
        final int windowSize = 1000;
        final long lateness = 2000;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        TumblingEventTimeWindows windowAssigner =
                TumblingEventTimeWindows.of(Duration.ofMillis(windowSize));

        final WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                windowAssigner,
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, TimeWindow, Tuple2<String, Integer>>()),
                                EventTimeTrigger.create(),
                                lateness,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

        long timestamp = Long.MAX_VALUE - 1750;
        Collection<TimeWindow> windows =
                windowAssigner.assignWindows(
                        new Tuple2<>("key2", 1),
                        timestamp,
                        new WindowAssigner.WindowAssignerContext() {
                            @Override
                            public long getCurrentProcessingTime() {
                                return ((WindowOperator<?, ?, ?, ?, ?>) testHarness.getOperator())
                                        .windowAssignerContext.getCurrentProcessingTime();
                            }
                        });
        TimeWindow window = Iterables.getOnlyElement(windows);

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), timestamp));

        // the garbage collection timer would wrap-around
        assertThat(window.maxTimestamp() + lateness).isLessThan(window.maxTimestamp());

        // and it would prematurely fire with watermark (Long.MAX_VALUE - 1500)
        assertThat(window.maxTimestamp() + lateness).isLessThan(Long.MAX_VALUE - 1500);

        // if we don't correctly prevent wrap-around in the garbage collection
        // timers this watermark will clean our window state for the just-added
        // element/window
        testHarness.processWatermark(new Watermark(Long.MAX_VALUE - 1500));

        // this watermark is before the end timestamp of our only window
        assertThat(window.maxTimestamp()).isStrictlyBetween(Long.MAX_VALUE - 1500, Long.MAX_VALUE);

        // push in a watermark that will trigger computation of our window
        testHarness.processWatermark(new Watermark(window.maxTimestamp()));

        expected.add(new Watermark(Long.MAX_VALUE - 1500));
        expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), window.maxTimestamp()));
        expected.add(new Watermark(window.maxTimestamp()));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expected,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());
        testHarness.close();
    }

    @Test
    void testSideOutputDueToLatenessTumbling() throws Exception {
        final int windowSize = 2;
        final long lateness = 0;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                TumblingEventTimeWindows.of(Duration.ofSeconds(windowSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, TimeWindow, Tuple2<String, Integer>>()),
                                EventTimeTrigger.create(),
                                lateness,
                                lateOutputTag);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Object> sideExpected = new ConcurrentLinkedQueue<>();

        // normal element
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(1985));

        expected.add(new Watermark(1985));

        // this will not be dropped because window.maxTimestamp() + allowedLateness >
        // currentWatermark
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1980));
        testHarness.processWatermark(new Watermark(1999));

        expected.add(new StreamRecord<>(new Tuple2<>("key2", 2), 1999));
        expected.add(new Watermark(1999));

        // sideoutput as late, will reuse previous timestamp since only input tuple is sideoutputed
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
        sideExpected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2001));
        testHarness.processWatermark(new Watermark(2999));

        expected.add(new Watermark(2999));

        testHarness.processWatermark(new Watermark(3999));

        expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
        expected.add(new Watermark(3999));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expected,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());
        TestHarnessUtil.assertOutputEqualsSorted(
                "SideOutput was not correct.",
                sideExpected,
                (Iterable) testHarness.getSideOutput(lateOutputTag),
                new Tuple2ResultSortComparator());
        testHarness.close();
    }

    @Test
    void testSideOutputDueToLatenessSliding() throws Exception {
        final int windowSize = 3;
        final int windowSlide = 1;
        final long lateness = 0;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                SlidingEventTimeWindows.of(
                                        Duration.ofSeconds(windowSize),
                                        Duration.ofSeconds(windowSlide)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, TimeWindow, Tuple2<String, Integer>>()),
                                EventTimeTrigger.create(),
                                lateness,
                                lateOutputTag /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Object> sideExpected = new ConcurrentLinkedQueue<>();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(1999));

        expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
        expected.add(new Watermark(1999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2000));
        testHarness.processWatermark(new Watermark(3000));

        expected.add(new StreamRecord<>(new Tuple2<>("key2", 2), 2999));
        expected.add(new Watermark(3000));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3001));

        // lateness is set to 0 and window size = 3 sec and slide 1, the following 2 elements (2400)
        // are assigned to windows ending at 2999, 3999, 4999.
        // The 2999 is dropped because it is already late (WM = 2999) but the rest are kept.

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2400));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2400));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3001));
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 3900));
        testHarness.processWatermark(new Watermark(6000));

        expected.add(new StreamRecord<>(new Tuple2<>("key2", 5), 3999));
        expected.add(new StreamRecord<>(new Tuple2<>("key1", 2), 3999));

        expected.add(new StreamRecord<>(new Tuple2<>("key2", 4), 4999));
        expected.add(new StreamRecord<>(new Tuple2<>("key1", 2), 4999));

        expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 5999));
        expected.add(new StreamRecord<>(new Tuple2<>("key1", 2), 5999));

        expected.add(new Watermark(6000));

        // sideoutput element due to lateness
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 3001));
        sideExpected.add(new StreamRecord<>(new Tuple2<>("key1", 1), 3001));

        testHarness.processWatermark(new Watermark(25000));

        expected.add(new Watermark(25000));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expected,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());
        TestHarnessUtil.assertOutputEqualsSorted(
                "SideOutput was not correct.",
                sideExpected,
                (Iterable) testHarness.getSideOutput(lateOutputTag),
                new Tuple2ResultSortComparator());
        testHarness.close();
    }

    @Test
    void testSideOutputDueToLatenessSessionZeroLatenessPurgingTrigger() throws Exception {
        final int gapSize = 3;
        final long lateness = 0;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(gapSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new ReducedSessionWindowFunction()),
                                PurgingTrigger.of(EventTimeTrigger.create()),
                                lateness,
                                lateOutputTag);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Object> sideExpected = new ConcurrentLinkedQueue<>();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(1999));

        expected.add(new Watermark(1999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2000));
        testHarness.processWatermark(new Watermark(4998));

        expected.add(new Watermark(4998));

        // this will not be dropped because the session we're adding two has maxTimestamp
        // after the current watermark
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 4500));

        // new session
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 8500));
        testHarness.processWatermark(new Watermark(7400));

        expected.add(new Watermark(7400));

        // this will merge the two sessions into one
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 7000));
        testHarness.processWatermark(new Watermark(11501));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-5", 1000L, 11500L), 11499));
        expected.add(new Watermark(11501));

        // new session
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 11600));
        testHarness.processWatermark(new Watermark(14600));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 11600L, 14600L), 14599));
        expected.add(new Watermark(14600));

        // this is side output as late
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 10000));
        sideExpected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 10000));

        // this is also side output as late (we test that they are not accidentally merged)
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 10100));
        sideExpected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 10100));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 14500));
        testHarness.processWatermark(new Watermark(20000));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 14500L, 17500L), 17499));
        expected.add(new Watermark(20000));

        testHarness.processWatermark(new Watermark(100000));

        expected.add(new Watermark(100000));

        ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();
        ConcurrentLinkedQueue<StreamRecord<Tuple2<String, Integer>>> sideActual =
                testHarness.getSideOutput(lateOutputTag);

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.", expected, actual, new Tuple2ResultSortComparator());
        TestHarnessUtil.assertOutputEqualsSorted(
                "SideOutput was not correct.",
                sideExpected,
                (Iterable) sideActual,
                new Tuple2ResultSortComparator());

        testHarness.close();
    }

    @Test
    void testSideOutputDueToLatenessSessionZeroLateness() throws Exception {
        final int gapSize = 3;
        final long lateness = 0;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(gapSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new ReducedSessionWindowFunction()),
                                EventTimeTrigger.create(),
                                lateness,
                                lateOutputTag);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Object> sideExpected = new ConcurrentLinkedQueue<>();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(1999));

        expected.add(new Watermark(1999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2000));
        testHarness.processWatermark(new Watermark(4998));

        expected.add(new Watermark(4998));

        // this will not be dropped because the session we're adding two has maxTimestamp
        // after the current watermark
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 4500));

        // new session
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 8500));
        testHarness.processWatermark(new Watermark(7400));

        expected.add(new Watermark(7400));

        // this will merge the two sessions into one
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 7000));
        testHarness.processWatermark(new Watermark(11501));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-5", 1000L, 11500L), 11499));
        expected.add(new Watermark(11501));

        // new session
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 11600));
        testHarness.processWatermark(new Watermark(14600));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 11600L, 14600L), 14599));
        expected.add(new Watermark(14600));

        // this is sideoutput as late, reuse last timestamp
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 10000));
        sideExpected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 10000));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 14500));
        testHarness.processWatermark(new Watermark(20000));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 14500L, 17500L), 17499));
        expected.add(new Watermark(20000));

        testHarness.processWatermark(new Watermark(100000));
        expected.add(new Watermark(100000));

        ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();
        ConcurrentLinkedQueue<StreamRecord<Tuple2<String, Integer>>> sideActual =
                testHarness.getSideOutput(lateOutputTag);
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.", expected, actual, new Tuple2ResultSortComparator());
        TestHarnessUtil.assertOutputEqualsSorted(
                "SideOutput was not correct.",
                sideExpected,
                (Iterable) sideActual,
                new Tuple2ResultSortComparator());
        testHarness.close();
    }

    @Test
    void testDropDueToLatenessSessionWithLatenessPurgingTrigger() throws Exception {

        // this has the same output as testSideOutputDueToLatenessSessionZeroLateness() because
        // the allowed lateness is too small to make a difference

        final int gapSize = 3;
        final long lateness = 10;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(gapSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new ReducedSessionWindowFunction()),
                                PurgingTrigger.of(EventTimeTrigger.create()),
                                lateness,
                                lateOutputTag);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(1999));

        expected.add(new Watermark(1999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2000));
        testHarness.processWatermark(new Watermark(4998));

        expected.add(new Watermark(4998));

        // this will not be dropped because the session we're adding two has maxTimestamp
        // after the current watermark
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 4500));

        // new session
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 8500));
        testHarness.processWatermark(new Watermark(7400));

        expected.add(new Watermark(7400));

        // this will merge the two sessions into one
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 7000));
        testHarness.processWatermark(new Watermark(11501));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-5", 1000L, 11500L), 11499));
        expected.add(new Watermark(11501));

        // new session
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 11600));
        testHarness.processWatermark(new Watermark(14600));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 11600L, 14600L), 14599));
        expected.add(new Watermark(14600));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 10000));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 10000L, 14600L), 14599));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 14500));
        testHarness.processWatermark(new Watermark(20000));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 10000L, 17500L), 17499));
        expected.add(new Watermark(20000));

        testHarness.processWatermark(new Watermark(100000));
        expected.add(new Watermark(100000));

        ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
        testHarness.close();
    }

    @Test
    void testNotSideOutputDueToLatenessSessionWithLateness() throws Exception {
        // same as testSideOutputDueToLatenessSessionWithLateness() but with an accumulating
        // trigger, i.e.
        // one that does not return FIRE_AND_PURGE when firing but just FIRE. The expected
        // results are therefore slightly different.

        final int gapSize = 3;
        final long lateness = 10;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(gapSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new ReducedSessionWindowFunction()),
                                EventTimeTrigger.create(),
                                lateness,
                                lateOutputTag /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(1999));

        expected.add(new Watermark(1999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2000));
        testHarness.processWatermark(new Watermark(4998));

        expected.add(new Watermark(4998));

        // this will not be sideoutput because the session we're adding two has maxTimestamp
        // after the current watermark
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 4500));

        // new session
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 8500));
        testHarness.processWatermark(new Watermark(7400));

        expected.add(new Watermark(7400));

        // this will merge the two sessions into one
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 7000));
        testHarness.processWatermark(new Watermark(11501));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-5", 1000L, 11500L), 11499));
        expected.add(new Watermark(11501));

        // new session
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 11600));
        testHarness.processWatermark(new Watermark(14600));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 11600L, 14600L), 14599));
        expected.add(new Watermark(14600));

        // because of the small allowed lateness and because the trigger is accumulating
        // this will be merged into the session (11600-14600) and therefore will not
        // be sideoutput as late
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 10000));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 14500));

        // adding ("key2", 1) extended the session to (10000-146000) for which
        // maxTimestamp <= currentWatermark. Therefore, we immediately get a firing
        // with the current version of EventTimeTrigger/EventTimeTriggerAccum
        expected.add(new StreamRecord<>(new Tuple3<>("key2-2", 10000L, 14600L), 14599));

        ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();
        ConcurrentLinkedQueue<StreamRecord<Tuple2<String, Integer>>> sideActual =
                testHarness.getSideOutput(lateOutputTag);

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
        assertThat(sideActual).isNull();

        testHarness.processWatermark(new Watermark(20000));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-3", 10000L, 17500L), 17499));
        expected.add(new Watermark(20000));

        testHarness.processWatermark(new Watermark(100000));

        expected.add(new Watermark(100000));

        actual = testHarness.getOutput();
        sideActual = testHarness.getSideOutput(lateOutputTag);
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
        assertThat(sideActual).isNull();

        testHarness.close();
    }

    @Test
    void testNotSideOutputDueToLatenessSessionWithHugeLatenessPurgingTrigger() throws Exception {

        final int gapSize = 3;
        final long lateness = 10000;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(gapSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new ReducedSessionWindowFunction()),
                                PurgingTrigger.of(EventTimeTrigger.create()),
                                lateness,
                                lateOutputTag /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(1999));

        expected.add(new Watermark(1999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2000));
        testHarness.processWatermark(new Watermark(4998));

        expected.add(new Watermark(4998));

        // this will not be sideoutput because the session we're adding two has maxTimestamp
        // after the current watermark
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 4500));

        // new session
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 8500));
        testHarness.processWatermark(new Watermark(7400));

        expected.add(new Watermark(7400));

        // this will merge the two sessions into one
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 7000));
        testHarness.processWatermark(new Watermark(11501));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-5", 1000L, 11500L), 11499));
        expected.add(new Watermark(11501));

        // new session
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 11600));
        testHarness.processWatermark(new Watermark(14600));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 11600L, 14600L), 14599));
        expected.add(new Watermark(14600));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 10000));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 1000L, 14600L), 14599));

        ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();
        ConcurrentLinkedQueue<StreamRecord<Tuple2<String, Integer>>> sideActual =
                testHarness.getSideOutput(lateOutputTag);
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
        assertThat(sideActual).isNull();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 14500));
        testHarness.processWatermark(new Watermark(20000));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 1000L, 17500L), 17499));
        expected.add(new Watermark(20000));

        testHarness.processWatermark(new Watermark(100000));

        expected.add(new Watermark(100000));

        actual = testHarness.getOutput();
        sideActual = testHarness.getSideOutput(lateOutputTag);
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
        assertThat(sideActual).isNull();

        testHarness.close();
    }

    @Test
    void testNotSideOutputDueToLatenessSessionWithHugeLateness() throws Exception {
        final int gapSize = 3;
        final long lateness = 10000;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(gapSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new ReducedSessionWindowFunction()),
                                EventTimeTrigger.create(),
                                lateness,
                                lateOutputTag /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(1999));

        expected.add(new Watermark(1999));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2000));
        testHarness.processWatermark(new Watermark(4998));

        expected.add(new Watermark(4998));

        // this will not be sideoutput because the session we're adding two has maxTimestamp
        // after the current watermark
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 4500));

        // new session
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 8500));
        testHarness.processWatermark(new Watermark(7400));

        expected.add(new Watermark(7400));

        // this will merge the two sessions into one
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 7000));
        testHarness.processWatermark(new Watermark(11501));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-5", 1000L, 11500L), 11499));
        expected.add(new Watermark(11501));

        // new session
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 11600));
        testHarness.processWatermark(new Watermark(14600));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 11600L, 14600L), 14599));
        expected.add(new Watermark(14600));

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 10000));

        // the maxTimestamp of the merged session is already late,
        // so we get an immediate firing
        expected.add(new StreamRecord<>(new Tuple3<>("key2-7", 1000L, 14600L), 14599));

        ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();
        ConcurrentLinkedQueue<StreamRecord<Tuple2<String, Integer>>> sideActual =
                testHarness.getSideOutput(lateOutputTag);
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
        assertThat(sideActual).isNull();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 14500));
        testHarness.processWatermark(new Watermark(20000));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-8", 1000L, 17500L), 17499));
        expected.add(new Watermark(20000));

        testHarness.processWatermark(new Watermark(100000));
        expected.add(new Watermark(100000));

        actual = testHarness.getOutput();
        sideActual = testHarness.getSideOutput(lateOutputTag);

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
        assertThat(sideActual).isNull();

        testHarness.close();
    }

    @Test
    void testCleanupTimerWithEmptyListStateForTumblingWindows2() throws Exception {
        final int windowSize = 2;
        final long lateness = 100;

        ListStateDescriptor<Tuple2<String, Integer>> windowStateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        String,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                TumblingEventTimeWindows.of(Duration.ofSeconds(windowSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                windowStateDesc,
                                new InternalIterableWindowFunction<>(new PassThroughFunction2()),
                                new EventTimeTriggerAccumGC(lateness),
                                lateness,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, String> testHarness =
                createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

        // normal element
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(1599));
        testHarness.processWatermark(new Watermark(1999));
        testHarness.processWatermark(new Watermark(2100));
        testHarness.processWatermark(new Watermark(5000));

        expected.add(new Watermark(1599));
        expected.add(new StreamRecord<>("GOT: (key2,1)", 1999));
        expected.add(new Watermark(1999)); // here it fires and purges
        expected.add(new Watermark(2100)); // here is the cleanup timer
        expected.add(new Watermark(5000));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expected,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());
        testHarness.close();
    }

    private static class PassThroughFunction2
            implements WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(
                String k,
                TimeWindow window,
                Iterable<Tuple2<String, Integer>> input,
                Collector<String> out)
                throws Exception {
            out.collect("GOT: " + Joiner.on(",").join(input));
        }
    }

    @Test
    void testCleanupTimerWithEmptyListStateForTumblingWindows() throws Exception {
        final int windowSize = 2;
        final long lateness = 1;

        ListStateDescriptor<Tuple2<String, Integer>> windowStateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                TumblingEventTimeWindows.of(Duration.ofSeconds(windowSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                windowStateDesc,
                                new InternalIterableWindowFunction<>(new PassThroughFunction()),
                                EventTimeTrigger.create(),
                                lateness,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

        // normal element
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(1599));
        testHarness.processWatermark(new Watermark(1999));
        testHarness.processWatermark(new Watermark(2000));
        testHarness.processWatermark(new Watermark(5000));

        expected.add(new Watermark(1599));
        expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
        expected.add(new Watermark(1999)); // here it fires and purges
        expected.add(new Watermark(2000)); // here is the cleanup timer
        expected.add(new Watermark(5000));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expected,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());
        testHarness.close();
    }

    @Test
    void testCleanupTimerWithEmptyReduceStateForTumblingWindows() throws Exception {
        final int windowSize = 2;
        final long lateness = 1;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                TumblingEventTimeWindows.of(Duration.ofSeconds(windowSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new PassThroughWindowFunction<
                                                String, TimeWindow, Tuple2<String, Integer>>()),
                                EventTimeTrigger.create(),
                                lateness,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

        // normal element
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(1599));
        testHarness.processWatermark(new Watermark(1999));
        testHarness.processWatermark(new Watermark(2000));
        testHarness.processWatermark(new Watermark(5000));

        expected.add(new Watermark(1599));
        expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
        expected.add(new Watermark(1999)); // here it fires and purges
        expected.add(new Watermark(2000)); // here is the cleanup timer
        expected.add(new Watermark(5000));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expected,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());
        testHarness.close();
    }

    @Test
    void testCleanupTimerWithEmptyListStateForSessionWindows() throws Exception {
        final int gapSize = 3;
        final long lateness = 10;

        ListStateDescriptor<Tuple2<String, Integer>> windowStateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(gapSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                windowStateDesc,
                                new InternalIterableWindowFunction<>(new PassThroughFunction()),
                                EventTimeTrigger.create(),
                                lateness,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(4998));

        expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
        expected.add(new Watermark(4998));

        testHarness.processWatermark(new Watermark(14600));
        expected.add(new Watermark(14600));

        ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.", expected, actual, new Tuple2ResultSortComparator());
        testHarness.close();
    }

    @Test
    void testCleanupTimerWithEmptyReduceStateForSessionWindows() throws Exception {

        final int gapSize = 3;
        final long lateness = 10;

        ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc =
                new ReducingStateDescriptor<>(
                        "window-contents",
                        new SumReducer(),
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Tuple2<String, Integer>,
                        Tuple3<String, Long, Long>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                EventTimeSessionWindows.withGap(Duration.ofSeconds(gapSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                stateDesc,
                                new InternalSingleValueWindowFunction<>(
                                        new ReducedSessionWindowFunction()),
                                EventTimeTrigger.create(),
                                lateness,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

        testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
        testHarness.processWatermark(new Watermark(4998));

        expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 1000L, 4000L), 3999));
        expected.add(new Watermark(4998));

        testHarness.processWatermark(new Watermark(14600));
        expected.add(new Watermark(14600));

        ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();
        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.", expected, actual, new Tuple2ResultSortComparator());
        testHarness.close();
    }

    @Test
    void testCleanupTimerWithEmptyStateNoResultForTumblingWindows() throws Exception {
        final int windowSize = 2;
        final long lateness = 1;

        ListStateDescriptor<Tuple2<String, Integer>> windowStateDesc =
                new ListStateDescriptor<>(
                        "window-contents",
                        STRING_INT_TUPLE.createSerializer(new SerializerConfigImpl()));

        WindowOperatorFactory<
                        String,
                        Tuple2<String, Integer>,
                        Iterable<Tuple2<String, Integer>>,
                        Tuple2<String, Integer>,
                        TimeWindow>
                operator =
                        new WindowOperatorFactory<>(
                                TumblingEventTimeWindows.of(Duration.ofSeconds(windowSize)),
                                new TimeWindow.Serializer(),
                                new TupleKeySelector(),
                                BasicTypeInfo.STRING_TYPE_INFO.createSerializer(
                                        new SerializerConfigImpl()),
                                windowStateDesc,
                                new InternalIterableWindowFunction<>(new EmptyReturnFunction()),
                                new FireEverytimeOnElementAndEventTimeTrigger(),
                                lateness,
                                null /* late data output tag */);

        OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>>
                testHarness = createTestHarness(operator);

        testHarness.open();

        ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
        // normal element
        testHarness.processElement(new StreamRecord<>(new Tuple2<>("test_key", 1), 1000));
        assertThat(
                        ((WindowOperator<?, ?, ?, ?, ?>) testHarness.getOperator())
                                .processContext
                                .windowState()
                                .getListState(windowStateDesc)
                                .get()
                                .toString())
                .isEqualTo("[(test_key,1)]");
        testHarness.processWatermark(new Watermark(1599));
        assertThat(
                        ((WindowOperator<?, ?, ?, ?, ?>) testHarness.getOperator())
                                .processContext
                                .windowState()
                                .getListState(windowStateDesc)
                                .get()
                                .toString())
                .isEqualTo("[(test_key,1)]");
        testHarness.processWatermark(new Watermark(1699));
        assertThat(
                        ((WindowOperator<?, ?, ?, ?, ?>) testHarness.getOperator())
                                .processContext
                                .windowState()
                                .getListState(windowStateDesc)
                                .get()
                                .toString())
                .isEqualTo("[(test_key,1)]");
        testHarness.processWatermark(new Watermark(1799));
        assertThat(
                        ((WindowOperator<?, ?, ?, ?, ?>) testHarness.getOperator())
                                .processContext
                                .windowState()
                                .getListState(windowStateDesc)
                                .get()
                                .toString())
                .isEqualTo("[(test_key,1)]");
        testHarness.processWatermark(new Watermark(1999));
        assertThat(
                        ((WindowOperator<?, ?, ?, ?, ?>) testHarness.getOperator())
                                .processContext
                                .windowState()
                                .getListState(windowStateDesc)
                                .get()
                                .toString())
                .isEqualTo("[(test_key,1)]");
        testHarness.processWatermark(new Watermark(2000));
        assertThat(
                        ((WindowOperator<?, ?, ?, ?, ?>) testHarness.getOperator())
                                .processContext
                                .windowState()
                                .getListState(windowStateDesc)
                                .get()
                                .toString())
                .isEqualTo("[]");
        testHarness.processWatermark(new Watermark(5000));
        assertThat(
                        ((WindowOperator<?, ?, ?, ?, ?>) testHarness.getOperator())
                                .processContext
                                .windowState()
                                .getListState(windowStateDesc)
                                .get()
                                .toString())
                .isEqualTo("[]");

        expected.add(new Watermark(1599));
        expected.add(new Watermark(1699));
        expected.add(new Watermark(1799));
        expected.add(new Watermark(1999)); // here it fires and purges
        expected.add(new Watermark(2000)); // here is the cleanup timer
        expected.add(new Watermark(5000));

        TestHarnessUtil.assertOutputEqualsSorted(
                "Output was not correct.",
                expected,
                testHarness.getOutput(),
                new Tuple2ResultSortComparator());
        testHarness.close();
    }

    // ------------------------------------------------------------------------
    //  UDFs
    // ------------------------------------------------------------------------

    private static class PassThroughFunction
            implements WindowFunction<
                    Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(
                String k,
                TimeWindow window,
                Iterable<Tuple2<String, Integer>> input,
                Collector<Tuple2<String, Integer>> out)
                throws Exception {
            for (Tuple2<String, Integer> in : input) {
                out.collect(in);
            }
        }
    }

    private static class EmptyReturnFunction
            implements WindowFunction<
                    Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(
                String k,
                TimeWindow window,
                Iterable<Tuple2<String, Integer>> input,
                Collector<Tuple2<String, Integer>> out)
                throws Exception {}
    }

    private static class SumReducer implements ReduceFunction<Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;

        @Override
        public Tuple2<String, Integer> reduce(
                Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
            return new Tuple2<>(value2.f0, value1.f1 + value2.f1);
        }
    }

    private static class RichSumReducer<W extends Window>
            extends RichWindowFunction<
                    Tuple2<String, Integer>, Tuple2<String, Integer>, String, W> {
        private static final long serialVersionUID = 1L;

        private boolean openCalled = false;

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            openCalled = true;
        }

        @Override
        public void close() throws Exception {
            super.close();
            closeCalled.incrementAndGet();
        }

        @Override
        public void apply(
                String key,
                W window,
                Iterable<Tuple2<String, Integer>> input,
                Collector<Tuple2<String, Integer>> out)
                throws Exception {

            assertThat(openCalled).as("Open was not called").isTrue();

            int sum = 0;

            for (Tuple2<String, Integer> t : input) {
                sum += t.f1;
            }
            out.collect(new Tuple2<>(key, sum));
        }
    }

    @SuppressWarnings("unchecked")
    private static class Tuple2ResultSortComparator implements Comparator<Object>, Serializable {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 instanceof Watermark || o2 instanceof Watermark) {
                return 0;
            } else {
                StreamRecord<Tuple2<String, Integer>> sr0 =
                        (StreamRecord<Tuple2<String, Integer>>) o1;
                StreamRecord<Tuple2<String, Integer>> sr1 =
                        (StreamRecord<Tuple2<String, Integer>>) o2;
                if (sr0.getTimestamp() != sr1.getTimestamp()) {
                    return (int) (sr0.getTimestamp() - sr1.getTimestamp());
                }
                int comparison = sr0.getValue().f0.compareTo(sr1.getValue().f0);
                if (comparison != 0) {
                    return comparison;
                } else {
                    return sr0.getValue().f1 - sr1.getValue().f1;
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static class Tuple3ResultSortComparator implements Comparator<Object>, Serializable {
        @Override
        public int compare(Object o1, Object o2) {
            if (o1 instanceof Watermark || o2 instanceof Watermark) {
                return 0;
            } else {
                StreamRecord<Tuple3<String, Long, Long>> sr0 =
                        (StreamRecord<Tuple3<String, Long, Long>>) o1;
                StreamRecord<Tuple3<String, Long, Long>> sr1 =
                        (StreamRecord<Tuple3<String, Long, Long>>) o2;
                if (sr0.getTimestamp() != sr1.getTimestamp()) {
                    return (int) (sr0.getTimestamp() - sr1.getTimestamp());
                }
                int comparison = sr0.getValue().f0.compareTo(sr1.getValue().f0);
                if (comparison != 0) {
                    return comparison;
                } else {
                    comparison = (int) (sr0.getValue().f1 - sr1.getValue().f1);
                    if (comparison != 0) {
                        return comparison;
                    }
                    return (int) (sr0.getValue().f2 - sr1.getValue().f2);
                }
            }
        }
    }

    private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
        private static final long serialVersionUID = 1L;

        @Override
        public String getKey(Tuple2<String, Integer> value) throws Exception {
            return value.f0;
        }
    }

    private static class SessionWindowFunction
            implements WindowFunction<
                    Tuple2<String, Integer>, Tuple3<String, Long, Long>, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(
                String key,
                TimeWindow window,
                Iterable<Tuple2<String, Integer>> values,
                Collector<Tuple3<String, Long, Long>> out)
                throws Exception {
            int sum = 0;
            for (Tuple2<String, Integer> i : values) {
                sum += i.f1;
            }
            String resultString = key + "-" + sum;
            out.collect(new Tuple3<>(resultString, window.getStart(), window.getEnd()));
        }
    }

    private static class ReducedSessionWindowFunction
            implements WindowFunction<
                    Tuple2<String, Integer>, Tuple3<String, Long, Long>, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void apply(
                String key,
                TimeWindow window,
                Iterable<Tuple2<String, Integer>> values,
                Collector<Tuple3<String, Long, Long>> out)
                throws Exception {
            for (Tuple2<String, Integer> val : values) {
                out.collect(new Tuple3<>(key + "-" + val.f1, window.getStart(), window.getEnd()));
            }
        }
    }

    private static class SessionProcessWindowFunction
            extends ProcessWindowFunction<
                    Tuple2<String, Integer>, Tuple3<String, Long, Long>, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(
                String key,
                Context context,
                Iterable<Tuple2<String, Integer>> values,
                Collector<Tuple3<String, Long, Long>> out)
                throws Exception {
            int sum = 0;
            for (Tuple2<String, Integer> i : values) {
                sum += i.f1;
            }
            String resultString = key + "-" + sum;
            TimeWindow window = context.window();
            out.collect(new Tuple3<>(resultString, window.getStart(), window.getEnd()));
        }
    }

    private static class ReducedProcessSessionWindowFunction
            extends ProcessWindowFunction<
                    Tuple2<String, Integer>, Tuple3<String, Long, Long>, String, TimeWindow> {
        private static final long serialVersionUID = 1L;

        @Override
        public void process(
                String key,
                Context context,
                Iterable<Tuple2<String, Integer>> values,
                Collector<Tuple3<String, Long, Long>> out)
                throws Exception {
            TimeWindow window = context.window();
            for (Tuple2<String, Integer> val : values) {
                out.collect(new Tuple3<>(key + "-" + val.f1, window.getStart(), window.getEnd()));
            }
        }
    }

    private static class PointSessionWindows extends EventTimeSessionWindows {
        private static final long serialVersionUID = 1L;

        private PointSessionWindows(long sessionTimeout) {
            super(sessionTimeout);
        }

        @Override
        @SuppressWarnings("unchecked")
        public Collection<TimeWindow> assignWindows(
                Object element, long timestamp, WindowAssignerContext ctx) {
            if (element instanceof Tuple2) {
                Tuple2<String, Integer> t2 = (Tuple2<String, Integer>) element;
                if (t2.f1 == 33) {
                    return Collections.singletonList(new TimeWindow(timestamp, timestamp));
                }
            }
            return Collections.singletonList(new TimeWindow(timestamp, timestamp + sessionTimeout));
        }
    }

    /**
     * A trigger that fires at the end of the window but does not purge the state of the fired
     * window. This is to test the state garbage collection mechanism.
     */
    private static class EventTimeTriggerAccumGC extends Trigger<Object, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private long cleanupTime;

        public EventTimeTriggerAccumGC(long cleanupTime) {
            this.cleanupTime = cleanupTime;
        }

        @Override
        public TriggerResult onElement(
                Object element, long timestamp, TimeWindow window, TriggerContext ctx)
                throws Exception {
            if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
                // if the watermark is already past the window fire immediately
                return TriggerResult.FIRE;
            } else {
                ctx.registerEventTimeTimer(window.maxTimestamp());
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
            return time == window.maxTimestamp() || time == window.maxTimestamp() + cleanupTime
                    ? TriggerResult.FIRE_AND_PURGE
                    : TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
                throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ctx.deleteEventTimeTimer(window.maxTimestamp());
        }

        @Override
        public boolean canMerge() {
            return true;
        }

        @Override
        public void onMerge(TimeWindow window, OnMergeContext ctx) {
            ctx.registerEventTimeTimer(window.maxTimestamp());
        }

        @Override
        public String toString() {
            return "EventTimeTrigger()";
        }
    }

    private static class FireEverytimeOnElementAndEventTimeTrigger
            extends Trigger<Tuple2<String, Integer>, TimeWindow> {
        @Override
        public TriggerResult onElement(
                Tuple2<String, Integer> element,
                long timestamp,
                TimeWindow window,
                TriggerContext ctx)
                throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx)
                throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx)
                throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {}
    }
}
