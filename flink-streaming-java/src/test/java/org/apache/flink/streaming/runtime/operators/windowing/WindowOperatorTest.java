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
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
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
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.TestLogger;

import org.apache.flink.shaded.guava18.com.google.common.base.Joiner;
import org.apache.flink.shaded.guava18.com.google.common.collect.Iterables;

import org.junit.Assert;
import org.junit.Test;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Tests for {@link WindowOperator}.
 */
@SuppressWarnings("serial")
public class WindowOperatorTest extends TestLogger {

	// For counting if close() is called the correct number of times on the SumReducer
	private static AtomicInteger closeCalled = new AtomicInteger(0);

	// late arriving event OutputTag<StreamRecord<IN>>
	private static final OutputTag<Tuple2<String, Integer>> lateOutputTag = new OutputTag<Tuple2<String, Integer>>("late-output") {};

	private void testSlidingEventTimeWindows(OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator) throws Exception {

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

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
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(1999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 1999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));
		expectedOutput.add(new Watermark(2999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// do a snapshot, close and restore again
		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();

		expectedOutput.clear();
		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), 3999));
		expectedOutput.add(new Watermark(3999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 4999));
		expectedOutput.add(new Watermark(4999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 5999));
		expectedOutput.add(new Watermark(5999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(6999));
		testHarness.processWatermark(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSlidingEventTimeWindowsReduce() throws Exception {
		closeCalled.set(0);

		final int windowSize = 3;
		final int windowSlide = 1;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
				new SumReducer(),
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				SlidingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS), Time.of(windowSlide, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		testSlidingEventTimeWindows(operator);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSlidingEventTimeWindowsApply() throws Exception {
		closeCalled.set(0);

		final int windowSize = 3;
		final int windowSlide = 1;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				SlidingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS), Time.of(windowSlide, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new RichSumReducer<TimeWindow>()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		testSlidingEventTimeWindows(operator);

		// we close once in the rest...
		Assert.assertEquals("Close was not called.", 2, closeCalled.get());
	}

	private void testTumblingEventTimeWindows(OneInputStreamOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> operator) throws Exception {
		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

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
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// do a snapshot, close and restore again
		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());
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
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(new Watermark(3999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new Watermark(4999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 5999));
		expectedOutput.add(new Watermark(5999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(6999));
		testHarness.processWatermark(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testTumblingEventTimeWindowsReduce() throws Exception {
		closeCalled.set(0);

		final int windowSize = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
				new SumReducer(),
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		testTumblingEventTimeWindows(operator);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testTumblingEventTimeWindowsApply() throws Exception {
		closeCalled.set(0);

		final int windowSize = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new RichSumReducer<TimeWindow>()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		testTumblingEventTimeWindows(operator);

		// we close once in the rest...
		Assert.assertEquals("Close was not called.", 2, closeCalled.get());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSessionWindows() throws Exception {
		closeCalled.set(0);

		final int sessionSize = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple3<String, Long, Long>, TimeWindow> operator = new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(sessionSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new SessionWindowFunction()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
				createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3), 2500));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 1000));

		// do a snapshot, close and restore again
		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSessionWindowsWithProcessFunction() throws Exception {
		closeCalled.set(0);

		final int sessionSize = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple3<String, Long, Long>, TimeWindow> operator = new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(sessionSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableProcessWindowFunction<>(new SessionProcessWindowFunction()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
				createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3), 2500));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 1000));

		// do a snapshot, close and restore again
		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testReduceSessionWindows() throws Exception {
		closeCalled.set(0);

		final int sessionSize = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>(
				"window-contents", new SumReducer(), inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Long, Long>, TimeWindow> operator = new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(sessionSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new ReducedSessionWindowFunction()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
				createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3), 2500));

		// do a snapshot, close and restore again
		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);
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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testReduceSessionWindowsWithProcessFunction() throws Exception {
		closeCalled.set(0);

		final int sessionSize = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>(
				"window-contents", new SumReducer(), inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Long, Long>, TimeWindow> operator = new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(sessionSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueProcessWindowFunction<>(new ReducedProcessSessionWindowFunction()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
				createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 2), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 3), 2500));

		// do a snapshot, close and restore again
		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);
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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

		testHarness.close();
	}

	/**
	 * This tests whether merging works correctly with the CountTrigger.
	 * @throws Exception
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testSessionWindowsWithCountTrigger() throws Exception {
		closeCalled.set(0);

		final int sessionSize = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple3<String, Long, Long>, TimeWindow> operator = new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(sessionSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new SessionWindowFunction()),
				PurgingTrigger.of(CountTrigger.of(4)),
				0,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
				createTestHarness(operator);

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
		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();

		expectedOutput.add(new StreamRecord<>(new Tuple3<>("key2-10", 0L, 6500L), 6499));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());
		expectedOutput.clear();

		testHarness = createTestHarness(operator);
		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 2500));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 6000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 2), 6500));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 7000));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

		// add an element that merges the two "key1" sessions, they should now have count 6, and therefore fire
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 10), 4500));

		expectedOutput.add(new StreamRecord<>(new Tuple3<>("key1-22", 10L, 10000L), 9999L));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

		testHarness.close();
	}

	/**
	 * This tests whether merging works correctly with the ContinuousEventTimeTrigger.
	 * @throws Exception
	 */
	@Test
	@SuppressWarnings("unchecked")
	public void testSessionWindowsWithContinuousEventTimeTrigger() throws Exception {
		closeCalled.set(0);

		final int sessionSize = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple3<String, Long, Long>, TimeWindow> operator = new WindowOperator<>(
			EventTimeSessionWindows.withGap(Time.seconds(sessionSize)),
			new TimeWindow.Serializer(),
			new TupleKeySelector(),
			BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
			stateDesc,
			new InternalIterableWindowFunction<>(new SessionWindowFunction()),
			ContinuousEventTimeTrigger.of(Time.seconds(2)),
			0,
			null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
			createTestHarness(operator);

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
		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	/**
	 * This tests a custom Session window assigner that assigns some elements to "point windows",
	 * windows that have the same timestamp for start and end.
	 *
	 * <p> In this test, elements that have 33 as the second tuple field will be put into a point
	 * window.
	 */
	public void testPointSessions() throws Exception {
		closeCalled.set(0);

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple3<String, Long, Long>, TimeWindow> operator = new WindowOperator<>(
				new PointSessionWindows(3000),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalIterableWindowFunction<>(new SessionWindowFunction()),
				EventTimeTrigger.create(),
				0,
				null /* late data output tag */);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
		OperatorStateHandles snapshot;

		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
				createTestHarness(operator)) {
			testHarness.open();

			// add elements out-of-order
			testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 0));
			testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 33), 1000));

			// do a snapshot, close and restore again
			snapshot = testHarness.snapshot(0L, 0L);
		}

		try (OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
				createTestHarness(operator)) {
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

			TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple3ResultSortComparator());
		}
	}

	private static <OUT> OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, OUT> createTestHarness(OneInputStreamOperator<Tuple2<String, Integer>, OUT> operator) throws Exception {
		return new KeyedOneInputStreamOperatorTestHarness<>(operator, new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testContinuousWatermarkTrigger() throws Exception {
		closeCalled.set(0);

		final int windowSize = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
				new SumReducer(),
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, GlobalWindow> operator = new WindowOperator<>(
				GlobalWindows.create(),
				new GlobalWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, GlobalWindow, Tuple2<String, Integer>>()),
				ContinuousEventTimeTrigger.of(Time.of(windowSize, TimeUnit.SECONDS)),
				0,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				createTestHarness(operator);

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
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(2000));
		expectedOutput.add(new Watermark(2000));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(3000));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), Long.MAX_VALUE));
		expectedOutput.add(new Watermark(3000));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(4000));
		expectedOutput.add(new Watermark(4000));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(5000));
		expectedOutput.add(new Watermark(5000));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(6000));

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), Long.MAX_VALUE));

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), Long.MAX_VALUE));
		expectedOutput.add(new Watermark(6000));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(7000));
		testHarness.processWatermark(new Watermark(8000));
		expectedOutput.add(new Watermark(7000));
		expectedOutput.add(new Watermark(8000));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCountTrigger() throws Exception {
		closeCalled.set(0);

		final int windowSize = 4;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
				new SumReducer(),
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, GlobalWindow> operator = new WindowOperator<>(
				GlobalWindows.create(),
				new GlobalWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, GlobalWindow, Tuple2<String, Integer>>()),
				PurgingTrigger.of(CountTrigger.of(windowSize)),
				0,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				createTestHarness(operator);

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
		OperatorStateHandles snapshot = testHarness.snapshot(0L, 0L);

		testHarness.close();

		ConcurrentLinkedQueue<Object> outputBeforeClose = testHarness.getOutput();

		stateDesc = new ReducingStateDescriptor<>("window-contents",
				new SumReducer(),
				inputType.createSerializer(new ExecutionConfig()));

		operator = new WindowOperator<>(
				GlobalWindows.create(),
				new GlobalWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, GlobalWindow, Tuple2<String, Integer>>()),
				PurgingTrigger.of(CountTrigger.of(windowSize)),
				0,
				null /* late data output tag */);

		testHarness = createTestHarness(operator);

		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 4), Long.MAX_VALUE));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, Iterables.concat(outputBeforeClose, testHarness.getOutput()), new Tuple2ResultSortComparator());

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 10999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 4), Long.MAX_VALUE));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 4), Long.MAX_VALUE));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, Iterables.concat(outputBeforeClose, testHarness.getOutput()), new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	public void testProcessingTimeTumblingWindows() throws Throwable {
		final int windowSize = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
				new SumReducer(),
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				TumblingProcessingTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				ProcessingTimeTrigger.create(),
				0,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				createTestHarness(operator);

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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 7000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 7000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 7000));

		testHarness.setProcessingTime(7000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 5999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	public void testProcessingTimeSlidingWindows() throws Throwable {
		final int windowSize = 3;
		final int windowSlide = 1;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
				new SumReducer(),
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				SlidingProcessingTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS), Time.of(windowSlide, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				ProcessingTimeTrigger.create(),
				0,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// timestamp is ignored in processing time
		testHarness.setProcessingTime(3);
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), Long.MAX_VALUE));

		testHarness.setProcessingTime(1000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 1), 999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), Long.MAX_VALUE));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), Long.MAX_VALUE));

		testHarness.setProcessingTime(2000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 1999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));

		testHarness.setProcessingTime(3000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 2), 2999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), Long.MAX_VALUE));

		testHarness.setProcessingTime(7000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 3999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 5), 3999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 5), 4999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 5999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	public void testProcessingTimeSessionWindows() throws Throwable {
		final int windowGap = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
				new SumReducer(),
				inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				ProcessingTimeSessionWindows.withGap(Time.of(windowGap, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				ProcessingTimeTrigger.create(),
				0,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				createTestHarness(operator);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// timestamp is ignored in processing time
		testHarness.setProcessingTime(3);
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1)); //Long.MAX_VALUE));

		testHarness.setProcessingTime(1000);
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1002)); //Long.MAX_VALUE));

		testHarness.setProcessingTime(5000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 3999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 5000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 5000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 5000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 5000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 5000));

		testHarness.setProcessingTime(10000);

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 7999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 7999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new Tuple2ResultSortComparator());

		assertEquals(expectedOutput.size(), testHarness.getOutput().size());
		for (Object elem : testHarness.getOutput()) {
			if (elem instanceof StreamRecord) {
				StreamRecord<Tuple2<String, Integer>> el = (StreamRecord<Tuple2<String, Integer>>) elem;
				assertTrue(expectedOutput.contains(el));
			}
		}
		testHarness.close();
	}

	@Test
	public void testLateness() throws Exception {
		final int windowSize = 2;
		final long lateness = 500;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				PurgingTrigger.of(EventTimeTrigger.create()),
				lateness,
				lateOutputTag);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

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

		// this will not be sideoutput because window.maxTimestamp() + allowedLateness > currentWatermark
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1997));
		testHarness.processWatermark(new Watermark(6000));

		// this is 1 and not 3 because the trigger fires and purges
		expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1999));
		expected.add(new Watermark(6000));

		// this will be side output because window.maxTimestamp() + allowedLateness < currentWatermark
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
		testHarness.processWatermark(new Watermark(7000));

		lateExpected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 1998));
		expected.add(new Watermark(7000));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new Tuple2ResultSortComparator());

		TestHarnessUtil.assertOutputEqualsSorted(
				"SideOutput was not correct.",
				lateExpected,
				(Iterable) testHarness.getSideOutput(lateOutputTag),
				new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	public void testCleanupTimeOverflow() throws Exception {
		final int windowSize = 1000;
		final long lateness = 2000;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(windowSize));

		final WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
					windowAssigner,
					new TimeWindow.Serializer(),
					new TupleKeySelector(),
					BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
					stateDesc,
					new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
					EventTimeTrigger.create(),
					lateness,
					null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		long timestamp = Long.MAX_VALUE - 1750;
		Collection<TimeWindow> windows = windowAssigner.assignWindows(new Tuple2<>("key2", 1), timestamp, new WindowAssigner.WindowAssignerContext() {
			@Override
			public long getCurrentProcessingTime() {
				return operator.windowAssignerContext.getCurrentProcessingTime();
			}
		});
		TimeWindow window = Iterables.getOnlyElement(windows);

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), timestamp));

		// the garbage collection timer would wrap-around
		Assert.assertTrue(window.maxTimestamp() + lateness < window.maxTimestamp());

		// and it would prematurely fire with watermark (Long.MAX_VALUE - 1500)
		Assert.assertTrue(window.maxTimestamp() + lateness < Long.MAX_VALUE - 1500);

		// if we don't correctly prevent wrap-around in the garbage collection
		// timers this watermark will clean our window state for the just-added
		// element/window
		testHarness.processWatermark(new Watermark(Long.MAX_VALUE - 1500));

		// this watermark is before the end timestamp of our only window
		Assert.assertTrue(Long.MAX_VALUE - 1500 < window.maxTimestamp());
		Assert.assertTrue(window.maxTimestamp() < Long.MAX_VALUE);

		// push in a watermark that will trigger computation of our window
		testHarness.processWatermark(new Watermark(window.maxTimestamp()));

		expected.add(new Watermark(Long.MAX_VALUE - 1500));
		expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), window.maxTimestamp()));
		expected.add(new Watermark(window.maxTimestamp()));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new Tuple2ResultSortComparator());
		testHarness.close();
	}

	@Test
	public void testSideOutputDueToLatenessTumbling() throws Exception {
		final int windowSize = 2;
		final long lateness = 0;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				EventTimeTrigger.create(),
				lateness,
				lateOutputTag);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();
		ConcurrentLinkedQueue<Object> sideExpected = new ConcurrentLinkedQueue<>();

		// normal element
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processWatermark(new Watermark(1985));

		expected.add(new Watermark(1985));

		// this will not be dropped because window.maxTimestamp() + allowedLateness > currentWatermark
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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new Tuple2ResultSortComparator());
		TestHarnessUtil.assertOutputEqualsSorted("SideOutput was not correct.", sideExpected, (Iterable) testHarness.getSideOutput(
				lateOutputTag), new Tuple2ResultSortComparator());
		testHarness.close();
	}

	@Test
	public void testSideOutputDueToLatenessSliding() throws Exception {
		final int windowSize = 3;
		final int windowSlide = 1;
		final long lateness = 0;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				SlidingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS), Time.of(windowSlide, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				EventTimeTrigger.create(),
				lateness,
				lateOutputTag /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new Tuple2ResultSortComparator());
		TestHarnessUtil.assertOutputEqualsSorted("SideOutput was not correct.", sideExpected, (Iterable) testHarness.getSideOutput(lateOutputTag), new Tuple2ResultSortComparator());
		testHarness.close();
	}

	@Test
	public void testSideOutputDueToLatenessSessionZeroLatenessPurgingTrigger() throws Exception {
		final int gapSize = 3;
		final long lateness = 0;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Long, Long>, TimeWindow> operator =
			new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(gapSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new ReducedSessionWindowFunction()),
				PurgingTrigger.of(EventTimeTrigger.create()),
				lateness,
				lateOutputTag);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
			createTestHarness(operator);

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
		ConcurrentLinkedQueue<StreamRecord<Tuple2<String, Integer>>> sideActual = testHarness.getSideOutput(lateOutputTag);

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new Tuple2ResultSortComparator());
		TestHarnessUtil.assertOutputEqualsSorted("SideOutput was not correct.", sideExpected, (Iterable) sideActual, new Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	public void testSideOutputDueToLatenessSessionZeroLateness() throws Exception {
		final int gapSize = 3;
		final long lateness = 0;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Long, Long>, TimeWindow> operator =
			new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(gapSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new ReducedSessionWindowFunction()),
				EventTimeTrigger.create(),
				lateness,
				lateOutputTag);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
			createTestHarness(operator);

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
		ConcurrentLinkedQueue<StreamRecord<Tuple2<String, Integer>>> sideActual = testHarness.getSideOutput(
				lateOutputTag);
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new Tuple2ResultSortComparator());
		TestHarnessUtil.assertOutputEqualsSorted("SideOutput was not correct.", sideExpected, (Iterable) sideActual, new Tuple2ResultSortComparator());
		testHarness.close();
	}

	@Test
	public void testDropDueToLatenessSessionWithLatenessPurgingTrigger() throws Exception {

		// this has the same output as testSideOutputDueToLatenessSessionZeroLateness() because
		// the allowed lateness is too small to make a difference

		final int gapSize = 3;
		final long lateness = 10;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Long, Long>, TimeWindow> operator =
			new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(gapSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new ReducedSessionWindowFunction()),
				PurgingTrigger.of(EventTimeTrigger.create()),
				lateness,
				lateOutputTag);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
			createTestHarness(operator);

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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
		testHarness.close();
	}

	@Test
	public void testNotSideOutputDueToLatenessSessionWithLateness() throws Exception {
		// same as testSideOutputDueToLatenessSessionWithLateness() but with an accumulating trigger, i.e.
		// one that does not return FIRE_AND_PURGE when firing but just FIRE. The expected
		// results are therefore slightly different.

		final int gapSize = 3;
		final long lateness = 10;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Long, Long>, TimeWindow> operator =
			new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(gapSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new ReducedSessionWindowFunction()),
				EventTimeTrigger.create(),
				lateness,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
			createTestHarness(operator);

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
		ConcurrentLinkedQueue<StreamRecord<Tuple2<String, Integer>>> sideActual = testHarness.getSideOutput(
				lateOutputTag);

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
		assertEquals(null, sideActual);

		testHarness.processWatermark(new Watermark(20000));

		expected.add(new StreamRecord<>(new Tuple3<>("key2-3", 10000L, 17500L), 17499));
		expected.add(new Watermark(20000));

		testHarness.processWatermark(new Watermark(100000));

		expected.add(new Watermark(100000));

		actual = testHarness.getOutput();
		sideActual = testHarness.getSideOutput(lateOutputTag);
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
		assertEquals(null, sideActual);

		testHarness.close();
	}

	@Test
	public void testNotSideOutputDueToLatenessSessionWithHugeLatenessPurgingTrigger() throws Exception {

		final int gapSize = 3;
		final long lateness = 10000;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Long, Long>, TimeWindow> operator =
			new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(gapSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new ReducedSessionWindowFunction()),
				PurgingTrigger.of(EventTimeTrigger.create()),
				lateness,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
			createTestHarness(operator);

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
		ConcurrentLinkedQueue<StreamRecord<Tuple2<String, Integer>>> sideActual = testHarness.getSideOutput(lateOutputTag);
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
		assertEquals(null, sideActual);

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 14500));
		testHarness.processWatermark(new Watermark(20000));

		expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 1000L, 17500L), 17499));
		expected.add(new Watermark(20000));

		testHarness.processWatermark(new Watermark(100000));

		expected.add(new Watermark(100000));

		actual = testHarness.getOutput();
		sideActual = testHarness.getSideOutput(lateOutputTag);
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
		assertEquals(null, sideActual);

		testHarness.close();
	}

	@Test
	public void testNotSideOutputDueToLatenessSessionWithHugeLateness() throws Exception {
		final int gapSize = 3;
		final long lateness = 10000;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Long, Long>, TimeWindow> operator =
			new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(gapSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new ReducedSessionWindowFunction()),
				EventTimeTrigger.create(),
				lateness,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
			createTestHarness(operator);

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
		ConcurrentLinkedQueue<StreamRecord<Tuple2<String, Integer>>> sideActual = testHarness.getSideOutput(lateOutputTag);
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
		assertEquals(null, sideActual);

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 14500));
		testHarness.processWatermark(new Watermark(20000));

		expected.add(new StreamRecord<>(new Tuple3<>("key2-8", 1000L, 17500L), 17499));
		expected.add(new Watermark(20000));

		testHarness.processWatermark(new Watermark(100000));
		expected.add(new Watermark(100000));

		actual = testHarness.getOutput();
		sideActual = testHarness.getSideOutput(lateOutputTag);

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new Tuple3ResultSortComparator());
		assertEquals(null, sideActual);

		testHarness.close();
	}

	@Test
	public void testCleanupTimerWithEmptyListStateForTumblingWindows2() throws Exception {
		final int windowSize = 2;
		final long lateness = 100;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<Tuple2<String, Integer>> windowStateDesc =
			new ListStateDescriptor<>("window-contents", inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, String, TimeWindow> operator =
			new WindowOperator<>(
				TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new Tuple2ResultSortComparator());
		testHarness.close();
	}

	private static class PassThroughFunction2 implements WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void apply(String k, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<String> out) throws Exception {
			out.collect("GOT: " + Joiner.on(",").join(input));
		}
	}

	@Test
	public void testCleanupTimerWithEmptyListStateForTumblingWindows() throws Exception {
		final int windowSize = 2;
		final long lateness = 1;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<Tuple2<String, Integer>> windowStateDesc =
			new ListStateDescriptor<>("window-contents", inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				windowStateDesc,
				new InternalIterableWindowFunction<>(new PassThroughFunction()),
				EventTimeTrigger.create(),
				lateness,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new Tuple2ResultSortComparator());
		testHarness.close();
	}

	@Test
	public void testCleanupTimerWithEmptyReduceStateForTumblingWindows() throws Exception {
		final int windowSize = 2;
		final long lateness = 1;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				EventTimeTrigger.create(),
				lateness,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new Tuple2ResultSortComparator());
		testHarness.close();
	}

	@Test
	public void testCleanupTimerWithEmptyFoldingStateForTumblingWindows() throws Exception {
		final int windowSize = 2;
		final long lateness = 1;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		FoldingStateDescriptor<Tuple2<String, Integer>, Tuple2<String, Integer>> windowStateDesc =
			new FoldingStateDescriptor<>(
				"window-contents",
				new Tuple2<>((String) null, 0),
				new FoldFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> fold(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
						return new Tuple2<>(value.f0, accumulator.f1 + value.f1);
					}
				},
				inputType);
		windowStateDesc.initializeSerializerUnlessSet(new ExecutionConfig());

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				TumblingEventTimeWindows.of(Time.of(windowSize, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				windowStateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughFunction()),
				EventTimeTrigger.create(),
				lateness,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

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

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new Tuple2ResultSortComparator());
		testHarness.close();
	}

	@Test
	public void testCleanupTimerWithEmptyListStateForSessionWindows() throws Exception {
		final int gapSize = 3;
		final long lateness = 10;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<Tuple2<String, Integer>> windowStateDesc =
			new ListStateDescriptor<>("window-contents", inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(gapSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				windowStateDesc,
				new InternalIterableWindowFunction<>(new PassThroughFunction()),
				EventTimeTrigger.create(),
				lateness,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processWatermark(new Watermark(4998));

		expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
		expected.add(new Watermark(4998));

		testHarness.processWatermark(new Watermark(14600));
		expected.add(new Watermark(14600));

		ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new Tuple2ResultSortComparator());
		testHarness.close();
	}

	@Test
	public void testCleanupTimerWithEmptyReduceStateForSessionWindows() throws Exception {

		final int gapSize = 3;
		final long lateness = 10;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Long, Long>, TimeWindow> operator =
			new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(gapSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new ReducedSessionWindowFunction()),
				EventTimeTrigger.create(),
				lateness,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
			createTestHarness(operator);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processWatermark(new Watermark(4998));

		expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 1000L, 4000L), 3999));
		expected.add(new Watermark(4998));

		testHarness.processWatermark(new Watermark(14600));
		expected.add(new Watermark(14600));

		ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new Tuple2ResultSortComparator());
		testHarness.close();
	}

	// TODO this test seems invalid, as it uses the unsupported combination of merging windows and folding window state
	@Test
	public void testCleanupTimerWithEmptyFoldingStateForSessionWindows() throws Exception {
		final int gapSize = 3;
		final long lateness = 10;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		FoldingStateDescriptor<Tuple2<String, Integer>, Tuple2<String, Integer>> windowStateDesc =
			new FoldingStateDescriptor<>(
				"window-contents",
				new Tuple2<>((String) null, 0),
				new FoldFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
					private static final long serialVersionUID = 1L;

					@Override
					public Tuple2<String, Integer> fold(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
						return new Tuple2<>(value.f0, accumulator.f1 + value.f1);
					}
				},
				inputType);
		windowStateDesc.initializeSerializerUnlessSet(new ExecutionConfig());

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				EventTimeSessionWindows.withGap(Time.seconds(gapSize)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				windowStateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughFunction()),
				EventTimeTrigger.create(),
				lateness,
				null /* late data output tag */);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			createTestHarness(operator);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processWatermark(new Watermark(4998));

		expected.add(new StreamRecord<>(new Tuple2<>("key2", 1), 3999));
		expected.add(new Watermark(4998));

		testHarness.processWatermark(new Watermark(14600));
		expected.add(new Watermark(14600));

		ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new Tuple2ResultSortComparator());
		testHarness.close();
	}

	// ------------------------------------------------------------------------
	//  UDFs
	// ------------------------------------------------------------------------

	private static class PassThroughFunction implements WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void apply(String k, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (Tuple2<String, Integer> in: input) {
				out.collect(in);
			}
		}
	}

	private static class SumReducer implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
				Tuple2<String, Integer> value2) throws Exception {
			return new Tuple2<>(value2.f0, value1.f1 + value2.f1);
		}
	}

	private static class RichSumReducer<W extends Window> extends RichWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, W> {
		private static final long serialVersionUID = 1L;

		private boolean openCalled = false;

		@Override
		public void open(Configuration parameters) throws Exception {
			super.open(parameters);
			openCalled = true;
		}

		@Override
		public void close() throws Exception {
			super.close();
			closeCalled.incrementAndGet();
		}

		@Override
		public void apply(String key,
				W window,
				Iterable<Tuple2<String, Integer>> input,
				Collector<Tuple2<String, Integer>> out) throws Exception {

			if (!openCalled) {
				fail("Open was not called");
			}
			int sum = 0;

			for (Tuple2<String, Integer> t: input) {
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
				StreamRecord<Tuple2<String, Integer>> sr0 = (StreamRecord<Tuple2<String, Integer>>) o1;
				StreamRecord<Tuple2<String, Integer>> sr1 = (StreamRecord<Tuple2<String, Integer>>) o2;
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
				StreamRecord<Tuple3<String, Long, Long>> sr0 = (StreamRecord<Tuple3<String, Long, Long>>) o1;
				StreamRecord<Tuple3<String, Long, Long>> sr1 = (StreamRecord<Tuple3<String, Long, Long>>) o2;
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

	private static class SessionWindowFunction implements WindowFunction<Tuple2<String, Integer>, Tuple3<String, Long, Long>, String, TimeWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void apply(String key,
				TimeWindow window,
				Iterable<Tuple2<String, Integer>> values,
				Collector<Tuple3<String, Long, Long>> out) throws Exception {
			int sum = 0;
			for (Tuple2<String, Integer> i: values) {
				sum += i.f1;
			}
			String resultString = key + "-" + sum;
			out.collect(new Tuple3<>(resultString, window.getStart(), window.getEnd()));
		}
	}

	private static class ReducedSessionWindowFunction implements WindowFunction<Tuple2<String, Integer>, Tuple3<String, Long, Long>, String, TimeWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void apply(String key,
				TimeWindow window,
				Iterable<Tuple2<String, Integer>> values,
				Collector<Tuple3<String, Long, Long>> out) throws Exception {
			for (Tuple2<String, Integer> val: values) {
				out.collect(new Tuple3<>(key + "-" + val.f1, window.getStart(), window.getEnd()));
			}
		}
	}

	private static class SessionProcessWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple3<String, Long, Long>, String, TimeWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void process(String key,
				Context context,
				Iterable<Tuple2<String, Integer>> values,
				Collector<Tuple3<String, Long, Long>> out) throws Exception {
			int sum = 0;
			for (Tuple2<String, Integer> i: values) {
				sum += i.f1;
			}
			String resultString = key + "-" + sum;
			TimeWindow window = context.window();
			out.collect(new Tuple3<>(resultString, window.getStart(), window.getEnd()));
		}
	}

	private static class ReducedProcessSessionWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple3<String, Long, Long>, String, TimeWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void process(String key,
				Context context,
				Iterable<Tuple2<String, Integer>> values,
				Collector<Tuple3<String, Long, Long>> out) throws Exception {
			TimeWindow window = context.window();
			for (Tuple2<String, Integer> val: values) {
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
		public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext ctx) {
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
	 * A trigger that fires at the end of the window but does not
	 * purge the state of the fired window. This is to test the state
	 * garbage collection mechanism.
	 */
	private static class EventTimeTriggerAccumGC extends Trigger<Object, TimeWindow> {
		private static final long serialVersionUID = 1L;

		private long cleanupTime;

		public EventTimeTriggerAccumGC(long cleanupTime) {
			this.cleanupTime = cleanupTime;
		}

		@Override
		public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
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
			return time == window.maxTimestamp() || time == window.maxTimestamp() + cleanupTime ?
				TriggerResult.FIRE_AND_PURGE :
				TriggerResult.CONTINUE;
		}

		@Override
		public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
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
}
