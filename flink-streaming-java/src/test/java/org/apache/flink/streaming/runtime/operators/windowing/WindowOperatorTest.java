/**
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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class WindowOperatorTest {

	// For counting if close() is called the correct number of times on the SumReducer
	private static AtomicInteger closeCalled = new AtomicInteger(0);

	private void testSlidingEventTimeWindows(OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness) throws Exception {

		long initialTime = 0L;

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 3999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 3000));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 20));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1998));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1000));


		testHarness.processWatermark(new Watermark(initialTime + 999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), initialTime + 999));
		expectedOutput.add(new Watermark(999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());


		testHarness.processWatermark(new Watermark(initialTime + 1999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), initialTime + 1999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), initialTime + 1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processWatermark(new Watermark(initialTime + 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), initialTime + 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), initialTime + 2999));
		expectedOutput.add(new Watermark(2999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processWatermark(new Watermark(initialTime + 3999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), initialTime + 3999));
		expectedOutput.add(new Watermark(3999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processWatermark(new Watermark(initialTime + 4999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), initialTime + 4999));
		expectedOutput.add(new Watermark(4999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processWatermark(new Watermark(initialTime + 5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), initialTime + 5999));
		expectedOutput.add(new Watermark(5999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());


		// those don't have any effect...
		testHarness.processWatermark(new Watermark(initialTime + 6999));
		testHarness.processWatermark(new Watermark(initialTime + 7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSlidingEventTimeWindowsReduce() throws Exception {
		closeCalled.set(0);

		final int WINDOW_SIZE = 3;
		final int WINDOW_SLIDE = 1;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				SlidingTimeWindows.of(Time.of(WINDOW_SIZE, TimeUnit.SECONDS), Time.of(WINDOW_SLIDE, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>(),
				EventTimeTrigger.create());

		operator.setInputType(inputType, new ExecutionConfig());

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.configureForKeyedStream(new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.open();

		testSlidingEventTimeWindows(testHarness);

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSlidingEventTimeWindowsApply() throws Exception {
		closeCalled.set(0);

		final int WINDOW_SIZE = 3;
		final int WINDOW_SLIDE = 1;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
			SlidingTimeWindows.of(Time.of(WINDOW_SIZE, TimeUnit.SECONDS), Time.of(WINDOW_SLIDE, TimeUnit.SECONDS)),
			new TimeWindow.Serializer(),
			new TupleKeySelector(),
			BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
			stateDesc,
			new RichSumReducer<TimeWindow>(),
			EventTimeTrigger.create());

		operator.setInputType(inputType, new ExecutionConfig());

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.configureForKeyedStream(new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.open();

		testSlidingEventTimeWindows(testHarness);

		testHarness.close();

		Assert.assertEquals("Close was not called.", 1, closeCalled.get());
	}

	private void testTumblingEventTimeWindows(OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness) throws Exception {
		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 3999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 3000));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 20));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1998));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1000));


		testHarness.processWatermark(new Watermark(initialTime + 999));
		expectedOutput.add(new Watermark(999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());


		testHarness.processWatermark(new Watermark(initialTime + 1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processWatermark(new Watermark(initialTime + 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), initialTime + 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), initialTime + 2999));
		expectedOutput.add(new Watermark(2999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processWatermark(new Watermark(initialTime + 3999));
		expectedOutput.add(new Watermark(3999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processWatermark(new Watermark(initialTime + 4999));
		expectedOutput.add(new Watermark(4999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processWatermark(new Watermark(initialTime + 5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), initialTime + 5999));
		expectedOutput.add(new Watermark(5999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());


		// those don't have any effect...
		testHarness.processWatermark(new Watermark(initialTime + 6999));
		testHarness.processWatermark(new Watermark(initialTime + 7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());
	}

		@Test
	@SuppressWarnings("unchecked")
	public void testTumblingEventTimeWindowsReduce() throws Exception {
		closeCalled.set(0);

		final int WINDOW_SIZE = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new SumReducer(),
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
				TumblingTimeWindows.of(Time.of(WINDOW_SIZE, TimeUnit.SECONDS)),
				new TimeWindow.Serializer(),
				new TupleKeySelector(),
				BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
				stateDesc,
				new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>(),
				EventTimeTrigger.create());

		operator.setInputType(TypeInfoParser.<Tuple2<String, Integer>>parse("Tuple2<String, Integer>"), new ExecutionConfig());

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.configureForKeyedStream(new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.open();

		testTumblingEventTimeWindows(testHarness);

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testTumblingEventTimeWindowsApply() throws Exception {
		closeCalled.set(0);

		final int WINDOW_SIZE = 3;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ListStateDescriptor<Tuple2<String, Integer>> stateDesc = new ListStateDescriptor<>("window-contents",
			inputType.createSerializer(new ExecutionConfig()));

		WindowOperator<String, Tuple2<String, Integer>, Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
			TumblingTimeWindows.of(Time.of(WINDOW_SIZE, TimeUnit.SECONDS)),
			new TimeWindow.Serializer(),
			new TupleKeySelector(),
			BasicTypeInfo.STRING_TYPE_INFO.createSerializer(new ExecutionConfig()),
			stateDesc,
			new RichSumReducer<TimeWindow>(),
			EventTimeTrigger.create());

		operator.setInputType(TypeInfoParser.<Tuple2<String, Integer>>parse("Tuple2<String, Integer>"), new ExecutionConfig());

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.configureForKeyedStream(new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);

		testHarness.open();

		testTumblingEventTimeWindows(testHarness);

		testHarness.close();

		Assert.assertEquals("Close was not called.", 1, closeCalled.get());
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testContinuousWatermarkTrigger() throws Exception {
		closeCalled.set(0);

		final int WINDOW_SIZE = 3;

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
				new PassThroughWindowFunction<String, GlobalWindow, Tuple2<String, Integer>>(),
				ContinuousEventTimeTrigger.of(Time.of(WINDOW_SIZE, TimeUnit.SECONDS)));

		operator.setInputType(TypeInfoParser.<Tuple2<String, Integer>>parse("Tuple2<String, Integer>"), new ExecutionConfig());

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.configureForKeyedStream(new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// The global window actually ignores these timestamps...

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime));

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 3000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 3999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 20));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1998));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1000));


		testHarness.processWatermark(new Watermark(initialTime + 1000));
		expectedOutput.add(new Watermark(1000));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());


		testHarness.processWatermark(new Watermark(initialTime + 2000));
		expectedOutput.add(new Watermark(2000));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processWatermark(new Watermark(initialTime + 3000));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), Long.MAX_VALUE));
		expectedOutput.add(new Watermark(3000));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processWatermark(new Watermark(initialTime + 4000));
		expectedOutput.add(new Watermark(4000));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processWatermark(new Watermark(initialTime + 5000));
		expectedOutput.add(new Watermark(5000));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processWatermark(new Watermark(initialTime + 6000));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), Long.MAX_VALUE));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), Long.MAX_VALUE));
		expectedOutput.add(new Watermark(6000));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());


		// those don't have any effect...
		testHarness.processWatermark(new Watermark(initialTime + 7000));
		testHarness.processWatermark(new Watermark(initialTime + 8000));
		expectedOutput.add(new Watermark(7000));
		expectedOutput.add(new Watermark(8000));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.close();
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testCountTrigger() throws Exception {
		closeCalled.set(0);

		final int WINDOW_SIZE = 4;

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
				new PassThroughWindowFunction<String, GlobalWindow, Tuple2<String, Integer>>(),
				PurgingTrigger.of(CountTrigger.of(WINDOW_SIZE)));

		operator.setInputType(TypeInfoParser.<Tuple2<String, Integer>>parse(
				"Tuple2<String, Integer>"), new ExecutionConfig());

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
				new OneInputStreamOperatorTestHarness<>(operator);

		testHarness.configureForKeyedStream(new TupleKeySelector(), BasicTypeInfo.STRING_TYPE_INFO);

		long initialTime = 0L;
		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		testHarness.open();

		// The global window actually ignores these timestamps...

		// add elements out-of-order
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 3000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 3999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 20));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1998));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1000));


		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 4), Long.MAX_VALUE));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), initialTime + 10999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), initialTime + 1000));

		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 4), Long.MAX_VALUE));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 4), Long.MAX_VALUE));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new ResultSortComparator());

		testHarness.close();
	}

	// ------------------------------------------------------------------------
	//  UDFs
	// ------------------------------------------------------------------------

	public static class SumReducer implements ReduceFunction<Tuple2<String, Integer>> {
		private static final long serialVersionUID = 1L;
		@Override
		public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1,
			Tuple2<String, Integer> value2) throws Exception {
			return new Tuple2<>(value2.f0, value1.f1 + value2.f1);
		}
	}


	public static class RichSumReducer<W extends Window> extends RichWindowFunction<Iterable<Tuple2<String, Integer>>, Tuple2<String, Integer>, String, W> {
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
				Assert.fail("Open was not called");
			}
			int sum = 0;

			for (Tuple2<String, Integer> t: input) {
				sum += t.f1;
			}
			out.collect(new Tuple2<>(key, sum));

		}

	}

	@SuppressWarnings("unchecked")
	private static class ResultSortComparator implements Comparator<Object> {
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

	private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}
}
