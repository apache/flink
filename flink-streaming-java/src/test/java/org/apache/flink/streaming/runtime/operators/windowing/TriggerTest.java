/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.TypeInfoParser;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.api.functions.windowing.PassThroughWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.triggerdsl.All;
import org.apache.flink.streaming.api.windowing.triggers.triggerdsl.Any;
import org.apache.flink.streaming.api.windowing.triggers.triggerdsl.Count;
import org.apache.flink.streaming.api.windowing.triggers.triggerdsl.DslTrigger;
import org.apache.flink.streaming.api.windowing.triggers.triggerdsl.DslTriggerRunner;
import org.apache.flink.streaming.api.windowing.triggers.triggerdsl.EventTime;
import org.apache.flink.streaming.api.windowing.triggers.triggerdsl.ProcessingTime;
import org.apache.flink.streaming.api.windowing.triggers.triggerdsl.Repeat;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalSingleValueWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.WindowingTestHarness;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public class TriggerTest {

	private <W extends Window> WindowingTestHarness<String, Tuple2<String, Integer>, W> createTestHarness(
		WindowAssigner<Object, W> assigner, DslTrigger<Object, W> trigger, long lateness) {

		ExecutionConfig config = new ExecutionConfig();
		KeySelector<Tuple2<String, Integer>, String> keySelector = new TupleKeySelector();
		TypeInformation<String> keyType = BasicTypeInfo.STRING_TYPE_INFO;
		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		DslTriggerRunner<Object, W> runner = new DslTriggerRunner<>(trigger);
		runner.createTriggerTree(assigner.getWindowSerializer(config), lateness);

		return new WindowingTestHarness<>(config, assigner, keySelector, keyType, inputType, runner, lateness);
	}

	@Test
	public void testEventTimeTriggerTumbling() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 100;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness =
			createTestHarness(windowAssigner, EventTime.<TimeWindow>afterEndOfWindow().accumulating(), ALLOWED_LATENESS);

		testHarness.open();

		testHarness.processWatermark(1000);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(2000);								// +6 on_time

		// added as late
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);		// no firing as no late trigger is specified,
																		// and this is not the default trigger

		testHarness.processWatermark(2200);								// cleanup

		// dropped
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		Assert.assertTrue(testHarness.getOutput().size() == 6 + 3);		// +3 for the watermarks

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerAfterFirstElementTumbling() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 200;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(EventTime.<TimeWindow>afterFirstElement(Time.milliseconds(100))).accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();

		testHarness.processWatermark(1000);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.processWatermark(1100);								// +3

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 2);		// +2 for the watermark

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(1200);								// +4

		Assert.assertTrue(testHarness.getOutput().size() == 7 + 3);		// +3 for the watermark

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.processWatermark(2000);								// +6

		Assert.assertTrue(testHarness.getOutput().size() == 13 + 4);	// +4 for the watermark

		// added as late
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		Assert.assertTrue(testHarness.getOutput().size() == 13 + 4);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(2100);								// +7

		Assert.assertTrue(testHarness.getOutput().size() == 20 + 5);

		// the following element is late but will be added,
		// but the timer it will set will be after the cleanup (2200),
		// so it will not fire.
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(2201);

		Assert.assertTrue(testHarness.getOutput().size() == 20 + 6); // +6 for the watermark

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerAfterFirstElementGreaterThanWindowTumbling() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 200;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(EventTime.<TimeWindow>afterFirstElement(Time.milliseconds(4000))).accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();

		testHarness.processWatermark(1000);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);		// register timer for 5000
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(2000);								// nothing to fire

		Assert.assertTrue(testHarness.getOutput().size() == 2);			// +2 for the watermarks

		// dropped as late
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.processWatermark(5000);								// no firing as we are after the cleanup

		Assert.assertTrue(testHarness.getOutput().size() == 3);			// +1 for the watermark

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testProcessingTimeTriggerTumbling() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 100;

		TumblingProcessingTimeWindows windowAssigner = TumblingProcessingTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			ProcessingTime.<TimeWindow>afterEndOfWindow().accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();
		testHarness.setProcessingTime(0);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(2000);								// +6 on_time

		Assert.assertTrue(testHarness.getOutput().size() == 6);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testProcessingTimeTriggerAfterFirstElementTumbling() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 100;

		TumblingProcessingTimeWindows windowAssigner = TumblingProcessingTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(100))).accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();
		testHarness.setProcessingTime(0);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(100);								//+3

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(210);								//+4

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(2000);							//+6

		Assert.assertTrue(testHarness.getOutput().size() == 13);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testProcessingTimeTriggerWithEarlyTumbling() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 100;

		TumblingProcessingTimeWindows windowAssigner = TumblingProcessingTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		DslTrigger<Object, TimeWindow> trigger = ProcessingTime.<TimeWindow>afterEndOfWindow()
			.withEarlyTrigger(Repeat.Forever(ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(10))))
			.accumulating();

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			trigger,
			ALLOWED_LATENESS);

		testHarness.open();
		testHarness.setProcessingTime(0);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(10);								// +2 early firing

		testHarness.setProcessingTime(12);

		Assert.assertTrue(testHarness.getOutput().size() == 2);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(22);								// +5 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 2 + 5);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(2000);							// +12 = +6 for on_time
																		//       +6 for early trigger from previous element

		Assert.assertTrue(testHarness.getOutput().size() == 2 + 5 + 6 + 6);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testProcessingTimeTriggerWithEarlyTumbling2() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 100;

		TumblingProcessingTimeWindows windowAssigner = TumblingProcessingTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			ProcessingTime.<TimeWindow>afterEndOfWindow()
				.withEarlyTrigger(Repeat.Forever(Count.<TimeWindow>atLeast(2))),
			ALLOWED_LATENESS);

		testHarness.open();
		testHarness.setProcessingTime(0);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);		// +2 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 2);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);		// +4 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 2 + 4);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(20);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);		// +6 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 2 + 4 + 6);

		testHarness.setProcessingTime(2000);							// +6 for on_time

		Assert.assertTrue(testHarness.getOutput().size() == 2 + 4 + 6 + 6);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testProcessingTimeTriggerWithEarlyTumblingOnce() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 100;

		TumblingProcessingTimeWindows windowAssigner = TumblingProcessingTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			ProcessingTime.<TimeWindow>afterEndOfWindow()
				.withEarlyTrigger(Count.<TimeWindow>atLeast(2)),
			ALLOWED_LATENESS);

		testHarness.open();
		testHarness.setProcessingTime(0);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);		// +2 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 2);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);		// no firing (ONCE)

		Assert.assertTrue(testHarness.getOutput().size() == 2);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(20);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);		// no firing (ONCE)

		Assert.assertTrue(testHarness.getOutput().size() == 2);

		testHarness.setProcessingTime(2000);							// +6 for on_time

		Assert.assertTrue(testHarness.getOutput().size() == 2 + 6);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testProcessingTimeTriggerAfterFirstElementGreaterThanWindowTumbling() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 100;

		TumblingProcessingTimeWindows windowAssigner = TumblingProcessingTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(4000))).accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();
		testHarness.setProcessingTime(0);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);				// will register timer for 4000

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(4000);									// here it should fire but window is cleaned up

		Assert.assertTrue(testHarness.getOutput().size() == 0);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithEarlyAndLateTumbling() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 300;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			EventTime.<TimeWindow>afterEndOfWindow()
				.withEarlyTrigger(Repeat.Forever(ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(100))))
				.withLateTrigger(ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(200)))
				.accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();

		testHarness.processWatermark(1000);
		testHarness.setProcessingTime(100);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(200);									// +3 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);			// +1 for the watermark

		testHarness.setProcessingTime(300);

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);			// no data, no firing

		testHarness.processElement(new Tuple2<>("key1", 1), 1500);
		testHarness.processElement(new Tuple2<>("key1", 1), 1500);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(400);									// +5 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 5 + 1);

		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.processWatermark(1999);									// +6 on-time firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 5 + 6 + 2);	// +2 for watermarks

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		// after the recovery, the watermark has been set back to Long.MIN_VALUE
		// so this also checks that the hasFiredOnTime flag does its job

		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);

		testHarness.setProcessingTime(500);									// no firing as we are in the late period
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 5 + 6 + 2);

		testHarness.setProcessingTime(600);									// +10 late firing
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 5 + 6 + 10 + 2);

		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.setProcessingTime(800);									// no late firing because it is repeat.Once

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 5 + 6 + 10 + 2);

		testHarness.processWatermark(2299);									// this is cleanup

		// dropped due to lateness
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 5 + 6 + 10 + 3);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithEarlyAndLateTumblingDiscarding() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 300;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			EventTime.<TimeWindow>afterEndOfWindow()
				.withEarlyTrigger(Repeat.Forever(ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(100))))
				.withLateTrigger(ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(200)))
				.discarding(),
			ALLOWED_LATENESS);

		testHarness.open();
		testHarness.processWatermark(1000);
		testHarness.setProcessingTime(100);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(10L, 10L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(200);									// +3 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);			// +1 for the watermark

		testHarness.setProcessingTime(300);

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);			// no data, no firings

		testHarness.processElement(new Tuple2<>("key1", 1), 1500);
		testHarness.processElement(new Tuple2<>("key1", 1), 1500);
		testHarness.setProcessingTime(400);									// +2 early firing FOREVER

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 2 + 1);

		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.processWatermark(2100);									// +1 on-time firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 2 + 1 + 2);	// +1 for the watermark

		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);

		testHarness.setProcessingTime(500);									// no firing as we are in the late period
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 2 + 1 + 2);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(10L, 11L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(600);									// +4 late firing
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 2 + 1 + 4 + 2);

		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.setProcessingTime(800);									// no late firing because we are at ONCE

		testHarness.processWatermark(2299); // this is cleanup

		// dropped due to lateness
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 2 + 1 + 4 + 3);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithEarlyOnlyTumbling() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 300;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			EventTime.<TimeWindow>afterEndOfWindow()
				.withEarlyTrigger(Repeat.Forever(ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(100))))
				.accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();
		testHarness.processWatermark(1000);
		testHarness.setProcessingTime(100);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		testHarness.setProcessingTime(200);										// +3 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);				// +1 for the watermark

		testHarness.setProcessingTime(300);

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);				// no data, no firing

		testHarness.processElement(new Tuple2<>("key1", 1), 1500);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1500);
		testHarness.setProcessingTime(400);										// +5 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 5 + 1);

		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.processWatermark(2100);										// + 6 on-time firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 5 + 6 + 2);		// +1 for the watermark

		testHarness.processElement(new Tuple2<>("key1", 1), 1900);				// no firing at every late element

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1900);				// no firing at every late element

		testHarness.setProcessingTime(500);										// no firing because we have no late trigger
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 5 + 6 + 2);

		testHarness.setProcessingTime(600);										// no firing because we have no late trigger
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 5 + 6 + 2);

		testHarness.processWatermark(2299);										// this is cleanup

		// dropped due to lateness
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 5 + 6 + 3);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithLateOnlyTumbling() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 300;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			EventTime.<TimeWindow>afterEndOfWindow()
				.withLateTrigger(ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(200)))
				.accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();
		testHarness.processWatermark(1000);
		testHarness.setProcessingTime(100);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(200);                            // no early trigger, no firing

		Assert.assertTrue(testHarness.getOutput().size() == 1);        // +1 for the watermark

		testHarness.setProcessingTime(300);

		Assert.assertTrue(testHarness.getOutput().size() == 1);        // no firing

		testHarness.processElement(new Tuple2<>("key1", 1), 1900);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(2100);                            // +4 on-time firing

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 2);    // +2 for the watermarks

		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);

		// do a snapshot, close and restore
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(500);                            // + 8 late firing
		Assert.assertTrue(testHarness.getOutput().size() == 4 + 8 + 2);

		testHarness.setProcessingTime(600);                            // no firing (ONCE)
		Assert.assertTrue(testHarness.getOutput().size() == 4 + 8 + 2);

		testHarness.processWatermark(2299);								// this is cleanup

		// dropped due to lateness
		testHarness.processElement(new Tuple2<>("key1", 1), 1900);

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 8 + 3);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testAnyOfTriggerWithTumblingWindows() throws Exception {
		final int WINDOW_SIZE = 2000;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		// this also tests that the accumulating/discarding mode of the parent overrides that of the child

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(Any.of(Count.<TimeWindow>atLeast(4), Count.<TimeWindow>atLeast(3)).discarding()).accumulating(),
			0);

		testHarness.open();

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		// normal element
		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);	// +3 count_3 fires

		Assert.assertTrue(testHarness.getOutput().size() == 3);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);	// no firing because both counters are reset

		Assert.assertTrue(testHarness.getOutput().size() == 3);

		testHarness.processElement(new Tuple2<>("key2", 1), 1004);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	// +6 count_3 fires

		Assert.assertTrue(testHarness.getOutput().size() == 9);

		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	//
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	// no firing
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	// +9 count_3 fires

		testHarness.processWatermark(1999);

		Assert.assertTrue(testHarness.getOutput().size() == 18 + 1);// +1 for the watermark

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testAnyOfTriggerWithTumblingWindows2() throws Exception {
		final int WINDOW_SIZE = 2000;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(Any.of(Count.<TimeWindow>atLeast(4), ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(100)))).accumulating(),
			0);

		testHarness.open();

		testHarness.setProcessingTime(10);

		// normal element
		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);

		testHarness.setProcessingTime(110);							// +2 processing time
		Assert.assertTrue(testHarness.getOutput().size() == 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 1001);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);	// no firing because both states are cleaned at previous firing
																	// implicitly all triggers are "after previous firing"

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1002);
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);	// +6 count_4 fires (accumulating)

		Assert.assertTrue(testHarness.getOutput().size() == 2 + 6);

		testHarness.setProcessingTime(250);							// no firing, state has been cleared

		Assert.assertTrue(testHarness.getOutput().size() == 2 + 6);

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(350);							// +9, processing time

		Assert.assertTrue(testHarness.getOutput().size() == 2 + 6 + 9);

		testHarness.processWatermark(2000);							// the cleanup timer

		// check the we have processed all the timers
		// (we do not have separate cleanup timer because lateness=0)
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testAnyOfTriggerWithTumblingWindows3() throws Exception {
		final int WINDOW_SIZE = 2000;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(Any.of(Count.<TimeWindow>atLeast(4), EventTime.<TimeWindow>afterEndOfWindow())).accumulating(),
			0);

		testHarness.open();

		// normal element
		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1002);		// +4 count_4 fires
		testHarness.processElement(new Tuple2<>("key2", 1), 1003);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(1999);								// +6 on-time firing

		Assert.assertTrue(testHarness.getOutput().size() == 10 + 1);	// +1 for the watermark

		// check the we have processed all the timers
		// (we do not have separate cleanup timer because lateness=0)
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testAllOfTriggerWithTumblingWindows() throws Exception {
		final int WINDOW_SIZE = 2000;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(All.of(Count.<TimeWindow>atLeast(4), Count.<TimeWindow>atLeast(2))).accumulating(),
			0);

		testHarness.open();

		// normal element
		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);		// +0 count_2 fires but ignored
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);		// +4 both fire here

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1004);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);		// +0 count_2 fires but ignored

		Assert.assertTrue(testHarness.getOutput().size() == 4);			// +1 for the watermark

		testHarness.processWatermark(1999);								// this will lead to processing the cleanup timer

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 1);		// +1 for the watermark

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testAllOfTriggerWithTumblingWindowsDiscarding() throws Exception {
		final int WINDOW_SIZE = 2000;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(All.of(Count.<TimeWindow>atLeast(3), Count.<TimeWindow>atLeast(2))).discarding(),
			0);

		testHarness.open();

		// normal element
		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);		// +0 count_2 fires but ignored
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);		// +3 all fire here

		Assert.assertTrue(testHarness.getOutput().size() == 3);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1004);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);		// +3 all fire here

		Assert.assertTrue(testHarness.getOutput().size() == 6);			// +1 for the watermark

		testHarness.processWatermark(1999);								// this will lead to processing the cleanup timer

		Assert.assertTrue(testHarness.getOutput().size() == 6 + 1);		// +1 for the watermark

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testAllOfTriggerWithTumblingWindows2() throws Exception {
		final int WINDOW_SIZE = 2000;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(All.of(Count.<TimeWindow>atLeast(4), EventTime.<TimeWindow>afterEndOfWindow())).accumulating(),
			0);

		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);		// the 4 fires here but is ignored
		testHarness.processElement(new Tuple2<>("key2", 1), 1004);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1006);
		testHarness.processElement(new Tuple2<>("key2", 1), 1007);

		testHarness.processWatermark(1999);								// +8 on-time firing with >4 elements

		Assert.assertTrue(testHarness.getOutput().size() == 8 + 1);		// +1 for the watermark

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testAllOfTriggerWithTumblingWindowsOnce() throws Exception {
		final int WINDOW_SIZE = 2000;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			All.of(Count.<TimeWindow>atLeast(4), Count.<TimeWindow>atLeast(5)).accumulating(),
			0);

		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);		// the 4 fires here but is ignored
		testHarness.processElement(new Tuple2<>("key2", 1), 1004);		// both fire here

		Assert.assertTrue(testHarness.getOutput().size() == 5);

		testHarness.processElement(new Tuple2<>("key2", 1), 1005);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1006);
		testHarness.processElement(new Tuple2<>("key2", 1), 1007);
		testHarness.processElement(new Tuple2<>("key2", 1), 1007);
		testHarness.processElement(new Tuple2<>("key2", 1), 1007);		// both would fire here but we are at ONCE

		Assert.assertTrue(testHarness.getOutput().size() == 5);

		testHarness.processWatermark(1999);								// cleanup

		Assert.assertTrue(testHarness.getOutput().size() == 5 + 1);		// +1 for the watermark

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithEarlyAndLateTumbling2() throws Exception {
		final int WINDOW_SIZE = 2000;
		final long allowedLateness = 100;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			EventTime.<TimeWindow>afterEndOfWindow()
				.withEarlyTrigger(Count.<TimeWindow>atLeast(4))
				.withLateTrigger(Count.<TimeWindow>atLeast(3))
				.accumulating(),
			allowedLateness);

		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 800);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1001);

		testHarness.processElement(new Tuple2<>("key1", 1), 1002);
		testHarness.processElement(new Tuple2<>("key1", 1), 1002);
		testHarness.processElement(new Tuple2<>("key1", 1), 1002);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1003);	// +4 early for key1
		Assert.assertTrue(testHarness.getOutput().size() == 4);

		testHarness.processElement(new Tuple2<>("key2", 1), 1004);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	// +4 early firing for key2

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 4);

		testHarness.processElement(new Tuple2<>("key2", 1), 1006);
		testHarness.processElement(new Tuple2<>("key2", 1), 1007);

		testHarness.processWatermark(1999);							// +10 on-time firing

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 4 + 10 + 1);		// +1 for the watermark

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		// late but accepted elements
		testHarness.processElement(new Tuple2<>("key2", 1), 1900);
		testHarness.processElement(new Tuple2<>("key2", 1), 1901);
		testHarness.processElement(new Tuple2<>("key2", 1), 1902);	// +9 late firing for key2

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 4 + 10 + 9 + 1);	// +1 for the watermark

		testHarness.processElement(new Tuple2<>("key1", 1), 1903);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1904);
		testHarness.processElement(new Tuple2<>("key1", 1), 1905);	// +7 late firing for key1

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 4 + 10 + 9 + 7 + 1);    // +1 for the watermark

		testHarness.processElement(new Tuple2<>("key1", 1), 1904);
		testHarness.processElement(new Tuple2<>("key1", 1), 1905);
		testHarness.processElement(new Tuple2<>("key1", 1), 1904);	// no more late firings

		testHarness.processElement(new Tuple2<>("key2", 1), 1906);

		testHarness.processWatermark(2200);

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 4 + 10 + 9 + 7 + 2); // +2 for the watermark

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithEarlyOnlyTumbling2() throws Exception {
		final int WINDOW_SIZE = 2000;
		final long allowedLateness = 100;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(
				EventTime.<TimeWindow>afterEndOfWindow()
					.withEarlyTrigger(Count.<TimeWindow>atLeast(4)))
				.accumulating(),
			allowedLateness);

		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 800);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);		// + 4 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 4);

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);
		testHarness.processElement(new Tuple2<>("key2", 1), 1003);
		testHarness.processElement(new Tuple2<>("key2", 1), 1003);
		testHarness.processElement(new Tuple2<>("key2", 1), 1003);		// + 8 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 8);

		testHarness.processElement(new Tuple2<>("key2", 1), 1004);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(1999);								// +10 on-time

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 8 + 10 + 1);

		// late but accepted elements
		testHarness.processElement(new Tuple2<>("key2", 1), 1900);		// no firing, no late trigger
		testHarness.processElement(new Tuple2<>("key2", 1), 1900);		// no firing, no late trigger

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 8 + 10 + 1);

		testHarness.processElement(new Tuple2<>("key1", 1), 2150);
		testHarness.processWatermark(10999);                        // fire just the new window

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 8 + 10 + 1 + 2);

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithLateOnlyTumbling2() throws Exception {
		final int WINDOW_SIZE = 2000;
		final long allowedLateness = 100;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			EventTime.<TimeWindow>afterEndOfWindow()
				.withLateTrigger(Count.<TimeWindow>atLeast(4)),
			allowedLateness);

		testHarness.open();

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 800);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);
		testHarness.processElement(new Tuple2<>("key2", 1), 1003);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1004);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);

		testHarness.processWatermark(1999);							// +6 on-time

		Assert.assertTrue(testHarness.getOutput().size() == 6 + 1);	// +1 for the watermark

		// late but accepted elements
		testHarness.processElement(new Tuple2<>("key2", 1), 1900);
		testHarness.processElement(new Tuple2<>("key2", 1), 1901);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1902);
		testHarness.processElement(new Tuple2<>("key2", 1), 1903);	// +10 late firing

		testHarness.processElement(new Tuple2<>("key2", 1), 1902);
		testHarness.processElement(new Tuple2<>("key2", 1), 1903);
		testHarness.processElement(new Tuple2<>("key2", 1), 1902);
		testHarness.processElement(new Tuple2<>("key2", 1), 1903);	// no more late firings

		testHarness.processWatermark(2200);

		Assert.assertTrue(testHarness.getOutput().size() == 16 + 2); // +2 for the watermarks

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	// SESSION WINDOW TESTING

	@Test
	public void testEventTimeTriggerWithEarlyAndLateSessionMerging() throws Exception {
		final int GAP_SIZE = 3;
		final long allowedLateness = 5000; // greater than the gap

		EventTimeSessionWindows windowAssigner = EventTimeSessionWindows.withGap(Time.seconds(GAP_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(
				EventTime.<TimeWindow>afterEndOfWindow()
					.withEarlyTrigger(Count.<TimeWindow>atLeast(3))
					.withLateTrigger(Count.<TimeWindow>atLeast(5))
			).accumulating(),
			allowedLateness);

		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 2000);
		testHarness.processWatermark(4998);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 4500);		// + 3 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);		// +1 for the watermark

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(7500);								// +3 onTime firing, with no new data

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 3000);		// late counter for old session is 1

																		// new session
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);		// early of new session is 2

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();
																		// MERGE the two sessions into one
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);		// here we must have a firing of the
																		// early trigger of the merged window
																		// because the new early count is 4		+7

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 7 + 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 9000);
		testHarness.processElement(new Tuple2<>("key2", 1), 9000);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 9000);		// early firing							+10

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 7 + 10 + 2);

		testHarness.processWatermark(15000);							// +10 onTime of new session, with no new data

		testHarness.processWatermark(20000);							// also process the last cleanup timer

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 7 + 10 + 10 + 4);

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithEarlyAndLateSessionMergingOnce() throws Exception {
		final int GAP_SIZE = 3;
		final long allowedLateness = 5000; // greater than the gap

		EventTimeSessionWindows windowAssigner = EventTimeSessionWindows.withGap(Time.seconds(GAP_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			EventTime.<TimeWindow>afterEndOfWindow()
				.withEarlyTrigger(Count.<TimeWindow>atLeast(3))
				.withLateTrigger(Count.<TimeWindow>atLeast(5)),
			allowedLateness);

		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 2000);
		testHarness.processWatermark(4998);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 4500);		// + 3 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);		// +1 for the watermark

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(7500);								// +3 onTime firing, with no new data

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 3000);
		testHarness.processElement(new Tuple2<>("key2", 1), 3000);
		testHarness.processElement(new Tuple2<>("key2", 1), 3000);
		testHarness.processElement(new Tuple2<>("key2", 1), 3000);
		testHarness.processElement(new Tuple2<>("key2", 1), 3000);		// + 8 late firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 8 + 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 3000);		// late of old session is 1

		// new session
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);		// early of new session is 2

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		// MERGE the two sessions into one
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);		// here we must have a firing of the
		// early trigger of the merged window
		// because the new early count is 4		+7

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 8 + 12 + 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 9000);
		testHarness.processElement(new Tuple2<>("key2", 1), 9000);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 9000);		// early firing							+15

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 8 + 12 + 15 + 2);

		testHarness.processWatermark(15000);							// +15 onTime of new session, with no new data

		testHarness.processWatermark(20000);							// also process the last cleanup timer

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 8 + 12 + 15 + 15 + 4);

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithEarlyAndLateSessionMerging2() throws Exception {
		final int GAP_SIZE = 3;
		final long allowedLateness = 10000; // greater than the gap

		EventTimeSessionWindows windowAssigner = EventTimeSessionWindows.withGap(Time.seconds(GAP_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(
				EventTime.<TimeWindow>afterEndOfWindow()
					.withEarlyTrigger(Count.<TimeWindow>atLeast(3))
					.withLateTrigger(Count.<TimeWindow>atLeast(5))
			).accumulating(),
			allowedLateness);

		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 2000);
		testHarness.processWatermark(4998);

		testHarness.processElement(new Tuple2<>("key2", 1), 4500);		// +3 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);

		testHarness.processWatermark(7500);								// +3 early firing with no new element

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 3000);		// late of old session is 1

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2);

		// new session
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);		// early of new session is 2

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(11500);							// + 2 onTime firing for new session

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 3);

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);		// late of new session is 1
		testHarness.processElement(new Tuple2<>("key2", 1), 9500);		// early of expanded new session is 2

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 9500);		// + 5 early of new session is 3
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 3 + 5);

		// this will merge the two sessions into one
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);		// the new early count is 2 (one was the late in the old session)

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);		// +11  early firing of new merged session

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 3 + 5 + 11);

		testHarness.processWatermark(12500);							// +11 onTime of new session, with no new data

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 3 + 5 + 11 + 11 + 1);

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);		// +16 late firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 3 + 5 + 11 + 11 + 16 + 1);

		testHarness.processWatermark(25000);

		//nothing happens at cleanup, we just add the last watermark
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 3 + 5 + 11 + 11 + 16 + 1 + 1);

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithEarlyOnlySessionMerging() throws Exception {
		final int GAP_SIZE = 3;
		final long allowedLateness = 10000; // greater than the gap

		EventTimeSessionWindows windowAssigner = EventTimeSessionWindows.withGap(Time.seconds(GAP_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			EventTime.<TimeWindow>afterEndOfWindow()
				.withEarlyTrigger(Count.<TimeWindow>atLeast(3)),
			allowedLateness);

		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 2000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(4998);

		testHarness.processElement(new Tuple2<>("key2", 1), 4500);		// +3 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);

		testHarness.processWatermark(7500);								// +3 onTime firing with no new element

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 3000);		// no firing for late elements	old_counter = 1

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2);

		// new session
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);		// early of new session is 2
		testHarness.processWatermark(11500);							// +2 onTime firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 3);

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);		// no late firing here			new_counter = 1
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 3);

		testHarness.processElement(new Tuple2<>("key2", 1), 9500);		//								new_counter = 2
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 3);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 9500);		// +5							new_counter = 0
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 5 + 3);

		testHarness.processElement(new Tuple2<>("key2", 1), 9500);		//								new_counter = 1

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 5 + 3);

		// this will merge the two sessions into one
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);		// + 11 early of merged

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 5 + 11 + 3);

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);		//									count = 1
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);		//									count = 2
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);		// no fire (ONCE)					count = 3

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 5 + 11 + 3);

		testHarness.processWatermark(12500);							// +14 on-time firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 5 + 11 + 14 + 4);

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);		//	no late firing					count = 1

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 5 + 11 + 14 + 4);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);	//	no late firing						count = 2

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 5 + 11 + 14 + 4);

		testHarness.processWatermark(25000);

		//nothing happens at cleanup, we just add the last watermark
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 5 + 11 + 14 + 5);

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithEarlyAndLateProcessingSessionMerging() throws Exception {
		final int GAP_SIZE = 3;
		final long allowedLateness = 10000; // greater than the gap

		EventTimeSessionWindows windowAssigner = EventTimeSessionWindows.withGap(Time.seconds(GAP_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(
				EventTime.<TimeWindow>afterEndOfWindow()
					.withEarlyTrigger(ProcessingTime.<TimeWindow>afterFirstElement(Time.seconds(1)))
					.withLateTrigger(ProcessingTime.<TimeWindow>afterFirstElement(Time.seconds(2)))),
			allowedLateness);

		testHarness.open();

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(0);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 2000);
		testHarness.processWatermark(4998);
		testHarness.processElement(new Tuple2<>("key2", 1), 4500);

		testHarness.setProcessingTime(1000);								// +3 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);

		testHarness.setProcessingTime(2000);								// no firing, no data

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);

		testHarness.processWatermark(7500);									// no firing, no data

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 3000);			// late of old session registers timer for 3000

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2);

		testHarness.setProcessingTime(2500);

		// new session
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);			// early of new session registers timer for 3500

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);

		// this will merge the two sessions into one
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);			// the new early timer has to be 3000

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(3000);								// +7 early firing of the merged session

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 7);

		testHarness.setProcessingTime(3700);								// the 3500 timer should be ignored because its window was merged

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 7);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(12500);								// +7 firing with no new element

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 7 + 7 + 1);

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);			// late registers timer for 5700
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(5800);

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 7 + 7 + 12 + 1);

		testHarness.processWatermark(25000);

		//nothing happens at cleanup, we just add the last watermark
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2 + 7 + 7 + 12 + 2);

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithEarlyAndLateMixSessionMerging() throws Exception {
		final int GAP_SIZE = 3;
		final long allowedLateness = 10000; // greater than the gap

		EventTimeSessionWindows windowAssigner = EventTimeSessionWindows.withGap(Time.seconds(GAP_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(
				EventTime.<TimeWindow>afterEndOfWindow()
					.withEarlyTrigger(ProcessingTime.<TimeWindow>afterFirstElement(Time.seconds(1)))
					.withLateTrigger(Count.<TimeWindow>atLeast(3))
			).accumulating(),
			allowedLateness);

		testHarness.open();

		testHarness.setProcessingTime(0);

		testHarness.processElement(new Tuple2<>("key2", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 2000);
		testHarness.processWatermark(4998);
		testHarness.processElement(new Tuple2<>("key2", 1), 4500);

		testHarness.setProcessingTime(1000);								// +3 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);

		testHarness.setProcessingTime(2000);								// no firing, no data

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);

		testHarness.processElement(new Tuple2<>("key2", 1), 4500);			// registers timer for 3000

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(7500);									// +4 onTime firing

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 4 + 2);

		testHarness.setProcessingTime(3000);								// this should not fire because we had an on-time firing
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 4 + 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 3000);			// late of old session is 1

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 3000);			// late of old session is 2
		testHarness.processElement(new Tuple2<>("key2", 1), 3000);			// +7 late firing of old session

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 4 + 7 + 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 3000);			// late of old session is 1 and register timer for 4000

		testHarness.setProcessingTime(5000);								// nothing here as we are in the late phase

		testHarness.processElement(new Tuple2<>("key2", 1), 3000);			// late of old session is 2 and register timer for 6000

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 4 + 7 + 2);

		testHarness.setProcessingTime(5500);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		// new session
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);			// early of new session registers timer for 6500
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);

		// this will merge the two sessions into one
		testHarness.processElement(new Tuple2<>("key2", 1), 7000);			// this should keep the 6000 timer after merging

		testHarness.setProcessingTime(6000);								// +12 early firing of merged session

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 4 + 7 + 12 + 2);

		testHarness.setProcessingTime(6700);								// nothing should happen here

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 4 + 7 + 12 + 2);

		testHarness.processWatermark(12500);								// +12 on time firing with no new element

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 4 + 7 + 12 + 12 + 3);

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);			// +15 late firing

		testHarness.setProcessingTime(10000);

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 4 + 7 + 12 + 12 + 15 + 3);

		testHarness.processWatermark(25000);

		//nothing happens at cleanup, we just add the last watermark
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 4 + 7 + 12 + 12 + 15 + 4);

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testOnceTriggerInSessions() throws Exception {
		final int GAP_SIZE = 3;
		final long allowedLateness = 10000; // greater than the gap

		EventTimeSessionWindows windowAssigner = EventTimeSessionWindows.withGap(Time.seconds(GAP_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner, 
			Count.<TimeWindow>atLeast(3),
			allowedLateness);

		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(1, 0)));	// cleanup for 13999

		testHarness.processElement(new Tuple2<>("key2", 1), 2000);
		testHarness.processWatermark(4998);
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(2, 0)));	// cleanup for 14999

		testHarness.processElement(new Tuple2<>("key2", 1), 4500);			// +3 early firing
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(3, 0)));	// cleanup for 17499

		testHarness.processElement(new Tuple2<>("key2", 1), 4500);
		testHarness.processElement(new Tuple2<>("key2", 1), 4500);
		testHarness.processElement(new Tuple2<>("key2", 1), 4500);			// no firing - we are in ONCE mode
		testHarness.processElement(new Tuple2<>("key2", 1), 4500);

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(3, 0)));	// the cleanup is already there so no re-adding

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);

		testHarness.processWatermark(7500);									// nothing because we only have a count trigger
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 2);

		// new session

		testHarness.processElement(new Tuple2<>("key2", 1), 7501);										// new session with one element
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(4, 0)));	// cleanup for 20500

		testHarness.processElement(new Tuple2<>("key2", 1), 8000);
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(5, 0)));	// cleanup for 20999

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 8000);			// +3 for new session
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(6, 0)));	// cleanup for 21499

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);			// +6 because these elements expand the window

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 6 + 2);	// no firing

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 6 + 2);	// no firing because we are in ONCE mode

		// merging the two sessions

		testHarness.processElement(new Tuple2<>("key2", 1), 6500);
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(7, 0)));	// cleanup for 21499 but for the whole window 1000 to 11500, so reinserted

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 6 + 17 + 2);

		testHarness.processWatermark(21500);								// to process all the cleanup timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testOnceTriggerInSessionsDiscarding() throws Exception {
		final int GAP_SIZE = 3;
		final long allowedLateness = 10000; // greater than the gap

		EventTimeSessionWindows windowAssigner = EventTimeSessionWindows.withGap(Time.seconds(GAP_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Count.<TimeWindow>atLeast(3).discarding(),
			allowedLateness);

		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(1, 0)));	// cleanup for 13999

		testHarness.processElement(new Tuple2<>("key2", 1), 2000);
		testHarness.processWatermark(4998);
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(2, 0)));	// cleanup for 14999

		testHarness.processElement(new Tuple2<>("key2", 1), 4500);										// +3 firing
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(3, 0)));	// cleanup for 17499

		testHarness.processElement(new Tuple2<>("key2", 1), 4500);
		testHarness.processElement(new Tuple2<>("key2", 1), 4500);
		testHarness.processElement(new Tuple2<>("key2", 1), 4500);
		testHarness.processElement(new Tuple2<>("key2", 1), 4500);

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(3, 0)));	// the timer is already there

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 1);

		testHarness.processWatermark(7500);																// nothing because we only have a count trigger
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 2);

		// new session

		testHarness.processElement(new Tuple2<>("key2", 1), 7501);										// new session with one element
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(4, 0)));	// cleanup for 20500

		testHarness.processElement(new Tuple2<>("key2", 1), 8000);
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(5, 0)));	// cleanup for 20999

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 8000);										// +3 for new session
		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(6, 0)));	// cleanup for 21499

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);										// +3 because these elements expand the window

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 3 + 2);								// no firing

		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);
		testHarness.processElement(new Tuple2<>("key2", 1), 8500);

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 3 + 2);								// no firing because we are in ONCE mode

		// merging the two sessions

		testHarness.processElement(new Tuple2<>("key2", 1), 6500);
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(7, 0)));	// cleanup for 21499 but for the whole window 1000 to 11500, so reinserted

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 3 + 3 + 8 + 2);

		testHarness.processWatermark(21500);															// to process all the cleanup timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	//			End of Session window testing

	@Test
	public void testStateDisambiguation() throws Exception {
		final int WINDOW_SIZE = 2;
		final long allowedLateness = 0; // greater than the gap

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.seconds(WINDOW_SIZE));

		Count<TimeWindow> count = Count.atLeast(5);
		Any<TimeWindow> child = Any.of(count, count, count);
		Any<TimeWindow> child2 = Any.of(child, child, count);
		Any<TimeWindow> root = Any.of(child2, child2);

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(root).accumulating(),
			allowedLateness);

		testHarness.open();

		// if we had collision, then we should have each element increasing the counter twice

		testHarness.processElement(new Tuple2<>("key1", 1), 100);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 100);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 100);	// so without disambiguation here we would fire
		testHarness.processWatermark(2001);

		Assert.assertTrue(testHarness.getOutput().size() == 1);		// just the watermark

		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	private static class TupleKeySelector implements KeySelector<Tuple2<String, Integer>, String> {
		private static final long serialVersionUID = 1L;

		@Override
		public String getKey(Tuple2<String, Integer> value) throws Exception {
			return value.f0;
		}
	}

	// Different state types tests.

	@Test
	public void testFoldingState() throws Exception {
		final int WINDOW_SIZE = 2;
		final long LATENESS = 100;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		FoldingStateDescriptor<Tuple2<String, Integer>, Tuple2<String, Integer>> windowStateDesc =
			new FoldingStateDescriptor<>(
				"window-contents",
				new Tuple2<>((String) null, 0),
				new FoldFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> fold(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> value) throws Exception {
						return new Tuple2<>(value.f0, accumulator.f1 + value.f1);
					}
				},
				inputType);

		ExecutionConfig config = new ExecutionConfig();
		KeySelector<Tuple2<String, Integer>, String> keySelector = new TupleKeySelector();
		TypeInformation<String> keyType = BasicTypeInfo.STRING_TYPE_INFO;

		TumblingEventTimeWindows assigner = TumblingEventTimeWindows.of(Time.of(WINDOW_SIZE, TimeUnit.SECONDS));
		DslTriggerRunner runner = new DslTriggerRunner<>(Repeat.Forever(EventTime.<TimeWindow>afterEndOfWindow()).discarding());
		runner.createTriggerTree(assigner.getWindowSerializer(config), LATENESS);

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				assigner,
				new TimeWindow.Serializer(),
				keySelector,
				keyType.createSerializer(config),
				windowStateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughFunction()),
				runner,
				LATENESS);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(operator, config, keySelector, keyType);

		operator.setInputType(inputType, config);
		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		// normal element
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 3), 1000));
		testHarness.processWatermark(new Watermark(1599));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processWatermark(new Watermark(1999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 1000)); // we do not fire now
		testHarness.processWatermark(new Watermark(2100));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 2000));
		testHarness.processWatermark(new Watermark(5000));

		expected.add(new Watermark(1599));
		expected.add(new StreamRecord<>(new Tuple2<>("key2", 2), 1999));
		expected.add(new StreamRecord<>(new Tuple2<>("key1", 3), 1999));
		expected.add(new Watermark(1999)); // here it fires and purges
		expected.add(new Watermark(2100)); // here is the cleanup timer
		expected.add(new StreamRecord<>(new Tuple2<>("key1", 1), 3999));
		expected.add(new Watermark(5000));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new WindowOperatorTest.Tuple2ResultSortComparator());
		testHarness.close();
	}

	@Test
	public void testCleanupTimerWithEmptyReduceStateForTumblingWindows() throws Exception {
		final int WINDOW_SIZE = 2;
		final long LATENESS = 100;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ExecutionConfig config = new ExecutionConfig();
		KeySelector<Tuple2<String, Integer>, String> keySelector = new TupleKeySelector();
		TypeInformation<String> keyType = BasicTypeInfo.STRING_TYPE_INFO;

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new WindowOperatorTest.SumReducer(),
			inputType.createSerializer(config));

		TumblingEventTimeWindows assigner = TumblingEventTimeWindows.of(Time.of(WINDOW_SIZE, TimeUnit.SECONDS));
		DslTriggerRunner runner = new DslTriggerRunner<>(Repeat.Forever(EventTime.afterEndOfWindow()).discarding());
		runner.createTriggerTree(assigner.getWindowSerializer(config), LATENESS);

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator =
			new WindowOperator<>(
				assigner,
				new TimeWindow.Serializer(),
				keySelector,
				keyType.createSerializer(config),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
				runner,
				LATENESS);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(operator, config, keySelector, keyType);

		operator.setInputType(inputType, config);
		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		// normal element
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processWatermark(new Watermark(1599));
		testHarness.processWatermark(new Watermark(1999));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 1000));  // no firing
		testHarness.processWatermark(new Watermark(2000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key1", 1), 2000));
		testHarness.processWatermark(new Watermark(5000));

		expected.add(new Watermark(1599));
		expected.add(new StreamRecord<>(new Tuple2<>("key2", 2), 1999));
		expected.add(new StreamRecord<>(new Tuple2<>("key1", 1), 1999));
		expected.add(new Watermark(1999)); // here it fires and purges
		expected.add(new Watermark(2000)); // here is the cleanup timer
		expected.add(new StreamRecord<>(new Tuple2<>("key1", 1), 3999));
		expected.add(new Watermark(5000));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, testHarness.getOutput(), new WindowOperatorTest.Tuple2ResultSortComparator());
		testHarness.close();
	}

	private class PassThroughFunction implements WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow> {
		private static final long serialVersionUID = 1L;

		@Override
		public void apply(String k, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
			for (Tuple2<String, Integer> in : input) {
				out.collect(in);
			}
		}
	}

	@Test
	@SuppressWarnings("unchecked")
	public void testSlidingEventTimeWindowsReduce() throws Exception {
		final int WINDOW_SIZE = 3;
		final int WINDOW_SLIDE = 1;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ExecutionConfig config = new ExecutionConfig();
		KeySelector<Tuple2<String, Integer>, String> keySelector = new TupleKeySelector();
		TypeInformation<String> keyType = BasicTypeInfo.STRING_TYPE_INFO;

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new WindowOperatorTest.SumReducer(),
			inputType.createSerializer(config));

		SlidingEventTimeWindows assigner = SlidingEventTimeWindows.of(Time.of(WINDOW_SIZE, TimeUnit.SECONDS), Time.of(WINDOW_SLIDE, TimeUnit.SECONDS));
		DslTriggerRunner runner = new DslTriggerRunner<>(Repeat.Forever(EventTime.afterEndOfWindow()).discarding());
		runner.createTriggerTree(assigner.getWindowSerializer(config), 0);

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow> operator = new WindowOperator<>(
			assigner,
			new TimeWindow.Serializer(),
			keySelector,
			keyType.createSerializer(config),
			stateDesc,
			new InternalSingleValueWindowFunction<>(new PassThroughWindowFunction<String, TimeWindow, Tuple2<String, Integer>>()),
			runner,
			0);

		operator.setInputType(inputType, config);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple2<String, Integer>> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(operator, config, keySelector, keyType);

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
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new WindowOperatorTest.Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(1999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 1999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 1999));
		expectedOutput.add(new Watermark(1999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new WindowOperatorTest.Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key1", 3), 2999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 3), 2999));
		expectedOutput.add(new Watermark(2999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new WindowOperatorTest.Tuple2ResultSortComparator());

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.setup();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(new Watermark(3999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 5), 3999));
		expectedOutput.add(new Watermark(3999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new WindowOperatorTest.Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(4999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 4999));
		expectedOutput.add(new Watermark(4999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new WindowOperatorTest.Tuple2ResultSortComparator());

		testHarness.processWatermark(new Watermark(5999));
		expectedOutput.add(new StreamRecord<>(new Tuple2<>("key2", 2), 5999));
		expectedOutput.add(new Watermark(5999));
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new WindowOperatorTest.Tuple2ResultSortComparator());

		// those don't have any effect...
		testHarness.processWatermark(new Watermark(6999));
		testHarness.processWatermark(new Watermark(7999));
		expectedOutput.add(new Watermark(6999));
		expectedOutput.add(new Watermark(7999));

		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expectedOutput, testHarness.getOutput(), new WindowOperatorTest.Tuple2ResultSortComparator());

		testHarness.close();
	}

	@Test
	public void testCleanupTimerWithEmptyReduceStateForSessionWindows() throws Exception {

		final int GAP_SIZE = 3;
		final long LATENESS = 10;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ExecutionConfig config = new ExecutionConfig();
		KeySelector<Tuple2<String, Integer>, String> keySelector = new TupleKeySelector();
		TypeInformation<String> keyType = BasicTypeInfo.STRING_TYPE_INFO;

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new WindowOperatorTest.SumReducer(), inputType.createSerializer(config));

		EventTimeSessionWindows assigner = EventTimeSessionWindows.withGap(Time.seconds(GAP_SIZE));
		DslTriggerRunner runner = new DslTriggerRunner<>(Repeat.Forever(EventTime.afterEndOfWindow()).accumulating());
		runner.createTriggerTree(assigner.getWindowSerializer(config), LATENESS);

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Long, Long>, TimeWindow> operator =
			new WindowOperator<>(
				assigner,
				new TimeWindow.Serializer(),
				keySelector,
				keyType.createSerializer(config),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new WindowOperatorTest.ReducedSessionWindowFunction()),
				runner,
				LATENESS);

		operator.setInputType(TypeInfoParser.<Tuple2<String, Integer>>parse("Tuple2<String, Integer>"), config);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(operator, config, keySelector, keyType);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processWatermark(new Watermark(3999));

		expected.add(new StreamRecord<>(new Tuple3<>("key2-2", 1000L, 4000L), 3999));
		expected.add(new Watermark(3999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2000));
		testHarness.processWatermark(new Watermark(4999));

		expected.add(new StreamRecord<>(new Tuple3<>("key2-3", 1000L, 5000L), 4999));
		expected.add(new Watermark(4999));

		testHarness.processWatermark(new Watermark(14600));
		expected.add(new Watermark(14600));

		ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new WindowOperatorTest.Tuple2ResultSortComparator());
		testHarness.close();
	}

	@Test
	public void testCleanupTimerWithEmptyReduceStateForSessionWindowsDiscarding() throws Exception {

		final int GAP_SIZE = 3;
		final long LATENESS = 10;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		ExecutionConfig config = new ExecutionConfig();

		ReducingStateDescriptor<Tuple2<String, Integer>> stateDesc = new ReducingStateDescriptor<>("window-contents",
			new WindowOperatorTest.SumReducer(), inputType.createSerializer(config));

		KeySelector<Tuple2<String, Integer>, String> keySelector = new TupleKeySelector();
		TypeInformation<String> keyType = BasicTypeInfo.STRING_TYPE_INFO;

		EventTimeSessionWindows assigner = EventTimeSessionWindows.withGap(Time.seconds(GAP_SIZE));
		DslTriggerRunner runner = new DslTriggerRunner<>(Repeat.Forever(EventTime.<TimeWindow>afterEndOfWindow()).discarding());
		runner.createTriggerTree(assigner.getWindowSerializer(config), LATENESS);

		WindowOperator<String, Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple3<String, Long, Long>, TimeWindow> operator =
			new WindowOperator<>(
				assigner,
				new TimeWindow.Serializer(),
				keySelector,
				keyType.createSerializer(config),
				stateDesc,
				new InternalSingleValueWindowFunction<>(new WindowOperatorTest.ReducedSessionWindowFunction()),
				runner,
				LATENESS);

		operator.setInputType(TypeInfoParser.<Tuple2<String, Integer>>parse("Tuple2<String, Integer>"), config);

		OneInputStreamOperatorTestHarness<Tuple2<String, Integer>, Tuple3<String, Long, Long>> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(operator, config, keySelector, keyType);

		testHarness.open();

		ConcurrentLinkedQueue<Object> expected = new ConcurrentLinkedQueue<>();

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 1000));
		testHarness.processWatermark(new Watermark(3999));

		expected.add(new StreamRecord<>(new Tuple3<>("key2-2", 1000L, 4000L), 3999));
		expected.add(new Watermark(3999));

		testHarness.processElement(new StreamRecord<>(new Tuple2<>("key2", 1), 2000));
		testHarness.processWatermark(new Watermark(4999));

		expected.add(new StreamRecord<>(new Tuple3<>("key2-1", 1000L, 5000L), 4999));	// because the previous window has not been gc'ed yet,
																						// we are still in the allowed lateness
		expected.add(new Watermark(4999));

		testHarness.processWatermark(new Watermark(14600));
		expected.add(new Watermark(14600));

		ConcurrentLinkedQueue<Object> actual = testHarness.getOutput();
		TestHarnessUtil.assertOutputEqualsSorted("Output was not correct.", expected, actual, new WindowOperatorTest.Tuple2ResultSortComparator());
		testHarness.close();
	}

	// Global windows testing.

	@Test
	public void testEventTimeTriggerGlobal() throws Exception {
		final int ALLOWED_LATENESS = 100;

		GlobalWindows windowAssigner = GlobalWindows.create();

		WindowingTestHarness<String, Tuple2<String, Integer>, GlobalWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(EventTime.<GlobalWindow>afterFirstElement(Time.milliseconds(100))).accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();

		testHarness.processWatermark(1000);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(1100);                            // + 6

		Assert.assertTrue(testHarness.getOutput().size() == 6 + 2);

		// added as late
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		testHarness.processWatermark(1200);                            // +7

		Assert.assertTrue(testHarness.getOutput().size() == 6 + 7 + 3);

		testHarness.processElement(new Tuple2<>("key1", 1), 1980);
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		testHarness.processWatermark(1300);                            // + 9

		Assert.assertTrue(testHarness.getOutput().size() == 6 + 7 + 9 + 4); // +3 for the watermark

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testProcessingTimeTriggerGlobal() throws Exception {
		final int ALLOWED_LATENESS = 100;

		GlobalWindows windowAssigner = GlobalWindows.create();

		WindowingTestHarness<String, Tuple2<String, Integer>, GlobalWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(ProcessingTime.<GlobalWindow>afterFirstElement(Time.milliseconds(100))).accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();

		testHarness.setProcessingTime(1000);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(1100);                            // + 6

		Assert.assertTrue(testHarness.getOutput().size() == 6);

		// added as late
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		testHarness.setProcessingTime(1200);                            // +7

		Assert.assertTrue(testHarness.getOutput().size() == 6 + 7);

		testHarness.processElement(new Tuple2<>("key1", 1), 1980);
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		testHarness.setProcessingTime(1300);                            // + 9

		Assert.assertTrue(testHarness.getOutput().size() == 6 + 7 + 9);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testCountTriggerGlobal() throws Exception {
		final int ALLOWED_LATENESS = 100;

		GlobalWindows windowAssigner = GlobalWindows.create();

		WindowingTestHarness<String, Tuple2<String, Integer>, GlobalWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(Count.<GlobalWindow>atLeast(4)).accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);		// +4

		Assert.assertTrue(testHarness.getOutput().size() == 4);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);		// +8

		Assert.assertTrue(testHarness.getOutput().size() == 4 + 8);

		// added as late
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testCountTriggerGlobalOnce() throws Exception {
		final int ALLOWED_LATENESS = 100;

		GlobalWindows windowAssigner = GlobalWindows.create();

		WindowingTestHarness<String, Tuple2<String, Integer>, GlobalWindow> testHarness = createTestHarness(
			windowAssigner,
			Count.<GlobalWindow>atLeast(4).accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);		// +4

		Assert.assertTrue(testHarness.getOutput().size() == 4);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);		// no firing

		Assert.assertTrue(testHarness.getOutput().size() == 4);

		// added as late
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerTumblingOnce() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 100;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			EventTime.<TimeWindow>afterEndOfWindow().accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();

		testHarness.processWatermark(1000);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(2000);							// ON_TIME					+6

		// added as late
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);	// no firing because we are at Repeat.ONCE

		testHarness.processWatermark(2200);    // this will be added but there is no late trigger specified so no firing

		// dropped
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		Assert.assertTrue(testHarness.getOutput().size() == 6 + 3);	// +3 for the watermark

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithEarlyCountTumblingOnce() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 100;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			EventTime.<TimeWindow>afterEndOfWindow().withEarlyTrigger(Count.<TimeWindow>atLeast(2)).accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();

		testHarness.processWatermark(1000);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);	//	+2 early firing

		Assert.assertTrue(testHarness.getOutput().size() == 2 + 1);

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processWatermark(2000);							// firing ON_TIME

		Assert.assertTrue(testHarness.getOutput().size() == 2 + 6 + 2);

		// added as late
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);	// no firing because we are at Repeat.ONCE

		testHarness.processWatermark(2200);    // this will be added but there is no late trigger specified so no firing

		// dropped
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		Assert.assertTrue(testHarness.getOutput().size() == 2 + 6 + 3);	// +3 for the watermark

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeTriggerWithLateCountTumblingOnce() throws Exception {
		final int WINDOW_SIZE = 2000;
		final int ALLOWED_LATENESS = 100;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			EventTime.<TimeWindow>afterEndOfWindow().withLateTrigger(Count.<TimeWindow>atLeast(2)).accumulating(),
			ALLOWED_LATENESS);

		testHarness.open();

		testHarness.processWatermark(1000);

		Assert.assertTrue(testHarness.getOutput().size() == 1);

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		// if we remove the following watermark, then we will have different
		// results because we do not checkpoint the watermark and the window
		// has not fired yet, to set the hasFiredOnTime flag.

		testHarness.processWatermark(2000);							// no firing ON_TIME because no elements

		Assert.assertTrue(testHarness.getOutput().size() == 2);		// for watermarks

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);	// +1 ON_TIME firing (although it is late)

		Assert.assertTrue(testHarness.getOutput().size() == 1 + 2);	// for watermarks

		testHarness.processElement(new Tuple2<>("key1", 1), 1000);
		testHarness.processElement(new Tuple2<>("key1", 1), 1000);	// +2 late firing

		Assert.assertTrue(testHarness.getOutput().size() == 1 + 3 + 2);

		// added as late
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);	// no firing because we are at Repeat.ONCE

		Assert.assertTrue(testHarness.getOutput().size() == 1 + 3 + 2);

		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		testHarness.processWatermark(2200);    // this will be added but there is no late trigger specified so no firing

		// dropped
		testHarness.processElement(new Tuple2<>("key1", 1), 1980);

		Assert.assertTrue(testHarness.getOutput().size() == 1 + 3 + 3);	// +3 for the watermarks

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testAnyOfTriggerWithTumblingWindowsOnce() throws Exception {
		final int WINDOW_SIZE = 2000;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Any.of(Count.<TimeWindow>atLeast(4),
				ProcessingTime.<TimeWindow>afterFirstElement(Time.milliseconds(100))).accumulating(),
			0);

		testHarness.open();

		testHarness.setProcessingTime(10);

		// normal element
		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);

		testHarness.setProcessingTime(110);							// +2 processing time
		Assert.assertTrue(testHarness.getOutput().size() == 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 1001);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);	// no firing

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1002);
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);	// +6 count_4 fires (accumulating)

		Assert.assertTrue(testHarness.getOutput().size() == 2);

		testHarness.setProcessingTime(250);							// no firing, state has been cleared

		Assert.assertTrue(testHarness.getOutput().size() == 2);

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(350);							// +9, processing time

		Assert.assertTrue(testHarness.getOutput().size() == 2);

		testHarness.processWatermark(2000);							// the cleanup timer

		// check the we have processed all the timers
		// (we do not have separate cleanup timer because lateness=0)
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testEventTimeWithEarlyOnce() throws Exception {
		final int WINDOW_SIZE = 2000;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			EventTime.<TimeWindow>afterEndOfWindow().withEarlyTrigger(Count.<TimeWindow>atLeast(3)).discarding(),
			0);

		testHarness.open();

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		// normal element
		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);	// +3 count_3 fires	key2

		Assert.assertTrue(testHarness.getOutput().size() == 3);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);

		Assert.assertTrue(testHarness.getOutput().size() == 3);

		testHarness.processElement(new Tuple2<>("key1", 1), 1004);
		testHarness.processElement(new Tuple2<>("key1", 1), 1004);

		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	// no firing (ONCE)

		Assert.assertTrue(testHarness.getOutput().size() == 3);

		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	// no firing

		testHarness.processWatermark(1999);							// +2 key1 +6 key2 fires on_time

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 2 + 6 + 1);// +1 for the watermark

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testAnyOfEventTimeWithEarlyOnce() throws Exception {
		final int WINDOW_SIZE = 2000;

		TypeInformation<Tuple2<String, Integer>> inputType = TypeInfoParser.parse("Tuple2<String, Integer>");

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Any.of(EventTime.<TimeWindow>afterEndOfWindow().withEarlyTrigger(Count.<TimeWindow>atLeast(3))).discarding(),
			0);

		testHarness.open();

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		// normal element
		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);	// +3 count_3 fires	key2

		Assert.assertTrue(testHarness.getOutput().size() == 3);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);

		Assert.assertTrue(testHarness.getOutput().size() == 3);

		testHarness.processElement(new Tuple2<>("key1", 1), 1004);
		testHarness.processElement(new Tuple2<>("key1", 1), 1004);

		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	// no firing (ONCE)

		Assert.assertTrue(testHarness.getOutput().size() == 3);

		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	// no firing

		testHarness.processWatermark(1999);							// +2 key1 fires on_time

		Assert.assertTrue(testHarness.getOutput().size() == 3 + 2 + 1);// +1 for the watermark

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testForeverAnyOfEventTimeWithEarlyOnce() throws Exception {
		final int WINDOW_SIZE = 2000;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(Any.of(EventTime.<TimeWindow>afterEndOfWindow().withEarlyTrigger(Count.<TimeWindow>atLeast(3)))).discarding(),
			0);

		// this will be translated to this: Repeat.Forever(Any.of [EventTimeTrigger.afterEndOfWindow().withEarlyTrigger(Repeat.Once(CountTrigger(3)))]).discarding()
		testHarness.open();

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		// normal element
		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);	// +3 count_3 fires	key2

		Assert.assertTrue(testHarness.getOutput().size() == 3);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);

		testHarness.processElement(new Tuple2<>("key1", 1), 1004);
		testHarness.processElement(new Tuple2<>("key1", 1), 1004);

		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	// +3 count_3 fires	key2

		Assert.assertTrue(testHarness.getOutput().size() == 6);

		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	// +3 count_3 fires	key2

		Assert.assertTrue(testHarness.getOutput().size() == 9);

		testHarness.processWatermark(1999);							// +2 key1 +0 key2 fires on_time

		Assert.assertTrue(testHarness.getOutput().size() == 9 + 2 + 1);// +1 for the watermark

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}

	@Test
	public void testForeverAnyOfEventTimeWithEarlyForever() throws Exception {
		final int WINDOW_SIZE = 2000;

		TumblingEventTimeWindows windowAssigner = TumblingEventTimeWindows.of(Time.milliseconds(WINDOW_SIZE));

		WindowingTestHarness<String, Tuple2<String, Integer>, TimeWindow> testHarness = createTestHarness(
			windowAssigner,
			Repeat.Forever(Any.of(EventTime.<TimeWindow>afterEndOfWindow().withEarlyTrigger(Repeat.Forever(Count.<TimeWindow>atLeast(3))))).discarding(),
			0);

		// this will be translated to this: Repeat.Forever(Any.of [EventTimeTrigger.afterEndOfWindow().withEarlyTrigger(Repeat.Forever(CountTrigger(3)))]).discarding()
		testHarness.open();

		// do a snapshot, close and restore again
		StreamStateHandle snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		// normal element
		testHarness.processElement(new Tuple2<>("key2", 1), 1000);
		testHarness.processElement(new Tuple2<>("key2", 1), 1001);
		testHarness.processElement(new Tuple2<>("key2", 1), 1002);	// +3 count_3 fires	key2

		Assert.assertTrue(testHarness.getOutput().size() == 3);

		// do a snapshot, close and restore again
		snapshot = testHarness.snapshot(0L, 0L);
		testHarness.close();
		testHarness.restore(snapshot);
		testHarness.open();

		testHarness.processElement(new Tuple2<>("key2", 1), 1003);

		testHarness.processElement(new Tuple2<>("key1", 1), 1004);
		testHarness.processElement(new Tuple2<>("key1", 1), 1004);

		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	// +3 count_3 fires	key2

		Assert.assertTrue(testHarness.getOutput().size() == 6);

		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);
		testHarness.processElement(new Tuple2<>("key2", 1), 1005);	// +3 count_3 fires	key2

		Assert.assertTrue(testHarness.getOutput().size() == 9);

		testHarness.processWatermark(1999);							// +2 key1 +6 key2 fires on_time

		Assert.assertTrue(testHarness.getOutput().size() == 9 + 2 + 1);// +1 for the watermark

		// check the we have processed all the timers
		Assert.assertTrue(testHarness.getOperator().getNumberOfTimers().equals(new Tuple2<>(0, 0)));

		testHarness.close();
		testHarness.dispose();
	}
}
