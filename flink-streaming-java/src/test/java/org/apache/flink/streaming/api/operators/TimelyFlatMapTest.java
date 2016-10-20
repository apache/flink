/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.streaming.api.operators;


import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.RichTimelyFlatMapFunction;
import org.apache.flink.streaming.api.functions.TimelyFlatMapFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.OperatorStateHandles;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;
import org.junit.Test;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;

/**
 * Tests {@link StreamTimelyFlatMap}.
 */
public class TimelyFlatMapTest extends TestLogger {

	@Test
	public void testCurrentEventTime() throws Exception {

		StreamTimelyFlatMap<Integer, Integer, String> operator =
				new StreamTimelyFlatMap<>(IntSerializer.INSTANCE, new QueryingFlatMapFunction(TimeDomain.EVENT_TIME));

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processWatermark(new Watermark(17));
		testHarness.processElement(new StreamRecord<>(5, 12L));

		testHarness.processWatermark(new Watermark(42));
		testHarness.processElement(new StreamRecord<>(6, 13L));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new Watermark(17L));
		expectedOutput.add(new StreamRecord<>("5TIME:17", 12L));
		expectedOutput.add(new Watermark(42L));
		expectedOutput.add(new StreamRecord<>("6TIME:42", 13L));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testCurrentProcessingTime() throws Exception {

		StreamTimelyFlatMap<Integer, Integer, String> operator =
				new StreamTimelyFlatMap<>(IntSerializer.INSTANCE, new QueryingFlatMapFunction(TimeDomain.PROCESSING_TIME));

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.setProcessingTime(17);
		testHarness.processElement(new StreamRecord<>(5));

		testHarness.setProcessingTime(42);
		testHarness.processElement(new StreamRecord<>(6));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new StreamRecord<>("5TIME:17"));
		expectedOutput.add(new StreamRecord<>("6TIME:42"));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testEventTimeTimers() throws Exception {

		StreamTimelyFlatMap<Integer, Integer, Integer> operator =
				new StreamTimelyFlatMap<>(IntSerializer.INSTANCE, new TriggeringFlatMapFunction(TimeDomain.EVENT_TIME));

		OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processWatermark(new Watermark(0));

		testHarness.processElement(new StreamRecord<>(17, 42L));

		testHarness.processWatermark(new Watermark(5));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new Watermark(0L));
		expectedOutput.add(new StreamRecord<>(17, 42L));
		expectedOutput.add(new StreamRecord<>(1777, 5L));
		expectedOutput.add(new Watermark(5L));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testProcessingTimeTimers() throws Exception {

		StreamTimelyFlatMap<Integer, Integer, Integer> operator =
				new StreamTimelyFlatMap<>(IntSerializer.INSTANCE, new TriggeringFlatMapFunction(TimeDomain.PROCESSING_TIME));

		OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processElement(new StreamRecord<>(17));

		testHarness.setProcessingTime(5);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new StreamRecord<>(17));
		expectedOutput.add(new StreamRecord<>(1777, 5L));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	/**
	 * Verifies that we don't have leakage between different keys.
	 */
	@Test
	public void testEventTimeTimerWithState() throws Exception {

		StreamTimelyFlatMap<Integer, Integer, String> operator =
				new StreamTimelyFlatMap<>(IntSerializer.INSTANCE, new TriggeringStatefulFlatMapFunction(TimeDomain.EVENT_TIME));

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processWatermark(new Watermark(1));
		testHarness.processElement(new StreamRecord<>(17, 0L)); // should set timer for 6

		testHarness.processWatermark(new Watermark(2));
		testHarness.processElement(new StreamRecord<>(42, 1L)); // should set timer for 7

		testHarness.processWatermark(new Watermark(6));
		testHarness.processWatermark(new Watermark(7));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new Watermark(1L));
		expectedOutput.add(new StreamRecord<>("INPUT:17", 0L));
		expectedOutput.add(new Watermark(2L));
		expectedOutput.add(new StreamRecord<>("INPUT:42", 1L));
		expectedOutput.add(new StreamRecord<>("STATE:17", 6L));
		expectedOutput.add(new Watermark(6L));
		expectedOutput.add(new StreamRecord<>("STATE:42", 7L));
		expectedOutput.add(new Watermark(7L));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	/**
	 * Verifies that we don't have leakage between different keys.
	 */
	@Test
	public void testProcessingTimeTimerWithState() throws Exception {

		StreamTimelyFlatMap<Integer, Integer, String> operator =
				new StreamTimelyFlatMap<>(IntSerializer.INSTANCE, new TriggeringStatefulFlatMapFunction(TimeDomain.PROCESSING_TIME));

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.setProcessingTime(1);
		testHarness.processElement(new StreamRecord<>(17)); // should set timer for 6

		testHarness.setProcessingTime(2);
		testHarness.processElement(new StreamRecord<>(42)); // should set timer for 7

		testHarness.setProcessingTime(6);
		testHarness.setProcessingTime(7);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new StreamRecord<>("INPUT:17"));
		expectedOutput.add(new StreamRecord<>("INPUT:42"));
		expectedOutput.add(new StreamRecord<>("STATE:17", 6L));
		expectedOutput.add(new StreamRecord<>("STATE:42", 7L));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testSnapshotAndRestore() throws Exception {

		StreamTimelyFlatMap<Integer, Integer, String> operator =
				new StreamTimelyFlatMap<>(IntSerializer.INSTANCE, new BothTriggeringFlatMapFunction());

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processElement(new StreamRecord<>(5, 12L));

		// snapshot and restore from scratch
		OperatorStateHandles snapshot = testHarness.snapshot(0, 0);

		testHarness.close();

		operator = new StreamTimelyFlatMap<>(IntSerializer.INSTANCE, new BothTriggeringFlatMapFunction());

		testHarness = new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(5);
		testHarness.processWatermark(new Watermark(6));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new StreamRecord<>("PROC:1777", 5L));
		expectedOutput.add(new StreamRecord<>("EVENT:1777", 6L));
		expectedOutput.add(new Watermark(6));

		System.out.println("GOT: " + testHarness.getOutput());

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	private static class IdentityKeySelector<T> implements KeySelector<T, T> {
		private static final long serialVersionUID = 1L;

		@Override
		public T getKey(T value) throws Exception {
			return value;
		}
	}

	private static class QueryingFlatMapFunction implements TimelyFlatMapFunction<Integer, String> {

		private static final long serialVersionUID = 1L;

		private final TimeDomain timeDomain;

		public QueryingFlatMapFunction(TimeDomain timeDomain) {
			this.timeDomain = timeDomain;
		}

		@Override
		public void flatMap(Integer value, TimerService timerService, Collector<String> out) throws Exception {
			if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
				out.collect(value + "TIME:" + timerService.currentWatermark());
			} else {
				out.collect(value + "TIME:" + timerService.currentProcessingTime());
			}
		}

		@Override
		public void onTimer(
				long timestamp,
				TimeDomain timeDomain,
				TimerService timerService,
				Collector<String> out) throws Exception {
		}
	}

	private static class TriggeringFlatMapFunction implements TimelyFlatMapFunction<Integer, Integer> {

		private static final long serialVersionUID = 1L;

		private final TimeDomain timeDomain;

		public TriggeringFlatMapFunction(TimeDomain timeDomain) {
			this.timeDomain = timeDomain;
		}

		@Override
		public void flatMap(Integer value, TimerService timerService, Collector<Integer> out) throws Exception {
			out.collect(value);
			if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
				timerService.registerEventTimeTimer(timerService.currentWatermark() + 5);
			} else {
				timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + 5);
			}
		}

		@Override
		public void onTimer(
				long timestamp,
				TimeDomain timeDomain,
				TimerService timerService,
				Collector<Integer> out) throws Exception {

			assertEquals(this.timeDomain, timeDomain);
			out.collect(1777);
		}
	}

	private static class TriggeringStatefulFlatMapFunction extends RichTimelyFlatMapFunction<Integer, String> {

		private static final long serialVersionUID = 1L;

		private final ValueStateDescriptor<Integer> state =
				new ValueStateDescriptor<>("seen-element", IntSerializer.INSTANCE, null);

		private final TimeDomain timeDomain;

		public TriggeringStatefulFlatMapFunction(TimeDomain timeDomain) {
			this.timeDomain = timeDomain;
		}

		@Override
		public void flatMap(Integer value, TimerService timerService, Collector<String> out) throws Exception {
			out.collect("INPUT:" + value);
			getRuntimeContext().getState(state).update(value);
			if (timeDomain.equals(TimeDomain.EVENT_TIME)) {
				timerService.registerEventTimeTimer(timerService.currentWatermark() + 5);
			} else {
				timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + 5);
			}
		}

		@Override
		public void onTimer(
				long timestamp,
				TimeDomain timeDomain,
				TimerService timerService,
				Collector<String> out) throws Exception {
			assertEquals(this.timeDomain, timeDomain);
			out.collect("STATE:" + getRuntimeContext().getState(state).value());
		}
	}

	private static class BothTriggeringFlatMapFunction implements TimelyFlatMapFunction<Integer, String> {

		private static final long serialVersionUID = 1L;

		@Override
		public void flatMap(Integer value, TimerService timerService, Collector<String> out) throws Exception {
			timerService.registerProcessingTimeTimer(5);
			timerService.registerEventTimeTimer(6);

		}

		@Override
		public void onTimer(
				long timestamp,
				TimeDomain timeDomain,
				TimerService timerService,
				Collector<String> out) throws Exception {
			if (TimeDomain.EVENT_TIME.equals(timeDomain)) {
				out.collect("EVENT:1777");
			} else {
				out.collect("PROC:1777");
			}
		}
	}

}
