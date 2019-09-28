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

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.TimeDomain;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.concurrent.ConcurrentLinkedQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests {@link KeyedProcessOperator}.
 */
public class KeyedProcessOperatorTest extends TestLogger {

	@Rule
	public ExpectedException expectedException = ExpectedException.none();

	@Test
	public void testKeyQuerying() throws Exception {

		class KeyQueryingProcessFunction extends KeyedProcessFunction<Integer, Tuple2<Integer, String>, String> {

			@Override
			public void processElement(
				Tuple2<Integer, String> value,
				Context ctx,
				Collector<String> out) throws Exception {

				assertTrue("Did not get expected key.", ctx.getCurrentKey().equals(value.f0));

				// we check that we receive this output, to ensure that the assert was actually checked
				out.collect(value.f1);
			}
		}

		KeyedProcessOperator<Integer, Tuple2<Integer, String>, String> operator =
			new KeyedProcessOperator<>(new KeyQueryingProcessFunction());

		try (
			OneInputStreamOperatorTestHarness<Tuple2<Integer, String>, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, (in) -> in.f0 , BasicTypeInfo.INT_TYPE_INFO)) {

			testHarness.setup();
			testHarness.open();

			testHarness.processElement(new StreamRecord<>(Tuple2.of(5, "5"), 12L));
			testHarness.processElement(new StreamRecord<>(Tuple2.of(42, "42"), 13L));

			ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();
			expectedOutput.add(new StreamRecord<>("5", 12L));
			expectedOutput.add(new StreamRecord<>("42", 13L));

			TestHarnessUtil.assertOutputEquals(
				"Output was not correct.",
				expectedOutput,
				testHarness.getOutput());
		}
	}

	@Test
	public void testTimestampAndWatermarkQuerying() throws Exception {

		KeyedProcessOperator<Integer, Integer, String> operator =
				new KeyedProcessOperator<>(new QueryingFlatMapFunction(TimeDomain.EVENT_TIME));

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
		expectedOutput.add(new StreamRecord<>("5TIME:17 TS:12", 12L));
		expectedOutput.add(new Watermark(42L));
		expectedOutput.add(new StreamRecord<>("6TIME:42 TS:13", 13L));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testTimestampAndProcessingTimeQuerying() throws Exception {

		KeyedProcessOperator<Integer, Integer, String> operator =
				new KeyedProcessOperator<>(new QueryingFlatMapFunction(TimeDomain.PROCESSING_TIME));

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.setProcessingTime(17);
		testHarness.processElement(new StreamRecord<>(5));

		testHarness.setProcessingTime(42);
		testHarness.processElement(new StreamRecord<>(6));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new StreamRecord<>("5TIME:17 TS:null"));
		expectedOutput.add(new StreamRecord<>("6TIME:42 TS:null"));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testEventTimeTimers() throws Exception {

		final int expectedKey = 17;

		KeyedProcessOperator<Integer, Integer, Integer> operator =
				new KeyedProcessOperator<>(new TriggeringFlatMapFunction(TimeDomain.EVENT_TIME, expectedKey));

		OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processWatermark(new Watermark(0));

		testHarness.processElement(new StreamRecord<>(expectedKey, 42L));

		testHarness.processWatermark(new Watermark(5));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new Watermark(0L));
		expectedOutput.add(new StreamRecord<>(expectedKey, 42L));
		expectedOutput.add(new StreamRecord<>(1777, 5L));
		expectedOutput.add(new Watermark(5L));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testProcessingTimeTimers() throws Exception {

		final int expectedKey = 17;

		KeyedProcessOperator<Integer, Integer, Integer> operator =
				new KeyedProcessOperator<>(new TriggeringFlatMapFunction(TimeDomain.PROCESSING_TIME, expectedKey));

		OneInputStreamOperatorTestHarness<Integer, Integer> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processElement(new StreamRecord<>(expectedKey));

		testHarness.setProcessingTime(5);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new StreamRecord<>(expectedKey));
		expectedOutput.add(new StreamRecord<>(1777));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	/**
	 * Verifies that we don't have leakage between different keys.
	 */
	@Test
	public void testEventTimeTimerWithState() throws Exception {

		KeyedProcessOperator<Integer, Integer, String> operator =
				new KeyedProcessOperator<>(new TriggeringStatefulFlatMapFunction(TimeDomain.EVENT_TIME));

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processWatermark(new Watermark(1));
		testHarness.processElement(new StreamRecord<>(17, 0L)); // should set timer for 6
		testHarness.processElement(new StreamRecord<>(13, 0L)); // should set timer for 6

		testHarness.processWatermark(new Watermark(2));
		testHarness.processElement(new StreamRecord<>(42, 1L)); // should set timer for 7
		testHarness.processElement(new StreamRecord<>(13, 1L)); // should delete timer

		testHarness.processWatermark(new Watermark(6));
		testHarness.processWatermark(new Watermark(7));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new Watermark(1L));
		expectedOutput.add(new StreamRecord<>("INPUT:17", 0L));
		expectedOutput.add(new StreamRecord<>("INPUT:13", 0L));
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

		KeyedProcessOperator<Integer, Integer, String> operator =
				new KeyedProcessOperator<>(new TriggeringStatefulFlatMapFunction(TimeDomain.PROCESSING_TIME));

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.setProcessingTime(1);
		testHarness.processElement(new StreamRecord<>(17)); // should set timer for 6
		testHarness.processElement(new StreamRecord<>(13)); // should set timer for 6

		testHarness.setProcessingTime(2);
		testHarness.processElement(new StreamRecord<>(13)); // should delete timer
		testHarness.processElement(new StreamRecord<>(42)); // should set timer for 7

		testHarness.setProcessingTime(6);
		testHarness.setProcessingTime(7);

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new StreamRecord<>("INPUT:17"));
		expectedOutput.add(new StreamRecord<>("INPUT:13"));
		expectedOutput.add(new StreamRecord<>("INPUT:42"));
		expectedOutput.add(new StreamRecord<>("STATE:17"));
		expectedOutput.add(new StreamRecord<>("STATE:42"));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testSnapshotAndRestore() throws Exception {

		final int expectedKey = 5;

		KeyedProcessOperator<Integer, Integer, String> operator =
				new KeyedProcessOperator<>(new BothTriggeringFlatMapFunction(expectedKey));

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processElement(new StreamRecord<>(expectedKey, 12L));

		// snapshot and restore from scratch
		OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

		testHarness.close();

		operator = new KeyedProcessOperator<>(new BothTriggeringFlatMapFunction(expectedKey));

		testHarness = new KeyedOneInputStreamOperatorTestHarness<>(operator, new IdentityKeySelector<Integer>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.initializeState(snapshot);
		testHarness.open();

		testHarness.setProcessingTime(5);
		testHarness.processWatermark(new Watermark(6));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new StreamRecord<>("PROC:1777"));
		expectedOutput.add(new StreamRecord<>("EVENT:1777", 6L));
		expectedOutput.add(new Watermark(6));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		testHarness.close();
	}

	@Test
	public void testNullOutputTagRefusal() throws Exception {
		KeyedProcessOperator<Integer, Integer, String> operator = new KeyedProcessOperator<>(new NullOutputTagEmittingProcessFunction());

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(
				operator, new IdentityKeySelector<>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.setProcessingTime(17);
		try {
			expectedException.expect(IllegalArgumentException.class);
			testHarness.processElement(new StreamRecord<>(5));
		} finally {
			testHarness.close();
		}
	}

	/**
	 * This also verifies that the timestamps ouf side-emitted records is correct.
	 */
	@Test
	public void testSideOutput() throws Exception {
		KeyedProcessOperator<Integer, Integer, String> operator = new KeyedProcessOperator<>(new SideOutputProcessFunction());

		OneInputStreamOperatorTestHarness<Integer, String> testHarness =
			new KeyedOneInputStreamOperatorTestHarness<>(
				operator, new IdentityKeySelector<>(), BasicTypeInfo.INT_TYPE_INFO);

		testHarness.setup();
		testHarness.open();

		testHarness.processElement(new StreamRecord<>(42, 17L /* timestamp */));

		ConcurrentLinkedQueue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

		expectedOutput.add(new StreamRecord<>("IN:42", 17L /* timestamp */));

		TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());

		ConcurrentLinkedQueue<StreamRecord<Integer>> expectedIntSideOutput = new ConcurrentLinkedQueue<>();
		expectedIntSideOutput.add(new StreamRecord<>(42, 17L /* timestamp */));
		ConcurrentLinkedQueue<StreamRecord<Integer>> intSideOutput =
			testHarness.getSideOutput(SideOutputProcessFunction.INTEGER_OUTPUT_TAG);
		TestHarnessUtil.assertOutputEquals(
			"Side output was not correct.",
			expectedIntSideOutput,
			intSideOutput);

		ConcurrentLinkedQueue<StreamRecord<Long>> expectedLongSideOutput = new ConcurrentLinkedQueue<>();
		expectedLongSideOutput.add(new StreamRecord<>(42L, 17L /* timestamp */));
		ConcurrentLinkedQueue<StreamRecord<Long>> longSideOutput =
			testHarness.getSideOutput(SideOutputProcessFunction.LONG_OUTPUT_TAG);
		TestHarnessUtil.assertOutputEquals(
			"Side output was not correct.",
			expectedLongSideOutput,
			longSideOutput);

		testHarness.close();
	}

	private static class NullOutputTagEmittingProcessFunction extends KeyedProcessFunction<Integer, Integer, String> {

		@Override
		public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
			ctx.output(null, value);
		}
	}

	private static class SideOutputProcessFunction extends KeyedProcessFunction<Integer, Integer, String> {

		static final OutputTag<Integer> INTEGER_OUTPUT_TAG = new OutputTag<Integer>("int-out") {};
		static final OutputTag<Long> LONG_OUTPUT_TAG = new OutputTag<Long>("long-out") {};

		@Override
		public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
			out.collect("IN:" + value);

			ctx.output(INTEGER_OUTPUT_TAG, value);
			ctx.output(LONG_OUTPUT_TAG, value.longValue());
		}
	}

	private static class IdentityKeySelector<T> implements KeySelector<T, T> {
		private static final long serialVersionUID = 1L;

		@Override
		public T getKey(T value) throws Exception {
			return value;
		}
	}

	private static class QueryingFlatMapFunction extends KeyedProcessFunction<Integer, Integer, String> {

		private static final long serialVersionUID = 1L;

		private final TimeDomain expectedTimeDomain;

		public QueryingFlatMapFunction(TimeDomain timeDomain) {
			this.expectedTimeDomain = timeDomain;
		}

		@Override
		public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
			if (expectedTimeDomain.equals(TimeDomain.EVENT_TIME)) {
				out.collect(value + "TIME:" + ctx.timerService().currentWatermark() + " TS:" + ctx.timestamp());
			} else {
				out.collect(value + "TIME:" + ctx.timerService().currentProcessingTime() + " TS:" + ctx.timestamp());
			}
		}

		@Override
		public void onTimer(
				long timestamp,
				OnTimerContext ctx,
				Collector<String> out) throws Exception {
			// Do nothing
		}
	}

	private static class TriggeringFlatMapFunction extends KeyedProcessFunction<Integer, Integer, Integer> {

		private static final long serialVersionUID = 1L;

		private final TimeDomain expectedTimeDomain;
		private final Integer expectedKey;

		public TriggeringFlatMapFunction(TimeDomain timeDomain, Integer expectedKey) {
			this.expectedTimeDomain = timeDomain;
			this.expectedKey = expectedKey;
		}

		@Override
		public void processElement(Integer value, Context ctx, Collector<Integer> out) throws Exception {
			out.collect(value);
			if (expectedTimeDomain.equals(TimeDomain.EVENT_TIME)) {
				ctx.timerService().registerEventTimeTimer(ctx.timerService().currentWatermark() + 5);
			} else {
				ctx.timerService().registerProcessingTimeTimer(ctx.timerService().currentProcessingTime() + 5);
			}
		}

		@Override
		public void onTimer(
				long timestamp,
				OnTimerContext ctx,
				Collector<Integer> out) throws Exception {
			assertEquals(expectedKey, ctx.getCurrentKey());
			assertEquals(expectedTimeDomain, ctx.timeDomain());
			out.collect(1777);
		}
	}

	private static class TriggeringStatefulFlatMapFunction extends KeyedProcessFunction<Integer, Integer, String> {

		private static final long serialVersionUID = 1L;

		private final ValueStateDescriptor<Integer> state =
				new ValueStateDescriptor<>("seen-element", IntSerializer.INSTANCE);

		private final TimeDomain expectedTimeDomain;

		public TriggeringStatefulFlatMapFunction(TimeDomain timeDomain) {
			this.expectedTimeDomain = timeDomain;
		}

		@Override
		public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
			final TimerService timerService = ctx.timerService();
			final ValueState<Integer> state = getRuntimeContext().getState(this.state);
			if (state.value() == null) {
				out.collect("INPUT:" + value);
				state.update(value);
				if (expectedTimeDomain.equals(TimeDomain.EVENT_TIME)) {
					timerService.registerEventTimeTimer(timerService.currentWatermark() + 5);
				} else {
					timerService.registerProcessingTimeTimer(timerService.currentProcessingTime() + 5);
				}
			} else {
				state.clear();
				if (expectedTimeDomain.equals(TimeDomain.EVENT_TIME)) {
					timerService.deleteEventTimeTimer(timerService.currentWatermark() + 4);
				} else {
					timerService.deleteProcessingTimeTimer(timerService.currentProcessingTime() + 4);
				}
			}
		}

		@Override
		public void onTimer(
				long timestamp,
				OnTimerContext ctx,
				Collector<String> out) throws Exception {
			assertEquals(expectedTimeDomain, ctx.timeDomain());
			out.collect("STATE:" + getRuntimeContext().getState(state).value());
		}
	}

	private static class BothTriggeringFlatMapFunction extends KeyedProcessFunction<Integer, Integer, String> {

		private static final long serialVersionUID = 1L;

		private final Integer expectedKey;

		public BothTriggeringFlatMapFunction(Integer expectedKey) {
			this.expectedKey = expectedKey;
		}

		@Override
		public void processElement(Integer value, Context ctx, Collector<String> out) throws Exception {
			final TimerService timerService = ctx.timerService();

			timerService.registerProcessingTimeTimer(3);
			timerService.registerEventTimeTimer(4);
			timerService.registerProcessingTimeTimer(5);
			timerService.registerEventTimeTimer(6);
			timerService.deleteProcessingTimeTimer(3);
			timerService.deleteEventTimeTimer(4);
		}

		@Override
		public void onTimer(
				long timestamp,
				OnTimerContext ctx,
				Collector<String> out) throws Exception {
			assertEquals(expectedKey, ctx.getCurrentKey());

			if (TimeDomain.EVENT_TIME.equals(ctx.timeDomain())) {
				out.collect("EVENT:1777");
			} else {
				out.collect("PROC:1777");
			}
		}
	}
}
