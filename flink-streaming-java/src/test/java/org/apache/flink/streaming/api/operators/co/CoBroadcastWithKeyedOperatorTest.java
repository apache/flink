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

package org.apache.flink.streaming.api.operators.co;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedTwoInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.TwoInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Function;

/**
 * Tests for the {@link CoBroadcastWithKeyedOperator}.
 */
public class CoBroadcastWithKeyedOperatorTest {

	private static final MapStateDescriptor<String, Integer> STATE_DESCRIPTOR =
			new MapStateDescriptor<>(
					"broadcast-state",
					BasicTypeInfo.STRING_TYPE_INFO,
					BasicTypeInfo.INT_TYPE_INFO
			);

	/** Test the iteration over the keyed state on the broadcast side. */
	@Test
	public void testAccessToKeyedStateIt() throws Exception {
		final List<String> test1content = new ArrayList<>();
		test1content.add("test1");
		test1content.add("test1");

		final List<String> test2content = new ArrayList<>();
		test2content.add("test2");
		test2content.add("test2");
		test2content.add("test2");
		test2content.add("test2");

		final List<String> test3content = new ArrayList<>();
		test3content.add("test3");
		test3content.add("test3");
		test3content.add("test3");

		final Map<String, List<String>> expectedState = new HashMap<>();
		expectedState.put("test1", test1content);
		expectedState.put("test2", test2content);
		expectedState.put("test3", test3content);

		try (
				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new StatefulFunctionWithKeyedStateAccessedOnBroadcast(expectedState))
		) {

			// send elements to the keyed state
			testHarness.processElement1(new StreamRecord<>("test1", 12L));
			testHarness.processElement1(new StreamRecord<>("test1", 12L));

			testHarness.processElement1(new StreamRecord<>("test2", 13L));
			testHarness.processElement1(new StreamRecord<>("test2", 13L));
			testHarness.processElement1(new StreamRecord<>("test2", 13L));

			testHarness.processElement1(new StreamRecord<>("test3", 14L));
			testHarness.processElement1(new StreamRecord<>("test3", 14L));
			testHarness.processElement1(new StreamRecord<>("test3", 14L));

			testHarness.processElement1(new StreamRecord<>("test2", 13L));

			// this is the element on the broadcast side that will trigger the verification
			// check the StatefulFunctionWithKeyedStateAccessedOnBroadcast#processBroadcastElement()
			testHarness.processElement2(new StreamRecord<>(1, 13L));
		}
	}

	/**
	 * Simple {@link KeyedBroadcastProcessFunction} that adds all incoming elements in the non-broadcast
	 * side to a listState and at the broadcast side it verifies if the stored data is the expected ones.
	 */
	private static class StatefulFunctionWithKeyedStateAccessedOnBroadcast
			extends KeyedBroadcastProcessFunction<String, String, Integer, String> {

		private static final long serialVersionUID = 7496674620398203933L;

		private final ListStateDescriptor<String> listStateDesc =
				new ListStateDescriptor<>("listStateTest", BasicTypeInfo.STRING_TYPE_INFO);

		private final Map<String, List<String>> expectedKeyedStates;

		StatefulFunctionWithKeyedStateAccessedOnBroadcast(Map<String, List<String>> expectedKeyedState) {
			this.expectedKeyedStates = Preconditions.checkNotNull(expectedKeyedState);
		}

		@Override
		public void processBroadcastElement(Integer value, KeyedContext ctx, Collector<String> out) throws Exception {
			// put an element in the broadcast state
			ctx.applyToKeyedState(
					listStateDesc,
					new KeyedStateFunction<String, ListState<String>>() {
						@Override
						public void process(String key, ListState<String> state) throws Exception {
							final Iterator<String> it = state.get().iterator();

							final List<String> list = new ArrayList<>();
							while (it.hasNext()) {
								list.add(it.next());
							}
							Assert.assertEquals(expectedKeyedStates.get(key), list);
						}
					});
		}

		@Override
		public void processElement(String value, KeyedReadOnlyContext ctx, Collector<String> out) throws Exception {
			getRuntimeContext().getListState(listStateDesc).add(value);
		}
	}

	@Test
	public void testFunctionWithTimer() throws Exception {

		try (
				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new FunctionWithTimerOnKeyed(41L))
		) {
			testHarness.processWatermark1(new Watermark(10L));
			testHarness.processWatermark2(new Watermark(10L));
			testHarness.processElement2(new StreamRecord<>(5, 12L));

			testHarness.processWatermark1(new Watermark(40L));
			testHarness.processWatermark2(new Watermark(40L));
			testHarness.processElement1(new StreamRecord<>("6", 13L));
			testHarness.processElement1(new StreamRecord<>("6", 15L));

			testHarness.processWatermark1(new Watermark(50L));
			testHarness.processWatermark2(new Watermark(50L));

			Queue<Object> expectedOutput = new ConcurrentLinkedQueue<>();

			expectedOutput.add(new Watermark(10L));
			expectedOutput.add(new StreamRecord<>("BR:5 WM:10 TS:12", 12L));
			expectedOutput.add(new Watermark(40L));
			expectedOutput.add(new StreamRecord<>("NON-BR:6 WM:40 TS:13", 13L));
			expectedOutput.add(new StreamRecord<>("NON-BR:6 WM:40 TS:15", 15L));
			expectedOutput.add(new StreamRecord<>("TIMER:41", 41L));
			expectedOutput.add(new Watermark(50L));

			TestHarnessUtil.assertOutputEquals("Output was not correct.", expectedOutput, testHarness.getOutput());
		}
	}

	/**
	 * {@link KeyedBroadcastProcessFunction} that registers a timer and emits
	 * for every element the watermark and the timestamp of the element.
	 */
	private static class FunctionWithTimerOnKeyed extends KeyedBroadcastProcessFunction<String, String, Integer, String> {

		private static final long serialVersionUID = 7496674620398203933L;

		private final long timerTS;

		FunctionWithTimerOnKeyed(long timerTS) {
			this.timerTS = timerTS;
		}

		@Override
		public void processBroadcastElement(Integer value, KeyedContext ctx, Collector<String> out) throws Exception {
			out.collect("BR:" + value + " WM:" + ctx.currentWatermark() + " TS:" + ctx.timestamp());
		}

		@Override
		public void processElement(String value, KeyedReadOnlyContext ctx, Collector<String> out) throws Exception {
			ctx.timerService().registerEventTimeTimer(timerTS);
			out.collect("NON-BR:" + value + " WM:" + ctx.currentWatermark() + " TS:" + ctx.timestamp());
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
			out.collect("TIMER:" + timestamp);
		}
	}

	@Test
	public void testSideOutput() throws Exception {
		try (
				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new FunctionWithSideOutput())
		) {

			testHarness.processWatermark1(new Watermark(10L));
			testHarness.processWatermark2(new Watermark(10L));
			testHarness.processElement2(new StreamRecord<>(5, 12L));

			testHarness.processWatermark1(new Watermark(40L));
			testHarness.processWatermark2(new Watermark(40L));
			testHarness.processElement1(new StreamRecord<>("6", 13L));
			testHarness.processElement1(new StreamRecord<>("6", 15L));

			testHarness.processWatermark1(new Watermark(50L));
			testHarness.processWatermark2(new Watermark(50L));

			Queue<StreamRecord<String>> expectedBr = new ConcurrentLinkedQueue<>();
			expectedBr.add(new StreamRecord<>("BR:5 WM:10 TS:12", 12L));

			Queue<StreamRecord<String>> expectedNonBr = new ConcurrentLinkedQueue<>();
			expectedNonBr.add(new StreamRecord<>("NON-BR:6 WM:40 TS:13", 13L));
			expectedNonBr.add(new StreamRecord<>("NON-BR:6 WM:40 TS:15", 15L));

			TestHarnessUtil.assertOutputEquals(
					"Wrong Side Output",
					expectedBr,
					testHarness.getSideOutput(FunctionWithSideOutput.BROADCAST_TAG));

			TestHarnessUtil.assertOutputEquals(
					"Wrong Side Output",
					expectedNonBr,
					testHarness.getSideOutput(FunctionWithSideOutput.NON_BROADCAST_TAG));
		}
	}

	/**
	 * {@link KeyedBroadcastProcessFunction} that emits elements on side outputs.
	 */
	private static class FunctionWithSideOutput extends KeyedBroadcastProcessFunction<String, String, Integer, String> {

		private static final long serialVersionUID = 7496674620398203933L;

		static final OutputTag<String> BROADCAST_TAG = new OutputTag<String>("br-out") {
			private static final long serialVersionUID = -6899484480421899631L;
		};

		static final OutputTag<String> NON_BROADCAST_TAG = new OutputTag<String>("non-br-out") {
			private static final long serialVersionUID = 3837387110613831791L;
		};

		@Override
		public void processBroadcastElement(Integer value, KeyedContext ctx, Collector<String> out) throws Exception {
			ctx.output(BROADCAST_TAG, "BR:" + value + " WM:" + ctx.currentWatermark() + " TS:" + ctx.timestamp());
		}

		@Override
		public void processElement(String value, KeyedReadOnlyContext ctx, Collector<String> out) throws Exception {
			ctx.output(NON_BROADCAST_TAG, "NON-BR:" + value + " WM:" + ctx.currentWatermark() + " TS:" + ctx.timestamp());
		}
	}

	@Test
	public void testFunctionWithBroadcastState() throws Exception {

		final Map<String, Integer> expectedBroadcastState = new HashMap<>();
		expectedBroadcastState.put("5.key", 5);
		expectedBroadcastState.put("34.key", 34);
		expectedBroadcastState.put("53.key", 53);
		expectedBroadcastState.put("12.key", 12);
		expectedBroadcastState.put("98.key", 98);

		try (
				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new FunctionWithBroadcastState("key", expectedBroadcastState, 41L))
		) {
			testHarness.processWatermark1(new Watermark(10L));
			testHarness.processWatermark2(new Watermark(10L));

			testHarness.processElement2(new StreamRecord<>(5, 10L));
			testHarness.processElement2(new StreamRecord<>(34, 12L));
			testHarness.processElement2(new StreamRecord<>(53, 15L));
			testHarness.processElement2(new StreamRecord<>(12, 16L));
			testHarness.processElement2(new StreamRecord<>(98, 19L));

			testHarness.processElement1(new StreamRecord<>("trigger", 13L));

			testHarness.processElement2(new StreamRecord<>(51, 21L));

			testHarness.processWatermark1(new Watermark(50L));
			testHarness.processWatermark2(new Watermark(50L));

			Queue<Object> output = testHarness.getOutput();
			Assert.assertEquals(3L, output.size());

			Object firstRawWm = output.poll();
			Assert.assertTrue(firstRawWm instanceof Watermark);
			Watermark firstWm = (Watermark) firstRawWm;
			Assert.assertEquals(10L, firstWm.getTimestamp());

			Object rawOutputElem = output.poll();
			Assert.assertTrue(rawOutputElem instanceof StreamRecord);
			StreamRecord<?> outputRec = (StreamRecord<?>) rawOutputElem;
			Assert.assertTrue(outputRec.getValue() instanceof String);
			String outputElem = (String) outputRec.getValue();

			expectedBroadcastState.put("51.key", 51);
			List<Map.Entry<String, Integer>> expectedEntries = new ArrayList<>();
			expectedEntries.addAll(expectedBroadcastState.entrySet());
			String expected = "TS:41 " + mapToString(expectedEntries);
			Assert.assertEquals(expected, outputElem);

			Object secondRawWm = output.poll();
			Assert.assertTrue(secondRawWm instanceof Watermark);
			Watermark secondWm = (Watermark) secondRawWm;
			Assert.assertEquals(50L, secondWm.getTimestamp());
		}
	}

	private static class FunctionWithBroadcastState extends KeyedBroadcastProcessFunction<String, String, Integer, String> {

		private static final long serialVersionUID = 7496674620398203933L;

		private final String keyPostfix;
		private final Map<String, Integer> expectedBroadcastState;
		private final long timerTs;

		FunctionWithBroadcastState(
				final String keyPostfix,
				final Map<String, Integer> expectedBroadcastState,
				final long timerTs
		) {
			this.keyPostfix = Preconditions.checkNotNull(keyPostfix);
			this.expectedBroadcastState = Preconditions.checkNotNull(expectedBroadcastState);
			this.timerTs = timerTs;
		}

		@Override
		public void processBroadcastElement(Integer value, KeyedContext ctx, Collector<String> out) throws Exception {
			// put an element in the broadcast state
			final String key = value + "." + keyPostfix;
			ctx.getBroadcastState(STATE_DESCRIPTOR).put(key, value);
		}

		@Override
		public void processElement(String value, KeyedReadOnlyContext ctx, Collector<String> out) throws Exception {
			Iterable<Map.Entry<String, Integer>> broadcastStateIt = ctx.getBroadcastState(STATE_DESCRIPTOR).immutableEntries();
			Iterator<Map.Entry<String, Integer>> iter = broadcastStateIt.iterator();

			for (int i = 0; i < expectedBroadcastState.size(); i++) {
				Assert.assertTrue(iter.hasNext());

				Map.Entry<String, Integer> entry = iter.next();
				Assert.assertTrue(expectedBroadcastState.containsKey(entry.getKey()));
				Assert.assertEquals(expectedBroadcastState.get(entry.getKey()), entry.getValue());
			}

			Assert.assertFalse(iter.hasNext());

			ctx.timerService().registerEventTimeTimer(timerTs);
		}

		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
			final Iterator<Map.Entry<String, Integer>> iter = ctx.getBroadcastState(STATE_DESCRIPTOR).immutableEntries().iterator();

			final List<Map.Entry<String, Integer>> map = new ArrayList<>();
			while (iter.hasNext()) {
				map.add(iter.next());
			}
			final String mapToStr = mapToString(map);
			out.collect("TS:" + timestamp + " " + mapToStr);
		}
	}

	@Test
	public void testScaleUp() throws Exception {
		final Set<String> keysToRegister = new HashSet<>();
		keysToRegister.add("test1");
		keysToRegister.add("test2");
		keysToRegister.add("test3");

		final OperatorSubtaskState mergedSnapshot;

		try (
				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness1 = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new TestFunctionWithOutput(keysToRegister),
						10,
						2,
						0);

				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness2 = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new TestFunctionWithOutput(keysToRegister),
						10,
						2,
						1)

		) {

			// make sure all operators have the same state
			testHarness1.processElement2(new StreamRecord<>(3));
			testHarness2.processElement2(new StreamRecord<>(3));

			mergedSnapshot = AbstractStreamOperatorTestHarness.repackageState(
					testHarness1.snapshot(0L, 0L),
					testHarness2.snapshot(0L, 0L)
			);
		}

		final Set<String> expected = new HashSet<>(3);
		expected.add("test1=3");
		expected.add("test2=3");
		expected.add("test3=3");

		try (
				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness1 = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new TestFunctionWithOutput(keysToRegister),
						10,
						3,
						0,
						mergedSnapshot);

				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness2 = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new TestFunctionWithOutput(keysToRegister),
						10,
						3,
						1,
						mergedSnapshot);

				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness3 = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new TestFunctionWithOutput(keysToRegister),
						10,
						3,
						2,
						mergedSnapshot)
		) {
			testHarness1.processElement1(new StreamRecord<>("trigger"));
			testHarness2.processElement1(new StreamRecord<>("trigger"));
			testHarness3.processElement1(new StreamRecord<>("trigger"));

			Queue<?> output1 = testHarness1.getOutput();
			Queue<?> output2 = testHarness2.getOutput();
			Queue<?> output3 = testHarness3.getOutput();

			Assert.assertEquals(expected.size(), output1.size());
			for (Object o: output1) {
				StreamRecord<String> rec = (StreamRecord<String>) o;
				Assert.assertTrue(expected.contains(rec.getValue()));
			}

			Assert.assertEquals(expected.size(), output2.size());
			for (Object o: output2) {
				StreamRecord<String> rec = (StreamRecord<String>) o;
				Assert.assertTrue(expected.contains(rec.getValue()));
			}

			Assert.assertEquals(expected.size(), output3.size());
			for (Object o: output3) {
				StreamRecord<String> rec = (StreamRecord<String>) o;
				Assert.assertTrue(expected.contains(rec.getValue()));
			}
		}
	}

	@Test
	public void testScaleDown() throws Exception {
		final Set<String> keysToRegister = new HashSet<>();
		keysToRegister.add("test1");
		keysToRegister.add("test2");
		keysToRegister.add("test3");

		final OperatorSubtaskState mergedSnapshot;

		try (
				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness1 = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new TestFunctionWithOutput(keysToRegister),
						10,
						3,
						0);

				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness2 = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new TestFunctionWithOutput(keysToRegister),
						10,
						3,
						1);

				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness3 = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new TestFunctionWithOutput(keysToRegister),
						10,
						3,
						2)
		) {

			// make sure all operators have the same state
			testHarness1.processElement2(new StreamRecord<>(3));
			testHarness2.processElement2(new StreamRecord<>(3));
			testHarness3.processElement2(new StreamRecord<>(3));

			mergedSnapshot = AbstractStreamOperatorTestHarness.repackageState(
					testHarness1.snapshot(0L, 0L),
					testHarness2.snapshot(0L, 0L),
					testHarness3.snapshot(0L, 0L)
			);
		}

		final Set<String> expected = new HashSet<>(3);
		expected.add("test1=3");
		expected.add("test2=3");
		expected.add("test3=3");

		try (
				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness1 = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new TestFunctionWithOutput(keysToRegister),
						10,
						2,
						0,
						mergedSnapshot);

				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness2 = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new TestFunctionWithOutput(keysToRegister),
						10,
						2,
						1,
						mergedSnapshot)
		) {

			testHarness1.processElement1(new StreamRecord<>("trigger"));
			testHarness2.processElement1(new StreamRecord<>("trigger"));

			Queue<?> output1 = testHarness1.getOutput();
			Queue<?> output2 = testHarness2.getOutput();

			Assert.assertEquals(expected.size(), output1.size());
			for (Object o: output1) {
				StreamRecord<String> rec = (StreamRecord<String>) o;
				Assert.assertTrue(expected.contains(rec.getValue()));
			}

			Assert.assertEquals(expected.size(), output2.size());
			for (Object o: output2) {
				StreamRecord<String> rec = (StreamRecord<String>) o;
				Assert.assertTrue(expected.contains(rec.getValue()));
			}
		}
	}

	private static class TestFunctionWithOutput extends KeyedBroadcastProcessFunction<String, String, Integer, String> {

		private static final long serialVersionUID = 7496674620398203933L;

		private final Set<String> keysToRegister;

		TestFunctionWithOutput(Set<String> keysToRegister) {
			this.keysToRegister = Preconditions.checkNotNull(keysToRegister);
		}

		@Override
		public void processBroadcastElement(Integer value, KeyedContext ctx, Collector<String> out) throws Exception {
			// put an element in the broadcast state
			for (String k : keysToRegister) {
				ctx.getBroadcastState(STATE_DESCRIPTOR).put(k, value);
			}
		}

		@Override
		public void processElement(String value, KeyedReadOnlyContext ctx, Collector<String> out) throws Exception {
			for (Map.Entry<String, Integer> entry : ctx.getBroadcastState(STATE_DESCRIPTOR).immutableEntries()) {
				out.collect(entry.toString());
			}
		}
	}

	@Test
	public void testNoKeyedStateOnBroadcastSide() throws Exception {

		boolean exceptionThrown = false;

		try (
				TwoInputStreamOperatorTestHarness<String, Integer, String> testHarness = getInitializedTestHarness(
						BasicTypeInfo.STRING_TYPE_INFO,
						new IdentityKeySelector<>(),
						new KeyedBroadcastProcessFunction<String, String, Integer, String>() {

							private static final long serialVersionUID = -1725365436500098384L;

							private final ValueStateDescriptor<String> valueState = new ValueStateDescriptor<>("any", BasicTypeInfo.STRING_TYPE_INFO);

							@Override
							public void processBroadcastElement(Integer value, KeyedContext ctx, Collector<String> out) throws Exception {
								getRuntimeContext().getState(valueState).value(); // this should fail
							}

							@Override
							public void processElement(String value, KeyedReadOnlyContext ctx, Collector<String> out) throws Exception {
								// do nothing
							}
						})
		) {
			testHarness.processWatermark1(new Watermark(10L));
			testHarness.processWatermark2(new Watermark(10L));
			testHarness.processElement2(new StreamRecord<>(5, 12L));
		} catch (NullPointerException e) {
			Assert.assertEquals("No key set. This method should not be called outside of a keyed context.", e.getMessage());
			exceptionThrown = true;
		}

		if (!exceptionThrown) {
			Assert.fail("No exception thrown");
		}
	}

	private static class IdentityKeySelector<T> implements KeySelector<T, T> {
		private static final long serialVersionUID = 1L;

		@Override
		public T getKey(T value) throws Exception {
			return value;
		}
	}

	private static <KEY, IN1, IN2, OUT> TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> getInitializedTestHarness(
			final TypeInformation<KEY> keyTypeInfo,
			final KeySelector<IN1, KEY> keyKeySelector,
			final KeyedBroadcastProcessFunction<KEY, IN1, IN2, OUT> function) throws Exception {

		return getInitializedTestHarness(
				keyTypeInfo,
				keyKeySelector,
				function,
				1,
				1,
				0);
	}

	private static <KEY, IN1, IN2, OUT> TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> getInitializedTestHarness(
			final TypeInformation<KEY> keyTypeInfo,
			final KeySelector<IN1, KEY> keyKeySelector,
			final KeyedBroadcastProcessFunction<KEY, IN1, IN2, OUT> function,
			final int maxParallelism,
			final int numTasks,
			final int taskIdx) throws Exception {

		return getInitializedTestHarness(
				keyTypeInfo,
				keyKeySelector,
				function,
				maxParallelism,
				numTasks,
				taskIdx,
				null);
	}

	private static <KEY, IN1, IN2, OUT> TwoInputStreamOperatorTestHarness<IN1, IN2, OUT> getInitializedTestHarness(
			final TypeInformation<KEY> keyTypeInfo,
			final KeySelector<IN1, KEY> keyKeySelector,
			final KeyedBroadcastProcessFunction<KEY, IN1, IN2, OUT> function,
			final int maxParallelism,
			final int numTasks,
			final int taskIdx,
			final OperatorSubtaskState initState) throws Exception {

		final TwoInputStreamOperatorTestHarness<IN1, IN2, OUT>  testHarness =
				new KeyedTwoInputStreamOperatorTestHarness<>(
						new CoBroadcastWithKeyedOperator<>(
								Preconditions.checkNotNull(function),
								Collections.singletonList(STATE_DESCRIPTOR)),
						keyKeySelector,
						null,
						keyTypeInfo,
						maxParallelism,
						numTasks,
						taskIdx
				);

		testHarness.setup();
		testHarness.initializeState(initState);
		testHarness.open();

		return testHarness;
	}

	private static String mapToString(List<Map.Entry<String, Integer>> entries) {
		entries.sort(
				Comparator.comparing(
						(Function<Map.Entry<String, Integer>, String>) Map.Entry::getKey
				).thenComparingInt(Map.Entry::getValue)
		);

		final StringBuilder builder = new StringBuilder();
		for (Map.Entry<String, Integer> entry : entries) {
			builder.append(' ')
					.append(entry.getKey())
					.append('=')
					.append(entry.getValue());
		}
		return builder.toString();
	}
}
