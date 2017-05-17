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

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AppendingState;
import org.apache.flink.api.common.state.FoldingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.util.OutputTag;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * These tests verify that {@link WindowOperator} correctly interacts with the other windowing
 * components: {@link WindowAssigner},
 * {@link Trigger}.
 * {@link org.apache.flink.streaming.api.functions.windowing.WindowFunction} and window state.
 *
 * <p>These tests document the implicit contract that exists between the windowing components.
 */
public class RegularWindowOperatorContractTest extends WindowOperatorContractTest {

	@Test
	public void testReducingWindow() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
		InternalWindowFunction<Integer, Void, Integer, TimeWindow> mockWindowFunction = mockWindowFunction();

		ReducingStateDescriptor<Integer> intReduceSumDescriptor =
				new ReducingStateDescriptor<>(
						"int-reduce",
						new ReduceFunction<Integer>() {
							private static final long serialVersionUID = 1L;

							@Override
							public Integer reduce(Integer a, Integer b) throws Exception {
								return a + b;
							}
						},
						IntSerializer.INSTANCE);

		final ValueStateDescriptor<String> valueStateDescriptor =
				new ValueStateDescriptor<>("string-state", StringSerializer.INSTANCE);

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
				createWindowOperator(mockAssigner, mockTrigger, 0L, intReduceSumDescriptor, mockWindowFunction);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		// insert two elements without firing
		testHarness.processElement(new StreamRecord<>(1, 0L));
		testHarness.processElement(new StreamRecord<>(1, 0L));

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				TimeWindow window = (TimeWindow) invocation.getArguments()[2];
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				context.registerEventTimeTimer(window.getEnd());
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.FIRE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(1, 0L));

		verify(mockWindowFunction, times(2)).process(eq(1), anyTimeWindow(), anyInternalWindowContext(), anyInt(), WindowOperatorContractTest.<Void>anyCollector());
		verify(mockWindowFunction, times(1)).process(eq(1), eq(new TimeWindow(0, 2)), anyInternalWindowContext(), eq(3), WindowOperatorContractTest.<Void>anyCollector());
		verify(mockWindowFunction, times(1)).process(eq(1), eq(new TimeWindow(2, 4)), anyInternalWindowContext(), eq(3), WindowOperatorContractTest.<Void>anyCollector());

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// FIRE should not purge contents
		assertEquals(4, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(4, testHarness.numEventTimeTimers()); // window timers/gc timers
	}

	@Test
	public void testFoldingWindow() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
		InternalWindowFunction<Integer, Void, Integer, TimeWindow> mockWindowFunction = mockWindowFunction();

		FoldingStateDescriptor<Integer, Integer> intFoldSumDescriptor =
			new FoldingStateDescriptor<>(
					"int-fold",
					0,
					new FoldFunction<Integer, Integer>() {
						private static final long serialVersionUID = 1L;

						@Override
						public Integer fold(Integer accumulator, Integer value) throws Exception {
							return accumulator + value;
						}
					},
					IntSerializer.INSTANCE);

		final ValueStateDescriptor<String> valueStateDescriptor =
				new ValueStateDescriptor<>("string-state", StringSerializer.INSTANCE);

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
				createWindowOperator(mockAssigner, mockTrigger, 0L, intFoldSumDescriptor, mockWindowFunction);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		// insert two elements without firing
		testHarness.processElement(new StreamRecord<>(1, 0L));
		testHarness.processElement(new StreamRecord<>(1, 0L));

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				TimeWindow window = (TimeWindow) invocation.getArguments()[2];
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				context.registerEventTimeTimer(window.getEnd());
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.FIRE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(1, 0L));

		verify(mockWindowFunction, times(2)).process(eq(1), anyTimeWindow(), anyInternalWindowContext(), anyInt(), WindowOperatorContractTest.<Void>anyCollector());
		verify(mockWindowFunction, times(1)).process(eq(1), eq(new TimeWindow(0, 2)), anyInternalWindowContext(), eq(3), WindowOperatorContractTest.<Void>anyCollector());
		verify(mockWindowFunction, times(1)).process(eq(1), eq(new TimeWindow(2, 4)), anyInternalWindowContext(), eq(3), WindowOperatorContractTest.<Void>anyCollector());

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// FIRE should not purge contents
		assertEquals(4, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(4, testHarness.numEventTimeTimers()); // window timers/gc timers
	}

	/**
	 * Special method for creating a {@link WindowOperator} with a custom {@link StateDescriptor}
	 * for the window contents state.
	 */
	private <W extends Window, ACC, OUT> KeyedOneInputStreamOperatorTestHarness<Integer, Integer, OUT> createWindowOperator(
			WindowAssigner<Integer, W> assigner,
			Trigger<Integer, W> trigger,
			long allowedLatenss,
			StateDescriptor<? extends AppendingState<Integer, ACC>, ?> stateDescriptor,
			InternalWindowFunction<ACC, OUT, Integer, W> windowFunction) throws Exception {

		KeySelector<Integer, Integer> keySelector = new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		};

		@SuppressWarnings("unchecked")
		WindowOperator<Integer, Integer, ACC, OUT, W> operator = new WindowOperator<>(
				assigner,
				assigner.getWindowSerializer(new ExecutionConfig()),
				keySelector,
				IntSerializer.INSTANCE,
				stateDescriptor,
				windowFunction,
				trigger,
				allowedLatenss,
				null /* late output tag */);

		return new KeyedOneInputStreamOperatorTestHarness<>(
				operator,
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);
	}

	@Override
	protected <W extends Window, OUT> KeyedOneInputStreamOperatorTestHarness<Integer, Integer, OUT> createWindowOperator(
			WindowAssigner<Integer, W> assigner,
			Trigger<Integer, W> trigger,
			long allowedLatenss,
			InternalWindowFunction<Iterable<Integer>, OUT, Integer, W> windowFunction,
			OutputTag<Integer> lateOutputTag) throws Exception {

		KeySelector<Integer, Integer> keySelector = new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		};

		ListStateDescriptor<Integer> intListDescriptor =
				new ListStateDescriptor<>("int-list", IntSerializer.INSTANCE);

		@SuppressWarnings("unchecked")
		WindowOperator<Integer, Integer, Iterable<Integer>, OUT, W> operator = new WindowOperator<>(
				assigner,
				assigner.getWindowSerializer(new ExecutionConfig()),
				keySelector,
				IntSerializer.INSTANCE,
				intListDescriptor,
				windowFunction,
				trigger,
				allowedLatenss,
				lateOutputTag);

		return new KeyedOneInputStreamOperatorTestHarness<>(
				operator,
				keySelector,
				BasicTypeInfo.INT_TYPE_INFO);
	}

	@Override
	protected <W extends Window, OUT> KeyedOneInputStreamOperatorTestHarness<Integer, Integer, OUT> createWindowOperator(
			WindowAssigner<Integer, W> assigner,
			Trigger<Integer, W> trigger,
			long allowedLatenss,
			InternalWindowFunction<Iterable<Integer>, OUT, Integer, W> windowFunction) throws Exception {

		return createWindowOperator(
				assigner,
				trigger,
				allowedLatenss,
				windowFunction,
				null /* late output tag */);
	}
}
