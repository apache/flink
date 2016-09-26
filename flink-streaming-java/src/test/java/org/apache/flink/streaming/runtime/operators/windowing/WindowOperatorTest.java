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


import com.google.common.collect.Lists;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalIterableWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.streaming.runtime.operators.windowing.StreamRecordMatchers.isWindowedValue;
import static org.apache.flink.streaming.runtime.operators.windowing.StreamRecordMatchers.timeWindow;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.*;

/**
 * Unit tests that verify that {@link WindowOperator} correctly interacts with the other windowing
 * components: {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner},
 * {@link org.apache.flink.streaming.api.windowing.triggers.Trigger}.
 * {@link org.apache.flink.streaming.api.functions.windowing.WindowFunction} and window state.
 *
 * <p>These tests document the implicit contract that exists between the windowing components.
 */
public class WindowOperatorTest extends TestLogger {

	private static ValueStateDescriptor<String> valueStateDescriptor =
			new ValueStateDescriptor<>("string-state", StringSerializer.INSTANCE, null);

	private static <T, W extends Window> Trigger<T, W> mockTrigger() throws Exception {
		@SuppressWarnings("unchecked")
		Trigger<T, W> mockTrigger = mock(Trigger.class);

		when(mockTrigger.onElement(Matchers.<T>any(), anyLong(), Matchers.<W>any(), anyTriggerContext())).thenReturn(TriggerResult.CONTINUE);
		when(mockTrigger.onEventTime(anyLong(), Matchers.<W>any(), anyTriggerContext())).thenReturn(TriggerResult.CONTINUE);
		when(mockTrigger.onProcessingTime(anyLong(), Matchers.<W>any(), anyTriggerContext())).thenReturn(TriggerResult.CONTINUE);

		return mockTrigger;
	}

	private static <T> WindowAssigner<T, TimeWindow> mockTimeWindowAssigner() throws Exception {
		@SuppressWarnings("unchecked")
		WindowAssigner<T, TimeWindow> mockAssigner = mock(WindowAssigner.class);

		when(mockAssigner.getWindowSerializer(Mockito.<ExecutionConfig>any())).thenReturn(new TimeWindow.Serializer());
		when(mockAssigner.isEventTime()).thenReturn(true);

		return mockAssigner;
	}

	private static <T> WindowAssigner<T, GlobalWindow> mockGlobalWindowAssigner() throws Exception {
		@SuppressWarnings("unchecked")
		WindowAssigner<T, GlobalWindow> mockAssigner = mock(WindowAssigner.class);

		when(mockAssigner.getWindowSerializer(Mockito.<ExecutionConfig>any())).thenReturn(new GlobalWindow.Serializer());
		when(mockAssigner.isEventTime()).thenReturn(true);
		when(mockAssigner.assignWindows(Mockito.<T>any(), anyLong(), anyAssignerContext())).thenReturn(Collections.singletonList(GlobalWindow.get()));

		return mockAssigner;
	}


	private static <T> MergingWindowAssigner<T, TimeWindow> mockMergingAssigner() throws Exception {
		@SuppressWarnings("unchecked")
		MergingWindowAssigner<T, TimeWindow> mockAssigner = mock(MergingWindowAssigner.class);

		when(mockAssigner.getWindowSerializer(Mockito.<ExecutionConfig>any())).thenReturn(new TimeWindow.Serializer());
		when(mockAssigner.isEventTime()).thenReturn(true);

		return mockAssigner;
	}


	private static WindowAssigner.WindowAssignerContext anyAssignerContext() {
		return Mockito.any();
	}

	public static Trigger.TriggerContext anyTriggerContext() {
		return Mockito.any();
	}

	public static TimeWindow anyTimeWindow() {
		return Mockito.any();
	}

	public static Trigger.OnMergeContext anyOnMergeContext() {
		return Mockito.any();
	}

	public static MergingWindowAssigner.MergeCallback anyMergeCallback() {
		return Mockito.any();
	}


	private static <T> void shouldRegisterEventTimeTimerOnElement(Trigger<T, TimeWindow> mockTrigger, final long timestamp) throws Exception {
		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				@SuppressWarnings("unchecked")
				Trigger.TriggerContext context =
						(Trigger.TriggerContext) invocation.getArguments()[3];
				context.registerEventTimeTimer(timestamp);
				return TriggerResult.CONTINUE;
			}
		})
		.when(mockTrigger).onElement(Matchers.<T>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());
	}

	private static <T> void shouldDeleteEventTimeTimerOnElement(Trigger<T, TimeWindow> mockTrigger, final long timestamp) throws Exception {
		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				@SuppressWarnings("unchecked")
				Trigger.TriggerContext context =
						(Trigger.TriggerContext) invocation.getArguments()[3];
				context.deleteEventTimeTimer(timestamp);
				return TriggerResult.CONTINUE;
			}
		})
		.when(mockTrigger).onElement(Matchers.<T>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());
	}

	private static <T> void shouldRegisterProcessingTimeTimer(Trigger<T, TimeWindow> mockTrigger, final long timestamp) throws Exception {
		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				@SuppressWarnings("unchecked")
				Trigger.TriggerContext context =
						(Trigger.TriggerContext) invocation.getArguments()[3];
				context.registerProcessingTimeTimer(timestamp);
				return TriggerResult.CONTINUE;
			}
		})
				.when(mockTrigger).onElement(Matchers.<T>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());
	}

	private static <T> void shouldDeleteProcessingTimeTimer(Trigger<T, TimeWindow> mockTrigger, final long timestamp) throws Exception {
		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				@SuppressWarnings("unchecked")
				Trigger.TriggerContext context =
						(Trigger.TriggerContext) invocation.getArguments()[3];
				context.deleteProcessingTimeTimer(timestamp);
				return TriggerResult.CONTINUE;
			}
		})
				.when(mockTrigger).onElement(Matchers.<T>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());
	}

	private static <T, W extends Window> void shouldMergeWindows(final MergingWindowAssigner<T, W> assigner, final Collection<? extends W> expectedWindows, final Collection<W> toMerge, final W mergeResult) {
		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Exception {
				@SuppressWarnings("unchecked")
				Collection<W> windows = (Collection<W>) invocation.getArguments()[0];
				MergingWindowAssigner.MergeCallback callback = (MergingWindowAssigner.MergeCallback) invocation.getArguments()[1];

				// verify the expected windows
				assertThat(windows, containsInAnyOrder(expectedWindows.toArray()));

				callback.merge(toMerge, mergeResult);
				return null;
			}
		})
				.when(assigner).mergeWindows(anyCollection(), Matchers.<MergingWindowAssigner.MergeCallback>anyObject());
	}

	private static <T> void shouldContinueOnElement(Trigger<T, TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onElement(Matchers.<T>anyObject(), anyLong(), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.CONTINUE);
	}

	private static <T> void shouldFireOnElement(Trigger<T, TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onElement(Matchers.<T>anyObject(), anyLong(), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.FIRE);
	}

	private static <T> void shouldPurgeOnElement(Trigger<T, TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onElement(Matchers.<T>anyObject(), anyLong(), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.PURGE);
	}

	private static <T> void shouldFireAndPurgeOnElement(Trigger<T, TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onElement(Matchers.<T>anyObject(), anyLong(), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.FIRE_AND_PURGE);
	}

	private static <T> void shouldContinueOnEventTime(Trigger<T, TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onEventTime(anyLong(), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.CONTINUE);
	}

	private static <T> void shouldFireOnEventTime(Trigger<T, TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onEventTime(anyLong(), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.FIRE);
	}

	private static <T> void shouldPurgeOnEventTime(Trigger<T, TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onEventTime(anyLong(), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.PURGE);
	}

	private static <T> void shouldFireAndPurgeOnEventTime(Trigger<T, TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onEventTime(anyLong(), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.FIRE_AND_PURGE);
	}

	private static <T> void shouldContinueOnProcessingTime(Trigger<T, TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onProcessingTime(anyLong(), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.CONTINUE);
	}

	private static <T> void shouldFireOnProcessingTime(Trigger<T, TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onProcessingTime(anyLong(), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.FIRE);
	}

	private static <T> void shouldPurgeOnProcessingTime(Trigger<T, TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onProcessingTime(anyLong(), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.PURGE);
	}

	private static <T> void shouldFireAndPurgeOnProcessingTime(Trigger<T, TimeWindow> mockTrigger) throws Exception {
		when(mockTrigger.onProcessingTime(anyLong(), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.FIRE_AND_PURGE);
	}

	@Test
	public void testAssignerIsInvokedOncePerElement() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		OneInputStreamOperatorTestHarness<Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Collections.singletonList(new TimeWindow(0, 0)));

		testHarness.processElement(new StreamRecord<>(0, 0L));

		verify(mockAssigner, times(1)).assignWindows(eq(0), eq(0L), anyAssignerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		verify(mockAssigner, times(2)).assignWindows(eq(0), eq(0L), anyAssignerContext());

	}

	@Test
	public void testAssignerWithMultipleWindows() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		OneInputStreamOperatorTestHarness<Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		shouldFireOnElement(mockTrigger);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertThat(testHarness.extractOutputStreamRecords(),
				containsInAnyOrder(
						isWindowedValue(contains(0), 1L, timeWindow(0, 2)),
						isWindowedValue(contains(0), 3L, timeWindow(2, 4))));

	}

	@Test
	public void testWindowsDontInterfere() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Collections.singletonList(new TimeWindow(0, 2)));

		testHarness.processElement(new StreamRecord<>(0, 0L));

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Collections.singletonList(new TimeWindow(0, 1)));

		testHarness.processElement(new StreamRecord<>(1, 0L));

		// no output so far
		assertTrue(testHarness.extractOutputStreamRecords().isEmpty());

		// state for two windows
		assertEquals(2, testHarness.numKeyedStateEntries());
		assertEquals(2, testHarness.numEventTimeTimers());

		// now we fire
		shouldFireOnElement(mockTrigger);

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Collections.singletonList(new TimeWindow(0, 1)));

		testHarness.processElement(new StreamRecord<>(1, 0L));

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Collections.singletonList(new TimeWindow(0, 2)));

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertThat(testHarness.extractOutputStreamRecords(),
				containsInAnyOrder(
						isWindowedValue(contains(0, 0), 1L, timeWindow(0, 2)),
						isWindowedValue(contains(1, 1), 0L, timeWindow(0, 1))));
	}

	@Test
	public void testOnElementCalledPerWindow() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		OneInputStreamOperatorTestHarness<Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		testHarness.processElement(new StreamRecord<>(42, 1L));

		verify(mockTrigger).onElement(eq(42), eq(1L), eq(new TimeWindow(2, 4)), anyTriggerContext());
		verify(mockTrigger).onElement(eq(42), eq(1L), eq(new TimeWindow(0, 2)), anyTriggerContext());

		verify(mockTrigger, times(2)).onElement(anyInt(), anyLong(), anyTimeWindow(), anyTriggerContext());
	}

	@Test
	public void testOnElementContinue() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				TimeWindow window = (TimeWindow) invocation.getArguments()[2];
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				context.registerEventTimeTimer(window.getEnd());
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// CONTINUE should not purge contents
		assertEquals(4, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(4, testHarness.numEventTimeTimers()); // window timers/gc timers

		// there should be no firing
		assertEquals(0, testHarness.getOutput().size());
	}

	@Test
	public void testOnElementFire() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

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

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// FIRE should not purge contents
		assertEquals(4, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(4, testHarness.numEventTimeTimers()); // window timers/gc timers

		// there should be two elements now
		assertThat(testHarness.extractOutputStreamRecords(),
				containsInAnyOrder(
						isWindowedValue(contains(0), 1L, timeWindow(0, 2)),
						isWindowedValue(contains(0), 3L, timeWindow(2, 4))));
	}

	@Test
	public void testOnElementFireAndPurge() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				TimeWindow window = (TimeWindow) invocation.getArguments()[2];
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				context.registerEventTimeTimer(window.getEnd());
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.FIRE_AND_PURGE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// FIRE_AND_PURGE should purge contents
		assertEquals(2, testHarness.numKeyedStateEntries()); // trigger state will stick around until GC time

		// timers will stick around
		assertEquals(4, testHarness.numEventTimeTimers());

		// there should be two elements now
		assertThat(testHarness.extractOutputStreamRecords(),
				containsInAnyOrder(
						isWindowedValue(contains(0), 1L, timeWindow(0, 2)),
						isWindowedValue(contains(0), 3L, timeWindow(2, 4))));
	}

	@Test
	public void testOnElementPurge() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				context.registerEventTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.PURGE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// PURGE should purge contents
		assertEquals(2, testHarness.numKeyedStateEntries()); // trigger state will stick around until GC time

		// timers will stick around
		assertEquals(4, testHarness.numEventTimeTimers()); // trigger timer and GC timer

		// no output
		assertEquals(0, testHarness.getOutput().size());
	}

	@Test
	public void testOnEventTimeContinue() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		// this should register two timers because we have two windows
		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// we don't want to fire the cleanup timer
				context.registerEventTimeTimer(0);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		shouldContinueOnEventTime(mockTrigger);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(4, testHarness.numKeyedStateEntries()); // window-contents plus trigger state for two windows
		assertEquals(4, testHarness.numEventTimeTimers()); // timers/gc timers for two windows

		testHarness.processWatermark(new Watermark(0L));

		assertEquals(4, testHarness.numKeyedStateEntries());
		assertEquals(2, testHarness.numEventTimeTimers()); // only gc timers left

		// there should be no firing
		assertEquals(0, testHarness.extractOutputStreamRecords().size());
	}

	@Test
	public void testOnEventTimeFire() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// don't interfere with cleanup timers
				context.registerEventTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		shouldFireOnEventTime(mockTrigger);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(4, testHarness.numKeyedStateEntries()); // window-contents and trigger state for two windows
		assertEquals(4, testHarness.numEventTimeTimers()); // timers/gc timers for two windows

		testHarness.processWatermark(new Watermark(0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// FIRE should not purge contents
		assertEquals(4, testHarness.numKeyedStateEntries());
		assertEquals(2, testHarness.numEventTimeTimers()); // only gc timers left

		// there should be two elements now
		assertThat(testHarness.extractOutputStreamRecords(),
				containsInAnyOrder(
						isWindowedValue(contains(0), 1L, timeWindow(0, 2)),
						isWindowedValue(contains(0), 3L, timeWindow(2, 4))));
	}

	@Test
	public void testOnEventTimeFireAndPurge() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				context.registerEventTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		shouldFireAndPurgeOnEventTime(mockTrigger);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(4, testHarness.numKeyedStateEntries()); // window-contents and trigger state for two windows
		assertEquals(4, testHarness.numEventTimeTimers()); // timers/gc timers for two windows

		testHarness.processWatermark(new Watermark(0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// FIRE_AND_PURGE should purge contents
		assertEquals(2, testHarness.numKeyedStateEntries()); // trigger state stays until GC time
		assertEquals(2, testHarness.numEventTimeTimers()); // gc timers are still there

		// there should be two elements now
		assertThat(testHarness.extractOutputStreamRecords(),
				containsInAnyOrder(
						isWindowedValue(contains(0), 1L, timeWindow(0, 2)),
						isWindowedValue(contains(0), 3L, timeWindow(2, 4))));
	}

	@Test
	public void testOnEventTimePurge() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// don't interfere with cleanup timers
				context.registerEventTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		shouldPurgeOnEventTime(mockTrigger);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(4, testHarness.numKeyedStateEntries()); // window-contents and trigger state for two windows
		assertEquals(4, testHarness.numEventTimeTimers()); // timers/gc timers for two windows

		testHarness.processWatermark(new Watermark(0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// PURGE should purge contents
		assertEquals(2, testHarness.numKeyedStateEntries()); // trigger state will stick around
		assertEquals(2, testHarness.numEventTimeTimers()); // gc timers are still there

		// still no output
		assertEquals(0, testHarness.extractOutputStreamRecords().size());
	}

	@Test
	public void testOnProcessingTimeContinue() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		when(mockAssigner.isEventTime()).thenReturn(false);
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		testHarness.setProcessingTime(Long.MIN_VALUE);

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		// this should register two timers because we have two windows
		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// don't interfere with the cleanup timers
				context.registerProcessingTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(4, testHarness.numKeyedStateEntries()); // window-contents and trigger state for two windows
		assertEquals(4, testHarness.numProcessingTimeTimers()); // timers/gc timers for two windows

		testHarness.setProcessingTime(0L);

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(4, testHarness.numKeyedStateEntries());
		assertEquals(2, testHarness.numProcessingTimeTimers()); // only gc timers left

		// there should be no firing
		assertEquals(0, testHarness.extractOutputStreamRecords().size());
	}

	@Test
	public void testOnProcessingTimeFire() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		when(mockAssigner.isEventTime()).thenReturn(false);
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		testHarness.setProcessingTime(Long.MIN_VALUE);

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				context.registerProcessingTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		shouldFireOnProcessingTime(mockTrigger);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(4, testHarness.numKeyedStateEntries()); // window-contents and trigger state for two windows
		assertEquals(4, testHarness.numProcessingTimeTimers()); // timers/gc timers for two windows

		testHarness.setProcessingTime(0L);

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// FIRE should not purge contents
		assertEquals(4, testHarness.numKeyedStateEntries());
		assertEquals(2, testHarness.numProcessingTimeTimers()); // only gc timers left

		// there should be two elements now
		assertThat(testHarness.extractOutputStreamRecords(),
				containsInAnyOrder(
						isWindowedValue(contains(0), 1L, timeWindow(0, 2)),
						isWindowedValue(contains(0), 3L, timeWindow(2, 4))));
	}

	@Test
	public void testOnProcessingTimeFireAndPurge() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		when(mockAssigner.isEventTime()).thenReturn(false);
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// don't interfere with cleanup timers
				context.registerProcessingTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		shouldFireAndPurgeOnProcessingTime(mockTrigger);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(4, testHarness.numKeyedStateEntries()); // window-contents and trigger state for two windows
		assertEquals(4, testHarness.numProcessingTimeTimers()); // timers/gc timers for two windows

		testHarness.setProcessingTime(0L);

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// FIRE_AND_PURGE should purge contents
		assertEquals(2, testHarness.numKeyedStateEntries());
		assertEquals(2, testHarness.numProcessingTimeTimers()); // gc timers are still there

		// there should be two elements now
		assertThat(testHarness.extractOutputStreamRecords(),
				containsInAnyOrder(
						isWindowedValue(contains(0), 1L, timeWindow(0, 2)),
						isWindowedValue(contains(0), 3L, timeWindow(2, 4))));
	}

	@Test
	public void testOnProcessingTimePurge() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		when(mockAssigner.isEventTime()).thenReturn(false);
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// don't interfere with cleanup timers
				context.registerProcessingTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		shouldPurgeOnProcessingTime(mockTrigger);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(4, testHarness.numKeyedStateEntries()); // window-contents and trigger state for two windows
		assertEquals(4, testHarness.numProcessingTimeTimers()); // timers/gc timers for two windows

		testHarness.setProcessingTime(0L);

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// PURGE should purge contents
		assertEquals(2, testHarness.numKeyedStateEntries()); // trigger state will stick around until cleanup time
		assertEquals(2, testHarness.numProcessingTimeTimers()); // gc timers are still there

		// still no output
		assertEquals(0, testHarness.getOutput().size());
	}


	@Test
	public void testEventTimeTimerCreationAndDeletion() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 2)));

		assertEquals(0, testHarness.numEventTimeTimers());

		shouldRegisterEventTimeTimerOnElement(mockTrigger, 17);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(2, testHarness.numEventTimeTimers()); // +1 because of the GC timer of the window

		shouldRegisterEventTimeTimerOnElement(mockTrigger, 42);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(3, testHarness.numEventTimeTimers()); // +1 because of the GC timer of the window

		shouldDeleteEventTimeTimerOnElement(mockTrigger, 42);
		testHarness.processElement(new StreamRecord<>(0, 0L));
		shouldDeleteEventTimeTimerOnElement(mockTrigger, 17);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(1, testHarness.numEventTimeTimers()); // +1 because of the GC timer of the window
	}

	@Test
	public void testEventTimeTimerFiring() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 100)));

		assertEquals(0, testHarness.numEventTimeTimers());

		shouldRegisterEventTimeTimerOnElement(mockTrigger, 1);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		shouldRegisterEventTimeTimerOnElement(mockTrigger, 17);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		shouldRegisterEventTimeTimerOnElement(mockTrigger, 42);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(4, testHarness.numEventTimeTimers()); // +1 because of the GC timer of the window

		testHarness.processWatermark(new Watermark(1));

		verify(mockTrigger).onEventTime(eq(1L), eq(new TimeWindow(0, 100)), anyTriggerContext());
		verify(mockTrigger, times(1)).onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext());
		assertEquals(3, testHarness.numEventTimeTimers()); // +1 because of the GC timer of the window

		// doesn't do anything
		testHarness.processWatermark(new Watermark(15));
		verify(mockTrigger, times(1)).onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext());

		testHarness.processWatermark(new Watermark(42));
		verify(mockTrigger).onEventTime(eq(17L), eq(new TimeWindow(0, 100)), anyTriggerContext());
		verify(mockTrigger).onEventTime(eq(42L), eq(new TimeWindow(0, 100)), anyTriggerContext());
		verify(mockTrigger, times(3)).onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext());
		assertEquals(1, testHarness.numEventTimeTimers()); // +1 because of the GC timer of the window
	}

	@Test
	public void testEventTimeDeletedTimerDoesNotFire() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 100)));

		assertEquals(0, testHarness.numEventTimeTimers());

		shouldRegisterEventTimeTimerOnElement(mockTrigger, 1);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		shouldDeleteEventTimeTimerOnElement(mockTrigger, 1);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		shouldRegisterEventTimeTimerOnElement(mockTrigger, 2);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		testHarness.processWatermark(new Watermark(50));

		verify(mockTrigger, times(0)).onEventTime(eq(1L), anyTimeWindow(), anyTriggerContext());
		verify(mockTrigger, times(1)).onEventTime(eq(2L), eq(new TimeWindow(0, 100)), anyTriggerContext());
		verify(mockTrigger, times(1)).onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext());
	}

	@Test
	public void testProcessingTimeTimerCreationAndDeletion() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 2)));

		when(mockAssigner.isEventTime()).thenReturn(false);

		assertEquals(0, testHarness.numProcessingTimeTimers());

		shouldRegisterProcessingTimeTimer(mockTrigger, 17);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(2, testHarness.numProcessingTimeTimers()); // +1 because of the GC timer of the window

		shouldRegisterProcessingTimeTimer(mockTrigger, 42);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(3, testHarness.numProcessingTimeTimers()); // +1 because of the GC timer of the window

		shouldDeleteProcessingTimeTimer(mockTrigger, 42);
		testHarness.processElement(new StreamRecord<>(0, 0L));
		shouldDeleteProcessingTimeTimer(mockTrigger, 17);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(1, testHarness.numProcessingTimeTimers()); // +1 because of the GC timer of the window
	}

	@Test
	public void testProcessingTimeTimerFiring() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 100)));

		when(mockAssigner.isEventTime()).thenReturn(false);

		assertEquals(0, testHarness.numProcessingTimeTimers());

		shouldRegisterProcessingTimeTimer(mockTrigger, 1);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		shouldRegisterProcessingTimeTimer(mockTrigger, 17);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		shouldRegisterProcessingTimeTimer(mockTrigger, 42);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(4, testHarness.numProcessingTimeTimers()); // +1 because of the GC timer of the window

		testHarness.setProcessingTime(1L);

		verify(mockTrigger).onProcessingTime(eq(1L), eq(new TimeWindow(0, 100)), anyTriggerContext());
		verify(mockTrigger, times(1)).onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext());
		assertEquals(3, testHarness.numProcessingTimeTimers()); // +1 because of the GC timer of the window

		// doesn't do anything
		testHarness.setProcessingTime(15L);
		verify(mockTrigger, times(1)).onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext());

		testHarness.setProcessingTime(42L);
		verify(mockTrigger).onProcessingTime(eq(17L), eq(new TimeWindow(0, 100)), anyTriggerContext());
		verify(mockTrigger).onProcessingTime(eq(42L), eq(new TimeWindow(0, 100)), anyTriggerContext());
		verify(mockTrigger, times(3)).onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext());
		assertEquals(1, testHarness.numProcessingTimeTimers()); // +1 because of the GC timer of the window
	}

	@Test
	public void testProcessingTimeDeletedTimerDoesNotFire() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 100)));

		when(mockAssigner.isEventTime()).thenReturn(false);

		assertEquals(0, testHarness.numProcessingTimeTimers());

		shouldRegisterProcessingTimeTimer(mockTrigger, 1);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		shouldDeleteProcessingTimeTimer(mockTrigger, 1);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		shouldRegisterProcessingTimeTimer(mockTrigger, 2);
		testHarness.processElement(new StreamRecord<>(0, 0L));

		testHarness.setProcessingTime(50L);

		verify(mockTrigger, times(0)).onProcessingTime(eq(1L), anyTimeWindow(), anyTriggerContext());
		verify(mockTrigger, times(1)).onProcessingTime(eq(2L), eq(new TimeWindow(0, 100)), anyTriggerContext());
		verify(mockTrigger, times(1)).onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext());
	}

	@Test
	public void testMergeWindowsIsCalled() throws Exception {

		MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();

		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		verify(mockAssigner).mergeWindows(eq(Lists.newArrayList(new TimeWindow(2, 4))), anyMergeCallback());
		verify(mockAssigner).mergeWindows(eq(Lists.newArrayList(new TimeWindow(2, 4), new TimeWindow(0, 2))), anyMergeCallback());
		verify(mockAssigner, times(2)).mergeWindows(anyCollection(), anyMergeCallback());


	}

	@Test
	public void testWindowsAreMergedEagerly() throws Exception {

		MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();

		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// don't intefere with cleanup timers
				context.registerEventTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.OnMergeContext context = (Trigger.OnMergeContext) invocation.getArguments()[1];
				// don't intefere with cleanup timers
				context.registerEventTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onMerge(anyTimeWindow(), anyOnMergeContext());

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[1];
				context.deleteEventTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).clear();
				return null;
			}
		}).when(mockTrigger).clear(anyTimeWindow(), anyTriggerContext());

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 2)));

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(3, testHarness.numKeyedStateEntries()); // window state plus trigger state plus merging window set
		assertEquals(2, testHarness.numEventTimeTimers()); // timer and GC timer

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4)));

		shouldMergeWindows(
				mockAssigner,
				Lists.newArrayList(new TimeWindow(0, 2), new TimeWindow(2, 4)),
				Lists.newArrayList(new TimeWindow(0, 2), new TimeWindow(2, 4)),
				new TimeWindow(0, 4));

		// don't register a timer or update state in onElement, this checks
		// whether onMerge does correctly set those things
		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());


		testHarness.processElement(new StreamRecord<>(0, 0L));

		verify(mockTrigger).onMerge(eq(new TimeWindow(0, 4)), anyOnMergeContext());

		assertEquals(3, testHarness.numKeyedStateEntries());
		assertEquals(2, testHarness.numEventTimeTimers());
	}

	@Test
	public void testMergingOfExistingWindows() throws Exception {
		// this test verifies that we only keep one of the underlying state windows
		// after a merge

		MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();

		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// don't interfere with cleanup timers
				context.registerEventTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.OnMergeContext context = (Trigger.OnMergeContext) invocation.getArguments()[1];
				// don't interfere with cleanup timers
				context.registerEventTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onMerge(anyTimeWindow(), anyOnMergeContext());

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[1];
				// don't interfere with cleanup timers
				context.deleteEventTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).clear();
				return null;
			}
		}).when(mockTrigger).clear(anyTimeWindow(), anyTriggerContext());

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 2)));

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(3, testHarness.numKeyedStateEntries()); // window state plus trigger state plus merging window set
		assertEquals(2, testHarness.numEventTimeTimers()); // trigger timer plus GC timer

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4)));

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(5, testHarness.numKeyedStateEntries()); // window state plus trigger state plus merging window set
		assertEquals(4, testHarness.numEventTimeTimers()); // trigger timer plus GC timer

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(1, 3)));

		shouldMergeWindows(
				mockAssigner,
				Lists.newArrayList(new TimeWindow(0, 2), new TimeWindow(2, 4), new TimeWindow(1, 3)),
				Lists.newArrayList(new TimeWindow(0, 2), new TimeWindow(2, 4), new TimeWindow(1, 3)),
				new TimeWindow(0, 4));

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(3, testHarness.numKeyedStateEntries()); // window contents plus trigger state plus merging window set
		assertEquals(2, testHarness.numEventTimeTimers()); // trigger timer plus GC timer

		assertEquals(0, testHarness.getOutput().size());
	}

	@Test
	public void testOnElementPurgeDoesNotCleanupMergingSet() throws Exception {

		MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				return TriggerResult.PURGE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(1, testHarness.numKeyedStateEntries()); // the merging window set

		assertEquals(1, testHarness.numEventTimeTimers()); // one cleanup timer

		assertEquals(0, testHarness.getOutput().size());
	}

	@Test
	public void testNoEventTimeGCTimerForGlobalWindow() throws Exception {

		WindowAssigner<Integer, GlobalWindow> mockAssigner = mockGlobalWindowAssigner();
		Trigger<Integer, GlobalWindow> mockTrigger = mockTrigger();

		// this needs to be true for the test to succeed
		assertEquals(Long.MAX_VALUE, GlobalWindow.get().maxTimestamp());

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, GlobalWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// just the window contents
		assertEquals(1, testHarness.numKeyedStateEntries());

		// no GC timer
		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numProcessingTimeTimers());
	}

	@Test
	public void testNoProcessingTimeGCTimerForGlobalWindow() throws Exception {

		WindowAssigner<Integer, GlobalWindow> mockAssigner = mockGlobalWindowAssigner();
		when(mockAssigner.isEventTime()).thenReturn(false);
		Trigger<Integer, GlobalWindow> mockTrigger = mockTrigger();

		// this needs to be true for the test to succeed
		assertEquals(Long.MAX_VALUE, GlobalWindow.get().maxTimestamp());

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, GlobalWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// just the window contents
		assertEquals(1, testHarness.numKeyedStateEntries());

		// no GC timer
		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numProcessingTimeTimers());
	}


	@Test
	public void testNoEventTimeGCTimerForLongMax() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, Long.MAX_VALUE - 10)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// just the window contents
		assertEquals(1, testHarness.numKeyedStateEntries());

		// no GC timer
		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numProcessingTimeTimers());
	}

	@Test
	public void testProcessingTimeGCTimerIsAlwaysWindowMaxTimestamp() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		when(mockAssigner.isEventTime()).thenReturn(false);
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, Long.MAX_VALUE - 10)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// just the window contents
		assertEquals(1, testHarness.numKeyedStateEntries());

		// no GC timer
		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(1, testHarness.numProcessingTimeTimers());

		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		testHarness.setProcessingTime(Long.MAX_VALUE - 10);

		verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numProcessingTimeTimers());
	}


	@Test
	public void testEventTimeGCTimer() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 20)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// just the window contents
		assertEquals(1, testHarness.numKeyedStateEntries());

		assertEquals(1, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numProcessingTimeTimers());

		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		testHarness.processWatermark(new Watermark(19 + 20)); // 19 is maxTime of the window

		verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(0, testHarness.numEventTimeTimers());
	}


	@Test
	public void testEventTimeTriggerTimerAndGCTimerCoincide() throws Exception {
		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 20)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// 19 is maxTime of window
				context.registerEventTimeTimer(19L + 20);
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// just the window contents
		assertEquals(1, testHarness.numKeyedStateEntries());

		assertEquals(1, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numProcessingTimeTimers());

		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		testHarness.processWatermark(new Watermark(19 + 20)); // 19 is maxTime of the window

		verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());
		verify(mockTrigger, times(1)).onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext());

		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numProcessingTimeTimers());
	}

	@Test
	public void testProcessingTimeGCTimer() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		when(mockAssigner.isEventTime()).thenReturn(false);
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 20)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// just the window contents
		assertEquals(1, testHarness.numKeyedStateEntries());

		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(1, testHarness.numProcessingTimeTimers());

		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		testHarness.setProcessingTime(19 + 20); // 19 is maxTime of the window

		verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numProcessingTimeTimers());
	}

	@Test
	public void testProcessingTimeTriggerTimerAndGCTimerCoincide() throws Exception {
		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		when(mockAssigner.isEventTime()).thenReturn(false);
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 20)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// 19 is maxTime of window, for processing time allowed lateness is always zero
				context.registerProcessingTimeTimer(19L);
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());


		testHarness.processElement(new StreamRecord<>(0, 0L));

		// just the window contents
		assertEquals(1, testHarness.numKeyedStateEntries());

		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(1, testHarness.numProcessingTimeTimers());

		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		testHarness.setProcessingTime(19); // 19 is maxTime of the window

		verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numProcessingTimeTimers());

	}

	@Test
	public void testStateAndTimerCleanupAtEventTimeGC() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 20)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// very far in the future so our watermark does not trigger it
				context.registerEventTimeTimer(1000);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[1];
				context.deleteEventTimeTimer(1000);
				context.getPartitionedState(valueStateDescriptor).clear();
				return null;
			}
		}).when(mockTrigger).clear(anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(2, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(2, testHarness.numEventTimeTimers()); // window timers/gc timers
		assertEquals(0, testHarness.numProcessingTimeTimers());

		testHarness.processWatermark(new Watermark(19 + 20)); // 19 is maxTime of the window

		verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(0, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(0, testHarness.numEventTimeTimers()); // window timers/gc timers
	}

	/**
	 * Verify that we correctly clean up even when a purging trigger has purged
	 * window state.
	 */
	@Test
	public void testStateAndTimerCleanupAtEventTimeGCWithPurgingTrigger() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 20)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// very far in the future so our watermark does not trigger it
				context.registerEventTimeTimer(1000);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.PURGE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[1];
				context.deleteEventTimeTimer(1000);
				context.getPartitionedState(valueStateDescriptor).clear();
				return null;
			}
		}).when(mockTrigger).clear(anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(1, testHarness.numKeyedStateEntries()); // just the trigger state remains
		assertEquals(2, testHarness.numEventTimeTimers()); // window timers/gc timers
		assertEquals(0, testHarness.numProcessingTimeTimers());

		testHarness.processWatermark(new Watermark(19 + 20)); // 19 is maxTime of the window

		verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(0, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(0, testHarness.numEventTimeTimers()); // window timers/gc timers
	}

	/**
	 * Verify that we correctly clean up even when a purging trigger has purged
	 * window state.
	 */
	@Test
	public void testStateAndTimerCleanupAtEventTimeGCWithPurgingTriggerAndMergingWindows() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 20)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// very far in the future so our watermark does not trigger it
				context.registerEventTimeTimer(1000);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.PURGE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[1];
				context.deleteEventTimeTimer(1000);
				context.getPartitionedState(valueStateDescriptor).clear();
				return null;
			}
		}).when(mockTrigger).clear(anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(2, testHarness.numKeyedStateEntries()); // trigger state plus merging window set
		assertEquals(2, testHarness.numEventTimeTimers()); // window timers/gc timers
		assertEquals(0, testHarness.numProcessingTimeTimers());

		testHarness.processWatermark(new Watermark(19 + 20)); // 19 is maxTime of the window

		verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(0, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(0, testHarness.numEventTimeTimers()); // window timers/gc timers
	}


	@Test
	public void testStateAndTimerCleanupAtProcessingTimeGC() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		when(mockAssigner.isEventTime()).thenReturn(false);
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 20)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// very far in the future so our cleanup trigger does not require triggering this
				context.registerProcessingTimeTimer(1000);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[1];
				context.deleteProcessingTimeTimer(1000);
				context.getPartitionedState(valueStateDescriptor).clear();
				return null;
			}
		}).when(mockTrigger).clear(anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(2, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(2, testHarness.numProcessingTimeTimers());
		assertEquals(0, testHarness.numEventTimeTimers());

		testHarness.setProcessingTime(19 + 20); // 19 is maxTime of the window

		verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(0, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(0, testHarness.numProcessingTimeTimers()); // window timers/gc timers
	}

	/**
	 * Verify that we correctly clean up even when a purging trigger has purged
	 * window state.
	 */
	@Test
	public void testStateAndTimerCleanupAtProcessingTimeGCWithPurgingTrigger() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		when(mockAssigner.isEventTime()).thenReturn(false);
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 20)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// very far in the future so our cleanup trigger does not require triggering this
				context.registerProcessingTimeTimer(1000);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.PURGE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[1];
				context.deleteProcessingTimeTimer(1000);
				context.getPartitionedState(valueStateDescriptor).clear();
				return null;
			}
		}).when(mockTrigger).clear(anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(1, testHarness.numKeyedStateEntries()); // just the trigger state
		assertEquals(2, testHarness.numProcessingTimeTimers());
		assertEquals(0, testHarness.numEventTimeTimers());

		testHarness.setProcessingTime(19 + 20); // 19 is maxTime of the window

		verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(0, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(0, testHarness.numProcessingTimeTimers()); // window timers/gc timers
	}

	/**
	 * Verify that we correctly clean up even when a purging trigger has purged
	 * window state.
	 */
	@Test
	public void testStateAndTimerCleanupAtProcessingTimeGCWithPurgingTriggerAndMergingWindows() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
		when(mockAssigner.isEventTime()).thenReturn(false);
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 20)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				// very far in the future so our cleanup trigger does not require triggering this
				context.registerProcessingTimeTimer(1000);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.PURGE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[1];
				context.deleteProcessingTimeTimer(1000);
				context.getPartitionedState(valueStateDescriptor).clear();
				return null;
			}
		}).when(mockTrigger).clear(anyTimeWindow(), anyTriggerContext());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(2, testHarness.numKeyedStateEntries()); // trigger state and merging window set
		assertEquals(2, testHarness.numProcessingTimeTimers());
		assertEquals(0, testHarness.numEventTimeTimers());

		testHarness.setProcessingTime(19 + 20); // 19 is maxTime of the window

		verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

		assertEquals(0, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(0, testHarness.numProcessingTimeTimers()); // window timers/gc timers
	}

	@Test
	public void testMergingWindowSetClearedAtEventTimeGC() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 20)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(2, testHarness.numKeyedStateEntries()); // window contents plus merging window set
		assertEquals(1, testHarness.numEventTimeTimers()); // gc timers

		testHarness.processWatermark(new Watermark(19 + 20)); // 19 is maxTime of the window

		assertEquals(0, testHarness.numKeyedStateEntries());
		assertEquals(0, testHarness.numEventTimeTimers());
	}

	@Test
	public void testMergingWindowSetClearedAtProcessingTimeGC() throws Exception {

		WindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
		when(mockAssigner.isEventTime()).thenReturn(false);
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 20)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		testHarness.processElement(new StreamRecord<>(0, 0L));

		assertEquals(2, testHarness.numKeyedStateEntries()); // window contents plus merging window set
		assertEquals(1, testHarness.numProcessingTimeTimers()); // gc timers

		testHarness.setProcessingTime(19 + 20); // 19 is maxTime of the window

		assertEquals(0, testHarness.numKeyedStateEntries());
		assertEquals(0, testHarness.numProcessingTimeTimers());
	}

	@Test
	public void testProcessingElementsWithinAllowedLateness() throws Exception {
		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				return TriggerResult.FIRE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		// 20 is just at the limit, window.maxTime() is 1 and allowed lateness is 20
		testHarness.processWatermark(new Watermark(20));

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// clear is only called at cleanup time/GC time
		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		// FIRE should not purge contents
		assertEquals(1, testHarness.numKeyedStateEntries()); // window contents plus trigger state
		assertEquals(1, testHarness.numEventTimeTimers()); // just the GC timer

		// there should be two elements now
		assertThat(testHarness.extractOutputStreamRecords(),
				containsInAnyOrder(
						isWindowedValue(contains(0), 1L, timeWindow(0, 2))));

	}

	@Test
	public void testLateWindowDropping() throws Exception {
		WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 20L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				return TriggerResult.FIRE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		// window.maxTime() == 1 plus 20L of allowed lateness
		testHarness.processWatermark(new Watermark(21));

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// there should be nothing
		assertEquals(0, testHarness.numKeyedStateEntries());
		assertEquals(0, testHarness.numEventTimeTimers());
		assertEquals(0, testHarness.numProcessingTimeTimers());

		// there should be two elements now
		assertEquals(0, testHarness.extractOutputStreamRecords().size());
	}

	@Test
	public void testStateSnapshotAndRestore() throws Exception {

		MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
		Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, TimeWindow>> testHarness =
				createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.open();

		when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
				.thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

		assertEquals(0, testHarness.getOutput().size());
		assertEquals(0, testHarness.numKeyedStateEntries());

		doAnswer(new Answer<TriggerResult>() {
			@Override
			public TriggerResult answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[3];
				context.registerEventTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).update("hello");
				return TriggerResult.CONTINUE;
			}
		}).when(mockTrigger).onElement(Matchers.<Integer>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());

		shouldFireAndPurgeOnEventTime(mockTrigger);

		testHarness.processElement(new StreamRecord<>(0, 0L));

		// window-contents and trigger state for two windows plus merging window set
		assertEquals(5, testHarness.numKeyedStateEntries());
		assertEquals(4, testHarness.numEventTimeTimers()); // timers/gc timers for two windows

		StreamStateHandle snapshot = testHarness.snapshot(0, 0);

		// restore
		mockAssigner = mockMergingAssigner();
		mockTrigger = mockTrigger();

		doAnswer(new Answer<Object>() {
			@Override
			public Object answer(InvocationOnMock invocation) throws Exception {
				Trigger.TriggerContext context = (Trigger.TriggerContext) invocation.getArguments()[1];
				context.deleteEventTimeTimer(0L);
				context.getPartitionedState(valueStateDescriptor).clear();
				return null;
			}
		}).when(mockTrigger).clear(anyTimeWindow(), anyTriggerContext());

		// only fire on the timestamp==0L timers, not the gc timers
		when(mockTrigger.onEventTime(eq(0L), Matchers.<TimeWindow>any(), anyTriggerContext())).thenReturn(TriggerResult.FIRE);

		testHarness = createListWindowOperator(mockAssigner, mockTrigger, 0L);

		testHarness.setup();
		testHarness.restore(snapshot);
		testHarness.open();

		assertEquals(0, testHarness.extractOutputStreamRecords().size());

		// verify that we still have all the state/timers/merging window set
		assertEquals(5, testHarness.numKeyedStateEntries());
		assertEquals(4, testHarness.numEventTimeTimers()); // timers/gc timers for two windows

		verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

		testHarness.processWatermark(new Watermark(20L));

		verify(mockTrigger, times(2)).clear(anyTimeWindow(), anyTriggerContext());

		// it's also called for the cleanup timers
		verify(mockTrigger, times(4)).onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext());
		verify(mockTrigger, times(1)).onEventTime(eq(0L), eq(new TimeWindow(0, 2)), anyTriggerContext());
		verify(mockTrigger, times(1)).onEventTime(eq(0L), eq(new TimeWindow(2, 4)), anyTriggerContext());

		assertEquals(0, testHarness.numKeyedStateEntries());
		assertEquals(0, testHarness.numEventTimeTimers());

		// there should be two elements now
		assertThat(testHarness.extractOutputStreamRecords(),
				containsInAnyOrder(
						isWindowedValue(contains(0), 1L, timeWindow(0, 2)),
						isWindowedValue(contains(0), 3L, timeWindow(2, 4))));

	}

	private <W extends Window> KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, W>> createListWindowOperator(
			WindowAssigner<Integer, W> assigner,
			Trigger<Integer, W> trigger,
			long allowedLatenss) throws Exception {

		ListStateDescriptor<Integer> stateDesc =
				new ListStateDescriptor<>("window-contents", IntSerializer.INSTANCE);

		KeySelector<Integer, Integer> keySelector = new KeySelector<Integer, Integer>() {
			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(Integer value) throws Exception {
				return value;
			}
		};

		WindowOperator<Integer, Integer, Iterable<Integer>, WindowedValue<List<Integer>, W>, W> operator = new WindowOperator<>(
				assigner,
				assigner.getWindowSerializer(new ExecutionConfig()),
				keySelector,
				IntSerializer.INSTANCE,
				stateDesc,
				new InternalIterableWindowFunction<>(new WindowFunction<Integer, WindowedValue<List<Integer>, W>, Integer, W>() {
					private static final long serialVersionUID = 1L;

					@Override
					public void apply(
							Integer integer,
							W window,
							Iterable<Integer> input,
							Collector<WindowedValue<List<Integer>, W>> out) throws Exception {
						out.collect(new WindowedValue<>((List<Integer>) Lists.newArrayList(input), window));
					}
				}),
				trigger,
				allowedLatenss);

		operator.setInputType(BasicTypeInfo.INT_TYPE_INFO, new ExecutionConfig());

		KeyedOneInputStreamOperatorTestHarness<Integer, Integer, WindowedValue<List<Integer>, W>> testHarness =
				new KeyedOneInputStreamOperatorTestHarness<>(
						operator,
						keySelector,
						BasicTypeInfo.INT_TYPE_INFO);

		return testHarness;
	}
}
