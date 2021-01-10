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
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.KeyedStateStore;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.MergingWindowAssigner;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.runtime.operators.windowing.functions.InternalWindowFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.AbstractStreamOperatorTestHarness;
import org.apache.flink.streaming.util.KeyedOneInputStreamOperatorTestHarness;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;
import org.mockito.verification.VerificationMode;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.streaming.util.StreamRecordMatchers.streamRecord;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyCollection;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.hamcrest.MockitoHamcrest.argThat;

/**
 * Base for window operator tests that verify correct interaction with the other windowing
 * components: {@link org.apache.flink.streaming.api.windowing.assigners.WindowAssigner}, {@link
 * org.apache.flink.streaming.api.windowing.triggers.Trigger}. {@link
 * org.apache.flink.streaming.api.functions.windowing.WindowFunction} and window state.
 *
 * <p>These tests document the implicit contract that exists between the windowing components.
 */
public abstract class WindowOperatorContractTest extends TestLogger {

    @Rule public ExpectedException expectedException = ExpectedException.none();

    private static ValueStateDescriptor<String> valueStateDescriptor =
            new ValueStateDescriptor<>("string-state", StringSerializer.INSTANCE, null);

    static <IN, OUT, KEY, W extends Window>
            InternalWindowFunction<IN, OUT, KEY, W> mockWindowFunction() throws Exception {
        @SuppressWarnings("unchecked")
        InternalWindowFunction<IN, OUT, KEY, W> mockWindowFunction =
                mock(InternalWindowFunction.class);

        return mockWindowFunction;
    }

    static <T, W extends Window> Trigger<T, W> mockTrigger() throws Exception {
        @SuppressWarnings("unchecked")
        Trigger<T, W> mockTrigger = mock(Trigger.class);

        when(mockTrigger.onElement(
                        Matchers.<T>any(), anyLong(), Matchers.<W>any(), anyTriggerContext()))
                .thenReturn(TriggerResult.CONTINUE);
        when(mockTrigger.onEventTime(anyLong(), Matchers.<W>any(), anyTriggerContext()))
                .thenReturn(TriggerResult.CONTINUE);
        when(mockTrigger.onProcessingTime(anyLong(), Matchers.<W>any(), anyTriggerContext()))
                .thenReturn(TriggerResult.CONTINUE);

        return mockTrigger;
    }

    static <T> WindowAssigner<T, TimeWindow> mockTimeWindowAssigner() throws Exception {
        @SuppressWarnings("unchecked")
        WindowAssigner<T, TimeWindow> mockAssigner = mock(WindowAssigner.class);

        when(mockAssigner.getWindowSerializer(Mockito.<ExecutionConfig>any()))
                .thenReturn(new TimeWindow.Serializer());
        when(mockAssigner.isEventTime()).thenReturn(true);

        return mockAssigner;
    }

    static <T> WindowAssigner<T, GlobalWindow> mockGlobalWindowAssigner() throws Exception {
        @SuppressWarnings("unchecked")
        WindowAssigner<T, GlobalWindow> mockAssigner = mock(WindowAssigner.class);

        when(mockAssigner.getWindowSerializer(Mockito.<ExecutionConfig>any()))
                .thenReturn(new GlobalWindow.Serializer());
        when(mockAssigner.isEventTime()).thenReturn(true);
        when(mockAssigner.assignWindows(Mockito.<T>any(), anyLong(), anyAssignerContext()))
                .thenReturn(Collections.singletonList(GlobalWindow.get()));

        return mockAssigner;
    }

    static <T> MergingWindowAssigner<T, TimeWindow> mockMergingAssigner() throws Exception {
        @SuppressWarnings("unchecked")
        MergingWindowAssigner<T, TimeWindow> mockAssigner = mock(MergingWindowAssigner.class);

        when(mockAssigner.getWindowSerializer(Mockito.<ExecutionConfig>any()))
                .thenReturn(new TimeWindow.Serializer());
        when(mockAssigner.isEventTime()).thenReturn(true);

        return mockAssigner;
    }

    static WindowAssigner.WindowAssignerContext anyAssignerContext() {
        return Mockito.any();
    }

    static Trigger.TriggerContext anyTriggerContext() {
        return Mockito.any();
    }

    static <T> Collector<T> anyCollector() {
        return Mockito.any();
    }

    static Iterable<Integer> anyIntIterable() {
        return Mockito.any();
    }

    @SuppressWarnings("unchecked")
    static Iterable<Integer> intIterable(Integer... values) {
        return (Iterable<Integer>) argThat(contains(values));
    }

    static TimeWindow anyTimeWindow() {
        return Mockito.any();
    }

    static InternalWindowFunction.InternalWindowContext anyInternalWindowContext() {
        return Mockito.any();
    }

    static Trigger.OnMergeContext anyOnMergeContext() {
        return Mockito.any();
    }

    static MergingWindowAssigner.MergeCallback anyMergeCallback() {
        return Mockito.any();
    }

    static <T> void shouldRegisterEventTimeTimerOnElement(
            Trigger<T, TimeWindow> mockTrigger, final long timestamp) throws Exception {
        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                @SuppressWarnings("unchecked")
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                context.registerEventTimeTimer(timestamp);
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<T>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());
    }

    private static <T> void shouldDeleteEventTimeTimerOnElement(
            Trigger<T, TimeWindow> mockTrigger, final long timestamp) throws Exception {
        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                @SuppressWarnings("unchecked")
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                context.deleteEventTimeTimer(timestamp);
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<T>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());
    }

    private static <T> void shouldRegisterProcessingTimeTimerOnElement(
            Trigger<T, TimeWindow> mockTrigger, final long timestamp) throws Exception {
        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                @SuppressWarnings("unchecked")
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                context.registerProcessingTimeTimer(timestamp);
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<T>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());
    }

    private static <T> void shouldDeleteProcessingTimeTimerOnElement(
            Trigger<T, TimeWindow> mockTrigger, final long timestamp) throws Exception {
        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                @SuppressWarnings("unchecked")
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                context.deleteProcessingTimeTimer(timestamp);
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<T>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext());
    }

    @SuppressWarnings("unchecked")
    private static <T, W extends Window> void shouldMergeWindows(
            final MergingWindowAssigner<T, W> assigner,
            final Collection<? extends W> expectedWindows,
            final Collection<W> toMerge,
            final W mergeResult) {
        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocation) throws Exception {
                                Collection<W> windows =
                                        (Collection<W>) invocation.getArguments()[0];

                                MergingWindowAssigner.MergeCallback callback =
                                        (MergingWindowAssigner.MergeCallback)
                                                invocation.getArguments()[1];

                                // verify the expected windows
                                assertThat(windows, containsInAnyOrder(expectedWindows.toArray()));

                                callback.merge(toMerge, mergeResult);
                                return null;
                            }
                        })
                .when(assigner)
                .mergeWindows(
                        anyCollection(), Matchers.<MergingWindowAssigner.MergeCallback>anyObject());
    }

    private static <T> void shouldContinueOnElement(Trigger<T, TimeWindow> mockTrigger)
            throws Exception {
        when(mockTrigger.onElement(
                        Matchers.<T>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.CONTINUE);
    }

    private static <T> void shouldFireOnElement(Trigger<T, TimeWindow> mockTrigger)
            throws Exception {
        when(mockTrigger.onElement(
                        Matchers.<T>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE);
    }

    private static <T> void shouldPurgeOnElement(Trigger<T, TimeWindow> mockTrigger)
            throws Exception {
        when(mockTrigger.onElement(
                        Matchers.<T>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.PURGE);
    }

    private static <T> void shouldFireAndPurgeOnElement(Trigger<T, TimeWindow> mockTrigger)
            throws Exception {
        when(mockTrigger.onElement(
                        Matchers.<T>anyObject(), anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE_AND_PURGE);
    }

    private static <T> void shouldContinueOnEventTime(Trigger<T, TimeWindow> mockTrigger)
            throws Exception {
        when(mockTrigger.onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.CONTINUE);
    }

    private static <T> void shouldFireOnEventTime(Trigger<T, TimeWindow> mockTrigger)
            throws Exception {
        when(mockTrigger.onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE);
    }

    private static <T> void shouldPurgeOnEventTime(Trigger<T, TimeWindow> mockTrigger)
            throws Exception {
        when(mockTrigger.onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.PURGE);
    }

    private static <T> void shouldFireAndPurgeOnEventTime(Trigger<T, TimeWindow> mockTrigger)
            throws Exception {
        when(mockTrigger.onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE_AND_PURGE);
    }

    private static <T> void shouldContinueOnProcessingTime(Trigger<T, TimeWindow> mockTrigger)
            throws Exception {
        when(mockTrigger.onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.CONTINUE);
    }

    private static <T> void shouldFireOnProcessingTime(Trigger<T, TimeWindow> mockTrigger)
            throws Exception {
        when(mockTrigger.onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE);
    }

    private static <T> void shouldPurgeOnProcessingTime(Trigger<T, TimeWindow> mockTrigger)
            throws Exception {
        when(mockTrigger.onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.PURGE);
    }

    private static <T> void shouldFireAndPurgeOnProcessingTime(Trigger<T, TimeWindow> mockTrigger)
            throws Exception {
        when(mockTrigger.onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE_AND_PURGE);
    }

    /**
     * Verify that there is no late-data side output if the {@code WindowAssigner} does not assign
     * any windows.
     */
    @Test
    public void testNoLateSideOutputForSkippedWindows() throws Exception {

        OutputTag<Integer> lateOutputTag = new OutputTag<Integer>("late") {};

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        OneInputStreamOperatorTestHarness<Integer, Void> testHarness =
                createWindowOperator(
                        mockAssigner, mockTrigger, 0L, mockWindowFunction, lateOutputTag);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Collections.<TimeWindow>emptyList());

        testHarness.processWatermark(0);
        testHarness.processElement(new StreamRecord<>(0, 5L));

        verify(mockAssigner, times(1)).assignWindows(eq(0), eq(5L), anyAssignerContext());

        assertTrue(
                testHarness.getSideOutput(lateOutputTag) == null
                        || testHarness.getSideOutput(lateOutputTag).isEmpty());
    }

    @Test
    public void testLateSideOutput() throws Exception {

        OutputTag<Integer> lateOutputTag = new OutputTag<Integer>("late") {};

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        OneInputStreamOperatorTestHarness<Integer, Void> testHarness =
                createWindowOperator(
                        mockAssigner, mockTrigger, 0L, mockWindowFunction, lateOutputTag);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Collections.singletonList(new TimeWindow(0, 0)));

        testHarness.processWatermark(20);
        testHarness.processElement(new StreamRecord<>(0, 5L));

        verify(mockAssigner, times(1)).assignWindows(eq(0), eq(5L), anyAssignerContext());

        assertThat(testHarness.getSideOutput(lateOutputTag), contains(streamRecord(0, 5L)));

        // we should also see side output if the WindowAssigner assigns no windows
        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Collections.<TimeWindow>emptyList());

        testHarness.processElement(new StreamRecord<>(0, 10L));

        verify(mockAssigner, times(1)).assignWindows(eq(0), eq(5L), anyAssignerContext());
        verify(mockAssigner, times(1)).assignWindows(eq(0), eq(10L), anyAssignerContext());

        assertThat(
                testHarness.getSideOutput(lateOutputTag),
                contains(streamRecord(0, 5L), streamRecord(0, 10L)));
    }

    /** This also verifies that the timestamps ouf side-emitted records is correct. */
    @Test
    public void testSideOutput() throws Exception {

        final OutputTag<Integer> integerOutputTag = new OutputTag<Integer>("int-out") {};
        final OutputTag<Long> longOutputTag = new OutputTag<Long>("long-out") {};

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> windowFunction =
                new InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow>() {
                    @Override
                    public void process(
                            Integer integer,
                            TimeWindow window,
                            InternalWindowContext ctx,
                            Iterable<Integer> input,
                            Collector<Void> out)
                            throws Exception {
                        Integer inputValue = input.iterator().next();

                        ctx.output(integerOutputTag, inputValue);
                        ctx.output(longOutputTag, inputValue.longValue());
                    }

                    @Override
                    public void clear(TimeWindow window, InternalWindowContext context)
                            throws Exception {}
                };

        OneInputStreamOperatorTestHarness<Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, windowFunction);

        testHarness.open();

        final long windowEnd = 42L;

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Collections.singletonList(new TimeWindow(0, windowEnd)));

        shouldFireOnElement(mockTrigger);

        testHarness.processElement(new StreamRecord<>(17, 5L));

        assertThat(
                testHarness.getSideOutput(integerOutputTag),
                contains(streamRecord(17, windowEnd - 1)));

        assertThat(
                testHarness.getSideOutput(longOutputTag),
                contains(streamRecord(17L, windowEnd - 1)));
    }

    @Test
    public void testAssignerIsInvokedOncePerElement() throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        OneInputStreamOperatorTestHarness<Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

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
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        OneInputStreamOperatorTestHarness<Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

        shouldFireOnElement(mockTrigger);

        testHarness.processElement(new StreamRecord<>(0, 0L));

        verify(mockWindowFunction, times(2))
                .process(
                        eq(0),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq((new TimeWindow(0, 2))),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(2, 4)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());
    }

    @Test
    public void testWindowsDontInterfere() throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

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

        verify(mockWindowFunction, times(2))
                .process(
                        anyInt(),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(0, 2)),
                        anyInternalWindowContext(),
                        intIterable(0, 0),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(1),
                        eq(new TimeWindow(0, 1)),
                        anyInternalWindowContext(),
                        intIterable(1, 1),
                        WindowOperatorContractTest.<Void>anyCollector());
    }

    @Test
    public void testOnElementCalledPerWindow() throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        OneInputStreamOperatorTestHarness<Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

        testHarness.processElement(new StreamRecord<>(42, 1L));

        verify(mockTrigger)
                .onElement(eq(42), eq(1L), eq(new TimeWindow(2, 4)), anyTriggerContext());
        verify(mockTrigger)
                .onElement(eq(42), eq(1L), eq(new TimeWindow(0, 2)), anyTriggerContext());

        verify(mockTrigger, times(2))
                .onElement(anyInt(), anyLong(), anyTimeWindow(), anyTriggerContext());
    }

    @Test
    public void testEmittingFromWindowFunction() throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, String, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, String> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Collections.singletonList(new TimeWindow(0, 2)));

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                return TriggerResult.FIRE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        doAnswer(
                        new Answer<Void>() {
                            @Override
                            public Void answer(InvocationOnMock invocation) throws Exception {
                                @SuppressWarnings("unchecked")
                                Collector<String> out = invocation.getArgument(4);
                                out.collect("Hallo");
                                out.collect("Ciao");
                                return null;
                            }
                        })
                .when(mockWindowFunction)
                .process(
                        eq(0),
                        eq(new TimeWindow(0, 2)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<String>anyCollector());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(0, 2)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<String>anyCollector());

        assertThat(
                testHarness.extractOutputStreamRecords(),
                contains(streamRecord("Hallo", 1L), streamRecord("Ciao", 1L)));
    }

    @Test
    public void testEmittingFromWindowFunctionOnEventTime() throws Exception {
        testEmittingFromWindowFunction(new EventTimeAdaptor());
    }

    @Test
    public void testEmittingFromWindowFunctionOnProcessingTime() throws Exception {
        testEmittingFromWindowFunction(new ProcessingTimeAdaptor());
    }

    private void testEmittingFromWindowFunction(TimeDomainAdaptor timeAdaptor) throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, String, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, String> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Collections.singletonList(new TimeWindow(0, 2)));

        doAnswer(
                        new Answer<Void>() {
                            @Override
                            public Void answer(InvocationOnMock invocation) throws Exception {
                                @SuppressWarnings("unchecked")
                                Collector<String> out = invocation.getArgument(4);
                                out.collect("Hallo");
                                out.collect("Ciao");
                                return null;
                            }
                        })
                .when(mockWindowFunction)
                .process(
                        eq(0),
                        eq(new TimeWindow(0, 2)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<String>anyCollector());

        timeAdaptor.shouldRegisterTimerOnElement(mockTrigger, 1);

        testHarness.processElement(new StreamRecord<>(0, 0L));

        verify(mockWindowFunction, never())
                .process(
                        anyInt(),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<String>anyCollector());
        assertTrue(testHarness.extractOutputStreamRecords().isEmpty());

        timeAdaptor.shouldFireOnTime(mockTrigger);

        timeAdaptor.advanceTime(testHarness, 1L);

        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(0, 2)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<String>anyCollector());

        assertThat(
                testHarness.extractOutputStreamRecords(),
                contains(streamRecord("Hallo", 1L), streamRecord("Ciao", 1L)));
    }

    @Test
    public void testOnElementContinue() throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                TimeWindow window = (TimeWindow) invocation.getArguments()[2];
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                context.registerEventTimeTimer(window.getEnd());
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

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
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                TimeWindow window = (TimeWindow) invocation.getArguments()[2];
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                context.registerEventTimeTimer(window.getEnd());
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.FIRE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        verify(mockWindowFunction, times(2))
                .process(
                        eq(0),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(0, 2)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(2, 4)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());

        // clear is only called at cleanup time/GC time
        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        // FIRE should not purge contents
        assertEquals(4, testHarness.numKeyedStateEntries()); // window contents plus trigger state
        assertEquals(4, testHarness.numEventTimeTimers()); // window timers/gc timers
    }

    @Test
    public void testOnElementFireAndPurge() throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                TimeWindow window = (TimeWindow) invocation.getArguments()[2];
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                context.registerEventTimeTimer(window.getEnd());
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.FIRE_AND_PURGE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        verify(mockWindowFunction, times(2))
                .process(
                        eq(0),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(0, 2)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(2, 4)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());

        // clear is only called at cleanup time/GC time
        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        // FIRE_AND_PURGE should purge contents
        assertEquals(
                2,
                testHarness
                        .numKeyedStateEntries()); // trigger state will stick around until GC time

        // timers will stick around
        assertEquals(4, testHarness.numEventTimeTimers());
    }

    @Test
    public void testOnElementPurge() throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                context.registerEventTimeTimer(0L);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.PURGE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        // clear is only called at cleanup time/GC time
        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        // PURGE should purge contents
        assertEquals(
                2,
                testHarness
                        .numKeyedStateEntries()); // trigger state will stick around until GC time

        // timers will stick around
        assertEquals(4, testHarness.numEventTimeTimers()); // trigger timer and GC timer

        // no output
        assertEquals(0, testHarness.getOutput().size());
    }

    @Test
    public void testOnEventTimeContinue() throws Exception {
        testOnTimeContinue(new EventTimeAdaptor());
    }

    @Test
    public void testOnProcessingTimeContinue() throws Exception {
        testOnTimeContinue(new ProcessingTimeAdaptor());
    }

    private void testOnTimeContinue(final TimeDomainAdaptor timeAdaptor) throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        timeAdaptor.advanceTime(testHarness, Long.MIN_VALUE);

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

        assertEquals(0, testHarness.extractOutputStreamRecords().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        // this should register two timers because we have two windows
        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                // we don't want to fire the cleanup timer
                                timeAdaptor.registerTimer(context, 0L);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        timeAdaptor.shouldContinueOnTime(mockTrigger);

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                4,
                testHarness.numKeyedStateEntries()); // window-contents plus trigger state for two
        // windows
        assertEquals(4, timeAdaptor.numTimers(testHarness)); // timers/gc timers for two windows

        timeAdaptor.advanceTime(testHarness, 0L);

        assertEquals(4, testHarness.numKeyedStateEntries());
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // only gc timers left

        // there should be no firing
        assertEquals(0, testHarness.extractOutputStreamRecords().size());
    }

    @Test
    public void testOnEventTimeFire() throws Exception {
        testOnTimeFire(new EventTimeAdaptor());
    }

    @Test
    public void testOnProcessingTimeFire() throws Exception {
        testOnTimeFire(new ProcessingTimeAdaptor());
    }

    private void testOnTimeFire(final TimeDomainAdaptor timeAdaptor) throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        timeAdaptor.advanceTime(testHarness, Long.MIN_VALUE);

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

        assertEquals(0, testHarness.extractOutputStreamRecords().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                // don't interfere with cleanup timers
                                timeAdaptor.registerTimer(context, 0L);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        timeAdaptor.shouldFireOnTime(mockTrigger);

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                4, testHarness.numKeyedStateEntries()); // window-contents and trigger state for two
        // windows
        assertEquals(4, timeAdaptor.numTimers(testHarness)); // timers/gc timers for two windows

        timeAdaptor.advanceTime(testHarness, 0L);

        verify(mockWindowFunction, times(2))
                .process(
                        eq(0),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(0, 2)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(2, 4)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());

        // clear is only called at cleanup time/GC time
        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        // FIRE should not purge contents
        assertEquals(4, testHarness.numKeyedStateEntries());
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // only gc timers left
    }

    @Test
    public void testOnEventTimeFireAndPurge() throws Exception {
        testOnTimeFireAndPurge(new EventTimeAdaptor());
    }

    @Test
    public void testOnProcessingTimeFireAndPurge() throws Exception {
        testOnTimeFireAndPurge(new ProcessingTimeAdaptor());
    }

    private void testOnTimeFireAndPurge(final TimeDomainAdaptor timeAdaptor) throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        timeAdaptor.advanceTime(testHarness, Long.MIN_VALUE);

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

        assertEquals(0, testHarness.extractOutputStreamRecords().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                timeAdaptor.registerTimer(context, 0L);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        timeAdaptor.shouldFireAndPurgeOnTime(mockTrigger);

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                4, testHarness.numKeyedStateEntries()); // window-contents and trigger state for two
        // windows
        assertEquals(4, timeAdaptor.numTimers(testHarness)); // timers/gc timers for two windows

        timeAdaptor.advanceTime(testHarness, 0L);

        verify(mockWindowFunction, times(2))
                .process(
                        eq(0),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(0, 2)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(2, 4)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());

        // clear is only called at cleanup time/GC time
        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        // FIRE_AND_PURGE should purge contents
        assertEquals(2, testHarness.numKeyedStateEntries()); // trigger state stays until GC time
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // gc timers are still there
    }

    @Test
    public void testOnEventTimePurge() throws Exception {
        testOnTimePurge(new EventTimeAdaptor());
    }

    @Test
    public void testOnProcessingTimePurge() throws Exception {
        testOnTimePurge(new ProcessingTimeAdaptor());
    }

    private void testOnTimePurge(final TimeDomainAdaptor timeAdaptor) throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        timeAdaptor.advanceTime(testHarness, Long.MIN_VALUE);

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(4, 6)));

        assertEquals(0, testHarness.extractOutputStreamRecords().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                // don't interfere with cleanup timers
                                timeAdaptor.registerTimer(context, 1L);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        timeAdaptor.shouldPurgeOnTime(mockTrigger);

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                4, testHarness.numKeyedStateEntries()); // window-contents and trigger state for two
        // windows
        assertEquals(4, timeAdaptor.numTimers(testHarness)); // timers/gc timers for two windows

        timeAdaptor.advanceTime(testHarness, 1L);

        // clear is only called at cleanup time/GC time
        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        // PURGE should purge contents
        assertEquals(2, testHarness.numKeyedStateEntries()); // trigger state will stick around
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // gc timers are still there

        // still no output
        assertEquals(0, testHarness.extractOutputStreamRecords().size());
    }

    @Test
    public void testNoEventTimeFiringForPurgedWindow() throws Exception {
        testNoTimerFiringForPurgedWindow(new EventTimeAdaptor());
    }

    @Test
    public void testNoProcessingTimeFiringForPurgedWindow() throws Exception {
        testNoTimerFiringForPurgedWindow(new ProcessingTimeAdaptor());
    }

    /**
     * Verify that we neither invoke the trigger nor the window function if a timer for a
     * non-existent window fires.
     */
    private void testNoTimerFiringForPurgedWindow(final TimeDomainAdaptor timeAdaptor)
            throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

        @SuppressWarnings("unchecked")
        InternalWindowFunction<Iterable<Integer>, List<Integer>, Integer, TimeWindow>
                mockWindowFunction = mock(InternalWindowFunction.class);

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, List<Integer>> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        timeAdaptor.advanceTime(testHarness, Long.MIN_VALUE);

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4)));

        assertEquals(0, testHarness.extractOutputStreamRecords().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                // don't interfere with cleanup timers
                                timeAdaptor.registerTimer(context, 0L);
                                return TriggerResult.PURGE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(0, testHarness.numKeyedStateEntries()); // not contents or state
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // timer and gc timer

        timeAdaptor.advanceTime(testHarness, 0L);

        // trigger is not called if there is no more window (timer is silently ignored)
        timeAdaptor.verifyTriggerCallback(mockTrigger, times(1), null, null);

        verify(mockWindowFunction, never())
                .process(
                        anyInt(),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<List<Integer>>anyCollector());

        assertEquals(1, timeAdaptor.numTimers(testHarness)); // only gc timers left
    }

    @Test
    public void testNoEventTimeFiringForPurgedMergingWindow() throws Exception {
        testNoTimerFiringForPurgedMergingWindow(new EventTimeAdaptor());
    }

    @Test
    public void testNoProcessingTimeFiringForPurgedMergingWindow() throws Exception {
        testNoTimerFiringForPurgedMergingWindow(new ProcessingTimeAdaptor());
    }

    /**
     * Verify that we neither invoke the trigger nor the window function if a timer for an empty
     * merging window fires.
     */
    private void testNoTimerFiringForPurgedMergingWindow(final TimeDomainAdaptor timeAdaptor)
            throws Exception {

        MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

        @SuppressWarnings("unchecked")
        InternalWindowFunction<Iterable<Integer>, List<Integer>, Integer, TimeWindow>
                mockWindowFunction = mock(InternalWindowFunction.class);

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, List<Integer>> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        timeAdaptor.advanceTime(testHarness, Long.MIN_VALUE);

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4)));

        assertEquals(0, testHarness.extractOutputStreamRecords().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                // don't interfere with cleanup timers
                                timeAdaptor.registerTimer(context, 0L);
                                return TriggerResult.PURGE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(1, testHarness.numKeyedStateEntries()); // just the merging window set
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // timer and gc timer

        timeAdaptor.advanceTime(testHarness, 0L);

        // trigger is not called if there is no more window (timer is silently ignored)
        timeAdaptor.verifyTriggerCallback(mockTrigger, times(1), null, null);

        verify(mockWindowFunction, never())
                .process(
                        anyInt(),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<List<Integer>>anyCollector());

        assertEquals(1, timeAdaptor.numTimers(testHarness)); // only gc timers left
    }

    @Test
    public void testNoEventTimeFiringForGarbageCollectedMergingWindow() throws Exception {
        testNoTimerFiringForGarbageCollectedMergingWindow(new EventTimeAdaptor());
    }

    @Test
    public void testNoProcessingTimeFiringForGarbageCollectedMergingWindow() throws Exception {
        testNoTimerFiringForGarbageCollectedMergingWindow(new ProcessingTimeAdaptor());
    }

    /**
     * Verify that we neither invoke the trigger nor the window function if a timer fires for a
     * merging window that was already garbage collected.
     */
    private void testNoTimerFiringForGarbageCollectedMergingWindow(
            final TimeDomainAdaptor timeAdaptor) throws Exception {

        MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();

        @SuppressWarnings("unchecked")
        InternalWindowFunction<Iterable<Integer>, List<Integer>, Integer, TimeWindow>
                mockWindowFunction = mock(InternalWindowFunction.class);

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, List<Integer>> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        timeAdaptor.advanceTime(testHarness, Long.MIN_VALUE);

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4)));

        assertEquals(0, testHarness.extractOutputStreamRecords().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                // set a timer for after the GC time
                                timeAdaptor.registerTimer(context, 10L);
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                2, testHarness.numKeyedStateEntries()); // window contents and merging window set
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // timer and gc timer

        timeAdaptor.shouldContinueOnTime(mockTrigger);

        // this should trigger GC
        timeAdaptor.advanceTime(testHarness, 4L);

        verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

        assertEquals(0, testHarness.numKeyedStateEntries());
        // we still have a dangling timer because our trigger doesn't do cleanup
        assertEquals(1, timeAdaptor.numTimers(testHarness));

        timeAdaptor.verifyTriggerCallback(mockTrigger, times(1), null, null);

        verify(mockWindowFunction, never())
                .process(
                        anyInt(),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<List<Integer>>anyCollector());

        // now we trigger the dangling timer
        timeAdaptor.advanceTime(testHarness, 10L);

        // we don't fire again
        timeAdaptor.verifyTriggerCallback(mockTrigger, times(1), null, null);
    }

    @Test
    public void testEventTimeTimerCreationAndDeletion() throws Exception {
        testTimerCreationAndDeletion(new EventTimeAdaptor());
    }

    @Test
    public void testProcessingTimeTimerCreationAndDeletion() throws Exception {
        testTimerCreationAndDeletion(new ProcessingTimeAdaptor());
    }

    private void testTimerCreationAndDeletion(TimeDomainAdaptor timeAdaptor) throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);

        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 2)));

        assertEquals(0, timeAdaptor.numTimers(testHarness));

        timeAdaptor.shouldRegisterTimerOnElement(mockTrigger, 17);
        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                2, timeAdaptor.numTimers(testHarness)); // +1 because of the GC timer of the window

        timeAdaptor.shouldRegisterTimerOnElement(mockTrigger, 42);
        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                3, timeAdaptor.numTimers(testHarness)); // +1 because of the GC timer of the window

        timeAdaptor.shouldDeleteTimerOnElement(mockTrigger, 42);
        testHarness.processElement(new StreamRecord<>(0, 0L));

        timeAdaptor.shouldDeleteTimerOnElement(mockTrigger, 17);
        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                1, timeAdaptor.numTimers(testHarness)); // +1 because of the GC timer of the window
    }

    @Test
    public void testEventTimeTimerFiring() throws Exception {
        testTimerFiring(new EventTimeAdaptor());
    }

    @Test
    public void testProcessingTimeTimerFiring() throws Exception {
        testTimerFiring(new ProcessingTimeAdaptor());
    }

    private void testTimerFiring(TimeDomainAdaptor timeAdaptor) throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 100)));

        assertEquals(0, timeAdaptor.numTimers(testHarness));

        timeAdaptor.shouldRegisterTimerOnElement(mockTrigger, 1);
        testHarness.processElement(new StreamRecord<>(0, 0L));

        timeAdaptor.shouldRegisterTimerOnElement(mockTrigger, 17);
        testHarness.processElement(new StreamRecord<>(0, 0L));

        timeAdaptor.shouldRegisterTimerOnElement(mockTrigger, 42);
        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                4, timeAdaptor.numTimers(testHarness)); // +1 because of the GC timer of the window

        timeAdaptor.advanceTime(testHarness, 1);

        timeAdaptor.verifyTriggerCallback(mockTrigger, atLeastOnce(), 1L, new TimeWindow(0, 100));
        timeAdaptor.verifyTriggerCallback(mockTrigger, times(1), null, null);
        assertEquals(
                3, timeAdaptor.numTimers(testHarness)); // +1 because of the GC timer of the window

        // doesn't do anything
        timeAdaptor.advanceTime(testHarness, 15);

        // so still the same
        timeAdaptor.verifyTriggerCallback(mockTrigger, times(1), null, null);

        timeAdaptor.advanceTime(testHarness, 42);

        timeAdaptor.verifyTriggerCallback(mockTrigger, atLeastOnce(), 17L, new TimeWindow(0, 100));
        timeAdaptor.verifyTriggerCallback(mockTrigger, atLeastOnce(), 42L, new TimeWindow(0, 100));
        timeAdaptor.verifyTriggerCallback(mockTrigger, times(3), null, null);
        assertEquals(
                1, timeAdaptor.numTimers(testHarness)); // +1 because of the GC timer of the window
    }

    @Test
    public void testEventTimeDeletedTimerDoesNotFire() throws Exception {
        testDeletedTimerDoesNotFire(new EventTimeAdaptor());
    }

    @Test
    public void testProcessingTimeDeletedTimerDoesNotFire() throws Exception {
        testDeletedTimerDoesNotFire(new ProcessingTimeAdaptor());
    }

    private void testDeletedTimerDoesNotFire(TimeDomainAdaptor timeAdaptor) throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 100)));

        assertEquals(0, timeAdaptor.numTimers(testHarness));

        timeAdaptor.shouldRegisterTimerOnElement(mockTrigger, 1);
        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(2, timeAdaptor.numTimers(testHarness)); // +1 for the GC timer

        timeAdaptor.shouldDeleteTimerOnElement(mockTrigger, 1);
        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(1, timeAdaptor.numTimers(testHarness)); // +1 for the GC timer

        timeAdaptor.shouldRegisterTimerOnElement(mockTrigger, 2);
        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(2, timeAdaptor.numTimers(testHarness)); // +1 for the GC timer

        timeAdaptor.advanceTime(testHarness, 50L);

        timeAdaptor.verifyTriggerCallback(mockTrigger, times(0), 1L, null);
        timeAdaptor.verifyTriggerCallback(mockTrigger, times(1), 2L, new TimeWindow(0, 100));
        timeAdaptor.verifyTriggerCallback(mockTrigger, times(1), null, null);

        assertEquals(1, timeAdaptor.numTimers(testHarness)); // +1 for the GC timer
    }

    @Test
    public void testMergeWindowsIsCalled() throws Exception {

        MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        verify(mockAssigner)
                .mergeWindows(
                        eq(Collections.singletonList(new TimeWindow(2, 4))), anyMergeCallback());
        verify(mockAssigner)
                .mergeWindows(
                        eq(Collections.singletonList(new TimeWindow(2, 4))), anyMergeCallback());

        verify(mockAssigner, times(2)).mergeWindows(anyCollection(), anyMergeCallback());
    }

    @Test
    public void testEventTimeWindowsAreMergedEagerly() throws Exception {
        testWindowsAreMergedEagerly(new EventTimeAdaptor());
    }

    @Test
    public void testProcessingTimeWindowsAreMergedEagerly() throws Exception {
        testWindowsAreMergedEagerly(new ProcessingTimeAdaptor());
    }

    /** Verify that windows are merged eagerly, if possible. */
    private void testWindowsAreMergedEagerly(final TimeDomainAdaptor timeAdaptor) throws Exception {
        // in this test we only have one state window and windows are eagerly
        // merged into the first window

        MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        timeAdaptor.advanceTime(testHarness, Long.MIN_VALUE);

        assertEquals(0, testHarness.extractOutputStreamRecords().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                // don't interfere with cleanup timers
                                timeAdaptor.registerTimer(context, 0L);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.OnMergeContext context =
                                        (Trigger.OnMergeContext) invocation.getArguments()[1];
                                // don't interfere with cleanup timers
                                timeAdaptor.registerTimer(context, 0L);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onMerge(anyTimeWindow(), anyOnMergeContext());

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocation) throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[1];
                                timeAdaptor.deleteTimer(context, 0L);
                                context.getPartitionedState(valueStateDescriptor).clear();
                                return null;
                            }
                        })
                .when(mockTrigger)
                .clear(anyTimeWindow(), anyTriggerContext());

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 2)));

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                3,
                testHarness.numKeyedStateEntries()); // window state plus trigger state plus merging
        // window set
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // timer and GC timer

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4)));

        shouldMergeWindows(
                mockAssigner,
                new ArrayList<>(Arrays.asList(new TimeWindow(0, 2), new TimeWindow(2, 4))),
                new ArrayList<>(Arrays.asList(new TimeWindow(0, 2), new TimeWindow(2, 4))),
                new TimeWindow(0, 4));

        // don't register a timer or update state in onElement, this checks
        // whether onMerge does correctly set those things
        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        verify(mockTrigger).onMerge(eq(new TimeWindow(0, 4)), anyOnMergeContext());

        assertEquals(3, testHarness.numKeyedStateEntries());
        assertEquals(2, timeAdaptor.numTimers(testHarness));
    }

    @Test
    public void testRejectShrinkingMergingEventTimeWindows() throws Exception {
        testRejectShrinkingMergingWindows(new EventTimeAdaptor());
    }

    @Test
    public void testRejectShrinkingMergingProcessingTimeWindows() throws Exception {
        testRejectShrinkingMergingWindows(new ProcessingTimeAdaptor());
    }

    /**
     * A misbehaving {@code WindowAssigner} can cause a window to become late by merging if it moves
     * the end-of-window time before the watermark. This verifies that we don't allow that.
     */
    void testRejectShrinkingMergingWindows(final TimeDomainAdaptor timeAdaptor) throws Exception {
        int allowedLateness = 10;

        if (timeAdaptor instanceof ProcessingTimeAdaptor) {
            // we don't have allowed lateness for processing time
            allowedLateness = 0;
        }

        MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(
                        mockAssigner, mockTrigger, allowedLateness, mockWindowFunction);

        testHarness.open();

        timeAdaptor.advanceTime(testHarness, 0);

        assertEquals(0, testHarness.extractOutputStreamRecords().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 22)));

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                2, testHarness.numKeyedStateEntries()); // window contents and merging window set
        assertEquals(1, timeAdaptor.numTimers(testHarness)); // cleanup timer

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 25)));

        timeAdaptor.advanceTime(testHarness, 20);

        // our window should still be there
        assertEquals(
                2, testHarness.numKeyedStateEntries()); // window contents and merging window set
        assertEquals(1, timeAdaptor.numTimers(testHarness)); // cleanup timer

        // the result timestamp is ... + 2 because a watermark t says no element with
        // timestamp <= t will come in the future and because window ends are exclusive:
        // a window (0, 12) will have 11 as maxTimestamp. With the watermark at 20, 10 would
        // already be considered late
        shouldMergeWindows(
                mockAssigner,
                new ArrayList<>(Arrays.asList(new TimeWindow(0, 22), new TimeWindow(0, 25))),
                new ArrayList<>(Arrays.asList(new TimeWindow(0, 22), new TimeWindow(0, 25))),
                new TimeWindow(0, 20 - allowedLateness + 2));

        testHarness.processElement(new StreamRecord<>(0, 0L));

        // now merge it to a window that is just late
        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 25)));

        shouldMergeWindows(
                mockAssigner,
                new ArrayList<>(
                        Arrays.asList(
                                new TimeWindow(0, 20 - allowedLateness + 2),
                                new TimeWindow(0, 25))),
                new ArrayList<>(
                        Arrays.asList(
                                new TimeWindow(0, 20 - allowedLateness + 2),
                                new TimeWindow(0, 25))),
                new TimeWindow(0, 20 - allowedLateness + 1));

        expectedException.expect(UnsupportedOperationException.class);
        testHarness.processElement(new StreamRecord<>(0, 0L));
    }

    @Test
    public void testMergingOfExistingEventTimeWindows() throws Exception {
        testMergingOfExistingWindows(new EventTimeAdaptor());
    }

    @Test
    public void testMergingOfExistingProcessingTimeWindows() throws Exception {
        testMergingOfExistingWindows(new ProcessingTimeAdaptor());
    }

    /**
     * Verify that we only keep one of the underlying state windows. This test also verifies that GC
     * timers are correctly deleted when merging windows.
     */
    private void testMergingOfExistingWindows(final TimeDomainAdaptor timeAdaptor)
            throws Exception {

        MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        timeAdaptor.advanceTime(testHarness, Long.MIN_VALUE);

        assertEquals(0, testHarness.extractOutputStreamRecords().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                // don't interfere with cleanup timers
                                timeAdaptor.registerTimer(context, 0L);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.OnMergeContext context =
                                        (Trigger.OnMergeContext) invocation.getArguments()[1];
                                // don't interfere with cleanup timers
                                timeAdaptor.registerTimer(context, 0L);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onMerge(anyTimeWindow(), anyOnMergeContext());

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocation) throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[1];
                                // don't interfere with cleanup timers
                                timeAdaptor.deleteTimer(context, 0L);
                                context.getPartitionedState(valueStateDescriptor).clear();
                                return null;
                            }
                        })
                .when(mockTrigger)
                .clear(anyTimeWindow(), anyTriggerContext());

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 2)));

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                3,
                testHarness.numKeyedStateEntries()); // window state plus trigger state plus merging
        // window set
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // trigger timer plus GC timer

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4)));

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                5,
                testHarness.numKeyedStateEntries()); // window state plus trigger state plus merging
        // window set
        assertEquals(4, timeAdaptor.numTimers(testHarness)); // trigger timer plus GC timer

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(1, 3)));

        shouldMergeWindows(
                mockAssigner,
                new ArrayList<>(
                        Arrays.asList(
                                new TimeWindow(0, 2), new TimeWindow(2, 4), new TimeWindow(1, 3))),
                new ArrayList<>(
                        Arrays.asList(
                                new TimeWindow(0, 2), new TimeWindow(2, 4), new TimeWindow(1, 3))),
                new TimeWindow(0, 4));

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                3,
                testHarness
                        .numKeyedStateEntries()); // window contents plus trigger state plus merging
        // window set
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // trigger timer plus GC timer

        assertEquals(0, testHarness.extractOutputStreamRecords().size());
    }

    @Test
    public void testOnElementPurgeDoesNotCleanupMergingSet() throws Exception {

        MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 2)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                return TriggerResult.PURGE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(1, testHarness.numKeyedStateEntries()); // the merging window set

        assertEquals(1, testHarness.numEventTimeTimers()); // one cleanup timer

        assertEquals(0, testHarness.getOutput().size());
    }

    @Test
    public void testOnEventTimePurgeDoesNotCleanupMergingSet() throws Exception {
        testOnTimePurgeDoesNotCleanupMergingSet(new EventTimeAdaptor());
    }

    @Test
    public void testOnProcessingTimePurgeDoesNotCleanupMergingSet() throws Exception {
        testOnTimePurgeDoesNotCleanupMergingSet(new ProcessingTimeAdaptor());
    }

    private void testOnTimePurgeDoesNotCleanupMergingSet(TimeDomainAdaptor timeAdaptor)
            throws Exception {

        MergingWindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 4)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        timeAdaptor.shouldRegisterTimerOnElement(mockTrigger, 1L);

        testHarness.processElement(new StreamRecord<>(0, 0L));

        timeAdaptor.shouldPurgeOnTime(mockTrigger);

        assertEquals(
                2, testHarness.numKeyedStateEntries()); // the merging window set + window contents
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // one cleanup timer + timer
        assertEquals(0, testHarness.getOutput().size());

        timeAdaptor.advanceTime(testHarness, 1L);

        assertEquals(1, testHarness.numKeyedStateEntries()); // the merging window set
        assertEquals(1, timeAdaptor.numTimers(testHarness)); // one cleanup timer
        assertEquals(0, testHarness.extractOutputStreamRecords().size());
    }

    @Test
    public void testNoEventTimeGarbageCollectionTimerForGlobalWindow() throws Exception {
        testNoGarbageCollectionTimerForGlobalWindow(new EventTimeAdaptor());
    }

    @Test
    public void testNoProcessingTimeGarbageCollectionTimerForGlobalWindow() throws Exception {
        testNoGarbageCollectionTimerForGlobalWindow(new ProcessingTimeAdaptor());
    }

    private void testNoGarbageCollectionTimerForGlobalWindow(TimeDomainAdaptor timeAdaptor)
            throws Exception {

        WindowAssigner<Integer, GlobalWindow> mockAssigner = mockGlobalWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, GlobalWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, GlobalWindow> mockWindowFunction =
                mockWindowFunction();

        // this needs to be true for the test to succeed
        assertEquals(Long.MAX_VALUE, GlobalWindow.get().maxTimestamp());

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        // just the window contents
        assertEquals(1, testHarness.numKeyedStateEntries());

        // verify we have no timers for either time domain
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numProcessingTimeTimers());
    }

    @Test
    public void testNoEventTimeGarbageCollectionTimerForLongMax() throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 20L, mockWindowFunction);

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
    public void testProcessingTimeGarbageCollectionTimerIsAlwaysWindowMaxTimestamp()
            throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        when(mockAssigner.isEventTime()).thenReturn(false);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 20L, mockWindowFunction);

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
    public void testEventTimeGarbageCollectionTimer() throws Exception {
        testGarbageCollectionTimer(new EventTimeAdaptor());
    }

    @Test
    public void testProcessingTimeGarbageCollectionTimer() throws Exception {
        testGarbageCollectionTimer(new ProcessingTimeAdaptor());
    }

    private void testGarbageCollectionTimer(TimeDomainAdaptor timeAdaptor) throws Exception {
        long allowedLateness = 20L;

        if (timeAdaptor instanceof ProcessingTimeAdaptor) {
            // we don't have allowed lateness for processing time
            allowedLateness = 0;
        }

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(
                        mockAssigner, mockTrigger, allowedLateness, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 20)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        // just the window contents
        assertEquals(1, testHarness.numKeyedStateEntries());

        assertEquals(1, timeAdaptor.numTimers(testHarness));
        assertEquals(0, timeAdaptor.numTimersOtherDomain(testHarness));

        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        // verify that we can still fire on the GC timer
        timeAdaptor.shouldFireOnTime(mockTrigger);

        timeAdaptor.advanceTime(testHarness, 19 + allowedLateness); // 19 is maxTime of the window

        // ensure that our trigger is still called
        timeAdaptor.verifyTriggerCallback(mockTrigger, times(1), 19L + allowedLateness, null);

        // ensure that our window function is called a last timer if the trigger
        // fires on the GC timer
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(0, 20)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());

        verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

        assertEquals(0, timeAdaptor.numTimers(testHarness));
        assertEquals(0, timeAdaptor.numTimersOtherDomain(testHarness));
    }

    @Test
    public void testEventTimeTriggerTimerAndGarbageCollectionTimerCoincide() throws Exception {
        testTriggerTimerAndGarbageCollectionTimerCoincide(new EventTimeAdaptor());
    }

    @Test
    public void testProcessingTimeTriggerTimerAndGarbageCollectionTimerCoincide() throws Exception {
        testTriggerTimerAndGarbageCollectionTimerCoincide(new ProcessingTimeAdaptor());
    }

    private void testTriggerTimerAndGarbageCollectionTimerCoincide(
            final TimeDomainAdaptor timeAdaptor) throws Exception {
        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 20)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                // 19 is maxTime of window
                                timeAdaptor.registerTimer(context, 19L);
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        // just the window contents
        assertEquals(1, testHarness.numKeyedStateEntries());

        assertEquals(1, timeAdaptor.numTimers(testHarness));
        assertEquals(0, timeAdaptor.numTimersOtherDomain(testHarness));

        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        timeAdaptor.advanceTime(testHarness, 19); // 19 is maxTime of the window

        verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());
        timeAdaptor.verifyTriggerCallback(mockTrigger, times(1), null, null);

        assertEquals(0, timeAdaptor.numTimers(testHarness));
        assertEquals(0, timeAdaptor.numTimersOtherDomain(testHarness));
    }

    @Test
    public void testStateAndTimerCleanupAtEventTimeGarbageCollection() throws Exception {
        testStateAndTimerCleanupAtEventTimeGarbageCollection(new EventTimeAdaptor());
    }

    @Test
    public void testStateAndTimerCleanupAtProcessingTimeGarbageCollection() throws Exception {
        testStateAndTimerCleanupAtEventTimeGarbageCollection(new ProcessingTimeAdaptor());
    }

    private void testStateAndTimerCleanupAtEventTimeGarbageCollection(
            final TimeDomainAdaptor timeAdaptor) throws Exception {
        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 20L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 20)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                // very far in the future so our watermark does not trigger it
                                timeAdaptor.registerTimer(context, 1000L);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocation) throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[1];
                                timeAdaptor.deleteTimer(context, 1000L);
                                context.getPartitionedState(valueStateDescriptor).clear();
                                return null;
                            }
                        })
                .when(mockTrigger)
                .clear(anyTimeWindow(), anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        // clear is only called at cleanup time/GC time
        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        assertEquals(2, testHarness.numKeyedStateEntries()); // window contents plus trigger state
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // window timers/gc timers
        assertEquals(0, timeAdaptor.numTimersOtherDomain(testHarness));

        timeAdaptor.advanceTime(testHarness, 19 + 20);

        verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

        assertEquals(0, testHarness.numKeyedStateEntries()); // window contents plus trigger state
        assertEquals(0, timeAdaptor.numTimers(testHarness)); // window timers/gc timers
        assertEquals(0, timeAdaptor.numTimersOtherDomain(testHarness));
    }

    @Test
    public void testStateAndTimerCleanupAtEventTimeGarbageCollectionWithPurgingTrigger()
            throws Exception {
        testStateAndTimerCleanupAtEventTimeGCWithPurgingTrigger(new EventTimeAdaptor());
    }

    @Test
    public void testStateAndTimerCleanupAtProcessingTimeGarbageCollectionWithPurgingTrigger()
            throws Exception {
        testStateAndTimerCleanupAtEventTimeGCWithPurgingTrigger(new ProcessingTimeAdaptor());
    }

    /** Verify that we correctly clean up even when a purging trigger has purged window state. */
    private void testStateAndTimerCleanupAtEventTimeGCWithPurgingTrigger(
            final TimeDomainAdaptor timeAdaptor) throws Exception {
        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 20L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 20)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                // very far in the future so our watermark does not trigger it
                                timeAdaptor.registerTimer(context, 1000L);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.PURGE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocation) throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[1];
                                timeAdaptor.deleteTimer(context, 1000L);
                                context.getPartitionedState(valueStateDescriptor).clear();
                                return null;
                            }
                        })
                .when(mockTrigger)
                .clear(anyTimeWindow(), anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        // clear is only called at cleanup time/GC time
        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        assertEquals(1, testHarness.numKeyedStateEntries()); // just the trigger state remains
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // window timers/gc timers
        assertEquals(0, timeAdaptor.numTimersOtherDomain(testHarness));

        timeAdaptor.advanceTime(testHarness, 19 + 20); // 19 is maxTime of the window

        verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

        assertEquals(0, testHarness.numKeyedStateEntries()); // window contents plus trigger state
        assertEquals(0, timeAdaptor.numTimers(testHarness)); // window timers/gc timers
        assertEquals(0, timeAdaptor.numTimersOtherDomain(testHarness));
    }

    @Test
    public void
            testStateAndTimerCleanupAtEventTimeGarbageCollectionWithPurgingTriggerAndMergingWindows()
                    throws Exception {
        testStateAndTimerCleanupAtGarbageCollectionWithPurgingTriggerAndMergingWindows(
                new EventTimeAdaptor());
    }

    @Test
    public void
            testStateAndTimerCleanupAtProcessingTimeGarbageCollectionWithPurgingTriggerAndMergingWindows()
                    throws Exception {
        testStateAndTimerCleanupAtGarbageCollectionWithPurgingTriggerAndMergingWindows(
                new ProcessingTimeAdaptor());
    }

    /** Verify that we correctly clean up even when a purging trigger has purged window state. */
    private void testStateAndTimerCleanupAtGarbageCollectionWithPurgingTriggerAndMergingWindows(
            final TimeDomainAdaptor timeAdaptor) throws Exception {
        WindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 20L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 20)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                // very far in the future so our watermark does not trigger it
                                timeAdaptor.registerTimer(context, 1000);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.PURGE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocation) throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[1];
                                timeAdaptor.deleteTimer(context, 1000);
                                context.getPartitionedState(valueStateDescriptor).clear();
                                return null;
                            }
                        })
                .when(mockTrigger)
                .clear(anyTimeWindow(), anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        // clear is only called at cleanup time/GC time
        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        assertEquals(
                2, testHarness.numKeyedStateEntries()); // trigger state plus merging window set
        assertEquals(2, timeAdaptor.numTimers(testHarness)); // window timers/gc timers
        assertEquals(0, timeAdaptor.numTimersOtherDomain(testHarness));

        timeAdaptor.advanceTime(testHarness, 19 + 20); // 19 is maxTime of the window

        verify(mockTrigger, times(1)).clear(anyTimeWindow(), anyTriggerContext());

        assertEquals(0, testHarness.numKeyedStateEntries()); // window contents plus trigger state
        assertEquals(0, timeAdaptor.numTimers(testHarness)); // window timers/gc timers
        assertEquals(0, timeAdaptor.numTimersOtherDomain(testHarness));
    }

    @Test
    public void testMergingWindowSetClearedAtEventTimeGarbageCollection() throws Exception {
        testMergingWindowSetClearedAtGarbageCollection(new EventTimeAdaptor());
    }

    @Test
    public void testMergingWindowSetClearedAtProcessingTimeGarbageCollection() throws Exception {
        testMergingWindowSetClearedAtGarbageCollection(new ProcessingTimeAdaptor());
    }

    private void testMergingWindowSetClearedAtGarbageCollection(TimeDomainAdaptor timeAdaptor)
            throws Exception {
        WindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 20L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 20)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(
                2, testHarness.numKeyedStateEntries()); // window contents plus merging window set
        assertEquals(1, timeAdaptor.numTimers(testHarness)); // gc timers

        timeAdaptor.advanceTime(testHarness, 19 + 20); // 19 is maxTime of the window

        assertEquals(0, testHarness.numKeyedStateEntries());
        assertEquals(0, timeAdaptor.numTimers(testHarness));
    }

    @Test
    public void testProcessingElementsWithinAllowedLateness() throws Exception {
        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 20L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 2)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        shouldFireOnElement(mockTrigger);

        // 20 is just at the limit, window.maxTime() is 1 and allowed lateness is 20
        testHarness.processWatermark(new Watermark(20));

        testHarness.processElement(new StreamRecord<>(0, 0L));

        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(0, 2)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());

        // clear is only called at cleanup time/GC time
        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        // FIRE should not purge contents
        assertEquals(1, testHarness.numKeyedStateEntries()); // window contents plus trigger state
        assertEquals(1, testHarness.numEventTimeTimers()); // just the GC timer
    }

    @Test
    public void testLateWindowDropping() throws Exception {
        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 20L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 2)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        shouldFireOnElement(mockTrigger);

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
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];
                                context.registerEventTimeTimer(0L);
                                context.getPartitionedState(valueStateDescriptor).update("hello");
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(
                        Matchers.<Integer>anyObject(),
                        anyLong(),
                        anyTimeWindow(),
                        anyTriggerContext());

        shouldFireAndPurgeOnEventTime(mockTrigger);

        testHarness.processElement(new StreamRecord<>(0, 0L));

        // window-contents and trigger state for two windows plus merging window set
        assertEquals(5, testHarness.numKeyedStateEntries());
        assertEquals(4, testHarness.numEventTimeTimers()); // timers/gc timers for two windows

        OperatorSubtaskState snapshot = testHarness.snapshot(0, 0);

        // restore
        mockAssigner = mockMergingAssigner();
        mockTrigger = mockTrigger();

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocation) throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[1];
                                context.deleteEventTimeTimer(0L);
                                context.getPartitionedState(valueStateDescriptor).clear();
                                return null;
                            }
                        })
                .when(mockTrigger)
                .clear(anyTimeWindow(), anyTriggerContext());

        // only fire on the timestamp==0L timers, not the gc timers
        when(mockTrigger.onEventTime(eq(0L), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE);

        mockWindowFunction = mockWindowFunction();

        testHarness = createWindowOperator(mockAssigner, mockTrigger, 0L, mockWindowFunction);

        testHarness.setup();
        testHarness.initializeState(snapshot);
        testHarness.open();

        assertEquals(0, testHarness.extractOutputStreamRecords().size());

        // verify that we still have all the state/timers/merging window set
        assertEquals(5, testHarness.numKeyedStateEntries());
        assertEquals(4, testHarness.numEventTimeTimers()); // timers/gc timers for two windows

        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        testHarness.processWatermark(new Watermark(20L));

        verify(mockTrigger, times(2)).clear(anyTimeWindow(), anyTriggerContext());

        verify(mockWindowFunction, times(2))
                .process(
                        eq(0),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(0, 2)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(0),
                        eq(new TimeWindow(2, 4)),
                        anyInternalWindowContext(),
                        intIterable(0),
                        WindowOperatorContractTest.<Void>anyCollector());

        // it's also called for the cleanup timers
        verify(mockTrigger, times(4)).onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext());
        verify(mockTrigger, times(1))
                .onEventTime(eq(0L), eq(new TimeWindow(0, 2)), anyTriggerContext());
        verify(mockTrigger, times(1))
                .onEventTime(eq(0L), eq(new TimeWindow(2, 4)), anyTriggerContext());

        assertEquals(0, testHarness.numKeyedStateEntries());
        assertEquals(0, testHarness.numEventTimeTimers());
    }

    @Test
    public void testPerWindowStateSetAndClearedOnEventTimePurge() throws Exception {
        testPerWindowStateSetAndClearedOnPurge(new EventTimeAdaptor());
    }

    @Test
    public void testPerWindowStateSetAndClearedOnProcessingTimePurge() throws Exception {
        testPerWindowStateSetAndClearedOnPurge(new ProcessingTimeAdaptor());
    }

    public void testPerWindowStateSetAndClearedOnPurge(TimeDomainAdaptor timeAdaptor)
            throws Exception {
        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 20L, mockWindowFunction);

        testHarness.open();

        when(mockTrigger.onElement(anyInt(), anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE);

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 20)));

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                InternalWindowFunction.InternalWindowContext context =
                                        (InternalWindowFunction.InternalWindowContext)
                                                invocationOnMock.getArguments()[2];
                                context.windowState()
                                        .getState(valueStateDescriptor)
                                        .update("hello");
                                return null;
                            }
                        })
                .when(mockWindowFunction)
                .process(
                        anyInt(),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<Void>anyCollector());

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                InternalWindowFunction.InternalWindowContext context =
                                        (InternalWindowFunction.InternalWindowContext)
                                                invocationOnMock.getArguments()[1];
                                context.windowState().getState(valueStateDescriptor).clear();
                                return null;
                            }
                        })
                .when(mockWindowFunction)
                .clear(anyTimeWindow(), anyInternalWindowContext());

        assertEquals(0, testHarness.getOutput().size());
        assertEquals(0, testHarness.numKeyedStateEntries());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertEquals(2, testHarness.numKeyedStateEntries()); // window contents plus value state
        assertEquals(1, timeAdaptor.numTimers(testHarness)); // gc timers

        timeAdaptor.advanceTime(testHarness, 19 + 20); // 19 is maxTime of the window

        assertEquals(0, testHarness.numKeyedStateEntries());
        assertEquals(0, timeAdaptor.numTimers(testHarness));
    }

    @Test
    public void testWindowStateNotAvailableToMergingWindows() throws Exception {
        WindowAssigner<Integer, TimeWindow> mockAssigner = mockMergingAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 20L, mockWindowFunction);

        testHarness.open();

        when(mockTrigger.onElement(anyInt(), anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE);

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 20)));

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                InternalWindowFunction.InternalWindowContext context =
                                        (InternalWindowFunction.InternalWindowContext)
                                                invocationOnMock.getArguments()[2];
                                context.windowState()
                                        .getState(valueStateDescriptor)
                                        .update("hello");
                                return null;
                            }
                        })
                .when(mockWindowFunction)
                .process(
                        anyInt(),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<Void>anyCollector());

        expectedException.expect(UnsupportedOperationException.class);
        expectedException.expectMessage(
                "Per-window state is not allowed when using merging windows.");
        testHarness.processElement(new StreamRecord<>(0, 0L));
    }

    @Test
    public void testEventTimeQuerying() throws Exception {
        testCurrentTimeQuerying(new EventTimeAdaptor());
    }

    @Test
    public void testProcessingTimeQuerying() throws Exception {
        testCurrentTimeQuerying(new ProcessingTimeAdaptor());
    }

    @Test
    public void testStateTypeIsConsistentFromWindowStateAndGlobalState() throws Exception {

        class NoOpAggregateFunction implements AggregateFunction<String, String, String> {

            @Override
            public String createAccumulator() {
                return null;
            }

            @Override
            public String add(String value, String accumulator) {
                return null;
            }

            @Override
            public String getResult(String accumulator) {
                return null;
            }

            @Override
            public String merge(String a, String b) {
                return null;
            }
        }

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 20L, mockWindowFunction);

        testHarness.open();

        when(mockTrigger.onElement(anyInt(), anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE);

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 20)));

        AtomicBoolean processWasInvoked = new AtomicBoolean(false);

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                InternalWindowFunction.InternalWindowContext context =
                                        (InternalWindowFunction.InternalWindowContext)
                                                invocationOnMock.getArguments()[2];
                                KeyedStateStore windowKeyedStateStore = context.windowState();
                                KeyedStateStore globalKeyedStateStore = context.globalState();

                                ListStateDescriptor<String> windowListStateDescriptor =
                                        new ListStateDescriptor<String>(
                                                "windowListState", String.class);
                                ListStateDescriptor<String> globalListStateDescriptor =
                                        new ListStateDescriptor<String>(
                                                "globalListState", String.class);
                                assertEquals(
                                        windowKeyedStateStore
                                                .getListState(windowListStateDescriptor)
                                                .getClass(),
                                        globalKeyedStateStore
                                                .getListState(globalListStateDescriptor)
                                                .getClass());

                                ValueStateDescriptor<String> windowValueStateDescriptor =
                                        new ValueStateDescriptor<String>(
                                                "windowValueState", String.class);
                                ValueStateDescriptor<String> globalValueStateDescriptor =
                                        new ValueStateDescriptor<String>(
                                                "globalValueState", String.class);
                                assertEquals(
                                        windowKeyedStateStore
                                                .getState(windowValueStateDescriptor)
                                                .getClass(),
                                        globalKeyedStateStore
                                                .getState(globalValueStateDescriptor)
                                                .getClass());

                                AggregatingStateDescriptor<String, String, String>
                                        windowAggStateDesc =
                                                new AggregatingStateDescriptor<
                                                        String, String, String>(
                                                        "windowAgg",
                                                        new NoOpAggregateFunction(),
                                                        String.class);

                                AggregatingStateDescriptor<String, String, String>
                                        globalAggStateDesc =
                                                new AggregatingStateDescriptor<
                                                        String, String, String>(
                                                        "globalAgg",
                                                        new NoOpAggregateFunction(),
                                                        String.class);
                                assertEquals(
                                        windowKeyedStateStore
                                                .getAggregatingState(windowAggStateDesc)
                                                .getClass(),
                                        globalKeyedStateStore
                                                .getAggregatingState(globalAggStateDesc)
                                                .getClass());

                                ReducingStateDescriptor<String> windowReducingStateDesc =
                                        new ReducingStateDescriptor<String>(
                                                "windowReducing", (a, b) -> a, String.class);
                                ReducingStateDescriptor<String> globalReducingStateDesc =
                                        new ReducingStateDescriptor<String>(
                                                "globalReducing", (a, b) -> a, String.class);
                                assertEquals(
                                        windowKeyedStateStore
                                                .getReducingState(windowReducingStateDesc)
                                                .getClass(),
                                        globalKeyedStateStore
                                                .getReducingState(globalReducingStateDesc)
                                                .getClass());

                                MapStateDescriptor<String, String> windowMapStateDescriptor =
                                        new MapStateDescriptor<String, String>(
                                                "windowMapState", String.class, String.class);
                                MapStateDescriptor<String, String> globalMapStateDescriptor =
                                        new MapStateDescriptor<String, String>(
                                                "globalMapState", String.class, String.class);
                                assertEquals(
                                        windowKeyedStateStore
                                                .getMapState(windowMapStateDescriptor)
                                                .getClass(),
                                        globalKeyedStateStore
                                                .getMapState(globalMapStateDescriptor)
                                                .getClass());

                                processWasInvoked.set(true);
                                return null;
                            }
                        })
                .when(mockWindowFunction)
                .process(
                        anyInt(),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<Void>anyCollector());

        testHarness.processElement(new StreamRecord<>(0, 0L));

        assertTrue(processWasInvoked.get());
    }

    public void testCurrentTimeQuerying(final TimeDomainAdaptor timeAdaptor) throws Exception {
        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        timeAdaptor.setIsEventTime(mockAssigner);
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Iterable<Integer>, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

        final KeyedOneInputStreamOperatorTestHarness<Integer, Integer, Void> testHarness =
                createWindowOperator(mockAssigner, mockTrigger, 20L, mockWindowFunction);

        testHarness.open();

        shouldFireOnElement(mockTrigger);

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(0, 20)));

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                InternalWindowFunction.InternalWindowContext context =
                                        (InternalWindowFunction.InternalWindowContext)
                                                invocationOnMock.getArguments()[2];
                                timeAdaptor.verifyCorrectTime(testHarness, context);
                                return null;
                            }
                        })
                .when(mockWindowFunction)
                .process(
                        anyInt(),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<Void>anyCollector());

        doAnswer(
                        new Answer<Object>() {
                            @Override
                            public Object answer(InvocationOnMock invocationOnMock)
                                    throws Throwable {
                                InternalWindowFunction.InternalWindowContext context =
                                        (InternalWindowFunction.InternalWindowContext)
                                                invocationOnMock.getArguments()[1];
                                timeAdaptor.verifyCorrectTime(testHarness, context);
                                return null;
                            }
                        })
                .when(mockWindowFunction)
                .clear(anyTimeWindow(), anyInternalWindowContext());

        timeAdaptor.advanceTime(testHarness, 10);

        testHarness.processElement(new StreamRecord<>(0, 0L));

        verify(mockWindowFunction, times(1))
                .process(
                        anyInt(),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyIntIterable(),
                        WindowOperatorContractTest.<Void>anyCollector());

        timeAdaptor.advanceTime(testHarness, 100);

        verify(mockWindowFunction, times(1)).clear(anyTimeWindow(), anyInternalWindowContext());
    }

    protected abstract <W extends Window, OUT>
            KeyedOneInputStreamOperatorTestHarness<Integer, Integer, OUT> createWindowOperator(
                    WindowAssigner<Integer, W> assigner,
                    Trigger<Integer, W> trigger,
                    long allowedLateness,
                    InternalWindowFunction<Iterable<Integer>, OUT, Integer, W> windowFunction,
                    OutputTag<Integer> lateOutputTag)
                    throws Exception;

    protected abstract <W extends Window, OUT>
            KeyedOneInputStreamOperatorTestHarness<Integer, Integer, OUT> createWindowOperator(
                    WindowAssigner<Integer, W> assigner,
                    Trigger<Integer, W> trigger,
                    long allowedLatenss,
                    InternalWindowFunction<Iterable<Integer>, OUT, Integer, W> windowFunction)
                    throws Exception;

    private interface TimeDomainAdaptor {

        void setIsEventTime(WindowAssigner<?, ?> mockAssigner);

        void advanceTime(OneInputStreamOperatorTestHarness testHarness, long timestamp)
                throws Exception;

        void registerTimer(Trigger.TriggerContext ctx, long timestamp);

        void deleteTimer(Trigger.TriggerContext ctx, long timestamp);

        int numTimers(AbstractStreamOperatorTestHarness testHarness);

        int numTimersOtherDomain(AbstractStreamOperatorTestHarness testHarness);

        void shouldRegisterTimerOnElement(Trigger<?, TimeWindow> mockTrigger, long timestamp)
                throws Exception;

        void shouldDeleteTimerOnElement(Trigger<?, TimeWindow> mockTrigger, long timestamp)
                throws Exception;

        void shouldContinueOnTime(Trigger<?, TimeWindow> mockTrigger) throws Exception;

        void shouldFireOnTime(Trigger<?, TimeWindow> mockTrigger) throws Exception;

        void shouldFireAndPurgeOnTime(Trigger<?, TimeWindow> mockTrigger) throws Exception;

        void shouldPurgeOnTime(Trigger<?, TimeWindow> mockTrigger) throws Exception;

        void verifyTriggerCallback(
                Trigger<?, TimeWindow> mockTrigger,
                VerificationMode verificationMode,
                Long time,
                TimeWindow window)
                throws Exception;

        void verifyCorrectTime(
                OneInputStreamOperatorTestHarness testHarness,
                InternalWindowFunction.InternalWindowContext context);
    }

    private static class EventTimeAdaptor implements TimeDomainAdaptor {

        @Override
        public void setIsEventTime(WindowAssigner<?, ?> mockAssigner) {
            when(mockAssigner.isEventTime()).thenReturn(true);
        }

        public void advanceTime(OneInputStreamOperatorTestHarness testHarness, long timestamp)
                throws Exception {
            testHarness.processWatermark(new Watermark(timestamp));
        }

        @Override
        public void registerTimer(Trigger.TriggerContext ctx, long timestamp) {
            ctx.registerEventTimeTimer(timestamp);
        }

        @Override
        public void deleteTimer(Trigger.TriggerContext ctx, long timestamp) {
            ctx.deleteEventTimeTimer(timestamp);
        }

        @Override
        public int numTimers(AbstractStreamOperatorTestHarness testHarness) {
            return testHarness.numEventTimeTimers();
        }

        @Override
        public int numTimersOtherDomain(AbstractStreamOperatorTestHarness testHarness) {
            return testHarness.numProcessingTimeTimers();
        }

        @Override
        public void shouldRegisterTimerOnElement(Trigger<?, TimeWindow> mockTrigger, long timestamp)
                throws Exception {
            shouldRegisterEventTimeTimerOnElement(mockTrigger, timestamp);
        }

        @Override
        public void shouldDeleteTimerOnElement(Trigger<?, TimeWindow> mockTrigger, long timestamp)
                throws Exception {
            shouldDeleteEventTimeTimerOnElement(mockTrigger, timestamp);
        }

        @Override
        public void shouldContinueOnTime(Trigger<?, TimeWindow> mockTrigger) throws Exception {
            shouldContinueOnEventTime(mockTrigger);
        }

        @Override
        public void shouldFireOnTime(Trigger<?, TimeWindow> mockTrigger) throws Exception {
            shouldFireOnEventTime(mockTrigger);
        }

        @Override
        public void shouldFireAndPurgeOnTime(Trigger<?, TimeWindow> mockTrigger) throws Exception {
            shouldFireAndPurgeOnEventTime(mockTrigger);
        }

        @Override
        public void shouldPurgeOnTime(Trigger<?, TimeWindow> mockTrigger) throws Exception {
            shouldPurgeOnEventTime(mockTrigger);
        }

        @Override
        public void verifyTriggerCallback(
                Trigger<?, TimeWindow> mockTrigger,
                VerificationMode verificationMode,
                Long time,
                TimeWindow window)
                throws Exception {
            if (time == null && window == null) {
                verify(mockTrigger, verificationMode)
                        .onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext());
            } else if (time == null) {
                verify(mockTrigger, verificationMode)
                        .onEventTime(anyLong(), eq(window), anyTriggerContext());
            } else if (window == null) {
                verify(mockTrigger, verificationMode)
                        .onEventTime(eq(time), anyTimeWindow(), anyTriggerContext());
            } else {
                verify(mockTrigger, verificationMode)
                        .onEventTime(eq(time), eq(window), anyTriggerContext());
            }
        }

        @Override
        public void verifyCorrectTime(
                OneInputStreamOperatorTestHarness testHarness,
                InternalWindowFunction.InternalWindowContext context) {
            assertEquals(testHarness.getCurrentWatermark(), context.currentWatermark());
        }
    }

    private static class ProcessingTimeAdaptor implements TimeDomainAdaptor {

        @Override
        public void setIsEventTime(WindowAssigner<?, ?> mockAssigner) {
            when(mockAssigner.isEventTime()).thenReturn(false);
        }

        public void advanceTime(OneInputStreamOperatorTestHarness testHarness, long timestamp)
                throws Exception {
            testHarness.setProcessingTime(timestamp);
        }

        @Override
        public void registerTimer(Trigger.TriggerContext ctx, long timestamp) {
            ctx.registerProcessingTimeTimer(timestamp);
        }

        @Override
        public void deleteTimer(Trigger.TriggerContext ctx, long timestamp) {
            ctx.deleteProcessingTimeTimer(timestamp);
        }

        @Override
        public int numTimers(AbstractStreamOperatorTestHarness testHarness) {
            return testHarness.numProcessingTimeTimers();
        }

        @Override
        public int numTimersOtherDomain(AbstractStreamOperatorTestHarness testHarness) {
            return testHarness.numEventTimeTimers();
        }

        @Override
        public void shouldRegisterTimerOnElement(Trigger<?, TimeWindow> mockTrigger, long timestamp)
                throws Exception {
            shouldRegisterProcessingTimeTimerOnElement(mockTrigger, timestamp);
        }

        @Override
        public void shouldDeleteTimerOnElement(Trigger<?, TimeWindow> mockTrigger, long timestamp)
                throws Exception {
            shouldDeleteProcessingTimeTimerOnElement(mockTrigger, timestamp);
        }

        @Override
        public void shouldContinueOnTime(Trigger<?, TimeWindow> mockTrigger) throws Exception {
            shouldContinueOnProcessingTime(mockTrigger);
        }

        @Override
        public void shouldFireOnTime(Trigger<?, TimeWindow> mockTrigger) throws Exception {
            shouldFireOnProcessingTime(mockTrigger);
        }

        @Override
        public void shouldFireAndPurgeOnTime(Trigger<?, TimeWindow> mockTrigger) throws Exception {
            shouldFireAndPurgeOnProcessingTime(mockTrigger);
        }

        @Override
        public void shouldPurgeOnTime(Trigger<?, TimeWindow> mockTrigger) throws Exception {
            shouldPurgeOnProcessingTime(mockTrigger);
        }

        @Override
        public void verifyTriggerCallback(
                Trigger<?, TimeWindow> mockTrigger,
                VerificationMode verificationMode,
                Long time,
                TimeWindow window)
                throws Exception {
            if (time == null && window == null) {
                verify(mockTrigger, verificationMode)
                        .onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext());
            } else if (time == null) {
                verify(mockTrigger, verificationMode)
                        .onProcessingTime(anyLong(), eq(window), anyTriggerContext());
            } else if (window == null) {
                verify(mockTrigger, verificationMode)
                        .onProcessingTime(eq(time), anyTimeWindow(), anyTriggerContext());
            } else {
                verify(mockTrigger, verificationMode)
                        .onProcessingTime(eq(time), eq(window), anyTriggerContext());
            }
        }

        @Override
        public void verifyCorrectTime(
                OneInputStreamOperatorTestHarness testHarness,
                InternalWindowFunction.InternalWindowContext context) {
            assertEquals(testHarness.getProcessingTime(), context.currentProcessingTime());
        }
    }
}
