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
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AppendingState;
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

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * These tests verify that {@link WindowOperator} correctly interacts with the other windowing
 * components: {@link WindowAssigner}, {@link Trigger}. {@link
 * org.apache.flink.streaming.api.functions.windowing.WindowFunction} and window state.
 *
 * <p>These tests document the implicit contract that exists between the windowing components.
 */
class RegularWindowOperatorContractTest extends WindowOperatorContractTest {

    @Test
    void testReducingWindow() throws Exception {

        WindowAssigner<Integer, TimeWindow> mockAssigner = mockTimeWindowAssigner();
        Trigger<Integer, TimeWindow> mockTrigger = mockTrigger();
        InternalWindowFunction<Integer, Void, Integer, TimeWindow> mockWindowFunction =
                mockWindowFunction();

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
                createWindowOperator(
                        mockAssigner, mockTrigger, 0L, intReduceSumDescriptor, mockWindowFunction);

        testHarness.open();

        when(mockAssigner.assignWindows(anyInt(), anyLong(), anyAssignerContext()))
                .thenReturn(Arrays.asList(new TimeWindow(2, 4), new TimeWindow(0, 2)));

        assertThat(testHarness.getOutput()).isEmpty();
        assertThat(testHarness.numKeyedStateEntries()).isZero();

        // insert two elements without firing
        testHarness.processElement(new StreamRecord<>(1, 0L));
        testHarness.processElement(new StreamRecord<>(1, 0L));

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
                .onElement(ArgumentMatchers.any(), anyLong(), anyTimeWindow(), anyTriggerContext());

        testHarness.processElement(new StreamRecord<>(1, 0L));

        verify(mockWindowFunction, times(2))
                .process(
                        eq(1),
                        anyTimeWindow(),
                        anyInternalWindowContext(),
                        anyInt(),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(1),
                        eq(new TimeWindow(0, 2)),
                        anyInternalWindowContext(),
                        eq(3),
                        WindowOperatorContractTest.<Void>anyCollector());
        verify(mockWindowFunction, times(1))
                .process(
                        eq(1),
                        eq(new TimeWindow(2, 4)),
                        anyInternalWindowContext(),
                        eq(3),
                        WindowOperatorContractTest.<Void>anyCollector());

        // clear is only called at cleanup time/GC time
        verify(mockTrigger, never()).clear(anyTimeWindow(), anyTriggerContext());

        // FIRE should not purge contents
        assertThat(testHarness.numKeyedStateEntries())
                .isEqualTo(4); // window contents plus trigger state
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(4); // window timers/gc timers
    }

    /**
     * Special method for creating a {@link WindowOperator} with a custom {@link StateDescriptor}
     * for the window contents state.
     */
    private <W extends Window, ACC, OUT>
            KeyedOneInputStreamOperatorTestHarness<Integer, Integer, OUT> createWindowOperator(
                    WindowAssigner<Integer, W> assigner,
                    Trigger<Integer, W> trigger,
                    long allowedLatenss,
                    StateDescriptor<? extends AppendingState<Integer, ACC>, ?> stateDescriptor,
                    InternalWindowFunction<ACC, OUT, Integer, W> windowFunction)
                    throws Exception {

        KeySelector<Integer, Integer> keySelector =
                new KeySelector<Integer, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                };

        @SuppressWarnings("unchecked")
        WindowOperatorFactory<Integer, Integer, ACC, OUT, W> operator =
                new WindowOperatorFactory<>(
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
                operator, keySelector, BasicTypeInfo.INT_TYPE_INFO);
    }

    @Override
    protected <W extends Window, OUT>
            KeyedOneInputStreamOperatorTestHarness<Integer, Integer, OUT> createWindowOperator(
                    WindowAssigner<Integer, W> assigner,
                    Trigger<Integer, W> trigger,
                    long allowedLatenss,
                    InternalWindowFunction<Iterable<Integer>, OUT, Integer, W> windowFunction,
                    OutputTag<Integer> lateOutputTag)
                    throws Exception {

        KeySelector<Integer, Integer> keySelector =
                new KeySelector<Integer, Integer>() {
                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer getKey(Integer value) throws Exception {
                        return value;
                    }
                };

        ListStateDescriptor<Integer> intListDescriptor =
                new ListStateDescriptor<>("int-list", IntSerializer.INSTANCE);

        @SuppressWarnings("unchecked")
        WindowOperatorFactory<Integer, Integer, Iterable<Integer>, OUT, W> operator =
                new WindowOperatorFactory<>(
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
                operator, keySelector, BasicTypeInfo.INT_TYPE_INFO);
    }

    @Override
    protected <W extends Window, OUT>
            KeyedOneInputStreamOperatorTestHarness<Integer, Integer, OUT> createWindowOperator(
                    WindowAssigner<Integer, W> assigner,
                    Trigger<Integer, W> trigger,
                    long allowedLatenss,
                    InternalWindowFunction<Iterable<Integer>, OUT, Integer, W> windowFunction)
                    throws Exception {

        return createWindowOperator(
                assigner, trigger, allowedLatenss, windowFunction, null /* late output tag */);
    }
}
