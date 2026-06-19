/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.runtime.operators.windowing;

import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.lang.reflect.Method;
import java.util.Collections;

import static org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorContractTest.anyOnMergeContext;
import static org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorContractTest.anyTimeWindow;
import static org.apache.flink.streaming.runtime.operators.windowing.WindowOperatorContractTest.anyTriggerContext;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests for {@link PurgingTrigger}. */
class PurgingTriggerTest {

    /**
     * Check if {@link PurgingTrigger} implements all methods of {@link Trigger}, as a sanity check.
     */
    @Test
    void testAllMethodsImplemented() throws NoSuchMethodException {
        for (Method triggerMethod : Trigger.class.getDeclaredMethods()) {

            // try retrieving the method, this will throw an exception if we can't find it
            PurgingTrigger.class.getDeclaredMethod(
                    triggerMethod.getName(), triggerMethod.getParameterTypes());
        }
    }

    @Test
    void testForwarding() throws Exception {
        Trigger<Object, TimeWindow> mockTrigger = mock(Trigger.class);

        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(
                        PurgingTrigger.of(mockTrigger), new TimeWindow.Serializer());

        when(mockTrigger.onElement(
                        ArgumentMatchers.any(), anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);

        when(mockTrigger.onElement(
                        ArgumentMatchers.any(), anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE_AND_PURGE);

        when(mockTrigger.onElement(
                        ArgumentMatchers.any(), anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE_AND_PURGE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE_AND_PURGE);

        when(mockTrigger.onElement(
                        ArgumentMatchers.any(), anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.PURGE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.PURGE);

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];

                                // register some timers that we can step through to call onEventTime
                                // several
                                // times in a row
                                context.registerEventTimeTimer(1);
                                context.registerEventTimeTimer(2);
                                context.registerEventTimeTimer(3);
                                context.registerEventTimeTimer(4);
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(ArgumentMatchers.any(), anyLong(), anyTimeWindow(), anyTriggerContext());

        // set up our timers
        testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2));

        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 2))).isEqualTo(4);

        when(mockTrigger.onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.CONTINUE);
        assertThat(testHarness.advanceWatermark(1, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);

        when(mockTrigger.onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE);
        assertThat(testHarness.advanceWatermark(2, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE_AND_PURGE);

        when(mockTrigger.onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE_AND_PURGE);
        assertThat(testHarness.advanceWatermark(3, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE_AND_PURGE);

        when(mockTrigger.onEventTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.PURGE);
        assertThat(testHarness.advanceWatermark(4, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.PURGE);

        doAnswer(
                        new Answer<TriggerResult>() {
                            @Override
                            public TriggerResult answer(InvocationOnMock invocation)
                                    throws Exception {
                                Trigger.TriggerContext context =
                                        (Trigger.TriggerContext) invocation.getArguments()[3];

                                // register some timers that we can step through to call onEventTime
                                // several
                                // times in a row
                                context.registerProcessingTimeTimer(1);
                                context.registerProcessingTimeTimer(2);
                                context.registerProcessingTimeTimer(3);
                                context.registerProcessingTimeTimer(4);
                                return TriggerResult.CONTINUE;
                            }
                        })
                .when(mockTrigger)
                .onElement(ArgumentMatchers.any(), anyLong(), anyTimeWindow(), anyTriggerContext());

        // set up our timers
        testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2));

        assertThat(testHarness.numProcessingTimeTimers(new TimeWindow(0, 2))).isEqualTo(4);
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 2))).isZero();

        when(mockTrigger.onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.CONTINUE);
        assertThat(testHarness.advanceProcessingTime(1, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);

        when(mockTrigger.onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE);
        assertThat(testHarness.advanceProcessingTime(2, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE_AND_PURGE);

        when(mockTrigger.onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.FIRE_AND_PURGE);
        assertThat(testHarness.advanceProcessingTime(3, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE_AND_PURGE);

        when(mockTrigger.onProcessingTime(anyLong(), anyTimeWindow(), anyTriggerContext()))
                .thenReturn(TriggerResult.PURGE);
        assertThat(testHarness.advanceProcessingTime(4, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.PURGE);

        testHarness.mergeWindows(
                new TimeWindow(0, 2), Collections.singletonList(new TimeWindow(0, 1)));
        verify(mockTrigger, times(1)).onMerge(anyTimeWindow(), anyOnMergeContext());

        testHarness.clearTriggerState(new TimeWindow(0, 2));
        verify(mockTrigger, times(1)).clear(eq(new TimeWindow(0, 2)), anyTriggerContext());
    }
}
