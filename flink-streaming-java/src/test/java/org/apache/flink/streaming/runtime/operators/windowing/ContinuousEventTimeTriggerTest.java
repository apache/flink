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

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link ContinuousEventTimeTrigger}. */
public class ContinuousEventTimeTriggerTest {

    /**
     * Verify that the trigger doesn't fail with an NPE if we insert a timer firing when there is no
     * trigger state.
     */
    @Test
    public void testTriggerHandlesAllOnTimerCalls() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(
                        ContinuousEventTimeTrigger.<TimeWindow>of(Time.milliseconds(5)),
                        new TimeWindow.Serializer());

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(0, testHarness.numEventTimeTimers());

        // this will make the elements we now process fall into late windows, i.e. no trigger state
        // will be created
        testHarness.advanceWatermark(10);

        // late fires immediately
        assertEquals(
                TriggerResult.FIRE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(0, 2)));

        // simulate a GC timer firing
        testHarness.invokeOnEventTime(20, new TimeWindow(0, 2));
    }

    /** Verify that state &lt;TimeWindow&gt;of separate windows does not leak into other windows. */
    @Test
    public void testWindowSeparationAndFiring() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(
                        ContinuousEventTimeTrigger.<TimeWindow>of(Time.hours(1)),
                        new TimeWindow.Serializer());

        // inject several elements
        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(0, 2)));
        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(0, 2)));
        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(0, 2)));
        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(2, 4)));
        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(2, 4)));

        assertEquals(2, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(4, testHarness.numEventTimeTimers());
        assertEquals(2, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(2, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));

        Collection<Tuple2<TimeWindow, TriggerResult>> triggerResults =
                testHarness.advanceWatermark(2);
        boolean sawFiring = false;
        for (Tuple2<TimeWindow, TriggerResult> r : triggerResults) {
            if (r.f0.equals(new TimeWindow(0, 2))) {
                sawFiring = true;
                assertTrue(r.f1.equals(TriggerResult.FIRE));
            }
        }
        assertTrue(sawFiring);

        assertEquals(2, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(2, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));

        triggerResults = testHarness.advanceWatermark(4);
        sawFiring = false;
        for (Tuple2<TimeWindow, TriggerResult> r : triggerResults) {
            if (r.f0.equals(new TimeWindow(2, 4))) {
                sawFiring = true;
                assertTrue(r.f1.equals(TriggerResult.FIRE));
            }
        }
        assertTrue(sawFiring);

        assertEquals(2, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(2, testHarness.numEventTimeTimers());
    }

    /**
     * Verify that late elements trigger immediately and also that we don't set a timer for those.
     */
    @Test
    public void testLateElementTriggersImmediately() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(
                        ContinuousEventTimeTrigger.<TimeWindow>of(Time.hours(1)),
                        new TimeWindow.Serializer());

        testHarness.advanceWatermark(2);

        assertEquals(
                TriggerResult.FIRE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(0, 2)));

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(0, testHarness.numEventTimeTimers());
    }

    /** Verify that clear() does not leak across windows. */
    @Test
    public void testClear() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(
                        ContinuousEventTimeTrigger.<TimeWindow>of(Time.hours(1)),
                        new TimeWindow.Serializer());

        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(0, 2)));
        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(2, 4)));

        assertEquals(2, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(4, testHarness.numEventTimeTimers());
        assertEquals(2, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(2, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));

        testHarness.clearTriggerState(new TimeWindow(2, 4));

        assertEquals(1, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(3, testHarness.numEventTimeTimers());
        assertEquals(2, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));

        testHarness.clearTriggerState(new TimeWindow(0, 2));

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(2, testHarness.numEventTimeTimers()); // doesn't clean up timers
    }

    @Test
    public void testMergingWindows() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(
                        ContinuousEventTimeTrigger.<TimeWindow>of(Time.hours(1)),
                        new TimeWindow.Serializer());

        assertTrue(ContinuousEventTimeTrigger.<TimeWindow>of(Time.hours(1)).canMerge());

        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(0, 2)));
        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(2, 4)));

        assertEquals(2, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(4, testHarness.numEventTimeTimers());
        assertEquals(2, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(2, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));

        testHarness.mergeWindows(
                new TimeWindow(0, 4),
                Lists.newArrayList(new TimeWindow(0, 2), new TimeWindow(2, 4)));

        assertEquals(1, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(5, testHarness.numEventTimeTimers()); // on merging, timers are not cleaned up
        assertEquals(2, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(2, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(0, 4)));

        Collection<Tuple2<TimeWindow, TriggerResult>> triggerResults =
                testHarness.advanceWatermark(4);
        boolean sawFiring = false;
        for (Tuple2<TimeWindow, TriggerResult> r : triggerResults) {
            if (r.f0.equals(new TimeWindow(0, 4))) {
                sawFiring = true;
                assertTrue(r.f1.equals(TriggerResult.FIRE));
            }
        }

        assertTrue(sawFiring);

        assertEquals(1, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(1, testHarness.numEventTimeTimers());
    }
}
