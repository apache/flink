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

import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Tests for {@link EventTimeTrigger}. */
public class EventTimeTriggerTest {

    /** Verify that state of separate windows does not leak into other windows. */
    @Test
    public void testWindowSeparationAndFiring() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(EventTimeTrigger.create(), new TimeWindow.Serializer());

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

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(2, testHarness.numEventTimeTimers());
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));

        assertEquals(TriggerResult.FIRE, testHarness.advanceWatermark(2, new TimeWindow(0, 2)));

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(1, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));

        assertEquals(TriggerResult.FIRE, testHarness.advanceWatermark(4, new TimeWindow(2, 4)));

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(0, testHarness.numEventTimeTimers());
    }

    /**
     * Verify that late elements trigger immediately and also that we don't set a timer for those.
     */
    @Test
    public void testLateElementTriggersImmediately() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(EventTimeTrigger.create(), new TimeWindow.Serializer());

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
                new TriggerTestHarness<>(EventTimeTrigger.create(), new TimeWindow.Serializer());

        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(0, 2)));
        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(2, 4)));

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(2, testHarness.numEventTimeTimers());
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));

        testHarness.clearTriggerState(new TimeWindow(2, 4));

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(1, testHarness.numEventTimeTimers());
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(0, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));

        testHarness.clearTriggerState(new TimeWindow(0, 2));

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(0, testHarness.numEventTimeTimers());
    }

    @Test
    public void testMergingWindows() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(EventTimeTrigger.create(), new TimeWindow.Serializer());

        assertTrue(EventTimeTrigger.create().canMerge());

        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(0, 2)));
        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(2, 4)));

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(2, testHarness.numEventTimeTimers());
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));

        testHarness.mergeWindows(
                new TimeWindow(0, 4),
                Lists.newArrayList(new TimeWindow(0, 2), new TimeWindow(2, 4)));

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(1, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(0, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(0, 4)));

        assertEquals(TriggerResult.FIRE, testHarness.advanceWatermark(4, new TimeWindow(0, 4)));

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(0, testHarness.numEventTimeTimers());
    }

    /**
     * Merging a late window should not register a timer, otherwise we would get two firings: one
     * from onElement() on the merged window and one from the timer.
     */
    @Test
    public void testMergingLateWindows() throws Exception {

        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(EventTimeTrigger.create(), new TimeWindow.Serializer());

        assertTrue(EventTimeTrigger.create().canMerge());

        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(0, 2)));
        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<Object>(1), new TimeWindow(2, 4)));

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(2, testHarness.numEventTimeTimers());
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(1, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));

        testHarness.advanceWatermark(10);

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(0, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));

        testHarness.mergeWindows(
                new TimeWindow(0, 4),
                Lists.newArrayList(new TimeWindow(0, 2), new TimeWindow(2, 4)));

        assertEquals(0, testHarness.numStateEntries());
        assertEquals(0, testHarness.numProcessingTimeTimers());
        assertEquals(0, testHarness.numEventTimeTimers());
        assertEquals(0, testHarness.numEventTimeTimers(new TimeWindow(0, 2)));
        assertEquals(0, testHarness.numEventTimeTimers(new TimeWindow(2, 4)));
        assertEquals(0, testHarness.numEventTimeTimers(new TimeWindow(0, 4)));
    }
}
