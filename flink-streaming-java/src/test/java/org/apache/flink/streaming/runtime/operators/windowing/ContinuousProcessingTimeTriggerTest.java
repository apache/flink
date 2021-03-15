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
import org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

/** Tests for {@link ContinuousProcessingTimeTrigger}. */
public class ContinuousProcessingTimeTriggerTest {
    @Test
    public void testMergingWindows() throws Exception {
        ContinuousProcessingTimeTrigger<TimeWindow> trigger =
                ContinuousProcessingTimeTrigger.of(Time.seconds(2));

        assertTrue(trigger.canMerge());

        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(trigger, new TimeWindow.Serializer());

        TimeWindow window1 = new TimeWindow(0, 2);
        TimeWindow window2 = new TimeWindow(2, 4);

        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)));
        assertEquals(
                TriggerResult.CONTINUE,
                testHarness.processElement(new StreamRecord<>(1), new TimeWindow(2, 4)));
        assertEquals(2, testHarness.numStateEntries());
        assertEquals(1, testHarness.numProcessingTimeTimers(new TimeWindow(0, 2)));
        assertEquals(1, testHarness.numProcessingTimeTimers(new TimeWindow(2, 4)));

        // window1 and window2 will be merged out.
        testHarness.mergeWindows(new TimeWindow(0, 4), Lists.newArrayList(window1, window2));
        assertEquals(1, testHarness.numStateEntries());
        assertEquals(1, testHarness.numProcessingTimeTimers(new TimeWindow(0, 4)));

        // old timers does not get chance to cleanup due to state merged out.
        assertEquals(1, testHarness.numProcessingTimeTimers(new TimeWindow(0, 2)));
        assertEquals(1, testHarness.numProcessingTimeTimers(new TimeWindow(2, 4)));

        Collection<Tuple2<TimeWindow, TriggerResult>> triggerResults =
                testHarness.advanceProcessingTime(4);
        assertThat(
                triggerResults,
                Matchers.hasItem(new Tuple2<>(new TimeWindow(0, 4), TriggerResult.FIRE)));

        assertEquals(1, testHarness.numProcessingTimeTimers());
        assertEquals(1, testHarness.numProcessingTimeTimers(new TimeWindow(0, 4)));

        // old timers are fired and does not get chance to register due to state merged out.
        assertEquals(0, testHarness.numProcessingTimeTimers(new TimeWindow(0, 2)));
        assertEquals(0, testHarness.numProcessingTimeTimers(new TimeWindow(2, 4)));
    }
}
