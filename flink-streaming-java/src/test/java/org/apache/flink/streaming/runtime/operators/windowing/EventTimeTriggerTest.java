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

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link EventTimeTrigger}. */
class EventTimeTriggerTest {

    /** Verify that state of separate windows does not leak into other windows. */
    @Test
    void testWindowSeparationAndFiring() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(EventTimeTrigger.create(), new TimeWindow.Serializer());

        // inject several elements
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(2, 4)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(2, 4)))
                .isEqualTo(TriggerResult.CONTINUE);

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(2);
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 2))).isOne();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(2, 4))).isOne();

        assertThat(testHarness.advanceWatermark(2, new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE);

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isOne();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 2))).isZero();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(2, 4))).isOne();

        assertThat(testHarness.advanceWatermark(4, new TimeWindow(2, 4)))
                .isEqualTo(TriggerResult.FIRE);

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isZero();
    }

    /**
     * Verify that late elements trigger immediately and also that we don't set a timer for those.
     */
    @Test
    void testLateElementTriggersImmediately() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(EventTimeTrigger.create(), new TimeWindow.Serializer());

        testHarness.advanceWatermark(2);

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE);

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isZero();
    }

    /** Verify that clear() does not leak across windows. */
    @Test
    void testClear() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(EventTimeTrigger.create(), new TimeWindow.Serializer());

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(2, 4)))
                .isEqualTo(TriggerResult.CONTINUE);

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(2);
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 2))).isOne();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(2, 4))).isOne();

        testHarness.clearTriggerState(new TimeWindow(2, 4));

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isOne();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 2))).isOne();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(2, 4))).isZero();

        testHarness.clearTriggerState(new TimeWindow(0, 2));

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isZero();
    }

    @Test
    void testMergingWindows() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(EventTimeTrigger.create(), new TimeWindow.Serializer());

        assertThat(EventTimeTrigger.create().canMerge()).isTrue();

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(2, 4)))
                .isEqualTo(TriggerResult.CONTINUE);

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(2);
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 2))).isOne();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(2, 4))).isOne();

        testHarness.mergeWindows(
                new TimeWindow(0, 4),
                Lists.newArrayList(new TimeWindow(0, 2), new TimeWindow(2, 4)));

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isOne();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 2))).isZero();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(2, 4))).isZero();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 4))).isOne();

        assertThat(testHarness.advanceWatermark(4, new TimeWindow(0, 4)))
                .isEqualTo(TriggerResult.FIRE);

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isZero();
    }

    /**
     * Merging a late window should not register a timer, otherwise we would get two firings: one
     * from onElement() on the merged window and one from the timer.
     */
    @Test
    void testMergingLateWindows() throws Exception {

        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(EventTimeTrigger.create(), new TimeWindow.Serializer());

        assertThat(EventTimeTrigger.create().canMerge()).isTrue();

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(2, 4)))
                .isEqualTo(TriggerResult.CONTINUE);

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isEqualTo(2);
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 2))).isOne();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(2, 4))).isOne();

        testHarness.advanceWatermark(10);

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 2))).isZero();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(2, 4))).isZero();

        testHarness.mergeWindows(
                new TimeWindow(0, 4),
                Lists.newArrayList(new TimeWindow(0, 2), new TimeWindow(2, 4)));

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 2))).isZero();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(2, 4))).isZero();
        assertThat(testHarness.numEventTimeTimers(new TimeWindow(0, 4))).isZero();
    }
}
