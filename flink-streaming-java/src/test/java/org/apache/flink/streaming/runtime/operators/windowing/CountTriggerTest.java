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

import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import org.apache.flink.shaded.guava31.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link CountTrigger}. */
class CountTriggerTest {

    /** Verify that state of separate windows does not leak into other windows. */
    @Test
    void testWindowSeparationAndFiring() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(CountTrigger.of(3), new TimeWindow.Serializer());

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(2, 4)))
                .isEqualTo(TriggerResult.CONTINUE);

        // shouldn't have any timers
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isZero();

        assertThat(testHarness.numStateEntries()).isEqualTo(2);
        assertThat(testHarness.numStateEntries(new TimeWindow(0, 2))).isOne();
        assertThat(testHarness.numStateEntries(new TimeWindow(2, 4))).isOne();

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.FIRE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(2, 4)))
                .isEqualTo(TriggerResult.CONTINUE);

        // right now, CountTrigger will clear it's state in onElement when firing
        // ideally, this should be moved to onFire()
        assertThat(testHarness.numStateEntries()).isOne();
        assertThat(testHarness.numStateEntries(new TimeWindow(0, 2))).isZero();
        assertThat(testHarness.numStateEntries(new TimeWindow(2, 4))).isOne();

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(2, 4)))
                .isEqualTo(TriggerResult.FIRE);

        // now all state should be gone
        assertThat(testHarness.numStateEntries()).isZero();
    }

    /** Verify that clear() does not leak across windows. */
    @Test
    void testClear() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(CountTrigger.of(3), new TimeWindow.Serializer());

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(2, 4)))
                .isEqualTo(TriggerResult.CONTINUE);

        // shouldn't have any timers
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isZero();

        assertThat(testHarness.numStateEntries()).isEqualTo(2);
        assertThat(testHarness.numStateEntries(new TimeWindow(0, 2))).isOne();
        assertThat(testHarness.numStateEntries(new TimeWindow(2, 4))).isOne();

        testHarness.clearTriggerState(new TimeWindow(2, 4));

        assertThat(testHarness.numStateEntries()).isOne();
        assertThat(testHarness.numStateEntries(new TimeWindow(0, 2))).isOne();
        assertThat(testHarness.numStateEntries(new TimeWindow(2, 4))).isZero();

        testHarness.clearTriggerState(new TimeWindow(0, 2));

        assertThat(testHarness.numStateEntries()).isZero();
        assertThat(testHarness.numStateEntries(new TimeWindow(0, 2))).isZero();
        assertThat(testHarness.numStateEntries(new TimeWindow(2, 4))).isZero();
    }

    @Test
    void testMergingWindows() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(CountTrigger.of(3), new TimeWindow.Serializer());

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 2)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(2, 4)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(4, 6)))
                .isEqualTo(TriggerResult.CONTINUE);

        // shouldn't have any timers
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isZero();

        assertThat(testHarness.numStateEntries()).isEqualTo(3);
        assertThat(testHarness.numStateEntries(new TimeWindow(0, 2))).isOne();
        assertThat(testHarness.numStateEntries(new TimeWindow(2, 4))).isOne();
        assertThat(testHarness.numStateEntries(new TimeWindow(4, 6))).isOne();

        testHarness.mergeWindows(
                new TimeWindow(0, 4),
                Lists.newArrayList(new TimeWindow(0, 2), new TimeWindow(2, 4)));

        assertThat(testHarness.numStateEntries()).isEqualTo(2);
        assertThat(testHarness.numStateEntries(new TimeWindow(0, 2))).isZero();
        assertThat(testHarness.numStateEntries(new TimeWindow(2, 4))).isZero();
        assertThat(testHarness.numStateEntries(new TimeWindow(0, 4))).isOne();
        assertThat(testHarness.numStateEntries(new TimeWindow(4, 6))).isOne();

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 4)))
                .isEqualTo(TriggerResult.FIRE);

        assertThat(testHarness.numStateEntries()).isOne();
        assertThat(testHarness.numStateEntries(new TimeWindow(0, 4))).isZero();
        assertThat(testHarness.numStateEntries(new TimeWindow(4, 6))).isOne();

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(4, 6)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(4, 6)))
                .isEqualTo(TriggerResult.FIRE);

        assertThat(testHarness.numStateEntries()).isZero();
    }

    @Test
    void testMergeSubsumingWindow() throws Exception {
        TriggerTestHarness<Object, TimeWindow> testHarness =
                new TriggerTestHarness<>(CountTrigger.of(3), new TimeWindow.Serializer());

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(2, 4)))
                .isEqualTo(TriggerResult.CONTINUE);
        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(4, 6)))
                .isEqualTo(TriggerResult.CONTINUE);

        // shouldn't have any timers
        assertThat(testHarness.numProcessingTimeTimers()).isZero();
        assertThat(testHarness.numEventTimeTimers()).isZero();

        assertThat(testHarness.numStateEntries()).isEqualTo(2);
        assertThat(testHarness.numStateEntries(new TimeWindow(2, 4))).isOne();
        assertThat(testHarness.numStateEntries(new TimeWindow(4, 6))).isOne();

        testHarness.mergeWindows(
                new TimeWindow(0, 8),
                Lists.newArrayList(new TimeWindow(2, 4), new TimeWindow(4, 6)));

        assertThat(testHarness.numStateEntries()).isOne();
        assertThat(testHarness.numStateEntries(new TimeWindow(2, 4))).isZero();
        assertThat(testHarness.numStateEntries(new TimeWindow(4, 6))).isZero();
        assertThat(testHarness.numStateEntries(new TimeWindow(0, 8))).isOne();

        assertThat(testHarness.processElement(new StreamRecord<>(1), new TimeWindow(0, 8)))
                .isEqualTo(TriggerResult.FIRE);

        assertThat(testHarness.numStateEntries()).isZero();
    }
}
