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

package org.apache.flink.streaming.api.operators.util;

import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

class PausableRelativeClockTest {
    private static final long TIME_STEP = 14;

    @Test
    void simpleTest() {
        ManualClock baseClock = new ManualClock();
        PausableRelativeClock relativeClock = new PausableRelativeClock(baseClock);

        long startMillis = relativeClock.relativeTimeMillis();
        long startNanos = relativeClock.relativeTimeNanos();
        baseClock.advanceTime(Duration.ofMillis(TIME_STEP));
        long endNanos = relativeClock.relativeTimeNanos();
        long endMillis = relativeClock.relativeTimeMillis();

        long durationNanos = endNanos - startNanos;
        long durationMillis = endMillis - startMillis;

        assertThat((durationNanos) / 1_000_000).isEqualTo(TIME_STEP);
        assertThat(durationMillis).isEqualTo(TIME_STEP);
    }

    @Test
    void pausedTest() throws Exception {
        ManualClock baseClock = new ManualClock();
        PausableRelativeClock relativeClock = new PausableRelativeClock(baseClock);

        long startMillis = relativeClock.relativeTimeMillis();
        long startNanos = relativeClock.relativeTimeNanos();

        baseClock.advanceTime(Duration.ofMillis(TIME_STEP));
        relativeClock.pause();
        baseClock.advanceTime(Duration.ofMillis(TIME_STEP)); // doesn't count
        relativeClock.pause();
        baseClock.advanceTime(Duration.ofMillis(TIME_STEP)); // doesn't count
        relativeClock.unPause();
        baseClock.advanceTime(Duration.ofMillis(TIME_STEP)); // doesn't count
        relativeClock.unPause();
        baseClock.advanceTime(Duration.ofMillis(TIME_STEP));
        relativeClock.pause();
        baseClock.advanceTime(Duration.ofMillis(TIME_STEP)); // doesn't count
        // leave the clock blocked

        long endNanos = relativeClock.relativeTimeNanos();
        long endMillis = relativeClock.relativeTimeMillis();

        long durationNanos = endNanos - startNanos;
        long durationMillis = endMillis - startMillis;

        assertThat((durationNanos) / 1_000_000).isEqualTo(TIME_STEP * 2);
        assertThat(durationMillis).isEqualTo(TIME_STEP * 2);
    }
}
