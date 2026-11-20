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

package org.apache.flink.api.common.eventtime;

import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Pins the idleness detection timing documented on {@link WatermarkStrategy#withIdleness}. */
class WatermarksWithIdlenessTimeoutTest {

    private static final Duration TIMEOUT = Duration.ofMillis(10);

    @Test
    void idleFiresAtFirstProbeStrictlyExceedingTimeoutAfterFirstQuietProbe() {
        // non-zero start: IdlenessTimer treats a relative timestamp of 0 as "no timer started"
        ManualClock clock = new ManualClock(1_000_000_000L);
        WatermarksWithIdleness<Object> generator =
                new WatermarksWithIdleness<>(new NoWatermarksGenerator<>(), TIMEOUT, clock);
        TestingWatermarkOutput output = new TestingWatermarkOutput();

        // event at t=4ms
        clock.advanceTime(4, TimeUnit.MILLISECONDS);
        generator.onEvent(new Object(), 1L, output);

        // probe t=5ms: sees activity, so no timer yet
        clock.advanceTime(1, TimeUnit.MILLISECONDS);
        generator.onPeriodicEmit(output);
        assertThat(output.isIdle()).isFalse();

        // probe t=10ms: first quiet probe, the timeout starts here and not at the event
        clock.advanceTime(5, TimeUnit.MILLISECONDS);
        generator.onPeriodicEmit(output);
        assertThat(output.isIdle()).isFalse();

        // probe t=15ms: elapsed since countdown start = 5ms, not > 10ms
        clock.advanceTime(5, TimeUnit.MILLISECONDS);
        generator.onPeriodicEmit(output);
        assertThat(output.isIdle()).isFalse();

        // probe t=20ms: elapsed = exactly 10ms; the comparison is strict, so not idle yet
        clock.advanceTime(5, TimeUnit.MILLISECONDS);
        generator.onPeriodicEmit(output);
        assertThat(output.isIdle()).isFalse();

        // probe t=25ms: elapsed = 15ms > 10ms, idle 21ms after the event
        clock.advanceTime(5, TimeUnit.MILLISECONDS);
        generator.onPeriodicEmit(output);
        assertThat(output.isIdle()).isTrue();
    }
}
