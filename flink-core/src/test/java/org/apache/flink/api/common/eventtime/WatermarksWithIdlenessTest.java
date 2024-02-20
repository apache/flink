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

import org.apache.flink.api.common.eventtime.WatermarksWithIdleness.IdlenessTimer;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for the {@link WatermarksWithIdleness} class. */
class WatermarksWithIdlenessTest {

    @Test
    void testZeroTimeout() {
        assertThatThrownBy(
                        () ->
                                new WatermarksWithIdleness<>(
                                        new AscendingTimestampsWatermarks<>(), Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testNegativeTimeout() {
        assertThatThrownBy(
                        () ->
                                new WatermarksWithIdleness<>(
                                        new AscendingTimestampsWatermarks<>(),
                                        Duration.ofMillis(-1L)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testInitiallyActive() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = new IdlenessTimer(clock, Duration.ofMillis(10));

        assertThat(timer.checkIfIdle()).isFalse();
    }

    @Test
    void testIdleWithoutEvents() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = new IdlenessTimer(clock, Duration.ofMillis(10));
        timer.checkIfIdle(); // start timer

        clock.advanceTime(11, MILLISECONDS);
        assertThat(timer.checkIfIdle()).isTrue();
    }

    @Test
    void testRepeatedIdleChecks() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = createTimerAndMakeIdle(clock, Duration.ofMillis(122));

        assertThat(timer.checkIfIdle()).isTrue();
        clock.advanceTime(100, MILLISECONDS);
        assertThat(timer.checkIfIdle()).isTrue();
    }

    @Test
    void testActiveAfterIdleness() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = createTimerAndMakeIdle(clock, Duration.ofMillis(10));

        timer.activity();
        assertThat(timer.checkIfIdle()).isFalse();
    }

    @Test
    void testIdleActiveIdle() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = createTimerAndMakeIdle(clock, Duration.ofMillis(122));

        // active again
        timer.activity();
        assertThat(timer.checkIfIdle()).isFalse();

        // idle again
        timer.checkIfIdle(); // start timer
        clock.advanceTime(Duration.ofMillis(123));
        assertThat(timer.checkIfIdle()).isTrue();
    }

    private static IdlenessTimer createTimerAndMakeIdle(ManualClock clock, Duration idleTimeout) {
        final IdlenessTimer timer = new IdlenessTimer(clock, idleTimeout);

        timer.checkIfIdle(); // start timer
        clock.advanceTime(Duration.ofMillis(idleTimeout.toMillis() + 1));
        assertThat(timer.checkIfIdle()).isTrue(); // rigger timer

        return timer;
    }
}
