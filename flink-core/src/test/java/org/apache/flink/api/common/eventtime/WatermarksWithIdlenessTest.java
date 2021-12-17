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

import org.junit.Test;

import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Test for the {@link WatermarksWithIdleness} class. */
public class WatermarksWithIdlenessTest {

    @Test(expected = IllegalArgumentException.class)
    public void testZeroTimeout() {
        new WatermarksWithIdleness<>(new AscendingTimestampsWatermarks<>(), Duration.ZERO);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNegativeTimeout() {
        new WatermarksWithIdleness<>(new AscendingTimestampsWatermarks<>(), Duration.ofMillis(-1L));
    }

    @Test
    public void testInitiallyActive() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = new IdlenessTimer(clock, Duration.ofMillis(10));

        assertFalse(timer.checkIfIdle());
    }

    @Test
    public void testIdleWithoutEvents() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = new IdlenessTimer(clock, Duration.ofMillis(10));
        timer.checkIfIdle(); // start timer

        clock.advanceTime(11, MILLISECONDS);
        assertTrue(timer.checkIfIdle());
    }

    @Test
    public void testRepeatedIdleChecks() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = createTimerAndMakeIdle(clock, Duration.ofMillis(122));

        assertTrue(timer.checkIfIdle());
        clock.advanceTime(100, MILLISECONDS);
        assertTrue(timer.checkIfIdle());
    }

    @Test
    public void testActiveAfterIdleness() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = createTimerAndMakeIdle(clock, Duration.ofMillis(10));

        timer.activity();
        assertFalse(timer.checkIfIdle());
    }

    @Test
    public void testIdleActiveIdle() {
        final ManualClock clock = new ManualClock(System.nanoTime());
        final IdlenessTimer timer = createTimerAndMakeIdle(clock, Duration.ofMillis(122));

        // active again
        timer.activity();
        assertFalse(timer.checkIfIdle());

        // idle again
        timer.checkIfIdle(); // start timer
        clock.advanceTime(Duration.ofMillis(123));
        assertTrue(timer.checkIfIdle());
    }

    private static IdlenessTimer createTimerAndMakeIdle(ManualClock clock, Duration idleTimeout) {
        final IdlenessTimer timer = new IdlenessTimer(clock, idleTimeout);

        timer.checkIfIdle(); // start timer
        clock.advanceTime(Duration.ofMillis(idleTimeout.toMillis() + 1));
        assertTrue(timer.checkIfIdle()); // rigger timer

        return timer;
    }
}
