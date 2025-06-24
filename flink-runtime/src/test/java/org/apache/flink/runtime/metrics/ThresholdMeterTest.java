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

package org.apache.flink.runtime.metrics;

import org.apache.flink.runtime.metrics.ThresholdMeter.ThresholdExceedException;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

/** Tests for {@link ThresholdMeter}. */
class ThresholdMeterTest {
    private static final double THRESHOLD_LARGE = 1000.0;
    private static final double THRESHOLD_SMALL = 5.0;
    private static final Duration INTERVAL = Duration.ofMillis(50);
    private static final long SLEEP = 10;
    private static final double ERROR = 1e-6;

    private static ManualClock clock;

    @BeforeEach
    void setup() {
        clock = new ManualClock(42_000_000);
    }

    @Test
    void testMarkEvent() {
        final ThresholdMeter thresholdMeter = createLargeThresholdMeter();

        thresholdMeter.markEvent();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        assertThat(thresholdMeter.getCount()).isOne();
        assertThat(thresholdMeter.getRate()).isCloseTo(toPerSecondRate(1), within(ERROR));

        thresholdMeter.markEvent();
        assertThat(thresholdMeter.getCount()).isEqualTo(2);
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        assertThat(thresholdMeter.getRate()).isCloseTo(toPerSecondRate(2), within(ERROR));
    }

    @Test
    void testMarkMultipleEvents() {
        final ThresholdMeter thresholdMeter = createLargeThresholdMeter();
        thresholdMeter.markEvent(2);
        clock.advanceTime(SLEEP * 2, TimeUnit.MILLISECONDS);
        assertThat(thresholdMeter.getCount()).isEqualTo(2);
        assertThat(thresholdMeter.getRate()).isCloseTo(toPerSecondRate(2), within(ERROR));
    }

    @Test
    void testCheckAgainstThresholdNotExceeded() {
        final ThresholdMeter thresholdMeter = createSmallThresholdMeter();
        for (int i = 0; i < THRESHOLD_SMALL - 1; ++i) {
            thresholdMeter.markEvent();
            clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
            thresholdMeter.checkAgainstThreshold();
        }
    }

    @Test
    void testCheckAgainstThreshold() {
        final ThresholdMeter thresholdMeter = createSmallThresholdMeter();

        // first THRESHOLD_SMALL - 1 events should not exceed threshold
        for (int i = 0; i < THRESHOLD_SMALL - 1; ++i) {
            thresholdMeter.markEvent();
            clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
            thresholdMeter.checkAgainstThreshold();
        }

        // the THRESHOLD_SMALL-th event should exceed threshold
        thresholdMeter.markEvent();
        assertThatThrownBy(() -> thresholdMeter.checkAgainstThreshold())
                .isInstanceOf(ThresholdExceedException.class);
    }

    @Test
    void testUpdateInterval() {
        final ThresholdMeter thresholdMeter = createSmallThresholdMeter();

        thresholdMeter.markEvent();
        clock.advanceTime(INTERVAL.toMillis() * 2, TimeUnit.MILLISECONDS);

        for (int i = 0; i < THRESHOLD_SMALL - 1; ++i) {
            thresholdMeter.markEvent();
        }

        assertThat(thresholdMeter.getCount()).isEqualTo((long) THRESHOLD_SMALL);
        assertThat(thresholdMeter.getRate())
                .isCloseTo(toPerSecondRate((int) (THRESHOLD_SMALL - 1)), within(ERROR));
        thresholdMeter.checkAgainstThreshold();
    }

    @Test
    void testConcurrentAccess() throws Exception {
        final ThresholdMeter thresholdMeter = new ThresholdMeter(THRESHOLD_LARGE, INTERVAL);
        final int repeatNum = 100;
        final int concurrency = 2;

        final List<Thread> threads = new ArrayList<>();

        threads.addAll(
                getConcurrentThreads(repeat(thresholdMeter::markEvent, repeatNum), concurrency));
        threads.addAll(
                getConcurrentThreads(repeat(thresholdMeter::getRate, repeatNum), concurrency));
        threads.addAll(
                getConcurrentThreads(
                        repeat(thresholdMeter::checkAgainstThreshold, repeatNum), concurrency));

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            thread.join();
        }

        assertThat(thresholdMeter.getCount()).isEqualTo(repeatNum * concurrency);
    }

    private static Runnable repeat(Runnable task, int repeatNum) {
        return () -> {
            for (int i = 0; i < repeatNum; ++i) {
                task.run();
            }
        };
    }

    private static List<Thread> getConcurrentThreads(Runnable task, int concurrency) {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < concurrency; ++i) {
            threads.add(new Thread(task));
        }
        return threads;
    }

    private static ThresholdMeter createLargeThresholdMeter() {
        return new ThresholdMeter(THRESHOLD_LARGE, INTERVAL, clock);
    }

    private static ThresholdMeter createSmallThresholdMeter() {
        return new ThresholdMeter(THRESHOLD_SMALL, INTERVAL, clock);
    }

    private static double toPerSecondRate(int eventsPerInterval) {
        return eventsPerInterval * 1000.0 / INTERVAL.toMillis();
    }
}
