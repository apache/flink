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

package org.apache.flink.metrics;

import org.apache.flink.metrics.ThresholdMeter.ThresholdExceedException;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for {@link ThresholdMeter}. */
public class ThresholdMeterTest extends TestLogger {
    private static final double THRESHOLD_LARGE = 1000.0;
    private static final double THRESHOLD_SMALL = 5.0;
    private static final Duration INTERVAL = Duration.ofMillis(50);
    private static final double ERROR = 1e-6;

    @Test
    public void testMarkEvent() {
        final ThresholdMeter thresholdMeter = createLargeThresholdMeter();

        thresholdMeter.markEvent();
        assertThat(thresholdMeter.getCount(), is(1L));
        assertThat(thresholdMeter.getRate(), closeTo(toPerSecondRate(1), ERROR));

        thresholdMeter.markEvent();
        assertThat(thresholdMeter.getCount(), is(2L));
        assertThat(thresholdMeter.getRate(), closeTo(toPerSecondRate(2), ERROR));
    }

    @Test
    public void testMarkMultipleEvents() {
        final ThresholdMeter thresholdMeter = createLargeThresholdMeter();
        thresholdMeter.markEvent(2);
        assertThat(thresholdMeter.getCount(), is(2L));
        assertThat(thresholdMeter.getRate(), closeTo(toPerSecondRate(2), ERROR));
    }

    @Test
    public void testCheckAgainstThresholdNotExceeded() {
        final ThresholdMeter thresholdMeter = createSmallThresholdMeter();
        for (int i = 0; i < THRESHOLD_SMALL - 1; ++i) {
            thresholdMeter.markEvent();
            thresholdMeter.checkAgainstThreshold();
        }
    }

    @Test
    public void testCheckAgainstThreshold() {
        final ThresholdMeter thresholdMeter = createSmallThresholdMeter();

        // first THRESHOLD_SMALL - 1 events should not exceed threshold
        for (int i = 0; i < THRESHOLD_SMALL - 1; ++i) {
            thresholdMeter.markEvent();
            thresholdMeter.checkAgainstThreshold();
        }

        // the THRESHOLD_SMALL-th event should exceed threshold
        thresholdMeter.markEvent();
        try {
            thresholdMeter.checkAgainstThreshold();
            fail();
        } catch (ThresholdExceedException e) {
            // expected
        }
    }

    @Test
    public void testUpdateInterval() throws InterruptedException {
        final ThresholdMeter thresholdMeter = createSmallThresholdMeter();

        thresholdMeter.markEvent();
        Thread.sleep(INTERVAL.toMillis() * 2);

        for (int i = 0; i < THRESHOLD_SMALL - 1; ++i) {
            thresholdMeter.markEvent();
        }

        assertThat(thresholdMeter.getCount(), is((long) THRESHOLD_SMALL));
        assertThat(
                thresholdMeter.getRate(),
                closeTo(toPerSecondRate((int) (THRESHOLD_SMALL - 1)), ERROR));
        thresholdMeter.checkAgainstThreshold();
    }

    private static ThresholdMeter createLargeThresholdMeter() {
        return new ThresholdMeter(THRESHOLD_LARGE, INTERVAL);
    }

    private static ThresholdMeter createSmallThresholdMeter() {
        return new ThresholdMeter(THRESHOLD_SMALL, INTERVAL);
    }

    private static double toPerSecondRate(int eventsPerInterval) {
        return eventsPerInterval * 1000.0 / INTERVAL.toMillis();
    }
}
