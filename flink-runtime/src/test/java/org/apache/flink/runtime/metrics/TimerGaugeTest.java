/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// ----------------------------------------------------------------------------
//  This class is largely adapted from "com.google.common.base.Preconditions",
//  which is part of the "Guava" library.
//
//  Because of frequent issues with dependency conflicts, this class was
//  added to the Flink code base to reduce dependency on Guava.
// ----------------------------------------------------------------------------

package org.apache.flink.runtime.metrics;

import org.apache.flink.metrics.View;
import org.apache.flink.util.clock.ManualClock;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

/** Tests for {@link TimerGauge}. */
public class TimerGaugeTest {
    private static final long SLEEP = 10;

    @Test
    public void testBasicUsage() {
        ManualClock clock = new ManualClock(42_000_000);
        TimerGauge gauge = new TimerGauge(clock, View.UPDATE_INTERVAL_SECONDS);

        gauge.update();
        assertThat(gauge.getValue(), is(0L));
        assertThat(gauge.getMaxSingleMeasurement(), is(0L));
        assertEquals(gauge.getAccumulatedCount(), 0L);

        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        gauge.update();

        assertThat(gauge.getValue(), greaterThanOrEqualTo(SLEEP / View.UPDATE_INTERVAL_SECONDS));
        assertThat(gauge.getMaxSingleMeasurement(), is(SLEEP));
        assertEquals(gauge.getAccumulatedCount(), SLEEP);

        // Check that the getMaxSingleMeasurement can go down after an update
        gauge.markStart();
        clock.advanceTime(SLEEP / 2, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        gauge.update();

        assertThat(gauge.getMaxSingleMeasurement(), is(SLEEP / 2));
        assertEquals(gauge.getAccumulatedCount(), SLEEP + SLEEP / 2);
    }

    @Test
    public void testUpdateWithoutMarkingEnd() {
        ManualClock clock = new ManualClock(42_000_000);
        TimerGauge gauge = new TimerGauge(clock, View.UPDATE_INTERVAL_SECONDS);

        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.update();

        assertThat(gauge.getValue(), greaterThanOrEqualTo(SLEEP / View.UPDATE_INTERVAL_SECONDS));
        assertThat(gauge.getMaxSingleMeasurement(), is(SLEEP));

        // keep the measurement going for another update
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.update();

        assertThat(gauge.getValue(), greaterThanOrEqualTo(SLEEP / View.UPDATE_INTERVAL_SECONDS));
        // max single measurement is now spanning two updates
        assertThat(gauge.getMaxSingleMeasurement(), is(SLEEP * 2));
    }

    @Test
    public void testGetWithoutUpdate() {
        ManualClock clock = new ManualClock(42_000_000);
        TimerGauge gauge = new TimerGauge(clock);

        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);

        assertThat(gauge.getValue(), is(0L));

        gauge.markEnd();

        assertThat(gauge.getValue(), is(0L));
        assertThat(gauge.getMaxSingleMeasurement(), is(0L));
    }

    @Test
    public void testLargerTimespan() {
        ManualClock clock = new ManualClock(42_000_000);
        TimerGauge gauge = new TimerGauge(clock, 2 * View.UPDATE_INTERVAL_SECONDS);

        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        gauge.update();

        assertThat(gauge.getValue(), is(SLEEP / View.UPDATE_INTERVAL_SECONDS));
        assertThat(gauge.getMaxSingleMeasurement(), is(SLEEP));
        assertEquals(gauge.getAccumulatedCount(), SLEEP);

        gauge.update();
        // One sleep in 2 intervals
        assertThat(gauge.getValue(), is(SLEEP / (View.UPDATE_INTERVAL_SECONDS * 2)));
        assertThat(gauge.getMaxSingleMeasurement(), is(0L));
        assertEquals(gauge.getAccumulatedCount(), SLEEP);

        // One sleep in each interval

        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        gauge.update();

        gauge.markStart();
        clock.advanceTime(SLEEP, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        gauge.update();

        assertThat(gauge.getValue(), is(SLEEP / (View.UPDATE_INTERVAL_SECONDS)));

        // Check that the getMaxSingleMeasurement can go down after an update
        gauge.markStart();
        clock.advanceTime(SLEEP / 2, TimeUnit.MILLISECONDS);
        gauge.markEnd();
        gauge.update();

        assertThat(gauge.getMaxSingleMeasurement(), is(SLEEP / 2));
        assertEquals(gauge.getAccumulatedCount(), 3 * SLEEP + SLEEP / 2);
    }
}
