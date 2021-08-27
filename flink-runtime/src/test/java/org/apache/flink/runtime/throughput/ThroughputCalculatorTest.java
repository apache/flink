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

package org.apache.flink.runtime.throughput;

import org.apache.flink.util.clock.ManualClock;

import junit.framework.TestCase;
import org.junit.Test;

import java.time.Duration;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

/** Test for {@link ThroughputCalculator}. */
public class ThroughputCalculatorTest extends TestCase {

    @Test
    public void testCorrectThroughputCalculation() {
        ManualClock clock = new ManualClock();
        ThroughputCalculator throughputCalculator = new ThroughputCalculator(clock, 10);

        throughputCalculator.incomingDataSize(6666);
        clock.advanceTime(Duration.ofMillis(1));
        throughputCalculator.incomingDataSize(3333);
        clock.advanceTime(Duration.ofMillis(33));
        throughputCalculator.incomingDataSize(1);
        clock.advanceTime(Duration.ofMillis(66));

        assertThat(throughputCalculator.calculateThroughput(), is(10_000L * 1_000 / 100));
    }

    @Test
    public void testResetValueAfterCalculation() {
        ManualClock clock = new ManualClock();
        ThroughputCalculator throughputCalculator = new ThroughputCalculator(clock, 10);

        throughputCalculator.incomingDataSize(666);
        clock.advanceTime(Duration.ofMillis(100));

        assertThat(throughputCalculator.calculateThroughput(), is(6660L));
        // It should be the same as previous time.
        assertThat(throughputCalculator.calculateThroughput(), is(6660L));

        clock.advanceTime(Duration.ofMillis(1));
        assertThat(throughputCalculator.calculateThroughput(), is(5449L));
    }

    @Test
    public void testIgnoringIdleTime() {
        ManualClock clock = new ManualClock();
        ThroughputCalculator throughputCalculator = new ThroughputCalculator(clock, 10);

        throughputCalculator.incomingDataSize(7);
        clock.advanceTime(Duration.ofMillis(1));
        throughputCalculator.pauseMeasurement(clock.absoluteTimeMillis());
        // This will be ignored because it is in idle now.
        clock.advanceTime(Duration.ofMillis(9));
        // This should resume the measurement time.
        throughputCalculator.incomingDataSize(3);
        clock.advanceTime(Duration.ofMillis(1));

        assertThat(throughputCalculator.calculateThroughput(), is(5L * 1_000));
    }

    @Test
    public void testCalculationDuringIdleTime() {
        ManualClock clock = new ManualClock();
        ThroughputCalculator throughputCalculator = new ThroughputCalculator(clock, 10);

        throughputCalculator.incomingDataSize(10);
        clock.advanceTime(Duration.ofMillis(1));
        throughputCalculator.pauseMeasurement(clock.absoluteTimeMillis());
        // This will be ignored because it is in idle now.
        clock.advanceTime(Duration.ofMillis(9));

        assertThat(throughputCalculator.calculateThroughput(), is(10L * 1_000));
    }

    @Test
    public void testMultiplyIdleEnd() {
        ManualClock clock = new ManualClock();
        ThroughputCalculator throughputCalculator = new ThroughputCalculator(clock, 10);

        throughputCalculator.incomingDataSize(10);
        // It won't be ignored.
        clock.advanceTime(Duration.ofMillis(3));
        throughputCalculator.resumeMeasurement(clock.absoluteTimeMillis());
        // It won't be ignored.
        clock.advanceTime(Duration.ofMillis(3));
        throughputCalculator.resumeMeasurement(clock.absoluteTimeMillis());
        // It won't be ignored.
        clock.advanceTime(Duration.ofMillis(3));
        throughputCalculator.resumeMeasurement(clock.absoluteTimeMillis());

        clock.advanceTime(Duration.ofMillis(1));

        // resumeMeasurement should not reset the time because pauseMeasurement was not called.
        assertThat(throughputCalculator.calculateThroughput(), is(1_000L));
    }

    @Test
    public void testNotRestartTimerOnCalculationDuringIdleTime() {
        ManualClock clock = new ManualClock();
        ThroughputCalculator throughputCalculator = new ThroughputCalculator(clock, 10);

        throughputCalculator.pauseMeasurement(clock.absoluteTimeMillis());

        // Should not resume measurement.
        throughputCalculator.calculateThroughput();

        // This will be ignored because it is still in idle.
        clock.advanceTime(Duration.ofMillis(9));

        // Resume measurement.
        throughputCalculator.incomingDataSize(10);
        clock.advanceTime(Duration.ofMillis(1));

        assertThat(throughputCalculator.calculateThroughput(), is(10L * 1_000));
    }
}
