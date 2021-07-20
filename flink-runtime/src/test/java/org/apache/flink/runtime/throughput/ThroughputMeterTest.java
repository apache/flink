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

/** Test for {@link ThroughputMeter}. */
public class ThroughputMeterTest extends TestCase {

    @Test
    public void testCorrectThroughputCalculation() {
        ManualClock clock = new ManualClock();
        ThroughputMeter throughputMeter =
                new ThroughputMeter(clock, new EMAThroughputCalculator(10));
        // Start throughput time.
        throughputMeter.calculateThroughput();

        throughputMeter.incomingDataSize(6666);
        clock.advanceTime(Duration.ofMillis(1));
        throughputMeter.incomingDataSize(3333);
        clock.advanceTime(Duration.ofMillis(33));
        throughputMeter.incomingDataSize(1);
        clock.advanceTime(Duration.ofMillis(66));

        assertThat(throughputMeter.calculateThroughput(), is(10_000L * 1_000 / 100));
    }

    @Test
    public void testResetValueAfterCalculation() {
        ManualClock clock = new ManualClock();
        ThroughputMeter throughputMeter =
                new ThroughputMeter(clock, new EMAThroughputCalculator(10));
        // Start throughput time.
        throughputMeter.calculateThroughput();

        throughputMeter.incomingDataSize(666);
        clock.advanceTime(Duration.ofMillis(100));

        assertThat(throughputMeter.calculateThroughput(), is(6660L));
        // It should be the same as previous time.
        assertThat(throughputMeter.calculateThroughput(), is(6660L));

        clock.advanceTime(Duration.ofMillis(1));
        assertThat(throughputMeter.calculateThroughput(), is(3330L));
    }

    @Test
    public void testIgnoringIdleTime() {
        ManualClock clock = new ManualClock();
        ThroughputMeter throughputMeter =
                new ThroughputMeter(clock, new EMAThroughputCalculator(10));
        // Start throughput time.
        throughputMeter.calculateThroughput();

        throughputMeter.incomingDataSize(7);
        clock.advanceTime(Duration.ofMillis(1));
        throughputMeter.idleStart();
        // This should be impossible situation(receiving data during idle time) but technically this
        // data will be taken into account.
        throughputMeter.incomingDataSize(3);
        // This will be ignored because it is in idle now.
        clock.advanceTime(Duration.ofMillis(9));
        throughputMeter.idleEnd();

        assertThat(throughputMeter.calculateThroughput(), is(10L * 1_000));
    }

    @Test
    public void testCalculationDuringIdleTime() {
        ManualClock clock = new ManualClock();
        ThroughputMeter throughputMeter =
                new ThroughputMeter(clock, new EMAThroughputCalculator(10));
        // Start throughput time.
        throughputMeter.calculateThroughput();

        throughputMeter.incomingDataSize(10);
        clock.advanceTime(Duration.ofMillis(1));
        throughputMeter.idleStart();
        // This will be ignored because it is in idle now.
        clock.advanceTime(Duration.ofMillis(9));

        assertThat(throughputMeter.calculateThroughput(), is(10L * 1_000));
    }

    @Test
    public void testMultiplyIdleEnd() {
        ManualClock clock = new ManualClock();
        ThroughputMeter throughputMeter =
                new ThroughputMeter(clock, new EMAThroughputCalculator(10));

        // Start throughput time.
        throughputMeter.calculateThroughput();

        throughputMeter.incomingDataSize(10);
        // It will be ignored.
        clock.advanceTime(Duration.ofMillis(3));
        throughputMeter.idleEnd();
        // It will be ignored.
        clock.advanceTime(Duration.ofMillis(3));
        throughputMeter.idleEnd();
        // It will be ignored.
        clock.advanceTime(Duration.ofMillis(3));
        throughputMeter.idleEnd();

        // Only this one will be taken into account.
        clock.advanceTime(Duration.ofMillis(1));

        assertThat(throughputMeter.calculateThroughput(), is(10L * 1_000));
    }
}
