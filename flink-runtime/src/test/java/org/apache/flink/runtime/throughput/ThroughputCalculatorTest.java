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

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for {@link ThroughputCalculator}. */
class ThroughputCalculatorTest {

    @Test
    void testCorrectThroughputCalculation() {
        ManualClock clock = new ManualClock();
        ThroughputCalculator throughputCalculator = new ThroughputCalculator(clock);

        throughputCalculator.incomingDataSize(6666);
        clock.advanceTime(Duration.ofMillis(1));
        throughputCalculator.incomingDataSize(3333);
        clock.advanceTime(Duration.ofMillis(33));
        throughputCalculator.incomingDataSize(1);
        clock.advanceTime(Duration.ofMillis(66));

        assertThat(throughputCalculator.calculateThroughput()).isEqualTo(10_000L * 1_000 / 100);
    }

    @Test
    void testResetValueAfterCalculation() {
        ManualClock clock = new ManualClock();
        ThroughputCalculator throughputCalculator = new ThroughputCalculator(clock);

        throughputCalculator.incomingDataSize(666);
        clock.advanceTime(Duration.ofMillis(100));

        assertThat(throughputCalculator.calculateThroughput()).isEqualTo(6660L);
        // It should be the same as previous time.
        assertThat(throughputCalculator.calculateThroughput()).isEqualTo(6660L);

        clock.advanceTime(Duration.ofMillis(1));
        assertThat(throughputCalculator.calculateThroughput()).isZero();
    }

    @Test
    void testIgnoringIdleTime() {
        ManualClock clock = new ManualClock();
        ThroughputCalculator throughputCalculator = new ThroughputCalculator(clock);

        throughputCalculator.incomingDataSize(7);
        clock.advanceTime(Duration.ofMillis(1));
        throughputCalculator.pauseMeasurement();
        // This will be ignored because it is in idle now.
        clock.advanceTime(Duration.ofMillis(9));
        // This should resume the measurement time.
        throughputCalculator.incomingDataSize(3);
        clock.advanceTime(Duration.ofMillis(1));

        assertThat(throughputCalculator.calculateThroughput()).isEqualTo(5L * 1_000);
    }

    @Test
    void testCalculationDuringIdleTime() {
        ManualClock clock = new ManualClock();
        ThroughputCalculator throughputCalculator = new ThroughputCalculator(clock);

        throughputCalculator.incomingDataSize(10);
        clock.advanceTime(Duration.ofMillis(1));
        throughputCalculator.pauseMeasurement();
        // This will be ignored because it is in idle now.
        clock.advanceTime(Duration.ofMillis(9));

        assertThat(throughputCalculator.calculateThroughput()).isEqualTo(10L * 1_000);
    }

    @Test
    void testMultiplyIdleEnd() {
        ManualClock clock = new ManualClock();
        ThroughputCalculator throughputCalculator = new ThroughputCalculator(clock);

        throughputCalculator.incomingDataSize(10);
        // It won't be ignored.
        clock.advanceTime(Duration.ofMillis(3));
        throughputCalculator.resumeMeasurement();
        // It won't be ignored.
        clock.advanceTime(Duration.ofMillis(3));
        throughputCalculator.resumeMeasurement();
        // It won't be ignored.
        clock.advanceTime(Duration.ofMillis(3));
        throughputCalculator.resumeMeasurement();

        clock.advanceTime(Duration.ofMillis(1));

        // resumeMeasurement should not reset the time because pauseMeasurement was not called.
        assertThat(throughputCalculator.calculateThroughput()).isEqualTo(1_000L);
    }

    @Test
    void testNotRestartTimerOnCalculationDuringIdleTime() {
        ManualClock clock = new ManualClock();
        ThroughputCalculator throughputCalculator = new ThroughputCalculator(clock);

        throughputCalculator.pauseMeasurement();

        // Should not resume measurement.
        throughputCalculator.calculateThroughput();

        // This will be ignored because it is still in idle.
        clock.advanceTime(Duration.ofMillis(9));

        // Resume measurement.
        throughputCalculator.incomingDataSize(10);
        clock.advanceTime(Duration.ofMillis(1));

        assertThat(throughputCalculator.calculateThroughput()).isEqualTo(10L * 1_000);
    }
}
