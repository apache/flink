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

package org.apache.flink.api.connector.source.util.ratelimit;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

/** Unit tests for {@link CheckpointDurationRateLimitCalculator}. */
class CheckpointDurationRateLimitCalculatorTest {

    private static final double MAX_RECORDS_PER_SECOND = 1000.0;
    private static final double THROTTLE_DOWN_FACTOR = 0.5;
    private static final double RAMP_UP_FACTOR = 1.1;
    private static final double MIN_DURATION_FRACTION = 0.1;
    private static final double TARGET_DURATION_FRACTION = 0.5;
    private static final double MAX_DURATION_FRACTION = 0.8;
    private static final long CHECKPOINT_INTERVAL_MS = 1000L;
    private static final long UNKNOWN_INTERVAL_MS = -1L;

    private final CheckpointDurationRateLimitCalculator calculator =
            new CheckpointDurationRateLimitCalculator();

    private static DynamicRateLimitParams params() {
        return new DynamicRateLimitParams(
                true,
                10,
                MAX_RECORDS_PER_SECOND,
                THROTTLE_DOWN_FACTOR,
                RAMP_UP_FACTOR,
                MIN_DURATION_FRACTION,
                TARGET_DURATION_FRACTION,
                MAX_DURATION_FRACTION);
    }

    private double compute(List<Long> durations, double throughput, long intervalMs) {
        return calculator.computeNewRate(durations, throughput, intervalMs, params());
    }

    @Test
    void testDurationBelowMinLeavesRateUnchanged() {
        // min = 0.1 * 1000ms = 100ms; the latest duration is below it despite the rising trend.
        assertThat(compute(Arrays.asList(10L, 10L, 50L, 50L), 200.0, CHECKPOINT_INTERVAL_MS))
                .isEqualTo(200.0);
    }

    @Test
    void testEmptyWindowReturnsThroughputUnchanged() {
        assertThat(compute(Collections.emptyList(), 100.0, CHECKPOINT_INTERVAL_MS))
                .isEqualTo(100.0);
        assertThat(compute(Collections.emptyList(), 100.0, UNKNOWN_INTERVAL_MS)).isEqualTo(100.0);
    }

    @Test
    void testSingleSampleBelowTargetRampsUp() {
        assertThat(compute(Collections.singletonList(200L), 100.0, CHECKPOINT_INTERVAL_MS))
                .isEqualTo(100.0 * RAMP_UP_FACTOR);
    }

    @Test
    void testAverageAboveTargetThrottlesDown() {
        // target = 0.5 * 1000ms = 500ms; max = 800ms is not exceeded.
        assertThat(compute(Arrays.asList(600L, 600L, 600L, 600L), 200.0, CHECKPOINT_INTERVAL_MS))
                .isEqualTo(200.0 * THROTTLE_DOWN_FACTOR);
    }

    @Test
    void testAverageBelowTargetRampsUp() {
        assertThat(compute(Arrays.asList(100L, 100L, 100L, 100L), 200.0, CHECKPOINT_INTERVAL_MS))
                .isEqualTo(200.0 * RAMP_UP_FACTOR);
    }

    @Test
    void testAverageIsUsedToSmoothOutASingleSlowCheckpointBelowMax() {
        // average = 400ms is below the target although the latest duration (700ms) is above it.
        assertThat(compute(Arrays.asList(300L, 300L, 300L, 700L), 200.0, CHECKPOINT_INTERVAL_MS))
                .isEqualTo(200.0 * RAMP_UP_FACTOR);
    }

    @Test
    void testLatestDurationAboveMaxThrottlesDownDespiteLowAverage() {
        // max = 800ms; the average (300ms) is below the target but the latest duration is not.
        assertThat(compute(Arrays.asList(100L, 100L, 100L, 900L), 200.0, CHECKPOINT_INTERVAL_MS))
                .isEqualTo(200.0 * THROTTLE_DOWN_FACTOR);
    }

    @Test
    void testSingleSampleAboveMaxThrottlesDown() {
        assertThat(compute(Collections.singletonList(900L), 200.0, CHECKPOINT_INTERVAL_MS))
                .isEqualTo(200.0 * THROTTLE_DOWN_FACTOR);
    }

    @Test
    void testRampUpIsCappedAtConfiguredMax() {
        // already at the ceiling
        assertThat(
                        compute(
                                Arrays.asList(100L, 100L),
                                MAX_RECORDS_PER_SECOND,
                                CHECKPOINT_INTERVAL_MS))
                .isEqualTo(MAX_RECORDS_PER_SECOND);
    }

    @Test
    void testResultNeverGoesBelowMinimumRate() {
        assertThat(compute(Arrays.asList(100L, 500L), 0.0, CHECKPOINT_INTERVAL_MS))
                .isCloseTo(1.0, within(1e-9));
    }

    @Test
    void testUnknownIntervalFallsBackToTrend() {
        // rising: older half avg = 100, recent half avg = 500
        assertThat(compute(Arrays.asList(100L, 100L, 500L, 500L), 200.0, UNKNOWN_INTERVAL_MS))
                .isEqualTo(200.0 * THROTTLE_DOWN_FACTOR);
        // flat
        assertThat(compute(Arrays.asList(900L, 900L, 900L, 900L), 200.0, UNKNOWN_INTERVAL_MS))
                .isEqualTo(200.0 * RAMP_UP_FACTOR);
        // improving: older half avg = 500, recent half avg = 100
        assertThat(compute(Arrays.asList(500L, 500L, 100L, 100L), 200.0, UNKNOWN_INTERVAL_MS))
                .isEqualTo(200.0 * RAMP_UP_FACTOR);
    }

    @Test
    void testUnknownIntervalSingleSampleReturnsThroughputUnchanged() {
        assertThat(compute(Collections.singletonList(100L), 100.0, UNKNOWN_INTERVAL_MS))
                .isEqualTo(100.0);
    }

    @Test
    void testUnknownIntervalZeroOlderHalfAverageIsTreatedAsThrottleDown() {
        // older half avg = 0 (checkpoints used to complete instantly), recent half avg = 100 ->
        // treated as an unbounded/infinite worsening trend.
        assertThat(compute(Arrays.asList(0L, 0L, 100L, 100L), 200.0, UNKNOWN_INTERVAL_MS))
                .isEqualTo(200.0 * THROTTLE_DOWN_FACTOR);
    }

    @Test
    void testUnknownIntervalAllZeroDurationsIsTreatedAsFlatTrend() {
        assertThat(compute(Arrays.asList(0L, 0L, 0L, 0L), 200.0, UNKNOWN_INTERVAL_MS))
                .isEqualTo(200.0 * RAMP_UP_FACTOR);
    }
}
