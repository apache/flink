/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.model.triton;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link TritonInferenceModelFunction#computeBackoffDelayMillis(int, long, long)}.
 *
 * <p>The retry backoff algorithm has several boundary conditions that are easy to regress on (shift
 * overflow when attempt is very large, multiplication overflow when base*2^n wraps negative, cap
 * monotonicity). Exercising it here without the full operator fixture keeps the tests fast and the
 * failure modes sharply diagnosable.
 */
class TritonRetryBackoffTest {

    @Test
    void testFirstAttemptReturnsBase() {
        // attempt 0 corresponds to "first retry waits base" in the docstring.
        assertThat(TritonInferenceModelFunction.computeBackoffDelayMillis(0, 100L, 30_000L))
                .isEqualTo(100L);
    }

    @Test
    void testExponentialDoubling() {
        assertThat(TritonInferenceModelFunction.computeBackoffDelayMillis(1, 100L, 30_000L))
                .isEqualTo(200L);
        assertThat(TritonInferenceModelFunction.computeBackoffDelayMillis(2, 100L, 30_000L))
                .isEqualTo(400L);
        assertThat(TritonInferenceModelFunction.computeBackoffDelayMillis(3, 100L, 30_000L))
                .isEqualTo(800L);
    }

    @Test
    void testCapIsEnforced() {
        // 100ms * 2^10 = ~102s, capped to 30s.
        assertThat(TritonInferenceModelFunction.computeBackoffDelayMillis(10, 100L, 30_000L))
                .isEqualTo(30_000L);
    }

    @Test
    void testLargeAttemptNumberDoesNotOverflow() {
        // Java masks the shift amount mod 64, so without the clamp this would wrap and produce
        // an incorrect delay. The output must still equal the cap.
        long cap = 30_000L;
        assertThat(TritonInferenceModelFunction.computeBackoffDelayMillis(1000, 100L, cap))
                .isEqualTo(cap);
        assertThat(
                        TritonInferenceModelFunction.computeBackoffDelayMillis(
                                Integer.MAX_VALUE, 100L, cap))
                .isEqualTo(cap);
    }

    @Test
    void testBaseTimesMultiplierOverflowFallsBackToCap() {
        // Long.MAX_VALUE * 2 would overflow; result must be the cap, not a negative number.
        long base = Long.MAX_VALUE / 2;
        long cap = 1_000L;
        assertThat(TritonInferenceModelFunction.computeBackoffDelayMillis(5, base, cap))
                .isEqualTo(cap);
    }

    @Test
    void testNegativeAttemptNumberIsTreatedAsZero() {
        // Defensive clamp: a negative attempt counter (should be impossible in production but
        // cheap to guard) must not trigger `1L << -1` (which equals `1L << 63`).
        assertThat(TritonInferenceModelFunction.computeBackoffDelayMillis(-1, 100L, 30_000L))
                .isEqualTo(100L);
    }

    @Test
    void testDelayIsMonotonic() {
        long base = 50L;
        long cap = 10_000L;
        long previous = 0L;
        for (int attempt = 0; attempt < 20; attempt++) {
            long delay = TritonInferenceModelFunction.computeBackoffDelayMillis(attempt, base, cap);
            assertThat(delay)
                    .as("delay must be non-decreasing across attempts (attempt=%d)", attempt)
                    .isGreaterThanOrEqualTo(previous);
            assertThat(delay)
                    .as("delay must never exceed cap (attempt=%d)", attempt)
                    .isLessThanOrEqualTo(cap);
            previous = delay;
        }
    }

    @Test
    void testZeroBaseReturnsZero() {
        // Degenerate contract: option validation normally forbids baseMs == 0, but the static
        // helper must still be total. Any attempt with base=0 produces 0 (0 << n == 0), and we
        // explicitly skip the overflow guard because `Long.MAX_VALUE / 0` would throw.
        assertThat(TritonInferenceModelFunction.computeBackoffDelayMillis(0, 0L, 30_000L))
                .isEqualTo(0L);
        assertThat(TritonInferenceModelFunction.computeBackoffDelayMillis(5, 0L, 30_000L))
                .isEqualTo(0L);
    }

    @Test
    void testCapEqualsBaseAlwaysReturnsBase() {
        // When the user configures retry-initial-backoff == retry-max-backoff, the cap clamps
        // every attempt to the base delay (effectively a fixed-delay retry schedule).
        long fixed = 500L;
        for (int attempt = 0; attempt < 10; attempt++) {
            assertThat(
                            TritonInferenceModelFunction.computeBackoffDelayMillis(
                                    attempt, fixed, fixed))
                    .as("cap == base collapses to fixed delay (attempt=%d)", attempt)
                    .isEqualTo(fixed);
        }
    }

    @Test
    void testJitterStaysWithinEqualJitterWindow() {
        // Equal jitter: the returned value must land in [nominal/2, nominal] inclusive. A strict
        // probabilistic bound check over many samples is a cheap, non-flaky way to verify the
        // window without asserting the exact distribution.
        long nominal = 1000L;
        for (int i = 0; i < 10_000; i++) {
            long jittered = TritonInferenceModelFunction.computeBackoffWithJitter(nominal);
            assertThat(jittered)
                    .as("jitter must not fall below nominal/2 (iteration=%d)", i)
                    .isGreaterThanOrEqualTo(nominal / 2L);
            assertThat(jittered)
                    .as("jitter must not exceed nominal (iteration=%d)", i)
                    .isLessThanOrEqualTo(nominal);
        }
    }

    @Test
    void testJitterProducesSpreadToAvoidThunderingHerd() {
        // A deterministic computeBackoffWithJitter would defeat the whole point of jitter and
        // cause a thundering herd. Sample a moderate number of draws and require at least a few
        // distinct values — with a uniform integer range of ~500 values the probability of all
        // 100 draws being identical is astronomically small (< 2^-800).
        long nominal = 1000L;
        java.util.Set<Long> distinct = new java.util.HashSet<>();
        for (int i = 0; i < 100; i++) {
            distinct.add(TritonInferenceModelFunction.computeBackoffWithJitter(nominal));
        }
        assertThat(distinct)
                .as("jitter must introduce variation across calls, got only %s", distinct)
                .hasSizeGreaterThan(1);
    }

    @Test
    void testJitterPassesThroughTrivialDelays() {
        // 0 and 1 ms delays are too small for meaningful jitter and must pass through unchanged
        // so that a degenerate config does not accidentally return a negative or truncated
        // value.
        assertThat(TritonInferenceModelFunction.computeBackoffWithJitter(0L)).isEqualTo(0L);
        assertThat(TritonInferenceModelFunction.computeBackoffWithJitter(1L)).isEqualTo(1L);
    }
}
