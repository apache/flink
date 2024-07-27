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

package org.apache.flink.runtime.executiongraph.failover;

import org.apache.flink.runtime.executiongraph.failover.ExponentialDelayRestartBackoffTimeStrategy.ExponentialDelayRestartBackoffTimeStrategyFactory;
import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Unit tests for {@link ExponentialDelayRestartBackoffTimeStrategy}. */
class ExponentialDelayRestartBackoffTimeStrategyTest {

    private final Exception failure = new Exception();

    @Test
    void testMaxAttempts() {
        int maxAttempts = 13;
        ManualClock clock = new ManualClock();
        long maxBackoffMS = 3L;
        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        clock, 1L, maxBackoffMS, 1.2, 10L, 0.25, maxAttempts);

        for (int i = 0; i <= maxAttempts; i++) {
            assertThat(restartStrategy.canRestart()).isTrue();
            assertThat(restartStrategy.notifyFailure(failure)).isTrue();
            clock.advanceTime(Duration.ofMillis(maxBackoffMS + 1));
        }
        assertThat(restartStrategy.canRestart()).isFalse();
    }

    @Test
    void testNotCallNotifyFailure() {
        long initialBackoffMS = 42L;

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        new ManualClock(), initialBackoffMS, 45L, 2.0, 8L, 0, 10);

        assertThatThrownBy(restartStrategy::getBackoffTime)
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Please call notifyFailure first.");
    }

    @Test
    void testInitialBackoff() {
        long initialBackoffMS = 42L;

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        new ManualClock(), initialBackoffMS, 45L, 2.0, 8L, 0, Integer.MAX_VALUE);

        restartStrategy.notifyFailure(failure);
        assertThat(restartStrategy.getBackoffTime()).isEqualTo(initialBackoffMS);
    }

    @Test
    void testMaxBackoff() {
        final long maxBackoffMS = 6L;

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        new ManualClock(), 1L, maxBackoffMS, 2.0, 8L, 0.25, Integer.MAX_VALUE);

        for (int i = 0; i < 10; i++) {
            restartStrategy.notifyFailure(failure);
            assertThat(restartStrategy.getBackoffTime()).isLessThanOrEqualTo(maxBackoffMS);
        }
    }

    @Test
    void testResetBackoff() {
        final long initialBackoffMS = 1L;
        final long resetBackoffThresholdMS = 8L;
        final ManualClock clock = new ManualClock();

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        clock,
                        initialBackoffMS,
                        5L,
                        2.0,
                        resetBackoffThresholdMS,
                        0.25,
                        Integer.MAX_VALUE);

        assertThat(restartStrategy.notifyFailure(failure)).isTrue();

        clock.advanceTime(
                resetBackoffThresholdMS + restartStrategy.getBackoffTime() - 1,
                TimeUnit.MILLISECONDS);
        assertThat(restartStrategy.notifyFailure(failure)).isTrue();
        assertThat(restartStrategy.getBackoffTime())
                .as("Backoff should be increased")
                .isEqualTo(2L);

        clock.advanceTime(
                resetBackoffThresholdMS + restartStrategy.getBackoffTime(), TimeUnit.MILLISECONDS);
        assertThat(restartStrategy.notifyFailure(failure)).isTrue();
        assertThat(restartStrategy.getBackoffTime())
                .as("Backoff should be reset")
                .isEqualTo(initialBackoffMS);
    }

    @Test
    void testBackoffMultiplier() {
        long initialBackoffMS = 4L;
        double jitterFactor = 0;
        double backoffMultiplier = 2.3;
        long maxBackoffMS = 300L;

        ManualClock clock = new ManualClock();
        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        clock,
                        initialBackoffMS,
                        maxBackoffMS,
                        backoffMultiplier,
                        Integer.MAX_VALUE,
                        jitterFactor,
                        10);

        assertThat(restartStrategy.notifyFailure(failure)).isTrue();
        assertThat(restartStrategy.getBackoffTime()).isEqualTo(4L); // 4
        clock.advanceTime(Duration.ofMillis(maxBackoffMS + 1));

        assertThat(restartStrategy.notifyFailure(failure)).isTrue();
        assertThat(restartStrategy.getBackoffTime()).isEqualTo(9L); // 4 * 2.3
        clock.advanceTime(Duration.ofMillis(maxBackoffMS + 1));

        assertThat(restartStrategy.notifyFailure(failure)).isTrue();
        assertThat(restartStrategy.getBackoffTime()).isEqualTo(21L); // 4 * 2.3 * 2.3
        clock.advanceTime(Duration.ofMillis(maxBackoffMS + 1));
    }

    @Test
    void testJitter() throws Exception {
        final long initialBackoffMS = 2L;
        final long maxBackoffMS = 7L;

        ManualClock clock = new ManualClock();
        final ExponentialDelayRestartBackoffTimeStrategyFactory restartStrategyFactory =
                new ExponentialDelayRestartBackoffTimeStrategyFactory(
                        clock,
                        initialBackoffMS,
                        maxBackoffMS,
                        2.0,
                        Integer.MAX_VALUE,
                        0.25,
                        Integer.MAX_VALUE);

        assertCorrectRandomRangeWithFailureCount(
                restartStrategyFactory, clock, maxBackoffMS + 1, 2, 3L, 4L, 5L);

        assertCorrectRandomRangeWithFailureCount(
                restartStrategyFactory, clock, maxBackoffMS + 1, 3, 6L, 7L);

        assertCorrectRandomRangeWithFailureCount(
                restartStrategyFactory, clock, maxBackoffMS + 1, 4, 7L);
    }

    @Test
    void testJitterNoHigherThanMax() throws Exception {
        double jitterFactor = 1;
        long maxBackoffMS = 7L;

        ManualClock clock = new ManualClock();
        final ExponentialDelayRestartBackoffTimeStrategyFactory restartStrategyFactory =
                new ExponentialDelayRestartBackoffTimeStrategyFactory(
                        clock,
                        1L,
                        maxBackoffMS,
                        2.0,
                        Integer.MAX_VALUE,
                        jitterFactor,
                        Integer.MAX_VALUE);

        assertCorrectRandomRangeWithFailureCount(
                restartStrategyFactory, clock, maxBackoffMS + 1, 1, 1L, 2L);

        assertCorrectRandomRangeWithFailureCount(
                restartStrategyFactory, clock, maxBackoffMS + 1, 2, 1L, 2L, 3L, 4L);

        assertCorrectRandomRangeWithFailureCount(
                restartStrategyFactory, clock, maxBackoffMS + 1, 3, 1L, 2L, 3L, 4L, 5L, 6L, 7L);
    }

    private void assertCorrectRandomRangeWithFailureCount(
            ExponentialDelayRestartBackoffTimeStrategyFactory factory,
            ManualClock clock,
            long advanceMsEachFailure,
            int failureCount,
            Long... expectedNumbers)
            throws Exception {
        assertCorrectRandomRange(
                () -> {
                    RestartBackoffTimeStrategy restartStrategy = factory.create();
                    for (int i = 0; i < failureCount; i++) {
                        clock.advanceTime(Duration.ofMillis(advanceMsEachFailure));
                        assertThat(restartStrategy.notifyFailure(failure)).isTrue();
                    }
                    return restartStrategy.getBackoffTime();
                },
                expectedNumbers);
    }

    @Test
    void testMultipleSettings() {
        ManualClock clock = new ManualClock();
        final long initialBackoffMS = 1L;
        final long maxBackoffMS = 9L;
        double backoffMultiplier = 2.0;
        final long resetBackoffThresholdMS = 80L;
        double jitterFactor = 0.25;

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        clock,
                        initialBackoffMS,
                        maxBackoffMS,
                        backoffMultiplier,
                        resetBackoffThresholdMS,
                        jitterFactor,
                        Integer.MAX_VALUE);

        // ensure initial data
        assertThat(restartStrategy.notifyFailure(failure)).isTrue();
        assertThat(restartStrategy.canRestart()).isTrue();
        assertThat(restartStrategy.getBackoffTime()).isEqualTo(initialBackoffMS);

        // ensure backoff time is initial after the first failure
        clock.advanceTime(resetBackoffThresholdMS + 1, TimeUnit.MILLISECONDS);
        assertThat(restartStrategy.notifyFailure(failure)).isTrue();
        assertThat(restartStrategy.canRestart()).isTrue();
        assertThat(restartStrategy.getBackoffTime()).isEqualTo(initialBackoffMS);

        // ensure backoff increases until threshold is reached
        clock.advanceTime(4, TimeUnit.MILLISECONDS);
        assertThat(restartStrategy.notifyFailure(failure)).isTrue();
        assertThat(restartStrategy.canRestart()).isTrue();
        assertThat(restartStrategy.getBackoffTime()).isEqualTo(2L);

        // ensure backoff is reset after threshold is reached
        clock.advanceTime(resetBackoffThresholdMS + 9 + 1, TimeUnit.MILLISECONDS);
        assertThat(restartStrategy.notifyFailure(failure)).isTrue();
        assertThat(restartStrategy.canRestart()).isTrue();
        assertThat(restartStrategy.getBackoffTime()).isOne();
        clock.advanceTime(Duration.ofMillis(maxBackoffMS + 1));

        // ensure backoff still increases
        assertThat(restartStrategy.notifyFailure(failure)).isTrue();
        assertThat(restartStrategy.canRestart()).isTrue();
        assertThat(restartStrategy.getBackoffTime()).isEqualTo(2L);
    }

    @Test
    void testMergeMultipleExceptionsIntoOneAttempt() {
        ManualClock clock = new ManualClock();
        long initialBackoffMS = 2L;
        double backoffMultiplier = 2.0d;
        final long maxBackoffMS = 6L;
        final long resetBackoffThresholdMS = 80L;

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        clock,
                        initialBackoffMS,
                        maxBackoffMS,
                        backoffMultiplier,
                        resetBackoffThresholdMS,
                        0.d,
                        3);

        // All exceptions merged into one attempt if the time is same.
        long currentBackOffMs = initialBackoffMS;
        checkMultipleExceptionsAreMerged(clock, currentBackOffMs, restartStrategy);

        // After advance time it's a new round, so new exception will be a new attempt.
        clock.advanceTime(1, TimeUnit.MILLISECONDS);
        currentBackOffMs *= backoffMultiplier;
        checkMultipleExceptionsAreMerged(clock, currentBackOffMs, restartStrategy);

        // After advance time it's a new round, so new exception will be a new attempt.
        clock.advanceTime(1, TimeUnit.MILLISECONDS);
        currentBackOffMs = maxBackoffMS;
        checkMultipleExceptionsAreMerged(clock, currentBackOffMs, restartStrategy);

        // After advance time it's a new round, and it reaches the maxAttempts.
        clock.advanceTime(1, TimeUnit.MILLISECONDS);
        assertThat(restartStrategy.notifyFailure(failure)).isTrue();
        assertThat(restartStrategy.canRestart()).isFalse();
    }

    @Test
    void testMergingExceptionsWorksWithResetting() {
        ManualClock clock = new ManualClock();
        long initialBackoffMS = 2L;
        double backoffMultiplier = 2.0d;
        final long maxBackoffMS = 6L;
        final long resetBackoffThresholdMS = 80L;

        final ExponentialDelayRestartBackoffTimeStrategy restartStrategy =
                new ExponentialDelayRestartBackoffTimeStrategy(
                        clock,
                        initialBackoffMS,
                        maxBackoffMS,
                        backoffMultiplier,
                        resetBackoffThresholdMS,
                        0.d,
                        3);

        // Test the merging logic works well after a series of resetting.
        for (int i = 0; i < 10; i++) {
            // All exceptions merged into one attempt if the time is same.
            long currentBackOffMs = initialBackoffMS;
            checkMultipleExceptionsAreMerged(clock, currentBackOffMs, restartStrategy);

            // After advance time it's a new round, so new exception will be a new attempt.
            clock.advanceTime(1, TimeUnit.MILLISECONDS);
            currentBackOffMs *= backoffMultiplier;
            checkMultipleExceptionsAreMerged(clock, currentBackOffMs, restartStrategy);

            // After advance time it's a new round, so new exception will be a new attempt.
            clock.advanceTime(1, TimeUnit.MILLISECONDS);
            currentBackOffMs = maxBackoffMS;
            checkMultipleExceptionsAreMerged(clock, currentBackOffMs, restartStrategy);

            // After resetBackoffThresholdMS, the restartStrategy should be reset.
            clock.advanceTime(resetBackoffThresholdMS, TimeUnit.MILLISECONDS);
        }
    }

    private void checkMultipleExceptionsAreMerged(
            ManualClock clock,
            long expectedBackoffMS,
            ExponentialDelayRestartBackoffTimeStrategy restartStrategy) {
        boolean expectedNewAttempt = true;
        for (int advanceMs = 0; advanceMs < expectedBackoffMS; advanceMs++) {
            for (int i = 0; i < 10; i++) {
                assertThat(restartStrategy.notifyFailure(failure)).isEqualTo(expectedNewAttempt);
                if (expectedNewAttempt) {
                    // Only the first one is new attempt, all rest of failures aren't new attempt.
                    expectedNewAttempt = false;
                }

                assertThat(restartStrategy.canRestart()).isTrue();
                assertThat(restartStrategy.getBackoffTime())
                        .isEqualTo(expectedBackoffMS - advanceMs);
            }
            clock.advanceTime(1, TimeUnit.MILLISECONDS);
        }
    }

    private void assertCorrectRandomRange(Callable<Long> numberGenerator, Long... expectedNumbers)
            throws Exception {
        Set<Long> generatedNumbers = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            long generatedNumber = numberGenerator.call();
            generatedNumbers.add(generatedNumber);
        }
        assertThat(generatedNumbers).isEqualTo(new HashSet<>(Arrays.asList(expectedNumbers)));
    }
}
