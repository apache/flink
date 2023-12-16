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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Restart strategy which tries to restart indefinitely number of times with an exponential backoff
 * time in between. The delay starts at initial value and keeps increasing (multiplying by backoff
 * multiplier) until maximum delay is reached.
 *
 * <p>If the tasks are running smoothly for some time, backoff is reset to its initial value.
 */
public class ExponentialDelayRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {

    private static final long DEFAULT_NEXT_RESTART_TIMESTAMP = Integer.MIN_VALUE;

    private final long initialBackoffMS;

    private final long maxBackoffMS;

    private final double backoffMultiplier;

    private final long resetBackoffThresholdMS;

    private final double jitterFactor;

    private final int attemptsBeforeResetBackoff;

    private final Clock clock;

    private int currentRestartAttempt;

    private long nextRestartTimestamp;

    ExponentialDelayRestartBackoffTimeStrategy(
            Clock clock,
            long initialBackoffMS,
            long maxBackoffMS,
            double backoffMultiplier,
            long resetBackoffThresholdMS,
            double jitterFactor,
            int attemptsBeforeResetBackoff) {

        checkArgument(initialBackoffMS >= 1, "Initial backoff must be at least 1.");
        checkArgument(maxBackoffMS >= 1, "Maximum backoff must be at least 1.");
        checkArgument(
                initialBackoffMS <= maxBackoffMS,
                "Initial backoff cannot be higher than maximum backoff.");
        checkArgument(backoffMultiplier > 1, "Backoff multiplier must be greater than 1.");
        checkArgument(
                resetBackoffThresholdMS >= 1,
                "Threshold duration for exponential backoff reset must be at least 1.");
        checkArgument(
                0 <= jitterFactor && jitterFactor <= 1, "Jitter factor must be >= 0 and <= 1.");
        checkArgument(
                attemptsBeforeResetBackoff >= 1,
                "The attemptsBeforeResetBackoff must be at least 1.");

        this.initialBackoffMS = initialBackoffMS;
        setInitialBackoff();
        this.maxBackoffMS = maxBackoffMS;
        this.backoffMultiplier = backoffMultiplier;
        this.resetBackoffThresholdMS = resetBackoffThresholdMS;
        this.jitterFactor = jitterFactor;
        this.attemptsBeforeResetBackoff = attemptsBeforeResetBackoff;

        this.clock = checkNotNull(clock);
        this.nextRestartTimestamp = DEFAULT_NEXT_RESTART_TIMESTAMP;
    }

    @Override
    public boolean canRestart() {
        return currentRestartAttempt <= attemptsBeforeResetBackoff;
    }

    @Override
    public long getBackoffTime() {
        checkState(
                nextRestartTimestamp != DEFAULT_NEXT_RESTART_TIMESTAMP,
                "Please call notifyFailure first.");
        return Math.max(0, nextRestartTimestamp - clock.absoluteTimeMillis());
    }

    @Override
    public boolean notifyFailure(Throwable cause) {
        long now = clock.absoluteTimeMillis();

        // Merge multiple failures into one attempt if there are tasks will be restarted later.
        if (now <= nextRestartTimestamp) {
            return false;
        }

        if ((now - nextRestartTimestamp) >= resetBackoffThresholdMS) {
            setInitialBackoff();
        }
        nextRestartTimestamp = now + calculateActualBackoffTime();
        currentRestartAttempt++;
        return true;
    }

    @VisibleForTesting
    long getInitialBackoffMS() {
        return initialBackoffMS;
    }

    @VisibleForTesting
    long getMaxBackoffMS() {
        return maxBackoffMS;
    }

    @VisibleForTesting
    double getBackoffMultiplier() {
        return backoffMultiplier;
    }

    @VisibleForTesting
    long getResetBackoffThresholdMS() {
        return resetBackoffThresholdMS;
    }

    @VisibleForTesting
    double getJitterFactor() {
        return jitterFactor;
    }

    @VisibleForTesting
    int getAttemptsBeforeResetBackoff() {
        return attemptsBeforeResetBackoff;
    }

    @Override
    public String toString() {
        return "ExponentialDelayRestartBackoffTimeStrategy(initialBackoffMS="
                + initialBackoffMS
                + ", maxBackoffMS="
                + maxBackoffMS
                + ", backoffMultiplier="
                + backoffMultiplier
                + ", resetBackoffThresholdMS="
                + resetBackoffThresholdMS
                + ", jitterFactor="
                + jitterFactor
                + ", attemptsBeforeResetBackoff="
                + attemptsBeforeResetBackoff
                + ", currentRestartAttempt="
                + currentRestartAttempt
                + ", nextRestartTimestamp="
                + nextRestartTimestamp
                + ")";
    }

    private void setInitialBackoff() {
        currentRestartAttempt = 0;
    }

    private long calculateActualBackoffTime() {
        long currentBackoffTime =
                (long) (initialBackoffMS * Math.pow(backoffMultiplier, currentRestartAttempt));
        return Math.max(
                initialBackoffMS,
                Math.min(
                        currentBackoffTime + calculateJitterBackoffMS(currentBackoffTime),
                        maxBackoffMS));
    }

    /**
     * Calculate jitter offset to avoid thundering herd scenario. The offset range increases with
     * the number of restarts.
     *
     * <p>F.e. for backoff time 8 with jitter 0.25, it generates random number in range [-2, 2].
     *
     * @return random value in interval [-n, n], where n represents jitter * current backoff
     */
    private long calculateJitterBackoffMS(long currentBackoffMS) {
        if (jitterFactor == 0) {
            return 0;
        } else {
            long offset = (long) (currentBackoffMS * jitterFactor);
            return ThreadLocalRandom.current().nextLong(-offset, offset + 1);
        }
    }

    public static ExponentialDelayRestartBackoffTimeStrategyFactory createFactory(
            final Configuration configuration) {
        long initialBackoffMS =
                configuration
                        .get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF)
                        .toMillis();
        long maxBackoffMS =
                configuration
                        .get(RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF)
                        .toMillis();
        double backoffMultiplier =
                configuration.get(
                        RestartStrategyOptions
                                .RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER);
        long resetBackoffThresholdMS =
                configuration
                        .get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD)
                        .toMillis();
        double jitterFactor =
                configuration.get(
                        RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR);

        int attemptsBeforeResetBackoff =
                configuration.get(
                        RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_ATTEMPTS);

        return new ExponentialDelayRestartBackoffTimeStrategyFactory(
                initialBackoffMS,
                maxBackoffMS,
                backoffMultiplier,
                resetBackoffThresholdMS,
                jitterFactor,
                attemptsBeforeResetBackoff);
    }

    /** The factory for creating {@link ExponentialDelayRestartBackoffTimeStrategy}. */
    public static class ExponentialDelayRestartBackoffTimeStrategyFactory implements Factory {

        private final Clock clock;
        private final long initialBackoffMS;
        private final long maxBackoffMS;
        private final double backoffMultiplier;
        private final long resetBackoffThresholdMS;
        private final double jitterFactor;
        private final int attemptsBeforeResetBackoff;

        public ExponentialDelayRestartBackoffTimeStrategyFactory(
                long initialBackoffMS,
                long maxBackoffMS,
                double backoffMultiplier,
                long resetBackoffThresholdMS,
                double jitterFactor,
                int attemptsBeforeResetBackoff) {
            this(
                    SystemClock.getInstance(),
                    initialBackoffMS,
                    maxBackoffMS,
                    backoffMultiplier,
                    resetBackoffThresholdMS,
                    jitterFactor,
                    attemptsBeforeResetBackoff);
        }

        @VisibleForTesting
        ExponentialDelayRestartBackoffTimeStrategyFactory(
                Clock clock,
                long initialBackoffMS,
                long maxBackoffMS,
                double backoffMultiplier,
                long resetBackoffThresholdMS,
                double jitterFactor,
                int attemptsBeforeResetBackoff) {
            this.clock = clock;
            this.initialBackoffMS = initialBackoffMS;
            this.maxBackoffMS = maxBackoffMS;
            this.backoffMultiplier = backoffMultiplier;
            this.resetBackoffThresholdMS = resetBackoffThresholdMS;
            this.jitterFactor = jitterFactor;
            this.attemptsBeforeResetBackoff = attemptsBeforeResetBackoff;
        }

        @Override
        public RestartBackoffTimeStrategy create() {
            return new ExponentialDelayRestartBackoffTimeStrategy(
                    clock,
                    initialBackoffMS,
                    maxBackoffMS,
                    backoffMultiplier,
                    resetBackoffThresholdMS,
                    jitterFactor,
                    attemptsBeforeResetBackoff);
        }
    }
}
