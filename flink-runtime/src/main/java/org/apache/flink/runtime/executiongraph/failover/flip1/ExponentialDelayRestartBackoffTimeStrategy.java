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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.SystemClock;

import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Restart strategy which tries to restart indefinitely number of times with an exponential backoff
 * time in between. The delay starts at initial value and keeps increasing (multiplying by backoff
 * multiplier) until maximum delay is reached.
 *
 * <p>If the tasks are running smoothly for some time, backoff is reset to its initial value.
 */
public class ExponentialDelayRestartBackoffTimeStrategy implements RestartBackoffTimeStrategy {

    private final long initialBackoffMS;

    private final long maxBackoffMS;

    private final double backoffMultiplier;

    private final long resetBackoffThresholdMS;

    private final double jitterFactor;

    private final String strategyString;

    private final Clock clock;

    private long currentBackoffMS;

    private long lastFailureTimestamp;

    ExponentialDelayRestartBackoffTimeStrategy(
            Clock clock,
            long initialBackoffMS,
            long maxBackoffMS,
            double backoffMultiplier,
            long resetBackoffThresholdMS,
            double jitterFactor) {

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

        this.initialBackoffMS = initialBackoffMS;
        setInitialBackoff();
        this.maxBackoffMS = maxBackoffMS;
        this.backoffMultiplier = backoffMultiplier;
        this.resetBackoffThresholdMS = resetBackoffThresholdMS;
        this.jitterFactor = jitterFactor;

        this.clock = checkNotNull(clock);
        this.lastFailureTimestamp = 0;
        this.strategyString = generateStrategyString();
    }

    @Override
    public boolean canRestart() {
        return true;
    }

    @Override
    public long getBackoffTime() {
        long backoffWithJitter = currentBackoffMS + calculateJitterBackoffMS();
        return Math.min(backoffWithJitter, maxBackoffMS);
    }

    @Override
    public void notifyFailure(Throwable cause) {
        long now = clock.absoluteTimeMillis();
        if ((now - lastFailureTimestamp) >= (resetBackoffThresholdMS + currentBackoffMS)) {
            setInitialBackoff();
        } else {
            increaseBackoff();
        }
        lastFailureTimestamp = now;
    }

    @Override
    public String toString() {
        return strategyString;
    }

    private void setInitialBackoff() {
        currentBackoffMS = initialBackoffMS;
    }

    private void increaseBackoff() {
        if (currentBackoffMS < maxBackoffMS) {
            currentBackoffMS *= backoffMultiplier;
        }
        currentBackoffMS = Math.max(initialBackoffMS, Math.min(currentBackoffMS, maxBackoffMS));
    }

    /**
     * Calculate jitter offset to avoid thundering herd scenario. The offset range increases with
     * the number of restarts.
     *
     * <p>F.e. for backoff time 8 with jitter 0.25, it generates random number in range [-2, 2].
     *
     * @return random value in interval [-n, n], where n represents jitter * current backoff
     */
    private long calculateJitterBackoffMS() {
        if (jitterFactor == 0) {
            return 0;
        } else {
            long offset = (long) (currentBackoffMS * jitterFactor);
            return ThreadLocalRandom.current().nextLong(-offset, offset + 1);
        }
    }

    private String generateStrategyString() {
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
                + ", currentBackoffMS="
                + currentBackoffMS
                + ", lastFailureTimestamp="
                + lastFailureTimestamp
                + ")";
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

        return new ExponentialDelayRestartBackoffTimeStrategyFactory(
                initialBackoffMS,
                maxBackoffMS,
                backoffMultiplier,
                resetBackoffThresholdMS,
                jitterFactor);
    }

    /** The factory for creating {@link ExponentialDelayRestartBackoffTimeStrategy}. */
    public static class ExponentialDelayRestartBackoffTimeStrategyFactory implements Factory {

        private final long initialBackoffMS;
        private final long maxBackoffMS;
        private final double backoffMultiplier;
        private final long resetBackoffThresholdMS;
        private final double jitterFactor;

        public ExponentialDelayRestartBackoffTimeStrategyFactory(
                long initialBackoffMS,
                long maxBackoffMS,
                double backoffMultiplier,
                long resetBackoffThresholdMS,
                double jitterFactor) {
            this.initialBackoffMS = initialBackoffMS;
            this.maxBackoffMS = maxBackoffMS;
            this.backoffMultiplier = backoffMultiplier;
            this.resetBackoffThresholdMS = resetBackoffThresholdMS;
            this.jitterFactor = jitterFactor;
        }

        @Override
        public RestartBackoffTimeStrategy create() {
            return new ExponentialDelayRestartBackoffTimeStrategy(
                    SystemClock.getInstance(),
                    initialBackoffMS,
                    maxBackoffMS,
                    backoffMultiplier,
                    resetBackoffThresholdMS,
                    jitterFactor);
        }
    }
}
