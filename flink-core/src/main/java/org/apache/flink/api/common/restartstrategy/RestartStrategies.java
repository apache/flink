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

package org.apache.flink.api.common.restartstrategy;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.RestartStrategyOptions.RestartStrategyType;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * This class defines methods to generate RestartStrategyConfigurations. These configurations are
 * used to create RestartStrategies at runtime.
 *
 * <p>The RestartStrategyConfigurations are used to decouple the core module from the runtime
 * module.
 *
 * @deprecated The {@link RestartStrategies} class is marked as deprecated because starting from
 *     Flink 1.19, all complex Java objects related to configuration should be replaced by
 *     ConfigOption. In a future major version of Flink, this class will be removed entirely. It is
 *     recommended to switch to using the ConfigOptions provided by {@link
 *     org.apache.flink.configuration.RestartStrategyOptions} for configuring restart strategies
 *     like the following code snippet:
 *     <pre>{@code
 * Configuration config = new Configuration();
 * config.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
 * config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 3);
 * config.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY, Duration.ofMinutes(1));
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
 * }</pre>
 *     For more details on using ConfigOption for restart strategies, please refer to the Flink
 *     documentation: <a
 *     href="https://nightlies.apache.org/flink/flink-docs-stable/docs/ops/state/task_failure_recovery/#restart-strategies">restart-strategies</a>
 */
@Deprecated
@PublicEvolving
public class RestartStrategies {

    /**
     * Generates NoRestartStrategyConfiguration.
     *
     * @return NoRestartStrategyConfiguration
     */
    public static RestartStrategyConfiguration noRestart() {
        return new NoRestartStrategyConfiguration();
    }

    public static RestartStrategyConfiguration fallBackRestart() {
        return new FallbackRestartStrategyConfiguration();
    }

    /**
     * Generates a FixedDelayRestartStrategyConfiguration.
     *
     * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
     * @param delayBetweenAttempts Delay in-between restart attempts for the
     *     FixedDelayRestartStrategy
     * @return FixedDelayRestartStrategy
     */
    public static RestartStrategyConfiguration fixedDelayRestart(
            int restartAttempts, long delayBetweenAttempts) {
        return fixedDelayRestart(
                restartAttempts, Time.of(delayBetweenAttempts, TimeUnit.MILLISECONDS));
    }

    /**
     * Generates a FixedDelayRestartStrategyConfiguration.
     *
     * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
     * @param delayInterval Delay in-between restart attempts for the FixedDelayRestartStrategy
     * @return FixedDelayRestartStrategy
     * @deprecated Use {@link #fixedDelayRestart(int, Duration)}
     */
    @Deprecated
    public static RestartStrategyConfiguration fixedDelayRestart(
            int restartAttempts, Time delayInterval) {
        return fixedDelayRestart(restartAttempts, Time.toDuration(delayInterval));
    }

    /**
     * Generates a FixedDelayRestartStrategyConfiguration.
     *
     * @param restartAttempts Number of restart attempts for the FixedDelayRestartStrategy
     * @param delayInterval Delay in-between restart attempts for the FixedDelayRestartStrategy
     * @return FixedDelayRestartStrategy
     */
    public static RestartStrategyConfiguration fixedDelayRestart(
            int restartAttempts, Duration delayInterval) {
        return new FixedDelayRestartStrategyConfiguration(restartAttempts, delayInterval);
    }

    /**
     * Generates a FailureRateRestartStrategyConfiguration.
     *
     * @param failureRate Maximum number of restarts in given interval {@code failureInterval}
     *     before failing a job
     * @param failureInterval Time interval for failures
     * @param delayInterval Delay in-between restart attempts
     * @deprecated Use {@link #failureRateRestart(int, Duration, Duration)}
     */
    @Deprecated
    public static FailureRateRestartStrategyConfiguration failureRateRestart(
            int failureRate, Time failureInterval, Time delayInterval) {
        return failureRateRestart(
                failureRate, Time.toDuration(failureInterval), Time.toDuration(delayInterval));
    }

    /**
     * Generates a FailureRateRestartStrategyConfiguration.
     *
     * @param failureRate Maximum number of restarts in given interval {@code failureInterval}
     *     before failing a job
     * @param failureInterval Time interval for failures
     * @param delayInterval Delay in-between restart attempts
     */
    public static FailureRateRestartStrategyConfiguration failureRateRestart(
            int failureRate, Duration failureInterval, Duration delayInterval) {
        return new FailureRateRestartStrategyConfiguration(
                failureRate, failureInterval, delayInterval);
    }

    /**
     * Generates a ExponentialDelayRestartStrategyConfiguration.
     *
     * @param initialBackoff Starting duration between restarts
     * @param maxBackoff The highest possible duration between restarts
     * @param backoffMultiplier Delay multiplier how many times is the delay longer than before
     * @param resetBackoffThreshold How long the job must run smoothly to reset the time interval
     * @param jitterFactor How much the delay may differ (in percentage)
     * @deprecated Use {@link #exponentialDelayRestart(Duration, Duration, double, Duration,
     *     double)}
     */
    @Deprecated
    public static ExponentialDelayRestartStrategyConfiguration exponentialDelayRestart(
            Time initialBackoff,
            Time maxBackoff,
            double backoffMultiplier,
            Time resetBackoffThreshold,
            double jitterFactor) {
        return exponentialDelayRestart(
                Time.toDuration(initialBackoff),
                Time.toDuration(maxBackoff),
                backoffMultiplier,
                Time.toDuration(resetBackoffThreshold),
                jitterFactor);
    }

    /**
     * Generates a ExponentialDelayRestartStrategyConfiguration.
     *
     * @param initialBackoff Starting duration between restarts
     * @param maxBackoff The highest possible duration between restarts
     * @param backoffMultiplier Delay multiplier how many times is the delay longer than before
     * @param resetBackoffThreshold How long the job must run smoothly to reset the time interval
     * @param jitterFactor How much the delay may differ (in percentage)
     */
    public static ExponentialDelayRestartStrategyConfiguration exponentialDelayRestart(
            Duration initialBackoff,
            Duration maxBackoff,
            double backoffMultiplier,
            Duration resetBackoffThreshold,
            double jitterFactor) {
        return new ExponentialDelayRestartStrategyConfiguration(
                initialBackoff, maxBackoff, backoffMultiplier, resetBackoffThreshold, jitterFactor);
    }

    /** Abstract configuration for restart strategies. */
    @PublicEvolving
    public abstract static class RestartStrategyConfiguration implements Serializable {
        private static final long serialVersionUID = 6285853591578313960L;

        private RestartStrategyConfiguration() {}

        /**
         * Returns a description which is shown in the web interface.
         *
         * @return Description of the restart strategy
         */
        public abstract String getDescription();

        @Override
        public String toString() {
            return getDescription();
        }
    }

    /** Configuration representing no restart strategy. */
    @PublicEvolving
    public static final class NoRestartStrategyConfiguration extends RestartStrategyConfiguration {
        private static final long serialVersionUID = -5894362702943349962L;

        @Override
        public String getDescription() {
            return "Restart deactivated.";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof NoRestartStrategyConfiguration;
        }

        @Override
        public int hashCode() {
            return Objects.hash();
        }
    }

    /** Configuration representing a fixed delay restart strategy. */
    @PublicEvolving
    public static final class FixedDelayRestartStrategyConfiguration
            extends RestartStrategyConfiguration {
        private static final long serialVersionUID = 4149870149673363190L;

        private final int restartAttempts;
        private final Duration delayBetweenAttemptsInterval;

        FixedDelayRestartStrategyConfiguration(
                int restartAttempts, Duration delayBetweenAttemptsInterval) {
            this.restartAttempts = restartAttempts;
            this.delayBetweenAttemptsInterval = delayBetweenAttemptsInterval;
        }

        public int getRestartAttempts() {
            return restartAttempts;
        }

        /** @deprecated Use {@link #getDurationBetweenAttempts()} */
        @Deprecated
        public Time getDelayBetweenAttemptsInterval() {
            return Time.fromDuration(getDurationBetweenAttempts());
        }

        public Duration getDurationBetweenAttempts() {
            return delayBetweenAttemptsInterval;
        }

        @Override
        public int hashCode() {
            int result = restartAttempts;
            result =
                    31 * result
                            + (delayBetweenAttemptsInterval != null
                                    ? delayBetweenAttemptsInterval.hashCode()
                                    : 0);
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj instanceof FixedDelayRestartStrategyConfiguration) {
                FixedDelayRestartStrategyConfiguration other =
                        (FixedDelayRestartStrategyConfiguration) obj;

                return restartAttempts == other.restartAttempts
                        && delayBetweenAttemptsInterval.equals(other.delayBetweenAttemptsInterval);
            } else {
                return false;
            }
        }

        @Override
        public String getDescription() {
            return String.format(
                    "Restart with fixed delay (%s). #%d restart attempts.",
                    delayBetweenAttemptsInterval, restartAttempts);
        }
    }

    /** Configuration representing an exponential delay restart strategy. */
    @PublicEvolving
    public static final class ExponentialDelayRestartStrategyConfiguration
            extends RestartStrategyConfiguration {
        private static final long serialVersionUID = 1467941615941965194L;

        private final Duration initialBackoff;
        private final Duration maxBackoff;
        private final double backoffMultiplier;
        private final Duration resetBackoffThreshold;
        private final double jitterFactor;

        /**
         * @deprecated Use {@link
         *     ExponentialDelayRestartStrategyConfiguration#ExponentialDelayRestartStrategyConfiguration(Duration,
         *     Duration, double, Duration, double)}
         */
        @Deprecated
        public ExponentialDelayRestartStrategyConfiguration(
                Time initialBackoff,
                Time maxBackoff,
                double backoffMultiplier,
                Time resetBackoffThreshold,
                double jitterFactor) {
            this(
                    Time.toDuration(initialBackoff),
                    Time.toDuration(maxBackoff),
                    backoffMultiplier,
                    Time.toDuration(resetBackoffThreshold),
                    jitterFactor);
        }

        public ExponentialDelayRestartStrategyConfiguration(
                Duration initialBackoff,
                Duration maxBackoff,
                double backoffMultiplier,
                Duration resetBackoffThreshold,
                double jitterFactor) {
            this.initialBackoff = initialBackoff;
            this.maxBackoff = maxBackoff;
            this.backoffMultiplier = backoffMultiplier;
            this.resetBackoffThreshold = resetBackoffThreshold;
            this.jitterFactor = jitterFactor;
        }

        /** @deprecated Use {@link #getInitialBackoffDuration()} */
        @Deprecated
        public Time getInitialBackoff() {
            return Time.fromDuration(getInitialBackoffDuration());
        }

        public Duration getInitialBackoffDuration() {
            return initialBackoff;
        }

        /** @deprecated Use {@link #getMaxBackoffDuration()} */
        @Deprecated
        public Time getMaxBackoff() {
            return Time.fromDuration(maxBackoff);
        }

        public Duration getMaxBackoffDuration() {
            return maxBackoff;
        }

        public double getBackoffMultiplier() {
            return backoffMultiplier;
        }

        /** @deprecated Use {@link #getResetBackoffDurationThreshold()} */
        @Deprecated
        public Time getResetBackoffThreshold() {
            return Time.fromDuration(resetBackoffThreshold);
        }

        public Duration getResetBackoffDurationThreshold() {
            return resetBackoffThreshold;
        }

        public double getJitterFactor() {
            return jitterFactor;
        }

        @Override
        public String getDescription() {
            return String.format(
                    "Restart with exponential delay: starting at %s, increasing by %f, with maximum %s. "
                            + "Delay resets after %s with jitter %f",
                    initialBackoff,
                    backoffMultiplier,
                    maxBackoff,
                    resetBackoffThreshold,
                    jitterFactor);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ExponentialDelayRestartStrategyConfiguration that =
                    (ExponentialDelayRestartStrategyConfiguration) o;
            return Double.compare(that.backoffMultiplier, backoffMultiplier) == 0
                    && Double.compare(that.jitterFactor, jitterFactor) == 0
                    && Objects.equals(initialBackoff, that.initialBackoff)
                    && Objects.equals(maxBackoff, that.maxBackoff)
                    && Objects.equals(resetBackoffThreshold, that.resetBackoffThreshold);
        }

        @Override
        public int hashCode() {
            int result = initialBackoff.hashCode();
            result = 31 * result + maxBackoff.hashCode();
            result = 31 * result + (int) backoffMultiplier;
            result = 31 * result + resetBackoffThreshold.hashCode();
            result = 31 * result + (int) jitterFactor;
            return result;
        }
    }

    /** Configuration representing a failure rate restart strategy. */
    @PublicEvolving
    public static final class FailureRateRestartStrategyConfiguration
            extends RestartStrategyConfiguration {
        private static final long serialVersionUID = 1195028697539661739L;
        private final int maxFailureRate;

        private final Duration failureInterval;
        private final Duration delayBetweenAttemptsInterval;

        /**
         * @deprecated Use {@link #FailureRateRestartStrategyConfiguration(int, Duration, Duration)}
         */
        @Deprecated
        public FailureRateRestartStrategyConfiguration(
                int maxFailureRate, Time failureInterval, Time delayBetweenAttemptsInterval) {
            this(
                    maxFailureRate,
                    Time.toDuration(failureInterval),
                    Time.toDuration(delayBetweenAttemptsInterval));
        }

        public FailureRateRestartStrategyConfiguration(
                int maxFailureRate,
                Duration failureInterval,
                Duration delayBetweenAttemptsInterval) {
            this.maxFailureRate = maxFailureRate;
            this.failureInterval = failureInterval;
            this.delayBetweenAttemptsInterval = delayBetweenAttemptsInterval;
        }

        public int getMaxFailureRate() {
            return maxFailureRate;
        }

        /** @deprecated Use {@link #getFailureIntervalDuration()} */
        @Deprecated
        public Time getFailureInterval() {
            return Time.fromDuration(getFailureIntervalDuration());
        }

        public Duration getFailureIntervalDuration() {
            return failureInterval;
        }

        /** @deprecated Use {@link #getDurationBetweenAttempts()} */
        @Deprecated
        public Time getDelayBetweenAttemptsInterval() {
            return Time.fromDuration(getDurationBetweenAttempts());
        }

        public Duration getDurationBetweenAttempts() {
            return delayBetweenAttemptsInterval;
        }

        @Override
        public String getDescription() {
            return String.format(
                    "Failure rate restart with maximum of %d failures within interval %s and fixed delay %s.",
                    maxFailureRate,
                    failureInterval.toString(),
                    delayBetweenAttemptsInterval.toString());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            FailureRateRestartStrategyConfiguration that =
                    (FailureRateRestartStrategyConfiguration) o;
            return maxFailureRate == that.maxFailureRate
                    && Objects.equals(failureInterval, that.failureInterval)
                    && Objects.equals(
                            delayBetweenAttemptsInterval, that.delayBetweenAttemptsInterval);
        }

        @Override
        public int hashCode() {
            return Objects.hash(maxFailureRate, failureInterval, delayBetweenAttemptsInterval);
        }
    }

    /**
     * Restart strategy configuration that could be used by jobs to use cluster level restart
     * strategy. Useful especially when one has a custom implementation of restart strategy set via
     * config.yaml.
     */
    @PublicEvolving
    public static final class FallbackRestartStrategyConfiguration
            extends RestartStrategyConfiguration {
        private static final long serialVersionUID = -4441787204284085544L;

        @Override
        public String getDescription() {
            return "Cluster level default restart strategy";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            return o instanceof FallbackRestartStrategyConfiguration;
        }

        @Override
        public int hashCode() {
            return Objects.hash();
        }
    }

    /**
     * Reads a {@link RestartStrategyConfiguration} from a given {@link ReadableConfig}.
     *
     * @param configuration configuration object to retrieve parameters from
     * @return {@link Optional#empty()} when no restart strategy parameters provided
     */
    public static Optional<RestartStrategyConfiguration> fromConfiguration(
            ReadableConfig configuration) {
        return configuration
                .getOptional(RestartStrategyOptions.RESTART_STRATEGY)
                .map(confName -> parseConfiguration(confName, configuration));
    }

    private static RestartStrategyConfiguration parseConfiguration(
            String restartstrategyKind, ReadableConfig configuration) {
        switch (RestartStrategyType.of(restartstrategyKind.toLowerCase())) {
            case NO_RESTART_STRATEGY:
                return noRestart();
            case FIXED_DELAY:
                int attempts =
                        configuration.get(
                                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS);
                Duration delay =
                        configuration.get(
                                RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY);
                return fixedDelayRestart(attempts, delay.toMillis());
            case EXPONENTIAL_DELAY:
                Duration initialBackoff =
                        configuration.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
                Duration maxBackoff =
                        configuration.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF);
                double backoffMultiplier =
                        configuration.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_BACKOFF_MULTIPLIER);
                Duration resetBackoffThreshold =
                        configuration.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_RESET_BACKOFF_THRESHOLD);
                double jitter =
                        configuration.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR);
                return exponentialDelayRestart(
                        initialBackoff,
                        maxBackoff,
                        backoffMultiplier,
                        resetBackoffThreshold,
                        jitter);
            case FAILURE_RATE:
                int maxFailures =
                        configuration.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL);
                Duration failureRateInterval =
                        configuration.get(
                                RestartStrategyOptions
                                        .RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL);
                Duration failureRateDelay =
                        configuration.get(
                                RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY);
                return failureRateRestart(
                        maxFailures,
                        Time.milliseconds(failureRateInterval.toMillis()),
                        Time.milliseconds(failureRateDelay.toMillis()));
            default:
                throw new IllegalArgumentException(
                        "Unknown restart strategy " + restartstrategyKind + ".");
        }
    }
}
