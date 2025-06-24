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

package org.apache.flink.api.common;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestartStrategyOptions;

import java.time.Duration;
import java.util.Optional;

/** Utility class for describing restart strategies. */
@Internal
public class RestartStrategyDescriptionUtils {

    /**
     * Returns a descriptive string of the restart strategy configured in the given Configuration
     * object.
     *
     * @param configuration the Configuration to extract the restart strategy from
     * @return a description of the restart strategy
     */
    public static String getRestartStrategyDescription(Configuration configuration) {
        final Optional<String> restartStrategyNameOptional =
                configuration.getOptional(RestartStrategyOptions.RESTART_STRATEGY);

        return restartStrategyNameOptional
                .map(
                        restartStrategyName -> {
                            switch (RestartStrategyOptions.RestartStrategyType.of(
                                    restartStrategyName.toLowerCase())) {
                                case NO_RESTART_STRATEGY:
                                    return "Restart deactivated.";
                                case FIXED_DELAY:
                                    return getFixedDelayDescription(configuration);
                                case FAILURE_RATE:
                                    return getFailureRateDescription(configuration);
                                case EXPONENTIAL_DELAY:
                                    return getExponentialDelayDescription(configuration);
                                default:
                                    throw new IllegalArgumentException(
                                            "Unknown restart strategy "
                                                    + restartStrategyName
                                                    + ".");
                            }
                        })
                .orElse("Cluster level default restart strategy");
    }

    private static String getExponentialDelayDescription(Configuration configuration) {
        Duration initialBackoff =
                configuration.get(
                        RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
        Duration maxBackoff =
                configuration.get(
                        RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF);
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
                        RestartStrategyOptions.RESTART_STRATEGY_EXPONENTIAL_DELAY_JITTER_FACTOR);
        return String.format(
                "Restart with exponential delay: starting at %s, increasing by %f, with maximum %s. "
                        + "Delay resets after %s with jitter %f",
                initialBackoff, backoffMultiplier, maxBackoff, resetBackoffThreshold, jitter);
    }

    private static String getFailureRateDescription(Configuration configuration) {
        int maxFailures =
                configuration.get(
                        RestartStrategyOptions
                                .RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL);
        Duration failureRateInterval =
                configuration.get(
                        RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL);
        Duration failureRateDelay =
                configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FAILURE_RATE_DELAY);

        return String.format(
                "Failure rate restart with maximum of %d failures within interval %s and fixed delay %s.",
                maxFailures, failureRateInterval, failureRateDelay);
    }

    private static String getFixedDelayDescription(Configuration configuration) {
        int attempts =
                configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS);
        Duration delay =
                configuration.get(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_DELAY);
        return String.format(
                "Restart with fixed delay (%s). #%d restart attempts.", delay, attempts);
    }
}
