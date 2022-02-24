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

package org.apache.flink.runtime.dispatcher.cleanup;

import org.apache.flink.configuration.CleanupOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;

import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * {@code CleanupRetryStrategyFactory} is used to create the {@link RetryStrategy} for the {@link
 * DispatcherResourceCleanerFactory}. It extracts all the necessary information from the passed
 * {@link Configuration}.
 *
 * @see CleanupOptions
 */
public enum CleanupRetryStrategyFactory {
    INSTANCE;

    /** Creates the {@link RetryStrategy} instance based on the passed {@link Configuration}. */
    public RetryStrategy createRetryStrategy(Configuration configuration) {
        final String configuredRetryStrategy =
                configuration.getString(CleanupOptions.CLEANUP_STRATEGY);
        if (isRetryStrategy(
                CleanupOptions.FIXED_DELAY_LABEL,
                configuration.getString(CleanupOptions.CLEANUP_STRATEGY))) {
            return createFixedRetryStrategy(configuration);
        } else if (isRetryStrategy(
                CleanupOptions.EXPONENTIAL_DELAY_LABEL,
                configuration.getString(CleanupOptions.CLEANUP_STRATEGY))) {
            return createExponentialBackoffRetryStrategy(configuration);
        } else if (retryingDisabled(configuredRetryStrategy)) {
            return createNoRetryStrategy();
        }

        throw new IllegalArgumentException(
                createInvalidCleanupStrategyErrorMessage(configuredRetryStrategy));
    }

    private static RetryStrategy createNoRetryStrategy() {
        return new FixedRetryStrategy(0, Duration.ZERO);
    }

    private static FixedRetryStrategy createFixedRetryStrategy(Configuration configuration) {
        final Duration delay = configuration.get(CleanupOptions.CLEANUP_STRATEGY_FIXED_DELAY_DELAY);
        final int maxAttempts =
                configuration.get(CleanupOptions.CLEANUP_STRATEGY_FIXED_DELAY_ATTEMPTS);

        return new FixedRetryStrategy(maxAttempts, delay);
    }

    private static ExponentialBackoffRetryStrategy createExponentialBackoffRetryStrategy(
            Configuration configuration) {
        final Duration minDelay =
                configuration.get(
                        CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
        final Duration maxDelay =
                configuration.get(CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF);
        final int maxAttempts =
                configuration.getInteger(
                        CleanupOptions.CLEANUP_STRATEGY_EXPONENTIAL_DELAY_MAX_ATTEMPTS,
                        Integer.MAX_VALUE);

        return new ExponentialBackoffRetryStrategy(maxAttempts, minDelay, maxDelay);
    }

    private static String createInvalidCleanupStrategyErrorMessage(String configuredRetryStrategy) {
        final StringBuilder msgBuilder = new StringBuilder();
        msgBuilder
                .append("Unknown cleanup retry strategy '")
                .append(configuredRetryStrategy)
                .append("'. Valid strategies are ");

        final String validOptionsStr =
                Stream.concat(
                                Stream.of(
                                        CleanupOptions.EXPONENTIAL_DELAY_LABEL,
                                        CleanupOptions.FIXED_DELAY_LABEL),
                                CleanupOptions.NONE_PARAM_VALUES.stream())
                        .collect(Collectors.joining(", "));

        return msgBuilder.append(validOptionsStr).toString();
    }

    private static boolean retryingDisabled(String configuredRetryStrategy) {
        final String retryStrategy = configuredRetryStrategy.toLowerCase();
        return CleanupOptions.NONE_PARAM_VALUES.contains(retryStrategy);
    }

    private static boolean isRetryStrategy(
            String retryStrategyLabel, String configuredRetryStrategy) {
        final String retryStrategy = configuredRetryStrategy.toLowerCase();
        return retryStrategy.equals(retryStrategyLabel)
                || retryStrategy.equals(
                        CleanupOptions.extractAlphaNumericCharacters(retryStrategyLabel));
    }
}
