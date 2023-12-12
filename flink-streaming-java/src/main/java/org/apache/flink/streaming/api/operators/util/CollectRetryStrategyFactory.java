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

package org.apache.flink.streaming.api.operators.util;

import org.apache.flink.configuration.CollectOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.IncrementalDelayRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;

import java.time.Duration;
import java.util.function.Supplier;

/**
 * {@code CollectRetryStrategyFactory} is used to create {@link RetryStrategy} for {@link
 * org.apache.flink.streaming.api.operators.collect.CollectResultFetcher}. It extracts all the
 * necessary information from the passed {@link Configuration}.
 *
 * @see CollectOptions
 */
public enum CollectRetryStrategyFactory {
    INSTANCE;

    /** Creates the {@link RetryStrategy} instance based on the passed {@link Configuration}. */
    public Supplier<RetryStrategy> createRetryStrategySupplier(ReadableConfig configuration) {
        final String configuredRetryStrategy = configuration.get(CollectOptions.COLLECT_STRATEGY);
        if (isRetryStrategy(
                CollectOptions.FIXED_DELAY_LABEL,
                configuration.get(CollectOptions.COLLECT_STRATEGY))) {
            return () -> createFixedRetryStrategy(configuration);
        } else if (isRetryStrategy(
                CollectOptions.EXPONENTIAL_DELAY_LABEL,
                configuration.get(CollectOptions.COLLECT_STRATEGY))) {
            return () -> createExponentialBackoffRetryStrategy(configuration);
        } else if (isRetryStrategy(
                CollectOptions.INCREMENTAL_DELAY_LABEL,
                configuration.get(CollectOptions.COLLECT_STRATEGY))) {
            return () -> createIncrementalDelayRetryStrategy(configuration);
        }

        throw new IllegalArgumentException(
                createInvalidCollectStrategyErrorMessage(configuredRetryStrategy));
    }

    private static FixedRetryStrategy createFixedRetryStrategy(ReadableConfig configuration) {
        final Duration delay = configuration.get(CollectOptions.COLLECT_STRATEGY_FIXED_DELAY_DELAY);
        final int maxAttempts =
                configuration.get(CollectOptions.COLLECT_STRATEGY_FIXED_DELAY_ATTEMPTS);

        return new FixedRetryStrategy(maxAttempts, delay);
    }

    private static ExponentialBackoffRetryStrategy createExponentialBackoffRetryStrategy(
            ReadableConfig configuration) {
        final Duration minDelay =
                configuration.get(
                        CollectOptions.COLLECT_STRATEGY_EXPONENTIAL_DELAY_INITIAL_BACKOFF);
        final Duration maxDelay =
                configuration.get(CollectOptions.COLLECT_STRATEGY_EXPONENTIAL_DELAY_MAX_BACKOFF);
        final int maxAttempts =
                configuration.get(CollectOptions.COLLECT_STRATEGY_EXPONENTIAL_DELAY_MAX_ATTEMPTS);

        return new ExponentialBackoffRetryStrategy(maxAttempts, minDelay, maxDelay);
    }

    private static IncrementalDelayRetryStrategy createIncrementalDelayRetryStrategy(
            ReadableConfig configuration) {
        final Duration initialDelay =
                configuration.get(CollectOptions.COLLECT_STRATEGY_INCREMENTAL_DELAY_INITIAL_DELAY);
        final Duration increment =
                configuration.get(CollectOptions.COLLECT_STRATEGY_INCREMENTAL_DELAY_INCREMENT);
        final int attempts =
                configuration.get(CollectOptions.COLLECT_STRATEGY_INCREMENTAL_DELAY_MAX_ATTEMPTS);
        final Duration maxDelay =
                configuration.get(CollectOptions.COLLECT_STRATEGY_INCREMENTAL_DELAY_MAX_DELAY);

        return new IncrementalDelayRetryStrategy(attempts, initialDelay, increment, maxDelay);
    }

    private static String createInvalidCollectStrategyErrorMessage(String configuredRetryStrategy) {
        final StringBuilder msgBuilder = new StringBuilder();
        msgBuilder
                .append("Unknown collect retry strategy '")
                .append(configuredRetryStrategy)
                .append("'. Valid strategies are ");

        final String validOptionsStr =
                String.join(
                        ", ",
                        CollectOptions.INCREMENTAL_DELAY_LABEL,
                        CollectOptions.EXPONENTIAL_DELAY_LABEL,
                        CollectOptions.FIXED_DELAY_LABEL);

        return msgBuilder.append(validOptionsStr).toString();
    }

    private static boolean isRetryStrategy(
            String retryStrategyLabel, String configuredRetryStrategy) {
        final String retryStrategy = configuredRetryStrategy.toLowerCase();
        return retryStrategy.equals(retryStrategyLabel)
                || retryStrategy.equals(
                        CollectOptions.extractAlphaNumericCharacters(retryStrategyLabel));
    }
}
