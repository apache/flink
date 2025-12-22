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

package org.apache.flink.streaming.util.retryable;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.streaming.api.functions.async.AsyncBatchRetryPredicate;
import org.apache.flink.streaming.api.functions.async.AsyncBatchRetryStrategy;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Utility class to create concrete {@link AsyncBatchRetryStrategy} implementations.
 *
 * <p>Provides commonly used retry strategies for batch async operations:
 *
 * <ul>
 *   <li>{@link FixedDelayRetryStrategy} - retries with a fixed delay between attempts
 *   <li>{@link ExponentialBackoffDelayRetryStrategy} - retries with exponentially increasing delays
 * </ul>
 *
 * <p><b>NOTICE:</b> For performance reasons, this utility's {@link AsyncBatchRetryStrategy}
 * implementation assumes the attempt always starts from 1 and will only increase by 1 each time.
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * // Fixed delay retry: max 3 attempts, 100ms between retries
 * AsyncBatchRetryStrategy<String> fixedDelay = new AsyncBatchRetryStrategies
 *     .FixedDelayRetryStrategyBuilder<String>(3, 100L)
 *     .ifException(e -> e instanceof IOException)
 *     .build();
 *
 * // Exponential backoff: max 5 attempts, initial 100ms, max 10s, multiplier 2.0
 * AsyncBatchRetryStrategy<String> exponential = new AsyncBatchRetryStrategies
 *     .ExponentialBackoffDelayRetryStrategyBuilder<String>(5, 100L, 10000L, 2.0)
 *     .ifException(e -> e instanceof TimeoutException)
 *     .build();
 * }</pre>
 */
@PublicEvolving
public class AsyncBatchRetryStrategies {

    /** A strategy that never retries. Use this as the default when no retry is needed. */
    @SuppressWarnings("rawtypes")
    public static final AsyncBatchRetryStrategy NO_RETRY_STRATEGY = new NoRetryStrategy();

    /**
     * Returns a type-safe no-retry strategy.
     *
     * @param <OUT> the output type
     * @return a strategy that never retries
     */
    @SuppressWarnings("unchecked")
    public static <OUT> AsyncBatchRetryStrategy<OUT> noRetry() {
        return (AsyncBatchRetryStrategy<OUT>) NO_RETRY_STRATEGY;
    }

    /** A strategy that never retries batch operations. */
    private static class NoRetryStrategy implements AsyncBatchRetryStrategy<Object> {
        private static final long serialVersionUID = 1L;

        private NoRetryStrategy() {}

        @Override
        public boolean canRetry(int currentAttempts) {
            return false;
        }

        @Override
        public long getBackoffTimeMillis(int currentAttempts) {
            return -1;
        }

        @Override
        public AsyncBatchRetryPredicate<Object> getRetryPredicate() {
            return new BatchRetryPredicate<>(null, null);
        }
    }

    /** Default implementation of {@link AsyncBatchRetryPredicate}. */
    private static class BatchRetryPredicate<OUT> implements AsyncBatchRetryPredicate<OUT> {
        private final Predicate<Collection<OUT>> resultPredicate;
        private final Predicate<Throwable> exceptionPredicate;

        public BatchRetryPredicate(
                Predicate<Collection<OUT>> resultPredicate,
                Predicate<Throwable> exceptionPredicate) {
            this.resultPredicate = resultPredicate;
            this.exceptionPredicate = exceptionPredicate;
        }

        @Override
        public Optional<Predicate<Collection<OUT>>> resultPredicate() {
            return Optional.ofNullable(resultPredicate);
        }

        @Override
        public Optional<Predicate<Throwable>> exceptionPredicate() {
            return Optional.ofNullable(exceptionPredicate);
        }
    }

    /**
     * A retry strategy that uses a fixed delay between retry attempts.
     *
     * @param <OUT> the type of output elements
     */
    public static class FixedDelayRetryStrategy<OUT> implements AsyncBatchRetryStrategy<OUT> {
        private static final long serialVersionUID = 1L;

        private final int maxAttempts;
        private final long backoffTimeMillis;
        private final Predicate<Collection<OUT>> resultPredicate;
        private final Predicate<Throwable> exceptionPredicate;

        private FixedDelayRetryStrategy(
                int maxAttempts,
                long backoffTimeMillis,
                Predicate<Collection<OUT>> resultPredicate,
                Predicate<Throwable> exceptionPredicate) {
            this.maxAttempts = maxAttempts;
            this.backoffTimeMillis = backoffTimeMillis;
            this.resultPredicate = resultPredicate;
            this.exceptionPredicate = exceptionPredicate;
        }

        @Override
        public boolean canRetry(int currentAttempts) {
            return currentAttempts <= maxAttempts;
        }

        @Override
        public long getBackoffTimeMillis(int currentAttempts) {
            return backoffTimeMillis;
        }

        @Override
        public AsyncBatchRetryPredicate<OUT> getRetryPredicate() {
            return new BatchRetryPredicate<>(resultPredicate, exceptionPredicate);
        }
    }

    /**
     * Builder for creating a {@link FixedDelayRetryStrategy}.
     *
     * @param <OUT> the type of output elements
     */
    public static class FixedDelayRetryStrategyBuilder<OUT> {
        private final int maxAttempts;
        private final long backoffTimeMillis;
        private Predicate<Collection<OUT>> resultPredicate;
        private Predicate<Throwable> exceptionPredicate;

        /**
         * Creates a builder with the specified retry parameters.
         *
         * @param maxAttempts maximum number of retry attempts (must be > 0)
         * @param backoffTimeMillis delay in milliseconds between retries (must be > 0)
         */
        public FixedDelayRetryStrategyBuilder(int maxAttempts, long backoffTimeMillis) {
            Preconditions.checkArgument(
                    maxAttempts > 0, "maxAttempts should be greater than zero.");
            Preconditions.checkArgument(
                    backoffTimeMillis > 0, "backoffTimeMillis should be greater than zero.");
            this.maxAttempts = maxAttempts;
            this.backoffTimeMillis = backoffTimeMillis;
        }

        /**
         * Sets the predicate to evaluate results and determine if a retry is needed.
         *
         * @param resultRetryPredicate predicate that returns true if retry should be triggered
         * @return this builder for method chaining
         */
        public FixedDelayRetryStrategyBuilder<OUT> ifResult(
                @Nonnull Predicate<Collection<OUT>> resultRetryPredicate) {
            this.resultPredicate = resultRetryPredicate;
            return this;
        }

        /**
         * Sets the predicate to evaluate exceptions and determine if a retry is needed.
         *
         * @param exceptionRetryPredicate predicate that returns true if retry should be triggered
         * @return this builder for method chaining
         */
        public FixedDelayRetryStrategyBuilder<OUT> ifException(
                @Nonnull Predicate<Throwable> exceptionRetryPredicate) {
            this.exceptionPredicate = exceptionRetryPredicate;
            return this;
        }

        /**
         * Builds the retry strategy.
         *
         * @return the configured retry strategy
         */
        public FixedDelayRetryStrategy<OUT> build() {
            return new FixedDelayRetryStrategy<>(
                    maxAttempts, backoffTimeMillis, resultPredicate, exceptionPredicate);
        }
    }

    /**
     * A retry strategy that uses exponentially increasing delays between retry attempts.
     *
     * <p>The delay for attempt N is: min(initialDelay * multiplier^(N-1), maxRetryDelay)
     *
     * @param <OUT> the type of output elements
     */
    public static class ExponentialBackoffDelayRetryStrategy<OUT>
            implements AsyncBatchRetryStrategy<OUT> {
        private static final long serialVersionUID = 1L;

        private final int maxAttempts;
        private final long maxRetryDelay;
        private final long initialDelay;
        private final double multiplier;
        private final Predicate<Collection<OUT>> resultPredicate;
        private final Predicate<Throwable> exceptionPredicate;

        // Note: This field is mutable for tracking retry state.
        // It's acceptable because each operator instance has its own strategy instance.
        private long lastRetryDelay;

        private ExponentialBackoffDelayRetryStrategy(
                int maxAttempts,
                long initialDelay,
                long maxRetryDelay,
                double multiplier,
                Predicate<Collection<OUT>> resultPredicate,
                Predicate<Throwable> exceptionPredicate) {
            this.maxAttempts = maxAttempts;
            this.maxRetryDelay = maxRetryDelay;
            this.multiplier = multiplier;
            this.resultPredicate = resultPredicate;
            this.exceptionPredicate = exceptionPredicate;
            this.initialDelay = initialDelay;
            this.lastRetryDelay = initialDelay;
        }

        @Override
        public boolean canRetry(int currentAttempts) {
            return currentAttempts <= maxAttempts;
        }

        @Override
        public long getBackoffTimeMillis(int currentAttempts) {
            if (currentAttempts <= 1) {
                // Reset to initialDelay for first attempt
                this.lastRetryDelay = initialDelay;
                return lastRetryDelay;
            }

            long backoff = Math.min((long) (lastRetryDelay * multiplier), maxRetryDelay);
            this.lastRetryDelay = backoff;
            return backoff;
        }

        @Override
        public AsyncBatchRetryPredicate<OUT> getRetryPredicate() {
            return new BatchRetryPredicate<>(resultPredicate, exceptionPredicate);
        }
    }

    /**
     * Builder for creating an {@link ExponentialBackoffDelayRetryStrategy}.
     *
     * @param <OUT> the type of output elements
     */
    public static class ExponentialBackoffDelayRetryStrategyBuilder<OUT> {
        private final int maxAttempts;
        private final long initialDelay;
        private final long maxRetryDelay;
        private final double multiplier;

        private Predicate<Collection<OUT>> resultPredicate;
        private Predicate<Throwable> exceptionPredicate;

        /**
         * Creates a builder with the specified exponential backoff parameters.
         *
         * @param maxAttempts maximum number of retry attempts (must be > 0)
         * @param initialDelay initial delay in milliseconds (must be > 0)
         * @param maxRetryDelay maximum delay in milliseconds (must be >= initialDelay)
         * @param multiplier multiplier for delay increase (must be >= 1.0)
         */
        public ExponentialBackoffDelayRetryStrategyBuilder(
                int maxAttempts, long initialDelay, long maxRetryDelay, double multiplier) {
            Preconditions.checkArgument(
                    maxAttempts > 0, "maxAttempts should be greater than zero.");
            Preconditions.checkArgument(
                    initialDelay > 0, "initialDelay should be greater than zero.");
            Preconditions.checkArgument(
                    maxRetryDelay >= initialDelay,
                    "maxRetryDelay should be greater than or equal to initialDelay.");
            Preconditions.checkArgument(
                    multiplier >= 1.0, "multiplier should be greater than or equal to 1.0.");
            this.maxAttempts = maxAttempts;
            this.initialDelay = initialDelay;
            this.maxRetryDelay = maxRetryDelay;
            this.multiplier = multiplier;
        }

        /**
         * Sets the predicate to evaluate results and determine if a retry is needed.
         *
         * @param resultRetryPredicate predicate that returns true if retry should be triggered
         * @return this builder for method chaining
         */
        public ExponentialBackoffDelayRetryStrategyBuilder<OUT> ifResult(
                @Nonnull Predicate<Collection<OUT>> resultRetryPredicate) {
            this.resultPredicate = resultRetryPredicate;
            return this;
        }

        /**
         * Sets the predicate to evaluate exceptions and determine if a retry is needed.
         *
         * @param exceptionRetryPredicate predicate that returns true if retry should be triggered
         * @return this builder for method chaining
         */
        public ExponentialBackoffDelayRetryStrategyBuilder<OUT> ifException(
                @Nonnull Predicate<Throwable> exceptionRetryPredicate) {
            this.exceptionPredicate = exceptionRetryPredicate;
            return this;
        }

        /**
         * Builds the retry strategy.
         *
         * @return the configured retry strategy
         */
        public ExponentialBackoffDelayRetryStrategy<OUT> build() {
            return new ExponentialBackoffDelayRetryStrategy<>(
                    maxAttempts,
                    initialDelay,
                    maxRetryDelay,
                    multiplier,
                    resultPredicate,
                    exceptionPredicate);
        }
    }
}
