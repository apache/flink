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

import org.apache.flink.streaming.api.functions.async.AsyncRetryPredicate;
import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.Optional;
import java.util.function.Predicate;

/** Utility class to create concrete {@link AsyncRetryStrategy}. */
public class AsyncRetryStrategies {
    public static final NoRetryStrategy NO_RETRY_STRATEGY = new NoRetryStrategy();

    /** NoRetryStrategy. */
    private static class NoRetryStrategy implements AsyncRetryStrategy {
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
        public AsyncRetryPredicate getRetryPredicate() {
            return new RetryPredicate(null, null);
        }
    }

    private static class RetryPredicate<OUT> implements AsyncRetryPredicate<OUT> {
        final Predicate<Collection<OUT>> resultPredicate;
        final Predicate<Throwable> exceptionPredicate;

        public RetryPredicate(
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

    /** FixedDelayRetryStrategy. */
    public static class FixedDelayRetryStrategy<OUT> implements AsyncRetryStrategy<OUT> {
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
        public AsyncRetryPredicate<OUT> getRetryPredicate() {
            return new RetryPredicate(resultPredicate, exceptionPredicate);
        }

        @Override
        public long getBackoffTimeMillis(int currentAttempts) {
            return backoffTimeMillis;
        }
    }

    /** FixedDelayRetryStrategyBuilder for building a FixedDelayRetryStrategy. */
    public static class FixedDelayRetryStrategyBuilder<OUT> {
        private int maxAttempts;
        private long backoffTimeMillis;
        private Predicate<Collection<OUT>> resultPredicate;
        private Predicate<Throwable> exceptionPredicate;

        public FixedDelayRetryStrategyBuilder(int maxAttempts, long backoffTimeMillis) {
            Preconditions.checkArgument(
                    maxAttempts > 0, "maxAttempts should be greater than zero.");
            Preconditions.checkArgument(
                    backoffTimeMillis > 0, "backoffTimeMillis should be greater than zero.");
            this.maxAttempts = maxAttempts;
            this.backoffTimeMillis = backoffTimeMillis;
        }

        public FixedDelayRetryStrategyBuilder<OUT> ifResult(
                @Nonnull Predicate<Collection<OUT>> resultRetryPredicate) {
            this.resultPredicate = resultRetryPredicate;
            return this;
        }

        public FixedDelayRetryStrategyBuilder<OUT> ifException(
                @Nonnull Predicate<Throwable> exceptionRetryPredicate) {
            this.exceptionPredicate = exceptionRetryPredicate;
            return this;
        }

        public FixedDelayRetryStrategy<OUT> build() {
            return new FixedDelayRetryStrategy<OUT>(
                    maxAttempts, backoffTimeMillis, resultPredicate, exceptionPredicate);
        }
    }

    /** ExponentialBackoffDelayRetryStrategy. */
    public static class ExponentialBackoffDelayRetryStrategy<OUT>
            implements AsyncRetryStrategy<OUT> {
        private static final long serialVersionUID = 1L;
        private final int maxAttempts;
        private final long maxRetryDelay;
        private final double multiplier;
        private final Predicate<Collection<OUT>> resultPredicate;
        private final Predicate<Throwable> exceptionPredicate;

        private long lastRetryDelay;

        public ExponentialBackoffDelayRetryStrategy(
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
            this.lastRetryDelay = initialDelay;
        }

        @Override
        public boolean canRetry(int currentAttempts) {
            return currentAttempts <= maxAttempts;
        }

        @Override
        public long getBackoffTimeMillis(int currentAttempts) {
            if (currentAttempts <= 1) {
                // equivalent to initial delay
                return lastRetryDelay;
            }
            long backoff = Math.min((long) (lastRetryDelay * multiplier), maxRetryDelay);
            this.lastRetryDelay = backoff;
            return backoff;
        }

        @Override
        public AsyncRetryPredicate<OUT> getRetryPredicate() {
            return new RetryPredicate<OUT>(resultPredicate, exceptionPredicate);
        }
    }

    /**
     * ExponentialBackoffDelayRetryStrategyBuilder for building a
     * ExponentialBackoffDelayRetryStrategy.
     */
    public static class ExponentialBackoffDelayRetryStrategyBuilder<OUT> {
        private final int maxAttempts;
        private final long initialDelay;
        private final long maxRetryDelay;
        private final double multiplier;

        private Predicate<Collection<OUT>> resultPredicate;
        private Predicate<Throwable> exceptionPredicate;

        public ExponentialBackoffDelayRetryStrategyBuilder(
                int maxAttempts, long initialDelay, long maxRetryDelay, double multiplier) {
            this.maxAttempts = maxAttempts;
            this.initialDelay = initialDelay;
            this.maxRetryDelay = maxRetryDelay;
            this.multiplier = multiplier;
        }

        public ExponentialBackoffDelayRetryStrategyBuilder<OUT> ifResult(
                @Nonnull Predicate<Collection<OUT>> resultRetryPredicate) {
            this.resultPredicate = resultRetryPredicate;
            return this;
        }

        public ExponentialBackoffDelayRetryStrategyBuilder<OUT> ifException(
                @Nonnull Predicate<Throwable> exceptionRetryPredicate) {
            this.exceptionPredicate = exceptionRetryPredicate;
            return this;
        }

        public ExponentialBackoffDelayRetryStrategy<OUT> build() {
            return new ExponentialBackoffDelayRetryStrategy<OUT>(
                    maxAttempts,
                    initialDelay,
                    maxRetryDelay,
                    multiplier,
                    resultPredicate,
                    exceptionPredicate);
        }
    }
}
