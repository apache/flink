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

package org.apache.flink.api.connector.source.util.ratelimit;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.util.ExecutorUtils;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * {@code PerSecondRateLimiterStrategy} provides {@link RateLimiter} instances when a certain amount
 * of records has been processed per second.
 */
@Experimental
public class PerSecondRateLimiterStrategy implements RateLimiterStrategy, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(PerSecondRateLimiterStrategy.class);

    private final double recordsPerSecond;

    private final ExecutorService executorService;
    private final Duration closeTimeout;

    /**
     * Creates a new {@code PerSecondRateLimiterStrategy} with external {@link Executor}.
     *
     * @param recordsPerSeconds The amount of records that shall be processed per second.
     * @param externalExecutor An external {@code Executor} that's used for asynchronously waiting
     *     for the trigger.
     */
    public static RateLimiterStrategy createWithExternalExecutor(
            double recordsPerSeconds, Executor externalExecutor) {
        return parallelism ->
                new PerSecondRateLimiter(
                        recordsPerSecondsWithParallelism(recordsPerSeconds, parallelism),
                        externalExecutor);
    }

    /**
     * Creates a new {@code PerSecondRateLimiterStrategy}.
     *
     * @param recordsPerSeconds The amount of records that shall be processed per second.
     */
    public static PerSecondRateLimiterStrategy create(double recordsPerSeconds) {
        return create(recordsPerSeconds, Duration.ofSeconds(10));
    }

    /**
     * Creates a new {@code PerSecondRateLimiterStrategy}.
     *
     * @param recordsPerSeconds The amount of records that shall be processed per second.
     * @param closeTimeout The timeout for closing the {@code PerSecondRateLimiterStrategy}.
     */
    public static PerSecondRateLimiterStrategy create(
            double recordsPerSeconds, Duration closeTimeout) {
        return create(recordsPerSeconds, "flink-per-second-rate-limiter", closeTimeout);
    }

    /**
     * Creates a new {@code PerSecondRateLimiterStrategy}.
     *
     * @param recordsPerSeconds The amount of records that shall be processed per second.
     * @param threadNamePrefix The name prefix of the thread that's used for
     * @param closeTimeout The timeout for closing the {@code PerSecondRateLimiterStrategy}.
     */
    public static PerSecondRateLimiterStrategy create(
            double recordsPerSeconds, String threadNamePrefix, Duration closeTimeout) {
        return new PerSecondRateLimiterStrategy(
                recordsPerSeconds,
                Executors.newSingleThreadExecutor(new ExecutorThreadFactory(threadNamePrefix)),
                closeTimeout);
    }

    private PerSecondRateLimiterStrategy(
            double recordsPerSecond, ExecutorService executorService, Duration closeTimeout) {
        this.recordsPerSecond = recordsPerSecond;
        this.executorService = executorService;
        this.closeTimeout = closeTimeout;
    }

    @Override
    public RateLimiter createRateLimiter(int parallelism) {
        return new PerSecondRateLimiter(
                recordsPerSecondsWithParallelism(this.recordsPerSecond, parallelism),
                executorService);
    }

    private static double recordsPerSecondsWithParallelism(
            double totalRecordsPerSecond, int parallelism) {
        return totalRecordsPerSecond / parallelism;
    }

    @Override
    public void close() throws Exception {
        final List<Runnable> outstandingTasks =
                ExecutorUtils.gracefulShutdown(
                        closeTimeout.toMillis(), TimeUnit.MILLISECONDS, executorService);

        if (!outstandingTasks.isEmpty()) {
            LOG.warn(
                    "Shutting down the {} left {} task(s) unfinished.",
                    PerSecondRateLimiterStrategy.class.getSimpleName(),
                    outstandingTasks.size());
        }
    }

    private static class PerSecondRateLimiter implements RateLimiter {

        private final org.apache.flink.shaded.guava32.com.google.common.util.concurrent.RateLimiter
                internalRateLimiter;
        private final Executor executor;

        private PerSecondRateLimiter(double maxPerSecond, Executor executor) {
            this.internalRateLimiter =
                    org.apache.flink.shaded.guava32.com.google.common.util.concurrent.RateLimiter
                            .create(maxPerSecond);
            this.executor = executor;
        }

        @Override
        public CompletionStage<Void> acquire() {
            return CompletableFuture.runAsync(internalRateLimiter::acquire, executor);
        }
    }
}
