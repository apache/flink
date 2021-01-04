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

package org.apache.flink.runtime.concurrent;

import org.apache.flink.util.Preconditions;

import java.time.Duration;

/**
 * An implementation of {@link RetryStrategy} that retries that has an exponential backoff with a
 * cap.
 */
public class ExponentialBackoffRetryStrategy implements RetryStrategy {
    private final int remainingRetries;
    private final Duration currentRetryDelay;
    private final Duration maxRetryDelay;

    /**
     * @param remainingRetries number of times to retry
     * @param currentRetryDelay the current delay between retries
     * @param maxRetryDelay the max delay between retries
     */
    public ExponentialBackoffRetryStrategy(
            int remainingRetries, Duration currentRetryDelay, Duration maxRetryDelay) {
        Preconditions.checkArgument(
                remainingRetries >= 0, "The number of retries must be greater or equal to 0.");
        this.remainingRetries = remainingRetries;
        Preconditions.checkArgument(
                currentRetryDelay.toMillis() >= 0, "The currentRetryDelay must be positive");
        this.currentRetryDelay = currentRetryDelay;
        Preconditions.checkArgument(
                maxRetryDelay.toMillis() >= 0, "The maxRetryDelay must be positive");
        this.maxRetryDelay = maxRetryDelay;
    }

    @Override
    public int getNumRemainingRetries() {
        return remainingRetries;
    }

    @Override
    public Duration getRetryDelay() {
        return currentRetryDelay;
    }

    @Override
    public RetryStrategy getNextRetryStrategy() {
        int nextRemainingRetries = remainingRetries - 1;
        Preconditions.checkState(
                nextRemainingRetries >= 0, "The number of remaining retries must not be negative");
        long nextRetryDelayMillis =
                Math.min(2 * currentRetryDelay.toMillis(), maxRetryDelay.toMillis());
        return new ExponentialBackoffRetryStrategy(
                nextRemainingRetries, Duration.ofMillis(nextRetryDelayMillis), maxRetryDelay);
    }
}
