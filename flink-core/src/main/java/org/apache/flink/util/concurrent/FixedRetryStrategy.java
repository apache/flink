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

package org.apache.flink.util.concurrent;

import org.apache.flink.util.Preconditions;

import java.time.Duration;

/** An implementation of {@link RetryStrategy} that retries at a fixed delay. */
public class FixedRetryStrategy implements RetryStrategy {
    private final int remainingRetries;
    private final Duration retryDelay;

    /**
     * @param remainingRetries number of times to retry
     * @param retryDelay delay between retries
     */
    public FixedRetryStrategy(int remainingRetries, Duration retryDelay) {
        Preconditions.checkArgument(
                remainingRetries >= 0, "The number of retries must be greater or equal to 0.");
        this.remainingRetries = remainingRetries;
        Preconditions.checkArgument(retryDelay.toMillis() >= 0, "The retryDelay must be positive");
        this.retryDelay = retryDelay;
    }

    @Override
    public int getNumRemainingRetries() {
        return remainingRetries;
    }

    @Override
    public Duration getRetryDelay() {
        return retryDelay;
    }

    @Override
    public RetryStrategy getNextRetryStrategy() {
        int nextRemainingRetries = remainingRetries - 1;
        Preconditions.checkState(
                nextRemainingRetries >= 0, "The number of remaining retries must not be negative");
        return new FixedRetryStrategy(nextRemainingRetries, retryDelay);
    }
}
