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

package org.apache.flink.api.connector.source.util.ratelimit;

import org.apache.flink.api.connector.source.mocks.MockSourceSplit;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link RateLimiterStrategy#dynamic(double)}. */
class RateLimiterStrategyTest {

    @Test
    void testDynamicCreatesADynamicRateLimiter() {
        RateLimiterStrategy strategy = RateLimiterStrategy.dynamic(100.0);

        RateLimiter<?> limiter = strategy.createRateLimiter(1);

        assertThat(limiter).isInstanceOf(DynamicRateLimiter.class);
    }

    @Test
    void testDynamicDividesTheRateAcrossParallelism() throws Exception {
        RateLimiterStrategy strategy = RateLimiterStrategy.dynamic(10.0);

        @SuppressWarnings("unchecked")
        RateLimiter<MockSourceSplit> highParallelism =
                (RateLimiter<MockSourceSplit>) strategy.createRateLimiter(10); // 1.0/s per subtask
        @SuppressWarnings("unchecked")
        RateLimiter<MockSourceSplit> lowParallelism =
                (RateLimiter<MockSourceSplit>) strategy.createRateLimiter(1); // 10.0/s per subtask

        // Consume each limiter's initial free permit so the next acquire is actually throttled.
        highParallelism.acquire().toCompletableFuture().get(5, TimeUnit.SECONDS);
        lowParallelism.acquire().toCompletableFuture().get(5, TimeUnit.SECONDS);

        long highParallelismElapsedNanos = timeToAcquireOnePermit(highParallelism);
        long lowParallelismElapsedNanos = timeToAcquireOnePermit(lowParallelism);

        // At 1 permit/s (parallelism 10) the next permit takes ~1s; at 10 permits/s (parallelism
        // 1) it takes ~0.1s, so the per-subtask rate is clearly lower under higher parallelism.
        assertThat(highParallelismElapsedNanos).isGreaterThan(lowParallelismElapsedNanos);
    }

    private long timeToAcquireOnePermit(RateLimiter<MockSourceSplit> limiter) throws Exception {
        long start = System.nanoTime();
        limiter.acquire().toCompletableFuture().get(5, TimeUnit.SECONDS);
        return System.nanoTime() - start;
    }
}
