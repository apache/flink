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

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link GuavaDynamicRateLimiter}. */
class GuavaDynamicRateLimiterTest {

    @Test
    void testInitialAcquireCompletesImmediately() throws Exception {
        GuavaDynamicRateLimiter<MockSourceSplit> limiter = new GuavaDynamicRateLimiter<>(1.0);

        // The token bucket starts full, so the very first permit is granted without delay.
        limiter.acquire().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    @Test
    void testUpdateRateRelaxesAFormerlyLowRate() throws Exception {
        GuavaDynamicRateLimiter<MockSourceSplit> limiter = new GuavaDynamicRateLimiter<>(1.0);
        // Consume the initial free permit so the next acquire would otherwise be throttled at the
        // original, very low rate.
        limiter.acquire().toCompletableFuture().get(5, TimeUnit.SECONDS);

        limiter.updateRate(1_000_000.0);
        // Guava keeps the permit already reserved at the old rate, so the first acquire after the
        // update still waits out that reservation. Absorb it before measuring.
        limiter.acquire().toCompletableFuture().get(5, TimeUnit.SECONDS);

        long start = System.nanoTime();
        for (int i = 0; i < 5; i++) {
            limiter.acquire().toCompletableFuture().get(5, TimeUnit.SECONDS);
        }
        long elapsedMillis = Duration.ofNanos(System.nanoTime() - start).toMillis();

        // At the old rate (1 permit/s) this would have taken several seconds; at the updated,
        // effectively unlimited rate it should complete almost instantly.
        assertThat(elapsedMillis).isLessThan(1000);
    }
}
