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

import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.ExponentialBackoffRetryStrategy;
import org.apache.flink.util.concurrent.RetryStrategy;

import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;

/** Tests for {@link ExponentialBackoffRetryStrategy}. */
public class ExponentialBackoffRetryStrategyTest extends TestLogger {

    @Test
    public void testGettersNotCapped() throws Exception {
        RetryStrategy retryStrategy =
                new ExponentialBackoffRetryStrategy(
                        10, Duration.ofMillis(5L), Duration.ofMillis(20L));
        assertEquals(10, retryStrategy.getNumRemainingRetries());
        assertEquals(Duration.ofMillis(5L), retryStrategy.getRetryDelay());

        RetryStrategy nextRetryStrategy = retryStrategy.getNextRetryStrategy();
        assertEquals(9, nextRetryStrategy.getNumRemainingRetries());
        assertEquals(Duration.ofMillis(10L), nextRetryStrategy.getRetryDelay());
    }

    @Test
    public void testGettersHitCapped() throws Exception {
        RetryStrategy retryStrategy =
                new ExponentialBackoffRetryStrategy(
                        5, Duration.ofMillis(15L), Duration.ofMillis(20L));
        assertEquals(5, retryStrategy.getNumRemainingRetries());
        assertEquals(Duration.ofMillis(15L), retryStrategy.getRetryDelay());

        RetryStrategy nextRetryStrategy = retryStrategy.getNextRetryStrategy();
        assertEquals(4, nextRetryStrategy.getNumRemainingRetries());
        assertEquals(Duration.ofMillis(20L), nextRetryStrategy.getRetryDelay());
    }

    @Test
    public void testGettersAtCap() throws Exception {
        RetryStrategy retryStrategy =
                new ExponentialBackoffRetryStrategy(
                        5, Duration.ofMillis(20L), Duration.ofMillis(20L));
        assertEquals(5, retryStrategy.getNumRemainingRetries());
        assertEquals(Duration.ofMillis(20L), retryStrategy.getRetryDelay());

        RetryStrategy nextRetryStrategy = retryStrategy.getNextRetryStrategy();
        assertEquals(4, nextRetryStrategy.getNumRemainingRetries());
        assertEquals(Duration.ofMillis(20L), nextRetryStrategy.getRetryDelay());
    }

    /** Tests that getting a next RetryStrategy below zero remaining retries fails. */
    @Test(expected = IllegalStateException.class)
    public void testRetryFailure() throws Throwable {
        new ExponentialBackoffRetryStrategy(0, Duration.ofMillis(20L), Duration.ofMillis(20L))
                .getNextRetryStrategy();
    }
}
