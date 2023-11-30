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

import org.apache.flink.streaming.api.functions.async.AsyncRetryStrategy;
import org.apache.flink.util.TestLogger;

import org.junit.Assert;
import org.junit.Test;

/** Tests for the {@link AsyncRetryStrategies}. */
public class AsyncRetryStrategiesTest extends TestLogger {

    @Test
    public void testExponentialBackoffDelayRetryStrategy() {
        int maxAttempts = 10;
        long initialDelay = 100L;
        long maxRetryDelay = 2000L;
        double multiplier = 2;

        AsyncRetryStrategy<Void> exponentialBackoffDelayRetryStrategy =
                new AsyncRetryStrategies.ExponentialBackoffDelayRetryStrategyBuilder<Void>(
                                maxAttempts, initialDelay, maxRetryDelay, multiplier)
                        .build();

        Assert.assertTrue(exponentialBackoffDelayRetryStrategy.canRetry(maxAttempts));
        Assert.assertFalse(exponentialBackoffDelayRetryStrategy.canRetry(maxAttempts + 1));

        // test if this strategy can be reused.
        for (int j = 1; j <= 5; j++) {
            long currentDelay = initialDelay;

            for (int i = 1; i <= maxAttempts; i++) {
                Assert.assertEquals(
                        currentDelay, exponentialBackoffDelayRetryStrategy.getBackoffTimeMillis(i));
                currentDelay = Math.min((long) (currentDelay * multiplier), maxRetryDelay);
            }
        }
    }
}
