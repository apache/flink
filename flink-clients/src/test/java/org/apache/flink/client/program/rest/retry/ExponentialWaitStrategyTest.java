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

package org.apache.flink.client.program.rest.retry;

import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/** Tests for {@link ExponentialWaitStrategy}. */
public class ExponentialWaitStrategyTest extends TestLogger {

    @Test
    public void testNegativeInitialWait() {
        try {
            new ExponentialWaitStrategy(0, 1);
            fail("Expected exception not thrown.");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("initialWait must be positive"));
        }
    }

    @Test
    public void testNegativeMaxWait() {
        try {
            new ExponentialWaitStrategy(1, -1);
            fail("Expected exception not thrown.");
        } catch (final IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("maxWait must be positive"));
        }
    }

    @Test
    public void testInitialWaitGreaterThanMaxWait() {
        try {
            new ExponentialWaitStrategy(2, 1);
            fail("Expected exception not thrown.");
        } catch (final IllegalArgumentException e) {
            assertThat(
                    e.getMessage(),
                    containsString("initialWait must be lower than or equal to maxWait"));
        }
    }

    @Test
    public void testMaxSleepTime() {
        final long sleepTime = new ExponentialWaitStrategy(1, 1).sleepTime(100);
        assertThat(sleepTime, equalTo(1L));
    }

    @Test
    public void testExponentialGrowth() {
        final ExponentialWaitStrategy exponentialWaitStrategy =
                new ExponentialWaitStrategy(1, 1000);
        assertThat(
                exponentialWaitStrategy.sleepTime(3) / exponentialWaitStrategy.sleepTime(2),
                equalTo(2L));
    }

    @Test
    public void testMaxAttempts() {
        final long maxWait = 1000;
        final ExponentialWaitStrategy exponentialWaitStrategy =
                new ExponentialWaitStrategy(1, maxWait);
        assertThat(exponentialWaitStrategy.sleepTime(Long.MAX_VALUE), equalTo(maxWait));
    }

    @Test
    public void test64Attempts() {
        final long maxWait = 1000;
        final ExponentialWaitStrategy exponentialWaitStrategy =
                new ExponentialWaitStrategy(1, maxWait);
        assertThat(exponentialWaitStrategy.sleepTime(64), equalTo(maxWait));
    }
}
