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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link ExponentialWaitStrategy}. */
class ExponentialWaitStrategyTest {

    @Test
    void testNegativeInitialWait() {
        assertThatThrownBy(() -> new ExponentialWaitStrategy(0, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialWait must be positive");
    }

    @Test
    void testNegativeMaxWait() {
        assertThatThrownBy(() -> new ExponentialWaitStrategy(1, -1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("maxWait must be positive");
    }

    @Test
    void testInitialWaitGreaterThanMaxWait() {
        assertThatThrownBy(() -> new ExponentialWaitStrategy(2, 1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("initialWait must be lower than or equal to maxWait");
    }

    @Test
    void testMaxSleepTime() {
        final long sleepTime = new ExponentialWaitStrategy(1, 1).sleepTime(100);
        assertThat(sleepTime).isEqualTo(1L);
    }

    @Test
    void testExponentialGrowth() {
        final ExponentialWaitStrategy exponentialWaitStrategy =
                new ExponentialWaitStrategy(1, 1000);
        assertThat(exponentialWaitStrategy.sleepTime(3) / exponentialWaitStrategy.sleepTime(2))
                .isEqualTo(2L);
    }

    @Test
    void testMaxAttempts() {
        final long maxWait = 1000;
        final ExponentialWaitStrategy exponentialWaitStrategy =
                new ExponentialWaitStrategy(1, maxWait);
        assertThat(exponentialWaitStrategy.sleepTime(Long.MAX_VALUE)).isEqualTo(maxWait);
    }

    @Test
    void test64Attempts() {
        final long maxWait = 1000;
        final ExponentialWaitStrategy exponentialWaitStrategy =
                new ExponentialWaitStrategy(1, maxWait);
        assertThat(exponentialWaitStrategy.sleepTime(64)).isEqualTo(maxWait);
    }
}
