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

package org.apache.flink.runtime.executiongraph.failover.flip1;

import org.apache.flink.util.clock.ManualClock;

import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/** Unit tests for {@link FailureRateRestartBackoffTimeStrategy}. */
class FailureRateRestartBackoffTimeStrategyTest {

    private final Exception failure = new Exception();

    @Test
    void testManyFailuresWithinRate() {
        final int numFailures = 3;
        final long intervalMS = 1L;

        ManualClock clock = new ManualClock();

        final FailureRateRestartBackoffTimeStrategy restartStrategy =
                new FailureRateRestartBackoffTimeStrategy(clock, 1, intervalMS, 0);

        for (int failuresLeft = numFailures; failuresLeft > 0; failuresLeft--) {
            restartStrategy.notifyFailure(failure);
            assertThat(restartStrategy.canRestart()).isTrue();
            clock.advanceTime(intervalMS + 1, TimeUnit.MILLISECONDS);
        }

        assertThat(restartStrategy.canRestart()).isTrue();
    }

    @Test
    void testFailuresExceedingRate() {
        final int numFailures = 3;
        final long intervalMS = 10_000L;

        final FailureRateRestartBackoffTimeStrategy restartStrategy =
                new FailureRateRestartBackoffTimeStrategy(
                        new ManualClock(), numFailures, intervalMS, 0);

        for (int failuresLeft = numFailures; failuresLeft > 0; failuresLeft--) {
            restartStrategy.notifyFailure(failure);
            assertThat(restartStrategy.canRestart()).isTrue();
        }

        restartStrategy.notifyFailure(failure);
        assertThat(restartStrategy.canRestart()).isFalse();
    }

    @Test
    void testBackoffTime() {
        final long backoffTimeMS = 10_000L;

        final FailureRateRestartBackoffTimeStrategy restartStrategy =
                new FailureRateRestartBackoffTimeStrategy(new ManualClock(), 1, 1, backoffTimeMS);

        assertThat(restartStrategy.getBackoffTime()).isEqualTo(backoffTimeMS);
    }
}
