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

package org.apache.flink.api.connector.source.lib.util;

import org.apache.flink.api.connector.source.lib.NumberSequenceSource.NumberSequenceSplit;
import org.apache.flink.api.connector.source.util.ratelimit.GatedRateLimiter;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class GatedRateLimiterTest {

    @Test
    void testCapacityNotExceededOnCheckpoint() {
        int capacityPerCycle = 5;

        final GatedRateLimiter<NumberSequenceSplit> gatedRateLimiter =
                new GatedRateLimiter<>(capacityPerCycle);
        for (int x = 0; x < capacityPerCycle; x++) {
            assertThat(gatedRateLimiter.acquire()).isCompleted();
        }

        CompletionStage<Void> postInitialBatch = gatedRateLimiter.acquire();
        assertThat(postInitialBatch).isNotCompleted();

        gatedRateLimiter.notifyCheckpointComplete(0);

        assertThat(postInitialBatch).isCompleted();
        for (int x = 0; x < capacityPerCycle - 1; x++) {
            assertThat(gatedRateLimiter.acquire()).isCompleted();
        }

        CompletionStage<Void> postCheckpoint = gatedRateLimiter.acquire();
        assertThat(postCheckpoint).isNotCompleted();
    }

    @Test
    void testCapacityNotExceededWhenAcquiringMultipleEvents() {
        int capacityPerCycle = 5;

        final GatedRateLimiter<NumberSequenceSplit> gatedRateLimiter =
                new GatedRateLimiter<>(capacityPerCycle);
        assertThat(gatedRateLimiter.acquire(3)).isCompleted();

        // Only two permits are left in this cycle, so a request for three events has to wait even
        // though the remaining capacity is still greater than zero.
        CompletionStage<Void> exceedsRemainingCapacity = gatedRateLimiter.acquire(3);
        assertThat(exceedsRemainingCapacity).isNotCompleted();

        gatedRateLimiter.notifyCheckpointComplete(0);

        assertThat(exceedsRemainingCapacity).isCompleted();
    }

    @Test
    void testRequestLargerThanCapacityIsReleasedByNextCycle() {
        final GatedRateLimiter<NumberSequenceSplit> gatedRateLimiter = new GatedRateLimiter<>(2);

        // A single request may legitimately exceed the capacity of an entire cycle. It must not
        // deadlock: resetting the capacity on the next completed checkpoint releases it.
        CompletionStage<Void> exceedsWholeCycle = gatedRateLimiter.acquire(3);
        assertThat(exceedsWholeCycle).isNotCompleted();

        gatedRateLimiter.notifyCheckpointComplete(0);

        assertThat(exceedsWholeCycle).isCompleted();

        // Because completing it took 3 events from a cycle that only had 2, a further checkpoint is
        // needed before requests are allowed again.
        CompletionStage<Void> followingRequest = gatedRateLimiter.acquire(1);
        assertThat(followingRequest).isNotCompleted();

        gatedRateLimiter.notifyCheckpointComplete(1);

        assertThat(followingRequest).isCompleted();
        assertThat(gatedRateLimiter.acquire(1)).isCompleted();
    }

    @Test
    void testCheckpointCompleteBeforeFirstAcquire() {
        int capacityPerCycle = 5;

        final GatedRateLimiter<NumberSequenceSplit> gatedRateLimiter =
                new GatedRateLimiter<>(capacityPerCycle);

        // A checkpoint can complete before the reader has emitted anything, for instance while it
        // is still waiting for its first split assignment.
        gatedRateLimiter.notifyCheckpointComplete(0);

        for (int x = 0; x < capacityPerCycle; x++) {
            assertThat(gatedRateLimiter.acquire()).isCompleted();
        }
        assertThat(gatedRateLimiter.acquire()).isNotCompleted();
    }

    @Test
    void testNonPositiveNumberOfEventsIsRejected() {
        final GatedRateLimiter<NumberSequenceSplit> gatedRateLimiter = new GatedRateLimiter<>(5);

        assertThatThrownBy(() -> gatedRateLimiter.acquire(0))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
        assertThatThrownBy(() -> gatedRateLimiter.acquire(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("positive");
    }
}
