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

import org.apache.flink.api.connector.source.util.ratelimit.GatedRateLimiter;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionStage;

import static org.assertj.core.api.Assertions.assertThat;

class GatedRateLimiterTest {

    @Test
    void testCapacityNotExceededOnCheckpoint() {
        int capacityPerCycle = 5;

        final GatedRateLimiter gatedRateLimiter = new GatedRateLimiter(capacityPerCycle);
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
}
