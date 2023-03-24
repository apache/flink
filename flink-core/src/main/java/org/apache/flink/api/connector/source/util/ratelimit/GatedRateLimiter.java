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

import org.apache.flink.annotation.Internal;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * An implementation of {@link RateLimiter} that completes defined number of futures in-between the
 * external notification events. The first cycle completes immediately, without waiting for the
 * external notifications.
 */
@Internal
public class GatedRateLimiter implements RateLimiter {

    private final int capacityPerCycle;
    private int capacityLeft;

    /**
     * Instantiates a new GatedRateLimiter.
     *
     * @param capacityPerCycle The number of completed futures per cycle.
     */
    public GatedRateLimiter(int capacityPerCycle) {
        checkArgument(capacityPerCycle > 0, "Capacity per cycle has to be a positive number.");
        this.capacityPerCycle = capacityPerCycle;
        this.capacityLeft = capacityPerCycle;
    }

    transient CompletableFuture<Void> gatingFuture = null;

    @Override
    public CompletionStage<Void> acquire() {
        if (gatingFuture == null) {
            gatingFuture = CompletableFuture.completedFuture(null);
        }
        if (capacityLeft <= 0) {
            gatingFuture = new CompletableFuture<>();
        }
        return gatingFuture.thenRun(() -> capacityLeft -= 1);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        capacityLeft = capacityPerCycle;
        gatingFuture.complete(null);
    }
}
