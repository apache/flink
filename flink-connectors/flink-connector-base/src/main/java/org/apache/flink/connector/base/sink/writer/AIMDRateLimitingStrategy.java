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

package org.apache.flink.connector.base.sink.writer;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

/**
 * Additive Increase/Multiplicative Decrease implementation of {@link RateLimitingStrategy}. This
 * implementation is not thread safe.
 */
@PublicEvolving
public final class AIMDRateLimitingStrategy implements RateLimitingStrategy {
    private final int increaseRate;
    private final double decreaseFactor;
    private final int rateThreshold;

    private int inFlightMessages;

    /**
     * @param increaseRate Linear increase value of rate limit on each acknowledgement.
     * @param decreaseFactor Exponential decrease factor of rate limit on each failure.
     * @param rateThreshold Threshold for maximum value of rate limit, this can be enforced due to
     *     writer or destination specific limits.
     * @param initialRate Initial rate limit to start with.
     */
    public AIMDRateLimitingStrategy(
            int increaseRate, double decreaseFactor, int rateThreshold, int initialRate) {
        Preconditions.checkArgument(
                decreaseFactor < 1.0 && decreaseFactor > 0.0,
                "Decrease factor must be between 0.0 and 1.0.");
        Preconditions.checkArgument(increaseRate > 0, "Increase rate must be positive integer.");
        Preconditions.checkArgument(
                rateThreshold >= initialRate, "Initial rate must not exceed threshold.");

        this.increaseRate = increaseRate;
        this.decreaseFactor = decreaseFactor;
        this.rateThreshold = rateThreshold;
        this.inFlightMessages = initialRate;
    }

    @Override
    public int getRateLimit() {
        return inFlightMessages;
    }

    @Override
    public void onAcknowledged() {
        inFlightMessages = (Math.min(inFlightMessages + increaseRate, rateThreshold));
    }

    @Override
    public void onThrottled() {
        inFlightMessages = Math.max(1, (int) Math.round(inFlightMessages * decreaseFactor));
    }
}
