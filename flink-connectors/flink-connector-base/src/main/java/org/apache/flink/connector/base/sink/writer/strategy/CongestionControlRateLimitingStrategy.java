/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.base.sink.writer.strategy;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.util.Preconditions;

/**
 * A {@code RateLimitingStrategy} implementation that does the following:
 *
 * <ul>
 *   <li>Scales up when any request is successful.
 *   <li>Scales down when any message in a request is unsuccessful.
 *   <li>Uses a scaling strategy to scale up/down depending on whether the request is successful.
 * </ul>
 *
 * <p>This strategy works well for throughput-limited record-based sinks (e.g. Kinesis, Kafka).
 */
@PublicEvolving
public class CongestionControlRateLimitingStrategy implements RateLimitingStrategy {

    private final int maxInFlightRequests;
    private final ScalingStrategy<Integer> scalingStrategy;
    private int maxInFlightMessages;

    private int currentInFlightRequests;
    private int currentInFlightMessages;

    private CongestionControlRateLimitingStrategy(
            int maxInFlightRequests,
            int initialMaxInFlightMessages,
            ScalingStrategy<Integer> scalingStrategy) {
        Preconditions.checkArgument(
                maxInFlightRequests > 0, "maxInFlightRequests must be a positive integer.");
        Preconditions.checkArgument(
                initialMaxInFlightMessages > 0,
                "initialMaxInFlightMessages must be a positive integer.");
        Preconditions.checkNotNull(scalingStrategy, "scalingStrategy must be provided.");

        this.maxInFlightRequests = maxInFlightRequests;
        this.maxInFlightMessages = initialMaxInFlightMessages;
        this.scalingStrategy = scalingStrategy;
    }

    @Override
    public void registerInFlightRequest(RequestInfo requestInfo) {
        currentInFlightRequests++;
        currentInFlightMessages += requestInfo.getBatchSize();
    }

    @Override
    public void registerCompletedRequest(ResultInfo resultInfo) {
        currentInFlightRequests = Math.max(0, currentInFlightRequests - 1);
        currentInFlightMessages -= resultInfo.getBatchSize();
        if (resultInfo.getFailedMessages() > 0) {
            maxInFlightMessages = scalingStrategy.scaleDown(maxInFlightMessages);
        } else {
            maxInFlightMessages = scalingStrategy.scaleUp(maxInFlightMessages);
        }
    }

    @Override
    public boolean shouldBlock(RequestInfo requestInfo) {
        return currentInFlightRequests >= maxInFlightRequests
                || (currentInFlightMessages + requestInfo.getBatchSize() > maxInFlightMessages);
    }

    @Override
    public int getMaxBatchSize() {
        return maxInFlightMessages;
    }

    @PublicEvolving
    public static CongestionControlRateLimitingStrategyBuilder builder() {
        return new CongestionControlRateLimitingStrategyBuilder();
    }

    /** Builder for {@link CongestionControlRateLimitingStrategy}. */
    @PublicEvolving
    public static class CongestionControlRateLimitingStrategyBuilder {

        private int maxInFlightRequests;
        private int initialMaxInFlightMessages;
        private ScalingStrategy<Integer> scalingStrategy;

        public CongestionControlRateLimitingStrategyBuilder setMaxInFlightRequests(
                int maxInFlightRequests) {
            this.maxInFlightRequests = maxInFlightRequests;
            return this;
        }

        public CongestionControlRateLimitingStrategyBuilder setInitialMaxInFlightMessages(
                int initialMaxInFlightMessages) {
            this.initialMaxInFlightMessages = initialMaxInFlightMessages;
            return this;
        }

        public CongestionControlRateLimitingStrategyBuilder setScalingStrategy(
                ScalingStrategy<Integer> scalingStrategy) {
            this.scalingStrategy = scalingStrategy;
            return this;
        }

        public CongestionControlRateLimitingStrategy build() {
            return new CongestionControlRateLimitingStrategy(
                    maxInFlightRequests, initialMaxInFlightMessages, scalingStrategy);
        }
    }
}
