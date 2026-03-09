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

package org.apache.flink;

import org.apache.flink.connector.base.sink.writer.strategy.RateLimitingStrategy;
import org.apache.flink.connector.base.sink.writer.strategy.RequestInfo;
import org.apache.flink.connector.base.sink.writer.strategy.ResultInfo;

import io.github.bucket4j.Bucket;

import java.io.Serializable;

public class TokenBucketRateLimitingStrategy implements RateLimitingStrategy, Serializable {
    private static final long serialVersionUID = 1L;
    private final int maxInFlightRequests;
    private final long tokensPerSecond;
    private final long tokensPerMinute;
    private transient int currentInFlightRequests = 0;
    private transient Bucket bucket;

    public TokenBucketRateLimitingStrategy(
            int maxInFlightRequests, long tokensPerSecond, long tokensPerMinute) {
        this.maxInFlightRequests = maxInFlightRequests;
        this.tokensPerSecond = tokensPerSecond;
        this.tokensPerMinute = tokensPerMinute;
    }

    private Bucket getBucket() {
        if (bucket == null) {
            bucket =
                    TokenBucketProvider.getInstance(
                            "TokenBucketRateLimitingStrategy", tokensPerSecond, tokensPerMinute);
        }
        return bucket;
    }

    @Override
    public boolean shouldBlock(RequestInfo requestInfo) {
        // This is the problematic condition: blocks when tokens unavailable OR max in-flight
        // reached
        // When currentInFlightRequests == 0 AND tokens are exhausted, this returns true
        // but nothing will unblock it since no requests will complete
        return currentInFlightRequests >= maxInFlightRequests || areTokensNotAvailable(requestInfo);
    }

    private boolean areTokensNotAvailable(RequestInfo requestInfo) {
        int batchSize = requestInfo.getBatchSize();
        if (batchSize <= 0) {
            return false;
        }
        // Check if tokens are available - if not, shouldBlock returns true
        // This causes the hang when currentInFlightRequests == 0
        return !getBucket().estimateAbilityToConsume(batchSize).canBeConsumed();
    }

    @Override
    public void registerInFlightRequest(RequestInfo requestInfo) {
        currentInFlightRequests++;
        int batchSize = requestInfo.getBatchSize();
        if (batchSize > 0) {
            getBucket().tryConsume(batchSize);
        }
    }

    @Override
    public void registerCompletedRequest(ResultInfo resultInfo) {
        // Only decrements counter - doesn't refill tokens
        // Tokens refill based on time, but if shouldBlock() returns true
        // when currentInFlightRequests == 0, nothing triggers a recheck
        currentInFlightRequests--;
    }

    @Override
    public int getMaxBatchSize() {
        return 100;
    }

    private void readObject(java.io.ObjectInputStream in)
            throws java.io.IOException, ClassNotFoundException {
        in.defaultReadObject();
        currentInFlightRequests = 0;
        bucket = null;
    }
}
