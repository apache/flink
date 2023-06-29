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
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.apache.flink.shaded.guava31.com.google.common.util.concurrent.RateLimiter;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

/** An implementation of {@link RateLimiter} based on Guava's RateLimiter. */
@Internal
public class GuavaRateLimiter
        implements org.apache.flink.api.connector.source.util.ratelimit.RateLimiter {

    private final Executor limiter =
            Executors.newSingleThreadExecutor(new ExecutorThreadFactory("flink-rate-limiter"));
    private final RateLimiter rateLimiter;

    public GuavaRateLimiter(double maxPerSecond) {
        this.rateLimiter = RateLimiter.create(maxPerSecond);
    }

    @Override
    public CompletionStage<Void> acquire() {
        return CompletableFuture.runAsync(rateLimiter::acquire, limiter);
    }
}
