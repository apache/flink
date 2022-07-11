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

package org.apache.flink.connector.base.sink.writer.strategy;

import org.apache.flink.annotation.PublicEvolving;

/**
 * Controls the rate of requests being made in the {@code AsyncSinkWriter}.
 *
 * <p>The RateLimitingStrategy is consulted in the {@code AsyncSinkWriter} before sending a request.
 */
@PublicEvolving
public interface RateLimitingStrategy {
    /**
     * Registers the information of requests being sent (e.g. to track the current inFlightMessages
     * / requests).
     *
     * @param requestInfo Data class containing information on request being sent
     */
    void registerInFlightRequest(RequestInfo requestInfo);

    /**
     * Registers the information of requests completing (e.g. to track the current inFlightMessages
     * / requests). Any dynamic scaling on failed requests should be done here too.
     *
     * @param requestInfo Data class containing information on request completed
     */
    void registerCompletedRequest(RequestInfo requestInfo);

    /**
     * Decides whether the next request should be blocked.
     *
     * @param requestInfo Data class containing information on request being sent
     */
    boolean shouldBlock(RequestInfo requestInfo);

    /**
     * Returns the max batch size to be used by the {@code AsyncSinkWriter}. This is required
     * because the {@code AsyncSinkWriter} generates the batch, and RateLimitingStrategy evaluates
     * whether the batch should be allowed through.
     */
    int getMaxBatchSize();
}
