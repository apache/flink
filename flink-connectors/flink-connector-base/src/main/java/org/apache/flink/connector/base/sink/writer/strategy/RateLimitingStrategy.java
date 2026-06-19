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
 * RateLimitingStrategy is used to control the rate of requests. It is given data using register
 * methods {@link #registerInFlightRequest(RequestInfo)} and {@link
 * #registerCompletedRequest(ResultInfo)}. It will then use that information to make a decision to
 * whether a given request should be blocked or allowed through.
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
     * Registers the result of completed requests (e.g. to track the current inFlightMessages /
     * requests). Any dynamic scaling on failed messages should be done here.
     *
     * @param resultInfo Data class containing information on request completed
     */
    void registerCompletedRequest(ResultInfo resultInfo);

    /**
     * Decides whether the next request should be blocked.
     *
     * @param requestInfo Data class containing information on request being sent
     */
    boolean shouldBlock(RequestInfo requestInfo);

    /**
     * Returns the current max batch size that RateLimitingStrategy will allow through. This is
     * required so that the component that constructs the {@link ResultInfo} that is passed into
     * {@link #shouldBlock(RequestInfo)} can construct a passable {@link ResultInfo}.
     */
    int getMaxBatchSize();
}
