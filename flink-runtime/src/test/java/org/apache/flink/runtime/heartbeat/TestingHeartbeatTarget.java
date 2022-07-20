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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;

class TestingHeartbeatTarget<T> implements HeartbeatTarget<T> {
    private final BiFunction<ResourceID, T, CompletableFuture<Void>> receiveHeartbeatFunction;

    private final BiFunction<ResourceID, T, CompletableFuture<Void>> requestHeartbeatFunction;

    TestingHeartbeatTarget(
            BiFunction<ResourceID, T, CompletableFuture<Void>> receiveHeartbeatFunction,
            BiFunction<ResourceID, T, CompletableFuture<Void>> requestHeartbeatFunction) {
        this.receiveHeartbeatFunction = receiveHeartbeatFunction;
        this.requestHeartbeatFunction = requestHeartbeatFunction;
    }

    @Override
    public CompletableFuture<Void> receiveHeartbeat(
            ResourceID heartbeatOrigin, T heartbeatPayload) {
        return receiveHeartbeatFunction.apply(heartbeatOrigin, heartbeatPayload);
    }

    @Override
    public CompletableFuture<Void> requestHeartbeat(ResourceID requestOrigin, T heartbeatPayload) {
        return requestHeartbeatFunction.apply(requestOrigin, heartbeatPayload);
    }
}
