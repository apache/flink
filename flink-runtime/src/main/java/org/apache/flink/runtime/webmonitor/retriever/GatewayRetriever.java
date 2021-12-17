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

package org.apache.flink.runtime.webmonitor.retriever;

import org.apache.flink.runtime.rpc.RpcGateway;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

/**
 * Generic retriever interface for {@link RpcGateway}.
 *
 * @param <T> type of the object to retrieve
 */
public interface GatewayRetriever<T extends RpcGateway> {

    /**
     * Get future of object to retrieve.
     *
     * @return Future object to retrieve
     */
    CompletableFuture<T> getFuture();

    /**
     * Returns the currently retrieved gateway if there is such an object. Otherwise it returns an
     * empty optional.
     *
     * @return Optional object to retrieve
     */
    default Optional<T> getNow() {
        CompletableFuture<T> leaderFuture = getFuture();
        if (leaderFuture != null) {
            if (leaderFuture.isCompletedExceptionally() || leaderFuture.isCancelled()) {
                return Optional.empty();
            } else if (leaderFuture.isDone()) {
                try {
                    return Optional.of(leaderFuture.get());
                } catch (Exception e) {
                    // this should never happen
                    throw new FlinkRuntimeException(
                            "Unexpected error while accessing the retrieved gateway.", e);
                }
            } else {
                return Optional.empty();
            }
        } else {
            return Optional.empty();
        }
    }
}
