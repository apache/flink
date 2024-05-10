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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.annotation.Internal;

import java.util.concurrent.CompletableFuture;

/**
 * Executor for executing batch {@link StateRequest}s.
 *
 * <p>Notice that the owner who create the {@code StateExecutor} is responsible for shutting down it
 * when it is no longer in use.
 */
@Internal
public interface StateExecutor {
    /**
     * Execute a batch of state requests.
     *
     * @param stateRequestContainer The StateRequestContainer which holds the given batch of
     *     processing requests.
     * @return A future can determine whether execution has completed.
     */
    CompletableFuture<Void> executeBatchRequests(StateRequestContainer stateRequestContainer);

    /**
     * Create a {@link StateRequestContainer} which is used to hold the batched {@link
     * StateRequest}.
     */
    StateRequestContainer createStateRequestContainer();

    /** Shutdown the StateExecutor, and new committed state execution requests will be rejected. */
    void shutdown();
}
