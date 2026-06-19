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

import org.apache.flink.util.Preconditions;

import java.util.concurrent.CompletableFuture;

/** The mocked {@link StateExecutor} for testing. */
public class MockStateExecutor implements StateExecutor {

    @Override
    public CompletableFuture<Void> executeBatchRequests(
            AsyncRequestContainer<StateRequest<?, ?, ?, ?>> asyncRequestContainer) {
        Preconditions.checkArgument(asyncRequestContainer instanceof MockAsyncRequestContainer);
        for (StateRequest<?, ?, ?, ?> request :
                ((MockAsyncRequestContainer<StateRequest<?, ?, ?, ?>>) asyncRequestContainer)
                        .getStateRequestList()) {
            executeRequestSync(request);
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public AsyncRequestContainer<StateRequest<?, ?, ?, ?>> createRequestContainer() {
        return new MockAsyncRequestContainer<>();
    }

    @Override
    public void executeRequestSync(StateRequest<?, ?, ?, ?> stateRequest) {
        stateRequest.getFuture().complete(null);
    }

    @Override
    public boolean fullyLoaded() {
        return false;
    }

    @Override
    public void shutdown() {}
}
