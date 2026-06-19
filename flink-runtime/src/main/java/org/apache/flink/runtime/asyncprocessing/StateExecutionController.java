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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.core.asyncprocessing.AsyncFutureImpl;
import org.apache.flink.core.asyncprocessing.InternalAsyncFuture;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.asyncprocessing.declare.DeclarationManager;
import org.apache.flink.runtime.state.v2.internal.InternalPartitionedState;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * The {@link StateExecutionController} is responsible for handling {@link StateRequest}s. It
 * extends {@link AsyncExecutionController} and implements {@link StateRequestHandler}.
 */
public class StateExecutionController<K>
        extends AsyncExecutionController<K, StateRequest<?, ?, ?, ?>>
        implements StateRequestHandler {

    public StateExecutionController(
            MailboxExecutor mailboxExecutor,
            AsyncFutureImpl.AsyncFrameworkExceptionHandler exceptionHandler,
            AsyncExecutor<StateRequest<?, ?, ?, ?>> stateExecutor,
            DeclarationManager declarationManager,
            EpochManager.ParallelMode epochParallelMode,
            int maxParallelism,
            int batchSize,
            long bufferTimeout,
            int maxInFlightRecords,
            @Nullable SwitchContextListener<K> switchContextListener,
            @Nullable MetricGroup metricGroup) {
        super(
                mailboxExecutor,
                exceptionHandler,
                stateExecutor,
                declarationManager,
                epochParallelMode,
                maxParallelism,
                batchSize,
                bufferTimeout,
                maxInFlightRecords,
                switchContextListener,
                metricGroup);
    }

    /**
     * Submit a {@link StateRequest} to this AsyncExecutionController and trigger it if needed.
     *
     * @param state the state to request.
     * @param type the type of this request.
     * @param payload the payload input for this request.
     * @return the state future.
     */
    @Override
    public <IN, OUT> InternalAsyncFuture<OUT> handleRequest(
            @Nullable State state, StateRequestType type, @Nullable IN payload) {
        return handleRequest(state, type, false, payload, false);
    }

    /**
     * Submit a {@link StateRequest} to this AsyncExecutionController and trigger it if needed.
     *
     * @param state the state to request.
     * @param type the type of this request.
     * @param sync whether to trigger the request synchronously once it's ready.
     * @param payload the payload input for this request.
     * @param allowOverdraft whether to allow overdraft.
     * @return the state future.
     */
    public <IN, OUT> InternalAsyncFuture<OUT> handleRequest(
            @Nullable State state,
            StateRequestType type,
            boolean sync,
            @Nullable IN payload,
            boolean allowOverdraft) {
        // Step 1: build state future & assign context.
        InternalAsyncFuture<OUT> stateFuture = asyncFutureFactory.create(currentContext);
        StateRequest<K, ?, IN, OUT> request =
                new StateRequest<>(state, type, sync, payload, stateFuture, currentContext);

        handleRequest(request, allowOverdraft);
        return stateFuture;
    }

    @Override
    public <IN, OUT> OUT handleRequestSync(
            State state, StateRequestType type, @Nullable IN payload) {
        InternalAsyncFuture<OUT> stateFuture = handleRequest(state, type, true, payload, false);
        waitUntil(stateFuture::isDone);
        return stateFuture.get();
    }

    @Override
    public <N> void setCurrentNamespaceForState(
            @Nonnull InternalPartitionedState<N> state, N namespace) {
        currentContext.setNamespace(state, namespace);
    }
}
