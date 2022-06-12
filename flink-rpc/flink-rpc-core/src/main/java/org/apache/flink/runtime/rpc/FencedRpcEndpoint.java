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

package org.apache.flink.runtime.rpc;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.io.Serializable;
import java.util.UUID;

/**
 * Base class for fenced {@link RpcEndpoint}. A fenced rpc endpoint expects all rpc messages being
 * enriched with fencing tokens. Furthermore, the rpc endpoint has its own fencing token assigned.
 * The rpc is then only executed if the attached fencing token equals the endpoint's own token.
 *
 * @param <F> type of the fencing token
 */
public abstract class FencedRpcEndpoint<F extends Serializable> extends RpcEndpoint {

    private final F fencingToken;
    private volatile MainThreadExecutor fencedMainThreadExecutor;

    protected FencedRpcEndpoint(RpcService rpcService, String endpointId, @Nonnull F fencingToken) {
        super(rpcService, endpointId);

        Preconditions.checkNotNull(fencingToken, "The fence token should be null");
        Preconditions.checkNotNull(rpcServer, "The rpc server should be null");

        // no fencing token == no leadership
        this.fencingToken = fencingToken;

        MainThreadExecutable mainThreadExecutable =
                getRpcService().fenceRpcServer(rpcServer, fencingToken);
        setFencedMainThreadExecutor(
                new MainThreadExecutor(
                        mainThreadExecutable, this::validateRunsInMainThread, endpointId));
    }

    protected FencedRpcEndpoint(RpcService rpcService, @Nonnull F fencingToken) {
        this(rpcService, UUID.randomUUID().toString(), fencingToken);
    }

    public F getFencingToken() {
        return fencingToken;
    }

    /**
     * Set fenced main thread executor and register it to closeable register.
     *
     * @param fencedMainThreadExecutor the given fenced main thread executor
     */
    private void setFencedMainThreadExecutor(MainThreadExecutor fencedMainThreadExecutor) {
        if (this.fencedMainThreadExecutor != null) {
            this.fencedMainThreadExecutor.close();
            unregisterResource(this.fencedMainThreadExecutor);
        }
        this.fencedMainThreadExecutor = fencedMainThreadExecutor;
        registerResource(this.fencedMainThreadExecutor);
    }

    /**
     * Returns a main thread executor which is bound to the currently valid fencing token. This
     * means that runnables which are executed with this executor fail after the fencing token has
     * changed. This allows to scope operations by the fencing token.
     *
     * @return MainThreadExecutor bound to the current fencing token
     */
    @Override
    protected MainThreadExecutor getMainThreadExecutor() {
        return fencedMainThreadExecutor;
    }

    @VisibleForTesting
    public boolean validateResourceClosed() {
        return super.validateResourceClosed()
                && (fencedMainThreadExecutor == null
                        || fencedMainThreadExecutor.validateScheduledExecutorClosed());
    }
}
