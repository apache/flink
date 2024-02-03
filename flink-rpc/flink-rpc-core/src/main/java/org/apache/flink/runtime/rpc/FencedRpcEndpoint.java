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

import org.apache.flink.util.Preconditions;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
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

    protected FencedRpcEndpoint(
            RpcService rpcService,
            String endpointId,
            F fencingToken,
            Map<String, String> loggingContext) {
        super(rpcService, endpointId, loggingContext);

        Preconditions.checkNotNull(fencingToken, "The fence token should be null");
        Preconditions.checkNotNull(rpcServer, "The rpc server should be null");

        this.fencingToken = fencingToken;
    }

    protected FencedRpcEndpoint(RpcService rpcService, String endpointId, F fencingToken) {
        this(rpcService, endpointId, fencingToken, Collections.emptyMap());
    }

    protected FencedRpcEndpoint(RpcService rpcService, F fencingToken) {
        this(rpcService, UUID.randomUUID().toString(), fencingToken);
    }

    public F getFencingToken() {
        return fencingToken;
    }
}
