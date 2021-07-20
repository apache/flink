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

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * {@link FencedRpcEndpoint} which is fenced with a fencing token which is bound to the lifetime of
 * the rpc endpoint.
 *
 * @param <F> type of the fencing token.
 */
public class PermanentlyFencedRpcEndpoint<F extends Serializable> extends FencedRpcEndpoint<F> {

    protected PermanentlyFencedRpcEndpoint(
            RpcService rpcService, String endpointId, F fencingToken) {
        super(rpcService, endpointId, Preconditions.checkNotNull(fencingToken));
    }

    protected PermanentlyFencedRpcEndpoint(RpcService rpcService, F fencingToken) {
        super(rpcService, Preconditions.checkNotNull(fencingToken));
    }

    @Override
    protected void setFencingToken(@Nullable F newFencingToken) {
        throw new UnsupportedOperationException(
                String.format(
                        "Cannot change the fencing token of a %s.",
                        PermanentlyFencedRpcEndpoint.class.getSimpleName()));
    }
}
