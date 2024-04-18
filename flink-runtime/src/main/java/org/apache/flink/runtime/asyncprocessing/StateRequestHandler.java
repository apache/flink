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
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.core.state.InternalStateFuture;

import javax.annotation.Nullable;

/** The handler which can process {@link StateRequest}. */
@Internal
public interface StateRequestHandler {

    /**
     * Submit a {@link StateRequest} to this StateRequestHandler.
     *
     * @param state the state to request. Could be {@code null} if the type is {@link
     *     StateRequestType#SYNC_POINT}.
     * @param type the type of this request.
     * @param payload the payload input for this request.
     * @return the state future.
     */
    <IN, OUT> InternalStateFuture<OUT> handleRequest(
            @Nullable State state, StateRequestType type, @Nullable IN payload);
}
