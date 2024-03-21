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

package org.apache.flink.runtime.taskprocessing;

import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.state.v2.StateFuture;

import java.util.Optional;

/**
 * A processing request encapsulates the parameters, the {@link State} to access, and the type of
 * access.
 *
 * @param <OUT> Type of value that request will return.
 */
public interface ProcessingRequest<OUT> {
    /** The underlying state to be accessed, can be empty. */
    Optional<?> getUnderlyingState();

    /** The parameter of the request. */
    Parameter getParameter();

    /** The future to collect the result of the request. */
    StateFuture<OUT> getFuture();

    RequestType getRequestType();

    /** The type of processing request. */
    enum RequestType {
        /** Process one record without state access. */
        SYNC,
        /** Get from one {@link State}. */
        /** Delete from one {@link State}. */
        DELETE,
        GET,
        /** Put to one {@link State}. */
        PUT,
        /** Merge value to an exist key in {@link State}. Mainly used for listState. */
        MERGE
    }

    /** The parameter of the request. */
    interface Parameter<K> {
        /**
         * The key of one request. Except for requests of {@link RequestType#SYNC}, all other
         * requests should provide a key.
         */
        Optional<K> getKey();

        /** The value of one request. */
        Optional<?> getValue();
    }
}
