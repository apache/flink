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

package org.apache.flink.api.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.concurrent.CompletableFuture;

/**
 * A interface of a split enumerator responsible for the followings: 1. discover the splits for the
 * {@link SourceReader} to read. 2. assign the splits to the source reader.
 */
@PublicEvolving
public interface CoordinationSplitEnumerator<SplitT extends SourceSplit, CheckpointT>
        extends SplitEnumerator<SplitT, CheckpointT>, CoordinationRequestHandler {

    /**
     * Handles a custom coordination event from dispatcher, which is typically initiated from an
     * external client or sidecar.
     *
     * <p>This method has a default implementation that throws and exception. The common events for
     * reader registration and split requests are not dispatched to this method, but rather invoke
     * the {@link #addReader(int)} and {@link #handleSplitRequest(int, String)} methods.
     *
     * @param request the coordination request from the dispathcer.
     */
    default CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        throw new FlinkRuntimeException(
                "Coordination event not supported by current source coordinator");
    }
}
