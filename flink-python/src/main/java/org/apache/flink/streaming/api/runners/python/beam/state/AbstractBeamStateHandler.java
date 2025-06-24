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

package org.apache.flink.streaming.api.runners.python.beam.state;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;

/** Abstract class extends {@link BeamStateHandler}, which implements the common handle logic. */
public abstract class AbstractBeamStateHandler<S> implements BeamStateHandler<S> {

    public BeamFnApi.StateResponse.Builder handle(BeamFnApi.StateRequest request, S state)
            throws Exception {
        switch (request.getRequestCase()) {
            case GET:
                return handleGet(request, state);
            case APPEND:
                return handleAppend(request, state);
            case CLEAR:
                return handleClear(request, state);
            default:
                throw new RuntimeException(
                        String.format(
                                "Unsupported request type %s for user state.",
                                request.getRequestCase()));
        }
    }
}
