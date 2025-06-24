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

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.streaming.api.utils.ByteArrayWrapper;

import org.apache.beam.model.fnexecution.v1.BeamFnApi;

/**
 * Interface for getting the underlying state based on Beam state request (keyed state or operator
 * state).
 */
public interface BeamStateStore {

    /** Parse {@link BeamFnApi.StateRequest} and return the corresponding {@link ListState}. */
    ListState<byte[]> getListState(BeamFnApi.StateRequest request) throws Exception;

    /** Parse {@link BeamFnApi.StateRequest} and return the corresponding {@link MapState}. */
    MapState<ByteArrayWrapper, byte[]> getMapState(BeamFnApi.StateRequest request) throws Exception;

    String PYTHON_STATE_PREFIX = "python-state-";

    static BeamStateStore unsupported() {
        return new BeamStateStore() {
            @Override
            public ListState<byte[]> getListState(BeamFnApi.StateRequest request) throws Exception {
                throw new RuntimeException("List state is not supported");
            }

            @Override
            public MapState<ByteArrayWrapper, byte[]> getMapState(BeamFnApi.StateRequest request)
                    throws Exception {
                throw new RuntimeException("Map state is not supported");
            }
        };
    }
}
