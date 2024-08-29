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

package org.apache.flink.runtime.state.v2.adaptor;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.asyncprocessing.StateExecutor;
import org.apache.flink.runtime.asyncprocessing.StateRequestHandler;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.KeyedStateBackend;
import org.apache.flink.runtime.state.v2.StateDescriptor;
import org.apache.flink.runtime.state.v2.StateDescriptorUtils;

import javax.annotation.Nonnull;

import java.io.IOException;

/**
 * A adaptor that transforms {@link KeyedStateBackend} into {@link AsyncKeyedStateBackend}.
 *
 * @param <K> The key by which state is keyed.
 */
public class AsyncKeyedStateBackendAdaptor<K> implements AsyncKeyedStateBackend {
    private final KeyedStateBackend<K> keyedStateBackend;

    public AsyncKeyedStateBackendAdaptor(KeyedStateBackend<K> keyedStateBackend) {
        this.keyedStateBackend = keyedStateBackend;
    }

    @Override
    public void setup(@Nonnull StateRequestHandler stateRequestHandler) {}

    @Nonnull
    @Override
    public <N, S extends State, SV> S createState(
            @Nonnull N defaultNamespace,
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<SV> stateDesc)
            throws Exception {
        org.apache.flink.api.common.state.StateDescriptor rawStateDesc =
                StateDescriptorUtils.transformFromV2ToV1(stateDesc);
        org.apache.flink.api.common.state.State rawState =
                keyedStateBackend.getOrCreateKeyedState(namespaceSerializer, rawStateDesc);
        switch (rawStateDesc.getType()) {
            case VALUE:
                return (S) new ValueStateWrapper((ValueState) rawState);
            default:
                throw new UnsupportedOperationException(
                        String.format("Unsupported state type: %s", rawStateDesc.getType()));
        }
    }

    @Nonnull
    @Override
    public StateExecutor createStateExecutor() {
        return null;
    }

    @Override
    public void dispose() {}

    @Override
    public void close() throws IOException {}
}
