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

package org.apache.flink.state.api.schema;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.CustomRestoreSerializerFactory;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonOptionsKeys;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys;
import org.apache.flink.state.api.input.deserializer.MissingClassSerializerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility for extracting {@link StateSchemaInfo} from a savepoint without instantiating the full
 * state backend or requiring user POJO classes on the classpath.
 *
 * <p>It reads the {@link KeyedBackendSerializationProxy} header that every heap/RocksDB keyed state
 * file starts with.
 */
@Internal
public final class StateSchemaExtractor {

    private StateSchemaExtractor() {}

    /**
     * Reads state schema information from the first available keyed state handle in the given
     * operator state.
     *
     * <p>Returns an empty list rather than throwing when no keyed state handle is found: an
     * operator may register only non-keyed (list/union/broadcast) state, in which case it has no
     * keyed state to describe.
     *
     * <p>The metadata header lives in different places depending on the state backend: heap ({@code
     * HashMapStateBackend}) savepoints hand back a {@code KeyGroupsStateHandle}, which is itself a
     * {@link StreamStateHandle} starting with the header; RocksDB (incremental or full native)
     * snapshots hand back an {@link IncrementalKeyedStateHandle}, whose own data stream starts with
     * the SST payload instead, so the header must be read from {@link
     * IncrementalKeyedStateHandle#getMetaDataStateHandle()}.
     *
     * @param operatorState the operator state from a loaded savepoint / checkpoint metadata
     * @return list of schema info, one entry per registered state; never null, may be empty
     * @throws IOException if the state header cannot be read
     */
    public static List<StateSchemaInfo> extractSchema(OperatorState operatorState)
            throws IOException {

        for (OperatorSubtaskState subtask : operatorState.getSubtaskStates().values()) {
            for (KeyedStateHandle handle : subtask.getManagedKeyedState()) {
                StreamStateHandle metadataHandle = null;
                if (handle instanceof IncrementalKeyedStateHandle) {
                    metadataHandle =
                            ((IncrementalKeyedStateHandle) handle).getMetaDataStateHandle();
                } else if (handle instanceof StreamStateHandle) {
                    metadataHandle = (StreamStateHandle) handle;
                }
                if (metadataHandle != null) {
                    try (java.io.InputStream stream = metadataHandle.openInputStream()) {
                        return extractSchema(new DataInputViewStreamWrapper(stream));
                    }
                }
            }
        }
        return Collections.emptyList();
    }

    /**
     * Package-private overload that accepts a {@link DataInputView} directly. Allows unit tests to
     * inject pre-built byte arrays without a real filesystem.
     */
    static List<StateSchemaInfo> extractSchema(DataInputView in) throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        KeyedBackendSerializationProxy<?> proxy = new KeyedBackendSerializationProxy<>(classLoader);
        CustomRestoreSerializerFactory.set(MissingClassSerializerFactory::create);
        proxy.read(in);

        TypeSerializerSnapshot<?> keySnapshot = proxy.getKeySerializerSnapshot();
        List<StateSchemaInfo> result = new ArrayList<>();

        for (StateMetaInfoSnapshot meta : proxy.getStateMetaInfoSnapshots()) {
            String kindStr = meta.getOption(CommonOptionsKeys.KEYED_STATE_TYPE);
            StateDescriptor.Type stateKind;
            try {
                stateKind = StateDescriptor.Type.valueOf(kindStr);
            } catch (IllegalArgumentException | NullPointerException e) {
                stateKind = StateDescriptor.Type.UNKNOWN;
            }

            TypeSerializerSnapshot<?> valueSnapshot =
                    meta.getTypeSerializerSnapshot(CommonSerializerKeys.VALUE_SERIALIZER);
            TypeSerializerSnapshot<?> mapKeySnapshot =
                    meta.getTypeSerializerSnapshot(CommonSerializerKeys.USER_KEY_SERIALIZER);
            TypeSerializerSnapshot<?> namespaceSnapshot =
                    meta.getTypeSerializerSnapshot(CommonSerializerKeys.NAMESPACE_SERIALIZER);

            if (valueSnapshot == null) {
                continue;
            }

            result.add(
                    new StateSchemaInfo(
                            meta.getName(),
                            stateKind,
                            keySnapshot,
                            valueSnapshot,
                            mapKeySnapshot,
                            namespaceSnapshot));
        }

        return result;
    }
}
