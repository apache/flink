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
import org.apache.flink.runtime.state.OperatorBackendSerializationProxy;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonOptionsKeys;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.CommonSerializerKeys;
import org.apache.flink.state.api.input.deserializer.MissingClassSerializerFactory;
import org.apache.flink.state.table.SavepointConnectorOptions.StateReaderMode;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Utility for extracting {@link StateSchemaInfo} / {@link OperatorStateSchemaInfo} from a savepoint
 * without instantiating the full state backend or requiring user POJO classes on the classpath, by
 * reading the {@link KeyedBackendSerializationProxy} / {@link OperatorBackendSerializationProxy}
 * header that every keyed / non-keyed state file starts with.
 */
@Internal
public final class StateSchemaExtractor {

    private StateSchemaExtractor() {}

    /**
     * Reads state schema info from the first available keyed state handle. Returns an empty list
     * (rather than throwing) when the operator registers only non-keyed state.
     *
     * <p>The metadata header's location depends on the backend: heap ({@code HashMapStateBackend})
     * hands back a {@code KeyGroupsStateHandle} that is itself a {@link StreamStateHandle} starting
     * with the header, while RocksDB hands back an {@link IncrementalKeyedStateHandle} whose stream
     * starts with the SST payload instead, so the header must come from {@link
     * IncrementalKeyedStateHandle#getMetaDataStateHandle()}.
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
                    try (InputStream stream = metadataHandle.openInputStream()) {
                        return extractSchema(new DataInputViewStreamWrapper(stream));
                    }
                }
            }
        }
        return Collections.emptyList();
    }

    /** Package-private overload for tests to inject pre-built bytes without a real filesystem. */
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

    /**
     * Reads non-keyed (operator) state schema info — {@code ListState}, {@code UnionState}, {@code
     * BroadcastState} — from the first available operator state handle. Returns an empty list
     * (rather than throwing) when none is found. Unlike keyed state, an {@link OperatorStateHandle}
     * is itself a {@link StreamStateHandle} starting with the metadata header, for every backend.
     */
    public static List<OperatorStateSchemaInfo> extractOperatorSchema(OperatorState operatorState)
            throws IOException {

        for (OperatorSubtaskState subtask : operatorState.getSubtaskStates().values()) {
            for (OperatorStateHandle handle : subtask.getManagedOperatorState()) {
                try (InputStream stream = handle.openInputStream()) {
                    return extractOperatorSchema(new DataInputViewStreamWrapper(stream));
                }
            }
        }
        return Collections.emptyList();
    }

    /** Package-private overload for tests to inject pre-built bytes without a real filesystem. */
    static List<OperatorStateSchemaInfo> extractOperatorSchema(DataInputView in)
            throws IOException {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        OperatorBackendSerializationProxy proxy =
                new OperatorBackendSerializationProxy(classLoader);
        CustomRestoreSerializerFactory.set(MissingClassSerializerFactory::create);
        proxy.read(in);

        List<OperatorStateSchemaInfo> result = new ArrayList<>();

        for (StateMetaInfoSnapshot meta : proxy.getOperatorStateMetaInfoSnapshots()) {
            TypeSerializerSnapshot<?> valueSnapshot =
                    meta.getTypeSerializerSnapshot(CommonSerializerKeys.VALUE_SERIALIZER);
            if (valueSnapshot == null) {
                continue;
            }

            // Only union-distributed list state is redistributed as a whole on rescale; every
            // other distribution mode behaves like a plain (split) ListState here.
            String distributionMode =
                    meta.getOption(CommonOptionsKeys.OPERATOR_STATE_DISTRIBUTION_MODE);
            StateReaderMode kind =
                    OperatorStateHandle.Mode.UNION.name().equals(distributionMode)
                            ? StateReaderMode.UNION
                            : StateReaderMode.LIST;

            result.add(new OperatorStateSchemaInfo(meta.getName(), kind, valueSnapshot, null));
        }

        for (StateMetaInfoSnapshot meta : proxy.getBroadcastStateMetaInfoSnapshots()) {
            TypeSerializerSnapshot<?> valueSnapshot =
                    meta.getTypeSerializerSnapshot(CommonSerializerKeys.VALUE_SERIALIZER);
            TypeSerializerSnapshot<?> keySnapshot =
                    meta.getTypeSerializerSnapshot(CommonSerializerKeys.KEY_SERIALIZER);
            if (valueSnapshot == null || keySnapshot == null) {
                continue;
            }

            result.add(
                    new OperatorStateSchemaInfo(
                            meta.getName(), StateReaderMode.BROADCAST, valueSnapshot, keySnapshot));
        }

        return result;
    }
}
