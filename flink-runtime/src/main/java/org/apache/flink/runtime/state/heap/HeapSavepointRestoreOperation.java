/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.ListDelimitedSerializer;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.restore.FullSnapshotRestoreOperation;
import org.apache.flink.runtime.state.restore.KeyGroup;
import org.apache.flink.runtime.state.restore.KeyGroupEntry;
import org.apache.flink.runtime.state.restore.SavepointRestoreResult;
import org.apache.flink.runtime.state.restore.ThrowingIterator;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.state.CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix;
import static org.apache.flink.runtime.state.CompositeKeySerializationUtils.readKey;
import static org.apache.flink.runtime.state.CompositeKeySerializationUtils.readKeyGroup;
import static org.apache.flink.runtime.state.CompositeKeySerializationUtils.readNamespace;

/**
 * Implementation of heap savepoint restore operation. Savepoint shares a common unified binary
 * format across all state backends.
 *
 * @param <K> The data type that the serializer serializes.
 */
public class HeapSavepointRestoreOperation<K> implements RestoreOperation<Void> {
    private final int keyGroupPrefixBytes;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final FullSnapshotRestoreOperation<K> savepointRestoreOperation;
    private final HeapMetaInfoRestoreOperation<K> heapMetaInfoRestoreOperation;
    /*
       Shared wrappers for deserializing an entry in the state handle. An optimization
       to reduce the number of objects created.
    */
    private final DataInputDeserializer entryKeyDeserializer;
    private final DataInputDeserializer entryValueDeserializer;
    private final ListDelimitedSerializer listDelimitedSerializer;

    HeapSavepointRestoreOperation(
            @Nonnull Collection<KeyedStateHandle> restoreStateHandles,
            StateSerializerProvider<K> keySerializerProvider,
            ClassLoader userCodeClassLoader,
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            @Nonnull KeyGroupRange keyGroupRange,
            int numberOfKeyGroups,
            StateTableFactory<K> stateTableFactory,
            InternalKeyContext<K> keyContext) {
        this.keySerializerProvider = keySerializerProvider;
        this.registeredKVStates = registeredKVStates;
        this.registeredPQStates = registeredPQStates;
        this.savepointRestoreOperation =
                new FullSnapshotRestoreOperation<>(
                        keyGroupRange,
                        userCodeClassLoader,
                        restoreStateHandles,
                        keySerializerProvider);
        this.keyGroupPrefixBytes = computeRequiredBytesInKeyGroupPrefix(numberOfKeyGroups);
        this.heapMetaInfoRestoreOperation =
                new HeapMetaInfoRestoreOperation<>(
                        keySerializerProvider,
                        priorityQueueSetFactory,
                        keyGroupRange,
                        numberOfKeyGroups,
                        stateTableFactory,
                        keyContext);
        this.entryKeyDeserializer = new DataInputDeserializer();
        this.entryValueDeserializer = new DataInputDeserializer();
        this.listDelimitedSerializer = new ListDelimitedSerializer();
    }

    @Override
    public Void restore() throws Exception {

        registeredKVStates.clear();
        registeredPQStates.clear();

        try (ThrowingIterator<SavepointRestoreResult> restore =
                this.savepointRestoreOperation.restore()) {
            while (restore.hasNext()) {
                SavepointRestoreResult restoreResult = restore.next();
                List<StateMetaInfoSnapshot> restoredMetaInfos =
                        restoreResult.getStateMetaInfoSnapshots();

                final Map<Integer, StateMetaInfoSnapshot> kvStatesById =
                        this.heapMetaInfoRestoreOperation.createOrCheckStateForMetaInfo(
                                restoredMetaInfos, registeredKVStates, registeredPQStates);

                try (ThrowingIterator<KeyGroup> keyGroups = restoreResult.getRestoredKeyGroups()) {
                    while (keyGroups.hasNext()) {
                        readKeyGroupStateData(
                                keyGroups.next(),
                                keySerializerProvider.previousSchemaSerializer(),
                                kvStatesById);
                    }
                }
            }
        }

        return null;
    }

    private void readKeyGroupStateData(
            KeyGroup keyGroup,
            TypeSerializer<K> keySerializer,
            Map<Integer, StateMetaInfoSnapshot> kvStatesById)
            throws Exception {

        try (ThrowingIterator<KeyGroupEntry> entries = keyGroup.getKeyGroupEntries()) {
            while (entries.hasNext()) {
                KeyGroupEntry groupEntry = entries.next();
                StateMetaInfoSnapshot infoSnapshot = kvStatesById.get(groupEntry.getKvStateId());
                switch (infoSnapshot.getBackendStateType()) {
                    case KEY_VALUE:
                        readKVStateData(keySerializer, groupEntry, infoSnapshot);
                        break;
                    case PRIORITY_QUEUE:
                        readPriorityQueue(groupEntry, infoSnapshot);
                        break;
                    case OPERATOR:
                    case BROADCAST:
                        throw new IllegalStateException(
                                "Expected only keyed state. Received: "
                                        + infoSnapshot.getBackendStateType());
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void readPriorityQueue(KeyGroupEntry groupEntry, StateMetaInfoSnapshot infoSnapshot)
            throws IOException {
        entryKeyDeserializer.setBuffer(groupEntry.getKey());
        entryKeyDeserializer.skipBytesToRead(keyGroupPrefixBytes);
        HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement>
                priorityQueueSnapshotRestoreWrapper =
                        (HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement>)
                                registeredPQStates.get(infoSnapshot.getName());
        HeapPriorityQueueElement timer =
                priorityQueueSnapshotRestoreWrapper
                        .getMetaInfo()
                        .getElementSerializer()
                        .deserialize(entryKeyDeserializer);
        HeapPriorityQueueSet<HeapPriorityQueueElement> priorityQueue =
                priorityQueueSnapshotRestoreWrapper.getPriorityQueue();
        priorityQueue.add(timer);
    }

    @SuppressWarnings("unchecked")
    private void readKVStateData(
            TypeSerializer<K> keySerializer,
            KeyGroupEntry groupEntry,
            StateMetaInfoSnapshot infoSnapshot)
            throws IOException {
        StateTable<K, Object, Object> stateTable =
                (StateTable<K, Object, Object>) registeredKVStates.get(infoSnapshot.getName());
        RegisteredKeyValueStateBackendMetaInfo<?, ?> metaInfo = stateTable.getMetaInfo();
        TypeSerializer<?> namespaceSerializer = metaInfo.getPreviousNamespaceSerializer();
        TypeSerializer<?> stateSerializer = metaInfo.getPreviousStateSerializer();
        boolean isAmbigousKey =
                keySerializer.getLength() < 0 && namespaceSerializer.getLength() < 0;
        entryKeyDeserializer.setBuffer(groupEntry.getKey());
        entryValueDeserializer.setBuffer(groupEntry.getValue());
        int keyGroup = readKeyGroup(keyGroupPrefixBytes, entryKeyDeserializer);
        K key = readKey(keySerializer, entryKeyDeserializer, isAmbigousKey);
        Object namespace = readNamespace(namespaceSerializer, entryKeyDeserializer, isAmbigousKey);
        switch (metaInfo.getStateType()) {
            case LIST:
                stateTable.put(
                        key,
                        keyGroup,
                        namespace,
                        listDelimitedSerializer.deserializeList(
                                groupEntry.getValue(),
                                ((ListSerializer<?>) stateSerializer).getElementSerializer()));
                break;
            case VALUE:
            case REDUCING:
            case FOLDING:
            case AGGREGATING:
                stateTable.put(
                        key,
                        keyGroup,
                        namespace,
                        stateSerializer.deserialize(entryValueDeserializer));
                break;
            case MAP:
                deserializeMapStateEntry(
                        (StateTable<K, Object, Map<Object, Object>>)
                                (StateTable<K, ?, ?>) stateTable,
                        keyGroup,
                        key,
                        namespace,
                        (MapSerializer<Object, Object>) stateSerializer);
                break;
            default:
                throw new IllegalStateException("Unknown state type: " + metaInfo.getStateType());
        }
    }

    private void deserializeMapStateEntry(
            StateTable<K, Object, Map<Object, Object>> stateTable,
            int keyGroup,
            K key,
            Object namespace,
            MapSerializer<Object, Object> stateSerializer)
            throws IOException {
        Object mapEntryKey = stateSerializer.getKeySerializer().deserialize(entryKeyDeserializer);
        boolean isNull = entryValueDeserializer.readBoolean();
        final Object mapEntryValue;
        if (isNull) {
            mapEntryValue = null;
        } else {
            mapEntryValue =
                    stateSerializer.getValueSerializer().deserialize(entryValueDeserializer);
        }

        Map<Object, Object> userMap = stateTable.get(key, namespace);
        if (userMap == null) {
            userMap = new HashMap<>();
            stateTable.put(key, keyGroup, namespace, userMap);
        }
        userMap.put(mapEntryKey, mapEntryValue);
    }
}
