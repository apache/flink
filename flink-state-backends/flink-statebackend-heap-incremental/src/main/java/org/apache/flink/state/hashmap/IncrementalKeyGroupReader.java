/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.hashmap;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.runtime.state.StateSnapshotKeyGroupReader;
import org.apache.flink.runtime.state.heap.HeapRestoreOperation.KeyGroupReaderFactory;
import org.apache.flink.runtime.state.heap.StateMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.flink.state.hashmap.IncrementalKeyedBackendSerializationProxy.VERSION;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Reads the contents of {@link IncrementalCopyOnWriteStateMap}.
 *
 * @see IncrementalKeyGroupWriter
 */
class IncrementalKeyGroupReader<K, N, S> implements StateSnapshotKeyGroupReader {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalKeyGroupReader.class);

    public static final KeyGroupReaderFactory FACTORY =
            (stateSnapshotRestore, readVersionHint) -> {
                if (readVersionHint >= VERSION
                        && stateSnapshotRestore instanceof IncrementalCopyOnWriteStateTable) {
                    return new IncrementalKeyGroupReader<>(
                            (IncrementalCopyOnWriteStateTable<?, ?, ?>) stateSnapshotRestore);
                } else {
                    return stateSnapshotRestore.keyGroupReader(readVersionHint);
                }
            };

    private final IncrementalCopyOnWriteStateTable<K, N, S> stateTable;

    public IncrementalKeyGroupReader(IncrementalCopyOnWriteStateTable<K, N, S> stateTable) {
        this.stateTable = stateTable;
    }

    @Override
    public void readMappingsInKeyGroup(DataInputView in, int keyGroupId) throws IOException {
        TypeSerializer<K> keySerializer = stateTable.getKeySerializer();
        TypeSerializer<N> namespaceSerializer = stateTable.getNamespaceSerializer();
        TypeSerializer<S> stateSerializer = stateTable.getStateSerializer();
        readEntries(in, keyGroupId, keySerializer, namespaceSerializer, stateSerializer);
        readRemovals(in, keyGroupId, keySerializer, namespaceSerializer);
    }

    /** @see IncrementalCopyOnWriteStateMapEntryIterator */
    private void readEntries(
            DataInputView in,
            int keyGroupId,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer,
            TypeSerializer<S> stateSerializer)
            throws IOException {
        StateMap<K, N, S> stateMap = stateTable.getMapForKeyGroup(keyGroupId);
        int numEntries = 0;
        while (in.readBoolean()) {
            N namespace = namespaceSerializer.deserialize(in);
            K key = keySerializer.deserialize(in);
            S state = stateSerializer.deserialize(in);
            stateMap.put(key, namespace, state);
            numEntries++;
        }
        LOG.debug("read {} entries", numEntries);
    }

    private void readRemovals(
            DataInputView in,
            int keyGroupId,
            TypeSerializer<K> keySerializer,
            TypeSerializer<N> namespaceSerializer)
            throws IOException {
        int numRemovals = in.readInt();
        for (int i = 0; i < numRemovals; i++) {
            K key = checkNotNull(keySerializer.deserialize(in));
            N namespace = checkNotNull(namespaceSerializer.deserialize(in));
            StateMap<K, N, S> map = stateTable.getMapForKeyGroup(keyGroupId);
            if (map instanceof IncrementalCopyOnWriteStateMap) {
                ((IncrementalCopyOnWriteStateMap<K, N, S>) map).removeOnRecovery(key, namespace);
            } else {
                map.remove(key, namespace);
            }
        }
        LOG.debug("read {} removals", numRemovals);
    }
}
