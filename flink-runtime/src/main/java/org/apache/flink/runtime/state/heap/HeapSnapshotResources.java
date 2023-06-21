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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.CollectionUtil;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A set of resources required to take a checkpoint or savepoint from a {@link
 * HeapKeyedStateBackend}.
 */
@Internal
final class HeapSnapshotResources<K> implements FullSnapshotResources<K> {
    private final List<StateMetaInfoSnapshot> metaInfoSnapshots;
    private final Map<StateUID, StateSnapshot> cowStateStableSnapshots;
    private final StreamCompressionDecorator streamCompressionDecorator;
    private final Map<StateUID, Integer> stateNamesToId;
    private final KeyGroupRange keyGroupRange;
    private final TypeSerializer<K> keySerializer;
    private final int totalKeyGroups;

    private HeapSnapshotResources(
            List<StateMetaInfoSnapshot> metaInfoSnapshots,
            Map<StateUID, StateSnapshot> cowStateStableSnapshots,
            StreamCompressionDecorator streamCompressionDecorator,
            Map<StateUID, Integer> stateNamesToId,
            KeyGroupRange keyGroupRange,
            TypeSerializer<K> keySerializer,
            int totalKeyGroups) {
        this.metaInfoSnapshots = metaInfoSnapshots;
        this.cowStateStableSnapshots = cowStateStableSnapshots;
        this.streamCompressionDecorator = streamCompressionDecorator;
        this.stateNamesToId = stateNamesToId;
        this.keyGroupRange = keyGroupRange;
        this.keySerializer = keySerializer;
        this.totalKeyGroups = totalKeyGroups;
    }

    public static <K> HeapSnapshotResources<K> create(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            StreamCompressionDecorator streamCompressionDecorator,
            KeyGroupRange keyGroupRange,
            TypeSerializer<K> keySerializer,
            int totalKeyGroups) {

        if (registeredKVStates.isEmpty() && registeredPQStates.isEmpty()) {
            return new HeapSnapshotResources<>(
                    Collections.emptyList(),
                    Collections.emptyMap(),
                    streamCompressionDecorator,
                    Collections.emptyMap(),
                    keyGroupRange,
                    keySerializer,
                    totalKeyGroups);
        }

        int numStates = registeredKVStates.size() + registeredPQStates.size();

        Preconditions.checkState(
                numStates <= Short.MAX_VALUE,
                "Too many states: "
                        + numStates
                        + ". Currently at most "
                        + Short.MAX_VALUE
                        + " states are supported");

        final List<StateMetaInfoSnapshot> metaInfoSnapshots = new ArrayList<>(numStates);
        final Map<StateUID, Integer> stateNamesToId =
                CollectionUtil.newHashMapWithExpectedSize(numStates);
        final Map<StateUID, StateSnapshot> cowStateStableSnapshots =
                CollectionUtil.newHashMapWithExpectedSize(numStates);

        processSnapshotMetaInfoForAllStates(
                metaInfoSnapshots,
                cowStateStableSnapshots,
                stateNamesToId,
                registeredKVStates,
                StateMetaInfoSnapshot.BackendStateType.KEY_VALUE);

        processSnapshotMetaInfoForAllStates(
                metaInfoSnapshots,
                cowStateStableSnapshots,
                stateNamesToId,
                registeredPQStates,
                StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE);

        return new HeapSnapshotResources<>(
                metaInfoSnapshots,
                cowStateStableSnapshots,
                streamCompressionDecorator,
                stateNamesToId,
                keyGroupRange,
                keySerializer,
                totalKeyGroups);
    }

    private static void processSnapshotMetaInfoForAllStates(
            List<StateMetaInfoSnapshot> metaInfoSnapshots,
            Map<StateUID, StateSnapshot> cowStateStableSnapshots,
            Map<StateUID, Integer> stateNamesToId,
            Map<String, ? extends StateSnapshotRestore> registeredStates,
            StateMetaInfoSnapshot.BackendStateType stateType) {

        for (Map.Entry<String, ? extends StateSnapshotRestore> kvState :
                registeredStates.entrySet()) {
            final StateUID stateUid = StateUID.of(kvState.getKey(), stateType);
            stateNamesToId.put(stateUid, stateNamesToId.size());
            StateSnapshotRestore state = kvState.getValue();
            if (null != state) {
                final StateSnapshot stateSnapshot = state.stateSnapshot();
                metaInfoSnapshots.add(stateSnapshot.getMetaInfoSnapshot());
                cowStateStableSnapshots.put(stateUid, stateSnapshot);
            }
        }
    }

    @Override
    public void release() {
        for (StateSnapshot stateSnapshot : cowStateStableSnapshots.values()) {
            stateSnapshot.release();
        }
    }

    public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
        return metaInfoSnapshots;
    }

    @Override
    public KeyValueStateIterator createKVStateIterator() throws IOException {
        return new HeapKeyValueStateIterator(
                keyGroupRange,
                keySerializer,
                totalKeyGroups,
                stateNamesToId,
                cowStateStableSnapshots);
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public StreamCompressionDecorator getStreamCompressionDecorator() {
        return streamCompressionDecorator;
    }

    public Map<StateUID, StateSnapshot> getCowStateStableSnapshots() {
        return cowStateStableSnapshots;
    }

    public Map<StateUID, Integer> getStateNamesToId() {
        return stateNamesToId;
    }
}
