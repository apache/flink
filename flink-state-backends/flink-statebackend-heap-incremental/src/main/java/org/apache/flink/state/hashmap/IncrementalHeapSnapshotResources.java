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
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.HeapSnapshotResourcesBase;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.runtime.state.heap.StateUID;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType.KEY_VALUE;
import static org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType.PRIORITY_QUEUE;

final class IncrementalHeapSnapshotResources<K> extends HeapSnapshotResourcesBase<K>
        implements SnapshotResources {
    private final Map<String, Map<Integer, Integer>> currentMapVersions;
    private final IncrementalSnapshot snapshotBase;

    IncrementalHeapSnapshotResources(
            List<StateMetaInfoSnapshot> metaInfoSnapshots,
            Map<StateUID, StateSnapshot> cowStateStableSnapshots,
            Map<StateUID, Integer> stateNamesToId,
            TypeSerializer<K> keySerializer,
            Map<String, Map<Integer, Integer>> currentMapVersions,
            IncrementalSnapshot snapshotBase) {
        super(metaInfoSnapshots, cowStateStableSnapshots, stateNamesToId, keySerializer);
        this.currentMapVersions = currentMapVersions;
        this.snapshotBase = snapshotBase;
    }

    public static <K> IncrementalHeapSnapshotResources<K> create(
            IncrementalSnapshotTracker snapshotTracker,
            Map<String, StateTable<K, ?, ?>> kvStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> pqStates,
            TypeSerializer<K> keySerializer) {

        List<StateMetaInfoSnapshot> metaInfoSnapshots = new ArrayList<>();
        Map<StateUID, StateSnapshot> snapshots = new HashMap<>();
        Map<StateUID, Integer> stateIds = new HashMap<>();
        Map<String, Map<Integer, Integer>> currentMapVersions = new HashMap<>();

        for (Map.Entry<String, StateTable<K, ?, ?>> e : kvStates.entrySet()) {
            StateUID uid = StateUID.of(e.getKey(), KEY_VALUE);
            StateTable<K, ?, ?> state = e.getValue();
            // WARN: this is the current map version; it is updated during the snapshot
            // DO NOT REORDER
            if (state instanceof IncrementalCopyOnWriteStateTable) {
                currentMapVersions.put(
                        e.getKey(),
                        ((IncrementalCopyOnWriteStateTable<?, ?, ?>) state).getMapVersions());
            }
            snapshot(state, uid, metaInfoSnapshots, snapshots, stateIds);
        }

        for (Map.Entry<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> e :
                pqStates.entrySet()) {
            snapshot(
                    e.getValue(),
                    StateUID.of(e.getKey(), PRIORITY_QUEUE),
                    metaInfoSnapshots,
                    snapshots,
                    stateIds);
        }

        return new IncrementalHeapSnapshotResources<>(
                metaInfoSnapshots,
                snapshots,
                stateIds,
                keySerializer,
                currentMapVersions,
                snapshotTracker.getCurrentBase());
    }

    private static void snapshot(
            StateSnapshotRestore state,
            StateUID stateUID,
            List<StateMetaInfoSnapshot> metaInfoSnapshots,
            Map<StateUID, StateSnapshot> snapshots,
            Map<StateUID, Integer> ids) {
        StateSnapshot snapshot = state.stateSnapshot();
        metaInfoSnapshots.add(snapshot.getMetaInfoSnapshot());
        ids.put(stateUID, ids.size());
        snapshots.put(stateUID, snapshot);
    }

    public Map<String, Map<Integer, Integer>> getCurrentMapVersions() {
        return currentMapVersions;
    }

    public IncrementalSnapshot getSnapshotBase() {
        return snapshotBase;
    }

    public List<Runnable> getConfirmCallbacks() {
        List<Runnable> result = new ArrayList<>();
        for (StateSnapshot snapshot : cowStateStableSnapshots.values()) {
            if (snapshot instanceof IncrementalCopyOnWriteStateTableSnapshot) {
                result.addAll(
                        ((IncrementalCopyOnWriteStateTableSnapshot<?, ?, ?>) snapshot)
                                .getConfirmCallbacks());
            }
        }
        return result;
    }
}
