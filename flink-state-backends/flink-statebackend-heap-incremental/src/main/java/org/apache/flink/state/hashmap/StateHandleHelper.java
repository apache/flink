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

import org.apache.flink.runtime.state.CheckpointBoundKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static java.util.Collections.emptyMap;
import static java.util.Comparator.comparing;
import static java.util.stream.Collectors.toList;

// todo: local recovery
// invariant: base state after init() can only contain IncrementalKeyedStateHandle
// invariant: IncrementalKeyedStateHandle always contains keys prefixed with CP ID (for ordering)
class StateHandleHelper {
    private static final Logger LOG = LoggerFactory.getLogger(StateHandleHelper.class);

    private final UUID backendIdentifier;
    private final KeyGroupRange keyGroupRange;

    StateHandleHelper(KeyGroupRange keyGroupRange, Collection<KeyedStateHandle> stateHandles) {
        this(extractBackendIdentifier(stateHandles), keyGroupRange);
    }

    StateHandleHelper(UUID backendIdentifier, KeyGroupRange keyGroupRange) {
        this.keyGroupRange = keyGroupRange;
        this.backendIdentifier = backendIdentifier;
    }

    /**
     * Transform the initial state after restore into a {@link SnapshotResult} that can be used as a
     * base for the next (incremental) checkpoints and for recovery (after {@link
     * #flattenForRecovery(IncrementalKeyedStateHandle)} flattening}).
     */
    public IncrementalKeyedStateHandle init(Collection<KeyedStateHandle> stateHandles) {
        Map<StateHandleID, StreamStateHandle> sharedState = new HashMap<>();
        long checkpointId = 0L;
        // multiple handles in case of down-scaling
        for (KeyedStateHandle handle : stateHandles) {
            // todo: on migration, initial checkpoint can be subsumed (in CLAIM mode) and its state
            // discarded (similar to FLINK-25872)
            if (handle instanceof IncrementalKeyedStateHandle) {
                addIncrementalState(sharedState, handle); // already incremental
            } else if (handle instanceof KeyGroupsStateHandle) {
                addNonIncrementalState(sharedState, handle, 0L); // migration from non-incremental
            } else if (handle != null) {
                throw new RuntimeException("Unexpected state type: " + handle.getClass());
            }
            checkpointId =
                    Math.max(
                            checkpointId,
                            handle instanceof CheckpointBoundKeyedStateHandle
                                    ? ((CheckpointBoundKeyedStateHandle) handle).getCheckpointId()
                                    : checkpointId);
        }
        LOG.info(
                "initialized state with {} handles originally from {} handles and max checkpoint {}; key group range: {}, backend: {}",
                sharedState.size(),
                stateHandles.size(),
                checkpointId,
                keyGroupRange,
                backendIdentifier);
        return new IncrementalRemoteKeyedStateHandle(
                backendIdentifier,
                keyGroupRange,
                checkpointId,
                sharedState,
                emptyMap(), // not used
                new ByteStreamStateHandle("empty", new byte[0]), // not used
                0);
    }

    /** Package old and new state snapshots into a single {@link SnapshotResult}. */
    public SnapshotResult<KeyedStateHandle> combine(
            SnapshotResult<KeyedStateHandle> baseState,
            long oldCheckpointID,
            SnapshotResult<KeyedStateHandle> newState,
            long newCheckpointID) {
        LOG.info(
                "base checkpoint {} on checkpoint {}, base state size: {}, new state size: {}",
                newCheckpointID,
                oldCheckpointID,
                baseState.getStateSize(),
                newState.getStateSize());

        if (baseState.isEmpty() && newState.isEmpty()) {
            LOG.debug("combined state is empty, checkpoint {}", newCheckpointID);
            return baseState;
        }
        Map<StateHandleID, StreamStateHandle> jmState = new HashMap<>();
        addIncrementalState(jmState, baseState.getJobManagerOwnedSnapshot());
        addNonIncrementalState(jmState, newState.getJobManagerOwnedSnapshot(), newCheckpointID);
        SnapshotResult<KeyedStateHandle> result =
                toSnapshotResult(jmState, newCheckpointID, newState.getStateSize());
        LOG.debug(
                "combined state size of checkpoint {}: {} ({} handles), base state size: {}, new state size: {}",
                newCheckpointID,
                result.getStateSize(),
                jmState.size(),
                baseState.getStateSize(),
                newState.getStateSize());
        return result;
    }

    private void addNonIncrementalState(
            Map<StateHandleID, StreamStateHandle> states,
            @Nullable KeyedStateHandle state,
            long checkpointId) {
        if (state != null) {
            StateHandleID id = buildStateID(checkpointId, state);
            LOG.trace(
                    "add non incremental state, checkpoint: {}, id: {}, state: {}, states: {}",
                    checkpointId,
                    id,
                    state,
                    states);
            states.put(id, (KeyGroupsStateHandle) state);
        }
    }

    private void addIncrementalState(
            Map<StateHandleID, StreamStateHandle> states, @Nullable KeyedStateHandle state) {
        if (state != null) {
            states.putAll(((IncrementalKeyedStateHandle) state).getSharedStateHandles());
        }
    }

    private SnapshotResult<KeyedStateHandle> toSnapshotResult(
            Map<StateHandleID, StreamStateHandle> sharedState,
            long checkpointId,
            long newStateSize) {
        return sharedState.isEmpty()
                ? SnapshotResult.empty()
                : SnapshotResult.of(
                        new IncrementalRemoteKeyedStateHandle(
                                backendIdentifier,
                                keyGroupRange,
                                checkpointId,
                                sharedState,
                                emptyMap(), // not used
                                new ByteStreamStateHandle("empty", new byte[0]), // not used
                                newStateSize));
    }

    Collection<KeyedStateHandle> flattenForRecovery(IncrementalKeyedStateHandle stateHandles) {
        return stateHandles.getSharedStateHandles().entrySet().stream()
                .sorted(comparing(e -> e.getKey().getKeyString()))
                .map(handle -> (KeyGroupsStateHandle) handle.getValue())
                .collect(toList());
    }

    private StateHandleID buildStateID(long checkpointId, KeyedStateHandle newKeyedState) {
        // handle down-scaling, allow sort by checkpoint ID
        return new StateHandleID(
                String.format(
                        "%06d-%s", checkpointId, newKeyedState.getStateHandleId().getKeyString()));
    }

    public SnapshotResult<KeyedStateHandle> rebuildWithPlaceHolders(
            SnapshotResult<KeyedStateHandle> stateSnapshot) {
        if (stateSnapshot.isEmpty()) {
            return stateSnapshot;
        }
        IncrementalKeyedStateHandle jmState =
                (IncrementalKeyedStateHandle) stateSnapshot.getJobManagerOwnedSnapshot();

        Map<StateHandleID, StreamStateHandle> sharedStateHandles = new HashMap<>();
        for (Map.Entry<StateHandleID, StreamStateHandle> e :
                jmState.getSharedStateHandles().entrySet()) {
            LOG.debug("replace shared state with placeholder, key: {}", e.getKey());
            sharedStateHandles.put(
                    e.getKey(), new PlaceholderStreamStateHandle(e.getValue().getStateSize()));
        }
        return toSnapshotResult(sharedStateHandles, jmState.getCheckpointId(), 0);
    }

    private static UUID extractBackendIdentifier(Collection<KeyedStateHandle> stateHandles) {
        // TODO: IncrementalKeyedStateHandle doesn't support re-scaling well because the key it uses
        // for shared state registration is composed from: 1) shared state ID; 2) backend ID; 3) key
        // range. The latter two might change on re-scaling, rendering the old registration as
        // unused and deleted.
        return stateHandles.stream()
                .filter(h -> h instanceof IncrementalKeyedStateHandle)
                .map(h -> ((IncrementalKeyedStateHandle) h).getBackendIdentifier())
                .findAny()
                .orElse(UUID.randomUUID());
    }

    SnapshotResult<KeyedStateHandle> asIncremental(
            SnapshotResult<KeyedStateHandle> snapshotResult, long checkpointID) {
        KeyedStateHandle state = snapshotResult.getJobManagerOwnedSnapshot();
        Map<StateHandleID, StreamStateHandle> jmState = new HashMap<>();
        if (state == null) {
            return SnapshotResult.empty();
        } else if (state instanceof IncrementalKeyedStateHandle) {
            addIncrementalState(jmState, state);
        } else {
            addNonIncrementalState(jmState, state, checkpointID);
        }
        return toSnapshotResult(jmState, checkpointID, state.getStateSize());
    }
}
