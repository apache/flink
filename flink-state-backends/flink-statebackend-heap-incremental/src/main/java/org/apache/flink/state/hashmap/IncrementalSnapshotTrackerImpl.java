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

import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.util.Collections.emptyMap;
import static org.apache.flink.util.Preconditions.checkState;

class IncrementalSnapshotTrackerImpl implements IncrementalSnapshotTracker {
    private static final Logger LOG = LoggerFactory.getLogger(IncrementalSnapshotTrackerImpl.class);

    private final NavigableMap<Long, UnconfirmedSnapshot> unconfirmedSnapshots = new TreeMap<>();
    private IncrementalSnapshot confirmed;
    private long lastConfirmedCheckpointID = -1L;
    private final StateHandleHelper stateHandleHelper;

    public IncrementalSnapshotTrackerImpl(
            IncrementalKeyedStateHandle initialState, StateHandleHelper stateHandleHelper) {
        this.confirmed =
                new IncrementalSnapshot(
                        emptyMap(),
                        SnapshotResult.of(initialState),
                        initialState.getCheckpointId());
        this.stateHandleHelper = stateHandleHelper; // NOTE: fence
    }

    @Override
    public synchronized IncrementalSnapshot getCurrentBase() {
        // Base on unconfirmed snapshot to avoid skipping checkpoints and allowing their state to be
        // discarded by JM as unused.
        // The unconfirmed state can not be discarded by JM (FLINK-24611), but it still can be
        // discarded if e.g. sending to JM fails.
        // TODO: prevent discarding uploaded state, or re-upload unconfirmed, or prevent removing
        // skipped state e.g. by including it but not using in the current checkpoint
        IncrementalSnapshot candidate =
                unconfirmedSnapshots.isEmpty()
                        ? confirmed
                        : unconfirmedSnapshots.lastEntry().getValue().snapshot;
        return candidate.getCheckpointID() <= lastConfirmedCheckpointID ? confirmed : candidate;
    }

    @Override
    public synchronized void track(
            long checkpointId, IncrementalSnapshot snapshot, List<Runnable> confirmCallbacks) {
        LOG.debug("track checkpoint {} snapshot {}", checkpointId, snapshot);
        checkState(
                !unconfirmedSnapshots.containsKey(checkpointId),
                "Checkpoint %s is already tracked",
                checkpointId);
        unconfirmedSnapshots.put(
                checkpointId,
                new UnconfirmedSnapshot(
                        new IncrementalSnapshot(
                                snapshot.getAllVersions(),
                                snapshot.getStateSnapshot(),
                                snapshot.getCheckpointID()),
                        confirmCallbacks));
    }

    @Override
    public void trackFullSnapshot(
            SnapshotResult<KeyedStateHandle> materializedSnapshot,
            IncrementalSnapshot.Versions versions) {
        long checkpointID =
                Math.max(
                        lastConfirmedCheckpointID,
                        unconfirmedSnapshots.isEmpty()
                                ? Long.MIN_VALUE
                                : unconfirmedSnapshots.lastKey());
        LOG.debug(
                "received full snapshot - making a base checkpoint {} (last confirmed checkpoint ID: {})",
                checkpointID,
                lastConfirmedCheckpointID);
        confirmed =
                new IncrementalSnapshot(
                        versions,
                        stateHandleHelper.asIncremental(materializedSnapshot, checkpointID),
                        checkpointID);
        // todo: split confirm() into release() and asConfirmed()
        unconfirmedSnapshots.values().forEach(snapshot -> snapshot.confirm(stateHandleHelper));
        unconfirmedSnapshots.clear();
    }

    @Override
    public synchronized void confirmSnapshot(long checkpointId) {
        if (lastConfirmedCheckpointID >= checkpointId) {
            return;
        }
        UnconfirmedSnapshot unconfirmedSnapshot = unconfirmedSnapshots.remove(checkpointId);
        if (unconfirmedSnapshot == null) {
            LOG.info(
                    "Confirmed checkpoint not found: {} (materialized? unconfirmed checkpoints: {})",
                    checkpointId,
                    unconfirmedSnapshots.keySet());
            return;
        }
        LOG.debug(
                "confirm snapshot {}, last confirmed: {}", checkpointId, lastConfirmedCheckpointID);
        confirmed = unconfirmedSnapshot.confirm(stateHandleHelper);
        lastConfirmedCheckpointID = checkpointId;
        unconfirmedSnapshots.headMap(checkpointId, false).clear();
    }

    private static class UnconfirmedSnapshot {
        private final IncrementalSnapshot snapshot;
        private final List<Runnable> confirmCallbacks;

        private UnconfirmedSnapshot(IncrementalSnapshot snapshot, List<Runnable> confirmCallbacks) {
            this.snapshot = snapshot;
            this.confirmCallbacks = confirmCallbacks;
        }

        /** Cleanup removal log, replace handles with placeholders. */
        public IncrementalSnapshot confirm(StateHandleHelper stateHandleHelper) {
            for (Runnable runnable : confirmCallbacks) {
                runnable.run();
            }
            return new IncrementalSnapshot(
                    snapshot.getAllVersions(),
                    stateHandleHelper.rebuildWithPlaceHolders(snapshot.getStateSnapshot()),
                    snapshot.getCheckpointID());
        }
    }
}
