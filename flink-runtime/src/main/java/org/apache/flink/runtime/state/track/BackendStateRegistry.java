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

package org.apache.flink.runtime.state.track;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A per-StateBackend StateRegistry. Tracks the usage of {@link StateEntry} by backend and its
 * snapshots. {@link StateEntry} will discard its state once all such registries are not using it.
 *
 * <p>Explicitly aware of checkpointing so that tracking of associations between checkpoints and
 * snapshots can be reused (instead of implementing in each backend).
 *
 * <h1>Tracking lifecycle</h1>
 *
 * <ol>
 *   <li>Upon {@link #stateUsed(Collection) registration}, {@link StateEntry} is added to {@link
 *       #currentState}
 *   <li>Once {@link #stateNotUsed(Set) not actively used} (usually after materialization), it is
 *       moved to {@link #previousStatesByHighestUsedCheckpoint} (if used by checkpoints; otherwise
 *       this step is skipped). At this point, <strong>any pending checkpoint is considered as
 *       potentially using the state</strong>
 *   <li>Once all checkpoint potentially using it are {@link #checkpointSubsumed(long) subsumed},
 *       tracking stops (i.e. it is removed from {@link #previousStatesByHighestUsedCheckpoint}).
 * </ol>
 *
 * On each step, the entry is notified about the event. When tracking stops it might decide to
 * discard the state.
 *
 * <h1>Checkpoint lifecycle</h1>
 *
 * Upon {@link #checkpointStarting(long) start}, every checkpoint first updates {@link
 * #lastStartedCheckpoint} and is added {@link #notSubsumedCheckpoints}.
 *
 * <h1>Thread safety</h1>
 *
 * The class is not thread-safe. Furthermore, it uses {@link StateEntry} potentially shared with
 * other instances used by other threads - so external synchronization required.
 *
 * @param <K> type of state identifier
 */
@NotThreadSafe
class BackendStateRegistry<K> {
    private final Logger LOG = LoggerFactory.getLogger(BackendStateRegistry.class);

    private final String backendId;

    private long lastStartedCheckpoint = -1L;
    private final NavigableSet<Long> notSubsumedCheckpoints = new TreeSet<>();

    private long lastSnapshottedCheckpoint = -1L;
    @Nullable private Set<K> lastSnapshot = null;

    private final Map<K, StateEntry<K>> currentState = new HashMap<>();
    private final NavigableMap<Long, List<StateEntry<K>>> previousStatesByHighestUsedCheckpoint =
            new TreeMap<>();

    public BackendStateRegistry(String backendId) {
        this.backendId = backendId;
    }

    public void stateUsed(Collection<StateEntry<K>> entries) {
        LOG.debug("State used, backend: {}, state: {}", backendId, entries);
        entries.forEach(e -> currentState.put(e.getKey(), e));
    }

    public void stateNotUsed(Set<K> stateIDs) {
        stateNotUsedInternal(stateIDs, notSubsumedCheckpoints);
    }

    private void stateNotUsedInternal(Set<K> stateIDs, NavigableSet<Long> pendingCheckpoints) {
        for (K key : stateIDs) {
            StateEntry<K> entry = currentState.remove(key);
            if (entry != null) {
                entry.notActivelyUsed(backendId, new TreeSet<>(pendingCheckpoints));
                trackNotActivelyUsedEntry(entry);
            }
        }
    }

    private void trackNotActivelyUsedEntry(StateEntry<K> entry) {
        Long highestUsingCheckpoint = entry.getHighestUsingCheckpoint(backendId);
        if (highestUsingCheckpoint == null) {
            LOG.debug("State entry not used, backend: {}, state: {}", backendId, entry);
        } else {
            previousStatesByHighestUsedCheckpoint
                    .computeIfAbsent(highestUsingCheckpoint, ign -> new ArrayList<>())
                    .add(entry);
            LOG.debug(
                    "State entry not used but {} pending checkpoints exist, backend: {}",
                    notSubsumedCheckpoints.size(),
                    backendId);
        }
    }

    public void checkpointStarting(long checkpointId) {
        LOG.debug("Checkpoint started, backend: {}, checkpoint: {}", backendId, checkpointId);
        checkState(
                lastStartedCheckpoint < checkpointId,
                "Out of order checkpoint: %s, backend: %s, previous: %s",
                checkpointId,
                backendId,
                lastStartedCheckpoint);
        notSubsumedCheckpoints.add(checkpointId);
        lastStartedCheckpoint = checkpointId;
    }

    public void stateSnapshotCreated(long checkpointId, Set<K> newStateKeys) {
        LOG.debug(
                "Checkpoint performed, backend: {}, checkpoint: {}, state objects: {}",
                backendId,
                checkpointId,
                newStateKeys);
        if (notSubsumedCheckpoints.contains(checkpointId)) {
            // note that we don't remove this checkpoint from pending for now - wait
            // until subsumed
            snapshotSentForCheckpoint(checkpointId, newStateKeys);
        } else {
            throw new IllegalStateException(
                    String.format("Unknown checkpoint %d for backend %s", checkpointId, backendId));
        }
    }

    private void snapshotSentForCheckpoint(long checkpointId, Set<K> newStateKeys) {
        if (lastSnapshottedCheckpoint < checkpointId) {
            if (lastSnapshot != null) {
                lastSnapshot.removeAll(newStateKeys);
                stateNotUsedInternal(
                        lastSnapshot, notSubsumedCheckpoints.headSet(checkpointId, false));
            }
            lastSnapshot = newStateKeys;
            lastSnapshottedCheckpoint = checkpointId;
        } else if (lastSnapshottedCheckpoint > checkpointId) {
            LOG.warn(
                    "Out of order snapshot (ignoring), checkpointId: {}, latest: {}, backendId: {}",
                    checkpointId,
                    lastSnapshottedCheckpoint,
                    backendId);
        } else {
            throw new IllegalStateException(
                    String.format(
                            "Backend %s reported checkpoint %d twice", backendId, checkpointId));
        }
    }

    public void checkpointSubsumed(long checkpointId) {
        LOG.debug("Checkpoint subsumed, backend: {}, checkpoint: {}", backendId, checkpointId);
        notSubsumedCheckpoints.headSet(checkpointId, true).clear();
        Collection<List<StateEntry<K>>> subsumed =
                previousStatesByHighestUsedCheckpoint.headMap(checkpointId, true).values();
        for (List<StateEntry<K>> entries : subsumed) {
            for (StateEntry<K> entry : entries) {
                entry.checkpointSubsumed(backendId, checkpointId);
            }
        }
        subsumed.clear();
    }

    public void checkpointAborted(long checkpointId) {
        if (notSubsumedCheckpoints.remove(checkpointId)) {
            LOG.debug("Checkpoint aborted, backend: {}, checkpoint: {}", backendId, checkpointId);
            for (List<StateEntry<K>> v :
                    previousStatesByHighestUsedCheckpoint.headMap(checkpointId, true).values()) {
                for (StateEntry<K> entry : v) {
                    entry.checkpointAborted(backendId, checkpointId);
                }
            }
        } else {
            LOG.debug(
                    "Unknown checkpoint aborted, backend: {}, checkpoint: {} (probably subsumed)",
                    backendId,
                    checkpointId);
        }
    }

    public void close() {
        LOG.debug("Close, backendId: {}", backendId);
        // State of completed checkpoints is discarded on JM if needed (e.g. job cancellation). All
        // pending checkpoints in TM are considered completed because the completion notification
        // might be missing (the only exception is currently stop-with-savepoint when
        // it's guaranteed to be received). So we only need to discard the state not used in any
        // checkpoints:
        stateNotUsed(new HashSet<>(currentState.keySet()));
        notSubsumedCheckpoints.clear();
        currentState.clear();
        previousStatesByHighestUsedCheckpoint.clear();
        lastSnapshot = null;
    }

    long getLastSnapshottedCheckpoint() {
        return lastSnapshottedCheckpoint;
    }
}
