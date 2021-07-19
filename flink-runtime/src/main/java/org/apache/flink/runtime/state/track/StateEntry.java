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

import org.apache.flink.runtime.state.StateObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Consumer;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * StateEntry holds the tracking information about the state to eventually discard it.
 *
 * <p>Lifecycle
 *
 * <ol>
 *   <li>Initially, the state is in active use, potentially by multiple state backends, as reflected
 *       by {@link #activelyUsingBackends}.
 *   <li>Once not actively used by a backend, the above counter is decremented and {@link
 *       #pendingCheckpointsByBackend} are updated
 *   <li>Each backend notifies it about the corresponding checkpoint updates (shrinking the above
 *       maps)
 *   <li>{@link #discardIfNotUsed()} Once no backend is actively using this entry, and no checkpoint
 *       is using it, the state is discarded
 * </ol>
 *
 * One can think of {@link #pendingCheckpointsByBackend} and {@link #activelyUsingBackends} as
 * potential usages in the past and in the future respectively.
 *
 * @param <K> type of state identifier
 */
@NotThreadSafe
class StateEntry<K> {
    private static final Logger LOG = LoggerFactory.getLogger(StateEntry.class);

    private final K key;
    private final StateObject state;
    private final TaskStateCleaner cleaner;
    private final Consumer<K> discardCallback;

    /** Number of state backends that may use this state in future checkpoints. */
    private int activelyUsingBackends;

    /**
     * Not yet subsumed checkpoints by backend potentially using this {@link #state}. Entries
     * (checkpoint IDs) for a backend are added once it stops using it and are removed on
     * subsumption and {@link #checkpointAborted(String, long) abortion}. Once it's empty and {@link
     * #activelyUsingBackends} is zero, {@link #state} can be discarded.
     *
     * <p>INVARIANT: does not contain empty sets or nulls.
     *
     * <p>Set instead of a single highest checkpoint ID is used to handle abortions; NavigableSet is
     * used to get this highest ID (a number would suffice at the cost of readability).
     */
    private final Map<String, NavigableSet<Long>> pendingCheckpointsByBackend = new HashMap<>();

    private boolean discarded = false;

    StateEntry(
            K key,
            StateObject state,
            int activelyUsingBackends,
            TaskStateCleaner cleaner,
            Consumer<K> discardCallback) {
        checkArgument(activelyUsingBackends > 0);
        this.key = checkNotNull(key);
        this.state = checkNotNull(state);
        this.cleaner = checkNotNull(cleaner);
        this.activelyUsingBackends = activelyUsingBackends;
        this.discardCallback = checkNotNull(discardCallback);
    }

    /**
     * Mark this entry as NOT used for future checkpoints by the given backend. Older checkpoints
     * may still use it.
     *
     * @param usingCheckpoints checkpoints that may be using this state - mutable for abortion
     */
    public void notActivelyUsed(String backendId, NavigableSet<Long> usingCheckpoints) {
        LOG.trace(
                "Update state entry usage, backendId: {}, usingCheckpoints: {}",
                backendId,
                usingCheckpoints);
        checkState(!discarded && --activelyUsingBackends >= 0);
        putOnceIfNonEmpty(pendingCheckpointsByBackend, usingCheckpoints, backendId);
        discardIfNotUsed();
    }

    private void discard() {
        if (discarded) {
            LOG.trace("Not discarding state entry - already discarded: {}", this);
            return;
        }
        discarded = true;
        discardCallback.accept(key);
        LOG.trace("Discarding state entry: {}", this);
        cleaner.discardAsync(state);
    }

    public void checkpointSubsumed(String backendId, long checkpointId) {
        // It's enough to simply clear remainingCheckpoints for this backend because it should
        // already track the highest checkpoint. But we'll check the remainingCheckpoints explicitly
        // - for robustness and clarity
        NavigableSet<Long> remainingCheckpoints = pendingCheckpointsByBackend.get(backendId);
        if (remainingCheckpoints != null
                && !remainingCheckpoints.isEmpty()
                && remainingCheckpoints.last() <= checkpointId) {
            pendingCheckpointsByBackend.remove(backendId);
            discardIfNotUsed();
        }
    }

    public void checkpointAborted(String backendId, long checkpointId) {
        Set<Long> remaining = pendingCheckpointsByBackend.get(backendId);
        if (remaining != null) {
            remaining.remove(checkpointId);
            discardIfNotUsed();
        }
    }

    private void discardIfNotUsed() {
        if (activelyUsingBackends == 0 && pendingCheckpointsByBackend.isEmpty()) {
            discard();
        }
    }

    @Nullable
    public Long getHighestUsingCheckpoint(String backendId) throws NoSuchElementException {
        return last(pendingCheckpointsByBackend.get(backendId));
    }

    public K getKey() {
        return key;
    }

    @Override
    public String toString() {
        return String.format(
                "key=%s, state=%s, discarded=%s, backendCount=%d",
                key, state, discarded, activelyUsingBackends);
    }

    private void putOnceIfNonEmpty(
            Map<String, NavigableSet<Long>> target, NavigableSet<Long> set, String backendId) {
        if (set != null && !set.isEmpty()) {
            NavigableSet<Long> prev = target.put(backendId, set);
            if (prev != null) {
                throw new RuntimeException(
                        String.format(
                                "Backend %s has already reported no active use of %s",
                                backendId, this));
            }
        }
    }

    @Nullable
    private static Long last(NavigableSet<Long> longs) {
        return longs == null || longs.isEmpty() ? null : longs.last();
    }
}
