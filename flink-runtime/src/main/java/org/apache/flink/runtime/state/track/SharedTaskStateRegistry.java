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

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.util.function.ThrowingConsumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.Function;

import static java.lang.Thread.holdsLock;
import static java.util.Collections.singleton;
import static org.apache.flink.runtime.state.track.SharedTaskStateRegistry.StateObjectIDExtractor.IDENTITY;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** @param <K> state object ID type (for {@link #distributedState}). */
@ThreadSafe
@Internal
public class SharedTaskStateRegistry<K> implements AutoCloseable {
    private static final Logger LOG = LoggerFactory.getLogger(SharedTaskStateRegistry.class);

    private final TaskStateCleaner cleaner;
    private final StateObjectIDExtractor<K> keyExtractor;

    // Synchronization is to prevent issues inside backend registries, not just Map modification.
    // E.g. backends may share State Entries and otherwise, would update them concurrently.
    @GuardedBy("lock")
    private final Map<String, BackendStateRegistry<K>> backendStateRegistries = new HashMap<>();

    @GuardedBy("lock")
    private final Set<K> usedState = new HashSet<>();

    // Synchronization is just to prevent concurrent Map modification issues.
    @GuardedBy("lock")
    private final Set<K> distributedState = new HashSet<>();

    private final Object lock = new Object();

    SharedTaskStateRegistry(TaskStateCleaner cleaner, StateObjectIDExtractor<K> keyExtractor) {
        this.cleaner = checkNotNull(cleaner);
        this.keyExtractor = checkNotNull(keyExtractor);
    }

    public void stateUsed(Set<String> backendIds, Collection<StateObject> states) {
        synchronized (lock) {
            stateUsedInternal(backendIds, states);
        }
    }

    private void stateUsedInternal(Set<String> backendIds, Collection<StateObject> states) {
        checkState(holdsLock(lock));
        List<StateEntry<K>> entries = toStateEntries(states, backendIds.size());
        if (!entries.isEmpty()) {
            for (StateEntry<K> e : entries) {
                usedState.add(e.getKey());
            }
            for (String backendId : backendIds) {
                withRegistry(backendId, registry -> registry.stateUsed(entries));
            }
        }
    }

    private List<StateEntry<K>> toStateEntries(Collection<StateObject> states, int numBackends) {
        checkState(holdsLock(lock));
        List<StateEntry<K>> entries = new ArrayList<>();
        for (StateObject stateObject : states) {
            for (Map.Entry<K, StateObject> entry : keyExtractor.apply(stateObject).entrySet()) {
                K stateKey = entry.getKey();
                StateObject state = entry.getValue();
                if (shouldTrack(stateKey)) {
                    entries.add(
                            new StateEntry<>(
                                    stateKey, state, numBackends, cleaner, usedState::remove));
                }
            }
        }
        return entries;
    }

    private boolean shouldTrack(K k) {
        return !usedState.contains(k) && !distributedState.contains(k);
    }

    void stateNotUsed(String backendId, StateObject state) {
        Set<K> keys = keyExtractor.apply(state).keySet();
        if (!keys.isEmpty()) {
            withRegistry(backendId, registry -> registry.stateNotUsed(keys));
        }
    }

    void checkpointStarting(String backendId, long checkpointId) {
        withRegistry(backendId, registry -> registry.checkpointStarting(checkpointId));
    }

    void stateSnapshotCreated(String backendId, StateObject state, long checkpointId) {
        withRegistry(
                backendId,
                registry -> {
                    if (checkpointId > registry.getLastSnapshottedCheckpoint()) {
                        stateUsedInternal(singleton(backendId), singleton(state));
                        registry.stateSnapshotCreated(
                                checkpointId, keyExtractor.apply(state).keySet());
                    }
                });
    }

    void checkpointSubsumed(String backendId, long checkpointId) {
        withRegistry(backendId, registry -> registry.checkpointSubsumed(checkpointId));
    }

    void checkpointAborted(String backendId, long checkpointId) {
        withRegistry(backendId, registry -> registry.checkpointAborted(checkpointId));
    }

    private void closeForBackend(String backendId) {
        withRegistry(backendId, BackendStateRegistry::close);
        backendStateRegistries.remove(backendId);
    }

    public void close() throws Exception {
        LOG.debug("Close, num backendStateRegistries: {}", backendStateRegistries.size());
        synchronized (lock) {
            backendStateRegistries.values().forEach(BackendStateRegistry::close);
            backendStateRegistries.clear();
            distributedState.clear();
        }
        cleaner.close();
    }

    public TaskStateRegistry forBackend(String backendId) {
        LOG.debug("Create TaskStateRegistry for backend {}", backendId);
        checkArgument(!backendStateRegistries.containsKey(backendId));
        return new TaskStateRegistry() {
            @Override
            public void stateUsed(Collection<StateObject> states) {
                SharedTaskStateRegistry.this.stateUsed(singleton(backendId), states);
            }

            @Override
            public void stateNotUsed(StateObject state) {
                SharedTaskStateRegistry.this.stateNotUsed(backendId, state);
            }

            @Override
            public void checkpointStarting(long checkpointId) {
                SharedTaskStateRegistry.this.checkpointStarting(backendId, checkpointId);
            }

            @Override
            public void stateSnapshotCreated(StateObject state, long checkpointId)
                    throws NoSuchElementException, IllegalStateException {
                SharedTaskStateRegistry.this.stateSnapshotCreated(backendId, state, checkpointId);
            }

            @Override
            public void checkpointAborted(long checkpointId) {
                SharedTaskStateRegistry.this.checkpointAborted(backendId, checkpointId);
            }

            @Override
            public void checkpointSubsumed(long checkpointId) {
                SharedTaskStateRegistry.this.checkpointSubsumed(backendId, checkpointId);
            }

            @Override
            public void close() {
                SharedTaskStateRegistry.this.closeForBackend(backendId);
            }
        };
    }

    // todo: move to interface? (when rebased)
    public void addDistributedState(Set<K> distributedState) {
        synchronized (lock) {
            this.distributedState.addAll(distributedState);
        }
    }

    private <E extends RuntimeException> void withRegistry(
            String backendId, ThrowingConsumer<BackendStateRegistry<K>, E> action) throws E {
        checkArgument(backendId != null && !backendId.isEmpty());
        synchronized (lock) {
            BackendStateRegistry<K> backendStateRegistry =
                    backendStateRegistries.computeIfAbsent(
                            backendId, ign -> new BackendStateRegistry<>(backendId));
            action.accept(backendStateRegistry);
        }
    }

    /** The returned map should be modifiable. */
    @ThreadSafe
    public interface StateObjectIDExtractor<T> extends Function<StateObject, Map<T, StateObject>> {
        StateObjectIDExtractor<StateObject> IDENTITY =
                stateObject -> {
                    Map<StateObject, StateObject> map = new IdentityHashMap<>();
                    map.put(stateObject, stateObject);
                    return map;
                };
    }

    public static SharedTaskStateRegistry<?> create(Executor ioExecutor) {
        /* todo: replace IDENTITY with a real implementation after FLINK-23342 merged */
        return new SharedTaskStateRegistry<>(TaskStateCleaner.create(ioExecutor), IDENTITY);
    }
}
