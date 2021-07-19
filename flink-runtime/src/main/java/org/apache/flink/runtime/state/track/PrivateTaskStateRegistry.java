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
import org.apache.flink.runtime.state.track.SharedTaskStateRegistry.StateObjectIDExtractor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;

import static java.util.Collections.singleton;
import static org.apache.flink.runtime.state.track.SharedTaskStateRegistry.StateObjectIDExtractor.IDENTITY;
import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A thin wrapper around {@link BackendStateRegistry} to provide single-backend {@link
 * TaskStateRegistry} implementation.
 *
 * @param <K> state object ID type (for {@link #distributedState}).
 */
@NotThreadSafe
@Internal
class PrivateTaskStateRegistry<K> implements TaskStateRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(PrivateTaskStateRegistry.class);

    private final TaskStateCleaner cleaner;
    private final StateObjectIDExtractor<K> keyExtractor;

    private final Set<K> distributedState = new HashSet<>();
    private final String backendId;
    private final BackendStateRegistry<K> registry;

    PrivateTaskStateRegistry(
            String backendId, TaskStateCleaner cleaner, StateObjectIDExtractor<K> keyExtractor) {
        checkArgument(backendId != null && !backendId.isEmpty());
        this.cleaner = checkNotNull(cleaner);
        this.keyExtractor = checkNotNull(keyExtractor);
        this.backendId = backendId;
        this.registry = new BackendStateRegistry<>(backendId);
    }

    @Override
    public void stateUsed(Collection<StateObject> states) {
        stateUsedInternal(states);
    }

    @Override
    public void stateNotUsed(StateObject state) {
        registry.stateNotUsed(keyExtractor.apply(state).keySet());
    }

    @Override
    public void checkpointStarting(long checkpointId) {
        registry.checkpointStarting(checkpointId);
    }

    @Override
    public void stateSnapshotCreated(StateObject state, long checkpointId) {
        if (checkpointId > registry.getLastSnapshottedCheckpoint()) {
            stateUsedInternal(singleton(state));
            registry.stateSnapshotCreated(checkpointId, keyExtractor.apply(state).keySet());
        }
    }

    @Override
    public void checkpointSubsumed(long checkpointId) {
        registry.checkpointSubsumed(checkpointId);
    }

    @Override
    public void checkpointAborted(long checkpointId) {
        registry.checkpointAborted(checkpointId);
    }

    @Override
    public void close() throws Exception {
        LOG.debug("Close, backendId: {}", backendId);
        distributedState.clear();
        registry.close();
        cleaner.close();
    }

    // todo: move to interface? (when rebased)
    public void addDistributedState(Set<K> distributedState) {
        this.distributedState.addAll(distributedState);
    }

    public static PrivateTaskStateRegistry<?> create(String backendId, Executor ioExecutor) {
        /* todo: replace IDENTITY with a real implementation after FLINK-23342 merged */
        return new PrivateTaskStateRegistry<>(
                backendId, TaskStateCleaner.create(ioExecutor), IDENTITY);
    }

    private void stateUsedInternal(Collection<StateObject> states) {
        List<StateEntry<K>> entries = new ArrayList<>();
        for (StateObject stateObject : states) {
            for (Map.Entry<K, StateObject> entry : keyExtractor.apply(stateObject).entrySet()) {
                K stateKey = entry.getKey();
                StateObject state = entry.getValue();
                if (!distributedState.contains(stateKey)) {
                    entries.add(new StateEntry<>(stateKey, state, 1, cleaner, unused -> {}));
                }
            }
        }
        registry.stateUsed(entries);
    }
}
