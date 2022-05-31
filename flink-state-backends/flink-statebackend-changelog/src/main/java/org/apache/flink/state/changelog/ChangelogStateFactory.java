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

package org.apache.flink.state.changelog;

import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.heap.InternalKeyContext;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.FlinkRuntimeException;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** Maintains the lifecycle of all {@link ChangelogState}s. */
public class ChangelogStateFactory {

    /**
     * Unwrapped changelog states used for recovery (not wrapped into e.g. TTL, latency tracking).
     */
    private final Map<String, ChangelogState> changelogStates;

    private final Map<String, ChangelogKeyGroupedPriorityQueue<?>> priorityQueueStatesByName;

    public ChangelogStateFactory() {
        this.changelogStates = new HashMap<>();
        this.priorityQueueStatesByName = new HashMap<>();
    }

    private static final Map<StateDescriptor.Type, StateFactory> STATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    StateDescriptor.Type.VALUE,
                                    (StateFactory) ChangelogValueState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.LIST,
                                    (StateFactory) ChangelogListState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.REDUCING,
                                    (StateFactory) ChangelogReducingState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.AGGREGATING,
                                    (StateFactory) ChangelogAggregatingState::create),
                            Tuple2.of(
                                    StateDescriptor.Type.MAP,
                                    (StateFactory) ChangelogMapState::create))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    public <K, N, V, S extends State> ChangelogState create(
            StateDescriptor<S, V> stateDescriptor,
            InternalKvState<K, N, V> internalKvState,
            KvStateChangeLogger<V, N> kvStateChangeLogger,
            InternalKeyContext<K> keyContext)
            throws Exception {
        ChangelogState changelogState =
                getStateFactory(stateDescriptor)
                        .create(internalKvState, kvStateChangeLogger, keyContext);
        changelogStates.put(stateDescriptor.getName(), changelogState);
        return changelogState;
    }

    public <T> ChangelogKeyGroupedPriorityQueue<T> create(
            String stateName,
            KeyGroupedInternalPriorityQueue<T> internalPriorityQueue,
            StateChangeLogger<T, Void> logger,
            TypeSerializer<T> serializer) {
        ChangelogKeyGroupedPriorityQueue<T> changelogKeyGroupedPriorityQueue =
                new ChangelogKeyGroupedPriorityQueue<>(internalPriorityQueue, logger, serializer);
        priorityQueueStatesByName.put(stateName, changelogKeyGroupedPriorityQueue);
        return changelogKeyGroupedPriorityQueue;
    }

    /**
     * @param name state name
     * @param type state type (the only supported type currently are: {@link
     *     StateMetaInfoSnapshot.BackendStateType#KEY_VALUE key value}, {@link
     *     StateMetaInfoSnapshot.BackendStateType#PRIORITY_QUEUE priority queue})
     * @return an existing state, i.e. the one that was already created. The returned state will not
     *     apply TTL to the passed values, regardless of the TTL settings. This prevents double
     *     applying of TTL (recovered values are TTL values if TTL was enabled). The state will,
     *     however, use TTL serializer if TTL is enabled. WARN: only valid during the recovery.
     * @throws UnsupportedOperationException if state type is not supported
     */
    public ChangelogState getExistingState(String name, StateMetaInfoSnapshot.BackendStateType type)
            throws UnsupportedOperationException {
        ChangelogState state;
        switch (type) {
            case KEY_VALUE:
                state = changelogStates.get(name);
                break;
            case PRIORITY_QUEUE:
                state = priorityQueueStatesByName.get(name);
                break;
            default:
                throw new UnsupportedOperationException(
                        String.format("Unknown state type %s (%s)", type, name));
        }
        return state;
    }

    public void resetAllWritingMetaFlags() {
        for (ChangelogState changelogState : changelogStates.values()) {
            changelogState.resetWritingMetaFlag();
        }

        for (ChangelogKeyGroupedPriorityQueue<?> priorityQueueState :
                priorityQueueStatesByName.values()) {
            priorityQueueState.resetWritingMetaFlag();
        }
    }

    public void dispose() {
        changelogStates.clear();
        priorityQueueStatesByName.clear();
    }

    private <S extends State, V> StateFactory getStateFactory(
            StateDescriptor<S, V> stateDescriptor) {
        StateFactory stateFactory = STATE_FACTORIES.get(stateDescriptor.getType());
        if (stateFactory == null) {
            String message =
                    String.format(
                            "State %s is not supported by %s",
                            stateDescriptor.getClass(), ChangelogKeyedStateBackend.class);
            throw new FlinkRuntimeException(message);
        }
        return stateFactory;
    }

    // Factory function interface
    private interface StateFactory {
        <K, N, SV, S extends State, IS extends S> IS create(
                InternalKvState<K, N, SV> kvState,
                KvStateChangeLogger<SV, N> changeLogger,
                InternalKeyContext<K> keyContext)
                throws Exception;
    }
}
