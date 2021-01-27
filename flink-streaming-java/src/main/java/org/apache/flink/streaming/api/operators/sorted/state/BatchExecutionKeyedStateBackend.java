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

package org.apache.flink.streaming.api.operators.sorted.state;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.PriorityComparator;
import org.apache.flink.runtime.state.SavepointResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link CheckpointableKeyedStateBackend} which keeps values for a single key at a time.
 *
 * <p><b>IMPORTANT:</b> Requires the incoming records to be sorted/grouped by the key. Used in a
 * BATCH style execution.
 */
class BatchExecutionKeyedStateBackend<K> implements CheckpointableKeyedStateBackend<K> {
    private static final Logger LOG =
            LoggerFactory.getLogger(BatchExecutionKeyedStateBackend.class);

    @SuppressWarnings("rawtypes")
    private static final Map<Class<? extends StateDescriptor>, StateFactory> STATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    ValueStateDescriptor.class,
                                    (StateFactory) BatchExecutionKeyValueState::create),
                            Tuple2.of(
                                    ListStateDescriptor.class,
                                    (StateFactory) BatchExecutionKeyListState::create),
                            Tuple2.of(
                                    MapStateDescriptor.class,
                                    (StateFactory) BatchExecutionKeyMapState::create),
                            Tuple2.of(
                                    AggregatingStateDescriptor.class,
                                    (StateFactory) BatchExecutionKeyAggregatingState::create),
                            Tuple2.of(
                                    ReducingStateDescriptor.class,
                                    (StateFactory) BatchExecutionKeyReducingState::create))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    private K currentKey = null;
    private final TypeSerializer<K> keySerializer;
    private final List<KeySelectionListener<K>> keySelectionListeners = new ArrayList<>();
    private final Map<String, State> states = new HashMap<>();
    private final Map<String, KeyGroupedInternalPriorityQueue<?>> priorityQueues = new HashMap<>();
    private final KeyGroupRange keyGroupRange;

    public BatchExecutionKeyedStateBackend(
            TypeSerializer<K> keySerializer, KeyGroupRange keyGroupRange) {
        this.keySerializer = keySerializer;
        this.keyGroupRange = keyGroupRange;
    }

    @Override
    public void setCurrentKey(K newKey) {
        if (!Objects.equals(newKey, currentKey)) {
            notifyKeySelected(newKey);
            for (State value : states.values()) {
                ((AbstractBatchExecutionKeyState<?, ?, ?>) value).clearAllNamespaces();
            }
            for (KeyGroupedInternalPriorityQueue<?> value : priorityQueues.values()) {
                while (value.poll() != null) {
                    // remove everything for the key
                }
            }
            this.currentKey = newKey;
        }
    }

    @Override
    public K getCurrentKey() {
        return currentKey;
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keySerializer;
    }

    @Override
    public <N, S extends State, T> void applyToAllKeys(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, T> stateDescriptor,
            KeyedStateFunction<K, S> function) {
        // we don't do anything here. This is correct because the BATCH broadcast operators
        // process the broadcast side first, meaning we know that the keyed side will always be
        // empty when this is called
        LOG.debug("Not iterating over all keyed in BATCH execution mode in applyToAllKeys().");
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        LOG.debug("Returning an empty stream in BATCH execution mode in getKeys().");
        // We return an empty Stream here. This is correct because the BATCH broadcast operators
        // process the broadcast side first, meaning we know that the keyed side will always be
        // empty when this is called
        return Stream.empty();
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        LOG.debug("Returning an empty stream in BATCH execution mode in getKeysAndNamespaces().");
        // We return an empty Stream here. This is correct because the BATCH broadcast operators
        // process the broadcast side first, meaning we know that the keyed side will always be
        // empty when this is called
        return Stream.empty();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception {
        checkNotNull(namespaceSerializer, "Namespace serializer");
        checkNotNull(
                keySerializer,
                "State key serializer has not been configured in the config. "
                        + "This operation cannot use partitioned state.");

        if (!stateDescriptor.isSerializerInitialized()) {
            stateDescriptor.initializeSerializerUnlessSet(new ExecutionConfig());
        }

        State state = states.get(stateDescriptor.getName());
        if (state == null) {
            state = createState(namespaceSerializer, stateDescriptor);
            states.put(stateDescriptor.getName(), state);
        }
        return (S) state;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <N, S extends State> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception {
        S state = getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        ((InternalKvState<K, N, ?>) state).setCurrentNamespace(namespace);
        return state;
    }

    @Override
    public void dispose() {}

    private void notifyKeySelected(K newKey) {
        // we prefer a for-loop over other iteration schemes for performance reasons here.
        for (KeySelectionListener<K> keySelectionListener : keySelectionListeners) {
            keySelectionListener.keySelected(newKey);
        }
    }

    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        keySelectionListeners.add(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return keySelectionListeners.remove(listener);
    }

    @Nonnull
    @Override
    public <N, SV, SEV, S extends State, IS extends S> IS createInternalState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc,
            @Nonnull
                    StateSnapshotTransformer.StateSnapshotTransformFactory<SEV>
                            snapshotTransformFactory)
            throws Exception {
        return createState(namespaceSerializer, stateDesc);
    }

    private <N, SV, S extends State, IS extends S> IS createState(
            @Nonnull TypeSerializer<N> namespaceSerializer,
            @Nonnull StateDescriptor<S, SV> stateDesc)
            throws Exception {
        StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
        if (stateFactory == null) {
            String message =
                    String.format(
                            "State %s is not supported by %s",
                            stateDesc.getClass(), this.getClass());
            throw new FlinkRuntimeException(message);
        }
        return stateFactory.createState(keySerializer, namespaceSerializer, stateDesc);
    }

    @Nonnull
    @Override
    @SuppressWarnings({"unchecked"})
    public <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        KeyGroupedInternalPriorityQueue<?> priorityQueue = priorityQueues.get(stateName);
        if (priorityQueue == null) {
            priorityQueue =
                    new BatchExecutionInternalPriorityQueueSet<>(
                            PriorityComparator.forPriorityComparableObjects(), 128);
            priorityQueues.put(stateName, priorityQueue);
        }
        return (KeyGroupedInternalPriorityQueue<T>) priorityQueue;
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyGroupRange;
    }

    @Override
    public void close() throws IOException {}

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {
        throw new UnsupportedOperationException(
                "Snapshotting is not supported in BATCH runtime mode.");
    }

    @Nonnull
    @Override
    public SavepointResources<K> savepoint() throws Exception {
        throw new UnsupportedOperationException(
                "Savepoints are not supported in BATCH runtime mode.");
    }

    @FunctionalInterface
    private interface StateFactory {
        <T, K, N, SV, S extends State, IS extends S> IS createState(
                TypeSerializer<K> keySerializer,
                TypeSerializer<N> namespaceSerializer,
                StateDescriptor<S, SV> stateDesc)
                throws Exception;
    }
}
