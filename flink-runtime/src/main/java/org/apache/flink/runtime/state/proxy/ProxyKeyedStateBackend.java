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

package org.apache.flink.runtime.state.proxy;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.State;
import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AbstractKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupedInternalPriorityQueue;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateFunction;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.runtime.state.ttl.TtlStateFactory;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.FlinkRuntimeException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class ProxyKeyedStateBackend<K>
        implements CheckpointableKeyedStateBackend<K>, CheckpointListener {
    AbstractKeyedStateBackend<K> keyedStateBackend;

    private static final Map<Class<? extends StateDescriptor>, StateFactory> STATE_FACTORIES =
            Stream.of(
                            Tuple2.of(
                                    ValueStateDescriptor.class,
                                    (StateFactory) ProxyValueState::create),
                            Tuple2.of(
                                    ListStateDescriptor.class,
                                    (StateFactory) ProxyListState::create),
                            Tuple2.of(
                                    ReducingStateDescriptor.class,
                                    (StateFactory) ProxyReducingState::create),
                            Tuple2.of(
                                    AggregatingStateDescriptor.class,
                                    (StateFactory) ProxyAggregatingState::create),
                            Tuple2.of(
                                    MapStateDescriptor.class, (StateFactory) ProxyMapState::create))
                    .collect(Collectors.toMap(t -> t.f0, t -> t.f1));

    // ==============================================================
    //  cache maintained by the proxyKeyedStateBackend itself
    //  not the same as the underlying wrapped keyedStateBackend
    //  InternalKvState is a ProxyXXState, XX stands for Value, List ...
    /** So that we can give out state when the user uses the same key. */
    protected final HashMap<String, InternalKvState<K, ?, ?>> keyValueStatesByName;

    @SuppressWarnings("rawtypes")
    protected InternalKvState lastState;

    /** For caching the last accessed partitioned state. */
    protected String lastName;

    // ==============================================================
    // ==== the same as the wrapped keyedStateBackend

    public final ExecutionConfig executionConfig;

    public final TtlTimeProvider ttlTimeProvider;

    public ProxyKeyedStateBackend(
            AbstractKeyedStateBackend<K> keyedStateBackend,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider) {
        this.keyedStateBackend = keyedStateBackend;
        this.executionConfig = executionConfig;
        this.ttlTimeProvider = ttlTimeProvider;

        this.keyValueStatesByName = new HashMap<>();
    }

    @Override
    public KeyGroupRange getKeyGroupRange() {
        return keyedStateBackend.getKeyGroupRange();
    }

    @Override
    public void close() throws IOException {
        keyedStateBackend.close();
    }

    @Override
    public void setCurrentKey(K newKey) {
        keyedStateBackend.setCurrentKey(newKey);
    }

    @Override
    public K getCurrentKey() {
        return keyedStateBackend.getCurrentKey();
    }

    @Override
    public TypeSerializer<K> getKeySerializer() {
        return keyedStateBackend.getKeySerializer();
    }

    @Override
    public <N, S extends State, T> void applyToAllKeys(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, T> stateDescriptor,
            KeyedStateFunction<K, S> function)
            throws Exception {
        try (Stream<K> keyStream = getKeys(stateDescriptor.getName(), namespace)) {

            final S state = getPartitionedState(namespace, namespaceSerializer, stateDescriptor);

            if (keyedStateBackend.supportConcurrentModification()) {
                keyStream.forEach(
                        (K key) -> {
                            setCurrentKey(key);
                            try {
                                function.process(key, state);
                            } catch (Throwable e) {
                                // we wrap the checked exception in an unchecked
                                // one and catch it (and re-throw it) later.
                                throw new RuntimeException(e);
                            }
                        });
            } else {
                final List<K> keys = keyStream.collect(Collectors.toList());
                for (K key : keys) {
                    setCurrentKey(key);
                    function.process(key, state);
                }
            }
        }
    }

    @Override
    public <N> Stream<K> getKeys(String state, N namespace) {
        return keyedStateBackend.getKeys(state, namespace);
    }

    @Override
    public <N> Stream<Tuple2<K, N>> getKeysAndNamespaces(String state) {
        return keyedStateBackend.getKeysAndNamespaces(state);
    }

    @Override
    public <N, S extends State, T> S getOrCreateKeyedState(
            TypeSerializer<N> namespaceSerializer, StateDescriptor<S, T> stateDescriptor)
            throws Exception {
        checkNotNull(namespaceSerializer, "Namespace serializer");
        checkNotNull(
                getKeySerializer(),
                "State key serializer has not been configured in the config. "
                        + "This operation cannot use partitioned state.");

        InternalKvState<K, ?, ?> kvState = keyValueStatesByName.get(stateDescriptor.getName());
        if (kvState == null) {
            if (!stateDescriptor.isSerializerInitialized()) {
                stateDescriptor.initializeSerializerUnlessSet(executionConfig);
            }
            kvState =
                    TtlStateFactory.createStateAndWrapWithTtlIfEnabled(
                            namespaceSerializer, stateDescriptor, this, ttlTimeProvider);
            keyValueStatesByName.put(stateDescriptor.getName(), kvState);
            keyedStateBackend.publishQueryableStateIfEnabled(stateDescriptor, kvState);
        }
        return (S) kvState;
    }

    @Override
    public <N, S extends State> S getPartitionedState(
            N namespace,
            TypeSerializer<N> namespaceSerializer,
            StateDescriptor<S, ?> stateDescriptor)
            throws Exception {

        checkNotNull(namespace, "Namespace");

        if (lastName != null && lastName.equals(stateDescriptor.getName())) {
            lastState.setCurrentNamespace(namespace);
            return (S) lastState;
        }

        InternalKvState<K, ?, ?> previous = keyValueStatesByName.get(stateDescriptor.getName());
        if (previous != null) {
            lastState = previous;
            lastState.setCurrentNamespace(namespace);
            lastName = stateDescriptor.getName();
            return (S) previous;
        }

        final S state = getOrCreateKeyedState(namespaceSerializer, stateDescriptor);
        final InternalKvState<K, N, ?> kvState = (InternalKvState<K, N, ?>) state;

        lastName = stateDescriptor.getName();
        lastState = kvState;
        kvState.setCurrentNamespace(namespace);

        return state;
    }

    @Override
    public void dispose() {
        keyedStateBackend.dispose();
        lastName = null;
        lastState = null;
        keyValueStatesByName.clear();
    }

    @Override
    public void registerKeySelectionListener(KeySelectionListener<K> listener) {
        keyedStateBackend.registerKeySelectionListener(listener);
    }

    @Override
    public boolean deregisterKeySelectionListener(KeySelectionListener<K> listener) {
        return keyedStateBackend.deregisterKeySelectionListener(listener);
    }

    @Override
    public int numKeyValueStateEntries() {
        return keyedStateBackend.numKeyValueStateEntries();
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
        StateFactory stateFactory = STATE_FACTORIES.get(stateDesc.getClass());
        if (stateFactory == null) {
            String message =
                    String.format(
                            "State %s is not supported by %s",
                            stateDesc.getClass(), this.getClass());
            throw new FlinkRuntimeException(message);
        }

        return stateFactory.create(
                keyedStateBackend.createInternalState(
                        namespaceSerializer, stateDesc, snapshotTransformFactory));
    }

    @Nonnull
    @Override
    public <T extends HeapPriorityQueueElement & PriorityComparable & Keyed>
            KeyGroupedInternalPriorityQueue<T> create(
                    @Nonnull String stateName,
                    @Nonnull TypeSerializer<T> byteOrderedElementSerializer) {
        return keyedStateBackend.create(stateName, byteOrderedElementSerializer);
    }

    @Nonnull
    @Override
    public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions)
            throws Exception {
        return keyedStateBackend.snapshot(
                checkpointId, timestamp, streamFactory, checkpointOptions);
    }

    // -------------------- CheckpointListener --------------------------------
    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        keyedStateBackend.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) throws Exception {
        keyedStateBackend.notifyCheckpointAborted(checkpointId);
    }

    public AbstractKeyedStateBackend<K> getProxiedKeyedStateBackend() {
        return keyedStateBackend;
    }

    // Factory function interface
    private interface StateFactory {
        <K, N, SV, S extends State, IS extends S> IS create(InternalKvState<K, N, SV> kvState)
                throws Exception;
    }
}
