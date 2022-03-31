/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state.heap;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RestoreOperation;
import org.apache.flink.runtime.state.SavepointKeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshotRestore;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import static org.apache.flink.runtime.state.SnapshotExecutionType.ASYNCHRONOUS;
import static org.apache.flink.runtime.state.SnapshotExecutionType.SYNCHRONOUS;

/**
 * Builder class for {@link HeapKeyedStateBackend} which handles all necessary initializations and
 * clean ups.
 *
 * @param <K> The data type that the key serializer serializes.
 */
public class HeapKeyedStateBackendBuilder<K> extends AbstractKeyedStateBackendBuilder<K> {
    /** The configuration of local recovery. */
    private final LocalRecoveryConfig localRecoveryConfig;
    /** Factory for state that is organized as priority queue. */
    private final HeapPriorityQueueSetFactory priorityQueueSetFactory;
    /** Whether asynchronous snapshot is enabled. */
    private final boolean asynchronousSnapshots;

    private final SnapshotStrategyFactory<K> snapshotStrategyFactory;
    private final StateTableFactory<K> stateTableFactory;
    private final HeapRestoreOperation.KeyGroupReaderFactory keyGroupReaderFactory;
    private final Function<ClassLoader, KeyedBackendSerializationProxy<K>>
            serializationProxyProvider;

    public HeapKeyedStateBackendBuilder(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            LocalRecoveryConfig localRecoveryConfig,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            boolean asynchronousSnapshots,
            CloseableRegistry cancelStreamRegistry) {
        this(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                numberOfKeyGroups,
                keyGroupRange,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                stateHandles,
                keyGroupCompressionDecorator,
                localRecoveryConfig,
                priorityQueueSetFactory,
                asynchronousSnapshots,
                cancelStreamRegistry,
                (kvStates, pqStates, keySerializerProvider) ->
                        new HeapSnapshotStrategy<>(
                                kvStates,
                                pqStates,
                                keyGroupCompressionDecorator,
                                localRecoveryConfig,
                                keyGroupRange,
                                keySerializerProvider,
                                numberOfKeyGroups,
                                StateSnapshotWriter.DEFAULT),
                CopyOnWriteStateTable::new,
                StateSnapshotRestore::keyGroupReader,
                KeyedBackendSerializationProxy::new);
    }

    public HeapKeyedStateBackendBuilder(
            TaskKvStateRegistry kvStateRegistry,
            TypeSerializer<K> keySerializer,
            ClassLoader userCodeClassLoader,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            ExecutionConfig executionConfig,
            TtlTimeProvider ttlTimeProvider,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            @Nonnull Collection<KeyedStateHandle> stateHandles,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            LocalRecoveryConfig localRecoveryConfig,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            boolean asynchronousSnapshots,
            CloseableRegistry cancelStreamRegistry,
            SnapshotStrategyFactory<K> snapshotStrategyFactory,
            StateTableFactory<K> stateTableFactory,
            HeapRestoreOperation.KeyGroupReaderFactory keyGroupReaderFactory,
            Function<ClassLoader, KeyedBackendSerializationProxy<K>> serializationProxyProvider) {
        super(
                kvStateRegistry,
                keySerializer,
                userCodeClassLoader,
                numberOfKeyGroups,
                keyGroupRange,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                stateHandles,
                keyGroupCompressionDecorator,
                cancelStreamRegistry);
        this.localRecoveryConfig = localRecoveryConfig;
        this.priorityQueueSetFactory = priorityQueueSetFactory;
        this.asynchronousSnapshots = asynchronousSnapshots;
        this.snapshotStrategyFactory = snapshotStrategyFactory;
        this.stateTableFactory = stateTableFactory;
        this.keyGroupReaderFactory = keyGroupReaderFactory;
        this.serializationProxyProvider = serializationProxyProvider;
    }

    @Override
    public HeapKeyedStateBackend<K> build() throws BackendBuildingException {
        // Map of registered Key/Value states
        Map<String, StateTable<K, ?, ?>> registeredKVStates = new HashMap<>();
        // Map of registered priority queue set states
        Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates =
                new HashMap<>();
        CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();
        SnapshotStrategy<KeyedStateHandle, ?> snapshotStrategy =
                snapshotStrategyFactory.initSnapshotStrategy(
                        registeredKVStates, registeredPQStates, keySerializerProvider);
        InternalKeyContext<K> keyContext =
                new InternalKeyContextImpl<>(keyGroupRange, numberOfKeyGroups);

        restoreState(registeredKVStates, registeredPQStates, keyContext, stateTableFactory);
        return new HeapKeyedStateBackend<>(
                kvStateRegistry,
                keySerializerProvider.currentSchemaSerializer(),
                userCodeClassLoader,
                executionConfig,
                ttlTimeProvider,
                latencyTrackingStateConfig,
                cancelStreamRegistryForBackend,
                keyGroupCompressionDecorator,
                registeredKVStates,
                registeredPQStates,
                localRecoveryConfig,
                priorityQueueSetFactory,
                snapshotStrategy,
                asynchronousSnapshots ? ASYNCHRONOUS : SYNCHRONOUS,
                stateTableFactory,
                keyContext);
    }

    private void restoreState(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            InternalKeyContext<K> keyContext,
            StateTableFactory<K> stateTableFactory)
            throws BackendBuildingException {
        final RestoreOperation<Void> restoreOperation;

        final KeyedStateHandle firstHandle;
        if (restoreStateHandles.isEmpty()) {
            firstHandle = null;
        } else {
            firstHandle = restoreStateHandles.iterator().next();
        }
        if (firstHandle instanceof SavepointKeyedStateHandle) {
            restoreOperation =
                    new HeapSavepointRestoreOperation<>(
                            restoreStateHandles,
                            keySerializerProvider,
                            userCodeClassLoader,
                            registeredKVStates,
                            registeredPQStates,
                            priorityQueueSetFactory,
                            keyGroupRange,
                            numberOfKeyGroups,
                            stateTableFactory,
                            keyContext);
        } else {
            restoreOperation =
                    new HeapRestoreOperation<>(
                            restoreStateHandles,
                            keySerializerProvider,
                            userCodeClassLoader,
                            registeredKVStates,
                            registeredPQStates,
                            cancelStreamRegistry,
                            priorityQueueSetFactory,
                            keyGroupRange,
                            numberOfKeyGroups,
                            stateTableFactory,
                            keyContext,
                            keyGroupReaderFactory,
                            serializationProxyProvider);
        }
        try {
            restoreOperation.restore();
            logger.info("Finished to build heap keyed state-backend.");
        } catch (Exception e) {
            throw new BackendBuildingException("Failed when trying to restore heap backend", e);
        }
    }

    public interface SnapshotStrategyFactory<K> {
        SnapshotStrategy<KeyedStateHandle, ? extends SnapshotResources> initSnapshotStrategy(
                Map<String, StateTable<K, ?, ?>> registeredKVStates,
                Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
                StateSerializerProvider<K> stateSerializerProvider);
    }
}
