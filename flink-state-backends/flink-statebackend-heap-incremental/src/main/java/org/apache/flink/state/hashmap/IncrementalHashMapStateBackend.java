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

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.IllegalConfigurationException;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.StateTable;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.runtime.taskmanager.AsynchronousException;
import org.apache.flink.state.common.PeriodicMaterializationManager;
import org.apache.flink.state.common.PeriodicMaterializationManager.MaterializationTarget;
import org.apache.flink.state.hashmap.IncrementalSnapshot.Versions;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/** {@link HashMapStateBackend} with incremental checkpoints support. */
@Experimental
public class IncrementalHashMapStateBackend extends HashMapStateBackend implements StateBackend {

    private static final long serialVersionUID = 1L;

    public IncrementalHashMapStateBackend() {}

    private IncrementalHashMapStateBackend(
            IncrementalHashMapStateBackend original, ReadableConfig config) {
        latencyTrackingConfigBuilder = original.latencyTrackingConfigBuilder.configure(config);
    }

    @Override
    protected <K> HeapKeyedStateBackendBuilder<K> createBuilder(
            Environment env,
            TypeSerializer<K> keySerializer,
            int numberOfKeyGroups,
            KeyGroupRange keyGroupRange,
            TaskKvStateRegistry kvStateRegistry,
            TtlTimeProvider ttlTimeProvider,
            Collection<KeyedStateHandle> stateHandles,
            CloseableRegistry cancelStreamRegistry,
            LocalRecoveryConfig localRecoveryConfig,
            HeapPriorityQueueSetFactory priorityQueueSetFactory,
            LatencyTrackingStateConfig latencyTrackingStateConfig,
            String operatorIdentifier) {

        StateHandleHelper stateHandleHelper = new StateHandleHelper(keyGroupRange, stateHandles);
        IncrementalKeyedStateHandle initialState = stateHandleHelper.init(stateHandles);

        StreamCompressionDecorator compressionDecorator =
                getCompressionDecorator(env.getExecutionConfig());

        CloseableRegistry cancelStreamRegistryForBackend = new CloseableRegistry();
        return new HeapKeyedStateBackendBuilder<>(
                kvStateRegistry,
                keySerializer,
                env.getUserCodeClassLoader().asClassLoader(),
                numberOfKeyGroups,
                keyGroupRange,
                env.getExecutionConfig(),
                ttlTimeProvider,
                latencyTrackingStateConfig,
                stateHandleHelper.flattenForRecovery(initialState),
                compressionDecorator,
                localRecoveryConfig,
                priorityQueueSetFactory,
                true,
                cancelStreamRegistry,
                (kvStates, pqStates, keySerializerProvider) ->
                        createSnapshotStrategy(
                                env,
                                keyGroupRange,
                                localRecoveryConfig,
                                operatorIdentifier,
                                stateHandleHelper,
                                initialState,
                                compressionDecorator,
                                cancelStreamRegistryForBackend,
                                kvStates,
                                pqStates,
                                keySerializerProvider),
                IncrementalCopyOnWriteStateTable::new,
                IncrementalKeyGroupReader.FACTORY,
                IncrementalKeyedBackendSerializationProxy::new,
                cancelStreamRegistryForBackend);
    }

    private <K> IncrementalHeapSnapshotStrategy<K> createSnapshotStrategy(
            Environment env,
            KeyGroupRange keyGroupRange,
            LocalRecoveryConfig localRecoveryConfig,
            String operatorIdentifier,
            StateHandleHelper stateHandleHelper,
            IncrementalKeyedStateHandle initialState,
            StreamCompressionDecorator compressionDecorator,
            CloseableRegistry cancelStreamRegistryForBackend,
            Map<String, StateTable<K, ?, ?>> kvStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> pqStates,
            StateSerializerProvider<K> keySerializerProvider) {

        IncrementalSnapshotTracker snapshotTracker =
                new IncrementalSnapshotTrackerImpl(initialState, stateHandleHelper);

        IncrementalHeapSnapshotStrategy<K> snapshotStrategy =
                new IncrementalHeapSnapshotStrategy<>(
                        kvStates,
                        pqStates,
                        compressionDecorator,
                        localRecoveryConfig,
                        keyGroupRange,
                        keySerializerProvider,
                        snapshotTracker,
                        stateHandleHelper);

        MaterializationTarget<Versions> materializationTarget =
                new HeapBackendMaterializationTarget(
                        snapshotStrategy,
                        snapshotTracker,
                        cancelStreamRegistryForBackend,
                        keyGroupRange,
                        compressionDecorator,
                        localRecoveryConfig,
                        getStreamFactory(env.getCheckpointStorageAccess()));

        new PeriodicMaterializationManager<>(
                        env.getMainMailboxExecutor(),
                        env.getAsyncOperationsThreadPool(),
                        env.getTaskInfo().getTaskName(),
                        (message, exception) ->
                                env.failExternally(new AsynchronousException(message, exception)),
                        materializationTarget,
                        300_000, // todo: read from config
                        100, // todo: read from config
                        operatorIdentifier)
                .start();

        return snapshotStrategy;
    }

    private CheckpointStreamFactory getStreamFactory(final CheckpointStorageAccess storageAccess) {
        return new CheckpointStreamFactory() {
            @Override
            public CheckpointStateOutputStream createCheckpointStateOutputStream(
                    CheckpointedStateScope scope) throws IOException {
                return storageAccess.createTaskOwnedStateStream();
            }

            @Override
            public boolean canFastDuplicate(
                    StreamStateHandle stateHandle, CheckpointedStateScope scope) {
                return false;
            }

            @Override
            public List<StreamStateHandle> duplicate(
                    List<StreamStateHandle> stateHandles, CheckpointedStateScope scope) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public boolean supportsNoClaimRestoreMode() {
        return false;
    }

    @Override
    public HashMapStateBackend configure(ReadableConfig config, ClassLoader classLoader)
            throws IllegalConfigurationException {
        return new IncrementalHashMapStateBackend(this, config);
    }
}
