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
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.runtime.state.heap.HeapKeyedStateBackendBuilder;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.metrics.LatencyTrackingStateConfig;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;

import java.util.Collection;

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
            LatencyTrackingStateConfig latencyTrackingStateConfig) {

        StateHandleHelper stateHandleHelper = new StateHandleHelper(keyGroupRange, stateHandles);
        IncrementalKeyedStateHandle initialState = stateHandleHelper.init(stateHandles);

        StreamCompressionDecorator compressionDecorator =
                getCompressionDecorator(env.getExecutionConfig());

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
                        new IncrementalHeapSnapshotStrategy<>(
                                kvStates,
                                pqStates,
                                compressionDecorator,
                                localRecoveryConfig,
                                keyGroupRange,
                                keySerializerProvider,
                                new IncrementalSnapshotTrackerImpl(initialState, stateHandleHelper),
                                stateHandleHelper),
                IncrementalCopyOnWriteStateTable::new,
                IncrementalKeyGroupReader.FACTORY,
                IncrementalKeyedBackendSerializationProxy::new);
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
