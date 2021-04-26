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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.FullSnapshotAsyncWriter;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamCompressionDecorator;

import javax.annotation.Nonnull;

import java.util.Map;

/** A strategy how to perform a snapshot of a {@link HeapKeyedStateBackend}. */
class HeapSavepointStrategy<K>
        implements SnapshotStrategy<KeyedStateHandle, FullSnapshotResources<K>> {

    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final StreamCompressionDecorator keyGroupCompressionDecorator;
    private final KeyGroupRange keyGroupRange;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final int totalKeyGroups;

    HeapSavepointStrategy(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            KeyGroupRange keyGroupRange,
            StateSerializerProvider<K> keySerializerProvider,
            int totalKeyGroups) {
        this.registeredKVStates = registeredKVStates;
        this.registeredPQStates = registeredPQStates;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.keyGroupRange = keyGroupRange;
        this.keySerializerProvider = keySerializerProvider;
        this.totalKeyGroups = totalKeyGroups;
    }

    @Override
    public FullSnapshotResources<K> syncPrepareResources(long checkpointId) {
        return HeapSnapshotResources.create(
                registeredKVStates,
                registeredPQStates,
                keyGroupCompressionDecorator,
                keyGroupRange,
                getKeySerializer(),
                totalKeyGroups);
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            FullSnapshotResources<K> syncPartResource,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        assert checkpointOptions.getCheckpointType().isSavepoint();
        return new FullSnapshotAsyncWriter<>(
                checkpointOptions.getCheckpointType(),
                () ->
                        CheckpointStreamWithResultProvider.createSimpleStream(
                                CheckpointedStateScope.EXCLUSIVE, streamFactory),
                syncPartResource);
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializerProvider.currentSchemaSerializer();
    }
}
