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

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy.MetadataWriter;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy.MetadataWriter.MetadataWriterImpl;
import org.apache.flink.runtime.state.heap.StateSnapshotWriter;
import org.apache.flink.runtime.state.heap.StateTable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

final class IncrementalHeapSnapshotStrategy<K>
        implements SnapshotStrategy<KeyedStateHandle, IncrementalHeapSnapshotResources<K>>,
                CheckpointListener {
    private static final Logger LOG =
            LoggerFactory.getLogger(IncrementalHeapSnapshotStrategy.class);

    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final StreamCompressionDecorator keyGroupCompressionDecorator;
    private final LocalRecoveryConfig localRecoveryConfig;
    private final KeyGroupRange keyGroupRange;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final IncrementalSnapshotTracker snapshotTracker;
    private final StateHandleHelper stateHandleHelper;

    IncrementalHeapSnapshotStrategy(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            LocalRecoveryConfig localRecoveryConfig,
            KeyGroupRange keyGroupRange,
            StateSerializerProvider<K> keySerializerProvider,
            IncrementalSnapshotTracker snapshotTracker,
            StateHandleHelper stateHandleHelper) {
        this.registeredKVStates = registeredKVStates;
        this.registeredPQStates = registeredPQStates;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.localRecoveryConfig = localRecoveryConfig;
        this.keyGroupRange = keyGroupRange;
        this.keySerializerProvider = keySerializerProvider;
        this.snapshotTracker = snapshotTracker;
        this.stateHandleHelper = stateHandleHelper;
        // todo: materialization
    }

    @Override
    public IncrementalHeapSnapshotResources<K> syncPrepareResources(long checkpointId) {
        LOG.info("starting sync phase, checkpoint: {}", checkpointId);
        // sync phase is the same regardless of the checkpoint type
        return IncrementalHeapSnapshotResources.create(
                snapshotTracker,
                registeredKVStates,
                registeredPQStates,
                keySerializerProvider.currentSchemaSerializer());
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            IncrementalHeapSnapshotResources<K> syncPartResource,
            long checkpointId,
            long timestamp,
            CheckpointStreamFactory streamFactory,
            CheckpointOptions checkpointOptions) {
        MetadataWriter kMetadataWriter =
                createMetadataWriter(
                        checkpointOptions, syncPartResource, keyGroupCompressionDecorator);
        return snapshotCloseableRegistry -> {
            LOG.info("starting async phase, checkpoint: {}", checkpointId);
            // write/upload the new state
            SnapshotResult<KeyedStateHandle> newHandle =
                    new HeapSnapshotStrategy.HeapSnapshotWriter<>(
                                    syncPartResource,
                                    keyGroupRange,
                                    keyGroupCompressionDecorator,
                                    selectDataWriter(checkpointOptions, syncPartResource),
                                    localRecoveryConfig,
                                    checkpointOptions,
                                    // use shared scope even if not incremental
                                    // so that it can be included later
                                    CheckpointedStateScope.SHARED,
                                    checkpointId,
                                    streamFactory,
                                    kMetadataWriter)
                            .get(snapshotCloseableRegistry);

            // combine with previous state
            SnapshotResult<KeyedStateHandle> newSnapshot =
                    stateHandleHelper.combine(
                            syncPartResource.getSnapshotBase().getStateSnapshot(),
                            syncPartResource.getSnapshotBase().getCheckpointID(),
                            newHandle,
                            checkpointId);

            // remember it to use later after JM confirmation
            snapshotTracker.track(
                    checkpointId,
                    new IncrementalSnapshot(
                            syncPartResource.getCurrentMapVersions(), newSnapshot, checkpointId),
                    syncPartResource.getConfirmCallbacks());

            LOG.debug("async phase finished, checkpoint: {}", checkpointId);
            return newSnapshot;
        };
    }

    private MetadataWriter createMetadataWriter(
            CheckpointOptions checkpointOptions,
            IncrementalHeapSnapshotResources<K> syncPartResource,
            StreamCompressionDecorator keyGroupCompressionDecorator) {
        switch (checkpointOptions.getCheckpointType().getSharingFilesStrategy()) {
            case FORWARD:
                return new MetadataWriterImpl<>(syncPartResource, keyGroupCompressionDecorator);
            case FORWARD_BACKWARD:
                return new IncrementalMetadataWriter<>(
                        syncPartResource, keyGroupCompressionDecorator);
            default:
                throw new UnsupportedOperationException(
                        checkpointOptions.getCheckpointType().getSharingFilesStrategy().name());
        }
    }

    private StateSnapshotWriter selectDataWriter(
            CheckpointOptions checkpointOptions,
            IncrementalHeapSnapshotResources<K> syncPartResource) {
        switch (checkpointOptions.getCheckpointType().getSharingFilesStrategy()) {
            case FORWARD:
                return StateSnapshotWriter.DEFAULT;
            case FORWARD_BACKWARD:
                return new IncrementalStateSnapshotWriter(syncPartResource.getSnapshotBase());
            default:
                throw new UnsupportedOperationException(
                        checkpointOptions.getCheckpointType().getSharingFilesStrategy().name());
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        snapshotTracker.confirmSnapshot(checkpointId);
    }
}
