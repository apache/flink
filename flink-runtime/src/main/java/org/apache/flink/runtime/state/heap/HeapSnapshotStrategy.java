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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StateSnapshot;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapSnapshotStrategy.MetadataWriter.MetadataWriterImpl;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.function.SupplierWithException;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.createDuplicatingStream;
import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.createSimpleStream;
import static org.apache.flink.runtime.state.CheckpointStreamWithResultProvider.toKeyedStateHandleSnapshotResult;

/** A strategy how to perform a snapshot of a {@link HeapKeyedStateBackend}. */
@Internal
// todo: move writer up and close access to this class
public class HeapSnapshotStrategy<K>
        implements SnapshotStrategy<KeyedStateHandle, HeapSnapshotResourcesBase<K>> {

    private final Map<String, StateTable<K, ?, ?>> registeredKVStates;
    private final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates;
    private final StreamCompressionDecorator keyGroupCompressionDecorator;
    private final LocalRecoveryConfig localRecoveryConfig;
    private final KeyGroupRange keyGroupRange;
    private final StateSerializerProvider<K> keySerializerProvider;
    private final int totalKeyGroups;
    private final StateSnapshotWriter snapshotWriter;

    HeapSnapshotStrategy(
            Map<String, StateTable<K, ?, ?>> registeredKVStates,
            Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            StreamCompressionDecorator keyGroupCompressionDecorator,
            LocalRecoveryConfig localRecoveryConfig,
            KeyGroupRange keyGroupRange,
            StateSerializerProvider<K> keySerializerProvider,
            int totalKeyGroups,
            StateSnapshotWriter snapshotWriter) {
        this.registeredKVStates = registeredKVStates;
        this.registeredPQStates = registeredPQStates;
        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
        this.localRecoveryConfig = localRecoveryConfig;
        this.keyGroupRange = keyGroupRange;
        this.keySerializerProvider = keySerializerProvider;
        this.totalKeyGroups = totalKeyGroups;
        this.snapshotWriter = snapshotWriter;
    }

    @Override
    public HeapSnapshotResourcesBase<K> syncPrepareResources(long checkpointId) {
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
            HeapSnapshotResourcesBase<K> syncPartResource,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        return new HeapSnapshotWriter<>(
                syncPartResource,
                keyGroupRange,
                keyGroupCompressionDecorator,
                snapshotWriter,
                localRecoveryConfig,
                checkpointOptions,
                CheckpointedStateScope.EXCLUSIVE,
                checkpointId,
                streamFactory,
                new MetadataWriterImpl<>(syncPartResource, keyGroupCompressionDecorator));
    }

    public static class HeapSnapshotWriter<K> implements SnapshotResultSupplier<KeyedStateHandle> {
        private final HeapSnapshotResourcesBase<K> syncPartResource;
        private final KeyGroupRange keyGroupRange;
        private final StreamCompressionDecorator keyGroupCompressionDecorator;
        private final StateSnapshotWriter snapshotWriter;
        private final LocalRecoveryConfig localRecoveryConfig;
        private final CheckpointOptions checkpointOptions;
        private final CheckpointedStateScope stateScope;
        private final long checkpointId;
        private final CheckpointStreamFactory streamFactory;
        private final MetadataWriter metadataWriter;

        public HeapSnapshotWriter(
                HeapSnapshotResourcesBase<K> syncPartResource,
                KeyGroupRange keyGroupRange,
                StreamCompressionDecorator keyGroupCompressionDecorator,
                StateSnapshotWriter snapshotWriter,
                LocalRecoveryConfig localRecoveryConfig,
                CheckpointOptions checkpointOptions,
                CheckpointedStateScope stateScope,
                long checkpointId,
                CheckpointStreamFactory streamFactory,
                MetadataWriter metadataWriter) {
            this.syncPartResource = syncPartResource;
            this.keyGroupRange = keyGroupRange;
            this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
            this.snapshotWriter = snapshotWriter;
            this.localRecoveryConfig = localRecoveryConfig;
            this.checkpointOptions = checkpointOptions;
            this.stateScope = stateScope;
            this.checkpointId = checkpointId;
            this.streamFactory = streamFactory;
            this.metadataWriter = metadataWriter;
        }

        @Override
        public SnapshotResult<KeyedStateHandle> get(CloseableRegistry snapshotCloseableRegistry)
                throws Exception {

            List<StateMetaInfoSnapshot> metaInfoSnapshots = syncPartResource.getMetaInfoSnapshots();
            if (metaInfoSnapshots.isEmpty()) {
                return SnapshotResult.empty();
            }

            final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                    checkpointStreamSupplier =
                            localRecoveryConfig.isLocalRecoveryEnabled()
                                            && !checkpointOptions.getCheckpointType().isSavepoint()
                                    ? () ->
                                            createDuplicatingStream(
                                                    checkpointId,
                                                    stateScope,
                                                    streamFactory,
                                                    localRecoveryConfig
                                                            .getLocalStateDirectoryProvider()
                                                            .orElseThrow(
                                                                    LocalRecoveryConfig
                                                                            .localRecoveryNotEnabled()))
                                    : () -> createSimpleStream(stateScope, streamFactory);

            final Map<StateUID, Integer> stateNamesToId = syncPartResource.getStateNamesToId();
            final Map<StateUID, StateSnapshot> cowStateStableSnapshots =
                    syncPartResource.getCowStateStableSnapshots();
            final CheckpointStreamWithResultProvider streamWithResultProvider =
                    checkpointStreamSupplier.get();

            snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

            final CheckpointStateOutputStream localStream =
                    streamWithResultProvider.getCheckpointOutputStream();

            final DataOutputViewStreamWrapper outView =
                    new DataOutputViewStreamWrapper(localStream);
            metadataWriter.write(outView);

            final long[] keyGroupRangeOffsets = new long[keyGroupRange.getNumberOfKeyGroups()];

            for (int keyGroupPos = 0;
                    keyGroupPos < keyGroupRange.getNumberOfKeyGroups();
                    ++keyGroupPos) {
                int keyGroupId = keyGroupRange.getKeyGroupId(keyGroupPos);
                keyGroupRangeOffsets[keyGroupPos] = localStream.getPos();
                outView.writeInt(keyGroupId);

                for (Map.Entry<StateUID, StateSnapshot> stateSnapshot :
                        cowStateStableSnapshots.entrySet()) {
                    try (OutputStream kgCompressionOut =
                            keyGroupCompressionDecorator.decorateWithCompression(localStream)) {
                        DataOutputViewStreamWrapper kgCompressionView =
                                new DataOutputViewStreamWrapper(kgCompressionOut);
                        kgCompressionView.writeShort(stateNamesToId.get(stateSnapshot.getKey()));
                        snapshotWriter.write(
                                stateSnapshot.getValue(), kgCompressionView, keyGroupId);
                    } // this will just close the outer compression stream
                }
            }

            if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                KeyGroupRangeOffsets kgOffs =
                        new KeyGroupRangeOffsets(keyGroupRange, keyGroupRangeOffsets);
                SnapshotResult<StreamStateHandle> result =
                        streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
                return toKeyedStateHandleSnapshotResult(result, kgOffs, KeyGroupsStateHandle::new);
            } else {
                throw new IOException("Stream already unregistered.");
            }
        }
    }

    public TypeSerializer<K> getKeySerializer() {
        return keySerializerProvider.currentSchemaSerializer();
    }

    public interface MetadataWriter {
        void write(DataOutputViewStreamWrapper outView) throws IOException;

        class MetadataWriterImpl<K> implements MetadataWriter {

            private final KeyedBackendSerializationProxy<K> serializationProxy;

            public MetadataWriterImpl(
                    HeapSnapshotResourcesBase<K> resources, StreamCompressionDecorator decorator) {
                serializationProxy =
                        new KeyedBackendSerializationProxy<>(
                                // TODO: this code assumes that writing a serializer is threadsafe,
                                // we should support to get a serialized form already at state
                                // registration time in the future
                                resources.getKeySerializer(),
                                resources.getMetaInfoSnapshots(),
                                !Objects.equals(
                                        UncompressedStreamCompressionDecorator.INSTANCE,
                                        decorator));
            }

            @Override
            public void write(DataOutputViewStreamWrapper outView) throws IOException {
                serializationProxy.write(outView);
            }
        }
    }
}
