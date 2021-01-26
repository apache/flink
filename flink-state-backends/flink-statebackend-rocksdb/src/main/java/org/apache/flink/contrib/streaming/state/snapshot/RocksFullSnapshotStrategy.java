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

package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.iterator.RocksStatesPerKeyGroupMergeIterator;
import org.apache.flink.contrib.streaming.state.iterator.RocksTransformingIteratorWrapper;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.FullSnapshotAsyncWriter;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.function.SupplierWithException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * Snapshot strategy to create full snapshots of {@link
 * org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend}. Iterates and writes all
 * states from a RocksDB snapshot of the column families.
 *
 * @param <K> type of the backend keys.
 */
public class RocksFullSnapshotStrategy<K>
        extends RocksDBSnapshotStrategyBase<K, FullSnapshotResources<K>> {

    private static final Logger LOG = LoggerFactory.getLogger(RocksFullSnapshotStrategy.class);

    private static final String DESCRIPTION = "Asynchronous full RocksDB snapshot";

    /** This decorator is used to apply compression per key-group for the written snapshot data. */
    @Nonnull private final StreamCompressionDecorator keyGroupCompressionDecorator;

    public RocksFullSnapshotStrategy(
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard rocksDBResourceGuard,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull LocalRecoveryConfig localRecoveryConfig,
            @Nonnull StreamCompressionDecorator keyGroupCompressionDecorator) {
        super(
                DESCRIPTION,
                db,
                rocksDBResourceGuard,
                keySerializer,
                kvStateInformation,
                keyGroupRange,
                keyGroupPrefixBytes,
                localRecoveryConfig);

        this.keyGroupCompressionDecorator = keyGroupCompressionDecorator;
    }

    @Override
    public FullSnapshotResources<K> syncPrepareResources(long checkpointId) throws Exception {

        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                new ArrayList<>(kvStateInformation.size());
        final List<RocksDbKvStateInfo> metaDataCopy = new ArrayList<>(kvStateInformation.size());

        for (RocksDbKvStateInfo stateInfo : kvStateInformation.values()) {
            // snapshot meta info
            stateMetaInfoSnapshots.add(stateInfo.metaInfo.snapshot());
            metaDataCopy.add(stateInfo);
        }

        final ResourceGuard.Lease lease = rocksDBResourceGuard.acquireResource();
        final Snapshot snapshot = db.getSnapshot();

        return new FullRocksDBSnapshotResources<>(
                lease,
                snapshot,
                metaDataCopy,
                stateMetaInfoSnapshots,
                db,
                keyGroupPrefixBytes,
                keyGroupRange,
                keySerializer,
                keyGroupCompressionDecorator);
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            FullSnapshotResources<K> fullRocksDBSnapshotResources,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory checkpointStreamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        if (fullRocksDBSnapshotResources.getMetaInfoSnapshots().isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning null.",
                        timestamp);
            }
            return registry -> SnapshotResult.empty();
        }

        final SupplierWithException<CheckpointStreamWithResultProvider, Exception>
                checkpointStreamSupplier =
                        createCheckpointStreamSupplier(
                                checkpointId, checkpointStreamFactory, checkpointOptions);

        return new FullSnapshotAsyncWriter<>(
                checkpointStreamSupplier, fullRocksDBSnapshotResources);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // nothing to do.
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // nothing to do.
    }

    private SupplierWithException<CheckpointStreamWithResultProvider, Exception>
            createCheckpointStreamSupplier(
                    long checkpointId,
                    CheckpointStreamFactory primaryStreamFactory,
                    CheckpointOptions checkpointOptions) {

        return localRecoveryConfig.isLocalRecoveryEnabled()
                        && !checkpointOptions.getCheckpointType().isSavepoint()
                ? () ->
                        CheckpointStreamWithResultProvider.createDuplicatingStream(
                                checkpointId,
                                CheckpointedStateScope.EXCLUSIVE,
                                primaryStreamFactory,
                                localRecoveryConfig.getLocalStateDirectoryProvider())
                : () ->
                        CheckpointStreamWithResultProvider.createSimpleStream(
                                CheckpointedStateScope.EXCLUSIVE, primaryStreamFactory);
    }

    static class FullRocksDBSnapshotResources<K> implements FullSnapshotResources<K> {
        private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;
        private final ResourceGuard.Lease lease;
        private final Snapshot snapshot;
        private final RocksDB db;
        private final List<MetaData> metaData;

        /** Number of bytes in the key-group prefix. */
        @Nonnegative private final int keyGroupPrefixBytes;

        private final KeyGroupRange keyGroupRange;
        private final TypeSerializer<K> keySerializer;
        private final StreamCompressionDecorator streamCompressionDecorator;

        public FullRocksDBSnapshotResources(
                ResourceGuard.Lease lease,
                Snapshot snapshot,
                List<RocksDbKvStateInfo> metaDataCopy,
                List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
                RocksDB db,
                int keyGroupPrefixBytes,
                KeyGroupRange keyGroupRange,
                TypeSerializer<K> keySerializer,
                StreamCompressionDecorator streamCompressionDecorator) {
            this.lease = lease;
            this.snapshot = snapshot;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
            this.db = db;
            this.keyGroupPrefixBytes = keyGroupPrefixBytes;
            this.keyGroupRange = keyGroupRange;
            this.keySerializer = keySerializer;
            this.streamCompressionDecorator = streamCompressionDecorator;

            // we need to to this in the constructor, i.e. in the synchronous part of the snapshot
            // TODO: better yet, we can do it outside the constructor
            this.metaData = fillMetaData(metaDataCopy);
        }

        private List<MetaData> fillMetaData(List<RocksDbKvStateInfo> metaDataCopy) {
            List<MetaData> metaData = new ArrayList<>(metaDataCopy.size());
            for (RocksDbKvStateInfo rocksDbKvStateInfo : metaDataCopy) {
                StateSnapshotTransformer<byte[]> stateSnapshotTransformer = null;
                if (rocksDbKvStateInfo.metaInfo instanceof RegisteredKeyValueStateBackendMetaInfo) {
                    stateSnapshotTransformer =
                            ((RegisteredKeyValueStateBackendMetaInfo<?, ?>)
                                            rocksDbKvStateInfo.metaInfo)
                                    .getStateSnapshotTransformFactory()
                                    .createForSerializedState()
                                    .orElse(null);
                }
                metaData.add(new MetaData(rocksDbKvStateInfo, stateSnapshotTransformer));
            }
            return metaData;
        }

        @Override
        public KeyValueStateIterator createKVStateIterator() throws IOException {
            CloseableRegistry closeableRegistry = new CloseableRegistry();

            try {
                ReadOptions readOptions = new ReadOptions();
                closeableRegistry.registerCloseable(readOptions::close);
                readOptions.setSnapshot(snapshot);

                List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators =
                        createKVStateIterators(closeableRegistry, readOptions);

                // Here we transfer ownership of the required resources to the
                // RocksStatesPerKeyGroupMergeIterator
                return new RocksStatesPerKeyGroupMergeIterator(
                        closeableRegistry, new ArrayList<>(kvStateIterators), keyGroupPrefixBytes);
            } catch (Throwable t) {
                // If anything goes wrong, clean up our stuff. If things went smoothly the
                // merging iterator is now responsible for closing the resources
                IOUtils.closeQuietly(closeableRegistry);
                throw new IOException("Error creating merge iterator", t);
            }
        }

        private List<Tuple2<RocksIteratorWrapper, Integer>> createKVStateIterators(
                CloseableRegistry closeableRegistry, ReadOptions readOptions) throws IOException {

            final List<Tuple2<RocksIteratorWrapper, Integer>> kvStateIterators =
                    new ArrayList<>(metaData.size());

            int kvStateId = 0;

            for (MetaData metaDataEntry : metaData) {
                RocksIteratorWrapper rocksIteratorWrapper =
                        createRocksIteratorWrapper(
                                db,
                                metaDataEntry.rocksDbKvStateInfo.columnFamilyHandle,
                                metaDataEntry.stateSnapshotTransformer,
                                readOptions);
                kvStateIterators.add(Tuple2.of(rocksIteratorWrapper, kvStateId));
                closeableRegistry.registerCloseable(rocksIteratorWrapper);
                ++kvStateId;
            }

            return kvStateIterators;
        }

        private static RocksIteratorWrapper createRocksIteratorWrapper(
                RocksDB db,
                ColumnFamilyHandle columnFamilyHandle,
                StateSnapshotTransformer<byte[]> stateSnapshotTransformer,
                ReadOptions readOptions) {
            RocksIterator rocksIterator = db.newIterator(columnFamilyHandle, readOptions);
            return stateSnapshotTransformer == null
                    ? new RocksIteratorWrapper(rocksIterator)
                    : new RocksTransformingIteratorWrapper(rocksIterator, stateSnapshotTransformer);
        }

        @Override
        public List<StateMetaInfoSnapshot> getMetaInfoSnapshots() {
            return stateMetaInfoSnapshots;
        }

        @Override
        public KeyGroupRange getKeyGroupRange() {
            return keyGroupRange;
        }

        @Override
        public TypeSerializer<K> getKeySerializer() {
            return keySerializer;
        }

        @Override
        public StreamCompressionDecorator getStreamCompressionDecorator() {
            return streamCompressionDecorator;
        }

        @Override
        public void release() {
            db.releaseSnapshot(snapshot);
            IOUtils.closeQuietly(snapshot);
            IOUtils.closeQuietly(lease);
        }

        private static class MetaData {
            final RocksDbKvStateInfo rocksDbKvStateInfo;
            final StateSnapshotTransformer<byte[]> stateSnapshotTransformer;

            private MetaData(
                    RocksDbKvStateInfo rocksDbKvStateInfo,
                    StateSnapshotTransformer<byte[]> stateSnapshotTransformer) {

                this.rocksDbKvStateInfo = rocksDbKvStateInfo;
                this.stateSnapshotTransformer = stateSnapshotTransformer;
            }
        }
    }
}
