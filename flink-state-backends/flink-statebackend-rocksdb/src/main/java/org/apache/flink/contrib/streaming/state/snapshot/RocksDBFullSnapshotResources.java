/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.iterator.RocksQueueIterator;
import org.apache.flink.contrib.streaming.state.iterator.RocksStatesPerKeyGroupMergeIterator;
import org.apache.flink.contrib.streaming.state.iterator.RocksTransformingIteratorWrapper;
import org.apache.flink.contrib.streaming.state.iterator.SingleStateIterator;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyValueStateIterator;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueStateSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;

import javax.annotation.Nonnegative;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** A {@link FullSnapshotResources} for the RocksDB backend. */
public class RocksDBFullSnapshotResources<K> implements FullSnapshotResources<K> {
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
    private final List<HeapPriorityQueueStateSnapshot<?>> heapPriorityQueuesSnapshots;

    public RocksDBFullSnapshotResources(
            ResourceGuard.Lease lease,
            Snapshot snapshot,
            List<RocksDBKeyedStateBackend.RocksDbKvStateInfo> metaDataCopy,
            List<HeapPriorityQueueStateSnapshot<?>> heapPriorityQueuesSnapshots,
            List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            RocksDB db,
            int keyGroupPrefixBytes,
            KeyGroupRange keyGroupRange,
            TypeSerializer<K> keySerializer,
            StreamCompressionDecorator streamCompressionDecorator) {
        this.lease = lease;
        this.snapshot = snapshot;
        this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
        this.heapPriorityQueuesSnapshots = heapPriorityQueuesSnapshots;
        this.db = db;
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.keyGroupRange = keyGroupRange;
        this.keySerializer = keySerializer;
        this.streamCompressionDecorator = streamCompressionDecorator;

        // we need to do this in the constructor, i.e. in the synchronous part of the snapshot
        // TODO: better yet, we can do it outside the constructor
        this.metaData = fillMetaData(metaDataCopy);
    }

    public static <K> RocksDBFullSnapshotResources<K> create(
            final LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo>
                    kvStateInformation,
            // TODO: was it important that this is a LinkedHashMap
            final Map<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            final RocksDB db,
            final ResourceGuard rocksDBResourceGuard,
            final KeyGroupRange keyGroupRange,
            final TypeSerializer<K> keySerializer,
            final int keyGroupPrefixBytes,
            final StreamCompressionDecorator keyGroupCompressionDecorator)
            throws IOException {

        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                new ArrayList<>(kvStateInformation.size());
        final List<RocksDBKeyedStateBackend.RocksDbKvStateInfo> metaDataCopy =
                new ArrayList<>(kvStateInformation.size());

        for (RocksDBKeyedStateBackend.RocksDbKvStateInfo stateInfo : kvStateInformation.values()) {
            // snapshot meta info
            stateMetaInfoSnapshots.add(stateInfo.metaInfo.snapshot());
            metaDataCopy.add(stateInfo);
        }

        List<HeapPriorityQueueStateSnapshot<?>> heapPriorityQueuesSnapshots =
                new ArrayList<>(registeredPQStates.size());
        for (HeapPriorityQueueSnapshotRestoreWrapper<?> stateInfo : registeredPQStates.values()) {
            stateMetaInfoSnapshots.add(stateInfo.getMetaInfo().snapshot());
            heapPriorityQueuesSnapshots.add(stateInfo.stateSnapshot());
        }

        final ResourceGuard.Lease lease = rocksDBResourceGuard.acquireResource();
        final Snapshot snapshot = db.getSnapshot();

        return new RocksDBFullSnapshotResources<>(
                lease,
                snapshot,
                metaDataCopy,
                heapPriorityQueuesSnapshots,
                stateMetaInfoSnapshots,
                db,
                keyGroupPrefixBytes,
                keyGroupRange,
                keySerializer,
                keyGroupCompressionDecorator);
    }

    private List<MetaData> fillMetaData(
            List<RocksDBKeyedStateBackend.RocksDbKvStateInfo> metaDataCopy) {
        List<MetaData> metaData = new ArrayList<>(metaDataCopy.size());
        for (RocksDBKeyedStateBackend.RocksDbKvStateInfo rocksDbKvStateInfo : metaDataCopy) {
            StateSnapshotTransformer<byte[]> stateSnapshotTransformer = null;
            if (rocksDbKvStateInfo.metaInfo instanceof RegisteredKeyValueStateBackendMetaInfo) {
                stateSnapshotTransformer =
                        ((RegisteredKeyValueStateBackendMetaInfo<?, ?>) rocksDbKvStateInfo.metaInfo)
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

            List<SingleStateIterator> heapPriorityQueueIterators =
                    createHeapPriorityQueueIterators();

            // Here we transfer ownership of the required resources to the
            // RocksStatesPerKeyGroupMergeIterator
            return new RocksStatesPerKeyGroupMergeIterator(
                    closeableRegistry,
                    kvStateIterators,
                    heapPriorityQueueIterators,
                    keyGroupPrefixBytes);
        } catch (Throwable t) {
            // If anything goes wrong, clean up our stuff. If things went smoothly the
            // merging iterator is now responsible for closing the resources
            IOUtils.closeQuietly(closeableRegistry);
            throw new IOException("Error creating merge iterator", t);
        }
    }

    private List<SingleStateIterator> createHeapPriorityQueueIterators() {
        int kvStateId = metaData.size();
        List<SingleStateIterator> queuesIterators =
                new ArrayList<>(heapPriorityQueuesSnapshots.size());
        for (HeapPriorityQueueStateSnapshot<?> queuesSnapshot : heapPriorityQueuesSnapshots) {
            queuesIterators.add(
                    new RocksQueueIterator(
                            queuesSnapshot, keyGroupRange, keyGroupPrefixBytes, kvStateId++));
        }
        return queuesIterators;
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
        final RocksDBKeyedStateBackend.RocksDbKvStateInfo rocksDbKvStateInfo;
        final StateSnapshotTransformer<byte[]> stateSnapshotTransformer;

        private MetaData(
                RocksDBKeyedStateBackend.RocksDbKvStateInfo rocksDbKvStateInfo,
                StateSnapshotTransformer<byte[]> stateSnapshotTransformer) {

            this.rocksDbKvStateInfo = rocksDbKvStateInfo;
            this.stateSnapshotTransformer = stateSnapshotTransformer;
        }
    }
}
