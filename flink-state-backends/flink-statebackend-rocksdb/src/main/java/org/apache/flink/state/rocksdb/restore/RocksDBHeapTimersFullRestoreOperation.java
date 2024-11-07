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

package org.apache.flink.state.rocksdb.restore;

import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.KeyExtractorFunction;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.Keyed;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PriorityComparable;
import org.apache.flink.runtime.state.RegisteredPriorityQueueStateBackendMetaInfo;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueElement;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSet;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSetFactory;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot.BackendStateType;
import org.apache.flink.runtime.state.restore.FullSnapshotRestoreOperation;
import org.apache.flink.runtime.state.restore.KeyGroup;
import org.apache.flink.runtime.state.restore.KeyGroupEntry;
import org.apache.flink.runtime.state.restore.SavepointRestoreResult;
import org.apache.flink.runtime.state.restore.ThrowingIterator;
import org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.state.rocksdb.RocksDBNativeMetricOptions;
import org.apache.flink.state.rocksdb.RocksDBWriteBatchWrapper;
import org.apache.flink.state.rocksdb.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Encapsulates the process of restoring a RocksDB instance from a full snapshot. */
public class RocksDBHeapTimersFullRestoreOperation<K> implements RocksDBRestoreOperation {
    private final FullSnapshotRestoreOperation<K> savepointRestoreOperation;
    /** Write batch size used in {@link RocksDBWriteBatchWrapper}. */
    private final long writeBatchSize;

    private final LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>>
            registeredPQStates;
    private final HeapPriorityQueueSetFactory priorityQueueFactory;
    private final int numberOfKeyGroups;
    private final DataInputDeserializer deserializer = new DataInputDeserializer();

    private final RocksDBHandle rocksHandle;
    private final KeyGroupRange keyGroupRange;
    private final int keyGroupPrefixBytes;
    private final ICloseableRegistry cancelStreamRegistryForRestore;

    public RocksDBHeapTimersFullRestoreOperation(
            KeyGroupRange keyGroupRange,
            int numberOfKeyGroups,
            ClassLoader userCodeClassLoader,
            Map<String, RocksDbKvStateInfo> kvStateInformation,
            LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>> registeredPQStates,
            HeapPriorityQueueSetFactory priorityQueueFactory,
            StateSerializerProvider<K> keySerializerProvider,
            File instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            RocksDBNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> restoreStateHandles,
            @Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nonnegative long writeBatchSize,
            Long writeBufferManagerCapacity,
            ICloseableRegistry cancelStreamRegistryForRestore) {
        this.writeBatchSize = writeBatchSize;
        this.rocksHandle =
                new RocksDBHandle(
                        kvStateInformation,
                        instanceRocksDBPath,
                        dbOptions,
                        columnFamilyOptionsFactory,
                        nativeMetricOptions,
                        metricGroup,
                        ttlCompactFiltersManager,
                        writeBufferManagerCapacity);
        this.savepointRestoreOperation =
                new FullSnapshotRestoreOperation<>(
                        keyGroupRange,
                        userCodeClassLoader,
                        restoreStateHandles,
                        keySerializerProvider);
        this.registeredPQStates = registeredPQStates;
        this.priorityQueueFactory = priorityQueueFactory;
        this.numberOfKeyGroups = numberOfKeyGroups;
        this.keyGroupRange = keyGroupRange;
        this.keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(
                        numberOfKeyGroups);
        this.cancelStreamRegistryForRestore = cancelStreamRegistryForRestore;
    }

    /** Restores all key-groups data that is referenced by the passed state handles. */
    @Override
    public RocksDBRestoreResult restore()
            throws IOException, StateMigrationException, RocksDBException {
        rocksHandle.openDB();
        try (ThrowingIterator<SavepointRestoreResult> restore =
                savepointRestoreOperation.restore()) {
            while (restore.hasNext()) {
                applyRestoreResult(restore.next());
            }
        }
        return new RocksDBRestoreResult(
                this.rocksHandle.getDb(),
                this.rocksHandle.getDefaultColumnFamilyHandle(),
                this.rocksHandle.getNativeMetricMonitor(),
                -1,
                null,
                null,
                null);
    }

    private void applyRestoreResult(SavepointRestoreResult savepointRestoreResult)
            throws IOException, RocksDBException, StateMigrationException {
        List<StateMetaInfoSnapshot> restoredMetaInfos =
                savepointRestoreResult.getStateMetaInfoSnapshots();
        Map<Integer, ColumnFamilyHandle> columnFamilyHandles = new HashMap<>();
        Map<Integer, HeapPriorityQueueSnapshotRestoreWrapper<?>> restoredPQStates = new HashMap<>();
        for (int i = 0; i < restoredMetaInfos.size(); i++) {
            StateMetaInfoSnapshot restoredMetaInfo = restoredMetaInfos.get(i);
            if (restoredMetaInfo.getBackendStateType() == BackendStateType.PRIORITY_QUEUE) {
                String stateName = restoredMetaInfo.getName();
                HeapPriorityQueueSnapshotRestoreWrapper<?> queueWrapper =
                        registeredPQStates.computeIfAbsent(
                                stateName,
                                key ->
                                        createInternal(
                                                new RegisteredPriorityQueueStateBackendMetaInfo<>(
                                                        restoredMetaInfo)));
                restoredPQStates.put(i, queueWrapper);
            } else {
                RocksDbKvStateInfo registeredStateCFHandle =
                        this.rocksHandle.getOrRegisterStateColumnFamilyHandle(
                                null, restoredMetaInfo, cancelStreamRegistryForRestore);
                columnFamilyHandles.put(i, registeredStateCFHandle.columnFamilyHandle);
            }
        }

        try (ThrowingIterator<KeyGroup> keyGroups = savepointRestoreResult.getRestoredKeyGroups()) {
            restoreKVStateData(keyGroups, columnFamilyHandles, restoredPQStates);
        }
    }

    /**
     * Restore the KV-state / ColumnFamily data for all key-groups referenced by the current state
     * handle.
     */
    private void restoreKVStateData(
            ThrowingIterator<KeyGroup> keyGroups,
            Map<Integer, ColumnFamilyHandle> columnFamilies,
            Map<Integer, HeapPriorityQueueSnapshotRestoreWrapper<?>> restoredPQStates)
            throws IOException, RocksDBException, StateMigrationException {
        // for all key-groups in the current state handle...
        try (RocksDBWriteBatchWrapper writeBatchWrapper =
                        new RocksDBWriteBatchWrapper(this.rocksHandle.getDb(), writeBatchSize);
                Closeable ignored =
                        cancelStreamRegistryForRestore.registerCloseableTemporarily(
                                writeBatchWrapper.getCancelCloseable())) {
            HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement> restoredPQ = null;
            ColumnFamilyHandle handle = null;
            while (keyGroups.hasNext()) {
                KeyGroup keyGroup = keyGroups.next();
                try (ThrowingIterator<KeyGroupEntry> groupEntries = keyGroup.getKeyGroupEntries()) {
                    int oldKvStateId = -1;
                    while (groupEntries.hasNext()) {
                        KeyGroupEntry groupEntry = groupEntries.next();
                        int kvStateId = groupEntry.getKvStateId();
                        if (kvStateId != oldKvStateId) {
                            oldKvStateId = kvStateId;
                            handle = columnFamilies.get(kvStateId);
                            restoredPQ = getRestoredPQ(restoredPQStates, kvStateId);
                        }
                        if (restoredPQ != null) {
                            restoreQueueElement(restoredPQ, groupEntry);
                        } else if (handle != null) {
                            writeBatchWrapper.put(
                                    handle, groupEntry.getKey(), groupEntry.getValue());
                        } else {
                            throw new IllegalStateException("Unknown state id: " + kvStateId);
                        }
                    }
                }
            }
        }
    }

    private void restoreQueueElement(
            HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement> restoredPQ,
            KeyGroupEntry groupEntry)
            throws IOException {
        deserializer.setBuffer(groupEntry.getKey());
        deserializer.skipBytesToRead(keyGroupPrefixBytes);
        HeapPriorityQueueElement queueElement =
                restoredPQ.getMetaInfo().getElementSerializer().deserialize(deserializer);
        restoredPQ.getPriorityQueue().add(queueElement);
    }

    @SuppressWarnings("unchecked")
    private HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement> getRestoredPQ(
            Map<Integer, HeapPriorityQueueSnapshotRestoreWrapper<?>> restoredPQStates,
            int kvStateId) {
        return (HeapPriorityQueueSnapshotRestoreWrapper<HeapPriorityQueueElement>)
                restoredPQStates.get(kvStateId);
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private <T extends HeapPriorityQueueElement & PriorityComparable<? super T> & Keyed<?>>
            HeapPriorityQueueSnapshotRestoreWrapper<T> createInternal(
                    RegisteredPriorityQueueStateBackendMetaInfo metaInfo) {

        final String stateName = metaInfo.getName();
        final HeapPriorityQueueSet<T> priorityQueue =
                priorityQueueFactory.create(stateName, metaInfo.getElementSerializer());

        return new HeapPriorityQueueSnapshotRestoreWrapper<>(
                priorityQueue,
                metaInfo,
                KeyExtractorFunction.forKeyedObjects(),
                keyGroupRange,
                numberOfKeyGroups);
    }

    @Override
    public void close() throws Exception {
        this.rocksHandle.close();
    }
}
