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

package org.apache.flink.contrib.streaming.state.restore;

import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.restore.FullSnapshotRestoreOperation;
import org.apache.flink.runtime.state.restore.KeyGroup;
import org.apache.flink.runtime.state.restore.KeyGroupEntry;
import org.apache.flink.runtime.state.restore.SavepointRestoreResult;
import org.apache.flink.runtime.state.restore.ThrowingIterator;
import org.apache.flink.util.StateMigrationException;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Encapsulates the process of restoring a RocksDB instance from a full snapshot. */
public class RocksDBFullRestoreOperation<K> extends AbstractRocksDBRestoreOperation<K> {
    private final FullSnapshotRestoreOperation<K> savepointRestoreOperation;
    /** Write batch size used in {@link RocksDBWriteBatchWrapper}. */
    private final long writeBatchSize;

    public RocksDBFullRestoreOperation(
            KeyGroupRange keyGroupRange,
            int keyGroupPrefixBytes,
            int numberOfTransferringThreads,
            CloseableRegistry cancelStreamRegistry,
            ClassLoader userCodeClassLoader,
            Map<String, RocksDbKvStateInfo> kvStateInformation,
            StateSerializerProvider<K> keySerializerProvider,
            File instanceBasePath,
            File instanceRocksDBPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            RocksDBNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> restoreStateHandles,
            @Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nonnegative long writeBatchSize,
            Long writeBufferManagerCapacity) {
        super(
                keyGroupRange,
                keyGroupPrefixBytes,
                numberOfTransferringThreads,
                cancelStreamRegistry,
                userCodeClassLoader,
                kvStateInformation,
                keySerializerProvider,
                instanceBasePath,
                instanceRocksDBPath,
                dbOptions,
                columnFamilyOptionsFactory,
                nativeMetricOptions,
                metricGroup,
                restoreStateHandles,
                ttlCompactFiltersManager,
                writeBufferManagerCapacity);
        checkArgument(writeBatchSize >= 0, "Write batch size have to be no negative.");
        this.writeBatchSize = writeBatchSize;
        this.savepointRestoreOperation =
                new FullSnapshotRestoreOperation<>(
                        keyGroupRange,
                        userCodeClassLoader,
                        restoreStateHandles,
                        keySerializerProvider);
    }

    /** Restores all key-groups data that is referenced by the passed state handles. */
    @Override
    public RocksDBRestoreResult restore()
            throws IOException, StateMigrationException, RocksDBException {
        openDB();
        try (ThrowingIterator<SavepointRestoreResult> restore =
                savepointRestoreOperation.restore()) {
            while (restore.hasNext()) {
                applyRestoreResult(restore.next());
            }
        }
        return new RocksDBRestoreResult(
                this.db, defaultColumnFamilyHandle, nativeMetricMonitor, -1, null, null);
    }

    private void applyRestoreResult(SavepointRestoreResult savepointRestoreResult)
            throws IOException, RocksDBException, StateMigrationException {
        List<StateMetaInfoSnapshot> restoredMetaInfos =
                savepointRestoreResult.getStateMetaInfoSnapshots();
        List<ColumnFamilyHandle> columnFamilyHandles =
                restoredMetaInfos.stream()
                        .map(
                                stateMetaInfoSnapshot -> {
                                    RocksDbKvStateInfo registeredStateCFHandle =
                                            getOrRegisterStateColumnFamilyHandle(
                                                    null, stateMetaInfoSnapshot);
                                    return registeredStateCFHandle.columnFamilyHandle;
                                })
                        .collect(Collectors.toList());
        try (ThrowingIterator<KeyGroup> keyGroups = savepointRestoreResult.getRestoredKeyGroups()) {
            restoreKVStateData(keyGroups, columnFamilyHandles);
        }
    }

    /**
     * Restore the KV-state / ColumnFamily data for all key-groups referenced by the current state
     * handle.
     */
    private void restoreKVStateData(
            ThrowingIterator<KeyGroup> keyGroups, List<ColumnFamilyHandle> columnFamilies)
            throws IOException, RocksDBException, StateMigrationException {
        // for all key-groups in the current state handle...
        try (RocksDBWriteBatchWrapper writeBatchWrapper =
                new RocksDBWriteBatchWrapper(db, writeBatchSize)) {
            while (keyGroups.hasNext()) {
                KeyGroup keyGroup = keyGroups.next();
                try (ThrowingIterator<KeyGroupEntry> groupEntries = keyGroup.getKeyGroupEntries()) {
                    while (groupEntries.hasNext()) {
                        KeyGroupEntry groupEntry = groupEntries.next();
                        ColumnFamilyHandle handle = columnFamilies.get(groupEntry.getKvStateId());
                        writeBatchWrapper.put(handle, groupEntry.getKey(), groupEntry.getValue());
                    }
                }
            }
        }
    }
}
