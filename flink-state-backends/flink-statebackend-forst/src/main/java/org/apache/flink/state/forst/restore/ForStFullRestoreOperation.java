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

package org.apache.flink.state.forst.restore;

import org.apache.flink.core.fs.ICloseableRegistry;
import org.apache.flink.core.fs.Path;
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
import org.apache.flink.state.forst.ForStDBTtlCompactFiltersManager;
import org.apache.flink.state.forst.ForStDBWriteBatchWrapper;
import org.apache.flink.state.forst.ForStNativeMetricOptions;
import org.apache.flink.state.forst.ForStOperationUtils;
import org.apache.flink.util.StateMigrationException;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.ColumnFamilyOptions;
import org.forstdb.DBOptions;
import org.forstdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/** Encapsulates the process of restoring a ForStDB instance from a full snapshot. */
public class ForStFullRestoreOperation<K> implements ForStRestoreOperation {

    private final FullSnapshotRestoreOperation<K> savepointRestoreOperation;
    private final long writeBatchSize;
    private final ForStHandle forstHandle;
    private final ICloseableRegistry cancelStreamRegistryForRestore;

    public ForStFullRestoreOperation(
            KeyGroupRange keyGroupRange,
            ClassLoader userCodeClassLoader,
            Map<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation,
            StateSerializerProvider<K> keySerializerProvider,
            Path instanceForStPath,
            DBOptions dbOptions,
            Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
            ForStNativeMetricOptions nativeMetricOptions,
            MetricGroup metricGroup,
            @Nonnull Collection<KeyedStateHandle> restoreStateHandles,
            @Nonnull ForStDBTtlCompactFiltersManager ttlCompactFiltersManager,
            @Nonnegative long writeBatchSize,
            Long writeBufferManagerCapacity,
            ICloseableRegistry cancelStreamRegistryForRestore) {
        this.writeBatchSize = writeBatchSize;
        this.cancelStreamRegistryForRestore = cancelStreamRegistryForRestore;
        this.forstHandle =
                new ForStHandle(
                        kvStateInformation,
                        instanceForStPath,
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
    }

    /** Restores all key-groups data that is referenced by the passed state handles. */
    @Override
    public ForStRestoreResult restore()
            throws IOException, StateMigrationException, RocksDBException {
        forstHandle.openDB();
        try (ThrowingIterator<SavepointRestoreResult> restore =
                savepointRestoreOperation.restore()) {
            while (restore.hasNext()) {
                applyRestoreResult(restore.next());
            }
        }
        return new ForStRestoreResult(
                this.forstHandle.getDb(),
                this.forstHandle.getDefaultColumnFamilyHandle(),
                this.forstHandle.getNativeMetricMonitor(),
                -1,
                null,
                null);
    }

    private void applyRestoreResult(SavepointRestoreResult savepointRestoreResult)
            throws IOException, RocksDBException, StateMigrationException {
        List<StateMetaInfoSnapshot> restoredMetaInfos =
                savepointRestoreResult.getStateMetaInfoSnapshots();
        Map<Integer, ColumnFamilyHandle> columnFamilyHandles = new HashMap<>();
        for (int i = 0; i < restoredMetaInfos.size(); i++) {
            ForStOperationUtils.ForStKvStateInfo registeredStateCFHandle =
                    this.forstHandle.getOrRegisterStateColumnFamilyHandle(
                            null, restoredMetaInfos.get(i), cancelStreamRegistryForRestore);
            columnFamilyHandles.put(i, registeredStateCFHandle.columnFamilyHandle);
        }

        try (ThrowingIterator<KeyGroup> keyGroups = savepointRestoreResult.getRestoredKeyGroups()) {
            restoreKVStateData(keyGroups, columnFamilyHandles);
        }
    }

    private void restoreKVStateData(
            ThrowingIterator<KeyGroup> keyGroups, Map<Integer, ColumnFamilyHandle> columnFamilies)
            throws IOException, RocksDBException, StateMigrationException {
        try (ForStDBWriteBatchWrapper writeBatchWrapper =
                        new ForStDBWriteBatchWrapper(this.forstHandle.getDb(), writeBatchSize);
                Closeable ignored =
                        cancelStreamRegistryForRestore.registerCloseableTemporarily(
                                writeBatchWrapper.getCancelCloseable())) {
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
                        }
                        writeBatchWrapper.put(handle, groupEntry.getKey(), groupEntry.getValue());
                    }
                }
            }
        }
    }

    @Override
    public void close() throws Exception {
        this.forstHandle.close();
    }
}
