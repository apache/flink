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
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.FullSnapshotAsyncWriter;
import org.apache.flink.runtime.state.FullSnapshotResources;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.heap.HeapPriorityQueueSnapshotRestoreWrapper;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.function.SupplierWithException;

import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.util.LinkedHashMap;

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

    private final LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>>
            registeredPQStates;

    public RocksFullSnapshotStrategy(
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard rocksDBResourceGuard,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation,
            @Nonnull
                    LinkedHashMap<String, HeapPriorityQueueSnapshotRestoreWrapper<?>>
                            registeredPQStates,
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
        this.registeredPQStates = registeredPQStates;
    }

    @Override
    public FullSnapshotResources<K> syncPrepareResources(long checkpointId) throws Exception {
        return RocksDBFullSnapshotResources.create(
                kvStateInformation,
                registeredPQStates,
                db,
                rocksDBResourceGuard,
                keyGroupRange,
                keySerializer,
                keyGroupPrefixBytes,
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
                checkpointOptions.getCheckpointType(),
                checkpointStreamSupplier,
                fullRocksDBSnapshotResources);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        // nothing to do.
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        // nothing to do.
    }

    @Override
    public void close() {
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
}
