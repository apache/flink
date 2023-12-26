/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.snapshot;

import org.apache.flink.api.common.state.StateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.contrib.streaming.state.RocksDBExtension;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.contrib.streaming.state.RocksDBStateUploader;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.TestLocalRecoveryConfig;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.ResourceGuard;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.flink.core.fs.Path.fromLocalFile;
import static org.apache.flink.core.fs.local.LocalFileSystem.getSharedInstance;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RocksIncrementalSnapshotStrategy}. */
class RocksIncrementalSnapshotStrategyTest {

    @TempDir public Path tmp;

    @RegisterExtension public RocksDBExtension rocksDBExtension = new RocksDBExtension();

    // Verify the next checkpoint is still incremental after a savepoint completed.
    @Test
    void testCheckpointIsIncremental() throws Exception {

        try (CloseableRegistry closeableRegistry = new CloseableRegistry();
                RocksIncrementalSnapshotStrategy checkpointSnapshotStrategy =
                        createSnapshotStrategy(closeableRegistry)) {
            FsCheckpointStreamFactory checkpointStreamFactory = createFsCheckpointStreamFactory();

            // make and notify checkpoint with id 1
            snapshot(1L, checkpointSnapshotStrategy, checkpointStreamFactory, closeableRegistry);
            checkpointSnapshotStrategy.notifyCheckpointComplete(1L);

            // notify savepoint with id 2
            checkpointSnapshotStrategy.notifyCheckpointComplete(2L);

            // make checkpoint with id 3
            IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle3 =
                    snapshot(
                            3L,
                            checkpointSnapshotStrategy,
                            checkpointStreamFactory,
                            closeableRegistry);

            // If 3rd checkpoint's full size > checkpointed size, it means 3rd checkpoint is
            // incremental.
            assertThat(incrementalRemoteKeyedStateHandle3.getStateSize())
                    .isGreaterThan(incrementalRemoteKeyedStateHandle3.getCheckpointedSize());
        }
    }

    public RocksIncrementalSnapshotStrategy createSnapshotStrategy(
            CloseableRegistry closeableRegistry) throws IOException, RocksDBException {

        ColumnFamilyHandle columnFamilyHandle = rocksDBExtension.createNewColumnFamily("test");
        RocksDB rocksDB = rocksDBExtension.getRocksDB();
        byte[] key = "checkpoint".getBytes();
        byte[] val = "incrementalTest".getBytes();
        rocksDB.put(columnFamilyHandle, key, val);

        // construct RocksIncrementalSnapshotStrategy
        long lastCompletedCheckpointId = -1L;
        ResourceGuard rocksDBResourceGuard = new ResourceGuard();
        SortedMap<Long, Collection<HandleAndLocalPath>> materializedSstFiles = new TreeMap<>();
        LinkedHashMap<String, RocksDBKeyedStateBackend.RocksDbKvStateInfo> kvStateInformation =
                new LinkedHashMap<>();

        RocksDBStateUploader rocksDBStateUploader =
                new RocksDBStateUploader(
                        RocksDBOptions.CHECKPOINT_TRANSFER_THREAD_NUM.defaultValue());

        int keyGroupPrefixBytes =
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(2);

        RegisteredKeyValueStateBackendMetaInfo<Integer, ArrayList<Integer>> metaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        StateDescriptor.Type.VALUE,
                        "test",
                        IntSerializer.INSTANCE,
                        new ArrayListSerializer<>(IntSerializer.INSTANCE));

        RocksDBKeyedStateBackend.RocksDbKvStateInfo rocksDbKvStateInfo =
                new RocksDBKeyedStateBackend.RocksDbKvStateInfo(columnFamilyHandle, metaInfo);
        kvStateInformation.putIfAbsent("test", rocksDbKvStateInfo);

        return new RocksIncrementalSnapshotStrategy<>(
                rocksDB,
                rocksDBResourceGuard,
                IntSerializer.INSTANCE,
                kvStateInformation,
                new KeyGroupRange(0, 1),
                keyGroupPrefixBytes,
                TestLocalRecoveryConfig.disabled(),
                closeableRegistry,
                TempDirUtils.newFolder(tmp),
                UUID.randomUUID(),
                materializedSstFiles,
                rocksDBStateUploader,
                lastCompletedCheckpointId);
    }

    public FsCheckpointStreamFactory createFsCheckpointStreamFactory() throws IOException {
        int threshold = 100;
        File checkpointsDir = TempDirUtils.newFolder(tmp, "checkpointsDir");
        File sharedStateDir = TempDirUtils.newFolder(tmp, "sharedStateDir");
        return new FsCheckpointStreamFactory(
                getSharedInstance(),
                fromLocalFile(checkpointsDir),
                fromLocalFile(sharedStateDir),
                threshold,
                threshold);
    }

    public IncrementalRemoteKeyedStateHandle snapshot(
            long checkpointId,
            RocksIncrementalSnapshotStrategy checkpointSnapshotStrategy,
            FsCheckpointStreamFactory checkpointStreamFactory,
            CloseableRegistry closeableRegistry)
            throws Exception {

        RocksIncrementalSnapshotStrategy.NativeRocksDBSnapshotResources snapshotResources =
                checkpointSnapshotStrategy.syncPrepareResources(checkpointId);

        return (IncrementalRemoteKeyedStateHandle)
                checkpointSnapshotStrategy
                        .asyncSnapshot(
                                snapshotResources,
                                checkpointId,
                                checkpointId,
                                checkpointStreamFactory,
                                CheckpointOptions.forCheckpointWithDefaultLocation())
                        .get(closeableRegistry)
                        .getJobManagerOwnedSnapshot();
    }
}
