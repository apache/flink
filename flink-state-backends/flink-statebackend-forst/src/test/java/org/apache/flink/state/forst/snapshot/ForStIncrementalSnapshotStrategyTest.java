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

package org.apache.flink.state.forst.snapshot;

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.ArrayListSerializer;
import org.apache.flink.runtime.state.CompositeKeySerializationUtils;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory;
import org.apache.flink.runtime.state.v2.RegisteredKeyValueStateBackendMetaInfo;
import org.apache.flink.runtime.state.v2.StateDescriptor;
import org.apache.flink.state.forst.ForStExtension;
import org.apache.flink.state.forst.ForStKeyedStateBackend;
import org.apache.flink.state.forst.ForStStateDataTransfer;
import org.apache.flink.testutils.junit.utils.TempDirUtils;

import org.forstdb.ColumnFamilyHandle;
import org.forstdb.RocksDB;
import org.forstdb.RocksDBException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.flink.core.fs.Path.fromLocalFile;
import static org.apache.flink.core.fs.local.LocalFileSystem.getSharedInstance;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ForStIncrementalSnapshotStrategy}. */
class ForStIncrementalSnapshotStrategyTest {

    @TempDir public Path tmp;

    @RegisterExtension public ForStExtension forstExtension = new ForStExtension();

    // Verify the next checkpoint is still incremental after a savepoint completed.
    @Test
    void testCheckpointIsIncremental() throws Exception {

        try (CloseableRegistry closeableRegistry = new CloseableRegistry();
                ForStIncrementalSnapshotStrategy<?> checkpointSnapshotStrategy =
                        createSnapshotStrategy()) {
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

    private ForStIncrementalSnapshotStrategy<?> createSnapshotStrategy()
            throws IOException, RocksDBException {

        ColumnFamilyHandle columnFamilyHandle = forstExtension.createNewColumnFamily("test");
        RocksDB db = forstExtension.getDB();
        byte[] key = "checkpoint".getBytes();
        byte[] val = "incrementalTest".getBytes();
        db.put(columnFamilyHandle, key, val);

        RegisteredKeyValueStateBackendMetaInfo<Integer, ArrayList<Integer>> metaInfo =
                new RegisteredKeyValueStateBackendMetaInfo<>(
                        "test",
                        StateDescriptor.Type.VALUE,
                        IntSerializer.INSTANCE,
                        new ArrayListSerializer<>(IntSerializer.INSTANCE));
        LinkedHashMap<String, ForStKeyedStateBackend.ForStKvStateInfo> kvStateInformation =
                new LinkedHashMap<>();
        kvStateInformation.putIfAbsent(
                "test", new ForStKeyedStateBackend.ForStKvStateInfo(columnFamilyHandle, metaInfo));

        return new ForStIncrementalSnapshotStrategy<>(
                db,
                forstExtension.getResourceGuard(),
                forstExtension.getResourceContainer(),
                IntSerializer.INSTANCE,
                kvStateInformation,
                new KeyGroupRange(0, 1),
                CompositeKeySerializationUtils.computeRequiredBytesInKeyGroupPrefix(2),
                UUID.randomUUID(),
                new TreeMap<>(),
                new ForStStateDataTransfer(ForStStateDataTransfer.DEFAULT_THREAD_NUM),
                -1);
    }

    private FsCheckpointStreamFactory createFsCheckpointStreamFactory() throws IOException {
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

    private IncrementalRemoteKeyedStateHandle snapshot(
            long checkpointId,
            ForStIncrementalSnapshotStrategy<?> snapshotStrategy,
            FsCheckpointStreamFactory checkpointStreamFactory,
            CloseableRegistry closeableRegistry)
            throws Exception {

        ForStIncrementalSnapshotStrategy.ForStNativeSnapshotResources snapshotResources =
                snapshotStrategy.syncPrepareResources(checkpointId);

        return (IncrementalRemoteKeyedStateHandle)
                snapshotStrategy
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
