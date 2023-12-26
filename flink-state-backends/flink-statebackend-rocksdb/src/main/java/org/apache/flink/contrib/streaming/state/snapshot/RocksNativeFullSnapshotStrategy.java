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
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBStateUploader;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.RocksDB;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * Snapshot strategy for {@link RocksDBKeyedStateBackend} based on RocksDB's native checkpoints and
 * creates full snapshots. the difference between savepoint is that sst files will be uploaded
 * rather than states.
 *
 * @param <K> type of the backend keys.
 */
public class RocksNativeFullSnapshotStrategy<K>
        extends RocksDBSnapshotStrategyBase<
                K, RocksDBSnapshotStrategyBase.NativeRocksDBSnapshotResources> {

    private static final String DESCRIPTION = "Asynchronous full RocksDB snapshot";

    /** The help class used to upload state files. */
    private final RocksDBStateUploader stateUploader;

    public RocksNativeFullSnapshotStrategy(
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard rocksDBResourceGuard,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull LocalRecoveryConfig localRecoveryConfig,
            @Nonnull File instanceBasePath,
            @Nonnull UUID backendUID,
            @Nonnull RocksDBStateUploader rocksDBStateUploader) {
        super(
                DESCRIPTION,
                db,
                rocksDBResourceGuard,
                keySerializer,
                kvStateInformation,
                keyGroupRange,
                keyGroupPrefixBytes,
                localRecoveryConfig,
                instanceBasePath,
                backendUID);
        this.stateUploader = rocksDBStateUploader;
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            NativeRocksDBSnapshotResources snapshotResources,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory checkpointStreamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        if (snapshotResources.stateMetaInfoSnapshots.isEmpty()) {
            return registry -> SnapshotResult.empty();
        }

        return new RocksDBNativeFullSnapshotOperation(
                checkpointId,
                checkpointStreamFactory,
                snapshotResources.snapshotDirectory,
                snapshotResources.stateMetaInfoSnapshots);
    }

    @Override
    public void notifyCheckpointComplete(long completedCheckpointId) {
        // nothing to do
    }

    @Override
    public void notifyCheckpointAborted(long abortedCheckpointId) {
        // nothing to do
    }

    @Override
    protected PreviousSnapshot snapshotMetaData(
            long checkpointId, @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
        for (Map.Entry<String, RocksDbKvStateInfo> stateMetaInfoEntry :
                kvStateInformation.entrySet()) {
            stateMetaInfoSnapshots.add(stateMetaInfoEntry.getValue().metaInfo.snapshot());
        }
        return EMPTY_PREVIOUS_SNAPSHOT;
    }

    @Override
    public void close() {
        stateUploader.close();
    }

    /** Encapsulates the process to perform a full snapshot of a RocksDBKeyedStateBackend. */
    private final class RocksDBNativeFullSnapshotOperation extends RocksDBSnapshotOperation {

        private RocksDBNativeFullSnapshotOperation(
                long checkpointId,
                @Nonnull CheckpointStreamFactory checkpointStreamFactory,
                @Nonnull SnapshotDirectory localBackupDirectory,
                @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {

            super(
                    checkpointId,
                    checkpointStreamFactory,
                    localBackupDirectory,
                    stateMetaInfoSnapshots);
        }

        @Override
        public SnapshotResult<KeyedStateHandle> get(CloseableRegistry snapshotCloseableRegistry)
                throws Exception {

            boolean completed = false;

            // Handle to the meta data file
            SnapshotResult<StreamStateHandle> metaStateHandle = null;
            // Handles to all the files in the current snapshot will go here
            final List<HandleAndLocalPath> privateFiles = new ArrayList<>();

            try {

                metaStateHandle =
                        materializeMetaData(
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry,
                                stateMetaInfoSnapshots,
                                checkpointId,
                                checkpointStreamFactory);

                // Sanity checks - they should never fail
                Preconditions.checkNotNull(metaStateHandle, "Metadata was not properly created.");
                Preconditions.checkNotNull(
                        metaStateHandle.getJobManagerOwnedSnapshot(),
                        "Metadata for job manager was not properly created.");

                long checkpointedSize = metaStateHandle.getStateSize();

                checkpointedSize +=
                        uploadSnapshotFiles(
                                privateFiles, snapshotCloseableRegistry, tmpResourcesRegistry);

                final IncrementalRemoteKeyedStateHandle jmIncrementalKeyedStateHandle =
                        new IncrementalRemoteKeyedStateHandle(
                                backendUID,
                                keyGroupRange,
                                checkpointId,
                                Collections.emptyList(),
                                privateFiles,
                                metaStateHandle.getJobManagerOwnedSnapshot(),
                                checkpointedSize);

                Optional<KeyedStateHandle> localSnapshot =
                        getLocalSnapshot(
                                metaStateHandle.getTaskLocalSnapshot(), Collections.emptyList());
                final SnapshotResult<KeyedStateHandle> snapshotResult =
                        localSnapshot
                                .map(
                                        keyedStateHandle ->
                                                SnapshotResult.withLocalState(
                                                        jmIncrementalKeyedStateHandle,
                                                        keyedStateHandle))
                                .orElseGet(() -> SnapshotResult.of(jmIncrementalKeyedStateHandle));

                completed = true;

                return snapshotResult;
            } finally {
                if (!completed) {
                    cleanupIncompleteSnapshot(tmpResourcesRegistry, localBackupDirectory);
                }
            }
        }

        /** upload files and return total uploaded size. */
        private long uploadSnapshotFiles(
                @Nonnull List<HandleAndLocalPath> privateFiles,
                @Nonnull CloseableRegistry snapshotCloseableRegistry,
                @Nonnull CloseableRegistry tmpResourcesRegistry)
                throws Exception {

            // write state data
            Preconditions.checkState(localBackupDirectory.exists());

            Path[] files = localBackupDirectory.listDirectory();
            long uploadedSize = 0;
            if (files != null) {
                // all sst files are private in full snapshot
                List<HandleAndLocalPath> uploadedFiles =
                        stateUploader.uploadFilesToCheckpointFs(
                                Arrays.asList(files),
                                checkpointStreamFactory,
                                CheckpointedStateScope.EXCLUSIVE,
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry);

                uploadedSize += uploadedFiles.stream().mapToLong(e -> e.getStateSize()).sum();

                privateFiles.addAll(uploadedFiles);
            }
            return uploadedSize;
        }
    }
}
