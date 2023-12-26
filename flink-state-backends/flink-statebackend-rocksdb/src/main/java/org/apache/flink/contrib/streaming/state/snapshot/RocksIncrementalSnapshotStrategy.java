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
import org.apache.flink.contrib.streaming.state.RocksDBStateUploader;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;

/**
 * Snapshot strategy for {@link org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend}
 * that is based on RocksDB's native checkpoints and creates incremental snapshots.
 *
 * @param <K> type of the backend keys.
 */
public class RocksIncrementalSnapshotStrategy<K>
        extends RocksDBSnapshotStrategyBase<
                K, RocksDBSnapshotStrategyBase.NativeRocksDBSnapshotResources> {

    private static final Logger LOG =
            LoggerFactory.getLogger(RocksIncrementalSnapshotStrategy.class);

    private static final String DESCRIPTION = "Asynchronous incremental RocksDB snapshot";

    /**
     * Stores the {@link StreamStateHandle} and corresponding local path of uploaded SST files that
     * build the incremental history. Once the checkpoint is confirmed by JM, they can be reused for
     * incremental checkpoint.
     */
    @Nonnull private final SortedMap<Long, Collection<HandleAndLocalPath>> uploadedSstFiles;

    /** The identifier of the last completed checkpoint. */
    private long lastCompletedCheckpointId;

    /** The help class used to upload state files. */
    private final RocksDBStateUploader stateUploader;

    public RocksIncrementalSnapshotStrategy(
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard rocksDBResourceGuard,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull LocalRecoveryConfig localRecoveryConfig,
            @Nonnull CloseableRegistry cancelStreamRegistry,
            @Nonnull File instanceBasePath,
            @Nonnull UUID backendUID,
            @Nonnull SortedMap<Long, Collection<HandleAndLocalPath>> uploadedStateHandles,
            @Nonnull RocksDBStateUploader rocksDBStateUploader,
            long lastCompletedCheckpointId) {

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

        this.uploadedSstFiles = new TreeMap<>(uploadedStateHandles);
        this.stateUploader = rocksDBStateUploader;
        this.lastCompletedCheckpointId = lastCompletedCheckpointId;
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            NativeRocksDBSnapshotResources snapshotResources,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory checkpointStreamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        if (snapshotResources.stateMetaInfoSnapshots.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning null.",
                        timestamp);
            }
            return registry -> SnapshotResult.empty();
        }

        final PreviousSnapshot previousSnapshot;
        final CheckpointType.SharingFilesStrategy sharingFilesStrategy =
                checkpointOptions.getCheckpointType().getSharingFilesStrategy();
        switch (sharingFilesStrategy) {
            case FORWARD_BACKWARD:
                previousSnapshot = snapshotResources.previousSnapshot;
                break;
            case FORWARD:
            case NO_SHARING:
                previousSnapshot = EMPTY_PREVIOUS_SNAPSHOT;
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported sharing files strategy: " + sharingFilesStrategy);
        }

        return new RocksDBIncrementalSnapshotOperation(
                checkpointId,
                checkpointStreamFactory,
                snapshotResources.snapshotDirectory,
                previousSnapshot,
                sharingFilesStrategy,
                snapshotResources.stateMetaInfoSnapshots);
    }

    @Override
    public void notifyCheckpointComplete(long completedCheckpointId) {
        synchronized (uploadedSstFiles) {
            // FLINK-23949: materializedSstFiles.keySet().contains(completedCheckpointId) make sure
            // the notified checkpointId is not a savepoint, otherwise next checkpoint will
            // degenerate into a full checkpoint
            if (completedCheckpointId > lastCompletedCheckpointId
                    && uploadedSstFiles.containsKey(completedCheckpointId)) {
                uploadedSstFiles
                        .keySet()
                        .removeIf(checkpointId -> checkpointId < completedCheckpointId);
                lastCompletedCheckpointId = completedCheckpointId;
            }
        }
    }

    @Override
    public void notifyCheckpointAborted(long abortedCheckpointId) {
        synchronized (uploadedSstFiles) {
            uploadedSstFiles.keySet().remove(abortedCheckpointId);
        }
    }

    @Override
    public void close() {
        stateUploader.close();
    }

    @Override
    protected PreviousSnapshot snapshotMetaData(
            long checkpointId, @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {

        final long lastCompletedCheckpoint;
        final Collection<HandleAndLocalPath> confirmedSstFiles;

        // use the last completed checkpoint as the comparison base.
        synchronized (uploadedSstFiles) {
            lastCompletedCheckpoint = lastCompletedCheckpointId;
            confirmedSstFiles = uploadedSstFiles.get(lastCompletedCheckpoint);
            LOG.trace(
                    "Use confirmed SST files for checkpoint {}: {}",
                    checkpointId,
                    confirmedSstFiles);
        }
        LOG.trace(
                "Taking incremental snapshot for checkpoint {}. Snapshot is based on last completed checkpoint {} "
                        + "assuming the following (shared) confirmed files as base: {}.",
                checkpointId,
                lastCompletedCheckpoint,
                confirmedSstFiles);

        // snapshot meta data to save
        for (Map.Entry<String, RocksDbKvStateInfo> stateMetaInfoEntry :
                kvStateInformation.entrySet()) {
            stateMetaInfoSnapshots.add(stateMetaInfoEntry.getValue().metaInfo.snapshot());
        }
        return new PreviousSnapshot(confirmedSstFiles);
    }

    /**
     * Encapsulates the process to perform an incremental snapshot of a RocksDBKeyedStateBackend.
     */
    private final class RocksDBIncrementalSnapshotOperation extends RocksDBSnapshotOperation {

        /** All sst files that were part of the last previously completed checkpoint. */
        @Nonnull private final PreviousSnapshot previousSnapshot;

        @Nonnull private final SnapshotType.SharingFilesStrategy sharingFilesStrategy;

        private RocksDBIncrementalSnapshotOperation(
                long checkpointId,
                @Nonnull CheckpointStreamFactory checkpointStreamFactory,
                @Nonnull SnapshotDirectory localBackupDirectory,
                @Nonnull PreviousSnapshot previousSnapshot,
                @Nonnull SnapshotType.SharingFilesStrategy sharingFilesStrategy,
                @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {

            super(
                    checkpointId,
                    checkpointStreamFactory,
                    localBackupDirectory,
                    stateMetaInfoSnapshots);
            this.previousSnapshot = previousSnapshot;
            this.sharingFilesStrategy = sharingFilesStrategy;
        }

        @Override
        public SnapshotResult<KeyedStateHandle> get(CloseableRegistry snapshotCloseableRegistry)
                throws Exception {

            boolean completed = false;

            // Handle to the meta data file
            SnapshotResult<StreamStateHandle> metaStateHandle = null;
            // Handles to new sst files since the last completed checkpoint will go here
            final List<HandleAndLocalPath> sstFiles = new ArrayList<>();
            // Handles to the misc files in the current snapshot will go here
            final List<HandleAndLocalPath> miscFiles = new ArrayList<>();

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
                                sstFiles,
                                miscFiles,
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry);

                // We make the 'sstFiles' as the 'sharedState' in IncrementalRemoteKeyedStateHandle,
                // whether they belong to the sharded CheckpointedStateScope or exclusive
                // CheckpointedStateScope.
                // In this way, the first checkpoint after job recovery can be an incremental
                // checkpoint in CLAIM mode, either restoring from checkpoint or restoring from
                // native savepoint.
                // And this has no effect on the registration of shareState currently, because the
                // snapshot result of native savepoint would not be registered into
                // 'SharedStateRegistry'.
                final IncrementalRemoteKeyedStateHandle jmIncrementalKeyedStateHandle =
                        new IncrementalRemoteKeyedStateHandle(
                                backendUID,
                                keyGroupRange,
                                checkpointId,
                                sstFiles,
                                miscFiles,
                                metaStateHandle.getJobManagerOwnedSnapshot(),
                                checkpointedSize);

                Optional<KeyedStateHandle> localSnapshot =
                        getLocalSnapshot(metaStateHandle.getTaskLocalSnapshot(), sstFiles);
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
                @Nonnull List<HandleAndLocalPath> sstFiles,
                @Nonnull List<HandleAndLocalPath> miscFiles,
                @Nonnull CloseableRegistry snapshotCloseableRegistry,
                @Nonnull CloseableRegistry tmpResourcesRegistry)
                throws Exception {

            // write state data
            Preconditions.checkState(localBackupDirectory.exists());

            Path[] files = localBackupDirectory.listDirectory();
            long uploadedSize = 0;
            if (files != null) {
                List<Path> sstFilePaths = new ArrayList<>(files.length);
                List<Path> miscFilePaths = new ArrayList<>(files.length);

                createUploadFilePaths(files, sstFiles, sstFilePaths, miscFilePaths);

                final CheckpointedStateScope stateScope =
                        sharingFilesStrategy == SnapshotType.SharingFilesStrategy.NO_SHARING
                                ? CheckpointedStateScope.EXCLUSIVE
                                : CheckpointedStateScope.SHARED;

                List<HandleAndLocalPath> sstFilesUploadResult =
                        stateUploader.uploadFilesToCheckpointFs(
                                sstFilePaths,
                                checkpointStreamFactory,
                                stateScope,
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry);
                uploadedSize +=
                        sstFilesUploadResult.stream().mapToLong(e -> e.getStateSize()).sum();
                sstFiles.addAll(sstFilesUploadResult);

                List<HandleAndLocalPath> miscFilesUploadResult =
                        stateUploader.uploadFilesToCheckpointFs(
                                miscFilePaths,
                                checkpointStreamFactory,
                                stateScope,
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry);
                uploadedSize +=
                        miscFilesUploadResult.stream().mapToLong(e -> e.getStateSize()).sum();
                miscFiles.addAll(miscFilesUploadResult);

                synchronized (uploadedSstFiles) {
                    switch (sharingFilesStrategy) {
                        case FORWARD_BACKWARD:
                        case FORWARD:
                            uploadedSstFiles.put(
                                    checkpointId, Collections.unmodifiableList(sstFiles));
                            break;
                        case NO_SHARING:
                            break;
                        default:
                            // This is just a safety precaution. It is checked before creating the
                            // RocksDBIncrementalSnapshotOperation
                            throw new IllegalArgumentException(
                                    "Unsupported sharing files strategy: " + sharingFilesStrategy);
                    }
                }
            }
            return uploadedSize;
        }

        private void createUploadFilePaths(
                Path[] files,
                List<HandleAndLocalPath> sstFiles,
                List<Path> sstFilePaths,
                List<Path> miscFilePaths) {
            for (Path filePath : files) {
                final String fileName = filePath.getFileName().toString();

                if (fileName.endsWith(SST_FILE_SUFFIX)) {
                    Optional<StreamStateHandle> uploaded = previousSnapshot.getUploaded(fileName);
                    if (uploaded.isPresent()) {
                        sstFiles.add(HandleAndLocalPath.of(uploaded.get(), fileName));
                    } else {
                        sstFilePaths.add(filePath); // re-upload
                    }
                } else {
                    miscFilePaths.add(filePath);
                }
            }
        }
    }
}
