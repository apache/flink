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
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateObject;
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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;
import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.getUploadedStateSize;

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
     * Stores the {@link StateHandleID IDs} of uploaded SST files that build the incremental
     * history. Once the checkpoint is confirmed by JM, only the ID paired with {@link
     * PlaceholderStreamStateHandle} can be sent.
     */
    @Nonnull private final SortedMap<Long, Map<StateHandleID, Long>> uploadedStateIDs;

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
            @Nonnull SortedMap<Long, Map<StateHandleID, StreamStateHandle>> uploadedStateHandles,
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
        this.uploadedStateIDs = new TreeMap<>();
        for (Map.Entry<Long, Map<StateHandleID, StreamStateHandle>> entry :
                uploadedStateHandles.entrySet()) {
            Map<StateHandleID, Long> map = new HashMap<>();
            for (Map.Entry<StateHandleID, StreamStateHandle> stateHandleEntry :
                    entry.getValue().entrySet()) {
                map.put(stateHandleEntry.getKey(), stateHandleEntry.getValue().getStateSize());
            }
            this.uploadedStateIDs.put(entry.getKey(), map);
        }
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
        synchronized (uploadedStateIDs) {
            // FLINK-23949: materializedSstFiles.keySet().contains(completedCheckpointId) make sure
            // the notified checkpointId is not a savepoint, otherwise next checkpoint will
            // degenerate into a full checkpoint
            if (completedCheckpointId > lastCompletedCheckpointId
                    && uploadedStateIDs.containsKey(completedCheckpointId)) {
                uploadedStateIDs
                        .keySet()
                        .removeIf(checkpointId -> checkpointId < completedCheckpointId);
                lastCompletedCheckpointId = completedCheckpointId;
            }
        }
    }

    @Override
    public void notifyCheckpointAborted(long abortedCheckpointId) {
        synchronized (uploadedStateIDs) {
            uploadedStateIDs.keySet().remove(abortedCheckpointId);
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
        final Map<StateHandleID, Long> confirmedSstFiles;
        final Map<StateHandleID, StreamStateHandle> uploadedSstFiles;

        // use the last completed checkpoint as the comparison base.
        synchronized (uploadedStateIDs) {
            lastCompletedCheckpoint = lastCompletedCheckpointId;
            confirmedSstFiles = uploadedStateIDs.get(lastCompletedCheckpoint);
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
            final Map<StateHandleID, StreamStateHandle> sstFiles = new HashMap<>();
            // Handles to the misc files in the current snapshot will go here
            final Map<StateHandleID, StreamStateHandle> miscFiles = new HashMap<>();

            try {

                metaStateHandle =
                        materializeMetaData(
                                snapshotCloseableRegistry,
                                stateMetaInfoSnapshots,
                                checkpointId,
                                checkpointStreamFactory);

                // Sanity checks - they should never fail
                Preconditions.checkNotNull(metaStateHandle, "Metadata was not properly created.");
                Preconditions.checkNotNull(
                        metaStateHandle.getJobManagerOwnedSnapshot(),
                        "Metadata for job manager was not properly created.");

                uploadSstFiles(sstFiles, miscFiles, snapshotCloseableRegistry);
                long checkpointedSize = metaStateHandle.getStateSize();
                checkpointedSize += getUploadedStateSize(sstFiles.values());
                checkpointedSize += getUploadedStateSize(miscFiles.values());

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
                    final List<StateObject> statesToDiscard =
                            new ArrayList<>(1 + miscFiles.size() + sstFiles.size());
                    statesToDiscard.add(metaStateHandle);
                    statesToDiscard.addAll(miscFiles.values());
                    statesToDiscard.addAll(sstFiles.values());
                    cleanupIncompleteSnapshot(statesToDiscard, localBackupDirectory);
                }
            }
        }

        private void uploadSstFiles(
                @Nonnull Map<StateHandleID, StreamStateHandle> sstFiles,
                @Nonnull Map<StateHandleID, StreamStateHandle> miscFiles,
                @Nonnull CloseableRegistry snapshotCloseableRegistry)
                throws Exception {

            // write state data
            Preconditions.checkState(localBackupDirectory.exists());

            Map<StateHandleID, Path> sstFilePaths = new HashMap<>();
            Map<StateHandleID, Path> miscFilePaths = new HashMap<>();

            Path[] files = localBackupDirectory.listDirectory();
            if (files != null) {
                createUploadFilePaths(files, sstFiles, sstFilePaths, miscFilePaths);

                final CheckpointedStateScope stateScope =
                        sharingFilesStrategy == SnapshotType.SharingFilesStrategy.NO_SHARING
                                ? CheckpointedStateScope.EXCLUSIVE
                                : CheckpointedStateScope.SHARED;
                sstFiles.putAll(
                        stateUploader.uploadFilesToCheckpointFs(
                                sstFilePaths,
                                checkpointStreamFactory,
                                stateScope,
                                snapshotCloseableRegistry));
                miscFiles.putAll(
                        stateUploader.uploadFilesToCheckpointFs(
                                miscFilePaths,
                                checkpointStreamFactory,
                                stateScope,
                                snapshotCloseableRegistry));

                synchronized (uploadedStateIDs) {
                    switch (sharingFilesStrategy) {
                        case FORWARD_BACKWARD:
                        case FORWARD:
                            uploadedStateIDs.put(
                                    checkpointId,
                                    sstFiles.entrySet().stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            Map.Entry::getKey,
                                                            t -> t.getValue().getStateSize())));
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
        }

        private void createUploadFilePaths(
                Path[] files,
                Map<StateHandleID, StreamStateHandle> sstFiles,
                Map<StateHandleID, Path> sstFilePaths,
                Map<StateHandleID, Path> miscFilePaths) {
            for (Path filePath : files) {
                final String fileName = filePath.getFileName().toString();
                final StateHandleID stateHandleID = new StateHandleID(fileName);

                if (fileName.endsWith(SST_FILE_SUFFIX)) {
                    Optional<StreamStateHandle> uploaded =
                            previousSnapshot.getUploaded(stateHandleID);
                    if (uploaded.isPresent()) {
                        sstFiles.put(stateHandleID, uploaded.get());
                    } else {
                        sstFilePaths.put(stateHandleID, filePath); // re-upload
                    }
                } else {
                    miscFilePaths.put(stateHandleID, filePath);
                }
            }
        }
    }
}
