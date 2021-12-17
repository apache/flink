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
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.rocksdb.Checkpoint;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
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
                K, RocksIncrementalSnapshotStrategy.IncrementalRocksDBSnapshotResources> {

    private static final Logger LOG =
            LoggerFactory.getLogger(RocksIncrementalSnapshotStrategy.class);

    private static final String DESCRIPTION = "Asynchronous incremental RocksDB snapshot";

    /** Base path of the RocksDB instance. */
    @Nonnull private final File instanceBasePath;

    /** The state handle ids of all sst files materialized in snapshots for previous checkpoints. */
    @Nonnull private final UUID backendUID;

    /**
     * Stores the {@link StateHandleID IDs} of uploaded SST files that build the incremental
     * history. Once the checkpoint is confirmed by JM, only the ID paired with {@link
     * PlaceholderStreamStateHandle} can be sent. Until that, original {@link StreamStateHandle}
     * must be used (held in {@link #lastUploadedSstFiles}).
     */
    @Nonnull private final SortedMap<Long, Set<StateHandleID>> uploadedStateIDs;

    /**
     * Last uploaded but potentially not confirmed SST files. Used if {@link #uploadedStateIDs}
     * doesn't contain the corresponding {@link StateHandleID}.
     */
    @Nonnull private final Map<StateHandleID, StreamStateHandle> lastUploadedSstFiles;

    /** The identifier of the last completed checkpoint. */
    private long lastCompletedCheckpointId;

    /** The identifier of the checkpoint for which SST files were uploaded (grows monotonically). */
    private long lastCheckpointIdUploadedSst = -1;

    /** The help class used to upload state files. */
    private final RocksDBStateUploader stateUploader;

    /** The local directory name of the current snapshot strategy. */
    private final String localDirectoryName;

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
            @Nonnull SortedMap<Long, Set<StateHandleID>> uploadedStateIDs,
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
                localRecoveryConfig);

        this.instanceBasePath = instanceBasePath;
        this.backendUID = backendUID;
        this.uploadedStateIDs = uploadedStateIDs;
        this.stateUploader = rocksDBStateUploader;
        this.lastCompletedCheckpointId = lastCompletedCheckpointId;
        this.localDirectoryName = backendUID.toString().replaceAll("[\\-]", "");
        this.lastUploadedSstFiles = new HashMap<>();
    }

    @Override
    public IncrementalRocksDBSnapshotResources syncPrepareResources(long checkpointId)
            throws Exception {

        final SnapshotDirectory snapshotDirectory = prepareLocalSnapshotDirectory(checkpointId);
        LOG.trace("Local RocksDB checkpoint goes to backup path {}.", snapshotDirectory);

        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                new ArrayList<>(kvStateInformation.size());
        final PreviousSnapshot previousSnapshot =
                snapshotMetaData(checkpointId, stateMetaInfoSnapshots);

        takeDBNativeCheckpoint(snapshotDirectory);

        return new IncrementalRocksDBSnapshotResources(
                snapshotDirectory, previousSnapshot, stateMetaInfoSnapshots);
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            IncrementalRocksDBSnapshotResources snapshotResources,
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

        return new RocksDBIncrementalSnapshotOperation(
                checkpointId,
                checkpointStreamFactory,
                snapshotResources.snapshotDirectory,
                snapshotResources.previousSnapshot,
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

    @Nonnull
    private SnapshotDirectory prepareLocalSnapshotDirectory(long checkpointId) throws IOException {

        if (localRecoveryConfig.isLocalRecoveryEnabled()) {
            // create a "permanent" snapshot directory for local recovery.
            LocalRecoveryDirectoryProvider directoryProvider =
                    localRecoveryConfig.getLocalStateDirectoryProvider();
            File directory = directoryProvider.subtaskSpecificCheckpointDirectory(checkpointId);

            if (!directory.exists() && !directory.mkdirs()) {
                throw new IOException(
                        "Local state base directory for checkpoint "
                                + checkpointId
                                + " does not exist and could not be created: "
                                + directory);
            }

            // introduces an extra directory because RocksDB wants a non-existing directory for
            // native checkpoints.
            // append localDirectoryName here to solve directory collision problem when two stateful
            // operators chained in one task.
            File rdbSnapshotDir = new File(directory, localDirectoryName);
            if (rdbSnapshotDir.exists()) {
                FileUtils.deleteDirectory(rdbSnapshotDir);
            }

            Path path = rdbSnapshotDir.toPath();
            // create a "permanent" snapshot directory because local recovery is active.
            try {
                return SnapshotDirectory.permanent(path);
            } catch (IOException ex) {
                try {
                    FileUtils.deleteDirectory(directory);
                } catch (IOException delEx) {
                    ex = ExceptionUtils.firstOrSuppressed(delEx, ex);
                }
                throw ex;
            }
        } else {
            // create a "temporary" snapshot directory because local recovery is inactive.
            File snapshotDir = new File(instanceBasePath, "chk-" + checkpointId);
            return SnapshotDirectory.temporary(snapshotDir);
        }
    }

    private PreviousSnapshot snapshotMetaData(
            long checkpointId, @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {

        final long lastCompletedCheckpoint;
        final Set<StateHandleID> confirmedSstFiles;
        final Map<StateHandleID, StreamStateHandle> uploadedSstFiles;

        // use the last completed checkpoint as the comparison base.
        synchronized (uploadedStateIDs) {
            lastCompletedCheckpoint = lastCompletedCheckpointId;
            confirmedSstFiles = uploadedStateIDs.get(lastCompletedCheckpoint);
            uploadedSstFiles = new HashMap<>(lastUploadedSstFiles);
            LOG.trace(
                    "Use lastUploadedSstFiles for checkpoint {}: {}",
                    checkpointId,
                    uploadedSstFiles);
        }
        LOG.trace(
                "Taking incremental snapshot for checkpoint {}. Snapshot is based on last completed checkpoint {} "
                        + "assuming the following (shared) confirmed files as base: {}, uploaded: {}.",
                checkpointId,
                lastCompletedCheckpoint,
                confirmedSstFiles,
                uploadedSstFiles);

        // snapshot meta data to save
        for (Map.Entry<String, RocksDbKvStateInfo> stateMetaInfoEntry :
                kvStateInformation.entrySet()) {
            stateMetaInfoSnapshots.add(stateMetaInfoEntry.getValue().metaInfo.snapshot());
        }
        return new PreviousSnapshot(confirmedSstFiles, uploadedSstFiles);
    }

    private void takeDBNativeCheckpoint(@Nonnull SnapshotDirectory outputDirectory)
            throws Exception {
        // create hard links of living files in the output path
        try (ResourceGuard.Lease ignored = rocksDBResourceGuard.acquireResource();
                Checkpoint checkpoint = Checkpoint.create(db)) {
            checkpoint.createCheckpoint(outputDirectory.getDirectory().toString());
        } catch (Exception ex) {
            try {
                outputDirectory.cleanup();
            } catch (IOException cleanupEx) {
                ex = ExceptionUtils.firstOrSuppressed(cleanupEx, ex);
            }
            throw ex;
        }
    }

    /**
     * Encapsulates the process to perform an incremental snapshot of a RocksDBKeyedStateBackend.
     */
    private final class RocksDBIncrementalSnapshotOperation
            implements SnapshotResultSupplier<KeyedStateHandle> {

        /** Id for the current checkpoint. */
        private final long checkpointId;

        /** Stream factory that creates the output streams to DFS. */
        @Nonnull private final CheckpointStreamFactory checkpointStreamFactory;

        /** The state meta data. */
        @Nonnull private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        /** Local directory for the RocksDB native backup. */
        @Nonnull private final SnapshotDirectory localBackupDirectory;

        /** All sst files that were part of the last previously completed checkpoint. */
        @Nonnull private final PreviousSnapshot previousSnapshot;

        private RocksDBIncrementalSnapshotOperation(
                long checkpointId,
                @Nonnull CheckpointStreamFactory checkpointStreamFactory,
                @Nonnull SnapshotDirectory localBackupDirectory,
                @Nonnull PreviousSnapshot previousSnapshot,
                @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {

            this.checkpointStreamFactory = checkpointStreamFactory;
            this.previousSnapshot = previousSnapshot;
            this.checkpointId = checkpointId;
            this.localBackupDirectory = localBackupDirectory;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
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

                metaStateHandle = materializeMetaData(snapshotCloseableRegistry);

                // Sanity checks - they should never fail
                Preconditions.checkNotNull(metaStateHandle, "Metadata was not properly created.");
                Preconditions.checkNotNull(
                        metaStateHandle.getJobManagerOwnedSnapshot(),
                        "Metadata for job manager was not properly created.");

                uploadSstFiles(sstFiles, miscFiles, snapshotCloseableRegistry);

                final IncrementalRemoteKeyedStateHandle jmIncrementalKeyedStateHandle =
                        new IncrementalRemoteKeyedStateHandle(
                                backendUID,
                                keyGroupRange,
                                checkpointId,
                                sstFiles,
                                miscFiles,
                                metaStateHandle.getJobManagerOwnedSnapshot());

                final DirectoryStateHandle directoryStateHandle =
                        localBackupDirectory.completeSnapshotAndGetHandle();
                final SnapshotResult<KeyedStateHandle> snapshotResult;
                if (directoryStateHandle != null
                        && metaStateHandle.getTaskLocalSnapshot() != null) {

                    IncrementalLocalKeyedStateHandle localDirKeyedStateHandle =
                            new IncrementalLocalKeyedStateHandle(
                                    backendUID,
                                    checkpointId,
                                    directoryStateHandle,
                                    keyGroupRange,
                                    metaStateHandle.getTaskLocalSnapshot(),
                                    sstFiles.keySet());

                    snapshotResult =
                            SnapshotResult.withLocalState(
                                    jmIncrementalKeyedStateHandle, localDirKeyedStateHandle);
                } else {
                    snapshotResult = SnapshotResult.of(jmIncrementalKeyedStateHandle);
                }

                completed = true;

                return snapshotResult;
            } finally {
                if (!completed) {
                    final List<StateObject> statesToDiscard =
                            new ArrayList<>(1 + miscFiles.size() + sstFiles.size());
                    statesToDiscard.add(metaStateHandle);
                    statesToDiscard.addAll(miscFiles.values());
                    statesToDiscard.addAll(sstFiles.values());
                    cleanupIncompleteSnapshot(statesToDiscard);
                }
            }
        }

        private void cleanupIncompleteSnapshot(@Nonnull List<StateObject> statesToDiscard) {

            try {
                StateUtil.bestEffortDiscardAllStateObjects(statesToDiscard);
            } catch (Exception e) {
                LOG.warn("Could not properly discard states.", e);
            }

            if (localBackupDirectory.isSnapshotCompleted()) {
                try {
                    DirectoryStateHandle directoryStateHandle =
                            localBackupDirectory.completeSnapshotAndGetHandle();
                    if (directoryStateHandle != null) {
                        directoryStateHandle.discardState();
                    }
                } catch (Exception e) {
                    LOG.warn("Could not properly discard local state.", e);
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

                sstFiles.putAll(
                        stateUploader.uploadFilesToCheckpointFs(
                                sstFilePaths, checkpointStreamFactory, snapshotCloseableRegistry));
                miscFiles.putAll(
                        stateUploader.uploadFilesToCheckpointFs(
                                miscFilePaths, checkpointStreamFactory, snapshotCloseableRegistry));

                synchronized (uploadedStateIDs) {
                    // ignore an older upload if it completed after a newer one has completed
                    if (checkpointId > lastCheckpointIdUploadedSst) {
                        lastCheckpointIdUploadedSst = checkpointId;
                        lastUploadedSstFiles.clear();
                        LOG.trace(
                                "Update lastUploadedSstFiles for checkpoint {}: {}",
                                checkpointId,
                                sstFiles);
                        lastUploadedSstFiles.putAll(sstFiles);
                    }
                    uploadedStateIDs.put(checkpointId, sstFiles.keySet());
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

        @Nonnull
        private SnapshotResult<StreamStateHandle> materializeMetaData(
                @Nonnull CloseableRegistry snapshotCloseableRegistry) throws Exception {

            CheckpointStreamWithResultProvider streamWithResultProvider =
                    localRecoveryConfig.isLocalRecoveryEnabled()
                            ? CheckpointStreamWithResultProvider.createDuplicatingStream(
                                    checkpointId,
                                    CheckpointedStateScope.EXCLUSIVE,
                                    checkpointStreamFactory,
                                    localRecoveryConfig.getLocalStateDirectoryProvider())
                            : CheckpointStreamWithResultProvider.createSimpleStream(
                                    CheckpointedStateScope.EXCLUSIVE, checkpointStreamFactory);

            snapshotCloseableRegistry.registerCloseable(streamWithResultProvider);

            try {
                // no need for compression scheme support because sst-files are already compressed
                KeyedBackendSerializationProxy<K> serializationProxy =
                        new KeyedBackendSerializationProxy<>(
                                keySerializer, stateMetaInfoSnapshots, false);

                DataOutputView out =
                        new DataOutputViewStreamWrapper(
                                streamWithResultProvider.getCheckpointOutputStream());

                serializationProxy.write(out);

                if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                    SnapshotResult<StreamStateHandle> result =
                            streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
                    streamWithResultProvider = null;
                    return result;
                } else {
                    throw new IOException("Stream already closed and cannot return a handle.");
                }
            } finally {
                if (streamWithResultProvider != null) {
                    if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                        IOUtils.closeQuietly(streamWithResultProvider);
                    }
                }
            }
        }
    }

    static class IncrementalRocksDBSnapshotResources implements SnapshotResources {
        @Nonnull private final SnapshotDirectory snapshotDirectory;
        @Nonnull private final PreviousSnapshot previousSnapshot;
        @Nonnull private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        public IncrementalRocksDBSnapshotResources(
                SnapshotDirectory snapshotDirectory,
                PreviousSnapshot previousSnapshot,
                List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
            this.snapshotDirectory = snapshotDirectory;
            this.previousSnapshot = previousSnapshot;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
        }

        @Override
        public void release() {
            try {
                if (snapshotDirectory.exists()) {
                    LOG.trace(
                            "Running cleanup for local RocksDB backup directory {}.",
                            snapshotDirectory);
                    boolean cleanupOk = snapshotDirectory.cleanup();

                    if (!cleanupOk) {
                        LOG.debug("Could not properly cleanup local RocksDB backup directory.");
                    }
                }
            } catch (IOException e) {
                LOG.warn("Could not properly cleanup local RocksDB backup directory.", e);
            }
        }
    }

    private static class PreviousSnapshot {

        @Nullable private final Set<StateHandleID> confirmedSstFiles;
        private final Map<StateHandleID, StreamStateHandle> uploadedSstFiles;

        private PreviousSnapshot(
                @Nullable Set<StateHandleID> confirmedSstFiles,
                @Nonnull Map<StateHandleID, StreamStateHandle> uploadedSstFiles) {
            this.confirmedSstFiles = confirmedSstFiles;
            this.uploadedSstFiles = Preconditions.checkNotNull(uploadedSstFiles);
        }

        private Optional<StreamStateHandle> getUploaded(StateHandleID stateHandleID) {
            if (isConfirmed(stateHandleID)) {
                // we introduce a placeholder state handle, that is replaced with the
                // original from the shared state registry (created from a previous checkpoint)
                return Optional.of(new PlaceholderStreamStateHandle());
            } else {
                // If the file was uploaded but not confirmed by JM the handle has to be resent
                // because JM might lose it during changing the leadership
                StreamStateHandle value = uploadedSstFiles.get(stateHandleID);
                if (value != null) {
                    LOG.trace("Using uploaded non-confirmed file: {}", value);
                }
                return Optional.ofNullable(value);
            }
        }

        private boolean isConfirmed(StateHandleID stateHandleID) {
            return confirmedSstFiles != null && confirmedSstFiles.contains(stateHandleID);
        }
    }
}
