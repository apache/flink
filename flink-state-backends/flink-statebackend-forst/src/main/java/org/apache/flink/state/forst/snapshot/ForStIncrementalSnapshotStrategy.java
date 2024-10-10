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

package org.apache.flink.state.forst.snapshot;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.forst.ForStKeyedStateBackend.ForStKvStateInfo;
import org.apache.flink.state.forst.ForStResourceContainer;
import org.apache.flink.state.forst.ForStStateDataTransfer;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.forstdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

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

import static org.apache.flink.state.forst.snapshot.ForStSnapshotUtil.CURRENT_FILE_NAME;
import static org.apache.flink.state.forst.snapshot.ForStSnapshotUtil.SST_FILE_SUFFIX;

/**
 * Snapshot strategy for {@link org.apache.flink.state.forst.ForStKeyedStateBackend} that is based
 * on disableFileDeletions()+getLiveFiles() of ForSt and creates incremental snapshots.
 *
 * @param <K> type of the backend keys.
 */
public class ForStIncrementalSnapshotStrategy<K>
        extends ForStSnapshotStrategyBase<
                K, ForStSnapshotStrategyBase.ForStNativeSnapshotResources> {

    private static final Logger LOG =
            LoggerFactory.getLogger(ForStIncrementalSnapshotStrategy.class);

    private static final String DESCRIPTION = "Asynchronous incremental ForSt snapshot";

    /**
     * Stores the {@link StreamStateHandle} and corresponding local path of uploaded SST files that
     * build the incremental history. Once the checkpoint is confirmed by JM, they can be reused for
     * incremental checkpoint.
     */
    @Nonnull private final SortedMap<Long, Collection<HandleAndLocalPath>> uploadedSstFiles;

    /** The identifier of the last completed checkpoint. */
    private long lastCompletedCheckpointId;

    /** The help class used to upload state files. */
    private final ForStStateDataTransfer stateTransfer;

    public ForStIncrementalSnapshotStrategy(
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard forstResourceGuard,
            @Nonnull ForStResourceContainer resourceContainer,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, ForStKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull UUID backendUID,
            @Nonnull SortedMap<Long, Collection<HandleAndLocalPath>> uploadedStateHandles,
            @Nonnull ForStStateDataTransfer stateTransfer,
            long lastCompletedCheckpointId) {

        super(
                DESCRIPTION,
                db,
                forstResourceGuard,
                resourceContainer,
                keySerializer,
                kvStateInformation,
                keyGroupRange,
                keyGroupPrefixBytes,
                backendUID);

        this.uploadedSstFiles = new TreeMap<>(uploadedStateHandles);
        this.stateTransfer = stateTransfer;
        this.lastCompletedCheckpointId = lastCompletedCheckpointId;
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            ForStNativeSnapshotResources snapshotResources,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory checkpointStreamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {

        if (snapshotResources.stateMetaInfoSnapshots.isEmpty()) {
            if (LOG.isDebugEnabled()) {
                LOG.debug(
                        "Asynchronous ForSt snapshot performed on empty keyed state at {}. Returning null.",
                        timestamp);
            }
            return registry -> SnapshotResult.empty();
        }

        final CheckpointType.SharingFilesStrategy sharingFilesStrategy =
                checkpointOptions.getCheckpointType().getSharingFilesStrategy();

        switch (sharingFilesStrategy) {
            case FORWARD_BACKWARD:
                // incremental checkpoint, use origin PreviousSnapshot
                break;
            case FORWARD:
            case NO_SHARING:
                // full checkpoint, use empty PreviousSnapshot
                snapshotResources.setPreviousSnapshot(EMPTY_PREVIOUS_SNAPSHOT);
                break;
            default:
                throw new IllegalArgumentException(
                        "Unsupported sharing files strategy: " + sharingFilesStrategy);
        }

        return new ForStIncrementalSnapshotOperation(
                checkpointId, snapshotResources, checkpointStreamFactory, sharingFilesStrategy);
    }

    @Override
    public void notifyCheckpointComplete(long completedCheckpointId) {
        synchronized (uploadedSstFiles) {
            LOG.info("Backend:{} checkpoint:{} complete.", backendUID, completedCheckpointId);

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
            LOG.info("Backend:{} checkpoint:{} aborted.", backendUID, abortedCheckpointId);
            uploadedSstFiles.keySet().remove(abortedCheckpointId);
        }
    }

    @Override
    public void close() {
        stateTransfer.close();
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
        for (Map.Entry<String, ForStKvStateInfo> stateMetaInfoEntry :
                kvStateInformation.entrySet()) {
            stateMetaInfoSnapshots.add(stateMetaInfoEntry.getValue().metaInfo.snapshot());
        }
        return new PreviousSnapshot(confirmedSstFiles);
    }

    /** Encapsulates the process to perform an incremental snapshot of a ForStKeyedStateBackend. */
    private final class ForStIncrementalSnapshotOperation extends ForStSnapshotOperation {

        @Nonnull private final SnapshotType.SharingFilesStrategy sharingFilesStrategy;

        private ForStIncrementalSnapshotOperation(
                long checkpointId,
                @Nonnull ForStNativeSnapshotResources snapshotResources,
                @Nonnull CheckpointStreamFactory checkpointStreamFactory,
                @Nonnull SnapshotType.SharingFilesStrategy sharingFilesStrategy) {

            super(checkpointId, snapshotResources, checkpointStreamFactory);
            this.sharingFilesStrategy = sharingFilesStrategy;
        }

        @Override
        public SnapshotResult<KeyedStateHandle> get(CloseableRegistry snapshotCloseableRegistry)
                throws Exception {

            boolean completed = false;

            final List<StreamStateHandle> reusedHandle = new ArrayList<>();

            try {

                // Handle to the meta data file
                SnapshotResult<StreamStateHandle> metaStateHandle =
                        materializeMetaData(
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry,
                                snapshotResources.stateMetaInfoSnapshots,
                                checkpointId,
                                checkpointStreamFactory);

                final List<HandleAndLocalPath> sstFiles = new ArrayList<>();
                final List<HandleAndLocalPath> miscFiles = new ArrayList<>();

                long checkpointedSize = metaStateHandle.getStateSize();
                checkpointedSize +=
                        transferSnapshotFiles(
                                sstFiles,
                                miscFiles,
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry,
                                reusedHandle);

                final IncrementalRemoteKeyedStateHandle jmIncrementalKeyedStateHandle =
                        new IncrementalRemoteKeyedStateHandle(
                                backendUID,
                                keyGroupRange,
                                checkpointId,
                                sstFiles,
                                miscFiles,
                                metaStateHandle.getJobManagerOwnedSnapshot(),
                                checkpointedSize);

                completed = true;

                return SnapshotResult.of(jmIncrementalKeyedStateHandle);
            } finally {
                snapshotResources.release();

                if (!completed) {
                    try {
                        tmpResourcesRegistry.close();
                    } catch (Exception e) {
                        LOG.warn("Could not properly clean tmp resources.", e);
                    }

                } else {
                    // Report the reuse of state handle to stream factory, which is essential for
                    // file merging mechanism.
                    checkpointStreamFactory.reusePreviousStateHandle(reusedHandle);
                }
            }
        }

        /**
         * Transfer files to checkpoint filesystem and return total uploaded size.
         *
         * @param sstHandles Empty container, all sst files which should be including in checkpoint
         *     will be put in it.
         * @param metaHandles Empty container, all meta files (include manifest file) which should
         *     be including in checkpoint will be put in it.
         * @return Total bytes transfer to checkpoint filesystem.
         */
        private long transferSnapshotFiles(
                @Nonnull List<HandleAndLocalPath> sstHandles,
                @Nonnull List<HandleAndLocalPath> metaHandles,
                @Nonnull CloseableRegistry snapshotCloseableRegistry,
                @Nonnull CloseableRegistry tmpResourcesRegistry,
                @Nonnull List<StreamStateHandle> reusedHandle)
                throws Exception {

            Preconditions.checkNotNull(
                    snapshotResources.liveFiles, "liveFiles were not properly created.");

            if (snapshotResources.liveFiles.isEmpty()) {
                return 0;
            }

            Tuple4<List<HandleAndLocalPath>, List<Path>, List<Path>, Path> classifiedFiles =
                    classifyFiles();

            sstHandles.addAll(classifiedFiles.f0);
            // Collect the reuse of state handle.
            sstHandles.stream().map(HandleAndLocalPath::getHandle).forEach(reusedHandle::add);

            final CheckpointedStateScope stateScope =
                    sharingFilesStrategy == SnapshotType.SharingFilesStrategy.NO_SHARING
                            ? CheckpointedStateScope.EXCLUSIVE
                            : CheckpointedStateScope.SHARED;

            long transferBytes = 0;

            List<HandleAndLocalPath> sstFilesTransferResult =
                    stateTransfer.transferFilesToCheckpointFs(
                            classifiedFiles.f1,
                            checkpointStreamFactory,
                            stateScope,
                            snapshotCloseableRegistry,
                            tmpResourcesRegistry);

            sstHandles.addAll(sstFilesTransferResult);
            transferBytes +=
                    sstFilesTransferResult.stream()
                            .mapToLong(HandleAndLocalPath::getStateSize)
                            .sum();

            List<HandleAndLocalPath> miscFilesTransferResult =
                    stateTransfer.transferFilesToCheckpointFs(
                            classifiedFiles.f2,
                            checkpointStreamFactory,
                            stateScope,
                            snapshotCloseableRegistry,
                            tmpResourcesRegistry);
            metaHandles.addAll(miscFilesTransferResult);
            transferBytes +=
                    miscFilesTransferResult.stream()
                            .mapToLong(HandleAndLocalPath::getStateSize)
                            .sum();

            HandleAndLocalPath manifestFileTransferResult =
                    stateTransfer.transferFileToCheckpointFs(
                            classifiedFiles.f3,
                            snapshotResources.manifestFileSize,
                            checkpointStreamFactory,
                            stateScope,
                            snapshotCloseableRegistry,
                            tmpResourcesRegistry);
            metaHandles.add(manifestFileTransferResult);
            transferBytes += manifestFileTransferResult.getStateSize();

            // To prevent the content of the current file change, the manifest file name is directly
            // used to rewrite the current file during the checkpoint asynchronous phase.
            HandleAndLocalPath currentFileWriteResult =
                    stateTransfer.writeFileToCheckpointFs(
                            CURRENT_FILE_NAME,
                            snapshotResources.getCurrentFileContent(),
                            checkpointStreamFactory,
                            stateScope,
                            snapshotCloseableRegistry,
                            tmpResourcesRegistry);
            metaHandles.add(currentFileWriteResult);
            transferBytes += currentFileWriteResult.getStateSize();

            recordReusableHandles(sstHandles);

            return transferBytes;
        }

        private void recordReusableHandles(List<HandleAndLocalPath> sstHandles) {
            synchronized (uploadedSstFiles) {
                switch (sharingFilesStrategy) {
                    case FORWARD_BACKWARD:
                    case FORWARD:
                        uploadedSstFiles.put(
                                checkpointId, Collections.unmodifiableList(sstHandles));
                        break;
                    case NO_SHARING:
                        break;
                    default:
                        // This is just a safety precaution. It is checked before creating the
                        // ForStIncrementalSnapshotOperation
                        throw new IllegalArgumentException(
                                "Unsupported sharing files strategy: " + sharingFilesStrategy);
                }
            }
        }

        private Tuple4<List<HandleAndLocalPath>, List<Path>, List<Path>, Path> classifyFiles() {
            int totalFileNum = snapshotResources.liveFiles.size();

            List<HandleAndLocalPath> transferredSstHandles = new ArrayList<>(totalFileNum);

            List<Path> toTransferSstFiles = new ArrayList<>(totalFileNum);
            List<Path> toTransferMiscFiles = new ArrayList<>(totalFileNum);
            Path toTransferManifestFile = null;

            for (Path filePath : snapshotResources.liveFiles) {
                final String fileName = filePath.getName();

                if (fileName.equals(snapshotResources.manifestFileName)) {
                    toTransferManifestFile = filePath;
                } else if (fileName.endsWith(SST_FILE_SUFFIX)) {
                    Optional<StreamStateHandle> uploaded =
                            snapshotResources.previousSnapshot.getUploaded(fileName);
                    if (uploaded.isPresent()
                            && checkpointStreamFactory.couldReuseStateHandle(uploaded.get())) {
                        transferredSstHandles.add(HandleAndLocalPath.of(uploaded.get(), fileName));
                    } else {
                        toTransferSstFiles.add(filePath); // re-transfer
                    }
                } else {
                    toTransferMiscFiles.add(filePath);
                }
            }

            return Tuple4.of(
                    transferredSstHandles,
                    toTransferSstFiles,
                    toTransferMiscFiles,
                    toTransferManifestFile);
        }
    }
}
