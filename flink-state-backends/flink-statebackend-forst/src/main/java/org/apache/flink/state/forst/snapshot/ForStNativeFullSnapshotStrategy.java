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
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.forst.ForStOperationUtils;
import org.apache.flink.state.forst.ForStResourceContainer;
import org.apache.flink.state.forst.datatransfer.ForStStateDataTransfer;
import org.apache.flink.util.ResourceGuard;

import org.forstdb.RocksDB;
import org.forstdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.state.forst.snapshot.ForStSnapshotUtil.CURRENT_FILE_NAME;
import static org.apache.flink.state.forst.snapshot.ForStSnapshotUtil.MANIFEST_FILE_PREFIX;

/**
 * Snapshot strategy for {@link org.apache.flink.state.forst.ForStKeyedStateBackend} that is based
 * on disableFileDeletions()+getLiveFiles() of ForSt and creates full snapshots.
 *
 * @param <K> type of the backend keys.
 */
public class ForStNativeFullSnapshotStrategy<K>
        extends ForStSnapshotStrategyBase<
                K, ForStSnapshotStrategyBase.ForStNativeSnapshotResources> {

    private static final Logger LOG =
            LoggerFactory.getLogger(ForStNativeFullSnapshotStrategy.class);

    private static final String DESCRIPTION = "Asynchronous full ForStDB snapshot";

    /** The help class used to upload state files. */
    protected final ForStStateDataTransfer stateTransfer;

    public ForStNativeFullSnapshotStrategy(
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard resourceGuard,
            @Nonnull ForStResourceContainer resourceContainer,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            int keyGroupPrefixBytes,
            @Nonnull UUID backendUID,
            @Nonnull ForStStateDataTransfer stateTransfer) {
        this(
                DESCRIPTION,
                db,
                resourceGuard,
                resourceContainer,
                keySerializer,
                kvStateInformation,
                keyGroupRange,
                keyGroupPrefixBytes,
                backendUID,
                stateTransfer);
    }

    public ForStNativeFullSnapshotStrategy(
            @Nonnull String description,
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard resourceGuard,
            @Nonnull ForStResourceContainer resourceContainer,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            int keyGroupPrefixBytes,
            @Nonnull UUID backendUID,
            @Nonnull ForStStateDataTransfer stateTransfer) {
        super(
                description,
                db,
                resourceGuard,
                resourceContainer,
                keySerializer,
                kvStateInformation,
                keyGroupRange,
                keyGroupPrefixBytes,
                backendUID);
        this.stateTransfer = stateTransfer;
    }

    @Override
    protected PreviousSnapshot snapshotMetaData(
            long checkpointId, @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
        for (Map.Entry<String, ForStOperationUtils.ForStKvStateInfo> stateMetaInfoEntry :
                kvStateInformation.entrySet()) {
            stateMetaInfoSnapshots.add(stateMetaInfoEntry.getValue().metaInfo.snapshot());
        }
        return EMPTY_PREVIOUS_SNAPSHOT;
    }

    @Override
    public void close() {
        stateTransfer.close();
    }

    @Override
    public void notifyCheckpointAborted(long abortedCheckpointId) {
        // nothing to do
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // nothing to do
    }

    @Override
    public ForStNativeSnapshotResources syncPrepareResources(long checkpointId) throws Exception {
        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                new ArrayList<>(kvStateInformation.size());
        final PreviousSnapshot previousSnapshot =
                snapshotMetaData(checkpointId, stateMetaInfoSnapshots);

        ResourceGuard.Lease lease = resourceGuard.acquireResource();
        // Disable file deletion for file transformation. ForSt will decide whether to allow
        // file
        // deletion based on the number of calls to disableFileDeletions() and
        // enableFileDeletions(), so disableFileDeletions() should be call only once.
        db.disableFileDeletions();
        try {
            // get live files with flush memtable
            RocksDB.LiveFiles liveFiles = db.getLiveFiles(true);
            List<Path> liveFilesPath =
                    liveFiles.files.stream()
                            .map(file -> new Path(resourceContainer.getDbPath(), file))
                            // Use manifest file name write CURRENT file to checkpoint directly.
                            .filter(file -> !file.getName().equals(CURRENT_FILE_NAME))
                            .collect(Collectors.toList());

            // todo: check whether there is only a manifest file exists simultaneously.
            Path manifestFile =
                    liveFilesPath.stream()
                            .filter(file -> file.getName().startsWith(MANIFEST_FILE_PREFIX))
                            .findAny()
                            .get(); // there must be a manifest file.

            logLiveFiles(checkpointId, liveFiles.manifestFileSize, liveFilesPath);

            return new ForStNativeSnapshotResources(
                    stateMetaInfoSnapshots,
                    liveFiles.manifestFileSize,
                    liveFilesPath,
                    manifestFile,
                    previousSnapshot,
                    () -> {
                        try {
                            db.enableFileDeletions(false);
                            lease.close();
                            LOG.info(
                                    "Release one file deletion lock with ForStNativeSnapshotResources, backendUID:{}, checkpointId:{}.",
                                    backendUID,
                                    checkpointId);
                        } catch (RocksDBException e) {
                            LOG.error(
                                    "Enable file deletion failed, backendUID:{}, checkpointId:{}.",
                                    backendUID,
                                    checkpointId,
                                    e);
                        }
                    });
        } catch (Exception e) {
            LOG.error(
                    "Exception thrown when prepare snapshot resources, enable file deletion and rethrow the exception, backendUID:{}, checkpointId:{}",
                    backendUID,
                    checkpointId);
            db.enableFileDeletions(false);
            lease.close();
            throw e;
        }
    }

    private void logLiveFiles(long checkpointId, long manifestFileSize, List<Path> liveFilesPath) {
        if (LOG.isDebugEnabled()) {
            StringBuilder sb =
                    new StringBuilder("    manifestFileSize:")
                            .append(manifestFileSize)
                            .append("\n");
            liveFilesPath.forEach(e -> sb.append("    file : ").append(e).append("\n"));
            LOG.debug(
                    "Backend:{} live files for checkpoint:{} : \n{}", backendUID, checkpointId, sb);
        }
    }

    @Override
    public SnapshotResultSupplier<KeyedStateHandle> asyncSnapshot(
            ForStNativeSnapshotResources syncPartResource,
            long checkpointId,
            long timestamp,
            @Nonnull CheckpointStreamFactory streamFactory,
            @Nonnull CheckpointOptions checkpointOptions) {
        if (syncPartResource.stateMetaInfoSnapshots.isEmpty()) {
            return registry -> SnapshotResult.empty();
        }
        return new ForStNativeFullSnapshotOperation(
                checkpointOptions.getCheckpointType().getSharingFilesStrategy(),
                checkpointId,
                streamFactory,
                syncPartResource);
    }

    /** Encapsulates the process to perform a full snapshot of a ForStKeyedStateBackend. */
    private final class ForStNativeFullSnapshotOperation extends ForStSnapshotOperation {

        @Nonnull private final ForStNativeSnapshotResources snapshotResources;
        @Nonnull private final SnapshotType.SharingFilesStrategy sharingFilesStrategy;

        private ForStNativeFullSnapshotOperation(
                SnapshotType.SharingFilesStrategy sharingFilesStrategy,
                long checkpointId,
                @Nonnull CheckpointStreamFactory checkpointStreamFactory,
                @Nonnull ForStNativeSnapshotResources snapshotResources) {
            super(checkpointId, checkpointStreamFactory);
            this.snapshotResources = snapshotResources;
            this.sharingFilesStrategy = sharingFilesStrategy;
        }

        @Override
        public SnapshotResult<KeyedStateHandle> get(CloseableRegistry snapshotCloseableRegistry)
                throws Exception {
            boolean completed = false;

            try {
                // Handle to the meta data file
                SnapshotResult<StreamStateHandle> metaStateHandle =
                        materializeMetaData(
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry,
                                snapshotResources.stateMetaInfoSnapshots,
                                checkpointId,
                                checkpointStreamFactory);

                final List<IncrementalKeyedStateHandle.HandleAndLocalPath> privateFiles =
                        new ArrayList<>();
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
                }
            }
        }

        /** upload files and return total uploaded size. */
        private long uploadSnapshotFiles(
                @Nonnull List<IncrementalKeyedStateHandle.HandleAndLocalPath> privateFiles,
                @Nonnull CloseableRegistry snapshotCloseableRegistry,
                @Nonnull CloseableRegistry tmpResourcesRegistry)
                throws Exception {
            long uploadedSize = 0;
            if (snapshotResources.liveFiles.size() > 0) {
                List<IncrementalKeyedStateHandle.HandleAndLocalPath> uploadedFiles =
                        stateTransfer.transferFilesToCheckpointFs(
                                sharingFilesStrategy,
                                snapshotResources.liveFiles.stream()
                                        .filter(
                                                file ->
                                                        !file.getName().endsWith(CURRENT_FILE_NAME)
                                                                && !file.getName()
                                                                        .startsWith(
                                                                                MANIFEST_FILE_PREFIX))
                                        .collect(Collectors.toList()),
                                checkpointStreamFactory,
                                CheckpointedStateScope.EXCLUSIVE,
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry,
                                true);
                uploadedSize += uploadedFiles.stream().mapToLong(e -> e.getStateSize()).sum();
                privateFiles.addAll(uploadedFiles);

                IncrementalKeyedStateHandle.HandleAndLocalPath manifestFileTransferResult =
                        stateTransfer.transferFileToCheckpointFs(
                                SnapshotType.SharingFilesStrategy.NO_SHARING,
                                snapshotResources.manifestFilePath,
                                snapshotResources.manifestFileSize,
                                checkpointStreamFactory,
                                CheckpointedStateScope.EXCLUSIVE,
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry,
                                true);
                privateFiles.add(manifestFileTransferResult);
                uploadedSize += manifestFileTransferResult.getStateSize();

                // To prevent the content of the current file change, the manifest file name is
                // directly
                // used to rewrite the current file during the checkpoint asynchronous phase.
                IncrementalKeyedStateHandle.HandleAndLocalPath currentFileWriteResult =
                        stateTransfer.writeFileToCheckpointFs(
                                CURRENT_FILE_NAME,
                                snapshotResources.getCurrentFileContent(),
                                checkpointStreamFactory,
                                CheckpointedStateScope.EXCLUSIVE,
                                snapshotCloseableRegistry,
                                tmpResourcesRegistry);
                privateFiles.add(currentFileWriteResult);
                uploadedSize += currentFileWriteResult.getStateSize();
            }
            return uploadedSize;
        }
    }
}
