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

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.forst.ForStOperationUtils;
import org.apache.flink.state.forst.ForStResourceContainer;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.forstdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Abstract base class for {@link SnapshotStrategy} implementations for ForSt state backend.
 *
 * @param <K> type of the backend keys.
 */
public abstract class ForStSnapshotStrategyBase<K, R extends SnapshotResources>
        implements CheckpointListener, SnapshotStrategy<KeyedStateHandle, R>, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(ForStSnapshotStrategyBase.class);

    @Nonnull private final String description;

    /** ForSt instance from the backend. */
    @Nonnull protected final RocksDB db;

    /** Resource guard for the ForSt instance. */
    @Nonnull protected final ResourceGuard resourceGuard;

    @Nonnull protected final ForStResourceContainer resourceContainer;

    /** The key serializer of the backend. */
    @Nonnull protected final TypeSerializer<K> keySerializer;

    /** Key/Value state meta info from the backend. */
    @Nonnull
    protected final LinkedHashMap<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation;

    /** The key-group range for the task. */
    @Nonnull protected final KeyGroupRange keyGroupRange;

    /** Number of bytes in the key-group prefix. */
    @Nonnegative protected final int keyGroupPrefixBytes;

    /** The state handle ids of all sst files materialized in snapshots for previous checkpoints. */
    @Nonnull protected final UUID backendUID;

    public ForStSnapshotStrategyBase(
            @Nonnull String description,
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard resourceGuard,
            @Nonnull ForStResourceContainer resourceContainer,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, ForStOperationUtils.ForStKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull UUID backendUID) {
        this.db = db;
        this.resourceGuard = resourceGuard;
        this.resourceContainer = resourceContainer;
        this.keySerializer = keySerializer;
        this.kvStateInformation = kvStateInformation;
        this.keyGroupRange = keyGroupRange;
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.description = description;
        this.backendUID = backendUID;
    }

    @Nonnull
    public String getDescription() {
        return description;
    }

    protected abstract PreviousSnapshot snapshotMetaData(
            long checkpointId, @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots);

    @Nonnull
    protected SnapshotResult<StreamStateHandle> materializeMetaData(
            @Nonnull CloseableRegistry snapshotCloseableRegistry,
            @Nonnull CloseableRegistry tmpResourcesRegistry,
            @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            long checkpointId,
            @Nonnull CheckpointStreamFactory checkpointStreamFactory)
            throws Exception {

        CheckpointStreamWithResultProvider streamWithResultProvider =
                CheckpointStreamWithResultProvider.createSimpleStream(
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
                tmpResourcesRegistry.registerCloseable(
                        () -> StateUtil.discardStateObjectQuietly(result));

                // Sanity checks - they should never fail
                Preconditions.checkNotNull(
                        result,
                        String.format(
                                "Backend:%s, checkpoint:%s, Metadata was not properly created.",
                                backendUID, checkpointId));
                Preconditions.checkNotNull(
                        result.getJobManagerOwnedSnapshot(),
                        String.format(
                                "Backend:%s, checkpoint:%s, Metadata for job manager was not properly created.",
                                backendUID, checkpointId));

                return result;
            } else {
                throw new IOException("Stream already closed and cannot return a handle.");
            }
        } finally {
            if (snapshotCloseableRegistry.unregisterCloseable(streamWithResultProvider)) {
                IOUtils.closeQuietly(streamWithResultProvider);
            }
        }
    }

    @Override
    public abstract void close();

    protected void cleanupIncompleteSnapshot(
            @Nonnull CloseableRegistry tmpResourcesRegistry,
            @Nonnull SnapshotDirectory localBackupDirectory) {
        try {
            tmpResourcesRegistry.close();
        } catch (Exception e) {
            LOG.warn("Could not properly clean tmp resources.", e);
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

    /** Common operation in native ForSt snapshot result supplier. */
    protected abstract class ForStSnapshotOperation
            implements SnapshotResultSupplier<KeyedStateHandle> {

        protected final long checkpointId;
        @Nonnull protected final CheckpointStreamFactory checkpointStreamFactory;
        @Nonnull protected final CloseableRegistry tmpResourcesRegistry;

        protected ForStSnapshotOperation(
                long checkpointId, @Nonnull CheckpointStreamFactory checkpointStreamFactory) {
            this.checkpointId = checkpointId;
            this.checkpointStreamFactory = checkpointStreamFactory;
            this.tmpResourcesRegistry = new CloseableRegistry();
        }

        protected Optional<KeyedStateHandle> getLocalSnapshot(
                SnapshotDirectory localBackupDirectory,
                @Nullable StreamStateHandle localStreamStateHandle,
                List<IncrementalKeyedStateHandle.HandleAndLocalPath> sharedState)
                throws IOException {
            final DirectoryStateHandle directoryStateHandle =
                    localBackupDirectory.completeSnapshotAndGetHandle();
            if (directoryStateHandle != null && localStreamStateHandle != null) {
                return Optional.of(
                        new IncrementalLocalKeyedStateHandle(
                                backendUID,
                                checkpointId,
                                directoryStateHandle,
                                keyGroupRange,
                                localStreamStateHandle,
                                sharedState));
            } else {
                return Optional.empty();
            }
        }
    }

    /** A {@link SnapshotResources} for native ForSt snapshot. */
    protected static class ForStNativeSnapshotResources implements SnapshotResources {

        @Nonnull protected final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;
        protected final long manifestFileSize;
        @Nonnull protected final List<Path> liveFiles;
        @Nonnull protected final String manifestFileName;
        @Nonnull protected final Path manifestFilePath;
        @Nonnull protected PreviousSnapshot previousSnapshot;
        @Nonnull protected final Runnable releaser;

        private final AtomicBoolean released;

        public ForStNativeSnapshotResources(
                @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
                long manifestFileSize,
                @Nonnull List<Path> liveFiles,
                @Nonnull Path manifestFilePath,
                @Nonnull PreviousSnapshot previousSnapshot,
                @Nonnull Runnable releaser) {
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
            this.manifestFileSize = manifestFileSize;
            this.liveFiles = liveFiles;
            this.manifestFilePath = manifestFilePath;
            this.manifestFileName = manifestFilePath.getName();
            this.previousSnapshot = previousSnapshot;
            this.releaser = releaser;
            this.released = new AtomicBoolean(false);
        }

        public void setPreviousSnapshot(@Nonnull PreviousSnapshot previousSnapshot) {
            this.previousSnapshot = previousSnapshot;
        }

        public String getCurrentFileContent() {
            // RocksDB require CURRENT file end with a new line
            return manifestFileName + "\n";
        }

        @Override
        public void release() {
            // make sure only release once
            if (released.compareAndSet(false, true)) {
                releaser.run();
            }
        }
    }

    /** A {@link SnapshotResources} for forst sync snapshot. */
    protected static class ForStSyncSnapshotResources implements SnapshotResources {
        @Nonnull protected final SnapshotDirectory snapshotDirectory;

        @Nonnull protected final PreviousSnapshot previousSnapshot;

        @Nonnull protected final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        public ForStSyncSnapshotResources(
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

    protected static final PreviousSnapshot EMPTY_PREVIOUS_SNAPSHOT =
            new PreviousSnapshot(Collections.emptyList());

    /** Previous snapshot with uploaded sst files. */
    protected static class PreviousSnapshot {

        @Nonnull private final Map<String, StreamStateHandle> confirmedSstFiles;

        protected PreviousSnapshot(@Nullable Collection<HandleAndLocalPath> confirmedSstFiles) {
            this.confirmedSstFiles =
                    confirmedSstFiles != null
                            ? confirmedSstFiles.stream()
                                    .collect(
                                            Collectors.toMap(
                                                    HandleAndLocalPath::getLocalPath,
                                                    HandleAndLocalPath::getHandle))
                            : Collections.emptyMap();
        }

        protected Optional<StreamStateHandle> getUploaded(String filename) {
            if (confirmedSstFiles.containsKey(filename)) {
                StreamStateHandle handle = confirmedSstFiles.get(filename);
                // We introduce a placeholder state handle to reduce network transfer overhead,
                // it will be replaced by the original handle from the shared state registry
                // (created from a previous checkpoint).
                return Optional.of(
                        new PlaceholderStreamStateHandle(
                                handle.getStreamStateHandleID(),
                                handle.getStateSize(),
                                FileMergingSnapshotManager.isFileMergingHandle(handle)));
            } else {
                // Don't use any uploaded but not confirmed handles because they might be deleted
                // (by TM) if the previous checkpoint failed. See FLINK-25395
                return Optional.empty();
            }
        }
    }
}
