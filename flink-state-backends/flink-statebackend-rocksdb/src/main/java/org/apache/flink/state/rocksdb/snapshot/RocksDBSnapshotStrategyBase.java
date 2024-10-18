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

package org.apache.flink.state.rocksdb.snapshot;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle.HandleAndLocalPath;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalSnapshotDirectoryProvider;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResources;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.state.rocksdb.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
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
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Abstract base class for {@link SnapshotStrategy} implementations for RocksDB state backend.
 *
 * @param <K> type of the backend keys.
 */
public abstract class RocksDBSnapshotStrategyBase<K, R extends SnapshotResources>
        implements CheckpointListener,
                SnapshotStrategy<
                        KeyedStateHandle,
                        RocksDBSnapshotStrategyBase.NativeRocksDBSnapshotResources>,
                AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RocksDBSnapshotStrategyBase.class);

    @Nonnull private final String description;
    /** RocksDB instance from the backend. */
    @Nonnull protected RocksDB db;

    /** Resource guard for the RocksDB instance. */
    @Nonnull protected final ResourceGuard rocksDBResourceGuard;

    /** The key serializer of the backend. */
    @Nonnull protected final TypeSerializer<K> keySerializer;

    /** Key/Value state meta info from the backend. */
    @Nonnull protected final LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation;

    /** The key-group range for the task. */
    @Nonnull protected final KeyGroupRange keyGroupRange;

    /** Number of bytes in the key-group prefix. */
    @Nonnegative protected final int keyGroupPrefixBytes;

    /** The configuration for local recovery. */
    @Nonnull protected final LocalRecoveryConfig localRecoveryConfig;

    /** Base path of the RocksDB instance. */
    @Nonnull protected final File instanceBasePath;

    /** The local directory name of the current snapshot strategy. */
    protected final String localDirectoryName;

    /** The state handle ids of all sst files materialized in snapshots for previous checkpoints. */
    @Nonnull protected final UUID backendUID;

    public RocksDBSnapshotStrategyBase(
            @Nonnull String description,
            @Nonnull RocksDB db,
            @Nonnull ResourceGuard rocksDBResourceGuard,
            @Nonnull TypeSerializer<K> keySerializer,
            @Nonnull LinkedHashMap<String, RocksDbKvStateInfo> kvStateInformation,
            @Nonnull KeyGroupRange keyGroupRange,
            @Nonnegative int keyGroupPrefixBytes,
            @Nonnull LocalRecoveryConfig localRecoveryConfig,
            @Nonnull File instanceBasePath,
            @Nonnull UUID backendUID) {
        this.db = db;
        this.rocksDBResourceGuard = rocksDBResourceGuard;
        this.keySerializer = keySerializer;
        this.kvStateInformation = kvStateInformation;
        this.keyGroupRange = keyGroupRange;
        this.keyGroupPrefixBytes = keyGroupPrefixBytes;
        this.localRecoveryConfig = localRecoveryConfig;
        this.description = description;
        this.instanceBasePath = instanceBasePath;
        this.localDirectoryName = backendUID.toString().replaceAll("[\\-]", "");
        this.backendUID = backendUID;
    }

    @Nonnull
    public String getDescription() {
        return description;
    }

    @Override
    public NativeRocksDBSnapshotResources syncPrepareResources(long checkpointId) throws Exception {

        final SnapshotDirectory snapshotDirectory = prepareLocalSnapshotDirectory(checkpointId);
        LOG.trace("Local RocksDB checkpoint goes to backup path {}.", snapshotDirectory);

        final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots =
                new ArrayList<>(kvStateInformation.size());
        final PreviousSnapshot previousSnapshot =
                snapshotMetaData(checkpointId, stateMetaInfoSnapshots);

        takeDBNativeCheckpoint(snapshotDirectory);

        return new NativeRocksDBSnapshotResources(
                snapshotDirectory, previousSnapshot, stateMetaInfoSnapshots);
    }

    protected abstract PreviousSnapshot snapshotMetaData(
            long checkpointId, @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots);

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

    @Nonnull
    protected SnapshotDirectory prepareLocalSnapshotDirectory(long checkpointId)
            throws IOException {

        if (localRecoveryConfig.isLocalBackupEnabled()) {
            // create a "permanent" snapshot directory for local recovery.
            LocalSnapshotDirectoryProvider directoryProvider =
                    localRecoveryConfig
                            .getLocalStateDirectoryProvider()
                            .orElseThrow(LocalRecoveryConfig.localRecoveryNotEnabled());
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

    @Nonnull
    protected SnapshotResult<StreamStateHandle> materializeMetaData(
            @Nonnull CloseableRegistry snapshotCloseableRegistry,
            @Nonnull CloseableRegistry tmpResourcesRegistry,
            @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
            long checkpointId,
            @Nonnull CheckpointStreamFactory checkpointStreamFactory)
            throws Exception {

        CheckpointStreamWithResultProvider streamWithResultProvider =
                localRecoveryConfig.isLocalBackupEnabled()
                        ? CheckpointStreamWithResultProvider.createDuplicatingStream(
                                checkpointId,
                                CheckpointedStateScope.EXCLUSIVE,
                                checkpointStreamFactory,
                                localRecoveryConfig
                                        .getLocalStateDirectoryProvider()
                                        .orElseThrow(LocalRecoveryConfig.localRecoveryNotEnabled()))
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
                tmpResourcesRegistry.registerCloseable(
                        () -> StateUtil.discardStateObjectQuietly(result));
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
    public abstract void close() throws IOException;

    /** Common operation in native rocksdb snapshot result supplier. */
    protected abstract class RocksDBSnapshotOperation
            implements SnapshotResultSupplier<KeyedStateHandle> {
        /** Id for the current checkpoint. */
        protected final long checkpointId;

        /** Stream factory that creates the output streams to DFS. */
        @Nonnull protected final CheckpointStreamFactory checkpointStreamFactory;

        /** The state meta data. */
        @Nonnull protected final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        /** Local directory for the RocksDB native backup. */
        @Nonnull protected final SnapshotDirectory localBackupDirectory;

        @Nonnull protected final CloseableRegistry tmpResourcesRegistry;

        protected RocksDBSnapshotOperation(
                long checkpointId,
                @Nonnull CheckpointStreamFactory checkpointStreamFactory,
                @Nonnull SnapshotDirectory localBackupDirectory,
                @Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
            this.checkpointId = checkpointId;
            this.checkpointStreamFactory = checkpointStreamFactory;
            this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
            this.localBackupDirectory = localBackupDirectory;
            this.tmpResourcesRegistry = new CloseableRegistry();
        }

        protected Optional<KeyedStateHandle> getLocalSnapshot(
                @Nullable StreamStateHandle localStreamStateHandle,
                List<HandleAndLocalPath> sharedState)
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

    /** A {@link SnapshotResources} for native rocksdb snapshot. */
    protected static class NativeRocksDBSnapshotResources implements SnapshotResources {
        @Nonnull protected final SnapshotDirectory snapshotDirectory;

        @Nonnull protected final PreviousSnapshot previousSnapshot;

        @Nonnull protected final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

        public NativeRocksDBSnapshotResources(
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
