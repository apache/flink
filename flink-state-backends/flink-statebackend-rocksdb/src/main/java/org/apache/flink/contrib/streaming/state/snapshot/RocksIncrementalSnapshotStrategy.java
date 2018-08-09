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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.state.AsyncSnapshotCallable;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.SnapshotDirectory;
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
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;

/**
 * Snapshot strategy for {@link org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend} that is based
 * on RocksDB's native checkpoints and creates incremental snapshots.
 *
 * @param <K> type of the backend keys.
 */
public class RocksIncrementalSnapshotStrategy<K> extends RocksDBSnapshotStrategyBase<K> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksIncrementalSnapshotStrategy.class);

	private static final String DESCRIPTION = "Asynchronous incremental RocksDB snapshot";

	/** Base path of the RocksDB instance. */
	@Nonnull
	private final File instanceBasePath;

	/** The state handle ids of all sst files materialized in snapshots for previous checkpoints. */
	@Nonnull
	private final UUID backendUID;

	/** Stores the materialized sstable files from all snapshots that build the incremental history. */
	@Nonnull
	private final SortedMap<Long, Set<StateHandleID>> materializedSstFiles;

	/** The identifier of the last completed checkpoint. */
	private long lastCompletedCheckpointId;

	public RocksIncrementalSnapshotStrategy(
		@Nonnull RocksDB db,
		@Nonnull ResourceGuard rocksDBResourceGuard,
		@Nonnull TypeSerializer<K> keySerializer,
		@Nonnull LinkedHashMap<String, Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase>> kvStateInformation,
		@Nonnull KeyGroupRange keyGroupRange,
		@Nonnegative int keyGroupPrefixBytes,
		@Nonnull LocalRecoveryConfig localRecoveryConfig,
		@Nonnull CloseableRegistry cancelStreamRegistry,
		@Nonnull File instanceBasePath,
		@Nonnull UUID backendUID,
		@Nonnull SortedMap<Long, Set<StateHandleID>> materializedSstFiles,
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
			cancelStreamRegistry);

		this.instanceBasePath = instanceBasePath;
		this.backendUID = backendUID;
		this.materializedSstFiles = materializedSstFiles;
		this.lastCompletedCheckpointId = lastCompletedCheckpointId;
	}

	@Nonnull
	@Override
	protected RunnableFuture<SnapshotResult<KeyedStateHandle>> doSnapshot(
		long checkpointId,
		long checkpointTimestamp,
		@Nonnull CheckpointStreamFactory checkpointStreamFactory,
		@Nonnull CheckpointOptions checkpointOptions) throws Exception {

		final SnapshotDirectory snapshotDirectory = prepareLocalSnapshotDirectory(checkpointId);
		LOG.trace("Local RocksDB checkpoint goes to backup path {}.", snapshotDirectory);

		final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = new ArrayList<>(kvStateInformation.size());
		final Set<StateHandleID> baseSstFiles = snapshotMetaData(checkpointId, stateMetaInfoSnapshots);

		takeDBNativeCheckpoint(snapshotDirectory);

		final RocksDBIncrementalSnapshotOperation snapshotOperation =
			new RocksDBIncrementalSnapshotOperation(
				checkpointId,
				checkpointStreamFactory,
				snapshotDirectory,
				baseSstFiles,
				stateMetaInfoSnapshots);

		return snapshotOperation.toAsyncSnapshotFutureTask(cancelStreamRegistry);
	}

	@Override
	public void notifyCheckpointComplete(long completedCheckpointId) {
		synchronized (materializedSstFiles) {
			if (completedCheckpointId > lastCompletedCheckpointId) {
				materializedSstFiles.keySet().removeIf(checkpointId -> checkpointId < completedCheckpointId);
				lastCompletedCheckpointId = completedCheckpointId;
			}
		}
	}

	@Nonnull
	private SnapshotDirectory prepareLocalSnapshotDirectory(long checkpointId) throws IOException {

		if (localRecoveryConfig.isLocalRecoveryEnabled()) {
			// create a "permanent" snapshot directory for local recovery.
			LocalRecoveryDirectoryProvider directoryProvider = localRecoveryConfig.getLocalStateDirectoryProvider();
			File directory = directoryProvider.subtaskSpecificCheckpointDirectory(checkpointId);

			if (directory.exists()) {
				FileUtils.deleteDirectory(directory);
			}

			if (!directory.mkdirs()) {
				throw new IOException("Local state base directory for checkpoint " + checkpointId +
					" already exists: " + directory);
			}

			// introduces an extra directory because RocksDB wants a non-existing directory for native checkpoints.
			File rdbSnapshotDir = new File(directory, "rocks_db");
			Path path = new Path(rdbSnapshotDir.toURI());
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
			Path path = new Path(instanceBasePath.getAbsolutePath(), "chk-" + checkpointId);
			return SnapshotDirectory.temporary(path);
		}
	}

	private Set<StateHandleID> snapshotMetaData(
		long checkpointId,
		@Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {

		final long lastCompletedCheckpoint;
		final Set<StateHandleID> baseSstFiles;

		// use the last completed checkpoint as the comparison base.
		synchronized (materializedSstFiles) {
			lastCompletedCheckpoint = lastCompletedCheckpointId;
			baseSstFiles = materializedSstFiles.get(lastCompletedCheckpoint);
		}
		LOG.trace("Taking incremental snapshot for checkpoint {}. Snapshot is based on last completed checkpoint {} " +
			"assuming the following (shared) files as base: {}.", checkpointId, lastCompletedCheckpoint, baseSstFiles);

		// snapshot meta data to save
		for (Map.Entry<String, Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase>> stateMetaInfoEntry
			: kvStateInformation.entrySet()) {
			stateMetaInfoSnapshots.add(stateMetaInfoEntry.getValue().f1.snapshot());
		}
		return baseSstFiles;
	}

	private void takeDBNativeCheckpoint(@Nonnull SnapshotDirectory outputDirectory) throws Exception {
		// create hard links of living files in the output path
		try (
			ResourceGuard.Lease ignored = rocksDBResourceGuard.acquireResource();
			Checkpoint checkpoint = Checkpoint.create(db)) {
			checkpoint.createCheckpoint(outputDirectory.getDirectory().getPath());
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
		extends AsyncSnapshotCallable<SnapshotResult<KeyedStateHandle>> {

		private static final int READ_BUFFER_SIZE = 16 * 1024;

		/** Id for the current checkpoint. */
		private final long checkpointId;

		/** Stream factory that creates the output streams to DFS. */
		@Nonnull
		private final CheckpointStreamFactory checkpointStreamFactory;

		/** The state meta data. */
		@Nonnull
		private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

		/** Local directory for the RocksDB native backup. */
		@Nonnull
		private final SnapshotDirectory localBackupDirectory;

		/** All sst files that were part of the last previously completed checkpoint. */
		@Nullable
		private final Set<StateHandleID> baseSstFiles;

		private RocksDBIncrementalSnapshotOperation(
			long checkpointId,
			@Nonnull CheckpointStreamFactory checkpointStreamFactory,
			@Nonnull SnapshotDirectory localBackupDirectory,
			@Nullable Set<StateHandleID> baseSstFiles,
			@Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {

			this.checkpointStreamFactory = checkpointStreamFactory;
			this.baseSstFiles = baseSstFiles;
			this.checkpointId = checkpointId;
			this.localBackupDirectory = localBackupDirectory;
			this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
		}

		@Override
		protected SnapshotResult<KeyedStateHandle> callInternal() throws Exception {

			boolean completed = false;

			// Handle to the meta data file
			SnapshotResult<StreamStateHandle> metaStateHandle = null;
			// Handles to new sst files since the last completed checkpoint will go here
			final Map<StateHandleID, StreamStateHandle> sstFiles = new HashMap<>();
			// Handles to the misc files in the current snapshot will go here
			final Map<StateHandleID, StreamStateHandle> miscFiles = new HashMap<>();

			try {

				metaStateHandle = materializeMetaData();

				// Sanity checks - they should never fail
				Preconditions.checkNotNull(metaStateHandle, "Metadata was not properly created.");
				Preconditions.checkNotNull(metaStateHandle.getJobManagerOwnedSnapshot(),
					"Metadata for job manager was not properly created.");

				uploadSstFiles(sstFiles, miscFiles);

				synchronized (materializedSstFiles) {
					materializedSstFiles.put(checkpointId, sstFiles.keySet());
				}

				final IncrementalKeyedStateHandle jmIncrementalKeyedStateHandle =
					new IncrementalKeyedStateHandle(
						backendUID,
						keyGroupRange,
						checkpointId,
						sstFiles,
						miscFiles,
						metaStateHandle.getJobManagerOwnedSnapshot());

				final DirectoryStateHandle directoryStateHandle = localBackupDirectory.completeSnapshotAndGetHandle();
				final SnapshotResult<KeyedStateHandle> snapshotResult;
				if (directoryStateHandle != null && metaStateHandle.getTaskLocalSnapshot() != null) {

					IncrementalLocalKeyedStateHandle localDirKeyedStateHandle =
						new IncrementalLocalKeyedStateHandle(
							backendUID,
							checkpointId,
							directoryStateHandle,
							keyGroupRange,
							metaStateHandle.getTaskLocalSnapshot(),
							sstFiles.keySet());

					snapshotResult = SnapshotResult.withLocalState(jmIncrementalKeyedStateHandle, localDirKeyedStateHandle);
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

		@Override
		protected void cleanupProvidedResources() {
			try {
				if (localBackupDirectory.exists()) {
					LOG.trace("Running cleanup for local RocksDB backup directory {}.", localBackupDirectory);
					boolean cleanupOk = localBackupDirectory.cleanup();

					if (!cleanupOk) {
						LOG.debug("Could not properly cleanup local RocksDB backup directory.");
					}
				}
			} catch (IOException e) {
				LOG.warn("Could not properly cleanup local RocksDB backup directory.", e);
			}
		}

		@Override
		protected void logAsyncSnapshotComplete(long startTime) {
			logAsyncCompleted(checkpointStreamFactory, startTime);
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
			@Nonnull Map<StateHandleID, StreamStateHandle> miscFiles) throws Exception {

			// write state data
			Preconditions.checkState(localBackupDirectory.exists());

			FileStatus[] fileStatuses = localBackupDirectory.listStatus();
			if (fileStatuses != null) {
				for (FileStatus fileStatus : fileStatuses) {
					final Path filePath = fileStatus.getPath();
					final String fileName = filePath.getName();
					final StateHandleID stateHandleID = new StateHandleID(fileName);

					if (fileName.endsWith(SST_FILE_SUFFIX)) {
						final boolean existsAlready =
							baseSstFiles != null && baseSstFiles.contains(stateHandleID);

						if (existsAlready) {
							// we introduce a placeholder state handle, that is replaced with the
							// original from the shared state registry (created from a previous checkpoint)
							sstFiles.put(
								stateHandleID,
								new PlaceholderStreamStateHandle());
						} else {
							sstFiles.put(stateHandleID, uploadLocalFileToCheckpointFs(filePath));
						}
					} else {
						StreamStateHandle fileHandle = uploadLocalFileToCheckpointFs(filePath);
						miscFiles.put(stateHandleID, fileHandle);
					}
				}
			}
		}

		private StreamStateHandle uploadLocalFileToCheckpointFs(Path filePath) throws Exception {
			FSDataInputStream inputStream = null;
			CheckpointStreamFactory.CheckpointStateOutputStream outputStream = null;

			try {
				final byte[] buffer = new byte[READ_BUFFER_SIZE];

				FileSystem backupFileSystem = localBackupDirectory.getFileSystem();
				inputStream = backupFileSystem.open(filePath);
				registerCloseableForCancellation(inputStream);

				outputStream = checkpointStreamFactory
					.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED);
				registerCloseableForCancellation(outputStream);

				while (true) {
					int numBytes = inputStream.read(buffer);

					if (numBytes == -1) {
						break;
					}

					outputStream.write(buffer, 0, numBytes);
				}

				StreamStateHandle result = null;
				if (unregisterCloseableFromCancellation(outputStream)) {
					result = outputStream.closeAndGetHandle();
					outputStream = null;
				}
				return result;

			} finally {

				if (unregisterCloseableFromCancellation(inputStream)) {
					IOUtils.closeQuietly(inputStream);
				}

				if (unregisterCloseableFromCancellation(outputStream)) {
					IOUtils.closeQuietly(outputStream);
				}
			}
		}

		@Nonnull
		private SnapshotResult<StreamStateHandle> materializeMetaData() throws Exception {

			CheckpointStreamWithResultProvider streamWithResultProvider =

				localRecoveryConfig.isLocalRecoveryEnabled() ?

					CheckpointStreamWithResultProvider.createDuplicatingStream(
						checkpointId,
						CheckpointedStateScope.EXCLUSIVE,
						checkpointStreamFactory,
						localRecoveryConfig.getLocalStateDirectoryProvider()) :

					CheckpointStreamWithResultProvider.createSimpleStream(
						CheckpointedStateScope.EXCLUSIVE,
						checkpointStreamFactory);

			registerCloseableForCancellation(streamWithResultProvider);

			try {
				//no need for compression scheme support because sst-files are already compressed
				KeyedBackendSerializationProxy<K> serializationProxy =
					new KeyedBackendSerializationProxy<>(
						keySerializer,
						stateMetaInfoSnapshots,
						false);

				DataOutputView out =
					new DataOutputViewStreamWrapper(streamWithResultProvider.getCheckpointOutputStream());

				serializationProxy.write(out);

				if (unregisterCloseableFromCancellation(streamWithResultProvider)) {
					SnapshotResult<StreamStateHandle> result =
						streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
					streamWithResultProvider = null;
					return result;
				} else {
					throw new IOException("Stream already closed and cannot return a handle.");
				}
			} finally {
				if (streamWithResultProvider != null) {
					if (unregisterCloseableFromCancellation(streamWithResultProvider)) {
						IOUtils.closeQuietly(streamWithResultProvider);
					}
				}
			}
		}
	}
}
