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
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.DoneFuture;
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
import org.apache.flink.runtime.state.SnapshotStrategy;
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.UUID;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;

/**
 * Snapshot strategy for {@link org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend} that is based
 * on RocksDB's native checkpoints and creates incremental snapshots.
 *
 * @param <K> type of the backend keys.
 */
public class RocksIncrementalSnapshotStrategy<K> extends SnapshotStrategyBase<K> {

	private static final Logger LOG = LoggerFactory.getLogger(RocksIncrementalSnapshotStrategy.class);

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

	/** We delegate snapshots that are for savepoints to this. */
	@Nonnull
	private final SnapshotStrategy<SnapshotResult<KeyedStateHandle>> savepointDelegate;

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
		long lastCompletedCheckpointId,
		@Nonnull SnapshotStrategy<SnapshotResult<KeyedStateHandle>> savepointDelegate) {

		super(
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
		this.savepointDelegate = savepointDelegate;
	}

	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> performSnapshot(
		long checkpointId,
		long checkpointTimestamp,
		CheckpointStreamFactory checkpointStreamFactory,
		CheckpointOptions checkpointOptions) throws Exception {

		// for savepoints, we delegate to the full snapshot strategy because savepoints are always self-contained.
		if (CheckpointType.SAVEPOINT == checkpointOptions.getCheckpointType()) {
			return savepointDelegate.performSnapshot(
				checkpointId,
				checkpointTimestamp,
				checkpointStreamFactory,
				checkpointOptions);
		}

		if (kvStateInformation.isEmpty()) {
			if (LOG.isDebugEnabled()) {
				LOG.debug("Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning null.", checkpointTimestamp);
			}
			return DoneFuture.of(SnapshotResult.empty());
		}

		SnapshotDirectory snapshotDirectory;

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
			snapshotDirectory = SnapshotDirectory.permanent(path);
		} else {
			// create a "temporary" snapshot directory because local recovery is inactive.
			Path path = new Path(instanceBasePath.getAbsolutePath(), "chk-" + checkpointId);
			snapshotDirectory = SnapshotDirectory.temporary(path);
		}

		final RocksDBIncrementalSnapshotOperation snapshotOperation =
			new RocksDBIncrementalSnapshotOperation(
				checkpointStreamFactory,
				snapshotDirectory,
				checkpointId);

		try {
			snapshotOperation.takeSnapshot();
		} catch (Exception e) {
			snapshotOperation.stop();
			snapshotOperation.releaseResources(true);
			throw e;
		}

		return new FutureTask<SnapshotResult<KeyedStateHandle>>(
			snapshotOperation::runSnapshot
		) {
			@Override
			public boolean cancel(boolean mayInterruptIfRunning) {
				snapshotOperation.stop();
				return super.cancel(mayInterruptIfRunning);
			}

			@Override
			protected void done() {
				snapshotOperation.releaseResources(isCancelled());
			}
		};
	}

	@Override
	public void notifyCheckpointComplete(long completedCheckpointId) {
		synchronized (materializedSstFiles) {

			if (completedCheckpointId < lastCompletedCheckpointId) {
				return;
			}

			materializedSstFiles.keySet().removeIf(checkpointId -> checkpointId < completedCheckpointId);

			lastCompletedCheckpointId = completedCheckpointId;
		}
	}

	/**
	 * Encapsulates the process to perform an incremental snapshot of a RocksDBKeyedStateBackend.
	 */
	private final class RocksDBIncrementalSnapshotOperation {

		/**
		 * Stream factory that creates the outpus streams to DFS.
		 */
		private final CheckpointStreamFactory checkpointStreamFactory;

		/**
		 * Id for the current checkpoint.
		 */
		private final long checkpointId;

		/**
		 * All sst files that were part of the last previously completed checkpoint.
		 */
		private Set<StateHandleID> baseSstFiles;

		/**
		 * The state meta data.
		 */
		private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

		/**
		 * Local directory for the RocksDB native backup.
		 */
		private SnapshotDirectory localBackupDirectory;

		// Registry for all opened i/o streams
		private final CloseableRegistry closeableRegistry;

		// new sst files since the last completed checkpoint
		private final Map<StateHandleID, StreamStateHandle> sstFiles;

		// handles to the misc files in the current snapshot
		private final Map<StateHandleID, StreamStateHandle> miscFiles;

		// This lease protects from concurrent disposal of the native rocksdb instance.
		private final ResourceGuard.Lease dbLease;

		private SnapshotResult<StreamStateHandle> metaStateHandle;

		private RocksDBIncrementalSnapshotOperation(
			CheckpointStreamFactory checkpointStreamFactory,
			SnapshotDirectory localBackupDirectory,
			long checkpointId) throws IOException {

			this.checkpointStreamFactory = checkpointStreamFactory;
			this.checkpointId = checkpointId;
			this.localBackupDirectory = localBackupDirectory;
			this.stateMetaInfoSnapshots = new ArrayList<>();
			this.closeableRegistry = new CloseableRegistry();
			this.sstFiles = new HashMap<>();
			this.miscFiles = new HashMap<>();
			this.metaStateHandle = null;
			this.dbLease = rocksDBResourceGuard.acquireResource();
		}

		private StreamStateHandle materializeStateData(Path filePath) throws Exception {
			FSDataInputStream inputStream = null;
			CheckpointStreamFactory.CheckpointStateOutputStream outputStream = null;

			try {
				final byte[] buffer = new byte[8 * 1024];

				FileSystem backupFileSystem = localBackupDirectory.getFileSystem();
				inputStream = backupFileSystem.open(filePath);
				closeableRegistry.registerCloseable(inputStream);

				outputStream = checkpointStreamFactory
					.createCheckpointStateOutputStream(CheckpointedStateScope.SHARED);
				closeableRegistry.registerCloseable(outputStream);

				while (true) {
					int numBytes = inputStream.read(buffer);

					if (numBytes == -1) {
						break;
					}

					outputStream.write(buffer, 0, numBytes);
				}

				StreamStateHandle result = null;
				if (closeableRegistry.unregisterCloseable(outputStream)) {
					result = outputStream.closeAndGetHandle();
					outputStream = null;
				}
				return result;

			} finally {

				if (closeableRegistry.unregisterCloseable(inputStream)) {
					inputStream.close();
				}

				if (closeableRegistry.unregisterCloseable(outputStream)) {
					outputStream.close();
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

			try {
				closeableRegistry.registerCloseable(streamWithResultProvider);

				//no need for compression scheme support because sst-files are already compressed
				KeyedBackendSerializationProxy<K> serializationProxy =
					new KeyedBackendSerializationProxy<>(
						keySerializer,
						stateMetaInfoSnapshots,
						false);

				DataOutputView out =
					new DataOutputViewStreamWrapper(streamWithResultProvider.getCheckpointOutputStream());

				serializationProxy.write(out);

				if (closeableRegistry.unregisterCloseable(streamWithResultProvider)) {
					SnapshotResult<StreamStateHandle> result =
						streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
					streamWithResultProvider = null;
					return result;
				} else {
					throw new IOException("Stream already closed and cannot return a handle.");
				}
			} finally {
				if (streamWithResultProvider != null) {
					if (closeableRegistry.unregisterCloseable(streamWithResultProvider)) {
						IOUtils.closeQuietly(streamWithResultProvider);
					}
				}
			}
		}

		void takeSnapshot() throws Exception {

			final long lastCompletedCheckpoint;

			// use the last completed checkpoint as the comparison base.
			synchronized (materializedSstFiles) {
				lastCompletedCheckpoint = lastCompletedCheckpointId;
				baseSstFiles = materializedSstFiles.get(lastCompletedCheckpoint);
			}

			LOG.trace("Taking incremental snapshot for checkpoint {}. Snapshot is based on last completed checkpoint {} " +
				"assuming the following (shared) files as base: {}.", checkpointId, lastCompletedCheckpoint, baseSstFiles);

			// save meta data
			for (Map.Entry<String, Tuple2<ColumnFamilyHandle, RegisteredStateMetaInfoBase>> stateMetaInfoEntry
				: kvStateInformation.entrySet()) {
				stateMetaInfoSnapshots.add(stateMetaInfoEntry.getValue().f1.snapshot());
			}

			LOG.trace("Local RocksDB checkpoint goes to backup path {}.", localBackupDirectory);

			if (localBackupDirectory.exists()) {
				throw new IllegalStateException("Unexpected existence of the backup directory.");
			}

			// create hard links of living files in the snapshot path
			try (Checkpoint checkpoint = Checkpoint.create(db)) {
				checkpoint.createCheckpoint(localBackupDirectory.getDirectory().getPath());
			}
		}

		@Nonnull
		SnapshotResult<KeyedStateHandle> runSnapshot() throws Exception {

			cancelStreamRegistry.registerCloseable(closeableRegistry);

			// write meta data
			metaStateHandle = materializeMetaData();

			// sanity checks - they should never fail
			Preconditions.checkNotNull(metaStateHandle,
				"Metadata was not properly created.");
			Preconditions.checkNotNull(metaStateHandle.getJobManagerOwnedSnapshot(),
				"Metadata for job manager was not properly created.");

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
							sstFiles.put(stateHandleID, materializeStateData(filePath));
						}
					} else {
						StreamStateHandle fileHandle = materializeStateData(filePath);
						miscFiles.put(stateHandleID, fileHandle);
					}
				}
			}

			synchronized (materializedSstFiles) {
				materializedSstFiles.put(checkpointId, sstFiles.keySet());
			}

			IncrementalKeyedStateHandle jmIncrementalKeyedStateHandle = new IncrementalKeyedStateHandle(
				backendUID,
				keyGroupRange,
				checkpointId,
				sstFiles,
				miscFiles,
				metaStateHandle.getJobManagerOwnedSnapshot());

			StreamStateHandle taskLocalSnapshotMetaDataStateHandle = metaStateHandle.getTaskLocalSnapshot();
			DirectoryStateHandle directoryStateHandle = null;

			try {

				directoryStateHandle = localBackupDirectory.completeSnapshotAndGetHandle();
			} catch (IOException ex) {

				Exception collector = ex;

				try {
					taskLocalSnapshotMetaDataStateHandle.discardState();
				} catch (Exception discardEx) {
					collector = ExceptionUtils.firstOrSuppressed(discardEx, collector);
				}

				LOG.warn("Problem with local state snapshot.", collector);
			}

			if (directoryStateHandle != null && taskLocalSnapshotMetaDataStateHandle != null) {

				IncrementalLocalKeyedStateHandle localDirKeyedStateHandle =
					new IncrementalLocalKeyedStateHandle(
						backendUID,
						checkpointId,
						directoryStateHandle,
						keyGroupRange,
						taskLocalSnapshotMetaDataStateHandle,
						sstFiles.keySet());
				return SnapshotResult.withLocalState(jmIncrementalKeyedStateHandle, localDirKeyedStateHandle);
			} else {
				return SnapshotResult.of(jmIncrementalKeyedStateHandle);
			}
		}

		void stop() {

			if (cancelStreamRegistry.unregisterCloseable(closeableRegistry)) {
				try {
					closeableRegistry.close();
				} catch (IOException e) {
					LOG.warn("Could not properly close io streams.", e);
				}
			}
		}

		void releaseResources(boolean canceled) {

			dbLease.close();

			if (cancelStreamRegistry.unregisterCloseable(closeableRegistry)) {
				try {
					closeableRegistry.close();
				} catch (IOException e) {
					LOG.warn("Exception on closing registry.", e);
				}
			}

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

			if (canceled) {
				Collection<StateObject> statesToDiscard =
					new ArrayList<>(1 + miscFiles.size() + sstFiles.size());

				statesToDiscard.add(metaStateHandle);
				statesToDiscard.addAll(miscFiles.values());
				statesToDiscard.addAll(sstFiles.values());

				try {
					StateUtil.bestEffortDiscardAllStateObjects(statesToDiscard);
				} catch (Exception e) {
					LOG.warn("Could not properly discard states.", e);
				}

				if (localBackupDirectory.isSnapshotCompleted()) {
					try {
						DirectoryStateHandle directoryStateHandle = localBackupDirectory.completeSnapshotAndGetHandle();
						if (directoryStateHandle != null) {
							directoryStateHandle.discardState();
						}
					} catch (Exception e) {
						LOG.warn("Could not properly discard local state.", e);
					}
				}
			}
		}
	}
}
