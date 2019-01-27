/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateSnapshot;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateSnapshot;
import org.apache.flink.runtime.state.InternalBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfo;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StateUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.flink.contrib.streaming.state.RocksDBStorageInstance.SST_FILE_SUFFIX;

/**
 * Incremental snapshot related operations of RocksDB state backend, this behaves better then {@link RocksDBFullSnapshotOperation} in general.
 */
public class RocksDBIncrementalSnapshotOperation {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBIncrementalSnapshotOperation.class);

	private final RocksDBInternalStateBackend stateBackend;

	/** Stream factory that creates the output streams to DFS. */
	private final CheckpointStreamFactory checkpointStreamFactory;

	/** Local directory for the RocksDB native backup. */
	private SnapshotDirectory localBackupDirectory;

	private final long checkpointId;

	/** All sst files that were part of the last previously completed checkpoint. */
	private Map<StateHandleID, Tuple2<String, StreamStateHandle>> baseSstFiles;

	/** Registry for all opened i/o streams. */
	private final CloseableRegistry closeableRegistry = new CloseableRegistry();

	/** new sst files since the last completed checkpoint. */
	private final Map<StateHandleID, Tuple2<String, StreamStateHandle>> sstFiles = new HashMap<>();

	/** handles to the misc files in the current snapshot. */
	private final Map<StateHandleID, StreamStateHandle> miscFiles = new HashMap<>();

	private final ResourceGuard.Lease dbLease;

	private List<StateMetaInfoSnapshot> keyedStateMetaInfos;

	private List<StateMetaInfoSnapshot> subKeyedStateMetaInfos;

	private SnapshotResult<StreamStateHandle> metaStateHandle = null;

	RocksDBIncrementalSnapshotOperation(
		RocksDBInternalStateBackend stateBackend,
		CheckpointStreamFactory checkpointStreamFactory,
		SnapshotDirectory localBackupDirectory,
		long checkpointId) throws IOException {

		this.stateBackend = stateBackend;
		this.checkpointStreamFactory = checkpointStreamFactory;
		this.checkpointId = checkpointId;
		this.dbLease = this.stateBackend.rocksDBResourceGuard.acquireResource();
		this.localBackupDirectory = localBackupDirectory;
		this.keyedStateMetaInfos = new ArrayList<>();
		this.subKeyedStateMetaInfos = new ArrayList<>();
	}

	void takeSnapshot() throws Exception {
		for (Map.Entry<String, RegisteredStateMetaInfo> stateMetaInfoEntry : stateBackend.getRegisteredStateMetaInfos().entrySet()) {
			String stateName = stateMetaInfoEntry.getKey();
			RegisteredStateMetaInfo registeredStateMetaInfo = stateBackend.getRegisteredStateMetaInfos().get(stateName);
			if (registeredStateMetaInfo.getStateType().isKeyedState()) {
				keyedStateMetaInfos.add(registeredStateMetaInfo.snapshot());
			} else {
				subKeyedStateMetaInfos.add(registeredStateMetaInfo.snapshot());
			}
		}

		final long lastCompletedCheckpoint;

		// use the last completed checkpoint as the comparison base.
		synchronized (stateBackend.materializedSstFiles) {
			lastCompletedCheckpoint = stateBackend.lastCompletedCheckpointId;
			baseSstFiles = stateBackend.materializedSstFiles.get(lastCompletedCheckpoint);
		}

		LOG.trace("Taking incremental snapshot for checkpoint {}. Snapshot is based on last completed checkpoint {} " +
			"assuming the following (shared) files as base: {}.", checkpointId, lastCompletedCheckpoint, baseSstFiles);

		LOG.trace("Local RocksDB checkpoint goes to backup path {}.", localBackupDirectory);

		if (localBackupDirectory.exists()) {
			throw new IllegalStateException("Unexpected existence of the backup directory.");
		}

		// create hard links of living files in the snapshot path
		stateBackend.takeDbSnapshot(localBackupDirectory.getDirectory().getPath());
	}

	@Nonnull
	SnapshotResult<KeyedStateHandle> runSnapshot() throws Exception {

		stateBackend.getCancelStreamRegistry().registerCloseable(closeableRegistry);

		// write meta data
		metaStateHandle = materializeMetaData();

		// sanity checks - they should never fail
		Preconditions.checkNotNull(metaStateHandle,
			"Metadata was not properly created.");

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
						baseSstFiles != null && baseSstFiles.containsKey(stateHandleID);

					if (existsAlready) {
						Tuple2<String, StreamStateHandle> tuple2 = baseSstFiles.get(stateHandleID);
						// we introduce a placeholder state handle, that is replaced with the
						// original from the shared state registry (created from a previous checkpoint)
						sstFiles.put(
							stateHandleID,
							Tuple2.of(tuple2.f0, new PlaceholderStreamStateHandle()));
					} else {
						StreamStateHandle streamStateHandle = materializeStateData(filePath);
						// unique id for the materialized stream state handle.
						// For file state-handle, this id is the file path.
						// For byte-stream state-handle, this id is the unique handle name.
						String uniqueId;
						if (streamStateHandle instanceof FileStateHandle) {
							uniqueId = ((FileStateHandle) streamStateHandle).getFilePath().toString();
						} else if (streamStateHandle instanceof ByteStreamStateHandle) {
							uniqueId = ((ByteStreamStateHandle) streamStateHandle).getHandleName();
						} else {
							throw new UnsupportedOperationException("RocksDB incremental snapshot operation cannot support non FileStateHandle/ByteStreamStateHandle");
						}
						sstFiles.put(stateHandleID, Tuple2.of(uniqueId, streamStateHandle));
					}
				} else {
					StreamStateHandle fileHandle = materializeStateData(filePath);
					miscFiles.put(stateHandleID, fileHandle);
				}
			}
		}

		synchronized (stateBackend.materializedSstFiles) {
			stateBackend.materializedSstFiles.put(checkpointId, sstFiles);
		}

		KeyedStateHandle stateSnapshot =
			new IncrementalKeyedStateSnapshot(
				stateBackend.getKeyGroupRange(),
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

				Map<StateHandleID, String> sharedStateHandleIDs = new HashMap<>();
				for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> entry : sstFiles.entrySet()) {
					sharedStateHandleIDs.put(entry.getKey(), entry.getValue().f0);
				}
				KeyedStateHandle localStateSnapshot =
					new IncrementalLocalKeyedStateSnapshot(
						stateBackend.getKeyGroupRange(),
						checkpointId,
						taskLocalSnapshotMetaDataStateHandle,
						directoryStateHandle,
						sharedStateHandleIDs);
				return SnapshotResult.withLocalState(stateSnapshot, localStateSnapshot);
			} else {
				return SnapshotResult.of(stateSnapshot);
			}
	}

	void stop() {
		if (stateBackend.getCancelStreamRegistry().unregisterCloseable(closeableRegistry)) {
			try {
				closeableRegistry.close();
			} catch (IOException e) {
				LOG.warn("Could not properly close io streams.", e);
			}
		}
	}

	void releaseResources(boolean canceled) {
		dbLease.close();

		if (stateBackend.getCancelStreamRegistry().unregisterCloseable(closeableRegistry)) {
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
			statesToDiscard.addAll(sstFiles.values().stream().map(t -> t.f1).collect(Collectors.toSet()));

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

	private StreamStateHandle materializeStateData(Path filePath) throws Exception {
		FSDataInputStream inputStream = null;
		CheckpointStreamFactory.CheckpointStateOutputStream outputStream = null;

		try {
			final byte[] buffer = new byte[8 * 1024];

			FileSystem backupFileSystem = localBackupDirectory.getFileSystem();
			inputStream = backupFileSystem.open(filePath);
			closeableRegistry.registerCloseable(inputStream);

			outputStream = checkpointStreamFactory
				.createCheckpointStateOutputStream(checkpointId, CheckpointedStateScope.SHARED);
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

	private SnapshotResult<StreamStateHandle> materializeMetaData() throws Exception {
		LocalRecoveryConfig localRecoveryConfig = stateBackend.localRecoveryConfig;

		CheckpointStreamWithResultProvider streamWithResultProvider =

			localRecoveryConfig.isLocalRecoveryEnabled() ?

				CheckpointStreamWithResultProvider.createDuplicatingStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					checkpointStreamFactory,
					localRecoveryConfig.getLocalStateDirectoryProvider()) :

				CheckpointStreamWithResultProvider.createSimpleStream(
					checkpointId,
					CheckpointedStateScope.EXCLUSIVE,
					checkpointStreamFactory);

		try {
			closeableRegistry.registerCloseable(streamWithResultProvider);

			CheckpointStreamFactory.CheckpointStateOutputStream outputStream = streamWithResultProvider.getCheckpointOutputStream();
			DataOutputViewStreamWrapper outputView =
				new DataOutputViewStreamWrapper(outputStream);

			InternalBackendSerializationProxy backendSerializationProxy = new InternalBackendSerializationProxy(
				keyedStateMetaInfos,
				subKeyedStateMetaInfos,
				!Objects.equals(
					UncompressedStreamCompressionDecorator.INSTANCE,
					stateBackend.getKeyGroupCompressionDecorator()));

			backendSerializationProxy.write(outputView);

			if (closeableRegistry.unregisterCloseable(streamWithResultProvider)) {
				SnapshotResult<StreamStateHandle> resultStateHandle = streamWithResultProvider.closeAndFinalizeCheckpointStreamResult();
				streamWithResultProvider = null;
				return resultStateHandle;
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
}
