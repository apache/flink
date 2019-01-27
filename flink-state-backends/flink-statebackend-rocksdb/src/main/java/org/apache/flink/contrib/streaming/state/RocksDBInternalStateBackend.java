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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.io.async.AbstractAsyncCallableWithResources;
import org.apache.flink.runtime.io.async.AsyncStoppableTaskWithCallback;
import org.apache.flink.runtime.query.TaskKvStateRegistry;
import org.apache.flink.runtime.state.AbstractInternalStateBackend;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointStreamWithResultProvider;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.DoneFuture;
import org.apache.flink.runtime.state.IncrementalKeyedStateSnapshot;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateSnapshot;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.LocalRecoveryDirectoryProvider;
import org.apache.flink.runtime.state.RegisteredStateMetaInfo;
import org.apache.flink.runtime.state.SnapshotDirectory;
import org.apache.flink.runtime.state.SnapshotResult;
import org.apache.flink.runtime.state.SnapshotStrategy;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateStorage;
import org.apache.flink.runtime.state.StorageIterator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.keyed.KeyedState;
import org.apache.flink.runtime.state.subkeyed.SubKeyedState;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.ResourceGuard;
import org.apache.flink.util.function.SupplierWithException;

import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.FutureTask;
import java.util.concurrent.RunnableFuture;
import java.util.stream.Collectors;

/**
 * A State Backend that stores its state in {@code RocksDB}. This state backend can
 * store very large state that exceeds memory and spills to disk.
 *
 * <p>All key/value state (including windows) is stored in the key/value index of rocksDB.
 * For persistence against loss of machines, checkpoints take a snapshot of the
 * rocksDB database, and persist that snapshot in a file system (by default) or
 * another configurable state backend.
 */
public class RocksDBInternalStateBackend extends AbstractInternalStateBackend {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBInternalStateBackend.class);

	/** The name of the merge operator in RocksDB. Do not change except you know exactly what you do. */
	private static final String MERGE_OPERATOR_NAME = "stringappendtest";

	/** The DB options from the options factory. */
	private final DBOptions dbOptions;

	/** The column family options from the options factory. */
	private final ColumnFamilyOptions columnOptions;

	/** Default column family handle of current RocksDB instance. */
	private ColumnFamilyHandle defaultColumnFamilyHandle;

	/** Default column family descriptor of current RocksDB instance. */
	private final ColumnFamilyDescriptor defaultColumnFamilyDescriptor;

	private final String defaultColumnFamilyName = "default";

	/**
	 * Protects access to RocksDB in other threads, like the checkpointing thread from parallel call that disposes the
	 * RocksDb object.
	 */
	final ResourceGuard rocksDBResourceGuard;

	/**
	 * The RocksDB instance associates with the current state backend.
	 * All data will write to it.
	 */
	private RocksDB db;

	/**
	 * Write options for RocksDB.
	 */
	private WriteOptions writeOptions;

	/**
	 * Opened ColumnFamily handles and its descriptors of current state backend.
	 */
	private final Map<String, Tuple2<ColumnFamilyHandle, ColumnFamilyDescriptor>> columnFamilyHandles;

	/** Path where this configured instance stores its data directory. */
	private File instanceBasePath;

	/** Path where this configured instance stores its RocksDB database. */
	private File instanceRocksDBPath;

	// -- runtime values, set on TaskManager when initializing / using the backend

	/** True if incremental checkpointing is enabled. */
	private final boolean enableIncrementalCheckpointing;

	/**
	 * The state handle ids of all sst files materialized in snapshots for previous checkpoints.
	 * This sortedMap contains checkpointId as key, another map as value.
	 * And the value-map contains local {@link StateHandleID} as key,
	 * a {@link Tuple2} of (unique global id, {@link StreamStateHandle}) as value.
	 */
	final SortedMap<Long, Map<StateHandleID, Tuple2<String, StreamStateHandle>>> materializedSstFiles;

	/** The identifier of the last completed checkpoint. */
	long lastCompletedCheckpointId = -1L;

	/** The configuration of local recovery. */
	final LocalRecoveryConfig localRecoveryConfig;

	/** The snapshot strategy, e.g., if we use full or incremental checkpoints, local state, and so on. */
	private final SnapshotStrategy<SnapshotResult<KeyedStateHandle>> snapshotStrategy;

	public RocksDBInternalStateBackend(
		ClassLoader userClassLoader,
		File instanceBasePath,
		DBOptions dbOptions,
		ColumnFamilyOptions columnOptions,
		int numberOfGroups,
		KeyGroupRange keyGroupRange,
		boolean enableIncrementalCheckpointing,
		LocalRecoveryConfig localRecoveryConfig,
		TaskKvStateRegistry kvStateRegistry,
		ExecutionConfig executionConfig) throws IOException {

		super(numberOfGroups, keyGroupRange, userClassLoader, kvStateRegistry, executionConfig);

		this.dbOptions = Preconditions.checkNotNull(dbOptions);
		// ensure that we use the right merge operator, because other code relies on this
		this.columnOptions = Preconditions.checkNotNull(columnOptions)
			.setMergeOperatorName(MERGE_OPERATOR_NAME);
		this.defaultColumnFamilyDescriptor = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, columnOptions);

		this.instanceBasePath = Preconditions.checkNotNull(instanceBasePath);
		this.instanceRocksDBPath = new File(instanceBasePath, "db");

		checkAndCreateDirectory(instanceBasePath);

		if (instanceRocksDBPath.exists()) {
			// Clear the base directory when the backend is created
			// in case something crashed and the backend never reached dispose()
			cleanInstanceBasePath();
		}

		this.rocksDBResourceGuard = new ResourceGuard();

		this.columnFamilyHandles = new HashMap<>();
		this.writeOptions = new WriteOptions().setDisableWAL(true);

		this.materializedSstFiles = new TreeMap<>();

		this.enableIncrementalCheckpointing = enableIncrementalCheckpointing;
		this.snapshotStrategy = enableIncrementalCheckpointing ?
			new RocksDBInternalStateBackend.IncrementalSnapshotStrategy() :
			new RocksDBInternalStateBackend.FullSnapshotStrategy();

		this.localRecoveryConfig = localRecoveryConfig;
	}

	@Override
	protected void closeImpl() {
		// This call will block until all clients that still acquire access to the RocksDB instance have released it,
		// so that we cannot release the native resources while clients are still working with it in parallel.
		rocksDBResourceGuard.close();

		// IMPORTANT: null reference to signal potential async checkpoint workers that the db was disposed, as
		// working on the disposed object results in SEGFAULTS.
		if (db != null) {
			IOUtils.closeQuietly(defaultColumnFamilyHandle);
			for (StateStorage stateStorage : getStateStorages().values()) {
				((RocksDBStorageInstance) stateStorage.getStorageInstance()).close();
			}
			IOUtils.closeQuietly(db);
			IOUtils.closeQuietly(writeOptions);
			IOUtils.closeQuietly(columnOptions);
			IOUtils.closeQuietly(dbOptions);

			db = null;
			cleanInstanceBasePath();
		}
	}

	@Override
	protected StateStorage getOrCreateStateStorageForKeyedState(RegisteredStateMetaInfo descriptor) {
		StateStorage stateStorage = stateStorages.get(descriptor.getName());

		if (stateStorage == null) {
			try {
				stateStorage = new RocksDBStateStorage(new RocksDBStorageInstance(
					db,
					getOrCreateColumnFamily(descriptor.getName()),
					writeOptions));
				stateStorages.put(descriptor.getName(), stateStorage);
			} catch (IOException e) {
				throw new StateAccessException(e);
			}
		}

		return stateStorage;
	}

	ColumnFamilyHandle getOrCreateColumnFamily(String handleName) throws IOException {
		if (columnFamilyHandles.containsKey(handleName)) {
			return columnFamilyHandles.get(handleName).f0;
		}

		try {
			ColumnFamilyDescriptor descriptor = createColumnFamilyDescriptor(handleName);
			ColumnFamilyHandle handle = db.createColumnFamily(descriptor);
			columnFamilyHandles.put(handleName, Tuple2.of(handle, descriptor));
			return handle;
		} catch (RocksDBException e) {
			throw new IOException("Error creating ColumnFamilyHandle.", e);
		}
	}

	ColumnFamilyDescriptor createColumnFamilyDescriptor(String cfName) {
		byte[] nameBytes = cfName.getBytes(ConfigConstants.DEFAULT_CHARSET);
		Preconditions.checkState(!Arrays.equals(RocksDB.DEFAULT_COLUMN_FAMILY, nameBytes),
			"The chosen state name 'default' collides with the name of the default column family!");

		return new ColumnFamilyDescriptor(nameBytes, columnOptions);
	}

	public WriteOptions getWriteOptions() {
		return this.writeOptions;
	}

	@Override
	protected StateStorage getOrCreateStateStorageForSubKeyedState(RegisteredStateMetaInfo stateMetaInfo) {
		StateStorage stateStorage = stateStorages.get(stateMetaInfo.getName());

		if (stateStorage == null) {
			try {
				stateStorage = new RocksDBStateStorage(
					new RocksDBStorageInstance(
						db,
						getOrCreateColumnFamily(stateMetaInfo.getName()),
						writeOptions));
			} catch (IOException e) {
				throw new StateAccessException(e);
			}
			stateStorages.put(stateMetaInfo.getName(), stateStorage);
		}

		return stateStorage;
	}

	@Override
	public void notifyCheckpointComplete(long completedCheckpointId) throws Exception {
		if (!enableIncrementalCheckpointing) {
			return;
		}

		synchronized (materializedSstFiles) {

			if (completedCheckpointId < lastCompletedCheckpointId) {
				return;
			}

			materializedSstFiles.keySet().removeIf(checkpointId -> checkpointId < completedCheckpointId);

			lastCompletedCheckpointId = completedCheckpointId;
		}
	}

	@Override
	public int numStateEntries() {
		int count = 0;
		List<StateStorage> stateStorages = getKeyedStates().values().stream().map(KeyedState::getStateStorage).collect(Collectors.toList());
		stateStorages.addAll(getSubKeyedStates().values().stream().map(SubKeyedState::getStateStorage).collect(Collectors.toList()));

		for (StateStorage stateStorage : stateStorages) {
			try (StorageIterator iterator = stateStorage.iterator()){

				while (iterator.hasNext()) {
					count++;
					iterator.next();
				}
			} catch (Exception e) {
				throw new StateAccessException(e);
			}
		}
		return count;
	}

	private void cleanInstanceBasePath() {
		LOG.info("Deleting existing instance base directory {}.", instanceBasePath);

		try {
			FileUtils.deleteDirectory(instanceBasePath);
		} catch (IOException ex) {
			LOG.warn("Could not delete instance base path for RocksDB: " + instanceBasePath, ex);
		}
	}

	/**
	 * Triggers an asynchronous snapshot of the keyed state backend from RocksDB. This snapshot can be canceled and
	 * is also stopped when the backend is closed through {@link #closeImpl()} ()}. For each backend, this method must always
	 * be called by the same thread.
	 *
	 * @param checkpointId  The Id of the checkpoint.
	 * @param timestamp     The timestamp of the checkpoint.
	 * @param streamFactory The factory that we can use for writing our state to streams.
	 * @param checkpointOptions Options for how to perform this checkpoint.
	 * @return Future to the state handle of the snapshot data.
	 * @throws Exception indicating a problem in the synchronous part of the checkpoint.
	 */
	@Override
	public RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot(
		long checkpointId,
		long timestamp,
		CheckpointStreamFactory streamFactory,
		CheckpointOptions checkpointOptions) throws Exception {

		return snapshotStrategy.performSnapshot(checkpointId, timestamp, streamFactory, checkpointOptions);
	}

	@Override
	public void restore(Collection<KeyedStateHandle> restoredSnapshots) throws Exception {
		LOG.info("Initializing RocksDB internal state backend.");

		if (LOG.isDebugEnabled()) {
			LOG.debug("Restoring snapshot from state handles: {}.", restoredSnapshots);
		}

		restoredKvStateMetaInfos.clear();

		try {
			if (restoredSnapshots == null || restoredSnapshots.isEmpty()) {
				createDB();
				LOG.info("Successfully created RocksDB state backend at {}.", instanceRocksDBPath);
			} else {
				LOG.info("Restoring RocksDB internal state backend at {}.", instanceRocksDBPath);

				long startMillis = System.currentTimeMillis();

				KeyedStateHandle stateSnapshot = restoredSnapshots.iterator().next();
				if (stateSnapshot instanceof IncrementalKeyedStateSnapshot || stateSnapshot instanceof IncrementalLocalKeyedStateSnapshot) {
					RocksDBIncrementalRestoreOperation restoreOperation = new RocksDBIncrementalRestoreOperation(this);
					restoreOperation.restore(restoredSnapshots);
				} else {
					RocksDBFullRestoreOperation restoreOperation = new RocksDBFullRestoreOperation(this);
					restoreOperation.restore(restoredSnapshots);
				}

				long endMillis = System.currentTimeMillis();
				LOG.info("Successfully restored RocksDB internal state-backend at {}, duration {} ms.", instanceRocksDBPath, (endMillis - startMillis));
			}
		} catch (Exception ex) {
			closeImpl();
			throw ex;
		}

	}

	File getInstanceBasePath() {
		if (instanceBasePath == null) {
			throw new IllegalStateException("RocksDBInternalStateBackend has not been initialized," +
				" it's illegal to get the instance base path.");
		}
		return instanceBasePath;
	}

	public File getInstanceRocksDBPath() {
		if (instanceRocksDBPath == null) {
			throw new IllegalStateException("RocksDBInternalStateBackend has not been initialized," +
				" it's illegal to get the instance DB path.");
		}
		return instanceRocksDBPath;
	}

	Path getLocalRestorePath(KeyGroupRange groupRange) {
		Preconditions.checkNotNull(instanceBasePath);
		String dirName = String.format("%s-%d-%d",
			"restore",
			groupRange.getStartKeyGroup(),
			groupRange.getEndKeyGroup());
		return new Path(instanceBasePath.getAbsolutePath(), dirName);
	}

	CloseableRegistry getCancelStreamRegistry() {
		return cancelStreamRegistry;
	}

	private static void checkAndCreateDirectory(File directory) throws IOException {
		if (directory.exists()) {
			if (!directory.isDirectory()) {
				throw new IOException("Not a directory: " + directory);
			}
		} else {
			if (!directory.mkdirs()) {
				throw new IOException(
					String.format("Could not create RocksDB data directory at %s.", directory));
			}
		}
	}

	private class IncrementalSnapshotStrategy implements SnapshotStrategy<SnapshotResult<KeyedStateHandle>> {

		private final SnapshotStrategy<SnapshotResult<KeyedStateHandle>> savepointDelegate;

		public IncrementalSnapshotStrategy() {
			this.savepointDelegate = new RocksDBInternalStateBackend.FullSnapshotStrategy();
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

			if (db == null) {
				throw new IOException("RocksDB closed.");
			}

			if (getKeyedStates().isEmpty() && getSubKeyedStates().isEmpty()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning empty snapshot.", checkpointTimestamp);
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
					RocksDBInternalStateBackend.this,
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
	}

	private class FullSnapshotStrategy implements SnapshotStrategy<SnapshotResult<KeyedStateHandle>> {

		@Override
		public RunnableFuture<SnapshotResult<KeyedStateHandle>> performSnapshot(
			long checkpointId,
			long timestamp,
			CheckpointStreamFactory primaryStreamFactory,
			CheckpointOptions checkpointOptions) throws Exception {

			long startTime = System.currentTimeMillis();
			if (registeredStateMetaInfos.isEmpty()) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Asynchronous RocksDB snapshot performed on empty keyed state at {}. Returning empty snapshot.", timestamp);
				}

				return DoneFuture.of(SnapshotResult.empty());
			}

			final SupplierWithException<CheckpointStreamWithResultProvider, Exception> supplier =

				localRecoveryConfig.isLocalRecoveryEnabled() &&
					(CheckpointType.SAVEPOINT != checkpointOptions.getCheckpointType()) ?

					() -> CheckpointStreamWithResultProvider.createDuplicatingStream(
						checkpointId,
						CheckpointedStateScope.EXCLUSIVE,
						primaryStreamFactory,
						localRecoveryConfig.getLocalStateDirectoryProvider()) :

					() -> CheckpointStreamWithResultProvider.createSimpleStream(
						checkpointId,
						CheckpointedStateScope.EXCLUSIVE,
						primaryStreamFactory);

			final CloseableRegistry snapshotCloseableRegistry = new CloseableRegistry();

			final RocksDBFullSnapshotOperation snapshotOperation =
				new RocksDBFullSnapshotOperation(
					RocksDBInternalStateBackend.this,
					checkpointId,
					supplier,
					snapshotCloseableRegistry);

			snapshotOperation.takeDBSnapShot();

			// implementation of the async IO operation, based on FutureTask
			AbstractAsyncCallableWithResources<SnapshotResult<KeyedStateHandle>> ioCallable =
				new AbstractAsyncCallableWithResources<SnapshotResult<KeyedStateHandle>>() {

					@Override
					protected void acquireResources() throws Exception {
						cancelStreamRegistry.registerCloseable(snapshotCloseableRegistry);
						snapshotOperation.openCheckpointStream();
					}

					@Override
					protected void releaseResources() throws Exception {
						closeLocalRegistry();
						releaseSnapshotOperationResources();
					}

					private void releaseSnapshotOperationResources() {
						// hold the db lock while operation on the db to guard us against async db disposal
						snapshotOperation.releaseSnapshotResources();
					}

					@Override
					protected void stopOperation() throws Exception {
						closeLocalRegistry();
					}

					private void closeLocalRegistry() {
						if (cancelStreamRegistry.unregisterCloseable(snapshotCloseableRegistry)) {
							try {
								snapshotCloseableRegistry.close();
							} catch (Exception ex) {
								LOG.warn("Error closing local registry", ex);
							}
						}
					}

					@Nonnull
					@Override
					public SnapshotResult<KeyedStateHandle> performOperation() throws Exception {
						long startTime = System.currentTimeMillis();

						if (isStopped()) {
							throw new IOException("RocksDB closed.");
						}

						snapshotOperation.writeDBSnapshot();

						LOG.info("Asynchronous RocksDB snapshot ({}, asynchronous part) in thread {} took {} ms.",
							primaryStreamFactory, Thread.currentThread(), (System.currentTimeMillis() - startTime));

						return snapshotOperation.getKeyGroupStateSnapshot();
					}
				};

			LOG.info("Asynchronous RocksDB snapshot ({}, synchronous part) in thread {} took {} ms.",
				primaryStreamFactory, Thread.currentThread(), (System.currentTimeMillis() - startTime));
			return AsyncStoppableTaskWithCallback.from(ioCallable);
		}
	}

	/**
	 * The exceptions thrown when the internal RocksDB instance cannot be created.
	 */
	private class RocksDBInitException extends RuntimeException{
		private static final long serialVersionUID = 1L;

		RocksDBInitException(String message) {
			super("The rocksDB init failed with reported message: " + message);
		}
	}

	public RocksDB getDbInstance() {
		if (db == null) {
			try {
				createDB();
			} catch (IOException e) {
				throw new RocksDBInitException(e.getMessage());
			}
		}
		return db;
	}

	void takeDbSnapshot(String localCheckpointPath) throws RocksDBException {
		Checkpoint checkpoint = Checkpoint.create(db);
		checkpoint.createCheckpoint(localCheckpointPath);
	}

	void createDB() throws IOException {
		List<ColumnFamilyDescriptor> columnFamilyDescriptors = Collections.singletonList(defaultColumnFamilyDescriptor);
		List<ColumnFamilyHandle> stateColumnFamilyHandles = new ArrayList<>(1);
		try {
			this.db = RocksDB.open(dbOptions, instanceRocksDBPath.getAbsolutePath(), columnFamilyDescriptors, stateColumnFamilyHandles);
			defaultColumnFamilyHandle = stateColumnFamilyHandles.remove(0);
		} catch (RocksDBException e) {
			throw new IOException("Error while opening rocksDB instance at " + instanceRocksDBPath, e);
		}
	}

	ColumnFamilyDescriptor getDefaultColumnFamilyDescriptor() {
		return defaultColumnFamilyDescriptor;
	}

	void createDBWithColumnFamily(
		List<ColumnFamilyDescriptor> descriptors,
		List<String> descriptorName) throws IOException {
		Preconditions.checkState(db == null, "Can not create db twice.");

		try {
			int size = descriptors.size();
			List<ColumnFamilyHandle> handles = new ArrayList<>(size);
			db = RocksDB.open(dbOptions, instanceRocksDBPath.getAbsolutePath(), descriptors, handles);
			for (int i = 1; i < size; ++i) {
				columnFamilyHandles.put(descriptorName.get(i), Tuple2.of(handles.get(i), descriptors.get(i)));
			}
			defaultColumnFamilyHandle = handles.remove(0);
		} catch (RocksDBException e) {
			throw new IOException("Error while opening rocksDB instance at " + instanceBasePath, e);
		}
	}

	Map<String, Tuple2<ColumnFamilyHandle, ColumnFamilyDescriptor>> getColumnFamilyHandles() {
		return columnFamilyHandles;
	}

	void registerAllStates() {
		for (RegisteredStateMetaInfo stateMetaInfo : registeredStateMetaInfos.values()) {
			if (stateMetaInfo.getStateType().isKeyedState()) {
				getOrCreateStateStorageForKeyedState(stateMetaInfo);
			} else {
				getOrCreateStateStorageForSubKeyedState(stateMetaInfo);
			}
		}
	}

	String getDefaultColumnFamilyName() {
		return defaultColumnFamilyName;
	}
}
