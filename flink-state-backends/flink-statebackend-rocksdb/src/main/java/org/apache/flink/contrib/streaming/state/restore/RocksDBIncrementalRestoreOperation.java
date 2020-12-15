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

package org.apache.flink.contrib.streaming.state.restore;

import org.apache.flink.contrib.streaming.state.RocksDBIncrementalCheckpointUtils;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOperationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBStateDownloader;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.FileUtils;
import org.apache.flink.util.IOUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;
import static org.apache.flink.runtime.state.StateUtil.unexpectedStateHandleException;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Encapsulates the process of restoring a RocksDB instance from an incremental snapshot.
 */
public class RocksDBIncrementalRestoreOperation<K> extends AbstractRocksDBRestoreOperation<K> {

	private final String operatorIdentifier;
	private final SortedMap<Long, Set<StateHandleID>> restoredSstFiles;
	private long lastCompletedCheckpointId;
	private UUID backendUID;
	private final long writeBatchSize;

	public RocksDBIncrementalRestoreOperation(
		String operatorIdentifier,
		KeyGroupRange keyGroupRange,
		int keyGroupPrefixBytes,
		int numberOfTransferringThreads,
		CloseableRegistry cancelStreamRegistry,
		ClassLoader userCodeClassLoader,
		Map<String, RocksDbKvStateInfo> kvStateInformation,
		StateSerializerProvider<K> keySerializerProvider,
		File instanceBasePath,
		File instanceRocksDBPath,
		DBOptions dbOptions,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		RocksDBNativeMetricOptions nativeMetricOptions,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> restoreStateHandles,
		@Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager,
		@Nonnegative long writeBatchSize,
		Long writeBufferManagerCapacity) {
		super(keyGroupRange,
			keyGroupPrefixBytes,
			numberOfTransferringThreads,
			cancelStreamRegistry,
			userCodeClassLoader,
			kvStateInformation,
			keySerializerProvider,
			instanceBasePath,
			instanceRocksDBPath,
			dbOptions,
			columnFamilyOptionsFactory,
			nativeMetricOptions,
			metricGroup,
			restoreStateHandles,
			ttlCompactFiltersManager,
			writeBufferManagerCapacity);
		this.operatorIdentifier = operatorIdentifier;
		this.restoredSstFiles = new TreeMap<>();
		this.lastCompletedCheckpointId = -1L;
		this.backendUID = UUID.randomUUID();
		checkArgument(writeBatchSize >= 0, "Write batch size have to be no negative.");
		this.writeBatchSize = writeBatchSize;
	}

	/**
	 * Root method that branches for different implementations of {@link KeyedStateHandle}.
	 */
	@Override
	public RocksDBRestoreResult restore() throws Exception {

		if (restoreStateHandles == null || restoreStateHandles.isEmpty()) {
			return null;
		}

		final KeyedStateHandle theFirstStateHandle = restoreStateHandles.iterator().next();

		boolean isRescaling = (restoreStateHandles.size() > 1 ||
			!Objects.equals(theFirstStateHandle.getKeyGroupRange(), keyGroupRange));

		if (isRescaling) {
			restoreWithRescaling(restoreStateHandles);
		} else {
			restoreWithoutRescaling(theFirstStateHandle);
		}
		return new RocksDBRestoreResult(this.db, defaultColumnFamilyHandle,
			nativeMetricMonitor, lastCompletedCheckpointId, backendUID, restoredSstFiles);
	}

	/**
	 * Recovery from a single remote incremental state without rescaling.
	 */
	@SuppressWarnings("unchecked")
	private void restoreWithoutRescaling(KeyedStateHandle keyedStateHandle) throws Exception {
		logger.info("Starting to restore from state handle: {} without rescaling.", keyedStateHandle);
		if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle =
				(IncrementalRemoteKeyedStateHandle) keyedStateHandle;
			restorePreviousIncrementalFilesStatus(incrementalRemoteKeyedStateHandle);
			restoreFromRemoteState(incrementalRemoteKeyedStateHandle);
		} else if (keyedStateHandle instanceof IncrementalLocalKeyedStateHandle) {
			IncrementalLocalKeyedStateHandle incrementalLocalKeyedStateHandle =
				(IncrementalLocalKeyedStateHandle) keyedStateHandle;
			restorePreviousIncrementalFilesStatus(incrementalLocalKeyedStateHandle);
			restoreFromLocalState(incrementalLocalKeyedStateHandle);
		} else {
			throw unexpectedStateHandleException(
					new Class[]{IncrementalRemoteKeyedStateHandle.class, IncrementalLocalKeyedStateHandle.class},
					keyedStateHandle.getClass());
		}
		logger.info("Finished restoring from state handle: {} without rescaling.", keyedStateHandle);
	}

	private void restorePreviousIncrementalFilesStatus(IncrementalKeyedStateHandle localKeyedStateHandle) {
		backendUID = localKeyedStateHandle.getBackendIdentifier();
		restoredSstFiles.put(
			localKeyedStateHandle.getCheckpointId(),
			localKeyedStateHandle.getSharedStateHandleIDs());
		lastCompletedCheckpointId = localKeyedStateHandle.getCheckpointId();
	}

	private void restoreFromRemoteState(IncrementalRemoteKeyedStateHandle stateHandle) throws Exception {
		// used as restore source for IncrementalRemoteKeyedStateHandle
		final Path tmpRestoreInstancePath = instanceBasePath.getAbsoluteFile().toPath().resolve(UUID.randomUUID().toString());
		try {
			restoreFromLocalState(
				transferRemoteStateToLocalDirectory(tmpRestoreInstancePath, stateHandle));
		} finally {
			cleanUpPathQuietly(tmpRestoreInstancePath);
		}
	}

	private void restoreFromLocalState(IncrementalLocalKeyedStateHandle localKeyedStateHandle) throws Exception {
		KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(localKeyedStateHandle.getMetaDataState());
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();
		columnFamilyDescriptors = createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, true, writeBufferManagerCapacity);
		columnFamilyHandles = new ArrayList<>(columnFamilyDescriptors.size() + 1);

		Path restoreSourcePath = localKeyedStateHandle.getDirectoryStateHandle().getDirectory();

		logger.debug("Restoring keyed backend uid in operator {} from incremental snapshot to {}.",
			operatorIdentifier, backendUID);

		if (!instanceRocksDBPath.mkdirs()) {
			String errMsg = "Could not create RocksDB data directory: " + instanceBasePath.getAbsolutePath();
			logger.error(errMsg);
			throw new IOException(errMsg);
		}

		restoreInstanceDirectoryFromPath(restoreSourcePath, dbPath);

		openDB();

		registerColumnFamilyHandles(stateMetaInfoSnapshots);
	}

	private IncrementalLocalKeyedStateHandle transferRemoteStateToLocalDirectory(
		Path temporaryRestoreInstancePath,
		IncrementalRemoteKeyedStateHandle restoreStateHandle) throws Exception {

		try (RocksDBStateDownloader rocksDBStateDownloader = new RocksDBStateDownloader(numberOfTransferringThreads)) {
			rocksDBStateDownloader.transferAllStateDataToDirectory(
				restoreStateHandle,
				temporaryRestoreInstancePath,
				cancelStreamRegistry);
		}

		// since we transferred all remote state to a local directory, we can use the same code as for
		// local recovery.
		return new IncrementalLocalKeyedStateHandle(
			restoreStateHandle.getBackendIdentifier(),
			restoreStateHandle.getCheckpointId(),
			new DirectoryStateHandle(temporaryRestoreInstancePath),
			restoreStateHandle.getKeyGroupRange(),
			restoreStateHandle.getMetaStateHandle(),
			restoreStateHandle.getSharedState().keySet());
	}

	private void cleanUpPathQuietly(@Nonnull Path path) {
		try {
			FileUtils.deleteDirectory(path.toFile());
		} catch (IOException ex) {
			logger.warn("Failed to clean up path " + path, ex);
		}
	}

	private void registerColumnFamilyHandles(List<StateMetaInfoSnapshot> metaInfoSnapshots) {
		// Register CF handlers
		for (int i = 0; i < metaInfoSnapshots.size(); ++i) {
			getOrRegisterStateColumnFamilyHandle(columnFamilyHandles.get(i), metaInfoSnapshots.get(i));
		}
	}

	/**
	 * Recovery from multi incremental states with rescaling. For rescaling, this method creates a temporary
	 * RocksDB instance for a key-groups shard. All contents from the temporary instance are copied into the
	 * real restore instance and then the temporary instance is discarded.
	 */
	private void restoreWithRescaling(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {

		// Prepare for restore with rescaling
		KeyedStateHandle initialHandle = RocksDBIncrementalCheckpointUtils.chooseTheBestStateHandleForInitial(
			restoreStateHandles, keyGroupRange);

		// Init base DB instance
		if (initialHandle != null) {
			restoreStateHandles.remove(initialHandle);
			initDBWithRescaling(initialHandle);
		} else {
			openDB();
		}

		// Transfer remaining key-groups from temporary instance into base DB
		byte[] startKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getStartKeyGroup(), startKeyGroupPrefixBytes);

		byte[] stopKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getEndKeyGroup() + 1, stopKeyGroupPrefixBytes);

		for (KeyedStateHandle rawStateHandle : restoreStateHandles) {

			if (!(rawStateHandle instanceof IncrementalRemoteKeyedStateHandle)) {
				throw unexpectedStateHandleException(IncrementalRemoteKeyedStateHandle.class, rawStateHandle.getClass());
			}

			logger.info("Starting to restore from state handle: {} with rescaling.", rawStateHandle);
			Path temporaryRestoreInstancePath = instanceBasePath.getAbsoluteFile().toPath().resolve(UUID.randomUUID().toString());
			try (RestoredDBInstance tmpRestoreDBInfo = restoreDBInstanceFromStateHandle(
				(IncrementalRemoteKeyedStateHandle) rawStateHandle,
				temporaryRestoreInstancePath);
				RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(this.db, writeBatchSize)) {

				List<ColumnFamilyDescriptor> tmpColumnFamilyDescriptors = tmpRestoreDBInfo.columnFamilyDescriptors;
				List<ColumnFamilyHandle> tmpColumnFamilyHandles = tmpRestoreDBInfo.columnFamilyHandles;

				// iterating only the requested descriptors automatically skips the default column family handle
				for (int i = 0; i < tmpColumnFamilyDescriptors.size(); ++i) {
					ColumnFamilyHandle tmpColumnFamilyHandle = tmpColumnFamilyHandles.get(i);

					ColumnFamilyHandle targetColumnFamilyHandle = getOrRegisterStateColumnFamilyHandle(
						null, tmpRestoreDBInfo.stateMetaInfoSnapshots.get(i))
						.columnFamilyHandle;

					try (RocksIteratorWrapper iterator = RocksDBOperationUtils.getRocksIterator(tmpRestoreDBInfo.db, tmpColumnFamilyHandle, tmpRestoreDBInfo.readOptions)) {

						iterator.seek(startKeyGroupPrefixBytes);

						while (iterator.isValid()) {

							if (RocksDBIncrementalCheckpointUtils.beforeThePrefixBytes(iterator.key(), stopKeyGroupPrefixBytes)) {
								writeBatchWrapper.put(targetColumnFamilyHandle, iterator.key(), iterator.value());
							} else {
								// Since the iterator will visit the record according to the sorted order,
								// we can just break here.
								break;
							}

							iterator.next();
						}
					} // releases native iterator resources
				}
				logger.info("Finished restoring from state handle: {} with rescaling.", rawStateHandle);
			} finally {
				cleanUpPathQuietly(temporaryRestoreInstancePath);
			}
		}
	}

	private void initDBWithRescaling(KeyedStateHandle initialHandle) throws Exception {

		assert (initialHandle instanceof IncrementalRemoteKeyedStateHandle);

		// 1. Restore base DB from selected initial handle
		restoreFromRemoteState((IncrementalRemoteKeyedStateHandle) initialHandle);

		// 2. Clip the base DB instance
		try {
			RocksDBIncrementalCheckpointUtils.clipDBWithKeyGroupRange(
				db,
				columnFamilyHandles,
				keyGroupRange,
				initialHandle.getKeyGroupRange(),
				keyGroupPrefixBytes,
				writeBatchSize);
		} catch (RocksDBException e) {
			String errMsg = "Failed to clip DB after initialization.";
			logger.error(errMsg, e);
			throw new BackendBuildingException(errMsg, e);
		}
	}

	/**
	 * Entity to hold the temporary RocksDB instance created for restore.
	 */
	private static class RestoredDBInstance implements AutoCloseable {

		@Nonnull
		private final RocksDB db;

		@Nonnull
		private final ColumnFamilyHandle defaultColumnFamilyHandle;

		@Nonnull
		private final List<ColumnFamilyHandle> columnFamilyHandles;

		@Nonnull
		private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;

		@Nonnull
		private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

		private final ReadOptions readOptions;

		private RestoredDBInstance(
			@Nonnull RocksDB db,
			@Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
			@Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
			@Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
			this.db = db;
			this.defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
			this.columnFamilyHandles = columnFamilyHandles;
			this.columnFamilyDescriptors = columnFamilyDescriptors;
			this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
			this.readOptions = RocksDBOperationUtils.createTotalOrderSeekReadOptions();
		}

		@Override
		public void close() {
			List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(columnFamilyDescriptors.size() + 1);
			columnFamilyDescriptors.forEach((cfd) -> columnFamilyOptions.add(cfd.getOptions()));
			RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(columnFamilyOptions, defaultColumnFamilyHandle);
			IOUtils.closeQuietly(defaultColumnFamilyHandle);
			IOUtils.closeAllQuietly(columnFamilyHandles);
			IOUtils.closeQuietly(db);
			IOUtils.closeAllQuietly(columnFamilyOptions);
			IOUtils.closeQuietly(readOptions);
		}
	}

	private RestoredDBInstance restoreDBInstanceFromStateHandle(
		IncrementalRemoteKeyedStateHandle restoreStateHandle,
		Path temporaryRestoreInstancePath) throws Exception {

		try (RocksDBStateDownloader rocksDBStateDownloader =
				new RocksDBStateDownloader(numberOfTransferringThreads)) {
			rocksDBStateDownloader.transferAllStateDataToDirectory(
				restoreStateHandle,
				temporaryRestoreInstancePath,
				cancelStreamRegistry);
		}

		KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(restoreStateHandle.getMetaStateHandle());
		// read meta data
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, false, writeBufferManagerCapacity);

		List<ColumnFamilyHandle> columnFamilyHandles =
			new ArrayList<>(stateMetaInfoSnapshots.size() + 1);

		RocksDB restoreDb = RocksDBOperationUtils.openDB(
			temporaryRestoreInstancePath.toString(),
			columnFamilyDescriptors,
			columnFamilyHandles,
			RocksDBOperationUtils.createColumnFamilyOptions(columnFamilyOptionsFactory, "default"),
			dbOptions);

		return new RestoredDBInstance(restoreDb, columnFamilyHandles, columnFamilyDescriptors, stateMetaInfoSnapshots);
	}

	/**
	 * This method recreates and registers all {@link ColumnFamilyDescriptor} from Flink's state meta data snapshot.
	 */
	private List<ColumnFamilyDescriptor> createAndRegisterColumnFamilyDescriptors(
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
		boolean registerTtlCompactFilter,
		Long writeBufferManagerCapacity) {

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			new ArrayList<>(stateMetaInfoSnapshots.size());

		for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
			RegisteredStateMetaInfoBase metaInfoBase =
				RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);
			ColumnFamilyDescriptor columnFamilyDescriptor = RocksDBOperationUtils.createColumnFamilyDescriptor(
				metaInfoBase, columnFamilyOptionsFactory, registerTtlCompactFilter ? ttlCompactFiltersManager : null,
				writeBufferManagerCapacity);

			columnFamilyDescriptors.add(columnFamilyDescriptor);
		}
		return columnFamilyDescriptors;
	}

	/**
	 * This recreates the new working directory of the recovered RocksDB instance and links/copies the contents from
	 * a local state.
	 */
	private void restoreInstanceDirectoryFromPath(Path source, String instanceRocksDBPath) throws IOException {
		final Path instanceRocksDBDirectory = Paths.get(instanceRocksDBPath);
		final Path[] files = FileUtils.listDirectory(source);

		for (Path file : files) {
			final String fileName = file.getFileName().toString();
			final Path targetFile = instanceRocksDBDirectory.resolve(fileName);
			if (fileName.endsWith(SST_FILE_SUFFIX)) {
				// hardlink'ing the immutable sst-files.
				Files.createLink(targetFile, file);
			} else {
				// true copy for all other files.
				Files.copy(file, targetFile, StandardCopyOption.REPLACE_EXISTING);
			}
		}
	}

	/**
	 * Reads Flink's state meta data file from the state handle.
	 */
	private KeyedBackendSerializationProxy<K> readMetaData(StreamStateHandle metaStateHandle) throws Exception {

		InputStream inputStream = null;

		try {
			inputStream = metaStateHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(inputStream);
			DataInputView in = new DataInputViewStreamWrapper(inputStream);
			return readMetaData(in);
		} finally {
			if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}
		}
	}
}
