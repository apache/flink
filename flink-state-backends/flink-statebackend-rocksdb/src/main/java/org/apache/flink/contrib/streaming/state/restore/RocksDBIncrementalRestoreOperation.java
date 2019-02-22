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

import org.apache.flink.configuration.ConfigConstants;
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
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.ttl.TtlTimeProvider;
import org.apache.flink.util.IOUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
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

/**
 * Encapsulates the process of restoring a RocksDB instance from an incremental snapshot.
 */
public class RocksDBIncrementalRestoreOperation<K> extends AbstractRocksDBRestoreOperation<K> {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBIncrementalRestoreOperation.class);

	private final String operatorIdentifier;
	protected final SortedMap<Long, Set<StateHandleID>> restoredSstFiles;
	protected long lastCompletedCheckpointId;
	protected UUID backendUID;

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
		TtlTimeProvider ttlTimeProvider) {
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
			ttlTimeProvider);
		this.operatorIdentifier = operatorIdentifier;
		this.restoredSstFiles = new TreeMap<>();
		this.lastCompletedCheckpointId = -1L;
		this.backendUID = UUID.randomUUID();
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
	void restoreWithoutRescaling(KeyedStateHandle rawStateHandle) throws Exception {
		// 1. Prepare for restore without rescaling
		Path temporaryRestoreInstancePath = new Path(
			instanceBasePath.getAbsolutePath(),
			UUID.randomUUID().toString()); // used as restore source for IncrementalKeyedStateHandle
		RocksDBIncrementalRestorePrepareResult prepareResult = prepareFiles(rawStateHandle, temporaryRestoreInstancePath);
		Path restoreSourcePath = prepareResult.getLocalKeyedStateHandle().getDirectoryStateHandle().getDirectory();
		if (rawStateHandle instanceof IncrementalKeyedStateHandle) {
			backendUID = ((IncrementalKeyedStateHandle) rawStateHandle).getBackendIdentifier();
		} else {
			backendUID = ((IncrementalLocalKeyedStateHandle) rawStateHandle).getBackendIdentifier();
		}
		LOG.debug("Restoring keyed backend uid in operator {} from incremental snapshot to {}.",
			this.operatorIdentifier, this.backendUID);
		if (!instanceRocksDBPath.mkdirs()) {
			String errMsg = "Could not create RocksDB data directory: " + instanceBasePath.getAbsolutePath();
			LOG.error(errMsg);
			throw new IOException(errMsg);
		}
		try {
			restoreInstanceDirectoryFromPath(restoreSourcePath, dbPath);
		} finally {
			cleanUpPathQuietly(restoreSourcePath);
		}
		// 2. Open db instance
		openDB();
		// 3. Use the restore sst files as the base for succeeding checkpoints
		IncrementalLocalKeyedStateHandle restoreStateHandle = prepareResult.getLocalKeyedStateHandle();
		restoredSstFiles.put(
			restoreStateHandle.getCheckpointId(),
			restoreStateHandle.getSharedStateHandleIDs());
		lastCompletedCheckpointId = restoreStateHandle.getCheckpointId();
		// 4. Register column family handles
		registerCFHandles(prepareResult.getStateMetaInfoSnapshots());
	}

	private void cleanUpPathQuietly(Path path) {
		try {
			FileSystem fileSystem = path.getFileSystem();
			if (fileSystem.exists(path)) {
				fileSystem.delete(path, true);
			}
		} catch (IOException ex) {
			LOG.warn("Failed to clean up path " + path, ex);
		}
	}

	private void registerCFHandles(List<StateMetaInfoSnapshot> metaInfoSnapshots)
		throws BackendBuildingException {
		// Register CF handlers
		for (int i = 0; i < metaInfoSnapshots.size(); ++i) {
			try {
				getOrRegisterStateColumnFamilyHandle(
					columnFamilyHandles.get(i),
					metaInfoSnapshots.get(i));
			} catch (RocksDBException e) {
				String errMsg = "Failed to register CF handle.";
				LOG.error(errMsg, e);
				throw new BackendBuildingException(errMsg, e);
			}
		}
	}

	public RocksDBIncrementalRestorePrepareResult prepareFiles(
		KeyedStateHandle rawStateHandle,
		Path restoreInstancePath) throws BackendBuildingException {
		IncrementalLocalKeyedStateHandle localKeyedStateHandle;
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;
		try {
			KeyedBackendSerializationProxy<K> serializationProxy;
			if (rawStateHandle instanceof IncrementalKeyedStateHandle) {
				IncrementalKeyedStateHandle restoreStateHandle = (IncrementalKeyedStateHandle) rawStateHandle;

				// read state data.
				try (RocksDBStateDownloader rocksDBStateDownloader =
						new RocksDBStateDownloader(numberOfTransferringThreads)) {
					rocksDBStateDownloader.transferAllStateDataToDirectory(
						restoreStateHandle,
						restoreInstancePath,
						cancelStreamRegistry);
				}

				serializationProxy = readMetaData(restoreStateHandle.getMetaStateHandle());
				stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();
				columnFamilyDescriptors = createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, true);

				// since we transferred all remote state to a local directory, we can use the same code as for
				// local recovery.
				localKeyedStateHandle = new IncrementalLocalKeyedStateHandle(
					restoreStateHandle.getBackendIdentifier(),
					restoreStateHandle.getCheckpointId(),
					new DirectoryStateHandle(restoreInstancePath),
					restoreStateHandle.getKeyGroupRange(),
					restoreStateHandle.getMetaStateHandle(),
					restoreStateHandle.getSharedState().keySet());
			} else if (rawStateHandle instanceof IncrementalLocalKeyedStateHandle) {
				// Recovery from local incremental state.
				localKeyedStateHandle = (IncrementalLocalKeyedStateHandle) rawStateHandle;
				serializationProxy = readMetaData(localKeyedStateHandle.getMetaDataState());
				stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();
				columnFamilyDescriptors = createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, true);
			} else {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected " + IncrementalKeyedStateHandle.class + " or " + IncrementalLocalKeyedStateHandle.class +
					", but found " + rawStateHandle.getClass());
			}
		} catch (Exception e) {
			// clean up
			cleanUpPathQuietly(restoreInstancePath);
			// log and rethrow
			String errMsg = "Failed to prepare files for restore from incremental state handle.";
			LOG.error(errMsg, e);
			throw new BackendBuildingException(errMsg, e);
		}
		columnFamilyHandles = new ArrayList<>(columnFamilyDescriptors.size() + 1);
		return new RocksDBIncrementalRestorePrepareResult(stateMetaInfoSnapshots, localKeyedStateHandle);
	}

	/**
	 * Recovery from multi incremental states with rescaling. For rescaling, this method creates a temporary
	 * RocksDB instance for a key-groups shard. All contents from the temporary instance are copied into the
	 * real restore instance and then the temporary instance is discarded.
	 */
	void restoreWithRescaling(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {
		// Init DB instance
		initDBWithRescaling(restoreStateHandles);
		// Do restore
		byte[] startKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getStartKeyGroup(), startKeyGroupPrefixBytes);

		byte[] stopKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getEndKeyGroup() + 1, stopKeyGroupPrefixBytes);

		for (KeyedStateHandle rawStateHandle : restoreStateHandles) {

			if (!(rawStateHandle instanceof IncrementalKeyedStateHandle)) {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected " + IncrementalKeyedStateHandle.class +
					", but found " + rawStateHandle.getClass());
			}

			Path temporaryRestoreInstancePath = new Path(instanceBasePath.getAbsolutePath() + UUID.randomUUID().toString());
			try (RestoredDBInstance tmpRestoreDBInfo = restoreDBInstanceFromStateHandle(
				(IncrementalKeyedStateHandle) rawStateHandle,
				temporaryRestoreInstancePath);
				RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(this.db)) {

				List<ColumnFamilyDescriptor> tmpColumnFamilyDescriptors = tmpRestoreDBInfo.columnFamilyDescriptors;
				List<ColumnFamilyHandle> tmpColumnFamilyHandles = tmpRestoreDBInfo.columnFamilyHandles;

				// iterating only the requested descriptors automatically skips the default column family handle
				for (int i = 0; i < tmpColumnFamilyDescriptors.size(); ++i) {
					ColumnFamilyHandle tmpColumnFamilyHandle = tmpColumnFamilyHandles.get(i);

					ColumnFamilyHandle targetColumnFamilyHandle = getOrRegisterStateColumnFamilyHandle(
						null, tmpRestoreDBInfo.stateMetaInfoSnapshots.get(i))
						.columnFamilyHandle;

					try (RocksIteratorWrapper iterator = RocksDBOperationUtils.getRocksIterator(tmpRestoreDBInfo.db, tmpColumnFamilyHandle)) {

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
			} finally {
				cleanUpPathQuietly(temporaryRestoreInstancePath);
			}
		}
	}

	private void initDBWithRescaling(Collection<KeyedStateHandle> restoreStateHandles) throws IOException {
		// Prepare for restore with rescaling
		IncrementalKeyedStateHandle initialHandle =
			(IncrementalKeyedStateHandle) RocksDBIncrementalCheckpointUtils.chooseTheBestStateHandleForInitial(
				restoreStateHandles, keyGroupRange);
		Path instancePath = new Path(dbPath);
		if (initialHandle != null) {
			restoreStateHandles.remove(initialHandle);
			// 1. Download target files
			RocksDBIncrementalRestorePrepareResult prepareResult = prepareFiles(initialHandle, instancePath);
			// 2. Open db instance
			openDB();
			// 3. Clip the DB instance
			try {
				RocksDBIncrementalCheckpointUtils.clipDBWithKeyGroupRange(
					db,
					columnFamilyHandles,
					keyGroupRange,
					initialHandle.getKeyGroupRange(),
					keyGroupPrefixBytes);
			} catch (RocksDBException e) {
				String errMsg = "Failed to clip DB after initialization.";
				LOG.error(errMsg, e);
				throw new BackendBuildingException(errMsg, e);
			}
			// 4. Register column family handles
			registerCFHandles(prepareResult.getStateMetaInfoSnapshots());
		} else {
			openDB();
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
		}
	}

	private RestoredDBInstance restoreDBInstanceFromStateHandle(
		IncrementalKeyedStateHandle restoreStateHandle,
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
			createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, false);

		List<ColumnFamilyHandle> columnFamilyHandles =
			new ArrayList<>(stateMetaInfoSnapshots.size() + 1);

		RocksDB restoreDb = RocksDBOperationUtils.openDB(
			temporaryRestoreInstancePath.getPath(),
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
		boolean registerTtlCompactFilter) {

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			new ArrayList<>(stateMetaInfoSnapshots.size());

		for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
			ColumnFamilyOptions options = RocksDBOperationUtils.createColumnFamilyOptions(
				columnFamilyOptionsFactory, stateMetaInfoSnapshot.getName());
			if (registerTtlCompactFilter) {
				ttlCompactFiltersManager.setAndRegisterCompactFilterIfStateTtl(ttlTimeProvider,
					stateMetaInfoSnapshot, options);
			}
			ColumnFamilyDescriptor columnFamilyDescriptor = new ColumnFamilyDescriptor(
				stateMetaInfoSnapshot.getName().getBytes(ConfigConstants.DEFAULT_CHARSET),
				options);

			columnFamilyDescriptors.add(columnFamilyDescriptor);
		}
		return columnFamilyDescriptors;
	}

	/**
	 * This recreates the new working directory of the recovered RocksDB instance and links/copies the contents from
	 * a local state.
	 */
	public void restoreInstanceDirectoryFromPath(Path source, String instanceRocksDBPath) throws IOException {

		FileSystem fileSystem = source.getFileSystem();

		final FileStatus[] fileStatuses = fileSystem.listStatus(source);

		if (fileStatuses == null) {
			throw new IOException("Cannot list file statues. Directory " + source + " does not exist.");
		}

		for (FileStatus fileStatus : fileStatuses) {
			final Path filePath = fileStatus.getPath();
			final String fileName = filePath.getName();
			File restoreFile = new File(source.getPath(), fileName);
			File targetFile = new File(instanceRocksDBPath, fileName);
			if (fileName.endsWith(SST_FILE_SUFFIX)) {
				// hardlink'ing the immutable sst-files.
				Files.createLink(targetFile.toPath(), restoreFile.toPath());
			} else {
				// true copy for all other files.
				Files.copy(restoreFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			}
		}
	}

	/**
	 * Reads Flink's state meta data file from the state handle.
	 */
	private KeyedBackendSerializationProxy<K> readMetaData(StreamStateHandle metaStateHandle) throws Exception {

		FSDataInputStream inputStream = null;

		try {
			inputStream = metaStateHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(inputStream);
			DataInputView in = new DataInputViewStreamWrapper(inputStream);
			KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(in);
			return serializationProxy;
		} finally {
			if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}
		}
	}
}
