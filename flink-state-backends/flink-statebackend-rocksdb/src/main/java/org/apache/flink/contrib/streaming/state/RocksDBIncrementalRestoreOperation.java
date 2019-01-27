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
import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.ByteArrayOutputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.IncrementalKeyedStateSnapshot;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateSnapshot;
import org.apache.flink.runtime.state.InternalBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.PlaceholderStreamStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfo;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.StateSerializerUtil;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.apache.flink.runtime.state.StateSerializerUtil.GROUP_WRITE_BYTES;

/**
 * Incremental restore operation for RocksDB InternalStateBackend.
 */
public class RocksDBIncrementalRestoreOperation {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBIncrementalRestoreOperation.class);

	private final RocksDBInternalStateBackend stateBackend;

	RocksDBIncrementalRestoreOperation(RocksDBInternalStateBackend stateBackend) {
		this.stateBackend = stateBackend;
	}

	private final CloseableRegistry closeableRegistry = new CloseableRegistry();

	void restore(Collection<KeyedStateHandle> restoredSnapshots) throws Exception {
		boolean hasExtraKeys = (restoredSnapshots.size() > 1 ||
			!Objects.equals(restoredSnapshots.iterator().next().getKeyGroupRange(), stateBackend.getKeyGroupRange()));

		if (hasExtraKeys) {
			long startMillis = System.currentTimeMillis();
			for (KeyedStateHandle rawStateSnapshot: restoredSnapshots) {
				if (!(rawStateSnapshot instanceof IncrementalKeyedStateSnapshot)) {
					throw new IllegalStateException("Unexpected state handle type, " +
						"expected: " + IncrementalKeyedStateSnapshot.class +
						", but found: " + rawStateSnapshot.getClass());
				}
				IncrementalKeyedStateSnapshot stateSnapshot = (IncrementalKeyedStateSnapshot) rawStateSnapshot;
				// temporary path.
				Path temporaryRestoreInstancePath = stateBackend.getLocalRestorePath(stateBackend.getKeyGroupRange());
				restoreFragmentedTabletInstance(stateSnapshot, temporaryRestoreInstancePath);
			}
			long endMills = System.currentTimeMillis();
			LOG.info("Restore Fragmented Tablet using {} ms", endMills - startMillis);
		} else {
			restoreIntegratedTabletInstance(restoredSnapshots.iterator().next());
		}
	}

	private void restoreIntegratedTabletInstance(KeyedStateHandle rawStateSnapshot) throws Exception {

		long startMills = System.currentTimeMillis();
		Map<StateHandleID, Tuple2<String, StreamStateHandle>> sstFiles = null;
		StreamStateHandle metaStateHandle = null;
		long checkpointID = -1;

		Path localDataPath = new Path(stateBackend.getInstanceRocksDBPath().getAbsolutePath());
		FileSystem localFileSystem = localDataPath.getFileSystem();

		try {
			if (rawStateSnapshot instanceof IncrementalKeyedStateSnapshot) {
				LOG.info("Restoring from the remote file system.");

				IncrementalKeyedStateSnapshot restoredStateSnapshot = (IncrementalKeyedStateSnapshot) rawStateSnapshot;

				metaStateHandle = restoredStateSnapshot.getMetaStateHandle();
				Map<StateHandleID, Tuple2<String, StreamStateHandle>> sharedStateHandle = restoredStateSnapshot.getSharedState();

				// download the files into the local data path
				for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> sharedStateHandleEntry : sharedStateHandle.entrySet()) {
					StateHandleID stateHandleID = sharedStateHandleEntry.getKey();
					String fileName = stateHandleID.getKeyString();
					StreamStateHandle stateHandle = sharedStateHandleEntry.getValue().f1;

					restoreFile(localDataPath, fileName, stateHandle);
				}

				for (Map.Entry<StateHandleID, StreamStateHandle> privateStateHandleEntry : restoredStateSnapshot.getPrivateState().entrySet()) {
					String stateName = privateStateHandleEntry.getKey().getKeyString();
					StreamStateHandle stateHandle = privateStateHandleEntry.getValue();
					restoreFile(localDataPath, stateName, stateHandle);
				}

				sstFiles = restoredStateSnapshot.getSharedState();
				checkpointID = restoredStateSnapshot.getCheckpointId();

			} else if (rawStateSnapshot instanceof IncrementalLocalKeyedStateSnapshot) {

				// Recovery from local incremental state.
				IncrementalLocalKeyedStateSnapshot restoredStateSnapshot = (IncrementalLocalKeyedStateSnapshot) rawStateSnapshot;
				LOG.info("Restoring from local recovery path {}.", restoredStateSnapshot.getDirectoryStateHandle().getDirectory());

				sstFiles = new HashMap<>();

				metaStateHandle = restoredStateSnapshot.getMetaStateHandle();

				for (Map.Entry<StateHandleID, String> entry : restoredStateSnapshot.getSharedStateHandleIDs().entrySet()) {
					StateHandleID stateHandleID = entry.getKey();
					String uniqueId = entry.getValue();

					sstFiles.put(stateHandleID, Tuple2.of(uniqueId, new PlaceholderStreamStateHandle()));
				}

				Path localRecoverDirectory = restoredStateSnapshot.getDirectoryStateHandle().getDirectory();
				FileStatus[] fileStatuses = localFileSystem.listStatus(localRecoverDirectory);

				if (!localFileSystem.mkdirs(localDataPath)) {
					throw new IOException("Cannot create local base path for RocksDB.");
				}

				if (fileStatuses == null) {
					throw new IOException("Cannot list file statues. Local recovery directory " + localRecoverDirectory + " does not exist.");
				}
				for (FileStatus fileStatus : fileStatuses) {
					String fileName = fileStatus.getPath().getName();

					File restoreFile = new File(localRecoverDirectory.getPath(), fileName);
					File targetFile = new File(localDataPath.getPath(), fileName);
					Files.createLink(targetFile.toPath(), restoreFile.toPath());
				}

				checkpointID = restoredStateSnapshot.getCheckpointId();
			}
		} catch (Exception e) {
			LOG.info("Fail to restore rocksDB instance at {}, and try to remove it if existed.", localDataPath);
			try {
				if (localFileSystem.exists(localDataPath)) {
					localFileSystem.delete(localDataPath, true);
				}
			} catch (IOException e1) {
				LOG.warn("Fail to remove local data path {} after restore operation failure.", localDataPath);
			}
			throw e;
		}

		// restore the state descriptors
		restoreMetaData(metaStateHandle);

		int cfLength = 1 + stateBackend.getRegisteredStateMetaInfos().size();
		List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(cfLength);
		List<String> descriptorNames = new ArrayList<>(cfLength);
		columnFamilyDescriptors.add(stateBackend.getDefaultColumnFamilyDescriptor());
		descriptorNames.add(stateBackend.getDefaultColumnFamilyName());

		for (Map.Entry<String, RegisteredStateMetaInfo> stateMetaInfoEntry : stateBackend.getRegisteredStateMetaInfos().entrySet()) {
			String stateName = stateMetaInfoEntry.getKey();
			columnFamilyDescriptors.add(stateBackend.createColumnFamilyDescriptor(stateName));
			descriptorNames.add(stateName);
		}

		stateBackend.createDBWithColumnFamily(columnFamilyDescriptors, descriptorNames);

		stateBackend.registerAllStates();

		synchronized (stateBackend.materializedSstFiles) {
			stateBackend.materializedSstFiles.put(checkpointID, sstFiles);
		}
		stateBackend.lastCompletedCheckpointId = checkpointID;

		long endMills = System.currentTimeMillis();
		LOG.info("Restore Integrated Tablet using {} ms.", endMills - startMills);
	}

	private void restoreFile(Path localRestorePath, String fileName, StreamStateHandle restoreStateHandle) throws IOException {
		Path localFilePath = new Path(localRestorePath, fileName);
		FileSystem localFileSystem = localFilePath.getFileSystem();

		FSDataInputStream inputStream = null;
		FSDataOutputStream outputStream = null;

		try {
			long startMillis = System.currentTimeMillis();

			inputStream = restoreStateHandle.openInputStream();
			closeableRegistry.registerCloseable(inputStream);

			outputStream = localFileSystem.create(localFilePath, FileSystem.WriteMode.OVERWRITE);
			closeableRegistry.registerCloseable(outputStream);

			byte[] buffer = new byte[64 * 1024];
			while (true) {
				int numBytes = inputStream.read(buffer);

				if (numBytes == -1) {
					break;
				}

				outputStream.write(buffer, 0, numBytes);
			}

			long endMillis = System.currentTimeMillis();
			LOG.debug("Successfully restored file {} from {}, {} bytes, {} ms",
				localFilePath, restoreStateHandle, restoreStateHandle.getStateSize(),
				(endMillis - startMillis));

			outputStream.close();
			closeableRegistry.unregisterCloseable(outputStream);
			outputStream = null;

			inputStream.close();
			closeableRegistry.unregisterCloseable(inputStream);
			inputStream = null;
		} finally {
			if (inputStream != null) {
				inputStream.close();
				closeableRegistry.unregisterCloseable(inputStream);
			}

			if (outputStream != null) {
				outputStream.close();
				closeableRegistry.unregisterCloseable(outputStream);
			}
		}
	}

	private void restoreMetaData(StreamStateHandle metaStateDatum) throws Exception {

		FSDataInputStream inputStream = null;

		try {
			inputStream = metaStateDatum.openInputStream();
			closeableRegistry.registerCloseable(inputStream);

			DataInputViewStreamWrapper inputView = new DataInputViewStreamWrapper(inputStream);

			// isSerializerPresenceRequired flag is set to false, since for the RocksDB state backend,
			// deserialization of state happens lazily during runtime; we depend on the fact
			// that the new serializer for states could be compatible, and therefore the restore can continue
			// without old serializers required to be present.
			InternalBackendSerializationProxy serializationProxy =
				new InternalBackendSerializationProxy(stateBackend.getUserClassLoader(), false);
			serializationProxy.read(inputView);

			List<StateMetaInfoSnapshot> keyedStateMetaInfos = serializationProxy.getKeyedStateMetaSnapshots();
			for (StateMetaInfoSnapshot keyedStateMetaSnapshot : keyedStateMetaInfos) {
				String stateName = keyedStateMetaSnapshot.getName();
				stateBackend.getRestoredKvStateMetaInfos().put(stateName, keyedStateMetaSnapshot);

				RegisteredStateMetaInfo keyedStateMetaInfo = RegisteredStateMetaInfo.createKeyedStateMetaInfo(keyedStateMetaSnapshot);
				stateBackend.getRegisteredStateMetaInfos().put(stateName, keyedStateMetaInfo);
			}

			List<StateMetaInfoSnapshot> subKeyedStateMetaInfos = serializationProxy.getSubKeyedStateMetaSnapshots();
			for (StateMetaInfoSnapshot subKeyedStateMetaSnapshot : subKeyedStateMetaInfos) {
				String stateName = subKeyedStateMetaSnapshot.getName();
				stateBackend.getRestoredKvStateMetaInfos().put(subKeyedStateMetaSnapshot.getName(), subKeyedStateMetaSnapshot);

				RegisteredStateMetaInfo subKeyedStateMetaInfo = RegisteredStateMetaInfo.createSubKeyedStateMetaInfo(subKeyedStateMetaSnapshot);
				stateBackend.getRegisteredStateMetaInfos().put(stateName, subKeyedStateMetaInfo);
			}

			inputStream.close();
			closeableRegistry.unregisterCloseable(inputStream);
			inputStream = null;
		} finally {
			if (inputStream != null) {
				inputStream.close();
				closeableRegistry.unregisterCloseable(inputStream);
			}
		}
	}

	private void restoreFragmentedTabletInstance(
		IncrementalKeyedStateSnapshot stateSnapshot,
		Path localRestorePath
	) throws Exception {
		FileSystem localFileSystem = localRestorePath.getFileSystem();
		if (localFileSystem.exists(localRestorePath)) {
			localFileSystem.delete(localRestorePath, true);
		}
		localFileSystem.mkdirs(localRestorePath);

		try {
			transferAllStateDataToDirectory(stateSnapshot, localRestorePath);
			restoreMetaData(stateSnapshot.getMetaStateHandle());

			int cfSize = 1 + stateBackend.getRegisteredStateMetaInfos().size();
			List<String> cfName = new ArrayList<>(cfSize);
			List<ColumnFamilyDescriptor> columnFamilyDescriptors = new ArrayList<>(cfSize);
			columnFamilyDescriptors.add(stateBackend.getDefaultColumnFamilyDescriptor());
			cfName.add(stateBackend.getDefaultColumnFamilyName());
			for (Map.Entry<String, RegisteredStateMetaInfo> stateMetaInfoEntry : stateBackend.getRegisteredStateMetaInfos().entrySet()) {
				String stateName = stateMetaInfoEntry.getKey();
				columnFamilyDescriptors.add(stateBackend.createColumnFamilyDescriptor(stateName));
				cfName.add(stateName);
			}

			List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>(cfSize);

			try (RocksDB db = RocksDB.open(localRestorePath.getPath(), columnFamilyDescriptors, columnFamilyHandles);
				RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(stateBackend.getDbInstance())) {
				final ColumnFamilyHandle defaultColumnFamily = columnFamilyHandles.get(0);
				Preconditions.checkState(columnFamilyHandles.size() == columnFamilyDescriptors.size());
				try {
					ByteArrayOutputStreamWithPos outputStream = new ByteArrayOutputStreamWithPos(GROUP_WRITE_BYTES);

					int startGroup = stateBackend.getKeyGroupRange().getIntersection(stateSnapshot.getKeyGroupRange()).getStartKeyGroup();
					StateSerializerUtil.writeGroup(outputStream, startGroup);
					byte[] startGroupBytes = outputStream.toByteArray();

					for (int i = 1; i < columnFamilyDescriptors.size(); ++i) {
						ColumnFamilyHandle columnFamilyHandle = columnFamilyHandles.get(i);
						ColumnFamilyHandle targetFamilyHandle = stateBackend.getOrCreateColumnFamily(cfName.get(i));
						try (RocksIterator iterator = db.newIterator(columnFamilyHandle)) {
							iterator.seek(startGroupBytes);

							while (iterator.isValid()) {
								int keyGroup = StateSerializerUtil.getGroupFromSerializedKey(iterator.key());

								if (stateBackend.getKeyGroupRange().contains(keyGroup)) {
									writeBatchWrapper.put(targetFamilyHandle, iterator.key(), iterator.value());
								} else {
									break;
								}

								iterator.next();
							}
						}
					}
				} finally {
					//release native tmp db column family resources
					IOUtils.closeQuietly(defaultColumnFamily);

					for (ColumnFamilyHandle flinkColumnFamilyHandle : columnFamilyHandles) {
						IOUtils.closeQuietly(flinkColumnFamilyHandle);
					}
				}
			}
		} catch (Exception e) {
			if (localFileSystem.exists(localRestorePath)) {
				try {
					localFileSystem.delete(localRestorePath, true);
				} catch (IOException e1) {
					LOG.warn("Delete local path failed.", e);
				}
			}
			throw e;
		}
	}

	private void transferAllStateDataToDirectory(
		IncrementalKeyedStateSnapshot stateSnapshot,
		Path localRestorePath) throws IOException {
		Map<StateHandleID, Tuple2<String, StreamStateHandle>> sharedState = stateSnapshot.getSharedState();
		for (Map.Entry<StateHandleID, Tuple2<String, StreamStateHandle>> stateHandleEntry : sharedState.entrySet()) {
			String stateName = stateHandleEntry.getKey().getKeyString();
			StreamStateHandle stateHandle = stateHandleEntry.getValue().f1;
			restoreFile(localRestorePath, stateName, stateHandle);
		}

		Map<StateHandleID, StreamStateHandle> privateState = stateSnapshot.getPrivateState();
		for (Map.Entry<StateHandleID, StreamStateHandle> privateFileEntry : privateState.entrySet()) {
			String privateFileName = privateFileEntry.getKey().getKeyString();
			StreamStateHandle privateFileStateHandle = privateFileEntry.getValue();
			restoreFile(localRestorePath, privateFileName, privateFileStateHandle);
		}
	}
}
