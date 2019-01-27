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

import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.runtime.state.InternalBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyGroupsStateSnapshot;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfo;
import org.apache.flink.runtime.state.SnappyStreamCompressionDecorator;
import org.apache.flink.runtime.state.StateAccessException;
import org.apache.flink.runtime.state.StateMetaInfoSnapshot;
import org.apache.flink.runtime.state.StreamCompressionDecorator;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.UncompressedStreamCompressionDecorator;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Full restore operation for RocksDB InternalStateBackend.
 */
public class RocksDBFullRestoreOperation {

	private static final Logger LOG = LoggerFactory.getLogger(RocksDBFullRestoreOperation.class);

	/** The state backend who wants to restore the snapshot.*/
	private final RocksDBInternalStateBackend stateBackend;

	private final Map<Integer, String> id2StateName = new HashMap<>();

	/** The compression decorator that was used for writing the state, as determined by the meta data. */
	private StreamCompressionDecorator keyGroupStreamCompressionDecorator;

	RocksDBFullRestoreOperation(RocksDBInternalStateBackend stateBackend) {
		this.stateBackend = stateBackend;
	}

	public void restore(
		Collection<KeyedStateHandle> restoredSnapshots
	) throws Exception {
		if (restoredSnapshots == null || restoredSnapshots.isEmpty()) {
			return;
		}

		long startMills = System.currentTimeMillis();

		stateBackend.createDB();
		for (KeyedStateHandle rawSnapshot : restoredSnapshots) {
			if (rawSnapshot == null) {
				continue;
			}
			Preconditions.checkState(rawSnapshot instanceof KeyGroupsStateSnapshot);
			KeyGroupsStateSnapshot snapshot =
				(KeyGroupsStateSnapshot) rawSnapshot;

			StreamStateHandle snapshotHandle = snapshot.getSnapshotHandle();
			if (snapshotHandle == null) {
				continue;
			}

			FSDataInputStream inputStream = snapshotHandle.openInputStream();
			try {
				DataInputViewStreamWrapper inputView =
					new DataInputViewStreamWrapper(inputStream);

				// isSerializerPresenceRequired flag is set to false, since for the RocksDB state backend,
				// deserialization of state happens lazily during runtime; we depend on the fact
				// that the new serializer for states could be compatible, and therefore the restore can continue
				// without old serializers required to be present.
				InternalBackendSerializationProxy serializationProxy =
					new InternalBackendSerializationProxy(stateBackend.getUserClassLoader(), false);
				serializationProxy.read(inputView);

				this.keyGroupStreamCompressionDecorator = serializationProxy.isUsingKeyGroupCompression() ?
					SnappyStreamCompressionDecorator.INSTANCE : UncompressedStreamCompressionDecorator.INSTANCE;

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
					stateBackend.getRestoredKvStateMetaInfos().put(stateName, subKeyedStateMetaSnapshot);

					RegisteredStateMetaInfo subKeyedStateMetaInfo = RegisteredStateMetaInfo.createSubKeyedStateMetaInfo(subKeyedStateMetaSnapshot);
					stateBackend.getRegisteredStateMetaInfos().put(stateName, subKeyedStateMetaInfo);
				}

				int numStates = inputView.readInt();
				for (int i = 0; i < numStates; ++i) {
					String stateName = InstantiationUtil.deserializeObject(
						inputStream, stateBackend.getUserClassLoader());
					Integer id = InstantiationUtil.deserializeObject(
						inputStream, stateBackend.getUserClassLoader());
					id2StateName.put(id, stateName);
				}

				stateBackend.registerAllStates();
				Map<Integer, Tuple2<Long, Integer>> metaInfos = snapshot.getMetaInfos();
				restoreData(metaInfos, inputStream);
			} finally {
				if (inputStream != null) {
					try {
						inputStream.close();
					} catch (Exception e) {
						LOG.warn("Could not properly close the input stream.", e);
					}
				}
			}
		}

		long endMills = System.currentTimeMillis();
		LOG.info("Full Restored with RocksDB state backend using {} ms.", endMills - startMills);
	}

	//--------------------------------------------------------------------------

	/**
	 * A helper method to restore the data from the snapshot.
	 *
	 * @param metaInfos The offsets and the number of entries of the groups
	 *                  in the snapshot.
	 * @param inputStream The input stream where the snapshot is read.
	 * @throws IOException Thrown when the backend fails to read the snapshot or
	 *                     to deserialize the state data from the snapshot.
	 */
	private void restoreData(
		Map<Integer, Tuple2<Long, Integer>> metaInfos,
		FSDataInputStream inputStream) throws IOException {
		for (int group : stateBackend.getKeyGroupRange()) {
			Tuple2<Long, Integer> metaInfo = metaInfos.get(group);
			if (metaInfo == null) {
				continue;
			}

			long offset = metaInfo.f0;
			int numEntries = metaInfo.f1;

			inputStream.seek(offset);

			if (numEntries != 0) {
				try (InputStream compressedKgIn = keyGroupStreamCompressionDecorator.decorateWithCompression(inputStream)) {
					DataInputViewStreamWrapper compressedKgInputView = new DataInputViewStreamWrapper(compressedKgIn);
					for (int i = 0; i < numEntries; ++i) {
						Integer id = IntSerializer.INSTANCE.deserialize(compressedKgInputView);
						String cfNameStr = id2StateName.get(id);
						Preconditions.checkNotNull(cfNameStr, "Unexpected state name for the id: " + id);
						ColumnFamilyHandle columnFamilyHandle = stateBackend.getOrCreateColumnFamily(cfNameStr);
						byte[] key = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
						byte[] value = BytePrimitiveArraySerializer.INSTANCE.deserialize(compressedKgInputView);
						try {
							stateBackend.getDbInstance().put(columnFamilyHandle, key, value);
						} catch (RocksDBException e) {
							throw new StateAccessException(e);
						}
					}
				}
			}
		}
	}
}
