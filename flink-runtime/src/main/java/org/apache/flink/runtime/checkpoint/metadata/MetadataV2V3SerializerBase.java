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

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.OperatorStreamStateHandle;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Base (De)serializer for checkpoint metadata format version 2 and 3.
 *
 * <p>The difference between versions 2 and 3 is minor. Version 3 includes
 * operator coordinator state for each operator, and drops some minor unused fields.
 *
 * <p>Basic checkpoint metadata layout:
 * <pre>
 *  +--------------+---------------+-----------------+
 *  | checkpointID | master states | operator states |
 *  +--------------+---------------+-----------------+
 *
 *  Master state:
 *  +--------------+---------------------+---------+------+---------------+
 *  | magic number | num remaining bytes | version | name | payload bytes |
 *  +--------------+---------------------+---------+------+---------------+
 * </pre>
 */
@Internal
public abstract class MetadataV2V3SerializerBase {

	/** Random magic number for consistency checks. */
	private static final int MASTER_STATE_MAGIC_NUMBER = 0xc96b1696;

	private static final byte NULL_HANDLE = 0;
	private static final byte BYTE_STREAM_STATE_HANDLE = 1;
	private static final byte FILE_STREAM_STATE_HANDLE = 2;
	private static final byte KEY_GROUPS_HANDLE = 3;
	private static final byte PARTITIONABLE_OPERATOR_STATE_HANDLE = 4;
	private static final byte INCREMENTAL_KEY_GROUPS_HANDLE = 5;

	// ------------------------------------------------------------------------
	//  (De)serialization entry points
	// ------------------------------------------------------------------------

	protected void serializeMetadata(CheckpointMetadata checkpointMetadata, DataOutputStream dos) throws IOException {
		// first: checkpoint ID
		dos.writeLong(checkpointMetadata.getCheckpointId());

		// second: master state
		final Collection<MasterState> masterStates = checkpointMetadata.getMasterStates();
		dos.writeInt(masterStates.size());
		for (MasterState ms : masterStates) {
			serializeMasterState(ms, dos);
		}

		// third: operator states
		Collection<OperatorState> operatorStates = checkpointMetadata.getOperatorStates();
		dos.writeInt(operatorStates.size());

		for (OperatorState operatorState : operatorStates) {
			serializeOperatorState(operatorState, dos);
		}
	}

	protected CheckpointMetadata deserializeMetadata(DataInputStream dis) throws IOException {
		// first: checkpoint ID
		final long checkpointId = dis.readLong();
		if (checkpointId < 0) {
			throw new IOException("invalid checkpoint ID: " + checkpointId);
		}

		// second: master state
		final List<MasterState> masterStates;
		final int numMasterStates = dis.readInt();

		if (numMasterStates == 0) {
			masterStates = Collections.emptyList();
		}
		else if (numMasterStates > 0) {
			masterStates = new ArrayList<>(numMasterStates);
			for (int i = 0; i < numMasterStates; i++) {
				masterStates.add(deserializeMasterState(dis));
			}
		}
		else {
			throw new IOException("invalid number of master states: " + numMasterStates);
		}

		// third: operator states
		final int numTaskStates = dis.readInt();
		final List<OperatorState> operatorStates = new ArrayList<>(numTaskStates);

		for (int i = 0; i < numTaskStates; i++) {
			operatorStates.add(deserializeOperatorState(dis));
		}

		return new CheckpointMetadata(checkpointId, operatorStates, masterStates);
	}

	// ------------------------------------------------------------------------
	//  master state (de)serialization methods
	// ------------------------------------------------------------------------

	protected void serializeMasterState(MasterState state, DataOutputStream dos) throws IOException {
		// magic number for error detection
		dos.writeInt(MASTER_STATE_MAGIC_NUMBER);

		// for safety, we serialize first into an array and then write the array and its
		// length into the checkpoint
		final ByteArrayOutputStream baos = new ByteArrayOutputStream();
		final DataOutputStream out = new DataOutputStream(baos);

		out.writeInt(state.version());
		out.writeUTF(state.name());

		final byte[] bytes = state.bytes();
		out.writeInt(bytes.length);
		out.write(bytes, 0, bytes.length);

		out.close();
		byte[] data = baos.toByteArray();

		dos.writeInt(data.length);
		dos.write(data, 0, data.length);
	}

	protected MasterState deserializeMasterState(DataInputStream dis) throws IOException {
		final int magicNumber = dis.readInt();
		if (magicNumber != MASTER_STATE_MAGIC_NUMBER) {
			throw new IOException("incorrect magic number in master styte byte sequence");
		}

		final int numBytes = dis.readInt();
		if (numBytes <= 0) {
			throw new IOException("found zero or negative length for master state bytes");
		}

		final byte[] data = new byte[numBytes];
		dis.readFully(data);

		final DataInputStream in = new DataInputStream(new ByteArrayInputStream(data));

		final int version = in.readInt();
		final String name = in.readUTF();

		final byte[] bytes = new byte[in.readInt()];
		in.readFully(bytes);

		// check that the data is not corrupt
		if (in.read() != -1) {
			throw new IOException("found trailing bytes in master state");
		}

		return new MasterState(name, bytes, version);
	}

	// ------------------------------------------------------------------------
	//  operator state (de)serialization methods
	// ------------------------------------------------------------------------

	protected abstract void serializeOperatorState(OperatorState operatorState, DataOutputStream dos) throws IOException;

	protected abstract OperatorState deserializeOperatorState(DataInputStream dis) throws IOException;

	// ------------------------------------------------------------------------
	//  operator subtask state (de)serialization methods
	// ------------------------------------------------------------------------

	protected void serializeSubtaskState(OperatorSubtaskState subtaskState, DataOutputStream dos) throws IOException {
		final OperatorStateHandle managedOperatorState = extractSingleton(subtaskState.getManagedOperatorState());
		if (managedOperatorState != null) {
			dos.writeInt(1);
			serializeOperatorStateHandle(managedOperatorState, dos);
		} else {
			dos.writeInt(0);
		}

		final OperatorStateHandle rawOperatorState = extractSingleton(subtaskState.getRawOperatorState());
		if (rawOperatorState != null) {
			dos.writeInt(1);
			serializeOperatorStateHandle(rawOperatorState, dos);
		} else {
			dos.writeInt(0);
		}

		final KeyedStateHandle keyedStateBackend = extractSingleton(subtaskState.getManagedKeyedState());
		serializeKeyedStateHandle(keyedStateBackend, dos);

		final KeyedStateHandle keyedStateStream = extractSingleton(subtaskState.getRawKeyedState());
		serializeKeyedStateHandle(keyedStateStream, dos);
	}

	protected OperatorSubtaskState deserializeSubtaskState(DataInputStream dis) throws IOException {
		final int hasManagedOperatorState = dis.readInt();
		final OperatorStateHandle managedOperatorState = hasManagedOperatorState == 0 ?
				null : deserializeOperatorStateHandle(dis);

		final int hasRawOperatorState = dis.readInt();
		final OperatorStateHandle rawOperatorState = hasRawOperatorState == 0 ?
				null : deserializeOperatorStateHandle(dis);

		final KeyedStateHandle managedKeyedState = deserializeKeyedStateHandle(dis);
		final KeyedStateHandle rawKeyedState = deserializeKeyedStateHandle(dis);

		return new OperatorSubtaskState(
				managedOperatorState,
				rawOperatorState,
				managedKeyedState,
				rawKeyedState);
	}

	@VisibleForTesting
	public static void serializeKeyedStateHandle(
			KeyedStateHandle stateHandle, DataOutputStream dos) throws IOException {

		if (stateHandle == null) {
			dos.writeByte(NULL_HANDLE);
		} else if (stateHandle instanceof KeyGroupsStateHandle) {
			KeyGroupsStateHandle keyGroupsStateHandle = (KeyGroupsStateHandle) stateHandle;

			dos.writeByte(KEY_GROUPS_HANDLE);
			dos.writeInt(keyGroupsStateHandle.getKeyGroupRange().getStartKeyGroup());
			dos.writeInt(keyGroupsStateHandle.getKeyGroupRange().getNumberOfKeyGroups());
			for (int keyGroup : keyGroupsStateHandle.getKeyGroupRange()) {
				dos.writeLong(keyGroupsStateHandle.getOffsetForKeyGroup(keyGroup));
			}
			serializeStreamStateHandle(keyGroupsStateHandle.getDelegateStateHandle(), dos);
		} else if (stateHandle instanceof IncrementalRemoteKeyedStateHandle) {
			IncrementalRemoteKeyedStateHandle incrementalKeyedStateHandle =
				(IncrementalRemoteKeyedStateHandle) stateHandle;

			dos.writeByte(INCREMENTAL_KEY_GROUPS_HANDLE);

			dos.writeLong(incrementalKeyedStateHandle.getCheckpointId());
			dos.writeUTF(String.valueOf(incrementalKeyedStateHandle.getBackendIdentifier()));
			dos.writeInt(incrementalKeyedStateHandle.getKeyGroupRange().getStartKeyGroup());
			dos.writeInt(incrementalKeyedStateHandle.getKeyGroupRange().getNumberOfKeyGroups());

			serializeStreamStateHandle(incrementalKeyedStateHandle.getMetaStateHandle(), dos);

			serializeStreamStateHandleMap(incrementalKeyedStateHandle.getSharedState(), dos);
			serializeStreamStateHandleMap(incrementalKeyedStateHandle.getPrivateState(), dos);
		} else {
			throw new IllegalStateException("Unknown KeyedStateHandle type: " + stateHandle.getClass());
		}
	}

	private static void serializeStreamStateHandleMap(
			Map<StateHandleID, StreamStateHandle> map,
			DataOutputStream dos) throws IOException {

		dos.writeInt(map.size());
		for (Map.Entry<StateHandleID, StreamStateHandle> entry : map.entrySet()) {
			dos.writeUTF(entry.getKey().toString());
			serializeStreamStateHandle(entry.getValue(), dos);
		}
	}

	private static Map<StateHandleID, StreamStateHandle> deserializeStreamStateHandleMap(
			DataInputStream dis) throws IOException {

		final int size = dis.readInt();
		Map<StateHandleID, StreamStateHandle> result = new HashMap<>(size);

		for (int i = 0; i < size; ++i) {
			StateHandleID stateHandleID = new StateHandleID(dis.readUTF());
			StreamStateHandle stateHandle = deserializeStreamStateHandle(dis);
			result.put(stateHandleID, stateHandle);
		}

		return result;
	}

	@VisibleForTesting
	public static KeyedStateHandle deserializeKeyedStateHandle(DataInputStream dis) throws IOException {
		final int type = dis.readByte();
		if (NULL_HANDLE == type) {

			return null;
		} else if (KEY_GROUPS_HANDLE == type) {

			int startKeyGroup = dis.readInt();
			int numKeyGroups = dis.readInt();
			KeyGroupRange keyGroupRange =
				KeyGroupRange.of(startKeyGroup, startKeyGroup + numKeyGroups - 1);
			long[] offsets = new long[numKeyGroups];
			for (int i = 0; i < numKeyGroups; ++i) {
				offsets[i] = dis.readLong();
			}
			KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(
				keyGroupRange, offsets);
			StreamStateHandle stateHandle = deserializeStreamStateHandle(dis);
			return new KeyGroupsStateHandle(keyGroupRangeOffsets, stateHandle);
		} else if (INCREMENTAL_KEY_GROUPS_HANDLE == type) {

			long checkpointId = dis.readLong();
			String backendId = dis.readUTF();
			int startKeyGroup = dis.readInt();
			int numKeyGroups = dis.readInt();
			KeyGroupRange keyGroupRange =
				KeyGroupRange.of(startKeyGroup, startKeyGroup + numKeyGroups - 1);

			StreamStateHandle metaDataStateHandle = deserializeStreamStateHandle(dis);
			Map<StateHandleID, StreamStateHandle> sharedStates = deserializeStreamStateHandleMap(dis);
			Map<StateHandleID, StreamStateHandle> privateStates = deserializeStreamStateHandleMap(dis);

			UUID uuid;

			try {
				uuid = UUID.fromString(backendId);
			} catch (Exception ex) {
				// compatibility with old format pre FLINK-6964:
				uuid = UUID.nameUUIDFromBytes(backendId.getBytes(StandardCharsets.UTF_8));
			}

			return new IncrementalRemoteKeyedStateHandle(
				uuid,
				keyGroupRange,
				checkpointId,
				sharedStates,
				privateStates,
				metaDataStateHandle);
		} else {
			throw new IllegalStateException("Reading invalid KeyedStateHandle, type: " + type);
		}
	}

	@VisibleForTesting
	public static void serializeOperatorStateHandle(
		OperatorStateHandle stateHandle, DataOutputStream dos) throws IOException {

		if (stateHandle != null) {
			dos.writeByte(PARTITIONABLE_OPERATOR_STATE_HANDLE);
			Map<String, OperatorStateHandle.StateMetaInfo> partitionOffsetsMap =
					stateHandle.getStateNameToPartitionOffsets();
			dos.writeInt(partitionOffsetsMap.size());
			for (Map.Entry<String, OperatorStateHandle.StateMetaInfo> entry : partitionOffsetsMap.entrySet()) {
				dos.writeUTF(entry.getKey());

				OperatorStateHandle.StateMetaInfo stateMetaInfo = entry.getValue();

				int mode = stateMetaInfo.getDistributionMode().ordinal();
				dos.writeByte(mode);

				long[] offsets = stateMetaInfo.getOffsets();
				dos.writeInt(offsets.length);
				for (long offset : offsets) {
					dos.writeLong(offset);
				}
			}
			serializeStreamStateHandle(stateHandle.getDelegateStateHandle(), dos);
		} else {
			dos.writeByte(NULL_HANDLE);
		}
	}

	@VisibleForTesting
	public static OperatorStateHandle deserializeOperatorStateHandle(
			DataInputStream dis) throws IOException {

		final int type = dis.readByte();
		if (NULL_HANDLE == type) {
			return null;
		} else if (PARTITIONABLE_OPERATOR_STATE_HANDLE == type) {
			int mapSize = dis.readInt();
			Map<String, OperatorStateHandle.StateMetaInfo> offsetsMap = new HashMap<>(mapSize);
			for (int i = 0; i < mapSize; ++i) {
				String key = dis.readUTF();

				int modeOrdinal = dis.readByte();
				OperatorStateHandle.Mode mode = OperatorStateHandle.Mode.values()[modeOrdinal];

				long[] offsets = new long[dis.readInt()];
				for (int j = 0; j < offsets.length; ++j) {
					offsets[j] = dis.readLong();
				}

				OperatorStateHandle.StateMetaInfo metaInfo =
						new OperatorStateHandle.StateMetaInfo(offsets, mode);
				offsetsMap.put(key, metaInfo);
			}
			StreamStateHandle stateHandle = deserializeStreamStateHandle(dis);
			return new OperatorStreamStateHandle(offsetsMap, stateHandle);
		} else {
			throw new IllegalStateException("Reading invalid OperatorStateHandle, type: " + type);
		}
	}

	@VisibleForTesting
	public static void serializeStreamStateHandle(
			StreamStateHandle stateHandle, DataOutputStream dos) throws IOException {

		if (stateHandle == null) {
			dos.writeByte(NULL_HANDLE);

		} else if (stateHandle instanceof FileStateHandle) {
			dos.writeByte(FILE_STREAM_STATE_HANDLE);
			FileStateHandle fileStateHandle = (FileStateHandle) stateHandle;
			dos.writeLong(stateHandle.getStateSize());
			dos.writeUTF(fileStateHandle.getFilePath().toString());

		} else if (stateHandle instanceof ByteStreamStateHandle) {
			dos.writeByte(BYTE_STREAM_STATE_HANDLE);
			ByteStreamStateHandle byteStreamStateHandle = (ByteStreamStateHandle) stateHandle;
			dos.writeUTF(byteStreamStateHandle.getHandleName());
			byte[] internalData = byteStreamStateHandle.getData();
			dos.writeInt(internalData.length);
			dos.write(byteStreamStateHandle.getData());
		} else {
			throw new IOException("Unknown implementation of StreamStateHandle: " + stateHandle.getClass());
		}

		dos.flush();
	}

	public static StreamStateHandle deserializeStreamStateHandle(DataInputStream dis) throws IOException {
		final int type = dis.read();
		if (NULL_HANDLE == type) {
			return null;
		} else if (FILE_STREAM_STATE_HANDLE == type) {
			long size = dis.readLong();
			String pathString = dis.readUTF();
			return new FileStateHandle(new Path(pathString), size);
		} else if (BYTE_STREAM_STATE_HANDLE == type) {
			String handleName = dis.readUTF();
			int numBytes = dis.readInt();
			byte[] data = new byte[numBytes];
			dis.readFully(data);
			return new ByteStreamStateHandle(handleName, data);
		} else {
			throw new IOException("Unknown implementation of StreamStateHandle, code: " + type);
		}
	}

	// ------------------------------------------------------------------------
	//  utilities
	// ------------------------------------------------------------------------

	@Nullable
	static <T> T extractSingleton(Collection<T> collection) {
		if (collection == null || collection.isEmpty()) {
			return null;
		}

		if (collection.size() == 1) {
			return collection.iterator().next();
		} else {
			throw new IllegalStateException("Expected singleton collection, but found size: " + collection.size());
		}
	}
}
