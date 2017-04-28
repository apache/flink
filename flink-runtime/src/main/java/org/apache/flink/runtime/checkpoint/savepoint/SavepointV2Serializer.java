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

package org.apache.flink.runtime.checkpoint.savepoint;

import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.MasterState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * (De)serializer for checkpoint metadata format version 2.
 * 
 * <p>This format version adds
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
class SavepointV2Serializer implements SavepointSerializer<SavepointV2> {

	/** Random magic number for consistency checks */
	private static final int MASTER_STATE_MAGIC_NUMBER = 0xc96b1696;

	private static final byte NULL_HANDLE = 0;
	private static final byte BYTE_STREAM_STATE_HANDLE = 1;
	private static final byte FILE_STREAM_STATE_HANDLE = 2;
	private static final byte KEY_GROUPS_HANDLE = 3;
	private static final byte PARTITIONABLE_OPERATOR_STATE_HANDLE = 4;

	/** The singleton instance of the serializer */
	public static final SavepointV2Serializer INSTANCE = new SavepointV2Serializer();

	// ------------------------------------------------------------------------

	/** Singleton, not meant to be instantiated */
	private SavepointV2Serializer() {}

	// ------------------------------------------------------------------------
	//  (De)serialization entry points
	// ------------------------------------------------------------------------

	@Override
	public void serialize(SavepointV2 checkpointMetadata, DataOutputStream dos) throws IOException {
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
			// Operator ID
			dos.writeLong(operatorState.getOperatorID().getLowerPart());
			dos.writeLong(operatorState.getOperatorID().getUpperPart());

			// Parallelism
			int parallelism = operatorState.getParallelism();
			dos.writeInt(parallelism);
			dos.writeInt(operatorState.getMaxParallelism());
			dos.writeInt(1);

			// Sub task states
			Map<Integer, OperatorSubtaskState> subtaskStateMap = operatorState.getSubtaskStates();
			dos.writeInt(subtaskStateMap.size());
			for (Map.Entry<Integer, OperatorSubtaskState> entry : subtaskStateMap.entrySet()) {
				dos.writeInt(entry.getKey());
				serializeSubtaskState(entry.getValue(), dos);
			}
		}
	}

	@Override
	public SavepointV2 deserialize(DataInputStream dis, ClassLoader cl) throws IOException {
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
		int numTaskStates = dis.readInt();
		List<OperatorState> operatorStates = new ArrayList<>(numTaskStates);

		for (int i = 0; i < numTaskStates; i++) {
			OperatorID jobVertexId = new OperatorID(dis.readLong(), dis.readLong());
			int parallelism = dis.readInt();
			int maxParallelism = dis.readInt();
			int chainLength = dis.readInt();

			// Add task state
			OperatorState taskState = new OperatorState(jobVertexId, parallelism, maxParallelism);
			operatorStates.add(taskState);

			// Sub task states
			int numSubTaskStates = dis.readInt();

			for (int j = 0; j < numSubTaskStates; j++) {
				int subtaskIndex = dis.readInt();

				OperatorSubtaskState subtaskState = deserializeSubtaskState(dis);
				taskState.putState(subtaskIndex, subtaskState);
			}
		}

		return new SavepointV2(checkpointId, operatorStates, masterStates);
	}

	// ------------------------------------------------------------------------
	//  master state (de)serialization methods
	// ------------------------------------------------------------------------

	private void serializeMasterState(MasterState state, DataOutputStream dos) throws IOException {
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

	private MasterState deserializeMasterState(DataInputStream dis) throws IOException {
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
	//  task state (de)serialization methods
	// ------------------------------------------------------------------------

	private static void serializeSubtaskState(OperatorSubtaskState subtaskState, DataOutputStream dos) throws IOException {

		dos.writeLong(-1);

		StreamStateHandle nonPartitionableState = subtaskState.getLegacyOperatorState();

		int len = nonPartitionableState != null ? 1 : 0;
		dos.writeInt(len);
		if (len == 1) {
			serializeStreamStateHandle(nonPartitionableState, dos);
		}

		OperatorStateHandle operatorStateBackend = subtaskState.getManagedOperatorState();

		len = operatorStateBackend != null ? 1 : 0;
		dos.writeInt(len);
		if (len == 1) {
			serializeOperatorStateHandle(operatorStateBackend, dos);
		}

		OperatorStateHandle operatorStateFromStream = subtaskState.getRawOperatorState();

		len = operatorStateFromStream != null ? 1 : 0;
		dos.writeInt(len);
		if (len == 1) {
			serializeOperatorStateHandle(operatorStateFromStream, dos);
		}

		KeyedStateHandle keyedStateBackend = subtaskState.getManagedKeyedState();
		serializeKeyedStateHandle(keyedStateBackend, dos);

		KeyedStateHandle keyedStateStream = subtaskState.getRawKeyedState();
		serializeKeyedStateHandle(keyedStateStream, dos);
	}

	private static OperatorSubtaskState deserializeSubtaskState(DataInputStream dis) throws IOException {
		// Duration field has been removed from SubtaskState
		long ignoredDuration = dis.readLong();

		int len = dis.readInt();
		StreamStateHandle nonPartitionableState = len == 0 ? null : deserializeStreamStateHandle(dis);

		len = dis.readInt();
		OperatorStateHandle operatorStateBackend = len == 0 ? null : deserializeOperatorStateHandle(dis);

		len = dis.readInt();
		OperatorStateHandle operatorStateStream = len == 0 ? null : deserializeOperatorStateHandle(dis);

		KeyedStateHandle keyedStateBackend = deserializeKeyedStateHandle(dis);

		KeyedStateHandle keyedStateStream = deserializeKeyedStateHandle(dis);


		return new OperatorSubtaskState(
				nonPartitionableState,
				operatorStateBackend,
				operatorStateStream,
				keyedStateBackend,
				keyedStateStream);
	}

	private static void serializeKeyedStateHandle(
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
		} else {
			throw new IllegalStateException("Unknown KeyedStateHandle type: " + stateHandle.getClass());
		}
	}

	private static KeyedStateHandle deserializeKeyedStateHandle(DataInputStream dis) throws IOException {
		final int type = dis.readByte();
		if (NULL_HANDLE == type) {
			return null;
		} else if (KEY_GROUPS_HANDLE == type) {
			int startKeyGroup = dis.readInt();
			int numKeyGroups = dis.readInt();
			KeyGroupRange keyGroupRange = KeyGroupRange.of(startKeyGroup, startKeyGroup + numKeyGroups - 1);
			long[] offsets = new long[numKeyGroups];
			for (int i = 0; i < numKeyGroups; ++i) {
				offsets[i] = dis.readLong();
			}
			KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(
				keyGroupRange, offsets);
			StreamStateHandle stateHandle = deserializeStreamStateHandle(dis);
			return new KeyGroupsStateHandle(keyGroupRangeOffsets, stateHandle);
		} else {
			throw new IllegalStateException("Reading invalid KeyedStateHandle, type: " + type);
		}
	}

	private static void serializeOperatorStateHandle(
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

	private static OperatorStateHandle deserializeOperatorStateHandle(
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
			return new OperatorStateHandle(offsetsMap, stateHandle);
		} else {
			throw new IllegalStateException("Reading invalid OperatorStateHandle, type: " + type);
		}
	}

	private static void serializeStreamStateHandle(
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

	private static StreamStateHandle deserializeStreamStateHandle(DataInputStream dis) throws IOException {
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
}
