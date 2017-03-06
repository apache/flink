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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.Preconditions;

/**
 * Generic serializer for {@link Savepoint} instances.
 *
 * <p>This is based on the {@link SavepointV1Serializer} of Flink 1.2.0 that
 * made sure no default Java serialization is used.
 *
 * <p>The generic class allows to customize the file state handle serialization
 * behaviour. This is the only practical difference between Flink 1.2.x and
 * versions >= Flink 1.3.0.
 */
class GenericSavepointSerializer<T extends Savepoint> implements SavepointSerializer<T> {

	private static final byte NULL_HANDLE = 0;
	private static final byte BYTE_STREAM_STATE_HANDLE = 1;
	private static final byte FILE_STREAM_STATE_HANDLE = 2;
	private static final byte KEY_GROUPS_HANDLE = 3;
	private static final byte PARTITIONABLE_OPERATOR_STATE_HANDLE = 4;

	private final SavepointFactory<T> savepointFactory;
	private final FileStateHandleSerializer fileStateHandleSerializer;

	GenericSavepointSerializer(
			SavepointFactory<T> savepointFactory,
			FileStateHandleSerializer fileStateHandleSerializer) {

		this.savepointFactory = Preconditions.checkNotNull(savepointFactory);
		this.fileStateHandleSerializer = Preconditions.checkNotNull(fileStateHandleSerializer);
	}

	@Override
	public void serialize(T savepoint, Path basePath, DataOutputStream dos) throws IOException {
		try {
			dos.writeLong(savepoint.getCheckpointId());

			Collection<TaskState> taskStates = savepoint.getTaskStates();
			dos.writeInt(taskStates.size());

			for (TaskState taskState : savepoint.getTaskStates()) {
				// Vertex ID
				dos.writeLong(taskState.getJobVertexID().getLowerPart());
				dos.writeLong(taskState.getJobVertexID().getUpperPart());

				// Parallelism
				int parallelism = taskState.getParallelism();
				dos.writeInt(parallelism);
				dos.writeInt(taskState.getMaxParallelism());
				dos.writeInt(taskState.getChainLength());

				// Sub task states
				Map<Integer, SubtaskState> subtaskStateMap = taskState.getSubtaskStates();
				dos.writeInt(subtaskStateMap.size());
				for (Map.Entry<Integer, SubtaskState> entry : subtaskStateMap.entrySet()) {
					dos.writeInt(entry.getKey());
					serializeSubtaskState(entry.getValue(), basePath, dos);
				}
			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public T deserialize(DataInputStream dis, Path basePath, ClassLoader userCodeClassLoader) throws IOException {
		long checkpointId = dis.readLong();

		// Task states
		int numTaskStates = dis.readInt();
		List<TaskState> taskStates = new ArrayList<>(numTaskStates);

		for (int i = 0; i < numTaskStates; i++) {
			JobVertexID jobVertexId = new JobVertexID(dis.readLong(), dis.readLong());
			int parallelism = dis.readInt();
			int maxParallelism = dis.readInt();
			int chainLength = dis.readInt();

			// Add task state
			TaskState taskState = new TaskState(jobVertexId, parallelism, maxParallelism, chainLength);
			taskStates.add(taskState);

			// Sub task states
			int numSubTaskStates = dis.readInt();

			for (int j = 0; j < numSubTaskStates; j++) {
				int subtaskIndex = dis.readInt();
				SubtaskState subtaskState = deserializeSubtaskState(basePath, dis);
				taskState.putState(subtaskIndex, subtaskState);
			}
		}

		return savepointFactory.createSavepoint(checkpointId, taskStates);
	}

	private void serializeSubtaskState(SubtaskState subtaskState, Path basePath, DataOutputStream dos) throws IOException {
		dos.writeLong(-1);

		ChainedStateHandle<StreamStateHandle> nonPartitionableState = subtaskState.getLegacyOperatorState();

		int len = nonPartitionableState != null ? nonPartitionableState.getLength() : 0;
		dos.writeInt(len);
		for (int i = 0; i < len; ++i) {
			StreamStateHandle stateHandle = nonPartitionableState.get(i);
			serializeStreamStateHandle(stateHandle, basePath, dos);
		}

		ChainedStateHandle<OperatorStateHandle> operatorStateBackend = subtaskState.getManagedOperatorState();

		len = operatorStateBackend != null ? operatorStateBackend.getLength() : 0;
		dos.writeInt(len);
		for (int i = 0; i < len; ++i) {
			OperatorStateHandle stateHandle = operatorStateBackend.get(i);
			serializeOperatorStateHandle(stateHandle, basePath, dos);
		}

		ChainedStateHandle<OperatorStateHandle> operatorStateFromStream = subtaskState.getRawOperatorState();

		len = operatorStateFromStream != null ? operatorStateFromStream.getLength() : 0;
		dos.writeInt(len);
		for (int i = 0; i < len; ++i) {
			OperatorStateHandle stateHandle = operatorStateFromStream.get(i);
			serializeOperatorStateHandle(stateHandle, basePath, dos);
		}

		KeyGroupsStateHandle keyedStateBackend = subtaskState.getManagedKeyedState();
		serializeKeyGroupStateHandle(keyedStateBackend, basePath, dos);

		KeyGroupsStateHandle keyedStateStream = subtaskState.getRawKeyedState();
		serializeKeyGroupStateHandle(keyedStateStream, basePath, dos);
	}

	private SubtaskState deserializeSubtaskState(Path basePath, DataInputStream dis) throws IOException {
		// Duration field has been removed from SubtaskState
		long ignoredDuration = dis.readLong();

		int len = dis.readInt();
		List<StreamStateHandle> nonPartitionableState = new ArrayList<>(len);
		for (int i = 0; i < len; ++i) {
			StreamStateHandle streamStateHandle = deserializeStreamStateHandle(basePath, dis);
			nonPartitionableState.add(streamStateHandle);
		}


		len = dis.readInt();
		List<OperatorStateHandle> operatorStateBackend = new ArrayList<>(len);
		for (int i = 0; i < len; ++i) {
			OperatorStateHandle streamStateHandle = deserializeOperatorStateHandle(basePath, dis);
			operatorStateBackend.add(streamStateHandle);
		}

		len = dis.readInt();
		List<OperatorStateHandle> operatorStateStream = new ArrayList<>(len);
		for (int i = 0; i < len; ++i) {
			OperatorStateHandle streamStateHandle = deserializeOperatorStateHandle(basePath, dis);
			operatorStateStream.add(streamStateHandle);
		}

		KeyGroupsStateHandle keyedStateBackend = deserializeKeyGroupStateHandle(basePath, dis);

		KeyGroupsStateHandle keyedStateStream = deserializeKeyGroupStateHandle(basePath, dis);

		ChainedStateHandle<StreamStateHandle> nonPartitionableStateChain =
			new ChainedStateHandle<>(nonPartitionableState);

		ChainedStateHandle<OperatorStateHandle> operatorStateBackendChain =
			new ChainedStateHandle<>(operatorStateBackend);

		ChainedStateHandle<OperatorStateHandle> operatorStateStreamChain =
			new ChainedStateHandle<>(operatorStateStream);

		return new SubtaskState(
			nonPartitionableStateChain,
			operatorStateBackendChain,
			operatorStateStreamChain,
			keyedStateBackend,
			keyedStateStream);
	}

	private void serializeKeyGroupStateHandle(KeyGroupsStateHandle stateHandle, Path basePath, DataOutputStream dos) throws IOException {
		if (stateHandle != null) {
			dos.writeByte(KEY_GROUPS_HANDLE);
			dos.writeInt(stateHandle.getGroupRangeOffsets().getKeyGroupRange().getStartKeyGroup());
			dos.writeInt(stateHandle.getNumberOfKeyGroups());
			for (int keyGroup : stateHandle.keyGroups()) {
				dos.writeLong(stateHandle.getOffsetForKeyGroup(keyGroup));
			}
			serializeStreamStateHandle(stateHandle.getDelegateStateHandle(), basePath, dos);
		} else {
			dos.writeByte(NULL_HANDLE);
		}
	}

	private KeyGroupsStateHandle deserializeKeyGroupStateHandle(Path basePath, DataInputStream dis) throws IOException {
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
			KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupRange, offsets);
			StreamStateHandle stateHandle = deserializeStreamStateHandle(basePath, dis);
			return new KeyGroupsStateHandle(keyGroupRangeOffsets, stateHandle);
		} else {
			throw new IllegalStateException("Reading invalid KeyGroupsStateHandle, type: " + type);
		}
	}

	private void serializeOperatorStateHandle(OperatorStateHandle stateHandle, Path basePath, DataOutputStream dos) throws IOException {

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
			serializeStreamStateHandle(stateHandle.getDelegateStateHandle(), basePath, dos);
		} else {
			dos.writeByte(NULL_HANDLE);
		}
	}

	private OperatorStateHandle deserializeOperatorStateHandle(Path basePath, DataInputStream dis) throws IOException {
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
			StreamStateHandle stateHandle = deserializeStreamStateHandle(basePath, dis);
			return new OperatorStateHandle(offsetsMap, stateHandle);
		} else {
			throw new IllegalStateException("Reading invalid OperatorStateHandle, type: " + type);
		}
	}

	private void serializeStreamStateHandle(StreamStateHandle stateHandle, Path basePath, DataOutputStream dos) throws IOException {
		if (stateHandle == null) {
			dos.writeByte(NULL_HANDLE);

		} else if (stateHandle instanceof FileStateHandle) {
			dos.writeByte(FILE_STREAM_STATE_HANDLE);
			FileStateHandle fileStateHandle = (FileStateHandle) stateHandle;
			fileStateHandleSerializer.serializeFileStreamStateHandle(fileStateHandle, basePath, dos);
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

	private StreamStateHandle deserializeStreamStateHandle(Path basePath, DataInputStream dis) throws IOException {
		int type = dis.read();
		if (NULL_HANDLE == type) {
			return null;
		} else if (FILE_STREAM_STATE_HANDLE == type) {
			return fileStateHandleSerializer.deserializeFileStreamStateHandle(basePath, dis);
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
