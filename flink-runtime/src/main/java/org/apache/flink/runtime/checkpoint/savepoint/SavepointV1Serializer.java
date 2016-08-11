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
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Serializer for {@link SavepointV1} instances.
 * <p>
 * <p>In contrast to previous savepoint versions, this serializer makes sure
 * that no default Java serialization is used for serialization. Therefore, we
 * don't rely on any involved Java classes to stay the same.
 */
class SavepointV1Serializer implements SavepointSerializer<SavepointV1> {

	private static final byte NULL_HANDLE = 0;
	private static final byte BYTE_STREAM_STATE_HANDLE = 1;
	private static final byte FILE_STREAM_STATE_HANDLE = 2;
	private static final byte KEY_GROUPS_HANDLE = 3;


	public static final SavepointV1Serializer INSTANCE = new SavepointV1Serializer();

	private SavepointV1Serializer() {
	}

	@Override
	public void serialize(SavepointV1 savepoint, DataOutputStream dos) throws IOException {
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

				// Sub task states
				Map<Integer, SubtaskState> subtaskStateMap = taskState.getSubtaskStates();
				dos.writeInt(subtaskStateMap.size());
				for (Map.Entry<Integer, SubtaskState> entry : subtaskStateMap.entrySet()) {
					dos.writeInt(entry.getKey());

					SubtaskState subtaskState = entry.getValue();
					ChainedStateHandle<StreamStateHandle> chainedStateHandle = subtaskState.getChainedStateHandle();
					dos.writeInt(chainedStateHandle.getLength());
					for (int j = 0; j < chainedStateHandle.getLength(); ++j) {
						StreamStateHandle stateHandle = chainedStateHandle.get(j);
						serializeStreamStateHandle(stateHandle, dos);
					}

					dos.writeLong(subtaskState.getDuration());
				}


				Map<Integer, KeyGroupsStateHandle> keyGroupsStateHandles = taskState.getKeyGroupsStateHandles();
				dos.writeInt(keyGroupsStateHandles.size());
				for (Map.Entry<Integer, KeyGroupsStateHandle> entry : keyGroupsStateHandles.entrySet()) {
					dos.writeInt(entry.getKey());
					serializeKeyGroupStateHandle(entry.getValue(), dos);
				}

			}
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	@Override
	public SavepointV1 deserialize(DataInputStream dis) throws IOException {
		long checkpointId = dis.readLong();

		// Task states
		int numTaskStates = dis.readInt();
		List<TaskState> taskStates = new ArrayList<>(numTaskStates);

		for (int i = 0; i < numTaskStates; i++) {
			JobVertexID jobVertexId = new JobVertexID(dis.readLong(), dis.readLong());
			int parallelism = dis.readInt();
			int maxParallelism = dis.readInt();

			// Add task state
			TaskState taskState = new TaskState(jobVertexId, parallelism, maxParallelism);
			taskStates.add(taskState);

			// Sub task states
			int numSubTaskStates = dis.readInt();

			for (int j = 0; j < numSubTaskStates; j++) {
				int subtaskIndex = dis.readInt();
				int chainedStateHandleSize = dis.readInt();
				List<StreamStateHandle> streamStateHandleList = new ArrayList<>(chainedStateHandleSize);
				for (int k = 0; k < chainedStateHandleSize; ++k) {
					StreamStateHandle streamStateHandle = deserializeStreamStateHandle(dis);
					streamStateHandleList.add(streamStateHandle);
				}

				long duration = dis.readLong();
				ChainedStateHandle<StreamStateHandle> chainedStateHandle = new ChainedStateHandle<>(streamStateHandleList);
				SubtaskState subtaskState = new SubtaskState(chainedStateHandle, duration);
				taskState.putState(subtaskIndex, subtaskState);
			}

			// Key group states
			int numKeyGroupStates = dis.readInt();
			for (int j = 0; j < numKeyGroupStates; j++) {
				int keyGroupIndex = dis.readInt();

				KeyGroupsStateHandle keyGroupsStateHandle = deserializeKeyGroupStateHandle(dis);
				if (keyGroupsStateHandle != null) {
					taskState.putKeyedState(keyGroupIndex, keyGroupsStateHandle);
				}
			}
		}

		return new SavepointV1(checkpointId, taskStates);
	}

	public static void serializeKeyGroupStateHandle(KeyGroupsStateHandle stateHandle, DataOutputStream dos) throws IOException {
		if (stateHandle != null) {
			dos.writeByte(KEY_GROUPS_HANDLE);
			dos.writeInt(stateHandle.getGroupRangeOffsets().getKeyGroupRange().getStartKeyGroup());
			dos.writeInt(stateHandle.getNumberOfKeyGroups());
			for (int keyGroup : stateHandle.keyGroups()) {
				dos.writeLong(stateHandle.getOffsetForKeyGroup(keyGroup));
			}
			serializeStreamStateHandle(stateHandle.getStateHandle(), dos);
		} else {
			dos.writeByte(NULL_HANDLE);
		}
	}

	public static KeyGroupsStateHandle deserializeKeyGroupStateHandle(DataInputStream dis) throws IOException {
		int type = dis.readByte();
		if (NULL_HANDLE == type) {
			return null;
		} else {
			int startKeyGroup = dis.readInt();
			int numKeyGroups = dis.readInt();
			KeyGroupRange keyGroupRange = KeyGroupRange.of(startKeyGroup, startKeyGroup + numKeyGroups - 1);
			long[] offsets = new long[numKeyGroups];
			for (int i = 0; i < numKeyGroups; ++i) {
				offsets[i] = dis.readLong();
			}
			KeyGroupRangeOffsets keyGroupRangeOffsets = new KeyGroupRangeOffsets(keyGroupRange, offsets);
			StreamStateHandle stateHandle = deserializeStreamStateHandle(dis);
			return new KeyGroupsStateHandle(keyGroupRangeOffsets, stateHandle);
		}
	}

	public static void serializeStreamStateHandle(StreamStateHandle stateHandle, DataOutputStream dos) throws IOException {

		if (stateHandle == null) {
			dos.writeByte(NULL_HANDLE);

		} else if (stateHandle instanceof FileStateHandle) {
			dos.writeByte(FILE_STREAM_STATE_HANDLE);
			FileStateHandle fileStateHandle = (FileStateHandle) stateHandle;
			dos.writeUTF(fileStateHandle.getFilePath().toString());

		} else if (stateHandle instanceof ByteStreamStateHandle) {
			dos.writeByte(BYTE_STREAM_STATE_HANDLE);
			ByteStreamStateHandle byteStreamStateHandle = (ByteStreamStateHandle) stateHandle;
			byte[] internalData = byteStreamStateHandle.getData();
			dos.writeInt(internalData.length);
			dos.write(byteStreamStateHandle.getData());

		} else {
			throw new IOException("Unknown implementation of StreamStateHandle: " + stateHandle.getClass());
		}

		dos.flush();
	}

	public static StreamStateHandle deserializeStreamStateHandle(DataInputStream dis) throws IOException {
		int type = dis.read();
		if (NULL_HANDLE == type) {
			return null;
		} else if (FILE_STREAM_STATE_HANDLE == type) {
			String pathString = dis.readUTF();
			return new FileStateHandle(new Path(pathString));
		} else if (BYTE_STREAM_STATE_HANDLE == type) {
			int numBytes = dis.readInt();
			byte[] data = new byte[numBytes];
			dis.read(data);
			return new ByteStreamStateHandle(data);
		} else {
			throw new IOException("Unknown implementation of StreamStateHandle, code: " + type);
		}
	}
}
