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

package org.apache.flink.migration.runtime.checkpoint.savepoint;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.fs.Path;
import org.apache.flink.migration.runtime.checkpoint.KeyGroupState;
import org.apache.flink.migration.runtime.checkpoint.SubtaskState;
import org.apache.flink.migration.runtime.checkpoint.TaskState;
import org.apache.flink.migration.runtime.state.AbstractStateBackend;
import org.apache.flink.migration.runtime.state.KvStateSnapshot;
import org.apache.flink.migration.runtime.state.StateHandle;
import org.apache.flink.migration.runtime.state.filesystem.AbstractFileStateHandle;
import org.apache.flink.migration.runtime.state.memory.SerializedStateHandle;
import org.apache.flink.migration.state.MigrationKeyGroupStateHandle;
import org.apache.flink.migration.state.MigrationStreamStateHandle;
import org.apache.flink.migration.streaming.runtime.tasks.StreamTaskState;
import org.apache.flink.migration.streaming.runtime.tasks.StreamTaskStateList;
import org.apache.flink.migration.util.SerializedValue;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointSerializer;
import org.apache.flink.runtime.checkpoint.savepoint.SavepointV1;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.ChainedStateHandle;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.KeyGroupRangeOffsets;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.MultiStreamStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FileStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.runtime.state.memory.MemCheckpointStreamFactory;
import org.apache.flink.util.IOUtils;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>
 * <p>In contrast to previous savepoint versions, this serializer makes sure
 * that no default Java serialization is used for serialization. Therefore, we
 * don't rely on any involved Java classes to stay the same.
 */
public class SavepointV0Serializer implements SavepointSerializer<SavepointV1> {

	public static final SavepointV0Serializer INSTANCE = new SavepointV0Serializer();
	private static final StreamStateHandle SIGNAL_0 = new ByteStreamStateHandle("SIGNAL_0", new byte[]{0});
	private static final StreamStateHandle SIGNAL_1 = new ByteStreamStateHandle("SIGNAL_1", new byte[]{1});

	private static final int MAX_SIZE = 4 * 1024 * 1024;

	private SavepointV0Serializer() {
	}


	@Override
	public void serialize(SavepointV1 savepoint, DataOutputStream dos) throws IOException {
		throw new UnsupportedOperationException("This serializer is read-only and only exists for backwards compatibility");
	}

	@Override
	public SavepointV1 deserialize(DataInputStream dis, ClassLoader userClassLoader) throws IOException {

		long checkpointId = dis.readLong();

		// Task states
		int numTaskStates = dis.readInt();
		List<TaskState> taskStates = new ArrayList<>(numTaskStates);

		for (int i = 0; i < numTaskStates; i++) {
			JobVertexID jobVertexId = new JobVertexID(dis.readLong(), dis.readLong());
			int parallelism = dis.readInt();

			// Add task state
			TaskState taskState = new TaskState(jobVertexId, parallelism);
			taskStates.add(taskState);

			// Sub task states
			int numSubTaskStates = dis.readInt();
			for (int j = 0; j < numSubTaskStates; j++) {
				int subtaskIndex = dis.readInt();

				int length = dis.readInt();

				SerializedValue<StateHandle<?>> serializedValue;
				if (length == -1) {
					serializedValue = new SerializedValue<>(null);
				} else {
					byte[] serializedData = new byte[length];
					dis.readFully(serializedData, 0, length);
					serializedValue = SerializedValue.fromBytes(serializedData);
				}

				long stateSize = dis.readLong();
				long duration = dis.readLong();

				SubtaskState subtaskState = new SubtaskState(
						serializedValue,
						stateSize,
						duration);

				taskState.putState(subtaskIndex, subtaskState);
			}

			// Key group states
			int numKvStates = dis.readInt();
			for (int j = 0; j < numKvStates; j++) {
				int keyGroupIndex = dis.readInt();

				int length = dis.readInt();

				SerializedValue<StateHandle<?>> serializedValue;
				if (length == -1) {
					serializedValue = new SerializedValue<>(null);
				} else {
					byte[] serializedData = new byte[length];
					dis.readFully(serializedData, 0, length);
					serializedValue = SerializedValue.fromBytes(serializedData);
				}

				long stateSize = dis.readLong();
				long duration = dis.readLong();

				KeyGroupState keyGroupState = new KeyGroupState(
						serializedValue,
						stateSize,
						duration);

				taskState.putKvState(keyGroupIndex, keyGroupState);
			}
		}

		try {
			return convertSavepoint(taskStates, userClassLoader, checkpointId);
		} catch (Exception e) {
			throw new IOException(e);
		}
	}

	private SavepointV1 convertSavepoint(
			List<TaskState> taskStates,
			ClassLoader userClassLoader,
			long checkpointID) throws Exception {

		List<org.apache.flink.runtime.checkpoint.TaskState> newTaskStates = new ArrayList<>(taskStates.size());

		for (TaskState taskState : taskStates) {
			newTaskStates.add(convertTaskState(taskState, userClassLoader, checkpointID));
		}

		return new SavepointV1(checkpointID, newTaskStates);
	}

	private org.apache.flink.runtime.checkpoint.TaskState convertTaskState(
			TaskState taskState,
			ClassLoader userClassLoader,
			long checkpointID) throws Exception {

		JobVertexID jobVertexID = taskState.getJobVertexID();
		int parallelism = taskState.getParallelism();
		int chainLength = determineOperatorChainLength(taskState, userClassLoader);

		org.apache.flink.runtime.checkpoint.TaskState newTaskState =
				new org.apache.flink.runtime.checkpoint.TaskState(
						jobVertexID,
						parallelism,
						parallelism,
						chainLength);

		if (chainLength > 0) {

			Map<Integer, SubtaskState> subtaskStates = taskState.getSubtaskStatesById();

			for (Map.Entry<Integer, SubtaskState> subtaskState : subtaskStates.entrySet()) {
				int parallelInstanceIdx = subtaskState.getKey();
				newTaskState.putState(parallelInstanceIdx, convertSubtaskState(
						subtaskState.getValue(),
						parallelInstanceIdx,
						userClassLoader,
						checkpointID));
			}
		}

		return newTaskState;
	}

	private org.apache.flink.runtime.checkpoint.SubtaskState convertSubtaskState(
			SubtaskState subtaskState,
			int parallelInstanceIdx,
			ClassLoader userClassLoader,
			long checkpointID) throws Exception {

		SerializedValue<StateHandle<?>> serializedValue = subtaskState.getState();

		StreamTaskStateList stateList = (StreamTaskStateList) serializedValue.deserializeValue(userClassLoader);
		StreamTaskState[] streamTaskStates = stateList.getState(userClassLoader);

		List<StreamStateHandle> newChainStateList = Arrays.asList(new StreamStateHandle[streamTaskStates.length]);
		KeyGroupsStateHandle newKeyedState = null;

		for (int chainIdx = 0; chainIdx < streamTaskStates.length; ++chainIdx) {

			StreamTaskState streamTaskState = streamTaskStates[chainIdx];
			if (streamTaskState == null) {
				continue;
			}

			newChainStateList.set(chainIdx, convertOperatorAndFunctionState(streamTaskState));
			HashMap<String, KvStateSnapshot<?, ?, ?, ?>> oldKeyedState = streamTaskState.getKvStates();

			if (null != oldKeyedState) {
				Preconditions.checkState(null == newKeyedState, "Found more than one keyed state in chain");
				newKeyedState = convertKeyedBackendState(oldKeyedState, parallelInstanceIdx, checkpointID);
			}
		}

		ChainedStateHandle<StreamStateHandle> newChainedState = new ChainedStateHandle<>(newChainStateList);
		ChainedStateHandle<OperatorStateHandle> nopChain =
				new ChainedStateHandle<>(Arrays.asList(new OperatorStateHandle[newChainedState.getLength()]));

		return new org.apache.flink.runtime.checkpoint.SubtaskState(
				newChainedState,
				nopChain,
				nopChain,
				newKeyedState,
				null);
	}

	/**
	 * This is public so that we can use it when restoring a legacy snapshot
	 * in {@code AbstractStreamOperatorTestHarness}.
	 */
	public static StreamStateHandle convertOperatorAndFunctionState(StreamTaskState streamTaskState) throws Exception {

		List<StreamStateHandle> mergeStateHandles = new ArrayList<>(4);

		StateHandle<Serializable> functionState = streamTaskState.getFunctionState();
		StateHandle<?> operatorState = streamTaskState.getOperatorState();

		if (null != functionState) {
			mergeStateHandles.add(SIGNAL_1);
			mergeStateHandles.add(convertStateHandle(functionState));
		} else {
			mergeStateHandles.add(SIGNAL_0);
		}

		if (null != operatorState) {
			mergeStateHandles.add(convertStateHandle(operatorState));
		}

		return new MigrationStreamStateHandle(new MultiStreamStateHandle(mergeStateHandles));
	}

	/**
	 * This is public so that we can use it when restoring a legacy snapshot
	 * in {@code AbstractStreamOperatorTestHarness}.
	 */
	public static KeyGroupsStateHandle convertKeyedBackendState(
			HashMap<String, KvStateSnapshot<?, ?, ?, ?>> oldKeyedState,
			int parallelInstanceIdx,
			long checkpointID) throws Exception {

		if (null != oldKeyedState) {

			CheckpointStreamFactory checkpointStreamFactory = new MemCheckpointStreamFactory(MAX_SIZE);

			CheckpointStreamFactory.CheckpointStateOutputStream keyedStateOut =
					checkpointStreamFactory.createCheckpointStateOutputStream(checkpointID, 0L);

			try {
				final long offset = keyedStateOut.getPos();

				InstantiationUtil.serializeObject(keyedStateOut, oldKeyedState);
				StreamStateHandle streamStateHandle = keyedStateOut.closeAndGetHandle();
				keyedStateOut = null; // makes IOUtils.closeQuietly(...) ignore this

				if (null != streamStateHandle) {
					KeyGroupRangeOffsets keyGroupRangeOffsets =
							new KeyGroupRangeOffsets(parallelInstanceIdx, parallelInstanceIdx, new long[]{offset});

					return new MigrationKeyGroupStateHandle(keyGroupRangeOffsets, streamStateHandle);
				}
			} finally {
				IOUtils.closeQuietly(keyedStateOut);
			}
		}
		return null;
	}

	private int determineOperatorChainLength(
			TaskState taskState,
			ClassLoader userClassLoader) throws IOException, ClassNotFoundException {

		Collection<SubtaskState> subtaskStates = taskState.getStates();

		if (subtaskStates == null || subtaskStates.isEmpty()) {
			return 0;
		}

		SubtaskState firstSubtaskState = subtaskStates.iterator().next();
		Object toCastTaskStateList = firstSubtaskState.getState().deserializeValue(userClassLoader);

		if (toCastTaskStateList instanceof StreamTaskStateList) {
			StreamTaskStateList taskStateList = (StreamTaskStateList) toCastTaskStateList;
			StreamTaskState[] streamTaskStates = taskStateList.getState(userClassLoader);

			return streamTaskStates.length;
		}
		return 0;
	}

	/**
	 * This is public so that we can use it when restoring a legacy snapshot
	 * in {@code AbstractStreamOperatorTestHarness}.
	 */
	public static StreamStateHandle convertStateHandle(StateHandle<?> oldStateHandle) throws Exception {
		if (oldStateHandle instanceof AbstractFileStateHandle) {
			Path path = ((AbstractFileStateHandle) oldStateHandle).getFilePath();
			return new FileStateHandle(path, oldStateHandle.getStateSize());
		} else if (oldStateHandle instanceof SerializedStateHandle) {
			byte[] data = ((SerializedStateHandle<?>) oldStateHandle).getSerializedData();
			return new ByteStreamStateHandle(String.valueOf(System.identityHashCode(data)), data);
		} else if (oldStateHandle instanceof org.apache.flink.migration.runtime.state.memory.ByteStreamStateHandle) {
			byte[] data =
					((org.apache.flink.migration.runtime.state.memory.ByteStreamStateHandle) oldStateHandle).getData();
			return new ByteStreamStateHandle(String.valueOf(System.identityHashCode(data)), data);
		} else if (oldStateHandle instanceof AbstractStateBackend.DataInputViewHandle) {
			return convertStateHandle(
					((AbstractStateBackend.DataInputViewHandle) oldStateHandle).getStreamStateHandle());
		}
		throw new IllegalArgumentException("Unknown state handle type: " + oldStateHandle);
	}

	@VisibleForTesting
	public void serializeOld(SavepointV0 savepoint, DataOutputStream dos) throws IOException {
		dos.writeLong(savepoint.getCheckpointId());

		Collection<org.apache.flink.migration.runtime.checkpoint.TaskState> taskStates = savepoint.getOldTaskStates();
		dos.writeInt(taskStates.size());

		for (org.apache.flink.migration.runtime.checkpoint.TaskState taskState : savepoint.getOldTaskStates()) {
			// Vertex ID
			dos.writeLong(taskState.getJobVertexID().getLowerPart());
			dos.writeLong(taskState.getJobVertexID().getUpperPart());

			// Parallelism
			int parallelism = taskState.getParallelism();
			dos.writeInt(parallelism);

			// Sub task states
			dos.writeInt(taskState.getNumberCollectedStates());

			for (int i = 0; i < parallelism; i++) {
				SubtaskState subtaskState = taskState.getState(i);

				if (subtaskState != null) {
					dos.writeInt(i);

					SerializedValue<?> serializedValue = subtaskState.getState();
					if (serializedValue == null) {
						dos.writeInt(-1); // null
					} else {
						byte[] serialized = serializedValue.getByteArray();
						dos.writeInt(serialized.length);
						dos.write(serialized, 0, serialized.length);
					}

					dos.writeLong(subtaskState.getStateSize());
					dos.writeLong(subtaskState.getDuration());
				}
			}

			// Key group states
			dos.writeInt(taskState.getNumberCollectedKvStates());

			for (int i = 0; i < parallelism; i++) {
				KeyGroupState keyGroupState = taskState.getKvState(i);

				if (keyGroupState != null) {
					dos.write(i);

					SerializedValue<?> serializedValue = keyGroupState.getKeyGroupState();
					if (serializedValue == null) {
						dos.writeInt(-1); // null
					} else {
						byte[] serialized = serializedValue.getByteArray();
						dos.writeInt(serialized.length);
						dos.write(serialized, 0, serialized.length);
					}

					dos.writeLong(keyGroupState.getStateSize());
					dos.writeLong(keyGroupState.getDuration());
				}
			}
		}
	}
}
