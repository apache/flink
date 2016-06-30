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

import org.apache.flink.runtime.checkpoint.KeyGroupState;
import org.apache.flink.runtime.checkpoint.SubtaskState;
import org.apache.flink.runtime.checkpoint.TaskState;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.state.StateHandle;
import org.apache.flink.util.SerializedValue;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Serializer for {@link SavepointV0} instances.
 *
 * <p>In contrast to previous savepoint versions, this serializer makes sure
 * that no default Java serialization is used for serialization. Therefore, we
 * don't rely on any involved Java classes to stay the same.
 */
class SavepointV1Serializer implements SavepointSerializer<SavepointV0> {

	public static final SavepointV1Serializer INSTANCE = new SavepointV1Serializer();

	private SavepointV1Serializer() {
	}

	@Override
	public void serialize(SavepointV0 savepoint, DataOutputStream dos) throws IOException {
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

	@Override
	public SavepointV0 deserialize(DataInputStream dis) throws IOException {
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
					dis.read(serializedData, 0, length);
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
					dis.read(serializedData, 0, length);
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

		return new SavepointV0(checkpointId, taskStates);
	}
}
