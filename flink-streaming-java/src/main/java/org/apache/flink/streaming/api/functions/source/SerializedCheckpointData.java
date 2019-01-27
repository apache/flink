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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.HashSet;
import java.util.Set;

/**
 * This class represents serialized checkpoint data for a collection of elements.
 */
class SerializedCheckpointData implements java.io.Serializable {

	private static final long serialVersionUID = -8783744683896503488L;

	/** ID of the checkpoint for which the IDs are stored. */
	private final long checkpointId;

	/** The serialized elements. */
	private final byte[] serializedData;

	/** The number of elements in the checkpoint. */
	private final int numIds;

	/**
	 * Creates a SerializedCheckpointData object for the given serialized data.
	 *
	 * @param checkpointId The checkpointId of the checkpoint.
	 * @param serializedData The serialized IDs in this checkpoint.
	 * @param numIds The number of IDs in the checkpoint.
	 */
	public SerializedCheckpointData(long checkpointId, byte[] serializedData, int numIds) {
		this.checkpointId = checkpointId;
		this.serializedData = serializedData;
		this.numIds = numIds;
	}

	/**
	 * Gets the checkpointId of the checkpoint.
	 * @return The checkpointId of the checkpoint.
	 */
	public long getCheckpointId() {
		return checkpointId;
	}

	/**
	 * Gets the binary data for the serialized elements.
	 * @return The binary data for the serialized elements.
	 */
	public byte[] getSerializedData() {
		return serializedData;
	}

	/**
	 * Gets the number of IDs in the checkpoint.
	 * @return The number of IDs in the checkpoint.
	 */
	public int getNumIds() {
		return numIds;
	}

	// ------------------------------------------------------------------------
	//  Serialize to Checkpoint
	// ------------------------------------------------------------------------

	/**
	 * Converts a list of checkpoints with elements into an array of SerializedCheckpointData.
	 *
	 * @param checkpoints The checkpoints to be converted into IdsCheckpointData.
	 * @param serializer The serializer to serialize the IDs.
	 * @param <T> The type of the ID.
	 * @return An array of serializable SerializedCheckpointData, one per entry in the queue.
	 *
	 * @throws IOException Thrown, if the serialization fails.
	 */
	public static <T> SerializedCheckpointData[] fromDeque(
			ArrayDeque<Tuple2<Long, Set<T>>> checkpoints,
			TypeSerializer<T> serializer) throws IOException {
		return fromDeque(checkpoints, serializer, new DataOutputSerializer(128));
	}

	/**
	 * Converts a list of checkpoints into an array of SerializedCheckpointData.
	 *
	 * @param checkpoints The checkpoints to be converted into IdsCheckpointData.
	 * @param serializer The serializer to serialize the IDs.
	 * @param outputBuffer The reusable serialization buffer.
	 * @param <T> The type of the ID.
	 * @return An array of serializable SerializedCheckpointData, one per entry in the queue.
	 *
	 * @throws IOException Thrown, if the serialization fails.
	 */
	public static <T> SerializedCheckpointData[] fromDeque(
			ArrayDeque<Tuple2<Long, Set<T>>> checkpoints,
			TypeSerializer<T> serializer,
			DataOutputSerializer outputBuffer) throws IOException {

		SerializedCheckpointData[] serializedCheckpoints = new SerializedCheckpointData[checkpoints.size()];

		int pos = 0;
		for (Tuple2<Long, Set<T>> checkpoint : checkpoints) {
			outputBuffer.clear();
			Set<T> checkpointIds = checkpoint.f1;

			for (T id : checkpointIds) {
				serializer.serialize(id, outputBuffer);
			}

			serializedCheckpoints[pos++] = new SerializedCheckpointData(
					checkpoint.f0, outputBuffer.getCopyOfBuffer(), checkpointIds.size());
		}

		return serializedCheckpoints;
	}

	// ------------------------------------------------------------------------
	//  De-Serialize from Checkpoint
	// ------------------------------------------------------------------------

	/**
	 * De-serializes an array of SerializedCheckpointData back into an ArrayDeque of element checkpoints.
	 *
	 * @param data The data to be deserialized.
	 * @param serializer The serializer used to deserialize the data.
	 * @param <T> The type of the elements.
	 * @return An ArrayDeque of element checkpoints.
	 *
	 * @throws IOException Thrown, if the serialization fails.
	 */
	public static <T> ArrayDeque<Tuple2<Long, Set<T>>> toDeque(
			SerializedCheckpointData[] data,
			TypeSerializer<T> serializer) throws IOException {

		ArrayDeque<Tuple2<Long, Set<T>>> deque = new ArrayDeque<>(data.length);
		DataInputDeserializer deser = null;

		for (SerializedCheckpointData checkpoint : data) {
			byte[] serializedData = checkpoint.getSerializedData();
			if (deser == null) {
				deser = new DataInputDeserializer(serializedData, 0, serializedData.length);
			}
			else {
				deser.setBuffer(serializedData, 0, serializedData.length);
			}

			final Set<T> ids = new HashSet<>(checkpoint.getNumIds());
			final int numIds = checkpoint.getNumIds();

			for (int i = 0; i < numIds; i++) {
				ids.add(serializer.deserialize(deser));
			}

			deque.addLast(new Tuple2<Long, Set<T>>(checkpoint.checkpointId, ids));
		}

		return deque;
	}
}
