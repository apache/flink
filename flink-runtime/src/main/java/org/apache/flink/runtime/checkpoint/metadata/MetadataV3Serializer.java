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
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

/**
 * (De)serializer for checkpoint metadata format version 3.
 * This format was introduced with Apache Flink 1.11.0.
 *
 * <p>Compared to format version 2, this drops some unused fields and introduces
 * operator coordinator state.
 *
 * <p>See {@link MetadataV2V3SerializerBase} for a description of the format layout.
 */
@Internal
public class MetadataV3Serializer extends MetadataV2V3SerializerBase implements MetadataSerializer {

	/** The metadata format version. */
	public static final int VERSION = 3;

	/** The singleton instance of the serializer. */
	public static final MetadataV3Serializer INSTANCE = new MetadataV3Serializer();

	/** Singleton, not meant to be instantiated. */
	private MetadataV3Serializer() {}

	@Override
	public int getVersion() {
		return VERSION;
	}

	// ------------------------------------------------------------------------
	//  (De)serialization entry points
	// ------------------------------------------------------------------------

	public static void serialize(CheckpointMetadata checkpointMetadata, DataOutputStream dos) throws IOException {
		INSTANCE.serializeMetadata(checkpointMetadata, dos);
	}

	@Override
	public CheckpointMetadata deserialize(DataInputStream dis, ClassLoader classLoader) throws IOException {
		return deserializeMetadata(dis);
	}

	// ------------------------------------------------------------------------
	//  version-specific serialization formats
	// ------------------------------------------------------------------------

	@Override
	protected void serializeOperatorState(OperatorState operatorState, DataOutputStream dos) throws IOException {
		// Operator ID
		dos.writeLong(operatorState.getOperatorID().getLowerPart());
		dos.writeLong(operatorState.getOperatorID().getUpperPart());

		// Parallelism
		dos.writeInt(operatorState.getParallelism());
		dos.writeInt(operatorState.getMaxParallelism());

		// Coordinator state
		serializeStreamStateHandle(operatorState.getCoordinatorState(), dos);

		// Sub task states
		final Map<Integer, OperatorSubtaskState> subtaskStateMap = operatorState.getSubtaskStates();
		dos.writeInt(subtaskStateMap.size());
		for (Map.Entry<Integer, OperatorSubtaskState> entry : subtaskStateMap.entrySet()) {
			dos.writeInt(entry.getKey());
			serializeSubtaskState(entry.getValue(), dos);
		}
	}

	@Override
	protected OperatorState deserializeOperatorState(DataInputStream dis) throws IOException {
		final OperatorID jobVertexId = new OperatorID(dis.readLong(), dis.readLong());
		final int parallelism = dis.readInt();
		final int maxParallelism = dis.readInt();

		final OperatorState operatorState = new OperatorState(jobVertexId, parallelism, maxParallelism);

		// Coordinator state
		operatorState.setCoordinatorState(deserializeStreamStateHandle(dis));

		// Sub task states
		final int numSubTaskStates = dis.readInt();

		for (int j = 0; j < numSubTaskStates; j++) {
			final int subtaskIndex = dis.readInt();
			final OperatorSubtaskState subtaskState = deserializeSubtaskState(dis);
			operatorState.putState(subtaskIndex, subtaskState);
		}

		return operatorState;
	}
}
