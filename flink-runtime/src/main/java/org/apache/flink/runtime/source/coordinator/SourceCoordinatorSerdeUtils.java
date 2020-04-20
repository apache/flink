/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.ReaderInfo;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

import static org.apache.flink.util.serde.SerdeUtils.readBytes;
import static org.apache.flink.util.serde.SerdeUtils.readInt;
import static org.apache.flink.util.serde.SerdeUtils.readLong;
import static org.apache.flink.util.serde.SerdeUtils.readString;
import static org.apache.flink.util.serde.SerdeUtils.writeBytes;
import static org.apache.flink.util.serde.SerdeUtils.writeInt;
import static org.apache.flink.util.serde.SerdeUtils.writeLong;
import static org.apache.flink.util.serde.SerdeUtils.writeString;

/**
 * A serialization util class for the {@link SourceCoordinator}.
 */
public class SourceCoordinatorSerdeUtils {
	/** The current source coordinator serde version. */
	private static final int CURRENT_VERSION = 0;

	/** Private constructor for utility class. */
	private SourceCoordinatorSerdeUtils() {}

	/** Write the current serde version. */
	static void writeCoordinatorSerdeVersion(ByteArrayOutputStream out) throws IOException {
		writeInt(CURRENT_VERSION, out);
	}

	/** Read and verify the serde version. */
	static void readAndVerifyCoordinatorSerdeVersion(ByteArrayInputStream in) throws IOException {
		int version = readInt(in);
		if (version > CURRENT_VERSION) {
			throw new IOException("Unsupported source coordinator serde version " + version);
		}
	}

	/**
	 * Get serialized size of the registered readers map.
	 *
	 * <p>The binary format is following:
	 * 4 Bytes - num entries.
	 * N Bytes - entries
	 * 		4 Bytes - subtask id
	 * 		N Bytes - reader info, see {@link #writeReaderInfo(ReaderInfo, ByteArrayOutputStream)}.
	 */
	static void writeRegisteredReaders(Map<Integer, ReaderInfo> registeredReaders, ByteArrayOutputStream out) throws IOException {
		writeInt(registeredReaders.size(), out);
		for (ReaderInfo info : registeredReaders.values()) {
			writeReaderInfo(info, out);
		}
	}

	static Map<Integer, ReaderInfo> readRegisteredReaders(ByteArrayInputStream in) {
		int numReaders = readInt(in);
		Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();
		for (int i = 0; i < numReaders; i++) {
			ReaderInfo info = readReaderInfo(in);
			registeredReaders.put(info.getSubtaskId(), info);
		}
		return registeredReaders;
	}

	/**
	 * Serialize the assignment by checkpoint ids.
	 */
	static <SplitT> void writeAssignmentsByCheckpointId(
			Map<Long, Map<Integer, LinkedHashSet<SplitT>>> assignmentByCheckpointIds,
			SimpleVersionedSerializer<SplitT> splitSerializer,
			ByteArrayOutputStream out) throws IOException {
		// SplitSerializer version.
		writeInt(splitSerializer.getVersion(), out);
		// Num checkpoints.
		writeInt(assignmentByCheckpointIds.size(), out);
		for (Map.Entry<Long, Map<Integer, LinkedHashSet<SplitT>>> assignments : assignmentByCheckpointIds.entrySet()) {
			long checkpointId = assignments.getKey();
			writeLong(checkpointId, out);

			int numSubtasks = assignments.getValue().size();
			writeInt(numSubtasks, out);
			for (Map.Entry<Integer, LinkedHashSet<SplitT>> assignment : assignments.getValue().entrySet()) {
				int subtaskId = assignment.getKey();
				writeInt(subtaskId, out);

				int numAssignedSplits = assignment.getValue().size();
				writeInt(numAssignedSplits, out);
				for (SplitT split : assignment.getValue()) {
					byte[] serializedSplit = splitSerializer.serialize(split);
					writeBytes(serializedSplit, out);
				}
			}
		}
	}

	/**
	 * Deserialize the assignment by checkpoint ids.
	 */
	static <SplitT> Map<Long, Map<Integer, LinkedHashSet<SplitT>>> readAssignmentsByCheckpointId(
			ByteArrayInputStream in,
			SimpleVersionedSerializer<SplitT> splitSerializer) throws IOException {
		int splitSerializerVersion = readInt(in);
		int numCheckpoints = readInt(in);
		Map<Long, Map<Integer, LinkedHashSet<SplitT>>> assignmentsByCheckpointIds = new HashMap<>(numCheckpoints);
		for (int i = 0; i < numCheckpoints; i++) {
			long checkpointId = readLong(in);
			int numSubtasks = readInt(in);
			Map<Integer, LinkedHashSet<SplitT>> assignments = new HashMap<>();
			assignmentsByCheckpointIds.put(checkpointId, assignments);
			for (int j = 0; j < numSubtasks; j++) {
				int subtaskId = readInt(in);
				int numAssignedSplits = readInt(in);
				LinkedHashSet<SplitT> splits = new LinkedHashSet<>(numAssignedSplits);
				assignments.put(subtaskId, splits);
				for (int k = 0; k < numAssignedSplits; k++) {
					byte[] serializedSplit = readBytes(in);
					SplitT split = splitSerializer.deserialize(splitSerializerVersion, serializedSplit);
					splits.add(split);
				}
			}
		}
		return assignmentsByCheckpointIds;
	}

	// ----- private helper methods -----

	/**
	 * Serialize {@link ReaderInfo}.
	 *
	 * <p>The binary format is following:
	 * 4 Bytes - subtask id
	 * 4 Bytes - location length
	 * N Bytes - location string
	 *
	 * @param readerInfo the given reader information to serialize.
	 */
	private static void writeReaderInfo(ReaderInfo readerInfo, ByteArrayOutputStream out) throws IOException {
		writeInt(readerInfo.getSubtaskId(), out);
		writeString(readerInfo.getLocation(), out);
	}

	private static ReaderInfo readReaderInfo(ByteArrayInputStream in) {
		int subtaskId = readInt(in);
		String location = readString(in);
		return new ReaderInfo(subtaskId, location);
	}
}
