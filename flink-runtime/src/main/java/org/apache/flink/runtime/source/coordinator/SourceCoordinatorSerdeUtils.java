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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

/** A serialization util class for the {@link SourceCoordinator}. */
public class SourceCoordinatorSerdeUtils {
    /** The current source coordinator serde version. */
    private static final int CURRENT_VERSION = 0;

    /** Private constructor for utility class. */
    private SourceCoordinatorSerdeUtils() {}

    /** Write the current serde version. */
    static void writeCoordinatorSerdeVersion(DataOutputStream out) throws IOException {
        out.writeInt(CURRENT_VERSION);
    }

    /** Read and verify the serde version. */
    static void readAndVerifyCoordinatorSerdeVersion(DataInputStream in) throws IOException {
        int version = in.readInt();
        if (version > CURRENT_VERSION) {
            throw new IOException("Unsupported source coordinator serde version " + version);
        }
    }

    static Map<Integer, ReaderInfo> readRegisteredReaders(DataInputStream in) throws IOException {
        int numReaders = in.readInt();
        Map<Integer, ReaderInfo> registeredReaders = new HashMap<>();
        for (int i = 0; i < numReaders; i++) {
            ReaderInfo info = readReaderInfo(in);
            registeredReaders.put(info.getSubtaskId(), info);
        }
        return registeredReaders;
    }

    /** Serialize the assignment by checkpoint ids. */
    static <SplitT> void writeAssignmentsByCheckpointId(
            Map<Long, Map<Integer, LinkedHashSet<SplitT>>> assignmentByCheckpointIds,
            SimpleVersionedSerializer<SplitT> splitSerializer,
            DataOutputStream out)
            throws IOException {
        // SplitSerializer version.
        out.writeInt(splitSerializer.getVersion());
        // Num checkpoints.
        out.writeInt(assignmentByCheckpointIds.size());
        for (Map.Entry<Long, Map<Integer, LinkedHashSet<SplitT>>> assignments :
                assignmentByCheckpointIds.entrySet()) {
            long checkpointId = assignments.getKey();
            out.writeLong(checkpointId);

            int numSubtasks = assignments.getValue().size();
            out.writeInt(numSubtasks);
            for (Map.Entry<Integer, LinkedHashSet<SplitT>> assignment :
                    assignments.getValue().entrySet()) {
                int subtaskId = assignment.getKey();
                out.writeInt(subtaskId);

                int numAssignedSplits = assignment.getValue().size();
                out.writeInt(numAssignedSplits);
                for (SplitT split : assignment.getValue()) {
                    byte[] serializedSplit = splitSerializer.serialize(split);
                    out.writeInt(serializedSplit.length);
                    out.write(serializedSplit);
                }
            }
        }
    }

    /** Deserialize the assignment by checkpoint ids. */
    static <SplitT> Map<Long, Map<Integer, LinkedHashSet<SplitT>>> readAssignmentsByCheckpointId(
            DataInputStream in, SimpleVersionedSerializer<SplitT> splitSerializer)
            throws IOException {
        int splitSerializerVersion = in.readInt();
        int numCheckpoints = in.readInt();
        Map<Long, Map<Integer, LinkedHashSet<SplitT>>> assignmentsByCheckpointIds =
                new HashMap<>(numCheckpoints);
        for (int i = 0; i < numCheckpoints; i++) {
            long checkpointId = in.readLong();
            int numSubtasks = in.readInt();
            Map<Integer, LinkedHashSet<SplitT>> assignments = new HashMap<>();
            assignmentsByCheckpointIds.put(checkpointId, assignments);
            for (int j = 0; j < numSubtasks; j++) {
                int subtaskId = in.readInt();
                int numAssignedSplits = in.readInt();
                LinkedHashSet<SplitT> splits = new LinkedHashSet<>(numAssignedSplits);
                assignments.put(subtaskId, splits);
                for (int k = 0; k < numAssignedSplits; k++) {
                    int serializedSplitSize = in.readInt();
                    byte[] serializedSplit = readBytes(in, serializedSplitSize);
                    SplitT split =
                            splitSerializer.deserialize(splitSerializerVersion, serializedSplit);
                    splits.add(split);
                }
            }
        }
        return assignmentsByCheckpointIds;
    }

    static byte[] readBytes(DataInputStream in, int size) throws IOException {
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        return bytes;
    }

    // ----- private helper methods -----

    private static ReaderInfo readReaderInfo(DataInputStream in) throws IOException {
        int subtaskId = in.readInt();
        String location = in.readUTF();
        return new ReaderInfo(subtaskId, location);
    }
}
