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

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;

/** A serialization util class for the {@link SourceCoordinator}. */
public class SourceCoordinatorSerdeUtils {

    public static final int VERSION_0 = 0;
    public static final int VERSION_1 = 1;
    public static final int VERSION_2 = 2; // Added tombstone support

    /** The current source coordinator serde version. */
    private static final int CURRENT_VERSION = VERSION_2;

    /** Private constructor for utility class. */
    private SourceCoordinatorSerdeUtils() {}

    /** Write the current serde version. */
    static void writeCoordinatorSerdeVersion(DataOutputStream out) throws IOException {
        out.writeInt(CURRENT_VERSION);
    }

    /** Read and verify the serde version. */
    static int readAndVerifyCoordinatorSerdeVersion(DataInputStream in) throws IOException {
        int version = in.readInt();
        if (version > CURRENT_VERSION) {
            throw new IOException("Unsupported source coordinator serde version " + version);
        }
        return version;
    }

    static byte[] readBytes(DataInputStream in, int size) throws IOException {
        byte[] bytes = new byte[size];
        in.readFully(bytes);
        return bytes;
    }

    static <SplitT> byte[] serializeAssignments(
            Map<Integer, LinkedHashSet<SplitT>> assignments,
            SimpleVersionedSerializer<SplitT> splitSerializer)
            throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputViewStreamWrapper(baos)) {
            out.writeInt(splitSerializer.getVersion());

            int numSubtasks = assignments.size();
            out.writeInt(numSubtasks);
            for (Map.Entry<Integer, LinkedHashSet<SplitT>> assignment : assignments.entrySet()) {
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
            out.flush();
            return baos.toByteArray();
        }
    }

    static <SplitT> Map<Integer, LinkedHashSet<SplitT>> deserializeAssignments(
            byte[] assignmentData, SimpleVersionedSerializer<SplitT> splitSerializer)
            throws IOException {

        try (ByteArrayInputStream bais = new ByteArrayInputStream(assignmentData);
                DataInputStream in = new DataInputViewStreamWrapper(bais)) {
            int splitSerializerVersion = in.readInt();

            int numSubtasks = in.readInt();
            Map<Integer, LinkedHashSet<SplitT>> assignments = new HashMap<>();
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

            return assignments;
        }
    }

    /**
     * Serialize tombstone entries for removed splits.
     *
     * @param tombstones map of split IDs to their removal information
     * @param splitSerializer serializer for split objects
     * @return serialized tombstone data
     */
    static <SplitT extends SourceSplit> byte[] serializeTombstones(
            Map<String, SplitAssignmentTracker.RemovedSplitInfo<SplitT>> tombstones,
            SimpleVersionedSerializer<SplitT> splitSerializer)
            throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputViewStreamWrapper(baos)) {
            out.writeInt(splitSerializer.getVersion());

            int numTombstones = tombstones.size();
            out.writeInt(numTombstones);

            for (Map.Entry<String, SplitAssignmentTracker.RemovedSplitInfo<SplitT>> entry :
                    tombstones.entrySet()) {
                // Write split ID
                out.writeUTF(entry.getKey());

                SplitAssignmentTracker.RemovedSplitInfo<SplitT> info = entry.getValue();

                // Write removal timestamp
                out.writeLong(info.getRemovalTimestamp());

                // Write last assigned subtask ID
                out.writeInt(info.getLastAssignedSubtaskId());

                // Write serialized split
                byte[] serializedSplit = splitSerializer.serialize(info.getSplit());
                out.writeInt(serializedSplit.length);
                out.write(serializedSplit);
            }

            out.flush();
            return baos.toByteArray();
        }
    }

    /**
     * Deserialize tombstone entries for removed splits.
     *
     * @param tombstoneData serialized tombstone data
     * @param splitSerializer serializer for split objects
     * @return map of split IDs to their removal information
     */
    static <SplitT extends SourceSplit>
            Map<String, SplitAssignmentTracker.RemovedSplitInfo<SplitT>> deserializeTombstones(
                    byte[] tombstoneData, SimpleVersionedSerializer<SplitT> splitSerializer)
                    throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(tombstoneData);
                DataInputStream in = new DataInputViewStreamWrapper(bais)) {
            int splitSerializerVersion = in.readInt();

            int numTombstones = in.readInt();
            Map<String, SplitAssignmentTracker.RemovedSplitInfo<SplitT>> tombstones =
                    new HashMap<>(numTombstones);

            for (int i = 0; i < numTombstones; i++) {
                // Read split ID
                String splitId = in.readUTF();

                // Read removal timestamp
                long removalTimestamp = in.readLong();

                // Read last assigned subtask ID
                int lastAssignedSubtaskId = in.readInt();

                // Read serialized split
                int serializedSplitSize = in.readInt();
                byte[] serializedSplit = readBytes(in, serializedSplitSize);
                SplitT split = splitSerializer.deserialize(splitSerializerVersion, serializedSplit);

                tombstones.put(
                        splitId,
                        new SplitAssignmentTracker.RemovedSplitInfo<>(
                                split, removalTimestamp, lastAssignedSubtaskId));
            }

            return tombstones;
        }
    }
}
