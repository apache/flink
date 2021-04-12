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

package org.apache.flink.connector.base.source.utils;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/** A util class with some helper method for serde in the sources. */
public class SerdeUtils {

    /** Private constructor for util class. */
    private SerdeUtils() {}

    /**
     * Serialize a mapping from subtask ids to lists of assigned splits. The serialized format is
     * following:
     *
     * <pre>
     * 4 bytes - number of subtasks
     * 4 bytes - split serializer version
     * N bytes - [assignment_for_subtask]
     * 		4 bytes - subtask id
     * 		4 bytes - number of assigned splits
     * 		N bytes - [assigned_splits]
     * 			4 bytes - serialized split length
     * 			N bytes - serialized splits
     * </pre>
     *
     * @param splitAssignments a mapping from subtask ids to lists of assigned splits.
     * @param splitSerializer the serializer of the split.
     * @param <SplitT> the type of the splits.
     * @param <C> the type of the collection to hold the assigned splits for a subtask.
     * @return the serialized bytes of the given subtask to splits assignment mapping.
     * @throws IOException when serialization failed.
     */
    public static <SplitT extends SourceSplit, C extends Collection<SplitT>>
            byte[] serializeSplitAssignments(
                    Map<Integer, C> splitAssignments,
                    SimpleVersionedSerializer<SplitT> splitSerializer)
                    throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            out.writeInt(splitAssignments.size());
            // Split serializer version.
            out.writeInt(splitSerializer.getVersion());
            // Write assignments for subtasks.
            for (Map.Entry<Integer, C> entry : splitAssignments.entrySet()) {
                // Subtask ID
                int subtaskId = entry.getKey();
                Collection<SplitT> splitsForSubtask = entry.getValue();
                // Number of the splits.
                out.writeInt(subtaskId);
                out.writeInt(splitsForSubtask.size());
                for (SplitT split : splitsForSubtask) {
                    byte[] serializedSplit = splitSerializer.serialize(split);
                    out.writeInt(serializedSplit.length);
                    out.write(serializedSplit);
                }
            }
            return baos.toByteArray();
        }
    }

    /**
     * Deserialize the given bytes returned by {@link #serializeSplitAssignments(Map,
     * SimpleVersionedSerializer)}.
     *
     * @param serialized the serialized bytes returned by {@link #serializeSplitAssignments(Map,
     *     SimpleVersionedSerializer)}.
     * @param splitSerializer the split serializer for the splits.
     * @param collectionSupplier the supplier for the {@link Collection} instance to hold the
     *     assigned splits for a subtask.
     * @param <SplitT> the type of the splits.
     * @param <C> the type of the collection to hold the assigned splits for a subtask.
     * @return A mapping from subtask id to its assigned splits.
     * @throws IOException when deserialization failed.
     */
    public static <SplitT extends SourceSplit, C extends Collection<SplitT>>
            Map<Integer, C> deserializeSplitAssignments(
                    byte[] serialized,
                    SimpleVersionedSerializer<SplitT> splitSerializer,
                    Function<Integer, C> collectionSupplier)
                    throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            int numSubtasks = in.readInt();
            Map<Integer, C> splitsAssignments = new HashMap<>(numSubtasks);
            int serializerVersion = in.readInt();
            for (int i = 0; i < numSubtasks; i++) {
                int subtaskId = in.readInt();
                int numAssignedSplits = in.readInt();
                C assignedSplits = collectionSupplier.apply(numAssignedSplits);
                for (int j = 0; j < numAssignedSplits; j++) {
                    int serializedSplitSize = in.readInt();
                    byte[] serializedSplit = new byte[serializedSplitSize];
                    in.readFully(serializedSplit);
                    SplitT split = splitSerializer.deserialize(serializerVersion, serializedSplit);
                    assignedSplits.add(split);
                }
                splitsAssignments.put(subtaskId, assignedSplits);
            }
            return splitsAssignments;
        }
    }
}
