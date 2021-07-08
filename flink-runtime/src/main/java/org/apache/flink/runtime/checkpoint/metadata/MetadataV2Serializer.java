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

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * (De)serializer for checkpoint metadata format version 2. This format was introduced with Apache
 * Flink 1.3.0.
 *
 * <p>See {@link MetadataV2V3SerializerBase} for a description of the format layout.
 */
@Internal
public class MetadataV2Serializer extends MetadataV2V3SerializerBase implements MetadataSerializer {

    /** The metadata format version. */
    public static final int VERSION = 2;

    /** The singleton instance of the serializer. */
    public static final MetadataV2Serializer INSTANCE = new MetadataV2Serializer();

    /** Singleton, not meant to be instantiated. */
    private MetadataV2Serializer() {}

    @Override
    public int getVersion() {
        return VERSION;
    }

    // ------------------------------------------------------------------------
    //  Deserialization entry point
    // ------------------------------------------------------------------------

    @Override
    public CheckpointMetadata deserialize(
            DataInputStream dis, ClassLoader classLoader, String externalPointer)
            throws IOException {
        return deserializeMetadata(dis, externalPointer);
    }

    // ------------------------------------------------------------------------
    //  version-specific serialization
    // ------------------------------------------------------------------------

    @Override
    protected void serializeOperatorState(OperatorState operatorState, DataOutputStream dos)
            throws IOException {
        checkState(
                !operatorState.isFullyFinished(),
                "Could not support finished Operator state in state serializers.");

        // Operator ID
        dos.writeLong(operatorState.getOperatorID().getLowerPart());
        dos.writeLong(operatorState.getOperatorID().getUpperPart());

        // Parallelism
        int parallelism = operatorState.getParallelism();
        dos.writeInt(parallelism);
        dos.writeInt(operatorState.getMaxParallelism());

        // this field was "chain length" before Flink 1.3, and it is still part
        // of the format, despite being unused
        dos.writeInt(1);

        // Sub task states
        Map<Integer, OperatorSubtaskState> subtaskStateMap = operatorState.getSubtaskStates();
        dos.writeInt(subtaskStateMap.size());
        for (Map.Entry<Integer, OperatorSubtaskState> entry : subtaskStateMap.entrySet()) {
            dos.writeInt(entry.getKey());
            serializeSubtaskState(entry.getValue(), dos);
        }
    }

    @Override
    protected OperatorState deserializeOperatorState(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {
        final OperatorID jobVertexId = new OperatorID(dis.readLong(), dis.readLong());
        final int parallelism = dis.readInt();
        final int maxParallelism = dis.readInt();

        // this field was "chain length" before Flink 1.3, and it is still part
        // of the format, despite being unused
        dis.readInt();

        // Add task state
        final OperatorState taskState = new OperatorState(jobVertexId, parallelism, maxParallelism);

        // Sub task states
        final int numSubTaskStates = dis.readInt();

        for (int j = 0; j < numSubTaskStates; j++) {
            final int subtaskIndex = dis.readInt();
            final OperatorSubtaskState subtaskState = deserializeSubtaskState(dis, context);
            taskState.putState(subtaskIndex, subtaskState);
        }

        return taskState;
    }

    @Override
    protected void serializeSubtaskState(OperatorSubtaskState subtaskState, DataOutputStream dos)
            throws IOException {
        // write two unused fields for compatibility:
        //   - "duration"
        //   - number of legacy states
        dos.writeLong(-1);
        dos.writeInt(0);

        super.serializeSubtaskState(subtaskState, dos);
    }

    @Override
    protected OperatorSubtaskState deserializeSubtaskState(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {
        // read two unused fields for compatibility:
        //   - "duration"
        //   - number of legacy states
        dis.readLong();
        final int numLegacyTaskStates = dis.readInt();

        if (numLegacyTaskStates > 0) {
            throw new IOException(
                    "Legacy state (from Flink <= 1.1, created through the 'Checkpointed' interface) is "
                            + "no longer supported.");
        }

        return super.deserializeSubtaskState(dis, context);
    }
}
