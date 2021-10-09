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
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.checkpoint.FinishedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.FullyFinishedOperatorState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.InputChannelStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.runtime.state.ResultSubpartitionStateHandle;
import org.apache.flink.runtime.state.StateObject;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;
import org.apache.flink.util.function.BiConsumerWithException;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * (De)serializer for checkpoint metadata format version 3. This format was introduced with Apache
 * Flink 1.11.0.
 *
 * <p>Compared to format version 2, this drops some unused fields and introduces operator
 * coordinator state.
 *
 * <p>See {@link MetadataV2V3SerializerBase} for a description of the format layout.
 */
@Internal
public class MetadataV3Serializer extends MetadataV2V3SerializerBase implements MetadataSerializer {

    /** The metadata format version. */
    public static final int VERSION = 3;

    /** The singleton instance of the serializer. */
    public static final MetadataV3Serializer INSTANCE = new MetadataV3Serializer();

    private final ChannelStateHandleSerializer channelStateHandleSerializer =
            new ChannelStateHandleSerializer();

    /** Singleton, not meant to be instantiated. */
    private MetadataV3Serializer() {}

    @Override
    public int getVersion() {
        return VERSION;
    }

    // ------------------------------------------------------------------------
    //  (De)serialization entry points
    // ------------------------------------------------------------------------

    public static void serialize(CheckpointMetadata checkpointMetadata, DataOutputStream dos)
            throws IOException {
        INSTANCE.serializeMetadata(checkpointMetadata, dos);
    }

    @Override
    public CheckpointMetadata deserialize(
            DataInputStream dis, ClassLoader classLoader, String externalPointer)
            throws IOException {
        return deserializeMetadata(dis, externalPointer);
    }

    // ------------------------------------------------------------------------
    //  version-specific serialization formats
    // ------------------------------------------------------------------------

    @Override
    protected void serializeOperatorState(OperatorState operatorState, DataOutputStream dos)
            throws IOException {
        // Operator ID
        dos.writeLong(operatorState.getOperatorID().getLowerPart());
        dos.writeLong(operatorState.getOperatorID().getUpperPart());

        // Parallelism
        dos.writeInt(operatorState.getParallelism());
        dos.writeInt(operatorState.getMaxParallelism());

        // Coordinator state
        serializeStreamStateHandle(operatorState.getCoordinatorState(), dos);

        // Sub task states
        if (operatorState.isFullyFinished()) {
            dos.writeInt(-1);
        } else {
            final Map<Integer, OperatorSubtaskState> subtaskStateMap =
                    operatorState.getSubtaskStates();
            dos.writeInt(subtaskStateMap.size());
            for (Map.Entry<Integer, OperatorSubtaskState> entry : subtaskStateMap.entrySet()) {
                boolean isFinished = entry.getValue().isFinished();
                serializeSubtaskIndexAndFinishedState(entry.getKey(), isFinished, dos);
                if (!isFinished) {
                    serializeSubtaskState(entry.getValue(), dos);
                }
            }
        }
    }

    private void serializeSubtaskIndexAndFinishedState(
            int subtaskIndex, boolean isFinished, DataOutputStream dos) throws IOException {
        if (isFinished) {
            // We store a negative index for the finished subtask. In consideration
            // of the index 0, the negative index would start from -1.
            dos.writeInt(-(subtaskIndex + 1));
        } else {
            dos.writeInt(subtaskIndex);
        }
    }

    @Override
    protected void serializeSubtaskState(OperatorSubtaskState subtaskState, DataOutputStream dos)
            throws IOException {
        super.serializeSubtaskState(subtaskState, dos);
        serializeCollection(
                subtaskState.getInputChannelState(), dos, this::serializeInputChannelStateHandle);
        serializeCollection(
                subtaskState.getResultSubpartitionState(),
                dos,
                this::serializeResultSubpartitionStateHandle);
    }

    @Override
    protected OperatorState deserializeOperatorState(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {
        final OperatorID jobVertexId = new OperatorID(dis.readLong(), dis.readLong());
        final int parallelism = dis.readInt();
        final int maxParallelism = dis.readInt();

        ByteStreamStateHandle coordinateState =
                deserializeAndCheckByteStreamStateHandle(dis, context);

        final int numSubTaskStates = dis.readInt();
        if (numSubTaskStates < 0) {
            checkState(
                    coordinateState == null,
                    "Coordinator State should be null for fully finished operator state");
            return new FullyFinishedOperatorState(jobVertexId, parallelism, maxParallelism);
        }

        final OperatorState operatorState =
                new OperatorState(jobVertexId, parallelism, maxParallelism);

        // Coordinator state
        operatorState.setCoordinatorState(coordinateState);

        // Sub task states
        for (int j = 0; j < numSubTaskStates; j++) {
            SubtaskAndFinishedState subtaskAndFinishedState =
                    deserializeSubtaskIndexAndFinishedState(dis);
            if (subtaskAndFinishedState.isFinished) {
                operatorState.putState(
                        subtaskAndFinishedState.subtaskIndex,
                        FinishedOperatorSubtaskState.INSTANCE);
            } else {
                final OperatorSubtaskState subtaskState = deserializeSubtaskState(dis, context);
                operatorState.putState(subtaskAndFinishedState.subtaskIndex, subtaskState);
            }
        }

        return operatorState;
    }

    private SubtaskAndFinishedState deserializeSubtaskIndexAndFinishedState(DataInputStream dis)
            throws IOException {
        int storedSubtaskIndex = dis.readInt();
        if (storedSubtaskIndex < 0) {
            return new SubtaskAndFinishedState(-storedSubtaskIndex - 1, true);
        } else {
            return new SubtaskAndFinishedState(storedSubtaskIndex, false);
        }
    }

    @VisibleForTesting
    @Override
    public void serializeResultSubpartitionStateHandle(
            ResultSubpartitionStateHandle handle, DataOutputStream dos) throws IOException {
        channelStateHandleSerializer.serialize(handle, dos);
    }

    @VisibleForTesting
    @Override
    public StateObjectCollection<ResultSubpartitionStateHandle>
            deserializeResultSubpartitionStateHandle(
                    DataInputStream dis, @Nullable DeserializationContext context)
                    throws IOException {
        return deserializeCollection(
                dis,
                context,
                channelStateHandleSerializer::deserializeResultSubpartitionStateHandle);
    }

    @VisibleForTesting
    @Override
    public void serializeInputChannelStateHandle(
            InputChannelStateHandle handle, DataOutputStream dos) throws IOException {
        channelStateHandleSerializer.serialize(handle, dos);
    }

    @VisibleForTesting
    @Override
    public StateObjectCollection<InputChannelStateHandle> deserializeInputChannelStateHandle(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {
        return deserializeCollection(
                dis, context, channelStateHandleSerializer::deserializeInputChannelStateHandle);
    }

    private <T extends StateObject> void serializeCollection(
            StateObjectCollection<T> stateObjectCollection,
            DataOutputStream dos,
            BiConsumerWithException<T, DataOutputStream, IOException> cons)
            throws IOException {
        if (stateObjectCollection == null) {
            dos.writeInt(0);
        } else {
            dos.writeInt(stateObjectCollection.size());
            for (T stateObject : stateObjectCollection) {
                cons.accept(stateObject, dos);
            }
        }
    }

    // ------------------------------------------------------------------------
    //  exposed static methods for test cases
    //
    //  NOTE: The fact that certain tests directly call these lower level
    //        serialization methods is a problem, because that way the tests
    //        bypass the versioning scheme. Especially tests that test for
    //        cross-version compatibility need to version themselves if we
    //        ever break the format of these low level state types.
    // ------------------------------------------------------------------------

    @VisibleForTesting
    public static void serializeStreamStateHandle(
            StreamStateHandle stateHandle, DataOutputStream dos) throws IOException {
        MetadataV2V3SerializerBase.serializeStreamStateHandle(stateHandle, dos);
    }

    @VisibleForTesting
    public static StreamStateHandle deserializeStreamStateHandle(DataInputStream dis)
            throws IOException {
        return MetadataV2V3SerializerBase.deserializeStreamStateHandle(dis, null);
    }

    @VisibleForTesting
    public static void serializeOperatorStateHandleUtil(
            OperatorStateHandle stateHandle, DataOutputStream dos) throws IOException {
        INSTANCE.serializeOperatorStateHandle(stateHandle, dos);
    }

    @VisibleForTesting
    public static OperatorStateHandle deserializeOperatorStateHandleUtil(DataInputStream dis)
            throws IOException {
        return INSTANCE.deserializeOperatorStateHandle(dis, null);
    }

    @VisibleForTesting
    public static void serializeKeyedStateHandleUtil(
            KeyedStateHandle stateHandle, DataOutputStream dos) throws IOException {
        INSTANCE.serializeKeyedStateHandle(stateHandle, dos);
    }

    @VisibleForTesting
    public static KeyedStateHandle deserializeKeyedStateHandleUtil(DataInputStream dis)
            throws IOException {
        return INSTANCE.deserializeKeyedStateHandle(dis, null);
    }

    @VisibleForTesting
    public static StateObjectCollection<InputChannelStateHandle> deserializeInputChannelStateHandle(
            DataInputStream dis) throws IOException {
        return INSTANCE.deserializeInputChannelStateHandle(dis, null);
    }

    @VisibleForTesting
    public StateObjectCollection<ResultSubpartitionStateHandle>
            deserializeResultSubpartitionStateHandle(DataInputStream dis) throws IOException {
        return INSTANCE.deserializeResultSubpartitionStateHandle(dis, null);
    }

    private static class SubtaskAndFinishedState {

        final int subtaskIndex;

        final boolean isFinished;

        public SubtaskAndFinishedState(int subtaskIndex, boolean isFinished) {
            this.subtaskIndex = subtaskIndex;
            this.isFinished = isFinished;
        }
    }
}
