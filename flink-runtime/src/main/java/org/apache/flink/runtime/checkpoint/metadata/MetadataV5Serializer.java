/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.metadata;

import org.apache.flink.annotation.Internal;
import org.apache.flink.runtime.checkpoint.FinishedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.FullyFinishedOperatorState;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.memory.ByteStreamStateHandle;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkState;

/** V5 serializer that adds Operator name and uid. */
@Internal
public class MetadataV5Serializer extends MetadataV4Serializer {

    public static final MetadataSerializer INSTANCE = new MetadataV5Serializer();
    public static final int VERSION = 5;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    protected void serializeOperatorState(OperatorState operatorState, DataOutputStream dos)
            throws IOException {
        if (operatorState.getOperatorName().isPresent()
                && operatorState.getOperatorName().get().isEmpty()) {
            throw new IllegalArgumentException("Empty string operator name is not allowed");
        }
        if (operatorState.getOperatorUid().isPresent()
                && operatorState.getOperatorUid().get().isEmpty()) {
            throw new IllegalArgumentException("Empty string operator uid is not allowed");
        }
        // Name and UID are null in the whole chain when not provided, but since we can store
        // strings in metadata we do conversion here
        dos.writeUTF(operatorState.getOperatorName().orElse(""));
        dos.writeUTF(operatorState.getOperatorUid().orElse(""));
        super.serializeOperatorState(operatorState, dos);
    }

    @Override
    protected OperatorState deserializeOperatorState(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {
        final String name = dis.readUTF();
        final String uid = dis.readUTF();

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
            return new FullyFinishedOperatorState(
                    name.isEmpty() ? null : name,
                    uid.isEmpty() ? null : uid,
                    jobVertexId,
                    parallelism,
                    maxParallelism);
        }

        // Name and UID are null in the whole chain when not provided, but since we can store
        // strings in metadata we do conversion here
        final OperatorState operatorState =
                new OperatorState(
                        name.isEmpty() ? null : name,
                        uid.isEmpty() ? null : uid,
                        jobVertexId,
                        parallelism,
                        maxParallelism);

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
}
