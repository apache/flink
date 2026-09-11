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
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;

import javax.annotation.Nullable;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.OptionalLong;

/**
 * V7 serializer that additionally persists the Regional Checkpoint reference id ({@code
 * refCheckpointId}) of each {@link OperatorSubtaskState}.
 *
 * <p>The reference id identifies the historical checkpoint a subtask's state originates from when
 * it was reused by a regional checkpoint (i.e. the subtask belonged to a failed region). It is
 * required by the checkpoint cleaner to protect referenced historical checkpoints from being
 * subsumed.
 *
 * <p><b>Format compatibility:</b> the {@code refCheckpointId} is written as an optional trailing
 * field after the V2/V3 subtask state layout (a presence byte, optionally followed by a {@code
 * long}). This keeps the layout strictly additive: older serializers (V3–V6) never read this field,
 * and their metadata is read back with an empty reference id. Savepoints use {@link
 * MetadataV2Serializer} which does not go through this serializer, so savepoint metadata never
 * contains the reference id.
 */
@Internal
public class MetadataV7Serializer extends MetadataV6Serializer {

    public static final MetadataSerializer INSTANCE = new MetadataV7Serializer();

    public static final int VERSION = 7;

    /** Marker indicating that a {@code refCheckpointId} value follows. */
    private static final byte HAS_REF_CHECKPOINT_ID = 1;

    /** Marker indicating that no {@code refCheckpointId} is present. */
    private static final byte NO_REF_CHECKPOINT_ID = 0;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    protected void serializeSubtaskState(
            OperatorSubtaskState subtaskState, DataOutputStream dos, SerializationContext context)
            throws IOException {
        super.serializeSubtaskState(subtaskState, dos, context);

        // Optional trailing field: the regional checkpoint reference id.
        final OptionalLong refCheckpointId = subtaskState.getRefCheckpointId();
        if (refCheckpointId.isPresent()) {
            dos.writeByte(HAS_REF_CHECKPOINT_ID);
            dos.writeLong(refCheckpointId.getAsLong());
        } else {
            dos.writeByte(NO_REF_CHECKPOINT_ID);
        }
    }

    @Override
    protected OperatorSubtaskState deserializeSubtaskState(
            DataInputStream dis, @Nullable DeserializationContext context) throws IOException {
        final OperatorSubtaskState subtaskState = super.deserializeSubtaskState(dis, context);

        // Optional trailing field: the regional checkpoint reference id.
        final byte marker = dis.readByte();
        if (marker == HAS_REF_CHECKPOINT_ID) {
            final long refCheckpointId = dis.readLong();
            return subtaskState.toBuilder().setRefCheckpointId(refCheckpointId).build();
        }
        return subtaskState;
    }
}
