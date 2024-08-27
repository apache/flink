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

package org.apache.flink.streaming.api.connector.sink2;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.StringUtils;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The serializer to serialize {@link CommittableMessage}s in custom operators.
 *
 * @param <CommT>
 */
@Internal
public class CommittableMessageSerializer<CommT>
        implements SimpleVersionedSerializer<CommittableMessage<CommT>> {

    @VisibleForTesting static final int VERSION = 1;
    private static final int COMMITTABLE = 1;
    private static final int SUMMARY = 2;
    private static final long EOI = Long.MAX_VALUE;

    private final SimpleVersionedSerializer<CommT> committableSerializer;

    public CommittableMessageSerializer(SimpleVersionedSerializer<CommT> committableSerializer) {
        this.committableSerializer = checkNotNull(committableSerializer);
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(CommittableMessage<CommT> obj) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        if (obj instanceof CommittableWithLineage) {
            out.writeByte(COMMITTABLE);
            SimpleVersionedSerialization.writeVersionAndSerialize(
                    committableSerializer,
                    ((CommittableWithLineage<CommT>) obj).getCommittable(),
                    out);
            writeCheckpointId(out, obj);
            out.writeInt(obj.getSubtaskId());
        } else if (obj instanceof CommittableSummary) {
            out.writeByte(SUMMARY);
            out.writeInt(obj.getSubtaskId());
            CommittableSummary<?> committableSummary = (CommittableSummary<?>) obj;
            out.writeInt(committableSummary.getNumberOfSubtasks());
            writeCheckpointId(out, obj);
            out.writeInt(committableSummary.getNumberOfCommittables());
            out.writeInt(committableSummary.getNumberOfPendingCommittables());
            out.writeInt(committableSummary.getNumberOfFailedCommittables());
        } else {
            throw new IllegalArgumentException("Unknown message: " + obj.getClass());
        }
        return out.getCopyOfBuffer();
    }

    @Override
    public CommittableMessage<CommT> deserialize(int version, byte[] serialized)
            throws IOException {
        DataInputDeserializer in = new DataInputDeserializer(serialized);
        byte messageType = in.readByte();
        switch (messageType) {
            case COMMITTABLE:
                return new CommittableWithLineage<>(
                        SimpleVersionedSerialization.readVersionAndDeSerialize(
                                committableSerializer, in),
                        readCheckpointId(in),
                        in.readInt());
            case SUMMARY:
                return new CommittableSummary<>(
                        in.readInt(),
                        in.readInt(),
                        readCheckpointId(in),
                        in.readInt(),
                        in.readInt(),
                        in.readInt());
            default:
                throw new IllegalStateException(
                        "Unexpected message type "
                                + messageType
                                + " in "
                                + StringUtils.byteToHexString(serialized));
        }
    }

    private void writeCheckpointId(DataOutputSerializer out, CommittableMessage<CommT> obj)
            throws IOException {
        out.writeLong(obj.getCheckpointId().orElse(EOI));
    }

    private Long readCheckpointId(DataInputDeserializer in) throws IOException {
        long checkpointId = in.readLong();
        return checkpointId == EOI ? null : checkpointId;
    }
}
