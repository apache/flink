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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.util.StringUtils;

import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

interface SinkMessage {
    long EOF = Long.MAX_VALUE;

    int getSubtaskId();

    long getCheckpointId();

    class Serializer<CommT> implements SimpleVersionedSerializer<SinkMessage> {
        public static final int COMMITTABLE = 1;
        public static final int SUMMARY = 2;
        private final SimpleVersionedSerializer<CommT> committableSerializer;

        Serializer(SimpleVersionedSerializer<CommT> committableSerializer) {
            this.committableSerializer = checkNotNull(committableSerializer);
        }

        @Override
        public int getVersion() {
            return COMMITTABLE;
        }

        @Override
        public byte[] serialize(SinkMessage obj) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            if (obj instanceof CommittableWrapper) {
                out.writeByte(COMMITTABLE);
                SimpleVersionedSerialization.writeVersionAndSerialize(
                        committableSerializer,
                        ((CommittableWrapper<CommT>) obj).getCommittable(),
                        out);
                out.writeInt(obj.getSubtaskId());
                out.writeInt(((CommittableWrapper<?>) obj).getCommittableIndex());
                out.writeLong(obj.getCheckpointId());
            } else if (obj instanceof CheckpointSummary) {
                out.writeByte(SUMMARY);
                out.writeInt(obj.getSubtaskId());
                out.writeInt(((CheckpointSummary) obj).getNumberOfSubtasks());
                out.writeLong(obj.getCheckpointId());
                out.writeInt(((CheckpointSummary) obj).getNumberCommittablesOfSubtask());
            } else {
                throw new IllegalArgumentException("Unknown message: " + obj.getClass());
            }
            return out.getCopyOfBuffer();
        }

        @Override
        public SinkMessage deserialize(int version, byte[] serialized) throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            byte messageType = in.readByte();
            switch (messageType) {
                case COMMITTABLE:
                    return new CommittableWrapper<>(
                            SimpleVersionedSerialization.readVersionAndDeSerialize(
                                    committableSerializer, in),
                            in.readInt(),
                            in.readInt(),
                            in.readLong());
                case SUMMARY:
                    return new CheckpointSummary(
                            in.readInt(), in.readInt(), in.readLong(), in.readInt());
                default:
                    throw new IllegalStateException(
                            "Unexpected message type "
                                    + messageType
                                    + " in "
                                    + StringUtils.byteToHexString(serialized));
            }
        }
    }
}
