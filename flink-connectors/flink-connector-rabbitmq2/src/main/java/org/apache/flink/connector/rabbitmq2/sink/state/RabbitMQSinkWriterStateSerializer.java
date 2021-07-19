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

package org.apache.flink.connector.rabbitmq2.sink.state;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkMessageWrapper;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import javax.annotation.Nullable;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serializer for a {@link RabbitMQSinkWriterState} used for at-least and exactly-once consistency
 * of the sink.
 */
public class RabbitMQSinkWriterStateSerializer<T>
        implements SimpleVersionedSerializer<RabbitMQSinkWriterState<T>> {
    private final DeserializationSchema<T> deserializationSchema;

    public RabbitMQSinkWriterStateSerializer(
            @Nullable DeserializationSchema<T> deserializationSchema) {
        this.deserializationSchema = deserializationSchema;
    }

    public RabbitMQSinkWriterStateSerializer() {
        this(null);
    }

    @Override
    public int getVersion() {
        return 1;
    }

    /**
     * Serializes all {@code outstandingMessages} of a state of a single sink writer.
     *
     * @param rabbitMQSinkWriterState A state containing a list of {@code outstandingMessages}
     * @throws IOException If output stream cant write the required data
     */
    @Override
    public byte[] serialize(RabbitMQSinkWriterState<T> rabbitMQSinkWriterState) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        serializeV1(out, rabbitMQSinkWriterState.getOutstandingMessages());
        return baos.toByteArray();
    }

    private void serializeV1(DataOutputStream out, List<RabbitMQSinkMessageWrapper<T>> messages)
            throws IOException {
        out.writeInt(messages.size());
        for (RabbitMQSinkMessageWrapper<T> message : messages) {
            out.writeInt(message.getBytes().length);
            out.write(message.getBytes());
        }
        out.flush();
    }

    /**
     * Deserializes {@link RabbitMQSinkMessageWrapper} objects that wrap the byte representation of
     * a message that needs to be delivered to RabbitMQ as well as the original object
     * representation if a deserialization schema is provided.
     *
     * @param version which deserialization version should be used
     * @param bytes Serialized outstanding sink messages
     * @return A list of messages that need to be redelivered to RabbitMQ
     * @throws IOException If input stream cant read the required data
     */
    @Override
    public RabbitMQSinkWriterState<T> deserialize(int version, byte[] bytes) throws IOException {
        switch (version) {
            case 1:
                return deserializeV1(bytes);
            default:
                throw new IOException("Unrecognized version or corrupt state: " + version);
        }
    }

    private RabbitMQSinkWriterState<T> deserializeV1(byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        return new RabbitMQSinkWriterState<>(readSinkMessages(in));
    }

    private List<RabbitMQSinkMessageWrapper<T>> readSinkMessages(DataInputStream in)
            throws IOException {
        final int numberOfMessages = in.readInt();
        List<RabbitMQSinkMessageWrapper<T>> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            byte[] bytes = new byte[in.readInt()];
            in.read(bytes);

            // In this case, the messages need to be deserialized again, so we can recompute publish
            // options
            if (deserializationSchema != null) {
                messages.add(
                        new RabbitMQSinkMessageWrapper<>(
                                deserializationSchema.deserialize(bytes), bytes));
            } else {
                messages.add(new RabbitMQSinkMessageWrapper<>(bytes));
            }
        }
        return messages;
    }
}
