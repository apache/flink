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
import org.apache.flink.connector.rabbitmq2.sink.SinkMessage;
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
 * Serializer for a {@link RabbitMQSinkWriterState} used for at-least and exactly-once behaviour of
 * the sink.
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
        return 0;
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
        writeSinkMessages(out, rabbitMQSinkWriterState.getOutstandingMessages());
        out.flush();
        return baos.toByteArray();
    }

    private void writeSinkMessages(DataOutputStream out, List<SinkMessage<T>> messages)
            throws IOException {
        out.writeInt(messages.size());
        for (SinkMessage<T> message : messages) {
            out.writeInt(message.getBytes().length);
            out.write(message.getBytes());
            out.writeInt(message.getRetries());
        }
    }

    /**
     * Deserializes {@link SinkMessage} objects that wrap the byte representation of a message that
     * needs to be delivered to RabbitMQ as well as the original object representation if a
     * deserialization schema is provided.
     *
     * @param i
     * @param bytes Serialized outstanding sink messages
     * @return A list of messages that need to be redelivered to RabbitMQ
     * @throws IOException If input stream cant read the required data
     */
    @Override
    public RabbitMQSinkWriterState<T> deserialize(int i, byte[] bytes) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream in = new DataInputStream(bais);
        return new RabbitMQSinkWriterState<>(readSinkMessages(in));
    }

    private List<SinkMessage<T>> readSinkMessages(DataInputStream in) throws IOException {
        final int numberOfMessages = in.readInt();
        List<SinkMessage<T>> messages = new ArrayList<>();
        for (int i = 0; i < numberOfMessages; i++) {
            byte[] bytes = in.readNBytes(in.readInt());
            int retries = in.readInt();
            // in this case, the messages need to be deserialized again, so we can recompute publish
            // options
            if (deserializationSchema != null) {
                messages.add(
                        new SinkMessage<>(
                                deserializationSchema.deserialize(bytes), bytes, retries));
            } else {
                messages.add(new SinkMessage<>(bytes, retries));
            }
        }
        return messages;
    }
}
