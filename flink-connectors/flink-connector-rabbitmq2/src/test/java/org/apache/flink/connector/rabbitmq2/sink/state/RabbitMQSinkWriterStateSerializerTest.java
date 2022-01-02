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

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.rabbitmq2.sink.common.RabbitMQSinkMessageWrapper;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Test the sink writer state serializer. */
public class RabbitMQSinkWriterStateSerializerTest {

    private RabbitMQSinkWriterState<String> getSinkWriterState() {
        List<RabbitMQSinkMessageWrapper<String>> outStandingMessages = new ArrayList<>();
        SimpleStringSchema serializer = new SimpleStringSchema();
        for (int i = 0; i < 5; i++) {
            String message = "Message " + i;
            RabbitMQSinkMessageWrapper<String> messageWrapper =
                    new RabbitMQSinkMessageWrapper<>(message, serializer.serialize(message));
            outStandingMessages.add(messageWrapper);
        }
        return new RabbitMQSinkWriterState<>(outStandingMessages);
    }

    @Test
    public void testWriterStateSerializer() throws IOException {
        RabbitMQSinkWriterState<String> writerState = getSinkWriterState();
        RabbitMQSinkWriterStateSerializer<String> serializer =
                new RabbitMQSinkWriterStateSerializer<>();

        byte[] serializedWriterState = serializer.serialize(writerState);
        RabbitMQSinkWriterState<String> deserializedWriterState =
                serializer.deserialize(serializer.getVersion(), serializedWriterState);

        List<byte[]> expectedBytes =
                writerState.getOutstandingMessages().stream()
                        .map(RabbitMQSinkMessageWrapper::getBytes)
                        .collect(Collectors.toList());
        List<byte[]> actualBytes =
                deserializedWriterState.getOutstandingMessages().stream()
                        .map(RabbitMQSinkMessageWrapper::getBytes)
                        .collect(Collectors.toList());

        assertEquals(expectedBytes.size(), actualBytes.size());
        for (int i = 0; i < expectedBytes.size(); i++) {
            Assert.assertArrayEquals(expectedBytes.get(i), actualBytes.get(i));
        }

        List<String> actualMessages =
                deserializedWriterState.getOutstandingMessages().stream()
                        .map(RabbitMQSinkMessageWrapper::getMessage)
                        .collect(Collectors.toList());

        for (String message : actualMessages) {
            assertNull(message);
        }
    }

    @Test
    public void testWriterStateSerializerWithDeserializationSchema() throws IOException {
        RabbitMQSinkWriterState<String> writerState = getSinkWriterState();
        SimpleStringSchema deserializer = new SimpleStringSchema();
        RabbitMQSinkWriterStateSerializer<String> serializer =
                new RabbitMQSinkWriterStateSerializer<>(deserializer);

        byte[] serializedWriterState = serializer.serialize(writerState);
        RabbitMQSinkWriterState<String> deserializedWriterState =
                serializer.deserialize(serializer.getVersion(), serializedWriterState);

        List<String> expectedMessages =
                writerState.getOutstandingMessages().stream()
                        .map(RabbitMQSinkMessageWrapper::getMessage)
                        .collect(Collectors.toList());
        List<byte[]> expectedBytes =
                writerState.getOutstandingMessages().stream()
                        .map(RabbitMQSinkMessageWrapper::getBytes)
                        .collect(Collectors.toList());

        List<String> actualMessages =
                deserializedWriterState.getOutstandingMessages().stream()
                        .map(RabbitMQSinkMessageWrapper::getMessage)
                        .collect(Collectors.toList());
        List<byte[]> actualBytes =
                deserializedWriterState.getOutstandingMessages().stream()
                        .map(RabbitMQSinkMessageWrapper::getBytes)
                        .collect(Collectors.toList());

        assertEquals(expectedMessages, actualMessages);
        assertEquals(expectedBytes.size(), actualBytes.size());
        for (int i = 0; i < expectedBytes.size(); i++) {
            Assert.assertArrayEquals(expectedBytes.get(i), actualBytes.get(i));
        }
    }
}
