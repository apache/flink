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

package org.apache.flink.connector.pulsar.source.reader.deserializer;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.connector.pulsar.SampleMessage.TestMessage;
import org.apache.flink.connector.testutils.source.deserialization.TestingDeserializationContext;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.types.StringValue;
import org.apache.flink.util.Collector;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.function.FunctionWithException;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.MessageImpl;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.flinkSchema;
import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.flinkTypeInfo;
import static org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema.pulsarSchema;
import static org.apache.flink.util.Preconditions.checkState;
import static org.apache.pulsar.client.api.Schema.PROTOBUF_NATIVE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** Unit tests for {@link PulsarDeserializationSchema}. */
class PulsarDeserializationSchemaTest {

    @Test
    void createFromFlinkDeserializationSchema() throws Exception {
        PulsarDeserializationSchema<String> schema = flinkSchema(new SimpleStringSchema());
        schema.open(new TestingDeserializationContext());
        assertDoesNotThrow(() -> InstantiationUtil.clone(schema));

        Message<byte[]> message = getMessage("some-sample-message", String::getBytes);
        SingleMessageCollector<String> collector = new SingleMessageCollector<>();
        schema.deserialize(message, collector);

        assertNotNull(collector.result);
        assertEquals(collector.result, "some-sample-message");
    }

    @Test
    void createFromPulsarSchema() throws Exception {
        Schema<TestMessage> schema1 = PROTOBUF_NATIVE(TestMessage.class);
        PulsarDeserializationSchema<TestMessage> schema2 = pulsarSchema(schema1, TestMessage.class);
        schema2.open(new TestingDeserializationContext());
        assertDoesNotThrow(() -> InstantiationUtil.clone(schema2));

        TestMessage message1 =
                TestMessage.newBuilder()
                        .setStringField(randomAlphabetic(10))
                        .setDoubleField(ThreadLocalRandom.current().nextDouble())
                        .setIntField(ThreadLocalRandom.current().nextInt())
                        .build();
        Message<byte[]> message2 = getMessage(message1, schema1::encode);
        SingleMessageCollector<TestMessage> collector = new SingleMessageCollector<>();
        schema2.deserialize(message2, collector);

        assertNotNull(collector.result);
        assertEquals(collector.result, message1);
    }

    @Test
    void createFromFlinkTypeInformation() throws Exception {
        PulsarDeserializationSchema<String> schema = flinkTypeInfo(Types.STRING, null);
        schema.open(new TestingDeserializationContext());
        assertDoesNotThrow(() -> InstantiationUtil.clone(schema));

        Message<byte[]> message =
                getMessage(
                        "test-content",
                        s -> {
                            DataOutputSerializer serializer = new DataOutputSerializer(10);
                            StringValue.writeString(s, serializer);
                            return serializer.getSharedBuffer();
                        });
        SingleMessageCollector<String> collector = new SingleMessageCollector<>();
        schema.deserialize(message, collector);

        assertNotNull(collector.result);
        assertEquals(collector.result, "test-content");
    }

    /** Create a test message by given bytes. The message don't contains any meta data. */
    private <T> Message<byte[]> getMessage(
            T message, FunctionWithException<T, byte[], Exception> decoder) throws Exception {
        byte[] bytes = decoder.apply(message);
        MessageMetadata metadata = new MessageMetadata();
        ByteBuffer payload = ByteBuffer.wrap(bytes);

        return MessageImpl.create(metadata, payload, Schema.BYTES, "");
    }

    /** This collector is used for collecting only one message. Used for test purpose. */
    private static class SingleMessageCollector<T> implements Collector<T> {

        private T result;

        @Override
        public void collect(T record) {
            checkState(result == null);
            this.result = record;
        }

        @Override
        public void close() {
            // do nothing
        }
    }
}
