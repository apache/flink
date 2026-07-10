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

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies that a deserialization failure does not leave stale bytes in the {@code BinaryDecoder}'s
 * internal read-ahead buffer, which would corrupt subsequent messages.
 *
 * <p>The bug: {@link RegistryAvroDeserializationSchema} reuses a single {@code BinaryDecoder}
 * across messages. When a decode fails mid-message the decoder's internal buffer retains the
 * unconsumed bytes from the failed message. The next call to {@code setBuffer()} resets the
 * underlying stream but does not clear the decoder buffer, so the next message is decoded starting
 * from those stale bytes and produces wrong results.
 */
class RegistryAvroDeserializationSchemaDecoderResetTest {

    private static final Schema SCHEMA =
            SchemaBuilder.record("Simple")
                    .namespace("test")
                    .fields()
                    .name("id")
                    .type()
                    .intType()
                    .noDefault()
                    .name("name")
                    .type()
                    .stringType()
                    .noDefault()
                    .endRecord();

    @Test
    void testValidMessageDecodedCorrectlyAfterPriorFailure() throws Exception {
        MockSchemaRegistryClient mockClient = new MockSchemaRegistryClient();
        int schemaId = mockClient.register("test", SCHEMA);

        ConfluentSchemaRegistryCoder coder = new ConfluentSchemaRegistryCoder(mockClient);
        RegistryAvroDeserializationSchema<GenericRecord> deserializer =
                new RegistryAvroDeserializationSchema<>(GenericRecord.class, SCHEMA, () -> coder);

        // Message 1: a malformed Avro payload.
        //   - 0x02 encodes id=1 (zigzag int, valid)
        //   - 0x01 encodes string length=-1 (zigzag, invalid -- triggers "Malformed data")
        //   - 10 zero bytes that will linger in the decoder's internal buffer after the failure
        byte[] badPayload = {0x02, 0x01, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0};
        byte[] badMessage = confluentMessage(schemaId, badPayload);

        // Deserialization of the malformed message is expected to fail.
        Exception firstError = null;
        try {
            deserializer.deserialize(badMessage);
        } catch (Exception e) {
            firstError = e;
        }
        assertThat(firstError).as("first (malformed) message must fail").isNotNull();

        // Message 2: a valid Avro payload for the same schema.
        byte[] goodMessage = confluentMessage(schemaId, encodeRecord(42, "hello"));

        // Without the fix the decoder buffer still holds the 10 zero bytes from message 1.
        // Those bytes decode as id=0 and name="" instead of id=42 and name="hello".
        GenericRecord result = deserializer.deserialize(goodMessage);

        assertThat(result.get("id"))
                .as("id must not be corrupted by stale bytes from the prior failure")
                .isEqualTo(42);
        assertThat(result.get("name").toString())
                .as("name must not be corrupted by stale bytes from the prior failure")
                .isEqualTo("hello");
    }

    // -------------------------------------------------------------------------
    //  Helpers
    // -------------------------------------------------------------------------

    /** Wraps an Avro binary payload in the Confluent wire format (magic byte + schema_id). */
    private static byte[] confluentMessage(int schemaId, byte[] avroPayload) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(out);
        dos.writeByte(0);
        dos.writeInt(schemaId);
        dos.flush();
        out.write(avroPayload);
        return out.toByteArray();
    }

    /** Serialises a two-field record using the standard Avro binary encoder. */
    private static byte[] encodeRecord(int id, String name) throws IOException {
        GenericRecord record = new GenericData.Record(SCHEMA);
        record.put("id", id);
        record.put("name", name);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<GenericRecord>(SCHEMA).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }
}
