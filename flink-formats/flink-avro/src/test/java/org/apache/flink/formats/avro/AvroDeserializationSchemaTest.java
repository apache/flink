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

package org.apache.flink.formats.avro;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.formats.avro.AvroFormatOptions.AvroEncoding;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.UnionLogicalType;
import org.apache.flink.formats.avro.utils.TestDataGenerator;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.Encoder;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.ByteArrayOutputStream;
import java.time.Instant;
import java.util.Random;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.createEncoder;
import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link AvroDeserializationSchema}. */
class AvroDeserializationSchemaTest {

    private static final Address address = TestDataGenerator.generateRandomAddress(new Random());

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testNullRecord(AvroEncoding encoding) throws Exception {
        DeserializationSchema<Address> deserializer =
                AvroDeserializationSchema.forSpecific(Address.class, encoding);

        Address deserializedAddress = deserializer.deserialize(null);
        assertThat(deserializedAddress).isNull();
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testGenericRecord(AvroEncoding encoding) throws Exception {
        DeserializationSchema<GenericRecord> deserializationSchema =
                AvroDeserializationSchema.forGeneric(address.getSchema(), encoding);

        byte[] encodedAddress = writeRecord(address, Address.getClassSchema(), encoding);
        GenericRecord genericRecord = deserializationSchema.deserialize(encodedAddress);
        assertThat(genericRecord.get("city").toString()).isEqualTo(address.getCity());
        assertThat(genericRecord.get("num")).isEqualTo(address.getNum());
        assertThat(genericRecord.get("state").toString()).isEqualTo(address.getState());
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testSpecificRecord(AvroEncoding encoding) throws Exception {
        DeserializationSchema<Address> deserializer =
                AvroDeserializationSchema.forSpecific(Address.class, encoding);

        byte[] encodedAddress = writeRecord(address, encoding);
        Address deserializedAddress = deserializer.deserialize(encodedAddress);
        assertThat(deserializedAddress).isEqualTo(address);
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testSpecificRecordWithUnionLogicalType(AvroEncoding encoding) throws Exception {
        Random rnd = new Random();
        UnionLogicalType data = new UnionLogicalType(Instant.ofEpochMilli(rnd.nextLong()));
        DeserializationSchema<UnionLogicalType> deserializer =
                AvroDeserializationSchema.forSpecific(UnionLogicalType.class, encoding);

        byte[] encodedData = writeRecord(data, encoding);
        UnionLogicalType deserializedData = deserializer.deserialize(encodedData);
        assertThat(deserializedData).isEqualTo(data);
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testFastReadWithGenericRecord(AvroEncoding encoding) throws Exception {
        // Create a schema with multiple fields
        Schema schema =
                SchemaBuilder.record("TestRecord")
                        .namespace("org.apache.flink.formats.avro.test")
                        .fields()
                        .requiredString("name")
                        .requiredInt("age")
                        .requiredDouble("score")
                        .endRecord();

        // Create a GenericRecord with test data
        GenericRecord record = new GenericData.Record(schema);
        record.put("name", "Alice");
        record.put("age", 30);
        record.put("score", 95.5);

        // Serialize the record
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GenericDatumWriter<IndexedRecord> datumWriter = new GenericDatumWriter<>(schema);
        Encoder encoder = createEncoder(encoding, schema, outputStream);
        datumWriter.write(record, encoder);
        encoder.flush();
        byte[] encodedData = outputStream.toByteArray();

        // Create deserializer with fast read enabled
        DeserializationSchema<GenericRecord> deserializer =
                AvroDeserializationSchema.forGeneric(null, schema, encoding, null, true);

        // Deserialize and verify
        GenericRecord deserializedRecord = deserializer.deserialize(encodedData);
        assertThat(deserializedRecord).isNotNull();
        assertThat(deserializedRecord.get("name").toString()).isEqualTo("Alice");
        assertThat(deserializedRecord.get("age")).isEqualTo(30);
        assertThat(deserializedRecord.get("score")).isEqualTo(95.5);
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testFastReadWithSpecificRecord(AvroEncoding encoding) throws Exception {
        // Use the existing Address test data
        Address address = TestDataGenerator.generateRandomAddress(new Random(42));

        // Serialize the address
        byte[] encodedAddress = writeRecord(address, encoding);

        // Create deserializer with fast read enabled
        DeserializationSchema<Address> deserializer =
                new AvroDeserializationSchema<>(Address.class, null, null, encoding, null, true);

        // Deserialize and verify
        Address deserializedAddress = deserializer.deserialize(encodedAddress);
        assertThat(deserializedAddress).isEqualTo(address);
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testColumnPruningWithGenericRecord(AvroEncoding encoding) throws Exception {
        // Create a full schema with 5 fields
        Schema fullSchema =
                SchemaBuilder.record("FullRecord")
                        .namespace("org.apache.flink.formats.avro.test")
                        .fields()
                        .requiredLong("id")
                        .requiredString("name")
                        .requiredInt("age")
                        .requiredString("email")
                        .requiredDouble("score")
                        .endRecord();

        // Create a projected schema with only 3 fields (id, name, score)
        Schema projectedSchema =
                SchemaBuilder.record("ProjectedRecord")
                        .namespace("org.apache.flink.formats.avro.test")
                        .fields()
                        .requiredLong("id")
                        .requiredString("name")
                        .requiredDouble("score")
                        .endRecord();

        // Create a GenericRecord with all 5 fields
        GenericRecord fullRecord = new GenericData.Record(fullSchema);
        fullRecord.put("id", 123L);
        fullRecord.put("name", "Bob");
        fullRecord.put("age", 25);
        fullRecord.put("email", "bob@example.com");
        fullRecord.put("score", 88.5);

        // Serialize the full record
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GenericDatumWriter<IndexedRecord> datumWriter = new GenericDatumWriter<>(fullSchema);
        Encoder encoder = createEncoder(encoding, fullSchema, outputStream);
        datumWriter.write(fullRecord, encoder);
        encoder.flush();
        byte[] encodedData = outputStream.toByteArray();

        // Create deserializer with column pruning (writer schema = full, reader schema
        // = projected)
        DeserializationSchema<GenericRecord> deserializer =
                AvroDeserializationSchema.forGeneric(
                        fullSchema, projectedSchema, encoding, null, false);

        // Deserialize and verify only projected fields are present
        GenericRecord deserializedRecord = deserializer.deserialize(encodedData);
        assertThat(deserializedRecord).isNotNull();
        assertThat(deserializedRecord.get("id")).isEqualTo(123L);
        assertThat(deserializedRecord.get("name").toString()).isEqualTo("Bob");
        assertThat(deserializedRecord.get("score")).isEqualTo(88.5);

        // Verify that the projected schema only has 3 fields
        assertThat(deserializedRecord.getSchema().getFields()).hasSize(3);
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testFastReadAndColumnPruningCombined(AvroEncoding encoding) throws Exception {
        // Create a full schema with 4 fields
        Schema fullSchema =
                SchemaBuilder.record("FullRecord")
                        .namespace("org.apache.flink.formats.avro.test")
                        .fields()
                        .requiredLong("id")
                        .requiredString("name")
                        .requiredInt("age")
                        .requiredDouble("score")
                        .endRecord();

        // Create a projected schema with only 2 fields (id, score)
        Schema projectedSchema =
                SchemaBuilder.record("ProjectedRecord")
                        .namespace("org.apache.flink.formats.avro.test")
                        .fields()
                        .requiredLong("id")
                        .requiredDouble("score")
                        .endRecord();

        // Create a GenericRecord with all 4 fields
        GenericRecord fullRecord = new GenericData.Record(fullSchema);
        fullRecord.put("id", 789L);
        fullRecord.put("name", "David");
        fullRecord.put("age", 35);
        fullRecord.put("score", 87.3);

        // Serialize the full record
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        GenericDatumWriter<IndexedRecord> datumWriter = new GenericDatumWriter<>(fullSchema);
        Encoder encoder = createEncoder(encoding, fullSchema, outputStream);
        datumWriter.write(fullRecord, encoder);
        encoder.flush();
        byte[] encodedData = outputStream.toByteArray();

        // Create deserializer with both fast read and column pruning enabled
        DeserializationSchema<GenericRecord> deserializer =
                AvroDeserializationSchema.forGeneric(
                        fullSchema, projectedSchema, encoding, null, true);

        // Deserialize and verify
        GenericRecord deserializedRecord = deserializer.deserialize(encodedData);
        assertThat(deserializedRecord).isNotNull();
        assertThat(deserializedRecord.getSchema().getFields()).hasSize(2);
        assertThat(deserializedRecord.get("id")).isEqualTo(789L);
        assertThat(deserializedRecord.get("score")).isEqualTo(87.3);
    }
}
