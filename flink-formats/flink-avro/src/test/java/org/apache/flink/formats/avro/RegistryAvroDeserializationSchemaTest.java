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

import org.apache.flink.formats.avro.AvroFormatOptions.AvroEncoding;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.SimpleRecord;
import org.apache.flink.formats.avro.utils.TestDataGenerator;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecord;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RegistryAvroDeserializationSchema}. */
class RegistryAvroDeserializationSchemaTest {

    private static final Address address = TestDataGenerator.generateRandomAddress(new Random());

    @Test
    void testGenericRecordReadWithCompatibleSchema() throws IOException {
        RegistryAvroDeserializationSchema<GenericRecord> deserializer =
                new RegistryAvroDeserializationSchema<>(
                        GenericRecord.class,
                        SchemaBuilder.record("Address")
                                .fields()
                                .requiredString("street")
                                .requiredInt("num")
                                .optionalString("country")
                                .endRecord(),
                        () ->
                                new SchemaCoder() {
                                    @Override
                                    public Schema readSchema(InputStream in) {
                                        return Address.getClassSchema();
                                    }

                                    @Override
                                    public void writeSchema(Schema schema, OutputStream out)
                                            throws IOException {
                                        // do nothing
                                    }
                                });

        GenericRecord genericRecord =
                deserializer.deserialize(writeRecord(address, Address.getClassSchema()));
        assertThat(genericRecord.get("num")).isEqualTo(address.getNum());
        assertThat(genericRecord.get("street").toString()).isEqualTo(address.getStreet());
        assertThat(genericRecord.get("country")).isNull();
    }

    @ParameterizedTest
    @EnumSource(AvroEncoding.class)
    void testSpecificRecordReadMoreFieldsThanWereWritten(AvroEncoding encoding) throws IOException {
        Schema smallerUserSchema =
                new Schema.Parser()
                        .parse(
                                "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                        + " \"type\": \"record\",\n"
                                        + " \"name\": \"SimpleRecord\",\n"
                                        + " \"fields\": [\n"
                                        + "     {\"name\": \"name\", \"type\": \"string\"}"
                                        + " ]\n"
                                        + "}");
        RegistryAvroDeserializationSchema<SimpleRecord> deserializer =
                new RegistryAvroDeserializationSchema<>(
                        SimpleRecord.class,
                        null,
                        () ->
                                new SchemaCoder() {
                                    @Override
                                    public Schema readSchema(InputStream in) {
                                        return smallerUserSchema;
                                    }

                                    @Override
                                    public void writeSchema(Schema schema, OutputStream out)
                                            throws IOException {
                                        // Do nothing
                                    }
                                },
                        encoding);

        GenericData.Record smallUser =
                new GenericRecordBuilder(smallerUserSchema).set("name", "someName").build();

        SimpleRecord simpleRecord =
                deserializer.deserialize(writeRecord(smallUser, smallerUserSchema, encoding));

        assertThat(simpleRecord.getName().toString()).isEqualTo("someName");
        assertThat(simpleRecord.getOptionalField()).isNull();
    }

    @Test
    void testNestedRecordWithEnumField() throws IOException {
        // Writer schema with nested record containing enum field
        // This pattern is common in CDC tools and schema registries
        Schema writerSchema =
                new Schema.Parser()
                        .parse(
                                "{\"namespace\": \"example.avro\",\n"
                                        + " \"type\": \"record\",\n"
                                        + " \"name\": \"Message\",\n"
                                        + " \"fields\": [\n"
                                        + "     {\"name\": \"metadata\", \"type\": {\n"
                                        + "         \"type\": \"record\",\n"
                                        + "         \"name\": \"Metadata\",\n"
                                        + "         \"fields\": [\n"
                                        + "             {\"name\": \"operation\", \"type\": {\n"
                                        + "                 \"type\": \"enum\",\n"
                                        + "                 \"name\": \"Operation\",\n"
                                        + "                 \"symbols\": [\"INSERT\", \"UPDATE\", \"DELETE\", \"REFRESH\"]\n"
                                        + "             }},\n"
                                        + "             {\"name\": \"timestamp\", \"type\": \"string\"}\n"
                                        + "         ]\n"
                                        + "     }}\n"
                                        + "  ]\n"
                                        + "}");

        // Reader schema with operation as string (simulates Flink DDL with VARCHAR)
        Schema readerSchema =
                new Schema.Parser()
                        .parse(
                                "{\"namespace\": \"example.avro\",\n"
                                        + " \"type\": \"record\",\n"
                                        + " \"name\": \"Message\",\n"
                                        + " \"fields\": [\n"
                                        + "     {\"name\": \"metadata\", \"type\": {\n"
                                        + "         \"type\": \"record\",\n"
                                        + "         \"name\": \"Metadata\",\n"
                                        + "         \"fields\": [\n"
                                        + "             {\"name\": \"operation\", \"type\": \"string\"},\n"
                                        + "             {\"name\": \"timestamp\", \"type\": \"string\"}\n"
                                        + "         ]\n"
                                        + "     }}\n"
                                        + "  ]\n"
                                        + "}");

        // Create deserializer with reader schema (enum -> string incompatibility)
        RegistryAvroDeserializationSchema<GenericRecord> deserializer =
                new RegistryAvroDeserializationSchema<>(
                        GenericRecord.class,
                        readerSchema,
                        () ->
                                new SchemaCoder() {
                                    @Override
                                    public Schema readSchema(InputStream in) {
                                        return writerSchema;
                                    }

                                    @Override
                                    public void writeSchema(Schema schema, OutputStream out)
                                            throws IOException {
                                        // do nothing
                                    }
                                });

        // Create test record with enum value
        GenericData.Record record = new GenericData.Record(writerSchema);
        Schema metadataSchema = writerSchema.getField("metadata").schema();
        GenericData.Record metadataRecord = new GenericData.Record(metadataSchema);

        Schema operationEnumSchema = metadataSchema.getField("operation").schema();
        metadataRecord.put("operation", new GenericData.EnumSymbol(operationEnumSchema, "UPDATE"));
        metadataRecord.put("timestamp", "2024-01-15T10:30:00Z");

        record.put("metadata", metadataRecord);

        // Deserialize - should succeed with writer schema approach
        GenericRecord result = deserializer.deserialize(writeRecord(record, writerSchema));

        GenericRecord metadata = (GenericRecord) result.get("metadata");
        assertThat(metadata.get("operation").toString()).isEqualTo("UPDATE");
        assertThat(metadata.get("timestamp").toString()).isEqualTo("2024-01-15T10:30:00Z");

        // Verify the operation field is an EnumSymbol that can be converted to string
        // This is how Flink's AvroToRowDataConverters handles enum -> VARCHAR conversion
        Object operation = metadata.get("operation");
        assertThat(operation).isInstanceOf(GenericData.EnumSymbol.class);
        assertThat(operation.toString()).isEqualTo("UPDATE");
    }
}
