/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.registry.confluent;

import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RegistryAvroDeserializationSchema;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.formats.avro.utils.TestDataGenerator;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecord;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link AvroRowDataDeserializationSchema} and {@link AvroRowDataSerializationSchema} for
 * schema registry avro.
 */
class RegistryAvroRowDataSeDeSchemaTest {
    private static final String ENUM_SUBJECT = "enum-record-value";

    private static final Schema ENUM_RECORD_SCHEMA =
            new Schema.Parser()
                    .parse(
                            "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                    + " \"type\": \"record\",\n"
                                    + " \"name\": \"EnumRecord\",\n"
                                    + " \"fields\": [\n"
                                    + "     {\"name\": \"name\", \"type\": \"string\"},\n"
                                    + "     {\"name\": \"color\", \"type\": [\"null\","
                                    + "         {\"type\": \"enum\", \"name\": \"Colors\","
                                    + "          \"symbols\": [\"RED\", \"GREEN\", \"BLUE\"]}]}\n"
                                    + "  ]\n"
                                    + "}");

    private static final Schema ADDRESS_SCHEMA = Address.getClassSchema();

    private static final Schema ADDRESS_SCHEMA_COMPATIBLE =
            new Schema.Parser()
                    .parse(
                            ""
                                    + "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                    + " \"type\": \"record\",\n"
                                    + " \"name\": \"Address\",\n"
                                    + " \"fields\": [\n"
                                    + "     {\"name\": \"num\", \"type\": \"int\"},\n"
                                    + "     {\"name\": \"street\", \"type\": \"string\"}\n"
                                    + "  ]\n"
                                    + "}");

    private static final String SUBJECT = "address-value";

    private static SchemaRegistryClient client;

    private Address address;

    @BeforeAll
    static void beforeClass() {
        client = new MockSchemaRegistryClient();
    }

    @BeforeEach
    void before() {
        this.address = TestDataGenerator.generateRandomAddress(new Random());
    }

    @AfterEach
    void after() throws IOException, RestClientException {
        client.deleteSubject(SUBJECT);
        client.deleteSubject(ENUM_SUBJECT);
    }

    @Test
    void testRowDataWriteReadWithFullSchema() throws Exception {
        testRowDataWriteReadWithSchema(ADDRESS_SCHEMA);
    }

    @Test
    void testRowDataWriteReadWithCompatibleSchema() throws Exception {
        testRowDataWriteReadWithSchema(ADDRESS_SCHEMA_COMPATIBLE);
        // Validates new schema has been registered.
        assertThat(client.getAllVersions(SUBJECT)).hasSize(1);
    }

    @Test
    void testRowDataWriteReadWithPreRegisteredSchema() throws Exception {
        client.register(SUBJECT, ADDRESS_SCHEMA);
        testRowDataWriteReadWithSchema(ADDRESS_SCHEMA);
        // Validates it does not produce new schema.
        assertThat(client.getAllVersions(SUBJECT)).hasSize(1);
    }

    @Test
    void testRowDataReadWithNonRegistryAvro() throws Exception {
        DataType dataType = AvroSchemaConverter.convertToDataType(ADDRESS_SCHEMA.toString());
        RowType rowType = (RowType) dataType.getLogicalType();

        AvroRowDataDeserializationSchema deserializer =
                getDeserializationSchema(rowType, ADDRESS_SCHEMA);

        deserializer.open(null);

        client.register(SUBJECT, ADDRESS_SCHEMA);
        byte[] oriBytes = writeRecord(address, ADDRESS_SCHEMA);
        assertThatThrownBy(() -> deserializer.deserialize(oriBytes))
                .isInstanceOf(IOException.class)
                .hasCause(new IOException("Unknown data format. Magic number does not match"));
    }

    @Test
    void testRowDataReadWithEnumFieldAndNullReaderSchema() throws Exception {
        DataType dataType = AvroSchemaConverter.convertToDataType(ENUM_RECORD_SCHEMA.toString());
        RowType rowType = (RowType) dataType.getLogicalType();

        int schemaId = client.register(ENUM_SUBJECT, ENUM_RECORD_SCHEMA);
        GenericRecord record = new GenericData.Record(ENUM_RECORD_SCHEMA);
        record.put("name", "Alice");
        record.put(
                "color",
                new GenericData.EnumSymbol(
                        ENUM_RECORD_SCHEMA.getField("color").schema().getTypes().get(1), "RED"));
        byte[] serialized = serializeWithRegistryFormat(record, ENUM_RECORD_SCHEMA, schemaId);

        AvroRowDataDeserializationSchema deserializer =
                getDeserializationSchemaForSubject(rowType, null, ENUM_SUBJECT);
        deserializer.open(null);

        RowData result = deserializer.deserialize(serialized);
        assertThat(result.getArity()).isEqualTo(2);
        assertThat(result.getString(0).toString()).isEqualTo("Alice");
        assertThat(result.getString(1).toString()).isEqualTo("RED");
    }

    private void testRowDataWriteReadWithSchema(Schema schema) throws Exception {
        DataType dataType = AvroSchemaConverter.convertToDataType(schema.toString());
        RowType rowType = (RowType) dataType.getLogicalType();

        AvroRowDataSerializationSchema serializer = getSerializationSchema(rowType, schema);
        Schema writeSchema = AvroSchemaConverter.convertToSchema(dataType.getLogicalType());
        AvroRowDataDeserializationSchema deserializer =
                getDeserializationSchema(rowType, writeSchema);

        serializer.open(null);
        deserializer.open(null);

        assertThat(deserializer.deserialize(null)).isNull();

        RowData oriData = address2RowData(address);
        byte[] serialized = serializer.serialize(oriData);
        RowData rowData = deserializer.deserialize(serialized);
        assertThat(rowData.getArity()).isEqualTo(schema.getFields().size());
        assertThat(rowData.getInt(0)).isEqualTo(address.getNum());
        assertThat(rowData.getString(1).toString()).isEqualTo(address.getStreet());
        if (schema != ADDRESS_SCHEMA_COMPATIBLE) {
            assertThat(rowData.getString(2).toString()).isEqualTo(address.getCity());
            assertThat(rowData.getString(3).toString()).isEqualTo(address.getState());
            assertThat(rowData.getString(4).toString()).isEqualTo(address.getZip());
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static AvroRowDataSerializationSchema getSerializationSchema(
            RowType rowType, Schema avroSchema) {
        return getSerializationSchemaForSubject(rowType, avroSchema, SUBJECT);
    }

    private static AvroRowDataSerializationSchema getSerializationSchemaForSubject(
            RowType rowType, Schema avroSchema, String subject) {
        ConfluentSchemaRegistryCoder registryCoder =
                new ConfluentSchemaRegistryCoder(subject, client);
        return new AvroRowDataSerializationSchema(
                rowType,
                new RegistryAvroSerializationSchema<GenericRecord>(
                        GenericRecord.class, avroSchema, () -> registryCoder),
                RowDataToAvroConverters.createConverter(rowType));
    }

    private static AvroRowDataDeserializationSchema getDeserializationSchema(
            RowType rowType, Schema avroSchema) {
        return getDeserializationSchemaForSubject(rowType, avroSchema, SUBJECT);
    }

    private static AvroRowDataDeserializationSchema getDeserializationSchemaForSubject(
            RowType rowType, Schema avroSchema, String subject) {
        ConfluentSchemaRegistryCoder registryCoder =
                new ConfluentSchemaRegistryCoder(subject, client);
        return new AvroRowDataDeserializationSchema(
                new RegistryAvroDeserializationSchema<GenericRecord>(
                        GenericRecord.class, avroSchema, () -> registryCoder),
                AvroToRowDataConverters.createRowConverter(rowType),
                InternalTypeInfo.of(rowType));
    }

    private static byte[] serializeWithRegistryFormat(
            GenericRecord record, Schema schema, int schemaId) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        out.write(0);
        out.write(ByteBuffer.allocate(4).putInt(schemaId).array());
        org.apache.avro.io.Encoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        new GenericDatumWriter<>(schema).write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    private static RowData address2RowData(Address address) {
        GenericRowData rowData = new GenericRowData(5);
        rowData.setField(0, address.getNum());
        rowData.setField(1, new BinaryStringData(address.getStreet().toString()));
        rowData.setField(2, new BinaryStringData(address.getCity().toString()));
        rowData.setField(3, new BinaryStringData(address.getState().toString()));
        rowData.setField(4, new BinaryStringData(address.getZip().toString()));
        return rowData;
    }
}
