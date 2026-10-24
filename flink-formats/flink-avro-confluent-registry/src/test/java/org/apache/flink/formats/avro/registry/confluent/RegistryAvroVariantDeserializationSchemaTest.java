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

import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.AvroVariantDeserializationSchema;
import org.apache.flink.formats.avro.RegistryAvroSerializationSchema;
import org.apache.flink.formats.avro.RegistryWriterAvroDeserializationSchema;
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
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.VariantType;
import org.apache.flink.types.variant.Variant;

import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Random;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link AvroVariantDeserializationSchema} with Confluent Schema Registry wire format
 * using {@link MockSchemaRegistryClient}.
 */
class RegistryAvroVariantDeserializationSchemaTest {

    private static final Schema ADDRESS_SCHEMA = Address.getClassSchema();
    private static final Schema ADDRESS_SCHEMA_V2 =
            new Schema.Parser()
                    .parse(
                            "{\"namespace\": \"org.apache.flink.formats.avro.generated\","
                                    + " \"type\": \"record\","
                                    + " \"name\": \"Address\","
                                    + " \"fields\": ["
                                    + "     {\"name\": \"num\", \"type\": \"int\"},"
                                    + "     {\"name\": \"street\", \"type\": \"string\"},"
                                    + "     {\"name\": \"city\", \"type\": \"string\"},"
                                    + "     {\"name\": \"state\", \"type\": \"string\"},"
                                    + "     {\"name\": \"zip\", \"type\": \"string\"},"
                                    + "     {\"name\": \"country\", \"type\": \"string\"}"
                                    + " ]}");
    private static final String SUBJECT = "address-value";
    private static final int SCHEMA_CACHE_CAPACITY = 1000;

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
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testAvroVariantDeserialization(boolean includeSchemaMetadata) throws Exception {
        DataType dataType = AvroSchemaConverter.convertToDataType(ADDRESS_SCHEMA.toString());
        RowType rowType = (RowType) dataType.getLogicalType();

        AvroRowDataSerializationSchema serializer = getSerializationSchema(rowType, ADDRESS_SCHEMA);
        serializer.open(null);

        RowData input = address2RowData(address);
        byte[] serialized = serializer.serialize(input);

        AvroVariantDeserializationSchema deserializer =
                getAvroVariantDeserializationSchema(includeSchemaMetadata);
        deserializer.open(null);

        RowData rowData = deserializer.deserialize(serialized);
        assertThat(rowData).isNotNull();
        assertThat(rowData.getArity()).isEqualTo(includeSchemaMetadata ? 2 : 1);

        Variant variant = rowData.getVariant(0);
        assertThat(variant.getField("num").getInt()).isEqualTo(address.getNum());
        assertThat(variant.getField("street").getString()).isEqualTo(address.getStreet());
        assertThat(variant.getField("city").getString()).isEqualTo(address.getCity());
        assertThat(variant.getField("state").getString()).isEqualTo(address.getState());
        assertThat(variant.getField("zip").getString()).isEqualTo(address.getZip());

        if (includeSchemaMetadata) {
            assertThat(rowData.getString(1).toString()).isEqualTo(ADDRESS_SCHEMA.toString());
        }
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void testSchemaEvolution(boolean includeSchemaMetadata) throws Exception {
        DataType v1DataType = AvroSchemaConverter.convertToDataType(ADDRESS_SCHEMA.toString());
        RowType v1RowType = (RowType) v1DataType.getLogicalType();
        AvroRowDataSerializationSchema v1Serializer =
                getSerializationSchema(v1RowType, ADDRESS_SCHEMA);
        v1Serializer.open(null);
        byte[] v1Bytes = v1Serializer.serialize(address2RowData(address));

        DataType v2DataType = AvroSchemaConverter.convertToDataType(ADDRESS_SCHEMA_V2.toString());
        RowType v2RowType = (RowType) v2DataType.getLogicalType();
        AvroRowDataSerializationSchema v2Serializer =
                getSerializationSchema(v2RowType, ADDRESS_SCHEMA_V2);
        v2Serializer.open(null);
        byte[] v2Bytes = v2Serializer.serialize(address2RowData(address, "US"));

        AvroVariantDeserializationSchema deserializer =
                getAvroVariantDeserializationSchema(includeSchemaMetadata);
        deserializer.open(null);

        RowData v1Row = deserializer.deserialize(v1Bytes);
        Variant variant1 = v1Row.getVariant(0);
        assertThat(variant1.getField("num").getInt()).isEqualTo(address.getNum());
        assertThat(variant1.getField("street").getString()).isEqualTo(address.getStreet());
        assertThat(variant1.getField("city").getString()).isEqualTo(address.getCity());
        assertThat(variant1.getField("state").getString()).isEqualTo(address.getState());
        assertThat(variant1.getField("zip").getString()).isEqualTo(address.getZip());

        if (includeSchemaMetadata) {
            assertThat(v1Row.getString(1).toString()).isEqualTo(ADDRESS_SCHEMA.toString());
        }

        RowData v2Row = deserializer.deserialize(v2Bytes);
        Variant variant2 = v2Row.getVariant(0);
        assertThat(variant2.getField("num").getInt()).isEqualTo(address.getNum());
        assertThat(variant2.getField("street").getString()).isEqualTo(address.getStreet());
        assertThat(variant2.getField("city").getString()).isEqualTo(address.getCity());
        assertThat(variant2.getField("state").getString()).isEqualTo(address.getState());
        assertThat(variant2.getField("zip").getString()).isEqualTo(address.getZip());
        assertThat(variant2.getField("country").getString()).isEqualTo("US");

        if (includeSchemaMetadata) {
            assertThat(v2Row.getString(1).toString()).isEqualTo(ADDRESS_SCHEMA_V2.toString());
        }
    }

    // ------------------------------------------------------------------------
    //  Utilities
    // ------------------------------------------------------------------------

    private static AvroRowDataSerializationSchema getSerializationSchema(
            RowType rowType, Schema avroSchema) {
        ConfluentSchemaRegistryCoder registryCoder =
                new ConfluentSchemaRegistryCoder(SUBJECT, client);
        return new AvroRowDataSerializationSchema(
                rowType,
                new RegistryAvroSerializationSchema<GenericRecord>(
                        GenericRecord.class, avroSchema, () -> registryCoder),
                RowDataToAvroConverters.createConverter(rowType));
    }

    private static AvroVariantDeserializationSchema getAvroVariantDeserializationSchema(
            boolean includeSchemaMetadata) {
        ConfluentSchemaRegistryCoder registryCoder = new ConfluentSchemaRegistryCoder(client);
        RowType rowType =
                includeSchemaMetadata
                        ? RowType.of(
                                new VariantType(), new VarCharType(true, VarCharType.MAX_LENGTH))
                        : RowType.of(new VariantType());
        return new AvroVariantDeserializationSchema(
                new RegistryWriterAvroDeserializationSchema(() -> registryCoder),
                includeSchemaMetadata,
                SCHEMA_CACHE_CAPACITY,
                InternalTypeInfo.of(rowType));
    }

    private static RowData address2RowData(Address address, String... extraFields) {
        GenericRowData rowData = new GenericRowData(5 + extraFields.length);
        rowData.setField(0, address.getNum());
        rowData.setField(1, new BinaryStringData(address.getStreet().toString()));
        rowData.setField(2, new BinaryStringData(address.getCity().toString()));
        rowData.setField(3, new BinaryStringData(address.getState().toString()));
        rowData.setField(4, new BinaryStringData(address.getZip().toString()));
        for (int i = 0; i < extraFields.length; i++) {
            rowData.setField(5 + i, new BinaryStringData(extraFields[i]));
        }
        return rowData;
    }
}
