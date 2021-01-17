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
import org.apache.avro.generic.GenericRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.Random;

import static org.apache.flink.core.testutils.FlinkMatchers.containsCause;
import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecord;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;

/**
 * Tests for {@link AvroRowDataDeserializationSchema} and {@link AvroRowDataSerializationSchema} for
 * schema registry avro.
 */
public class RegistryAvroRowDataSeDeSchemaTest {
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

    @Rule public ExpectedException expectedEx = ExpectedException.none();

    @BeforeClass
    public static void beforeClass() {
        client = new MockSchemaRegistryClient();
    }

    @Before
    public void before() {
        this.address = TestDataGenerator.generateRandomAddress(new Random());
    }

    @After
    public void after() throws IOException, RestClientException {
        client.deleteSubject(SUBJECT);
    }

    @Test
    public void testRowDataWriteReadWithFullSchema() throws Exception {
        testRowDataWriteReadWithSchema(ADDRESS_SCHEMA);
    }

    @Test
    public void testRowDataWriteReadWithCompatibleSchema() throws Exception {
        testRowDataWriteReadWithSchema(ADDRESS_SCHEMA_COMPATIBLE);
        // Validates new schema has been registered.
        assertThat(client.getAllVersions(SUBJECT).size(), is(1));
    }

    @Test
    public void testRowDataWriteReadWithPreRegisteredSchema() throws Exception {
        client.register(SUBJECT, ADDRESS_SCHEMA);
        testRowDataWriteReadWithSchema(ADDRESS_SCHEMA);
        // Validates it does not produce new schema.
        assertThat(client.getAllVersions(SUBJECT).size(), is(1));
    }

    @Test
    public void testRowDataReadWithNonRegistryAvro() throws Exception {
        DataType dataType = AvroSchemaConverter.convertToDataType(ADDRESS_SCHEMA.toString());
        RowType rowType = (RowType) dataType.getLogicalType();

        AvroRowDataDeserializationSchema deserializer =
                getDeserializationSchema(rowType, ADDRESS_SCHEMA);

        deserializer.open(null);

        client.register(SUBJECT, ADDRESS_SCHEMA);
        byte[] oriBytes = writeRecord(address, ADDRESS_SCHEMA);
        expectedEx.expect(IOException.class);
        expectedEx.expect(
                containsCause(new IOException("Unknown data format. Magic number does not match")));
        deserializer.deserialize(oriBytes);
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

        assertNull(deserializer.deserialize(null));

        RowData oriData = address2RowData(address);
        byte[] serialized = serializer.serialize(oriData);
        RowData rowData = deserializer.deserialize(serialized);
        assertThat(rowData.getArity(), equalTo(schema.getFields().size()));
        assertEquals(address.getNum(), rowData.getInt(0));
        assertEquals(address.getStreet(), rowData.getString(1).toString());
        if (schema != ADDRESS_SCHEMA_COMPATIBLE) {
            assertEquals(address.getCity(), rowData.getString(2).toString());
            assertEquals(address.getState(), rowData.getString(3).toString());
            assertEquals(address.getZip(), rowData.getString(4).toString());
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

    private static AvroRowDataDeserializationSchema getDeserializationSchema(
            RowType rowType, Schema avroSchema) {
        ConfluentSchemaRegistryCoder registryCoder =
                new ConfluentSchemaRegistryCoder(SUBJECT, client);
        return new AvroRowDataDeserializationSchema(
                new RegistryAvroDeserializationSchema<GenericRecord>(
                        GenericRecord.class, avroSchema, () -> registryCoder),
                AvroToRowDataConverters.createRowConverter(rowType),
                InternalTypeInfo.of(rowType));
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
