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

import org.apache.flink.formats.avro.generated.Address;
import org.apache.flink.formats.avro.generated.SimpleRecord;
import org.apache.flink.formats.avro.utils.TestDataGenerator;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import static org.apache.flink.formats.avro.utils.AvroTestUtils.writeRecord;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/** Tests for {@link RegistryAvroDeserializationSchema}. */
public class RegistryAvroDeserializationSchemaTest {

    private static final Address address = TestDataGenerator.generateRandomAddress(new Random());

    @Test
    public void testGenericRecordReadWithCompatibleSchema() throws IOException {
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
        assertEquals(address.getNum(), genericRecord.get("num"));
        assertEquals(address.getStreet(), genericRecord.get("street").toString());
        assertNull(genericRecord.get("country"));
    }

    @Test
    public void testSpecificRecordReadMoreFieldsThanWereWritten() throws IOException {
        Schema smallerUserSchema =
                new Schema.Parser()
                        .parse(
                                "{\"namespace\": \"org.apache.flink.formats.avro.generated\",\n"
                                        + " \"type\": \"record\",\n"
                                        + " \"name\": \"SimpleRecord\",\n"
                                        + " \"fields\": [\n"
                                        + "     {\"name\": \"name\", \"type\": \"string\"}"
                                        + " ]\n"
                                        + "}]");
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
                                });

        GenericData.Record smallUser =
                new GenericRecordBuilder(smallerUserSchema).set("name", "someName").build();

        SimpleRecord simpleRecord =
                deserializer.deserialize(writeRecord(smallUser, smallerUserSchema));

        assertEquals("someName", simpleRecord.getName().toString());
        assertNull(simpleRecord.getOptionalField());
    }
}
