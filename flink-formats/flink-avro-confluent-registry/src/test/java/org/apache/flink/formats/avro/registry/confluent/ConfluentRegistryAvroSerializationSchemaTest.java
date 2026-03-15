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

import org.apache.flink.formats.avro.generated.Address;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link ConfluentRegistryAvroSerializationSchema}. */
class ConfluentRegistryAvroSerializationSchemaTest {

    private static final String SUBJECT = "test-subject";
    private static final String SCHEMA_REGISTRY_URL = "someUrl";

    @Test
    void testForPrimitiveTypeWithStringSchema() {
        Schema stringSchema = Schema.create(Schema.Type.STRING);

        ConfluentRegistryAvroSerializationSchema<Object> schema =
                ConfluentRegistryAvroSerializationSchema.forPrimitiveType(
                        SUBJECT, stringSchema, SCHEMA_REGISTRY_URL, null);

        assertThat(schema).isNotNull();
    }

    @Test
    void testForPrimitiveTypeWithStringSchemaAndConfigs() {
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        Map<String, String> configs = new HashMap<>();
        configs.put("basic.auth.credentials.source", "USER_INFO");
        configs.put("basic.auth.user.info", "user:password");

        ConfluentRegistryAvroSerializationSchema<Object> schema =
                ConfluentRegistryAvroSerializationSchema.forPrimitiveType(
                        SUBJECT, stringSchema, SCHEMA_REGISTRY_URL, configs);

        assertThat(schema).isNotNull();
    }

    @Test
    void testForPrimitiveTypeWithIntSchema() {
        Schema intSchema = Schema.create(Schema.Type.INT);

        ConfluentRegistryAvroSerializationSchema<Object> schema =
                ConfluentRegistryAvroSerializationSchema.forPrimitiveType(
                        SUBJECT, intSchema, SCHEMA_REGISTRY_URL, null);

        assertThat(schema).isNotNull();
    }

    @Test
    void testForPrimitiveTypeWithLongSchema() {
        Schema longSchema = Schema.create(Schema.Type.LONG);

        ConfluentRegistryAvroSerializationSchema<Object> schema =
                ConfluentRegistryAvroSerializationSchema.forPrimitiveType(
                        SUBJECT, longSchema, SCHEMA_REGISTRY_URL, null);

        assertThat(schema).isNotNull();
    }

    @Test
    void testForPrimitiveTypeWithBooleanSchema() {
        Schema booleanSchema = Schema.create(Schema.Type.BOOLEAN);

        ConfluentRegistryAvroSerializationSchema<Object> schema =
                ConfluentRegistryAvroSerializationSchema.forPrimitiveType(
                        SUBJECT, booleanSchema, SCHEMA_REGISTRY_URL, null);

        assertThat(schema).isNotNull();
    }

    @Test
    void testForPrimitiveTypeWithFloatSchema() {
        Schema floatSchema = Schema.create(Schema.Type.FLOAT);

        ConfluentRegistryAvroSerializationSchema<Object> schema =
                ConfluentRegistryAvroSerializationSchema.forPrimitiveType(
                        SUBJECT, floatSchema, SCHEMA_REGISTRY_URL, null);

        assertThat(schema).isNotNull();
    }

    @Test
    void testForPrimitiveTypeWithDoubleSchema() {
        Schema doubleSchema = Schema.create(Schema.Type.DOUBLE);

        ConfluentRegistryAvroSerializationSchema<Object> schema =
                ConfluentRegistryAvroSerializationSchema.forPrimitiveType(
                        SUBJECT, doubleSchema, SCHEMA_REGISTRY_URL, null);

        assertThat(schema).isNotNull();
    }

    @Test
    void testForPrimitiveTypeInstantiation() throws Exception {
        Schema stringSchema = Schema.create(Schema.Type.STRING);

        ConfluentRegistryAvroSerializationSchema<Object> schema =
                ConfluentRegistryAvroSerializationSchema.forPrimitiveType(
                        SUBJECT, stringSchema, SCHEMA_REGISTRY_URL, null);

        assertThat(schema).isNotNull();
        // Test that the schema was created with correct parameters
        // We can't test actual serialization without a running schema registry,
        // but we can verify the factory method works correctly
    }

    @Test
    void testForPrimitiveTypeConsistencyWithFactoryMethods() {
        Schema stringSchema = Schema.create(Schema.Type.STRING);
        GenericRecord genericRecord = new GenericData.Record(createRecordSchema());

        ConfluentRegistryAvroSerializationSchema<Object> primitiveSchema =
                ConfluentRegistryAvroSerializationSchema.forPrimitiveType(
                        SUBJECT, stringSchema, SCHEMA_REGISTRY_URL, null);

        ConfluentRegistryAvroSerializationSchema<GenericRecord> genericSchema =
                ConfluentRegistryAvroSerializationSchema.forGeneric(
                        SUBJECT, createRecordSchema(), SCHEMA_REGISTRY_URL, null);

        ConfluentRegistryAvroSerializationSchema<Address> specificSchema =
                ConfluentRegistryAvroSerializationSchema.forSpecific(
                        Address.class, SUBJECT, SCHEMA_REGISTRY_URL, null);

        assertThat(primitiveSchema).isNotNull();
        assertThat(genericSchema).isNotNull();
        assertThat(specificSchema).isNotNull();

        assertThat(primitiveSchema.getClass()).isEqualTo(genericSchema.getClass());
        assertThat(primitiveSchema.getClass()).isEqualTo(specificSchema.getClass());
    }

    private Schema createRecordSchema() {
        return Schema.createRecord(
                "TestRecord",
                null,
                "org.apache.flink.test",
                false,
                java.util.Arrays.asList(
                        new Schema.Field(
                                "testField", Schema.create(Schema.Type.STRING), null, null)));
    }
}
