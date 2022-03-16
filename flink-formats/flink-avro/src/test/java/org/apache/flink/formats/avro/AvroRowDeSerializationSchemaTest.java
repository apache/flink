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

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.formats.avro.utils.AvroTestUtils;
import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificRecord;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;

/** Test for the Avro serialization and deserialization schema. */
class AvroRowDeSerializationSchemaTest {

    @Test
    void testSpecificSerializeDeserializeFromClass() throws IOException {
        final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData =
                AvroTestUtils.getSpecificTestData();

        final AvroRowSerializationSchema serializationSchema =
                new AvroRowSerializationSchema(testData.f0);
        final AvroRowDeserializationSchema deserializationSchema =
                new AvroRowDeserializationSchema(testData.f0);

        final byte[] bytes = serializationSchema.serialize(testData.f2);
        final Row actual = deserializationSchema.deserialize(bytes);

        assertThat(actual).isEqualTo(testData.f2);
    }

    @Test
    void testSpecificSerializeDeserializeFromSchema() throws IOException {
        final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData =
                AvroTestUtils.getSpecificTestData();
        final String schemaString = testData.f1.getSchema().toString();

        final AvroRowSerializationSchema serializationSchema =
                new AvroRowSerializationSchema(schemaString);
        final AvroRowDeserializationSchema deserializationSchema =
                new AvroRowDeserializationSchema(schemaString);

        final byte[] bytes = serializationSchema.serialize(testData.f2);
        final Row actual = deserializationSchema.deserialize(bytes);

        assertThat(actual).isEqualTo(testData.f2);
    }

    @Test
    void testGenericSerializeDeserialize() throws IOException {
        final Tuple3<GenericRecord, Row, Schema> testData = AvroTestUtils.getGenericTestData();

        final AvroRowSerializationSchema serializationSchema =
                new AvroRowSerializationSchema(testData.f2.toString());
        final AvroRowDeserializationSchema deserializationSchema =
                new AvroRowDeserializationSchema(testData.f2.toString());

        final byte[] bytes = serializationSchema.serialize(testData.f1);
        final Row actual = deserializationSchema.deserialize(bytes);

        assertThat(actual).isEqualTo(testData.f1);
    }

    @Test
    void testSpecificSerializeFromClassSeveralTimes() throws IOException {
        final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData =
                AvroTestUtils.getSpecificTestData();

        final AvroRowSerializationSchema serializationSchema =
                new AvroRowSerializationSchema(testData.f0);
        final AvroRowDeserializationSchema deserializationSchema =
                new AvroRowDeserializationSchema(testData.f0);

        serializationSchema.serialize(testData.f2);
        serializationSchema.serialize(testData.f2);
        final byte[] bytes = serializationSchema.serialize(testData.f2);
        final Row actual = deserializationSchema.deserialize(bytes);

        assertThat(actual).isEqualTo(testData.f2);
    }

    @Test
    void testSpecificSerializeFromSchemaSeveralTimes() throws IOException {
        final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData =
                AvroTestUtils.getSpecificTestData();
        final String schemaString = testData.f1.getSchema().toString();

        final AvroRowSerializationSchema serializationSchema =
                new AvroRowSerializationSchema(schemaString);
        final AvroRowDeserializationSchema deserializationSchema =
                new AvroRowDeserializationSchema(schemaString);

        serializationSchema.serialize(testData.f2);
        serializationSchema.serialize(testData.f2);
        final byte[] bytes = serializationSchema.serialize(testData.f2);
        final Row actual = deserializationSchema.deserialize(bytes);

        assertThat(actual).isEqualTo(testData.f2);
    }

    @Test
    void testGenericSerializeSeveralTimes() throws IOException {
        final Tuple3<GenericRecord, Row, Schema> testData = AvroTestUtils.getGenericTestData();

        final AvroRowSerializationSchema serializationSchema =
                new AvroRowSerializationSchema(testData.f2.toString());
        final AvroRowDeserializationSchema deserializationSchema =
                new AvroRowDeserializationSchema(testData.f2.toString());

        serializationSchema.serialize(testData.f1);
        serializationSchema.serialize(testData.f1);
        final byte[] bytes = serializationSchema.serialize(testData.f1);
        final Row actual = deserializationSchema.deserialize(bytes);

        assertThat(actual).isEqualTo(testData.f1);
    }

    @Test
    void testSpecificDeserializeFromClassSeveralTimes() throws IOException {
        final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData =
                AvroTestUtils.getSpecificTestData();

        final AvroRowSerializationSchema serializationSchema =
                new AvroRowSerializationSchema(testData.f0);
        final AvroRowDeserializationSchema deserializationSchema =
                new AvroRowDeserializationSchema(testData.f0);

        final byte[] bytes = serializationSchema.serialize(testData.f2);
        deserializationSchema.deserialize(bytes);
        deserializationSchema.deserialize(bytes);
        final Row actual = deserializationSchema.deserialize(bytes);

        assertThat(actual).isEqualTo(testData.f2);
    }

    @Test
    void testSpecificDeserializeFromSchemaSeveralTimes() throws IOException {
        final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData =
                AvroTestUtils.getSpecificTestData();
        final String schemaString = testData.f1.getSchema().toString();

        final AvroRowSerializationSchema serializationSchema =
                new AvroRowSerializationSchema(schemaString);
        final AvroRowDeserializationSchema deserializationSchema =
                new AvroRowDeserializationSchema(schemaString);

        final byte[] bytes = serializationSchema.serialize(testData.f2);
        deserializationSchema.deserialize(bytes);
        deserializationSchema.deserialize(bytes);
        final Row actual = deserializationSchema.deserialize(bytes);

        assertThat(actual).isEqualTo(testData.f2);
    }

    @Test
    void testGenericDeserializeSeveralTimes() throws IOException {
        final Tuple3<GenericRecord, Row, Schema> testData = AvroTestUtils.getGenericTestData();

        final AvroRowSerializationSchema serializationSchema =
                new AvroRowSerializationSchema(testData.f2.toString());
        final AvroRowDeserializationSchema deserializationSchema =
                new AvroRowDeserializationSchema(testData.f2.toString());

        final byte[] bytes = serializationSchema.serialize(testData.f1);
        deserializationSchema.deserialize(bytes);
        deserializationSchema.deserialize(bytes);
        final Row actual = deserializationSchema.deserialize(bytes);

        assertThat(actual).isEqualTo(testData.f1);
    }

    @Test
    void testSerializability() throws Exception {
        final Tuple3<Class<? extends SpecificRecord>, SpecificRecord, Row> testData =
                AvroTestUtils.getSpecificTestData();
        final String schemaString = testData.f1.getSchema().toString();

        // from class
        final AvroRowSerializationSchema classSer = new AvroRowSerializationSchema(testData.f0);
        final AvroRowDeserializationSchema classDeser =
                new AvroRowDeserializationSchema(testData.f0);
        testSerializability(classSer, classDeser, testData.f2);

        // from schema string
        final AvroRowSerializationSchema schemaSer = new AvroRowSerializationSchema(schemaString);
        final AvroRowDeserializationSchema schemaDeser =
                new AvroRowDeserializationSchema(schemaString);
        testSerializability(schemaSer, schemaDeser, testData.f2);
    }

    private void testSerializability(
            AvroRowSerializationSchema ser, AvroRowDeserializationSchema deser, Row data)
            throws Exception {
        final byte[] serBytes = InstantiationUtil.serializeObject(ser);
        final byte[] deserBytes = InstantiationUtil.serializeObject(deser);

        final AvroRowSerializationSchema serCopy =
                InstantiationUtil.deserializeObject(
                        serBytes, Thread.currentThread().getContextClassLoader());
        final AvroRowDeserializationSchema deserCopy =
                InstantiationUtil.deserializeObject(
                        deserBytes, Thread.currentThread().getContextClassLoader());

        final byte[] bytes = serCopy.serialize(data);
        deserCopy.deserialize(bytes);
        deserCopy.deserialize(bytes);
        final Row actual = deserCopy.deserialize(bytes);

        assertThat(actual).isEqualTo(data);
    }
}
