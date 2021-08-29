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

package org.apache.flink.connector.pulsar.common.schema;

import org.apache.flink.connector.pulsar.SampleMessage.SubMessage;
import org.apache.flink.connector.pulsar.SampleMessage.SubMessage.NestedMessage;
import org.apache.flink.connector.pulsar.SampleMessage.TestMessage;
import org.apache.flink.connector.pulsar.testutils.SampleData.Bar;
import org.apache.flink.connector.pulsar.testutils.SampleData.FL;
import org.apache.flink.connector.pulsar.testutils.SampleData.Foo;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Unit tests for {@link PulsarSchemaUtils}. */
class PulsarSchemaUtilsTest {

    @Test
    void haveProtobufShouldReturnTrueIfWeProvidedIt() {
        assertTrue(PulsarSchemaUtils.haveProtobuf());
    }

    @Test
    void protobufClassValidation() {
        assertTrue(PulsarSchemaUtils.isProtobufTypeClass(TestMessage.class));
        assertTrue(PulsarSchemaUtils.isProtobufTypeClass(SubMessage.class));
        assertTrue(PulsarSchemaUtils.isProtobufTypeClass(NestedMessage.class));

        assertFalse(PulsarSchemaUtils.isProtobufTypeClass(Bar.class));
        assertFalse(PulsarSchemaUtils.isProtobufTypeClass(FL.class));
        assertFalse(PulsarSchemaUtils.isProtobufTypeClass(Foo.class));
    }

    @Test
    @SuppressWarnings("java:S5778")
    void createSchemaForComplexSchema() {
        // Avro
        Schema<Foo> avro1 = Schema.AVRO(Foo.class);
        PulsarSchema<Foo> avro2 = new PulsarSchema<>(avro1, Foo.class);
        assertThrows(
                NullPointerException.class,
                () -> PulsarSchemaUtils.createSchema(avro1.getSchemaInfo()));

        Schema<Foo> schema = PulsarSchemaUtils.createSchema(avro2.getSchemaInfo());
        assertNotEquals(schema.getSchemaInfo(), avro1.getSchemaInfo());
        assertEquals(schema.getSchemaInfo(), avro2.getSchemaInfo());

        // JSON
        Schema<FL> json1 = Schema.JSON(FL.class);
        PulsarSchema<FL> json2 = new PulsarSchema<>(json1, FL.class);
        Schema<FL> json3 = PulsarSchemaUtils.createSchema(json2.getSchemaInfo());
        assertNotEquals(json3.getSchemaInfo(), json1.getSchemaInfo());
        assertEquals(json3.getSchemaInfo(), json2.getSchemaInfo());

        // Protobuf Native
        Schema<TestMessage> proto1 = Schema.PROTOBUF_NATIVE(TestMessage.class);
        PulsarSchema<TestMessage> proto2 = new PulsarSchema<>(proto1, TestMessage.class);
        Schema<TestMessage> proto3 = PulsarSchemaUtils.createSchema(proto2.getSchemaInfo());
        assertNotEquals(proto3.getSchemaInfo(), proto1.getSchemaInfo());
        assertEquals(proto3.getSchemaInfo(), proto2.getSchemaInfo());

        // KeyValue
        Schema<KeyValue<byte[], byte[]>> kvBytes1 = Schema.KV_BYTES();
        PulsarSchema<KeyValue<byte[], byte[]>> kvBytes2 =
                new PulsarSchema<>(kvBytes1, byte[].class, byte[].class);
        Schema<KeyValue<byte[], byte[]>> kvBytes3 =
                PulsarSchemaUtils.createSchema(kvBytes2.getSchemaInfo());
        assertNotEquals(kvBytes3.getSchemaInfo(), kvBytes1.getSchemaInfo());
    }

    @Test
    void encodeAndDecodeClassInfo() {
        Schema<Foo> schema = Schema.AVRO(Foo.class);
        SchemaInfo info = schema.getSchemaInfo();
        SchemaInfo newInfo = PulsarSchemaUtils.encodeClassInfo(info, Foo.class);
        assertDoesNotThrow(() -> PulsarSchemaUtils.decodeClassInfo(newInfo));

        Class<Foo> clazz = PulsarSchemaUtils.decodeClassInfo(newInfo);
        assertEquals(clazz, Foo.class);
    }
}
