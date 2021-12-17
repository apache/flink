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

package org.apache.flink.connector.pulsar.common.schema.factories;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.pulsar.SampleMessage.SubMessage;
import org.apache.flink.connector.pulsar.SampleMessage.SubMessage.NestedMessage;
import org.apache.flink.connector.pulsar.SampleMessage.TestEnum;
import org.apache.flink.connector.pulsar.SampleMessage.TestMessage;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchemaTypeInformation;
import org.apache.flink.util.InstantiationUtil;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.ProtobufNativeSchema;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ThreadLocalRandom;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link ProtobufNativeSchemaFactory}. */
class ProtobufNativeSchemaFactoryTest {

    @Test
    void createProtobufNativeSchemaFromSchemaInfo() {
        ProtobufNativeSchema<TestMessage> schema1 = ProtobufNativeSchema.of(TestMessage.class);
        PulsarSchema<TestMessage> pulsarSchema = new PulsarSchema<>(schema1, TestMessage.class);
        ProtobufNativeSchemaFactory<TestMessage> factory = new ProtobufNativeSchemaFactory<>();

        Schema<TestMessage> schema2 = factory.createSchema(pulsarSchema.getSchemaInfo());
        assertThat(schema2)
                .hasFieldOrPropertyWithValue("schemaInfo", pulsarSchema.getSchemaInfo())
                .isInstanceOf(ProtobufNativeSchema.class);

        // Encode by original schema and decode by new schema
        TestMessage message =
                TestMessage.newBuilder()
                        .setStringField(randomAlphabetic(10))
                        .setDoubleField(ThreadLocalRandom.current().nextDouble())
                        .setIntField(ThreadLocalRandom.current().nextInt())
                        .setTestEnum(TestEnum.SHARED)
                        .setNestedField(
                                SubMessage.newBuilder()
                                        .setFoo(randomAlphabetic(10))
                                        .setBar(ThreadLocalRandom.current().nextDouble())
                                        .build())
                        .addRepeatedField(randomAlphabetic(13))
                        .build();
        byte[] bytes = schema1.encode(message);
        TestMessage message1 = schema2.decode(bytes);

        assertEquals(message, message1);
    }

    @Test
    void createProtobufNativeTypeInfoFromSchemaInfo() {
        ProtobufNativeSchema<NestedMessage> schema1 = ProtobufNativeSchema.of(NestedMessage.class);
        PulsarSchema<NestedMessage> pulsarSchema = new PulsarSchema<>(schema1, NestedMessage.class);
        ProtobufNativeSchemaFactory<NestedMessage> factory = new ProtobufNativeSchemaFactory<>();

        TypeInformation<NestedMessage> typeInfo =
                factory.createTypeInfo(pulsarSchema.getSchemaInfo());
        assertDoesNotThrow(() -> InstantiationUtil.clone(typeInfo));

        assertThat(typeInfo)
                .isInstanceOf(PulsarSchemaTypeInformation.class)
                .hasFieldOrPropertyWithValue("typeClass", NestedMessage.class);
    }
}
