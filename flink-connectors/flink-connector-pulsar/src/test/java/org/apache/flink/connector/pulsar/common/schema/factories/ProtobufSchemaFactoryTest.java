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
import org.apache.flink.connector.pulsar.SampleMessage.TestMessage;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchemaTypeInformation;
import org.apache.flink.util.InstantiationUtil;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.ProtobufSchema;
import org.junit.jupiter.api.Test;

import static org.apache.flink.connector.pulsar.common.schema.PulsarSchemaUtils.decodeClassInfo;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/** Unit tests for {@link ProtobufSchemaFactory}. */
class ProtobufSchemaFactoryTest {

    @Test
    void createProtobufSchemaFromSchemaInfo() {
        ProtobufSchema<SubMessage> schema = ProtobufSchema.of(SubMessage.class);
        PulsarSchema<SubMessage> pulsarSchema = new PulsarSchema<>(schema, SubMessage.class);
        ProtobufSchemaFactory<SubMessage> factory = new ProtobufSchemaFactory<>();

        Schema<SubMessage> schema2 = factory.createSchema(pulsarSchema.getSchemaInfo());
        assertThat(schema2)
                .hasFieldOrPropertyWithValue("schemaInfo", pulsarSchema.getSchemaInfo())
                .isInstanceOf(ProtobufSchema.class);

        assertEquals(decodeClassInfo(schema2.getSchemaInfo()), SubMessage.class);
        assertNotSame(schema, schema2);
    }

    @Test
    void createProtobufTypeInfoFromSchemaInfo() {
        ProtobufSchema<TestMessage> schema = ProtobufSchema.of(TestMessage.class);
        PulsarSchema<TestMessage> pulsarSchema = new PulsarSchema<>(schema, TestMessage.class);
        ProtobufSchemaFactory<TestMessage> factory = new ProtobufSchemaFactory<>();

        TypeInformation<TestMessage> typeInfo =
                factory.createTypeInfo(pulsarSchema.getSchemaInfo());
        assertDoesNotThrow(() -> InstantiationUtil.clone(typeInfo));
        assertThat(typeInfo)
                .isInstanceOf(PulsarSchemaTypeInformation.class)
                .hasFieldOrPropertyWithValue("typeClass", TestMessage.class);
    }
}
