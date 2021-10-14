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
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchemaTypeInformation;
import org.apache.flink.connector.pulsar.testutils.SampleData.FL;
import org.apache.flink.connector.pulsar.testutils.SampleData.Foo;
import org.apache.flink.util.InstantiationUtil;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.SchemaDefinition;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Unit tests for {@link JSONSchemaFactory}. */
class JSONSchemaFactoryTest {

    @Test
    void createJSONSchemaFromSchemaInfo() {
        JSONSchema<Foo> schema =
                JSONSchema.of(
                        SchemaDefinition.<Foo>builder()
                                .withPojo(Foo.class)
                                .withAlwaysAllowNull(false)
                                .build());
        PulsarSchema<Foo> pulsarSchema = new PulsarSchema<>(schema, Foo.class);
        JSONSchemaFactory<Foo> factory = new JSONSchemaFactory<>();
        Schema<Foo> decodedSchema = factory.createSchema(pulsarSchema.getSchemaInfo());

        assertThat(decodedSchema)
                .isInstanceOf(JSONSchema.class)
                .hasFieldOrPropertyWithValue("schemaInfo", pulsarSchema.getSchemaInfo())
                .hasFieldOrPropertyWithValue("pojo", Foo.class);
    }

    @Test
    void createJSONTypeInformationFromSchemaInfo() {
        JSONSchema<FL> schema = JSONSchema.of(FL.class);
        PulsarSchema<FL> pulsarSchema = new PulsarSchema<>(schema, FL.class);
        JSONSchemaFactory<FL> factory = new JSONSchemaFactory<>();
        TypeInformation<FL> typeInfo = factory.createTypeInfo(pulsarSchema.getSchemaInfo());

        assertThat(typeInfo)
                .isInstanceOf(PulsarSchemaTypeInformation.class)
                .hasFieldOrPropertyWithValue("typeClass", FL.class);

        // TypeInformation serialization.
        assertDoesNotThrow(() -> InstantiationUtil.clone(typeInfo));
    }
}
