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
import org.apache.flink.connector.pulsar.testutils.SampleData.Bar;
import org.apache.flink.connector.pulsar.testutils.SampleData.FA;
import org.apache.flink.connector.pulsar.testutils.SampleData.FM;
import org.apache.flink.util.InstantiationUtil;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.apache.pulsar.client.impl.schema.KeyValueSchemaImpl;
import org.apache.pulsar.common.schema.KeyValue;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

/** Unit tests for {@link KeyValueSchemaFactory}. */
class KeyValueSchemaFactoryTest {

    @Test
    void createKeyValueSchemaFromSchemaInfo() {
        Schema<KeyValue<FM, FA>> schema1 =
                KeyValueSchemaImpl.of(FM.class, FA.class, SchemaType.AVRO);
        PulsarSchema<KeyValue<FM, FA>> pulsarSchema =
                new PulsarSchema<>(schema1, FM.class, FA.class);
        KeyValueSchemaFactory<FM, FA> factory = new KeyValueSchemaFactory<>();

        Schema<KeyValue<FM, FA>> schema2 = factory.createSchema(pulsarSchema.getSchemaInfo());
        assertThat(schema2)
                .isInstanceOf(KeyValueSchemaImpl.class)
                .hasFieldOrPropertyWithValue("schemaInfo", pulsarSchema.getSchemaInfo());

        KeyValueSchemaImpl<FM, FA> schema3 = (KeyValueSchemaImpl<FM, FA>) schema2;
        assertThat(schema3.getKeySchema()).isInstanceOf(AvroSchema.class);
        assertThat(schema3.getValueSchema()).isInstanceOf(AvroSchema.class);
    }

    @Test
    void createKeyValueTypeInformationFromSchemaInfo() throws Exception {
        Schema<KeyValue<Bar, FA>> schema1 =
                KeyValueSchemaImpl.of(Bar.class, FA.class, SchemaType.JSON);
        PulsarSchema<KeyValue<Bar, FA>> pulsarSchema =
                new PulsarSchema<>(schema1, Bar.class, FA.class);
        KeyValueSchemaFactory<Bar, FA> factory = new KeyValueSchemaFactory<>();

        TypeInformation<KeyValue<Bar, FA>> info =
                factory.createTypeInfo(pulsarSchema.getSchemaInfo());
        // TypeInformation serialization.
        assertDoesNotThrow(() -> InstantiationUtil.clone(info));
        assertThat(InstantiationUtil.clone(info))
                .isInstanceOf(PulsarSchemaTypeInformation.class)
                .hasFieldOrPropertyWithValue("typeClass", KeyValue.class);
    }
}
