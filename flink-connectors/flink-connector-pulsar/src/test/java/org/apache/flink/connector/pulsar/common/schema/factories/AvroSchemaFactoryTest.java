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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.ComparatorTestBase.TestInputView;
import org.apache.flink.api.common.typeutils.ComparatorTestBase.TestOutputView;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchema;
import org.apache.flink.connector.pulsar.common.schema.PulsarSchemaTypeInformation;
import org.apache.flink.util.InstantiationUtil;

import org.apache.avro.reflect.AvroDefault;
import org.apache.avro.reflect.Nullable;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.AvroSchema;
import org.junit.jupiter.api.Test;

import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Unit tests for {@link AvroSchemaFactory}. */
class AvroSchemaFactoryTest {

    private final AvroSchemaFactory<?> factory = new AvroSchemaFactory<>();

    @Test
    void createAvroSchemaFromSchemaInfo() {
        AvroSchema<DefaultStruct> schema1 = AvroSchema.of(DefaultStruct.class);

        // AvroSchema should provide type class
        assertThrows(
                IllegalArgumentException.class,
                () -> new PulsarSchema<>(schema1),
                "Avro Schema should provide the type class");

        PulsarSchema<DefaultStruct> pulsarSchema = new PulsarSchema<>(schema1, DefaultStruct.class);
        AvroSchemaFactory<DefaultStruct> factory = new AvroSchemaFactory<>();
        Schema<DefaultStruct> schema2 = factory.createSchema(pulsarSchema.getSchemaInfo());

        assertThat(schema2)
                .isInstanceOf(AvroSchema.class)
                .hasFieldOrPropertyWithValue("schemaInfo", pulsarSchema.getSchemaInfo());

        // Serialize and deserialize.
        DefaultStruct struct1 = new DefaultStruct();
        struct1.setField1(ThreadLocalRandom.current().nextInt());
        struct1.setField2(randomAlphabetic(10));
        struct1.setField3(ThreadLocalRandom.current().nextLong());

        byte[] bytes = schema1.encode(struct1);
        DefaultStruct struct2 = schema2.decode(bytes);

        assertEquals(struct1, struct2);
    }

    @Test
    void createAvroTypeInformationAndSerializeValues() throws Exception {
        AvroSchema<StructWithAnnotations> schema = AvroSchema.of(StructWithAnnotations.class);
        PulsarSchema<StructWithAnnotations> pulsarSchema =
                new PulsarSchema<>(schema, StructWithAnnotations.class);

        StructWithAnnotations struct1 = new StructWithAnnotations();
        struct1.setField1(5678);

        AvroSchemaFactory<StructWithAnnotations> factory = new AvroSchemaFactory<>();
        TypeInformation<StructWithAnnotations> information =
                factory.createTypeInfo(pulsarSchema.getSchemaInfo());
        assertThat(information)
                .isInstanceOf(PulsarSchemaTypeInformation.class)
                .hasFieldOrPropertyWithValue("typeClass", StructWithAnnotations.class);

        // Serialize by type information.
        TypeSerializer<StructWithAnnotations> serializer =
                information.createSerializer(new ExecutionConfig());
        // TypeInformation serialization.
        assertDoesNotThrow(() -> InstantiationUtil.clone(information));
        assertDoesNotThrow(() -> InstantiationUtil.clone(serializer));

        TestOutputView output = new TestOutputView();
        serializer.serialize(struct1, output);

        TestInputView input = output.getInputView();
        StructWithAnnotations struct2 = serializer.deserialize(input);

        assertThat(struct2)
                .hasFieldOrPropertyWithValue("field1", struct1.getField1())
                .hasFieldOrPropertyWithValue("field2", null)
                .hasFieldOrPropertyWithValue("field3", null);
    }

    private static class DefaultStruct {
        int field1;
        String field2;
        Long field3;

        public int getField1() {
            return field1;
        }

        public void setField1(int field1) {
            this.field1 = field1;
        }

        public String getField2() {
            return field2;
        }

        public void setField2(String field2) {
            this.field2 = field2;
        }

        public Long getField3() {
            return field3;
        }

        public void setField3(Long field3) {
            this.field3 = field3;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            DefaultStruct that = (DefaultStruct) o;
            return field1 == that.field1
                    && Objects.equals(field2, that.field2)
                    && Objects.equals(field3, that.field3);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field1, field2, field3);
        }
    }

    private static class StructWithAnnotations {
        int field1;
        @Nullable String field2;

        @AvroDefault("\"1000\"")
        Long field3;

        public int getField1() {
            return field1;
        }

        public void setField1(int field1) {
            this.field1 = field1;
        }

        public String getField2() {
            return field2;
        }

        public void setField2(String field2) {
            this.field2 = field2;
        }

        public Long getField3() {
            return field3;
        }

        public void setField3(Long field3) {
            this.field3 = field3;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            StructWithAnnotations that = (StructWithAnnotations) o;
            return field1 == that.field1
                    && Objects.equals(field2, that.field2)
                    && Objects.equals(field3, that.field3);
        }

        @Override
        public int hashCode() {
            return Objects.hash(field1, field2, field3);
        }
    }
}
