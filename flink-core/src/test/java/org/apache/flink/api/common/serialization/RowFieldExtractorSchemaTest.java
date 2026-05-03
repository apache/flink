/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.common.serialization;

import org.apache.flink.types.Row;
import org.apache.flink.util.InstantiationUtil;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link RowFieldExtractorSchema}. */
class RowFieldExtractorSchemaTest {

    @Test
    void testSerializeByteArrayField() {
        RowFieldExtractorSchema schema = new RowFieldExtractorSchema(0);
        byte[] value = "test-value".getBytes(StandardCharsets.UTF_8);
        Row row = Row.of(value, 123);

        byte[] result = schema.serialize(row);

        assertThat(result).isEqualTo(value);
    }

    @Test
    void testSerializeNonByteArrayFieldThrowsException() {
        RowFieldExtractorSchema schema = new RowFieldExtractorSchema(1);
        Row row = Row.of("key", 42); // field 1 is Integer, not byte[]

        assertThatThrownBy(() -> schema.serialize(row))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("must be of type byte[]");
    }

    @Test
    void testSerializeNullRow() {
        RowFieldExtractorSchema schema = new RowFieldExtractorSchema(0);

        byte[] result = schema.serialize(null);

        assertThat(result).isEmpty();
    }

    @Test
    void testSerializeNullField() {
        RowFieldExtractorSchema schema = new RowFieldExtractorSchema(0);
        Row row = Row.of(null, "value");

        byte[] result = schema.serialize(row);

        assertThat(result).isEmpty();
    }

    @Test
    void testSerializeOutOfBoundsIndex() {
        RowFieldExtractorSchema schema = new RowFieldExtractorSchema(5);
        Row row = Row.of("field0", "field1");

        assertThatThrownBy(() -> schema.serialize(row))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Cannot access field 5 in Row with arity 2");
    }

    @Test
    void testNegativeFieldIndexThrowsException() {
        assertThatThrownBy(() -> new RowFieldExtractorSchema(-1))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Field index must be non-negative");
    }

    @Test
    void testSerializability() throws IOException, ClassNotFoundException {
        RowFieldExtractorSchema schema = new RowFieldExtractorSchema(3);

        RowFieldExtractorSchema deserialized =
                InstantiationUtil.deserializeObject(
                        InstantiationUtil.serializeObject(schema), getClass().getClassLoader());

        assertThat(deserialized.getFieldIndex()).isEqualTo(3);
    }

    @Test
    void testEquals() {
        RowFieldExtractorSchema schema1 = new RowFieldExtractorSchema(1);
        RowFieldExtractorSchema schema2 = new RowFieldExtractorSchema(1);
        RowFieldExtractorSchema schema3 = new RowFieldExtractorSchema(2);

        assertThat(schema1).isEqualTo(schema2);
        assertThat(schema1).isNotEqualTo(schema3);
    }

    @Test
    void testHashCode() {
        RowFieldExtractorSchema schema1 = new RowFieldExtractorSchema(1);
        RowFieldExtractorSchema schema2 = new RowFieldExtractorSchema(1);

        assertThat(schema1.hashCode()).isEqualTo(schema2.hashCode());
    }
}
