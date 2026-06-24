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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.serialization.SerializerConfigImpl;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests the global gate at {@link InternalTypeInfo#createSerializer}: when the table option {@code
 * table.exec.state.schema-evolution.enabled} is on, a {@code ROW} type's serializer retains field
 * names so backward-compatible schema changes can be migrated on restore; when off, the serializer
 * is byte-identical to today's names-less one.
 */
class InternalTypeInfoSchemaEvolutionTest {

    private static final RowType ROW_TYPE =
            RowType.of(
                    new LogicalType[] {new IntType(), new BigIntType()}, new String[] {"f0", "f1"});

    private static SerializerConfigImpl configWithEvolution(boolean enabled) {
        Configuration configuration = new Configuration();
        configuration.setString(
                "table.exec.state.schema-evolution.enabled", String.valueOf(enabled));
        return new SerializerConfigImpl(configuration);
    }

    @Test
    void evolutionEnabledRetainsFieldNames() {
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(ROW_TYPE);

        TypeSerializer<RowData> serializer = typeInfo.createSerializer(configWithEvolution(true));

        assertThat(serializer).isInstanceOf(RowDataSerializer.class);
        assertThat(((RowDataSerializer) serializer).getFieldNames()).containsExactly("f0", "f1");
    }

    @Test
    void evolutionDisabledLeavesNamesNull() {
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(ROW_TYPE);

        TypeSerializer<RowData> serializer = typeInfo.createSerializer(new SerializerConfigImpl());

        assertThat(serializer).isInstanceOf(RowDataSerializer.class);
        assertThat(((RowDataSerializer) serializer).getFieldNames()).isNull();
    }

    @Test
    void evolutionEnabledRecursesIntoNestedRow() {
        RowType nested =
                RowType.of(
                        new LogicalType[] {new IntType(), new BigIntType()},
                        new String[] {"a", "b"});
        RowType rowType =
                RowType.of(
                        new LogicalType[] {nested, new IntType()}, new String[] {"outer", "tail"});
        InternalTypeInfo<RowData> typeInfo = InternalTypeInfo.of(rowType);

        TypeSerializer<RowData> serializer = typeInfo.createSerializer(configWithEvolution(true));

        TypeSerializer<?> nestedChild = ((RowDataSerializer) serializer).fieldSerializers()[0];
        assertThat(nestedChild).isInstanceOf(RowDataSerializer.class);
        assertThat(((RowDataSerializer) nestedChild).getFieldNames()).containsExactly("a", "b");
    }
}
