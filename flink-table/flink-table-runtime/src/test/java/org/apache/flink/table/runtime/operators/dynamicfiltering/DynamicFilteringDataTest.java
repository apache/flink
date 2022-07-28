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

package org.apache.flink.table.runtime.operators.dynamicfiltering;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for {@link DynamicFilteringData}. The tests are here since it's hard to test
 * DynamicFilteringData without the support of flink-table-runtime.
 */
class DynamicFilteringDataTest {
    @Test
    void testContains() {
        RowType rowType = RowType.of(new IntType(), new BigIntType(), new VarCharType());
        TypeInformation<RowData> rowTypeInfo = InternalTypeInfo.of(rowType);
        List<RowData> buildRows = new ArrayList<>();
        buildRows.add(rowData(1, 1L, "a"));
        buildRows.add(rowData(2, 1L, null));
        buildRows.add(rowData(1, null, "b"));
        buildRows.add(rowData(null, 2L, "c"));
        buildRows.add(rowData(0, 31L, "d"));
        List<byte[]> serializedData =
                buildRows.stream().map(r -> serialize(rowTypeInfo, r)).collect(Collectors.toList());

        DynamicFilteringData data =
                new DynamicFilteringData(rowTypeInfo, rowType, serializedData, true);

        for (RowData r : buildRows) {
            assertThat(data.contains(r)).isTrue();
        }
        assertThat(data.contains(rowData(0, 1L, "a"))).isFalse();
        assertThat(data.contains(rowData(1, 1L, null))).isFalse();
        // Has the same hash as (0, 31L, "d")
        assertThat(data.contains(rowData(1, 0L, "d"))).isFalse();
    }

    @Test
    void testNotFiltering() {
        RowType rowType = RowType.of(new IntType(), new BigIntType(), new VarCharType());
        DynamicFilteringData data =
                new DynamicFilteringData(
                        InternalTypeInfo.of(rowType), rowType, Collections.emptyList(), false);
        assertThat(data.contains(rowData(1, 1L, "a"))).isTrue();
    }

    @Test
    void testAddHashConflictingData() {
        RowType rowType = RowType.of(new IntType(), new BigIntType(), new VarCharType());
        TypeInformation<RowData> rowTypeInfo = InternalTypeInfo.of(rowType);
        List<RowData> buildRows = new ArrayList<>();
        buildRows.add(rowData(0, 31L, "d"));
        // Has the same hash as (0, 31L, "d")
        buildRows.add(rowData(1, 0L, "d"));

        List<byte[]> serializedData =
                buildRows.stream().map(r -> serialize(rowTypeInfo, r)).collect(Collectors.toList());

        DynamicFilteringData data =
                new DynamicFilteringData(rowTypeInfo, rowType, serializedData, true);

        for (RowData r : buildRows) {
            assertThat(data.contains(r)).isTrue();
        }
    }

    @Test
    void testMismatchingRowDataArity() {
        RowType rowType = RowType.of(new IntType(), new BigIntType(), new VarCharType());
        DynamicFilteringData data =
                new DynamicFilteringData(
                        InternalTypeInfo.of(rowType), rowType, Collections.emptyList(), true);
        assertThatThrownBy(() -> data.contains(rowData(1, 1L)))
                .isInstanceOf(TableException.class)
                .hasMessage("The arity of RowData is different");
    }

    private RowData rowData(Object... values) {
        GenericRowData rowData = new GenericRowData(values.length);
        for (int i = 0; i < values.length; ++i) {
            Object value = values[i];
            value = value instanceof String ? new BinaryStringData((String) value) : value;
            rowData.setField(i, value);
        }
        return rowData;
    }

    private byte[] serialize(TypeInformation<RowData> typeInfo, RowData row) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try {
            typeInfo.createSerializer(new ExecutionConfig())
                    .serialize(row, new DataOutputViewStreamWrapper(baos));
        } catch (IOException e) {
            // throw as RuntimeException so the function can use in lambda
            throw new RuntimeException(e);
        }
        return baos.toByteArray();
    }
}
