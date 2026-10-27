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

package org.apache.flink.state.table;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.RowKind;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class StateValueConverterTest {

    private final StateValueConverter converter = new StateValueConverter();

    @Test
    void convertsBinaryStateKey() {
        final RowType type = RowType.of(new IntType(false));
        final BinaryRowData source = new RowDataSerializer(type).toBinaryRow(GenericRowData.of(42));

        final RowData result = (RowData) converter.getValue(type, source);

        assertThat(result.getInt(0)).isEqualTo(42);
        assertThat(result.getRowKind()).isEqualTo(RowKind.INSERT);
    }

    @Test
    void convertsBinaryMapKey() {
        final RowType type =
                RowType.of(new IntType(false), new BooleanType(), new TimestampType(3));
        final TimestampData timestamp = TimestampData.fromEpochMillis(123456789L);
        final BinaryRowData source =
                new RowDataSerializer(type).toBinaryRow(GenericRowData.of(7, true, timestamp));

        final RowData result = (RowData) converter.getValue(type, source);

        assertThat(result.getInt(0)).isEqualTo(7);
        assertThat(result.getBoolean(1)).isTrue();
        assertThat(result.getTimestamp(2, 3)).isEqualTo(timestamp);
    }

    @Test
    void preservesNullBinaryFields() {
        final RowType type = RowType.of(new IntType(), new VarCharType(), new TimestampType(3));
        final BinaryRowData source =
                new RowDataSerializer(type).toBinaryRow(GenericRowData.of(null, null, null));

        final RowData result = (RowData) converter.getValue(type, source);

        assertThat(result.isNullAt(0)).isTrue();
        assertThat(result.isNullAt(1)).isTrue();
        assertThat(result.isNullAt(2)).isTrue();
    }

    @Test
    void readsNestedBinaryRows() {
        final RowType nestedType = RowType.of(new IntType(), new VarCharType());
        final RowType type = RowType.of(new IntType(), nestedType);
        final BinaryRowData source =
                new RowDataSerializer(type)
                        .toBinaryRow(
                                GenericRowData.of(
                                        1, GenericRowData.of(2, StringData.fromString("nested"))));

        final RowData result = (RowData) converter.getValue(type, source);
        final RowData nested = result.getRow(1, 2);

        assertThat(result.getInt(0)).isEqualTo(1);
        assertThat(nested.getInt(0)).isEqualTo(2);
        assertThat(nested.getString(1).toString()).isEqualTo("nested");
    }

    @Test
    void preservesGenericRowNormalizationAndMissingFields() {
        final RowType type = RowType.of(new IntType(), new VarCharType(), new BooleanType());
        final GenericRowData source = GenericRowData.of(3, StringData.fromString("existing"));
        source.setRowKind(RowKind.UPDATE_AFTER);

        final RowData result = (RowData) converter.getValue(type, source);

        assertThat(result).isNotSameAs(source);
        assertThat(result.getRowKind()).isEqualTo(RowKind.INSERT);
        assertThat(result.getInt(0)).isEqualTo(3);
        assertThat(result.getString(1).toString()).isEqualTo("existing");
        assertThat(result.isNullAt(2)).isTrue();
    }
}
