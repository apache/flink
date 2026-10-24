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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.data.conversion.DataStructureConverters;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the copying of internal state values by the {@link StateConverter}s. */
class StateConverterTest {

    /** POJO state used to cover structured-type state. */
    public static class Counter {
        public long count;
    }

    private static final DataType NESTED_ROW =
            DataTypes.ROW(DataTypes.FIELD("value", DataTypes.INT()));

    @Test
    void testListViewStateCopyIsIndependentOfItsElements() {
        final ListViewStateConverter converter =
                new ListViewStateConverter(
                        (ArrayType) DataTypes.ARRAY(NESTED_ROW).getLogicalType(),
                        converterFor(NESTED_ROW));

        final GenericRowData element = GenericRowData.of(1);
        final ArrayData copy =
                (ArrayData) converter.copyInternal(new GenericArrayData(new Object[] {element}));

        element.setField(0, 2);

        assertThat(copy.size()).isEqualTo(1);
        assertThat(copy.getRow(0, 1).getInt(0)).isEqualTo(1);
    }

    @Test
    void testListViewStateCopyKeepsNullElements() {
        final ListViewStateConverter converter =
                new ListViewStateConverter(
                        (ArrayType) DataTypes.ARRAY(DataTypes.INT()).getLogicalType(),
                        converterFor(DataTypes.INT()));

        final ArrayData copy =
                (ArrayData) converter.copyInternal(new GenericArrayData(new Object[] {1, null}));

        assertThat(copy.size()).isEqualTo(2);
        assertThat(copy.isNullAt(1)).isTrue();
        assertThat(copy.getInt(0)).isEqualTo(1);
    }

    @Test
    void testMapViewStateCopyIsIndependentOfItsValues() {
        final MapViewStateConverter converter =
                new MapViewStateConverter(
                        (MapType) DataTypes.MAP(DataTypes.STRING(), NESTED_ROW).getLogicalType(),
                        converterFor(DataTypes.STRING()),
                        converterFor(NESTED_ROW));

        final GenericRowData value = GenericRowData.of(1);
        final Map<Object, Object> entries = new HashMap<>();
        entries.put(StringData.fromString("a"), value);

        final MapData copy = (MapData) converter.copyInternal(new GenericMapData(entries));

        value.setField(0, 2);

        assertThat(copy.size()).isEqualTo(1);
        assertThat(copy.keyArray().getString(0).toString()).isEqualTo("a");
        assertThat(copy.valueArray().getRow(0, 1).getInt(0)).isEqualTo(1);
    }

    @Test
    void testRowStateCopyIsIndependentOfTheOriginal() {
        final DataType rowType = DataTypes.ROW(DataTypes.FIELD("count", DataTypes.BIGINT()));
        final RowStateConverter converter =
                new RowStateConverter((RowType) rowType.getLogicalType(), converterFor(rowType));

        final GenericRowData original = GenericRowData.of(1L);
        final RowData copy = (RowData) converter.copyInternal(original);

        original.setField(0, 2L);

        assertThat(copy.getLong(0)).isEqualTo(1L);
    }

    @Test
    void testStructuredTypeStateCopyIsADistinctInstance() {
        final DataType stateType =
                DataTypes.STRUCTURED(Counter.class, DataTypes.FIELD("count", DataTypes.BIGINT()));
        final StructuredTypeStateConverter converter =
                new StructuredTypeStateConverter(stateType, converterFor(stateType));

        final Counter counter = new Counter();
        counter.count = 7L;

        final Object internal = converter.toInternal(counter);
        final Object copy = converter.copyInternal(internal);

        assertThat(copy).isNotSameAs(internal);
        assertThat(((Counter) converter.toExternal(copy)).count).isEqualTo(7L);
    }

    @Test
    void testValueViewStateCopyIsIndependentOfItsValue() {
        final ValueViewStateConverter converter =
                new ValueViewStateConverter(NESTED_ROW, converterFor(NESTED_ROW));

        final GenericRowData value = GenericRowData.of(1);
        final RowData copy = (RowData) converter.copyInternal(value);

        value.setField(0, 2);

        assertThat(copy.getInt(0)).isEqualTo(1);
    }

    @Test
    void testValueViewStateCopyKeepsAnEmptyViewEmpty() {
        final ValueViewStateConverter converter =
                new ValueViewStateConverter(DataTypes.INT(), converterFor(DataTypes.INT()));

        // An empty value view is represented by a null internal value, so this is the only
        // converter whose copyInternal is legitimately handed null.
        assertThat(converter.copyInternal(converter.createNewInternalState())).isNull();
    }

    private static DataStructureConverter<Object, Object> converterFor(DataType dataType) {
        final DataStructureConverter<Object, Object> converter =
                DataStructureConverters.getConverter(dataType);
        converter.open(Thread.currentThread().getContextClassLoader());
        return converter;
    }
}
