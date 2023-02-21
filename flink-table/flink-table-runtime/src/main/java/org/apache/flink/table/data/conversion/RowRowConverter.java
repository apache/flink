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

package org.apache.flink.table.data.conversion;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowUtils;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.getFieldNames;

/** Converter for {@link RowType} of {@link Row} external type. */
@Internal
public class RowRowConverter implements DataStructureConverter<RowData, Row> {

    private static final long serialVersionUID = 1L;

    private final DataStructureConverter<Object, Object>[] fieldConverters;

    private final RowData.FieldGetter[] fieldGetters;

    private final LinkedHashMap<String, Integer> positionByName;

    private RowRowConverter(
            DataStructureConverter<Object, Object>[] fieldConverters,
            RowData.FieldGetter[] fieldGetters,
            LinkedHashMap<String, Integer> positionByName) {
        this.fieldConverters = fieldConverters;
        this.fieldGetters = fieldGetters;
        this.positionByName = positionByName;
    }

    @Override
    public void open(ClassLoader classLoader) {
        for (DataStructureConverter<Object, Object> fieldConverter : fieldConverters) {
            fieldConverter.open(classLoader);
        }
    }

    @Override
    public RowData toInternal(Row external) {
        final int length = fieldConverters.length;
        final GenericRowData genericRow = new GenericRowData(external.getKind(), length);

        final Set<String> fieldNames = external.getFieldNames(false);

        // position-based field access
        if (fieldNames == null) {
            for (int pos = 0; pos < length; pos++) {
                final Object value = external.getField(pos);
                genericRow.setField(pos, fieldConverters[pos].toInternalOrNull(value));
            }
        }
        // name-based field access
        else {
            for (String fieldName : fieldNames) {
                final Integer targetPos = positionByName.get(fieldName);
                if (targetPos == null) {
                    throw new IllegalArgumentException(
                            String.format(
                                    "Unknown field name '%s' for mapping to a row position. "
                                            + "Available names are: %s",
                                    fieldName, positionByName.keySet()));
                }
                final Object value = external.getField(fieldName);
                genericRow.setField(targetPos, fieldConverters[targetPos].toInternalOrNull(value));
            }
        }

        return genericRow;
    }

    @Override
    public Row toExternal(RowData internal) {
        final int length = fieldConverters.length;
        final Object[] fieldByPosition = new Object[length];
        for (int pos = 0; pos < length; pos++) {
            final Object value = fieldGetters[pos].getFieldOrNull(internal);
            fieldByPosition[pos] = fieldConverters[pos].toExternalOrNull(value);
        }
        return RowUtils.createRowWithNamedPositions(
                internal.getRowKind(), fieldByPosition, positionByName);
    }

    // --------------------------------------------------------------------------------------------
    // Factory method
    // --------------------------------------------------------------------------------------------

    @SuppressWarnings({"unchecked", "Convert2MethodRef"})
    public static RowRowConverter create(DataType dataType) {
        final List<DataType> fields = dataType.getChildren();
        final DataStructureConverter<Object, Object>[] fieldConverters =
                fields.stream()
                        .map(dt -> DataStructureConverters.getConverter(dt))
                        .toArray(DataStructureConverter[]::new);
        final RowData.FieldGetter[] fieldGetters =
                IntStream.range(0, fields.size())
                        .mapToObj(
                                pos ->
                                        RowData.createFieldGetter(
                                                fields.get(pos).getLogicalType(), pos))
                        .toArray(RowData.FieldGetter[]::new);
        final List<String> fieldNames = getFieldNames(dataType.getLogicalType());
        final LinkedHashMap<String, Integer> positionByName = new LinkedHashMap<>();
        for (int i = 0; i < fieldNames.size(); i++) {
            positionByName.put(fieldNames.get(i), i);
        }
        return new RowRowConverter(fieldConverters, fieldGetters, positionByName);
    }
}
