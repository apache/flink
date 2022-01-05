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

package org.apache.flink.table.test;

import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.types.Row;

import org.junit.jupiter.api.Assertions;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Function;

class InternalDataUtils {

    static Object toGenericInternalData(Object object, LogicalType logicalType) {
        if (object instanceof RowData) {
            return toGenericRow((RowData) object, logicalType);
        } else if (object instanceof ArrayData) {
            return toGenericArray((ArrayData) object, logicalType);
        } else if (object instanceof MapData) {
            return toGenericMap((MapData) object, logicalType);
        }
        return object;
    }

    static GenericRowData toGenericRow(RowData rowData, LogicalType logicalType) {
        final List<LogicalType> fieldTypes = LogicalTypeChecks.getFieldTypes(logicalType);

        final GenericRowData row = new GenericRowData(fieldTypes.size());
        row.setRowKind(rowData.getRowKind());

        for (int i = 0; i < fieldTypes.size(); i++) {
            if (rowData.isNullAt(i)) {
                row.setField(i, null);
            } else {
                LogicalType fieldType = fieldTypes.get(i);
                RowData.FieldGetter fieldGetter = RowData.createFieldGetter(fieldType, i);
                row.setField(
                        i, toGenericInternalData(fieldGetter.getFieldOrNull(rowData), fieldType));
            }
        }
        return row;
    }

    static GenericArrayData toGenericArray(ArrayData arrayData, LogicalType logicalType) {
        final LogicalType innerElement = ((ArrayType) logicalType).getElementType();
        final ArrayData.ElementGetter elementGetter = ArrayData.createElementGetter(innerElement);

        final Object[] newArray = new Object[arrayData.size()];

        for (int i = 0; i < arrayData.size(); i++) {
            if (arrayData.isNullAt(i)) {
                newArray[i] = null;
            } else {
                newArray[i] =
                        toGenericInternalData(
                                elementGetter.getElementOrNull(arrayData, i), innerElement);
            }
        }
        return new GenericArrayData(newArray);
    }

    static GenericMapData toGenericMap(MapData mapData, LogicalType logicalType) {
        final LogicalType keyType =
                logicalType.is(LogicalTypeRoot.MULTISET)
                        ? ((MultisetType) logicalType).getElementType()
                        : ((MapType) logicalType).getKeyType();
        final LogicalType valueType =
                logicalType.is(LogicalTypeRoot.MULTISET)
                        ? new IntType(false)
                        : ((MapType) logicalType).getValueType();

        final ArrayData.ElementGetter keyGetter = ArrayData.createElementGetter(keyType);
        final ArrayData.ElementGetter valueGetter = ArrayData.createElementGetter(keyType);

        final ArrayData keys = mapData.keyArray();
        final ArrayData values = mapData.valueArray();

        final LinkedHashMap<Object, Object> newMap = new LinkedHashMap<>();

        for (int i = 0; i < mapData.size(); i++) {
            Object key = null;
            Object value = null;
            if (!keys.isNullAt(i)) {
                key = toGenericInternalData(keyGetter.getElementOrNull(keys, i), keyType);
            }
            if (!values.isNullAt(i)) {
                value = toGenericInternalData(valueGetter.getElementOrNull(values, i), valueType);
            }
            newMap.put(key, value);
        }
        return new GenericMapData(newMap);
    }

    static Function<RowData, Row> resolveToExternalOrNull(DataType dataType) {
        try {
            // Create the converter
            Method getConverter =
                    Class.forName("org.apache.flink.table.data.conversion.DataStructureConverters")
                            .getMethod("getConverter", DataType.class);
            Object converter = getConverter.invoke(null, dataType);

            // Open the converter
            converter
                    .getClass()
                    .getMethod("open", ClassLoader.class)
                    .invoke(converter, Thread.currentThread().getContextClassLoader());
            Method toExternalOrNull =
                    converter.getClass().getMethod("toExternalOrNull", Object.class);

            // Return the lambda to invoke the converter
            return rowData -> {
                try {
                    return (Row) toExternalOrNull.invoke(converter, rowData);
                } catch (IllegalAccessException | InvocationTargetException e) {
                    Assertions.fail(
                            "Something went wrong when trying to use the DataStructureConverter from flink-table-runtime",
                            e);
                    return null; // For the compiler
                }
            };
        } catch (ClassNotFoundException
                | InvocationTargetException
                | NoSuchMethodException
                | IllegalAccessException e) {
            Assertions.fail(
                    "Error when trying to use the RowData to Row conversion. "
                            + "Perhaps you miss flink-table-runtime in your test classpath?",
                    e);
            return null; // For the compiler
        }
    }
}
