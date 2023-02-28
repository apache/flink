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

package org.apache.flink.formats.parquet.utils;

import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.parquet.schema.ConversionPatterns;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;

import java.util.ArrayList;
import java.util.List;

/** Schema converter converts Parquet schema to and from Flink internal types. */
public class ParquetSchemaConverter {

    static final String MAP_REPEATED_NAME = "key_value";
    static final String LIST_ELEMENT_NAME = "element";

    public static MessageType convertToParquetMessageType(String name, RowType rowType) {
        Type[] types = new Type[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            types[i] = convertToParquetType(rowType.getFieldNames().get(i), rowType.getTypeAt(i));
        }
        return new MessageType(name, types);
    }

    public static Type convertToParquetType(String name, LogicalType type) {
        return convertToParquetType(name, type, Type.Repetition.OPTIONAL);
    }

    private static Type convertToParquetType(
            String name, LogicalType type, Type.Repetition repetition) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                        .as(OriginalType.UTF8)
                        .named(name);
            case BOOLEAN:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition)
                        .named(name);
            case BINARY:
            case VARBINARY:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                        .named(name);
            case DECIMAL:
                int precision = ((DecimalType) type).getPrecision();
                int scale = ((DecimalType) type).getScale();
                int numBytes = computeMinBytesForDecimalPrecision(precision);
                return Types.primitive(
                                PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, repetition)
                        .precision(precision)
                        .scale(scale)
                        .length(numBytes)
                        .as(OriginalType.DECIMAL)
                        .named(name);
            case TINYINT:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                        .as(OriginalType.INT_8)
                        .named(name);
            case SMALLINT:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                        .as(OriginalType.INT_16)
                        .named(name);
            case INTEGER:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                        .named(name);
            case BIGINT:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                        .named(name);
            case FLOAT:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition)
                        .named(name);
            case DOUBLE:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition)
                        .named(name);
            case DATE:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                        .as(OriginalType.DATE)
                        .named(name);
            case TIME_WITHOUT_TIME_ZONE:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                        .as(OriginalType.TIME_MILLIS)
                        .named(name);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return Types.primitive(PrimitiveType.PrimitiveTypeName.INT96, repetition)
                        .named(name);
            case ARRAY:
                ArrayType arrayType = (ArrayType) type;
                return ConversionPatterns.listOfElements(
                        repetition,
                        name,
                        convertToParquetType(LIST_ELEMENT_NAME, arrayType.getElementType()));
            case MAP:
                MapType mapType = (MapType) type;
                return ConversionPatterns.mapType(
                        repetition,
                        name,
                        MAP_REPEATED_NAME,
                        convertToParquetType("key", mapType.getKeyType()),
                        convertToParquetType("value", mapType.getValueType()));
            case MULTISET:
                MultisetType multisetType = (MultisetType) type;
                return ConversionPatterns.mapType(
                        repetition,
                        name,
                        MAP_REPEATED_NAME,
                        convertToParquetType("key", multisetType.getElementType()),
                        convertToParquetType("value", new IntType(false)));
            case ROW:
                RowType rowType = (RowType) type;
                return new GroupType(repetition, name, convertToParquetTypes(rowType));
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    private static List<Type> convertToParquetTypes(RowType rowType) {
        List<Type> types = new ArrayList<>(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            types.add(convertToParquetType(rowType.getFieldNames().get(i), rowType.getTypeAt(i)));
        }
        return types;
    }

    public static int computeMinBytesForDecimalPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }

    // From DecimalDataUtils
    public static boolean is32BitDecimal(int precision) {
        return precision <= 9;
    }

    public static boolean is64BitDecimal(int precision) {
        return precision <= 18 && precision > 9;
    }
}
