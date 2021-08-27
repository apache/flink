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

package org.apache.flink.table.types.utils;

import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DayTimeIntervalType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeVisitor;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.NullType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.table.types.logical.SymbolType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.YearMonthIntervalType;
import org.apache.flink.table.types.logical.ZonedTimestampType;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/** A converter between {@link LogicalType} and {@link DataType}. */
public final class LogicalTypeDataTypeConverter {

    private static final DefaultDataTypeCreator dataTypeCreator = new DefaultDataTypeCreator();

    /** Returns the data type of a logical type without explicit conversions. */
    public static DataType toDataType(LogicalType logicalType) {
        return logicalType.accept(dataTypeCreator);
    }

    /** Returns the logical type of a data type. */
    public static LogicalType toLogicalType(DataType dataType) {
        return dataType.getLogicalType();
    }

    // --------------------------------------------------------------------------------------------

    private static class DefaultDataTypeCreator implements LogicalTypeVisitor<DataType> {

        @Override
        public DataType visit(CharType charType) {
            return new AtomicDataType(charType);
        }

        @Override
        public DataType visit(VarCharType varCharType) {
            return new AtomicDataType(varCharType);
        }

        @Override
        public DataType visit(BooleanType booleanType) {
            return new AtomicDataType(booleanType);
        }

        @Override
        public DataType visit(BinaryType binaryType) {
            return new AtomicDataType(binaryType);
        }

        @Override
        public DataType visit(VarBinaryType varBinaryType) {
            return new AtomicDataType(varBinaryType);
        }

        @Override
        public DataType visit(DecimalType decimalType) {
            return new AtomicDataType(decimalType);
        }

        @Override
        public DataType visit(TinyIntType tinyIntType) {
            return new AtomicDataType(tinyIntType);
        }

        @Override
        public DataType visit(SmallIntType smallIntType) {
            return new AtomicDataType(smallIntType);
        }

        @Override
        public DataType visit(IntType intType) {
            return new AtomicDataType(intType);
        }

        @Override
        public DataType visit(BigIntType bigIntType) {
            return new AtomicDataType(bigIntType);
        }

        @Override
        public DataType visit(FloatType floatType) {
            return new AtomicDataType(floatType);
        }

        @Override
        public DataType visit(DoubleType doubleType) {
            return new AtomicDataType(doubleType);
        }

        @Override
        public DataType visit(DateType dateType) {
            return new AtomicDataType(dateType);
        }

        @Override
        public DataType visit(TimeType timeType) {
            return new AtomicDataType(timeType);
        }

        @Override
        public DataType visit(TimestampType timestampType) {
            return new AtomicDataType(timestampType);
        }

        @Override
        public DataType visit(ZonedTimestampType zonedTimestampType) {
            return new AtomicDataType(zonedTimestampType);
        }

        @Override
        public DataType visit(LocalZonedTimestampType localZonedTimestampType) {
            return new AtomicDataType(localZonedTimestampType);
        }

        @Override
        public DataType visit(YearMonthIntervalType yearMonthIntervalType) {
            return new AtomicDataType(yearMonthIntervalType);
        }

        @Override
        public DataType visit(DayTimeIntervalType dayTimeIntervalType) {
            return new AtomicDataType(dayTimeIntervalType);
        }

        @Override
        public DataType visit(ArrayType arrayType) {
            return new CollectionDataType(arrayType, arrayType.getElementType().accept(this));
        }

        @Override
        public DataType visit(MultisetType multisetType) {
            return new CollectionDataType(multisetType, multisetType.getElementType().accept(this));
        }

        @Override
        public DataType visit(MapType mapType) {
            return new KeyValueDataType(
                    mapType,
                    mapType.getKeyType().accept(this),
                    mapType.getValueType().accept(this));
        }

        @Override
        public DataType visit(RowType rowType) {
            final List<DataType> fieldDataTypes =
                    rowType.getFields().stream()
                            .map(f -> f.getType().accept(this))
                            .collect(Collectors.toList());
            return new FieldsDataType(rowType, fieldDataTypes);
        }

        @Override
        public DataType visit(DistinctType distinctType) {
            return new FieldsDataType(
                    distinctType,
                    Collections.singletonList(distinctType.getSourceType().accept(this)));
        }

        @Override
        public DataType visit(StructuredType structuredType) {
            final List<DataType> attributeDataTypes =
                    structuredType.getAttributes().stream()
                            .map(a -> a.getType().accept(this))
                            .collect(Collectors.toList());
            return new FieldsDataType(structuredType, attributeDataTypes);
        }

        @Override
        public DataType visit(NullType nullType) {
            return new AtomicDataType(nullType);
        }

        @Override
        public DataType visit(RawType<?> rawType) {
            return new AtomicDataType(rawType);
        }

        @Override
        public DataType visit(SymbolType<?> symbolType) {
            return new AtomicDataType(symbolType);
        }

        @Override
        public DataType visit(LogicalType other) {
            return new AtomicDataType(other);
        }
    }

    // --------------------------------------------------------------------------------------------

    private LogicalTypeDataTypeConverter() {
        // do not instantiate
    }
}
