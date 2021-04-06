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

package org.apache.flink.table.runtime.types;

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.DecimalDataUtils;
import org.apache.flink.table.runtime.typeutils.BigDecimalTypeInfo;
import org.apache.flink.table.runtime.typeutils.DecimalDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyInstantTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyLocalDateTimeTypeInfo;
import org.apache.flink.table.runtime.typeutils.LegacyTimestampTypeInfo;
import org.apache.flink.table.runtime.typeutils.StringDataTypeInfo;
import org.apache.flink.table.runtime.typeutils.TimestampDataTypeInfo;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.table.runtime.types.TypeInfoLogicalTypeConverter.fromTypeInfoToLogicalType;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.BINARY_STRING;
import static org.apache.flink.table.types.logical.LogicalTypeFamily.CHARACTER_STRING;

/** Utilities for {@link LogicalType} and {@link DataType}.. */
public class PlannerTypeUtils {

    /**
     * A conversion that removes all {@link LegacyTypeInformationType}s by mapping to corresponding
     * new types.
     */
    public static LogicalType removeLegacyTypes(LogicalType t) {
        return t.accept(new LegacyTypeToPlannerTypeConverter());
    }

    public static boolean isPrimitive(LogicalType type) {
        return isPrimitive(type.getTypeRoot());
    }

    public static boolean isPrimitive(LogicalTypeRoot root) {
        switch (root) {
            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }

    /**
     * Can the two types operate with each other. Such as: 1.CodeGen: equal, cast, assignment.
     * 2.Join keys.
     */
    public static boolean isInteroperable(LogicalType t1, LogicalType t2) {
        if (t1.getTypeRoot().getFamilies().contains(CHARACTER_STRING)
                && t2.getTypeRoot().getFamilies().contains(CHARACTER_STRING)) {
            return true;
        }
        if (t1.getTypeRoot().getFamilies().contains(BINARY_STRING)
                && t2.getTypeRoot().getFamilies().contains(BINARY_STRING)) {
            return true;
        }
        if (t1.getTypeRoot() != t2.getTypeRoot()) {
            return false;
        }

        switch (t1.getTypeRoot()) {
            case ARRAY:
            case MAP:
            case MULTISET:
            case ROW:
                List<LogicalType> children1 = t1.getChildren();
                List<LogicalType> children2 = t2.getChildren();
                if (children1.size() != children2.size()) {
                    return false;
                }
                for (int i = 0; i < children1.size(); i++) {
                    if (!isInteroperable(children1.get(i), children2.get(i))) {
                        return false;
                    }
                }
                return true;
            default:
                return t1.copy(true).equals(t2.copy(true));
        }
    }

    /**
     * Now in the conversion to the TypeInformation from DataType, type may loose some information
     * about nullable and precision. So we add this method to do a soft check.
     *
     * <p>The difference of {@link #isInteroperable} is ignore precisions.
     */
    public static boolean isAssignable(LogicalType t1, LogicalType t2) {
        // Soft check for CharType, it is converted to String TypeInformation and loose char
        // information.
        if (t1.getTypeRoot().getFamilies().contains(CHARACTER_STRING)
                && t2.getTypeRoot().getFamilies().contains(CHARACTER_STRING)) {
            return true;
        }
        if (t1.getTypeRoot().getFamilies().contains(BINARY_STRING)
                && t2.getTypeRoot().getFamilies().contains(BINARY_STRING)) {
            return true;
        }
        if (t1.getTypeRoot() != t2.getTypeRoot()) {
            return false;
        }

        switch (t1.getTypeRoot()) {
                // only support precisions for DECIMAL, TIMESTAMP_WITHOUT_TIME_ZONE,
                // TIMESTAMP_WITH_LOCAL_TIME_ZONE
                // still consider precision for others (e.g. TIME).
                // TODO: add other precision types here in the future
            case DECIMAL:
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                return true;
            default:
                if (t1.getChildren().isEmpty()) {
                    return t1.copy(true).equals(t2.copy(true));
                } else {
                    List<LogicalType> children1 = t1.getChildren();
                    List<LogicalType> children2 = t2.getChildren();
                    if (children1.size() != children2.size()) {
                        return false;
                    }
                    for (int i = 0; i < children1.size(); i++) {
                        if (!isAssignable(children1.get(i), children2.get(i))) {
                            return false;
                        }
                    }
                    return true;
                }
        }
    }

    private static class LegacyTypeToPlannerTypeConverter
            extends LogicalTypeDefaultVisitor<LogicalType> {
        @Override
        protected LogicalType defaultMethod(LogicalType logicalType) {
            if (logicalType instanceof LegacyTypeInformationType) {
                TypeInformation<?> typeInfo =
                        ((LegacyTypeInformationType<?>) logicalType).getTypeInformation();
                if (typeInfo.equals(BasicTypeInfo.BIG_DEC_TYPE_INFO)) {
                    // BigDecimal have infinity precision and scale, but we converted it into a
                    // limited
                    // Decimal(38, 18). If the user's BigDecimal is more precision than this, we
                    // will
                    // throw Exception to remind user to use GenericType in real data conversion.
                    return DecimalDataUtils.DECIMAL_SYSTEM_DEFAULT;
                } else if (typeInfo.equals(StringDataTypeInfo.INSTANCE)) {
                    return DataTypes.STRING().getLogicalType();
                } else if (typeInfo instanceof BasicArrayTypeInfo) {
                    return new ArrayType(
                            fromTypeInfoToLogicalType(
                                    ((BasicArrayTypeInfo<?, ?>) typeInfo).getComponentInfo()));
                } else if (typeInfo instanceof CompositeType) {
                    CompositeType<?> compositeType = (CompositeType<?>) typeInfo;
                    return RowType.of(
                            Stream.iterate(0, x -> x + 1)
                                    .limit(compositeType.getArity())
                                    .map(
                                            (Function<Integer, TypeInformation<?>>)
                                                    compositeType::getTypeAt)
                                    .map(TypeInfoLogicalTypeConverter::fromTypeInfoToLogicalType)
                                    .toArray(LogicalType[]::new),
                            compositeType.getFieldNames());
                } else if (typeInfo instanceof DecimalDataTypeInfo) {
                    DecimalDataTypeInfo decimalType = (DecimalDataTypeInfo) typeInfo;
                    return new DecimalType(decimalType.precision(), decimalType.scale());
                } else if (typeInfo instanceof BigDecimalTypeInfo) {
                    BigDecimalTypeInfo decimalType = (BigDecimalTypeInfo) typeInfo;
                    return new DecimalType(decimalType.precision(), decimalType.scale());
                } else if (typeInfo instanceof TimestampDataTypeInfo) {
                    TimestampDataTypeInfo timestampDataTypeInfo = (TimestampDataTypeInfo) typeInfo;
                    return new TimestampType(timestampDataTypeInfo.getPrecision());
                } else if (typeInfo instanceof LegacyLocalDateTimeTypeInfo) {
                    LegacyLocalDateTimeTypeInfo dateTimeType =
                            (LegacyLocalDateTimeTypeInfo) typeInfo;
                    return new TimestampType(dateTimeType.getPrecision());
                } else if (typeInfo instanceof LegacyTimestampTypeInfo) {
                    LegacyTimestampTypeInfo timstampType = (LegacyTimestampTypeInfo) typeInfo;
                    return new TimestampType(timstampType.getPrecision());
                } else if (typeInfo instanceof LegacyInstantTypeInfo) {
                    LegacyInstantTypeInfo instantTypeInfo = (LegacyInstantTypeInfo) typeInfo;
                    return new LocalZonedTimestampType(instantTypeInfo.getPrecision());
                } else if (typeInfo instanceof InternalTypeInfo) {
                    return ((InternalTypeInfo<?>) typeInfo).toLogicalType();
                } else {
                    return new TypeInformationRawType<>(typeInfo);
                }
            } else {
                return logicalType;
            }
        }

        @Override
        public LogicalType visit(ArrayType arrayType) {
            return new ArrayType(arrayType.isNullable(), arrayType.getElementType().accept(this));
        }

        @Override
        public LogicalType visit(MultisetType multisetType) {
            return new MultisetType(
                    multisetType.isNullable(), multisetType.getElementType().accept(this));
        }

        @Override
        public LogicalType visit(MapType mapType) {
            return new MapType(
                    mapType.isNullable(),
                    mapType.getKeyType().accept(this),
                    mapType.getValueType().accept(this));
        }

        @Override
        public LogicalType visit(RowType rowType) {
            return new RowType(
                    rowType.isNullable(),
                    rowType.getFields().stream()
                            .map(
                                    field ->
                                            new RowType.RowField(
                                                    field.getName(),
                                                    field.getType()
                                                            .accept(
                                                                    LegacyTypeToPlannerTypeConverter
                                                                            .this)))
                            .collect(Collectors.toList()));
        }
    }
}
