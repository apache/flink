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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.CompositeType;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.PojoField;
import org.apache.flink.api.java.typeutils.PojoTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfoBase;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.extraction.ExtractionUtils;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RawType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.StructuredType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.extraction.ExtractionUtils.extractAssigningConstructor;
import static org.apache.flink.table.types.extraction.ExtractionUtils.getStructuredField;

/**
 * Converter from {@link TypeInformation} to {@link DataType}.
 *
 * <p>{@link DataType} is richer than {@link TypeInformation} as it also includes details about the
 * {@link LogicalType}. Therefore, some details will be added implicitly during the conversion. The
 * conversion from {@link DataType} to {@link TypeInformation} is provided by the planner.
 *
 * <p>The behavior of this converter can be summarized as follows:
 *
 * <ul>
 *   <li>All subclasses of {@link TypeInformation} are mapped to {@link LogicalType} including
 *       nullability that is aligned with serializers.
 *   <li>{@link TupleTypeInfoBase} is translated into {@link RowType} or {@link StructuredType}.
 *   <li>{@link BigDecimal} is converted to {@code DECIMAL(38, 18)} by default.
 *   <li>The order of {@link PojoTypeInfo} fields is determined by the converter.
 *   <li>{@link GenericTypeInfo} and other type information that cannot be mapped to a logical type
 *       is converted to {@link RawType} by considering the current configuration.
 *   <li>{@link TypeInformation} that originated from Table API will keep its {@link DataType}
 *       information when implementing {@link DataTypeQueryable}.
 * </ul>
 */
@Internal
public final class TypeInfoDataTypeConverter {

    private static final Map<TypeInformation<?>, DataType> conversionMap = new HashMap<>();

    static {
        conversionMap.put(Types.STRING, DataTypes.STRING().nullable().bridgedTo(String.class));
        conversionMap.put(Types.BOOLEAN, DataTypes.BOOLEAN().notNull().bridgedTo(Boolean.class));
        conversionMap.put(Types.BYTE, DataTypes.TINYINT().notNull().bridgedTo(Byte.class));
        conversionMap.put(Types.SHORT, DataTypes.SMALLINT().notNull().bridgedTo(Short.class));
        conversionMap.put(Types.INT, DataTypes.INT().notNull().bridgedTo(Integer.class));
        conversionMap.put(Types.LONG, DataTypes.BIGINT().notNull().bridgedTo(Long.class));
        conversionMap.put(Types.FLOAT, DataTypes.FLOAT().notNull().bridgedTo(Float.class));
        conversionMap.put(Types.DOUBLE, DataTypes.DOUBLE().notNull().bridgedTo(Double.class));
        conversionMap.put(
                Types.BIG_DEC, DataTypes.DECIMAL(38, 18).nullable().bridgedTo(BigDecimal.class));
        conversionMap.put(Types.LOCAL_DATE, DataTypes.DATE().nullable().bridgedTo(LocalDate.class));
        conversionMap.put(
                Types.LOCAL_TIME, DataTypes.TIME(9).nullable().bridgedTo(LocalTime.class));
        conversionMap.put(
                Types.LOCAL_DATE_TIME,
                DataTypes.TIMESTAMP(9).nullable().bridgedTo(LocalDateTime.class));
        conversionMap.put(
                Types.INSTANT,
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(9).bridgedTo(Instant.class));
        conversionMap.put(
                Types.SQL_DATE, DataTypes.DATE().nullable().bridgedTo(java.sql.Date.class));
        conversionMap.put(
                Types.SQL_TIME, DataTypes.TIME(0).nullable().bridgedTo(java.sql.Time.class));
        conversionMap.put(
                Types.SQL_TIMESTAMP,
                DataTypes.TIMESTAMP(9).nullable().bridgedTo(java.sql.Timestamp.class));
        conversionMap.put(
                PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.ARRAY(DataTypes.BOOLEAN().notNull().bridgedTo(boolean.class))
                        .notNull()
                        .bridgedTo(boolean[].class));
        conversionMap.put(
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.BYTES().notNull().bridgedTo(byte[].class));
        conversionMap.put(
                PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.ARRAY(DataTypes.SMALLINT().notNull().bridgedTo(short.class))
                        .notNull()
                        .bridgedTo(short[].class));
        conversionMap.put(
                PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.ARRAY(DataTypes.INT().notNull().bridgedTo(int.class))
                        .notNull()
                        .bridgedTo(int[].class));
        conversionMap.put(
                PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.ARRAY(DataTypes.BIGINT().notNull().bridgedTo(long.class))
                        .notNull()
                        .bridgedTo(long[].class));
        conversionMap.put(
                PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.ARRAY(DataTypes.FLOAT().notNull().bridgedTo(float.class))
                        .notNull()
                        .bridgedTo(float[].class));
        conversionMap.put(
                PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.ARRAY(DataTypes.DOUBLE().notNull().bridgedTo(double.class))
                        .bridgedTo(double[].class));
    }

    /** Converts the given {@link TypeInformation} into {@link DataType}. */
    @SuppressWarnings("rawtypes")
    public static DataType toDataType(
            DataTypeFactory dataTypeFactory, TypeInformation<?> typeInfo) {
        if (typeInfo instanceof DataTypeQueryable) {
            return ((DataTypeQueryable) typeInfo).getDataType();
        }

        final DataType foundDataType = conversionMap.get(typeInfo);
        if (foundDataType != null) {
            return foundDataType;
        }

        if (typeInfo instanceof RowTypeInfo) {
            return convertToRowType(dataTypeFactory, (RowTypeInfo) typeInfo);
        } else if (typeInfo instanceof ObjectArrayTypeInfo) {
            return convertToArrayType(
                    dataTypeFactory,
                    typeInfo.getTypeClass(),
                    ((ObjectArrayTypeInfo) typeInfo).getComponentInfo());
        } else if (typeInfo instanceof BasicArrayTypeInfo) {
            return convertToArrayType(
                    dataTypeFactory,
                    typeInfo.getTypeClass(),
                    ((BasicArrayTypeInfo) typeInfo).getComponentInfo());
        } else if (typeInfo instanceof ListTypeInfo) {
            return convertToListArrayType(
                    dataTypeFactory, ((ListTypeInfo) typeInfo).getElementTypeInfo());
        } else if (typeInfo instanceof MultisetTypeInfo) {
            return convertToMultisetType(
                    dataTypeFactory, ((MultisetTypeInfo) typeInfo).getElementTypeInfo());
        } else if (typeInfo instanceof MapTypeInfo) {
            return convertToMapType(
                    dataTypeFactory,
                    ((MapTypeInfo) typeInfo).getKeyTypeInfo(),
                    ((MapTypeInfo) typeInfo).getValueTypeInfo());
        } else if (typeInfo instanceof CompositeType) {
            return convertToStructuredType(dataTypeFactory, (CompositeType) typeInfo);
        }

        // treat everything else as RAW type
        return dataTypeFactory.createRawDataType(typeInfo);
    }

    private static DataType convertToRowType(
            DataTypeFactory dataTypeFactory, RowTypeInfo rowTypeInfo) {
        final String[] fieldNames = rowTypeInfo.getFieldNames();
        final DataTypes.Field[] fields =
                IntStream.range(0, rowTypeInfo.getArity())
                        .mapToObj(
                                i -> {
                                    DataType fieldType =
                                            toDataType(dataTypeFactory, rowTypeInfo.getTypeAt(i));
                                    // all fields are nullable
                                    return DataTypes.FIELD(fieldNames[i], fieldType.nullable());
                                })
                        .toArray(DataTypes.Field[]::new);
        return DataTypes.ROW(fields).notNull().bridgedTo(Row.class);
    }

    private static DataType convertToArrayType(
            DataTypeFactory dataTypeFactory,
            Class<?> arrayClass,
            TypeInformation<?> elementTypeInfo) {
        // all elements are nullable
        return DataTypes.ARRAY(toDataType(dataTypeFactory, elementTypeInfo).nullable())
                .notNull()
                .bridgedTo(arrayClass);
    }

    private static DataType convertToListArrayType(
            DataTypeFactory dataTypeFactory, TypeInformation<?> elementTypeInfo) {
        // element nullability depends on the data type
        return DataTypes.ARRAY(toDataType(dataTypeFactory, elementTypeInfo))
                .notNull()
                .bridgedTo(List.class);
    }

    private static DataType convertToMultisetType(
            DataTypeFactory dataTypeFactory, TypeInformation<?> elementTypeInfo) {
        // all elements are nullable
        return DataTypes.MULTISET(toDataType(dataTypeFactory, elementTypeInfo).nullable())
                .notNull()
                .bridgedTo(Map.class);
    }

    private static DataType convertToMapType(
            DataTypeFactory dataTypeFactory,
            TypeInformation<?> keyTypeInfo,
            TypeInformation<?> valueTypeInfo) {
        // all values are nullable, key nullability depends on the data type
        return DataTypes.MAP(
                        toDataType(dataTypeFactory, keyTypeInfo),
                        toDataType(dataTypeFactory, valueTypeInfo).nullable())
                .notNull()
                .bridgedTo(Map.class);
    }

    private static DataType convertToStructuredType(
            DataTypeFactory dataTypeFactory, CompositeType<?> compositeType) {
        final int arity = compositeType.getArity();
        final String[] fieldNames = compositeType.getFieldNames();
        final Class<?> typeClass = compositeType.getTypeClass();

        final Map<String, DataType> fieldDataTypes = new LinkedHashMap<>();
        IntStream.range(0, arity)
                .forEachOrdered(
                        pos ->
                                fieldDataTypes.put(
                                        fieldNames[pos],
                                        toDataType(dataTypeFactory, compositeType.getTypeAt(pos))));

        final List<String> fieldNamesReordered;
        final boolean isNullable;
        // for POJOs and Avro records
        if (compositeType instanceof PojoTypeInfo) {
            final PojoTypeInfo<?> pojoTypeInfo = (PojoTypeInfo<?>) compositeType;
            final List<Field> pojoFields =
                    IntStream.range(0, arity)
                            .mapToObj(pojoTypeInfo::getPojoFieldAt)
                            .map(PojoField::getField)
                            .collect(Collectors.toList());

            // POJO serializer supports top-level nulls
            isNullable = true;

            // based on type information all fields are boxed classes,
            // therefore we need to check the reflective field for more details
            fieldDataTypes.replaceAll(
                    (name, dataType) -> {
                        final Class<?> fieldClass =
                                pojoFields.stream()
                                        .filter(f -> f.getName().equals(name))
                                        .findFirst()
                                        .orElseThrow(IllegalStateException::new)
                                        .getType();
                        if (fieldClass.isPrimitive()) {
                            return dataType.notNull().bridgedTo(fieldClass);
                        }
                        // serializer supports nullable fields
                        return dataType.nullable();
                    });

            // best effort extraction of the field order, if it fails we use the default order of
            // PojoTypeInfo which is alphabetical
            fieldNamesReordered = extractStructuredTypeFieldOrder(typeClass, pojoFields);
        }
        // for tuples and case classes
        else {
            // serializers don't support top-level nulls
            isNullable = false;

            // based on type information all fields are boxed classes,
            // but case classes might contain primitives
            fieldDataTypes.replaceAll(
                    (name, dataType) -> {
                        try {
                            final Class<?> fieldClass =
                                    getStructuredField(typeClass, name).getType();
                            if (fieldClass.isPrimitive()) {
                                return dataType.notNull().bridgedTo(fieldClass);
                            }
                        } catch (Throwable t) {
                            // ignore extraction errors and keep the original conversion class
                        }
                        return dataType;
                    });

            // field order from type information is correct
            fieldNamesReordered = null;
        }

        final DataTypes.Field[] structuredFields;
        if (fieldNamesReordered != null) {
            structuredFields =
                    fieldNamesReordered.stream()
                            .map(name -> DataTypes.FIELD(name, fieldDataTypes.get(name)))
                            .toArray(DataTypes.Field[]::new);
        } else {
            structuredFields =
                    fieldDataTypes.entrySet().stream()
                            .map(e -> DataTypes.FIELD(e.getKey(), e.getValue()))
                            .toArray(DataTypes.Field[]::new);
        }

        final DataType structuredDataType = DataTypes.STRUCTURED(typeClass, structuredFields);
        if (isNullable) {
            return structuredDataType.nullable();
        } else {
            return structuredDataType.notNull();
        }
    }

    private static @Nullable List<String> extractStructuredTypeFieldOrder(
            Class<?> typeClass, List<Field> fields) {
        try {
            final ExtractionUtils.AssigningConstructor constructor =
                    extractAssigningConstructor(typeClass, fields);
            if (constructor == null) {
                return null;
            }
            return constructor.parameterNames;
        } catch (Throwable t) {
            return null;
        }
    }
}
