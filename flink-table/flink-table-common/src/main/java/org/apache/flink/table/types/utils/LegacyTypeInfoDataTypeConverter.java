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
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.MultisetTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.AtomicDataType;
import org.apache.flink.table.types.CollectionDataType;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.DataTypeQueryable;
import org.apache.flink.table.types.FieldsDataType;
import org.apache.flink.table.types.KeyValueDataType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TimestampKind;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.hasRoot;
import static org.apache.flink.table.types.logical.utils.LogicalTypeChecks.isRowtimeAttribute;

/**
 * Converter between {@link TypeInformation} and {@link DataType} that reflects the behavior before
 * Flink 1.9. The conversion is a 1:1 mapping that allows back-and-forth conversion. Note that
 * nullability constraints might get lost during the back-and-forth conversion.
 *
 * <p>This converter only exists to still support deprecated methods that take or return {@link
 * TypeInformation}. Some methods will still support type information in the future, however, the
 * future type information support will integrate nicer with the new type stack.
 *
 * <p>This converter reflects the old behavior that includes:
 *
 * <ul>
 *   <li>Use old {@code java.sql.*} time classes for time data types.
 *   <li>Only support millisecond precision for timestamps or day-time intervals.
 *   <li>Let variable precision and scale for decimal types pass through the planner.
 *   <li>Inconsistent nullability. Most types are nullable even though type information does not
 *       support it.
 *   <li>Distinction between {@link BasicArrayTypeInfo} and {@link ObjectArrayTypeInfo}.
 *   <li>Let POJOs, case classes, and tuples pass through the planner.
 * </ul>
 *
 * <p>Any changes here need to be applied to the legacy planner as well.
 *
 * @deprecated Use {@link DataTypeFactory#createDataType(TypeInformation)} instead. Note that this
 *     method will not create legacy types anymore. It fully uses the new type system available only
 *     in the Blink planner.
 */
@Internal
@Deprecated
public final class LegacyTypeInfoDataTypeConverter {

    private static final Map<TypeInformation<?>, DataType> typeInfoDataTypeMap = new HashMap<>();
    private static final Map<DataType, TypeInformation<?>> dataTypeTypeInfoMap = new HashMap<>();

    static {
        addMapping(Types.STRING, DataTypes.STRING().bridgedTo(String.class));
        addMapping(Types.BOOLEAN, DataTypes.BOOLEAN().bridgedTo(Boolean.class));
        addMapping(Types.BYTE, DataTypes.TINYINT().bridgedTo(Byte.class));
        addMapping(Types.SHORT, DataTypes.SMALLINT().bridgedTo(Short.class));
        addMapping(Types.INT, DataTypes.INT().bridgedTo(Integer.class));
        addMapping(Types.LONG, DataTypes.BIGINT().bridgedTo(Long.class));
        addMapping(Types.FLOAT, DataTypes.FLOAT().bridgedTo(Float.class));
        addMapping(Types.DOUBLE, DataTypes.DOUBLE().bridgedTo(Double.class));
        addMapping(Types.BIG_DEC, createLegacyType(LogicalTypeRoot.DECIMAL, Types.BIG_DEC));
        addMapping(Types.LOCAL_DATE, DataTypes.DATE().bridgedTo(LocalDate.class));
        addMapping(Types.LOCAL_TIME, DataTypes.TIME(0).bridgedTo(LocalTime.class));
        addMapping(Types.LOCAL_DATE_TIME, DataTypes.TIMESTAMP(3).bridgedTo(LocalDateTime.class));
        addMapping(
                Types.INSTANT,
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3).bridgedTo(Instant.class));
        addMapping(Types.SQL_DATE, DataTypes.DATE().bridgedTo(java.sql.Date.class));
        addMapping(Types.SQL_TIME, DataTypes.TIME(0).bridgedTo(java.sql.Time.class));
        addMapping(Types.SQL_TIMESTAMP, DataTypes.TIMESTAMP(3).bridgedTo(java.sql.Timestamp.class));
        addMapping(
                TimeIntervalTypeInfo.INTERVAL_MONTHS,
                DataTypes.INTERVAL(DataTypes.MONTH()).bridgedTo(Integer.class));
        addMapping(
                TimeIntervalTypeInfo.INTERVAL_MILLIS,
                DataTypes.INTERVAL(DataTypes.SECOND(3)).bridgedTo(Long.class));
        addMapping(
                PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.ARRAY(DataTypes.BOOLEAN().notNull().bridgedTo(boolean.class))
                        .bridgedTo(boolean[].class));
        addMapping(
                PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.BYTES().bridgedTo(byte[].class));
        addMapping(
                PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.ARRAY(DataTypes.SMALLINT().notNull().bridgedTo(short.class))
                        .bridgedTo(short[].class));
        addMapping(
                PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.ARRAY(DataTypes.INT().notNull().bridgedTo(int.class))
                        .bridgedTo(int[].class));
        addMapping(
                PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.ARRAY(DataTypes.BIGINT().notNull().bridgedTo(long.class))
                        .bridgedTo(long[].class));
        addMapping(
                PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.ARRAY(DataTypes.FLOAT().notNull().bridgedTo(float.class))
                        .bridgedTo(float[].class));
        addMapping(
                PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO,
                DataTypes.ARRAY(DataTypes.DOUBLE().notNull().bridgedTo(double.class))
                        .bridgedTo(double[].class));
    }

    private static void addMapping(TypeInformation<?> typeInfo, DataType dataType) {
        Preconditions.checkArgument(!typeInfoDataTypeMap.containsKey(typeInfo));
        typeInfoDataTypeMap.put(typeInfo, dataType);
        dataTypeTypeInfoMap.put(dataType, typeInfo);
    }

    public static DataType toDataType(TypeInformation<?> typeInfo) {
        // time indicators first as their hashCode/equals is shared with those of regular timestamps
        if (typeInfo instanceof TimeIndicatorTypeInfo) {
            return convertToTimeAttributeType((TimeIndicatorTypeInfo) typeInfo);
        }

        final DataType foundDataType = typeInfoDataTypeMap.get(typeInfo);
        if (foundDataType != null) {
            return foundDataType;
        }

        if (typeInfo instanceof RowTypeInfo) {
            return convertToRowType((RowTypeInfo) typeInfo);
        } else if (typeInfo instanceof ObjectArrayTypeInfo) {
            return convertToArrayType(
                    typeInfo.getTypeClass(), ((ObjectArrayTypeInfo) typeInfo).getComponentInfo());
        } else if (typeInfo instanceof BasicArrayTypeInfo) {
            return createLegacyType(LogicalTypeRoot.ARRAY, typeInfo);
        } else if (typeInfo instanceof MultisetTypeInfo) {
            return convertToMultisetType(((MultisetTypeInfo) typeInfo).getElementTypeInfo());
        } else if (typeInfo instanceof MapTypeInfo) {
            return convertToMapType((MapTypeInfo) typeInfo);
        } else if (typeInfo instanceof CompositeType || isRowData(typeInfo)) {
            return createLegacyType(LogicalTypeRoot.STRUCTURED_TYPE, typeInfo);
        }

        return createLegacyType(LogicalTypeRoot.RAW, typeInfo);
    }

    public static TypeInformation<?> toLegacyTypeInfo(DataType dataType) {
        // time indicators first as their hashCode/equals is shared with those of regular timestamps
        if (canConvertToTimeAttributeTypeInfo(dataType)) {
            return convertToTimeAttributeTypeInfo((TimestampType) dataType.getLogicalType());
        }

        // check in the map but relax the nullability constraint as every not null data type can be
        // stored in the corresponding nullable type information
        final TypeInformation<?> foundTypeInfo = dataTypeTypeInfoMap.get(dataType.nullable());
        if (foundTypeInfo != null) {
            return foundTypeInfo;
        }

        // we are relaxing the constraint for DECIMAL, CHAR, VARCHAR, TIMESTAMP_WITHOUT_TIME_ZONE to
        // support value literals in legacy planner
        LogicalType logicalType = dataType.getLogicalType();
        if (hasRoot(logicalType, LogicalTypeRoot.DECIMAL)) {
            return Types.BIG_DEC;
        } else if (hasRoot(logicalType, LogicalTypeRoot.CHAR)) {
            return Types.STRING;
        } else if (hasRoot(logicalType, LogicalTypeRoot.VARCHAR)) {
            return Types.STRING;
        }

        // relax the precision constraint as Timestamp can store the highest precision
        else if (hasRoot(logicalType, LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                && dataType.getConversionClass() == Timestamp.class) {
            return Types.SQL_TIMESTAMP;
        }

        // relax the precision constraint as LocalDateTime can store the highest precision
        else if (hasRoot(logicalType, LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                && dataType.getConversionClass() == LocalDateTime.class) {
            return Types.LOCAL_DATE_TIME;
        }

        // relax the precision constraint as LocalTime can store the highest precision
        else if (hasRoot(logicalType, LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE)
                && dataType.getConversionClass() == LocalTime.class) {
            return Types.LOCAL_TIME;
        } else if (canConvertToLegacyTypeInfo(dataType)) {
            return convertToLegacyTypeInfo(dataType);
        } else if (canConvertToRowTypeInfo(dataType)) {
            return convertToRowTypeInfo((FieldsDataType) dataType);
        }

        // this could also match for basic array type info but this is covered by legacy type info
        else if (canConvertToObjectArrayTypeInfo(dataType)) {
            return convertToObjectArrayTypeInfo((CollectionDataType) dataType);
        } else if (canConvertToMultisetTypeInfo(dataType)) {
            return convertToMultisetTypeInfo((CollectionDataType) dataType);
        } else if (canConvertToMapTypeInfo(dataType)) {
            return convertToMapTypeInfo((KeyValueDataType) dataType);
        }

        // makes the raw type accessible in the legacy planner
        else if (canConvertToRawTypeInfo(dataType)) {
            return convertToRawTypeInfo(dataType);
        }

        throw new TableException(
                String.format(
                        "Unsupported conversion from data type '%s' (conversion class: %s) to type information. Only data types "
                                + "that originated from type information fully support a reverse conversion.",
                        dataType, dataType.getConversionClass().getName()));
    }

    private static DataType createLegacyType(
            LogicalTypeRoot typeRoot, TypeInformation<?> typeInfo) {
        return new AtomicDataType(new LegacyTypeInformationType<>(typeRoot, typeInfo))
                .bridgedTo(typeInfo.getTypeClass());
    }

    private static DataType convertToTimeAttributeType(
            TimeIndicatorTypeInfo timeIndicatorTypeInfo) {
        final TimestampKind kind;
        if (timeIndicatorTypeInfo.isEventTime()) {
            kind = TimestampKind.ROWTIME;
        } else {
            kind = TimestampKind.PROCTIME;
        }
        return new AtomicDataType(new TimestampType(true, kind, 3))
                .bridgedTo(java.sql.Timestamp.class);
    }

    private static boolean canConvertToTimeAttributeTypeInfo(DataType dataType) {
        return hasRoot(dataType.getLogicalType(), LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE)
                && dataTypeTypeInfoMap.containsKey(dataType.nullable())
                && // checks precision and conversion and ignore nullable
                ((TimestampType) dataType.getLogicalType()).getKind() != TimestampKind.REGULAR;
    }

    private static TypeInformation<?> convertToTimeAttributeTypeInfo(TimestampType timestampType) {
        if (isRowtimeAttribute(timestampType)) {
            return TimeIndicatorTypeInfo.ROWTIME_INDICATOR;
        } else {
            return TimeIndicatorTypeInfo.PROCTIME_INDICATOR;
        }
    }

    private static DataType convertToRowType(RowTypeInfo rowTypeInfo) {
        final String[] fieldNames = rowTypeInfo.getFieldNames();
        final DataTypes.Field[] fields =
                IntStream.range(0, rowTypeInfo.getArity())
                        .mapToObj(
                                i -> {
                                    DataType fieldType = toDataType(rowTypeInfo.getTypeAt(i));

                                    return DataTypes.FIELD(fieldNames[i], fieldType);
                                })
                        .toArray(DataTypes.Field[]::new);

        return DataTypes.ROW(fields).bridgedTo(Row.class);
    }

    private static boolean canConvertToRowTypeInfo(DataType dataType) {
        return hasRoot(dataType.getLogicalType(), LogicalTypeRoot.ROW)
                && dataType.getConversionClass().equals(Row.class)
                && ((RowType) dataType.getLogicalType())
                        .getFields().stream().noneMatch(f -> f.getDescription().isPresent());
    }

    private static TypeInformation<?> convertToRowTypeInfo(FieldsDataType fieldsDataType) {
        final RowType rowType = (RowType) fieldsDataType.getLogicalType();

        final String[] fieldNames =
                rowType.getFields().stream().map(RowType.RowField::getName).toArray(String[]::new);

        final TypeInformation<?>[] fieldTypes =
                fieldsDataType.getChildren().stream()
                        .map(LegacyTypeInfoDataTypeConverter::toLegacyTypeInfo)
                        .toArray(TypeInformation[]::new);

        return Types.ROW_NAMED(fieldNames, fieldTypes);
    }

    private static DataType convertToArrayType(
            Class<?> arrayClass, TypeInformation<?> elementTypeInfo) {
        return DataTypes.ARRAY(toDataType(elementTypeInfo)).bridgedTo(arrayClass);
    }

    private static boolean canConvertToObjectArrayTypeInfo(DataType dataType) {
        return hasRoot(dataType.getLogicalType(), LogicalTypeRoot.ARRAY)
                && dataType.getConversionClass().isArray();
    }

    private static TypeInformation<?> convertToObjectArrayTypeInfo(
            CollectionDataType collectionDataType) {
        // Types.OBJECT_ARRAY would return a basic type info for strings
        return ObjectArrayTypeInfo.getInfoFor(
                toLegacyTypeInfo(collectionDataType.getElementDataType()));
    }

    private static DataType convertToMultisetType(TypeInformation elementTypeInfo) {
        return DataTypes.MULTISET(toDataType(elementTypeInfo)).bridgedTo(Map.class);
    }

    private static boolean canConvertToMultisetTypeInfo(DataType dataType) {
        return hasRoot(dataType.getLogicalType(), LogicalTypeRoot.MULTISET)
                && dataType.getConversionClass() == Map.class;
    }

    private static TypeInformation<?> convertToMultisetTypeInfo(
            CollectionDataType collectionDataType) {
        return new MultisetTypeInfo<>(toLegacyTypeInfo(collectionDataType.getElementDataType()));
    }

    private static DataType convertToMapType(MapTypeInfo typeInfo) {
        return DataTypes.MAP(
                        toDataType(typeInfo.getKeyTypeInfo()),
                        toDataType(typeInfo.getValueTypeInfo()))
                .bridgedTo(Map.class);
    }

    private static boolean canConvertToMapTypeInfo(DataType dataType) {
        return hasRoot(dataType.getLogicalType(), LogicalTypeRoot.MAP)
                && dataType.getConversionClass() == Map.class;
    }

    private static TypeInformation<?> convertToMapTypeInfo(KeyValueDataType dataType) {
        return Types.MAP(
                toLegacyTypeInfo(dataType.getKeyDataType()),
                toLegacyTypeInfo(dataType.getValueDataType()));
    }

    private static boolean canConvertToLegacyTypeInfo(DataType dataType) {
        return dataType.getLogicalType() instanceof LegacyTypeInformationType;
    }

    private static TypeInformation<?> convertToLegacyTypeInfo(DataType dataType) {
        return ((LegacyTypeInformationType) dataType.getLogicalType()).getTypeInformation();
    }

    private static boolean canConvertToRawTypeInfo(DataType dataType) {
        return dataType.getLogicalType() instanceof TypeInformationRawType
                && dataType.getConversionClass()
                        .equals(
                                ((TypeInformationRawType) dataType.getLogicalType())
                                        .getTypeInformation()
                                        .getTypeClass());
    }

    private static TypeInformation<?> convertToRawTypeInfo(DataType dataType) {
        return ((TypeInformationRawType) dataType.getLogicalType()).getTypeInformation();
    }

    /**
     * Temporary solution to enable tests with type information and internal data structures until
     * we drop all legacy types.
     */
    private static boolean isRowData(TypeInformation<?> typeInfo) {
        if (!(typeInfo instanceof DataTypeQueryable)) {
            return false;
        }
        final DataType dataType = ((DataTypeQueryable) typeInfo).getDataType();
        return dataType.getConversionClass() == RowData.class;
    }

    private LegacyTypeInfoDataTypeConverter() {
        // no instantiation
    }
}
