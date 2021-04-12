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

import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;

/** Schema converter converts Parquet schema to and from Flink internal types. */
public class ParquetSchemaConverter {
    private static final Logger LOGGER = LoggerFactory.getLogger(ParquetSchemaConverter.class);
    public static final String MAP_VALUE = "value";
    public static final String LIST_ARRAY_TYPE = "array";
    public static final String LIST_ELEMENT = "element";
    public static final String LIST_GROUP_NAME = "list";
    public static final String MESSAGE_ROOT = "root";

    /**
     * Converts Parquet schema to Flink Internal Type.
     *
     * @param type Parquet schema
     * @return Flink type information
     */
    public static TypeInformation<?> fromParquetType(MessageType type) {
        return convertFields(type.getFields());
    }

    /**
     * Converts Flink Internal Type to Parquet schema.
     *
     * @param typeInformation Flink type information
     * @param legacyMode is standard LIST and MAP schema or back-compatible schema
     * @return Parquet schema
     */
    public static MessageType toParquetType(
            TypeInformation<?> typeInformation, boolean legacyMode) {
        return (MessageType)
                convertField(null, typeInformation, Type.Repetition.OPTIONAL, legacyMode);
    }

    public static TypeInformation<?> convertFields(List<Type> parquetFields) {
        List<TypeInformation<?>> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        for (Type field : parquetFields) {
            TypeInformation<?> subType = convertParquetTypeToTypeInfo(field);
            if (subType != null) {
                types.add(subType);
                names.add(field.getName());
            } else {
                LOGGER.error(
                        "Parquet field {} in schema type {} can not be converted to Flink Internal Type",
                        field.getName(),
                        field.getOriginalType().name());
            }
        }

        return new RowTypeInfo(
                types.toArray(new TypeInformation<?>[0]), names.toArray(new String[0]));
    }

    public static TypeInformation<?> convertParquetTypeToTypeInfo(final Type fieldType) {
        TypeInformation<?> typeInfo;
        if (fieldType.isPrimitive()) {
            OriginalType originalType = fieldType.getOriginalType();
            PrimitiveType primitiveType = fieldType.asPrimitiveType();
            switch (primitiveType.getPrimitiveTypeName()) {
                case BINARY:
                    if (originalType != null) {
                        switch (originalType) {
                            case DECIMAL:
                                typeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO;
                                break;
                            case UTF8:
                            case ENUM:
                            case JSON:
                            case BSON:
                                typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported original type : "
                                                + originalType.name()
                                                + " for primitive type BINARY");
                        }
                    } else {
                        typeInfo = BasicTypeInfo.STRING_TYPE_INFO;
                    }
                    break;
                case BOOLEAN:
                    typeInfo = BasicTypeInfo.BOOLEAN_TYPE_INFO;
                    break;
                case INT32:
                    if (originalType != null) {
                        switch (originalType) {
                            case DECIMAL: // for 1 <= precision (number of digits before the decimal
                                // point) <= 9, the INT32 stores the unscaled value
                                typeInfo = BasicTypeInfo.INT_TYPE_INFO;
                                break;
                            case TIME_MICROS:
                            case TIME_MILLIS:
                                typeInfo = SqlTimeTypeInfo.TIME;
                                break;
                            case TIMESTAMP_MICROS:
                            case TIMESTAMP_MILLIS:
                                typeInfo = SqlTimeTypeInfo.TIMESTAMP;
                                break;
                            case DATE:
                                typeInfo = SqlTimeTypeInfo.DATE;
                                break;
                            case UINT_8:
                            case UINT_16:
                            case UINT_32:
                                typeInfo = BasicTypeInfo.INT_TYPE_INFO;
                                break;
                            case INT_8:
                                typeInfo = org.apache.flink.api.common.typeinfo.Types.BYTE;
                                break;
                            case INT_16:
                                typeInfo = org.apache.flink.api.common.typeinfo.Types.SHORT;
                                break;
                            case INT_32:
                                typeInfo = BasicTypeInfo.INT_TYPE_INFO;
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported original type : "
                                                + originalType.name()
                                                + " for primitive type INT32");
                        }
                    } else {
                        typeInfo = BasicTypeInfo.INT_TYPE_INFO;
                    }
                    break;
                case INT64:
                    if (originalType != null) {
                        switch (originalType) {
                            case TIME_MICROS:
                                typeInfo = SqlTimeTypeInfo.TIME;
                                break;
                            case TIMESTAMP_MICROS:
                            case TIMESTAMP_MILLIS:
                                typeInfo = SqlTimeTypeInfo.TIMESTAMP;
                                break;
                            case INT_64:
                            case DECIMAL: // for 1 <= precision (number of digits before the decimal
                                // point) <= 18, the INT64 stores the unscaled value
                                typeInfo = BasicTypeInfo.LONG_TYPE_INFO;
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported original type : "
                                                + originalType.name()
                                                + " for primitive type INT64");
                        }
                    } else {
                        typeInfo = BasicTypeInfo.LONG_TYPE_INFO;
                    }
                    break;
                case INT96:
                    // It stores a timestamp type data, we read it as millisecond
                    typeInfo = SqlTimeTypeInfo.TIMESTAMP;
                    break;
                case FLOAT:
                    typeInfo = BasicTypeInfo.FLOAT_TYPE_INFO;
                    break;
                case DOUBLE:
                    typeInfo = BasicTypeInfo.DOUBLE_TYPE_INFO;
                    break;
                case FIXED_LEN_BYTE_ARRAY:
                    if (originalType != null) {
                        switch (originalType) {
                            case DECIMAL:
                                typeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO;
                                break;
                            default:
                                throw new UnsupportedOperationException(
                                        "Unsupported original type : "
                                                + originalType.name()
                                                + " for primitive type FIXED_LEN_BYTE_ARRAY");
                        }
                    } else {
                        typeInfo = BasicTypeInfo.BIG_DEC_TYPE_INFO;
                    }
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported schema: " + fieldType);
            }
        } else {
            GroupType parquetGroupType = fieldType.asGroupType();
            OriginalType originalType = parquetGroupType.getOriginalType();
            if (originalType != null) {
                switch (originalType) {
                    case LIST:
                        if (parquetGroupType.getFieldCount() != 1) {
                            throw new UnsupportedOperationException(
                                    "Invalid list type " + parquetGroupType);
                        }
                        Type repeatedType = parquetGroupType.getType(0);
                        if (!repeatedType.isRepetition(Type.Repetition.REPEATED)) {
                            throw new UnsupportedOperationException(
                                    "Invalid list type " + parquetGroupType);
                        }

                        if (repeatedType.isPrimitive()) {
                            typeInfo = convertParquetPrimitiveListToFlinkArray(repeatedType);
                        } else {
                            // Backward-compatibility element group name can be any string
                            // (element/array/other)
                            GroupType elementType = repeatedType.asGroupType();
                            // If the repeated field is a group with multiple fields, then its type
                            // is the element
                            // type and elements are required.
                            if (elementType.getFieldCount() > 1) {
                                typeInfo =
                                        convertGroupElementToArrayTypeInfo(
                                                parquetGroupType, elementType);
                            } else {
                                Type internalType = elementType.getType(0);
                                if (internalType.isPrimitive()) {
                                    typeInfo =
                                            convertParquetPrimitiveListToFlinkArray(internalType);
                                } else {
                                    // No need to do special process for group named array and tuple
                                    GroupType tupleGroup = internalType.asGroupType();
                                    if (tupleGroup.getFieldCount() == 1
                                            && tupleGroup
                                                    .getFields()
                                                    .get(0)
                                                    .isRepetition(Type.Repetition.REQUIRED)) {
                                        typeInfo =
                                                ObjectArrayTypeInfo.getInfoFor(
                                                        convertParquetTypeToTypeInfo(internalType));
                                    } else {
                                        typeInfo =
                                                convertGroupElementToArrayTypeInfo(
                                                        parquetGroupType, tupleGroup);
                                    }
                                }
                            }
                        }
                        break;

                    case MAP_KEY_VALUE:
                    case MAP:
                        // The outer-most level must be a group annotated with MAP
                        // that contains a single field named key_value
                        if (parquetGroupType.getFieldCount() != 1
                                || parquetGroupType.getType(0).isPrimitive()) {
                            throw new UnsupportedOperationException(
                                    "Invalid map type " + parquetGroupType);
                        }

                        // The middle level  must be a repeated group with a key field for map keys
                        // and, optionally, a value field for map values. But we can't enforce two
                        // strict condition here
                        // the schema generated by Parquet lib doesn't contain LogicalType
                        // ! mapKeyValType.getOriginalType().equals(OriginalType.MAP_KEY_VALUE)
                        GroupType mapKeyValType = parquetGroupType.getType(0).asGroupType();
                        if (!mapKeyValType.isRepetition(Type.Repetition.REPEATED)
                                || mapKeyValType.getFieldCount() != 2) {
                            throw new UnsupportedOperationException(
                                    "The middle level of Map should be single field named key_value. Invalid map type "
                                            + parquetGroupType);
                        }

                        Type keyType = mapKeyValType.getType(0);

                        // The key field encodes the map's key type. This field must have repetition
                        // required and
                        // must always be present.
                        if (!keyType.isPrimitive()
                                || !keyType.isRepetition(Type.Repetition.REQUIRED)
                                || !keyType.asPrimitiveType()
                                        .getPrimitiveTypeName()
                                        .equals(PrimitiveType.PrimitiveTypeName.BINARY)
                                || !keyType.getOriginalType().equals(OriginalType.UTF8)) {
                            throw new IllegalArgumentException(
                                    "Map key type must be required binary (UTF8): " + keyType);
                        }

                        Type valueType = mapKeyValType.getType(1);
                        return new MapTypeInfo<>(
                                BasicTypeInfo.STRING_TYPE_INFO,
                                convertParquetTypeToTypeInfo(valueType));
                    default:
                        throw new UnsupportedOperationException("Unsupported schema: " + fieldType);
                }
            } else {
                // if no original type than it is a record
                return convertFields(parquetGroupType.getFields());
            }
        }

        return typeInfo;
    }

    private static ObjectArrayTypeInfo convertGroupElementToArrayTypeInfo(
            GroupType arrayFieldType, GroupType elementType) {
        for (Type type : elementType.getFields()) {
            if (!type.isRepetition(Type.Repetition.REQUIRED)) {
                throw new UnsupportedOperationException(
                        String.format(
                                "List field [%s] in List [%s] has to be required. ",
                                type.toString(), arrayFieldType.getName()));
            }
        }
        return ObjectArrayTypeInfo.getInfoFor(convertParquetTypeToTypeInfo(elementType));
    }

    private static TypeInformation<?> convertParquetPrimitiveListToFlinkArray(Type type) {
        // Backward-compatibility element group doesn't exist also allowed
        TypeInformation<?> flinkType = convertParquetTypeToTypeInfo(type);
        if (flinkType.isBasicType()) {
            return BasicArrayTypeInfo.getInfoFor(
                    Array.newInstance(flinkType.getTypeClass(), 0).getClass());
        } else {
            // flinkType here can be either SqlTimeTypeInfo or BasicTypeInfo.BIG_DEC_TYPE_INFO,
            // So it should be converted to ObjectArrayTypeInfo
            return ObjectArrayTypeInfo.getInfoFor(flinkType);
        }
    }

    private static Type convertField(
            String fieldName,
            TypeInformation<?> typeInfo,
            Type.Repetition inheritRepetition,
            boolean legacyMode) {
        Type fieldType = null;

        Type.Repetition repetition =
                inheritRepetition == null ? Type.Repetition.OPTIONAL : inheritRepetition;
        if (typeInfo instanceof BasicTypeInfo) {
            BasicTypeInfo basicTypeInfo = (BasicTypeInfo) typeInfo;
            if (basicTypeInfo.equals(BasicTypeInfo.BIG_DEC_TYPE_INFO)
                    || basicTypeInfo.equals(BasicTypeInfo.BIG_INT_TYPE_INFO)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                                .as(OriginalType.DECIMAL)
                                .named(fieldName);
            } else if (basicTypeInfo.equals(BasicTypeInfo.INT_TYPE_INFO)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                                .as(OriginalType.INT_32)
                                .named(fieldName);
            } else if (basicTypeInfo.equals(BasicTypeInfo.DOUBLE_TYPE_INFO)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, repetition)
                                .named(fieldName);
            } else if (basicTypeInfo.equals(BasicTypeInfo.FLOAT_TYPE_INFO)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, repetition)
                                .named(fieldName);
            } else if (basicTypeInfo.equals(BasicTypeInfo.LONG_TYPE_INFO)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                                .as(OriginalType.INT_64)
                                .named(fieldName);
            } else if (basicTypeInfo.equals(BasicTypeInfo.SHORT_TYPE_INFO)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                                .as(OriginalType.INT_16)
                                .named(fieldName);
            } else if (basicTypeInfo.equals(BasicTypeInfo.BYTE_TYPE_INFO)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                                .as(OriginalType.INT_8)
                                .named(fieldName);
            } else if (basicTypeInfo.equals(BasicTypeInfo.CHAR_TYPE_INFO)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                                .as(OriginalType.UTF8)
                                .named(fieldName);
            } else if (basicTypeInfo.equals(BasicTypeInfo.BOOLEAN_TYPE_INFO)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.BOOLEAN, repetition)
                                .named(fieldName);
            } else if (basicTypeInfo.equals(BasicTypeInfo.DATE_TYPE_INFO)
                    || basicTypeInfo.equals(BasicTypeInfo.STRING_TYPE_INFO)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, repetition)
                                .as(OriginalType.UTF8)
                                .named(fieldName);
            }
        } else if (typeInfo instanceof MapTypeInfo) {
            MapTypeInfo mapTypeInfo = (MapTypeInfo) typeInfo;

            if (mapTypeInfo.getKeyTypeInfo().equals(BasicTypeInfo.STRING_TYPE_INFO)) {
                fieldType =
                        Types.map(repetition)
                                .value(
                                        convertField(
                                                MAP_VALUE,
                                                mapTypeInfo.getValueTypeInfo(),
                                                Type.Repetition.OPTIONAL,
                                                legacyMode))
                                .named(fieldName);
            } else {
                throw new UnsupportedOperationException(
                        String.format(
                                "Can not convert Flink MapTypeInfo %s to Parquet"
                                        + " Map type as key has to be String",
                                typeInfo.toString()));
            }
        } else if (typeInfo instanceof ObjectArrayTypeInfo) {
            ObjectArrayTypeInfo objectArrayTypeInfo = (ObjectArrayTypeInfo) typeInfo;

            // Get all required sub fields
            GroupType componentGroup =
                    (GroupType)
                            convertField(
                                    LIST_ELEMENT,
                                    objectArrayTypeInfo.getComponentInfo(),
                                    Type.Repetition.REQUIRED,
                                    legacyMode);

            if (legacyMode) {
                // LegacyMode is 2 Level List schema
                fieldType =
                        Types.buildGroup(repetition)
                                .addField(componentGroup)
                                .as(OriginalType.LIST)
                                .named(fieldName);
            } else {
                // Add extra layer of Group according to Parquet's standard
                Type listGroup =
                        Types.repeatedGroup().addField(componentGroup).named(LIST_GROUP_NAME);
                fieldType =
                        Types.buildGroup(repetition)
                                .addField(listGroup)
                                .as(OriginalType.LIST)
                                .named(fieldName);
            }
        } else if (typeInfo instanceof BasicArrayTypeInfo) {
            BasicArrayTypeInfo basicArrayType = (BasicArrayTypeInfo) typeInfo;

            // LegacyMode is 2 Level List schema
            if (legacyMode) {
                PrimitiveType primitiveTyp =
                        convertField(
                                        fieldName,
                                        basicArrayType.getComponentInfo(),
                                        Type.Repetition.REQUIRED,
                                        legacyMode)
                                .asPrimitiveType();
                fieldType =
                        Types.buildGroup(repetition)
                                .addField(primitiveTyp)
                                .as(OriginalType.LIST)
                                .named(fieldName);
            } else {
                // Add extra layer of Group according to Parquet's standard
                Type listGroup =
                        Types.repeatedGroup()
                                .addField(
                                        convertField(
                                                LIST_ELEMENT,
                                                basicArrayType.getComponentInfo(),
                                                Type.Repetition.REQUIRED,
                                                legacyMode))
                                .named(LIST_GROUP_NAME);

                fieldType =
                        Types.buildGroup(repetition)
                                .addField(listGroup)
                                .as(OriginalType.LIST)
                                .named(fieldName);
            }
        } else if (typeInfo instanceof SqlTimeTypeInfo) {
            if (typeInfo.equals(SqlTimeTypeInfo.DATE)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                                .as(OriginalType.DATE)
                                .named(fieldName);
            } else if (typeInfo.equals(SqlTimeTypeInfo.TIME)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, repetition)
                                .as(OriginalType.TIME_MILLIS)
                                .named(fieldName);
            } else if (typeInfo.equals(SqlTimeTypeInfo.TIMESTAMP)) {
                fieldType =
                        Types.primitive(PrimitiveType.PrimitiveTypeName.INT64, repetition)
                                .as(OriginalType.TIMESTAMP_MILLIS)
                                .named(fieldName);
            } else {
                throw new UnsupportedOperationException(
                        "Unsupported SqlTimeTypeInfo " + typeInfo.toString());
            }

        } else {
            RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;
            List<Type> types = new ArrayList<>();
            String[] fieldNames = rowTypeInfo.getFieldNames();
            TypeInformation<?>[] fieldTypes = rowTypeInfo.getFieldTypes();
            for (int i = 0; i < rowTypeInfo.getArity(); i++) {
                types.add(convertField(fieldNames[i], fieldTypes[i], repetition, legacyMode));
            }

            if (fieldName == null) {
                fieldType = new MessageType(MESSAGE_ROOT, types);
            } else {
                fieldType = new GroupType(repetition, fieldName, types);
            }
        }

        return fieldType;
    }

    public static MessageType convertToParquetMessageType(String name, RowType rowType) {
        Type[] types = new Type[rowType.getFieldCount()];
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            types[i] = convertToParquetType(rowType.getFieldNames().get(i), rowType.getTypeAt(i));
        }
        return new MessageType(name, types);
    }

    private static Type convertToParquetType(String name, LogicalType type) {
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
            default:
                throw new UnsupportedOperationException("Unsupported type: " + type);
        }
    }

    public static int computeMinBytesForDecimalPrecision(int precision) {
        int numBytes = 1;
        while (Math.pow(2.0, 8 * numBytes - 1) < Math.pow(10.0, precision)) {
            numBytes += 1;
        }
        return numBytes;
    }
}
