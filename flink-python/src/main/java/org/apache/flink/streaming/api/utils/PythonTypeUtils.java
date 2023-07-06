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

package org.apache.flink.streaming.api.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.LocalTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BigIntSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.CharSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.apache.flink.api.common.typeutils.base.InstantSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.api.common.typeutils.base.LocalDateSerializer;
import org.apache.flink.api.common.typeutils.base.LocalDateTimeSerializer;
import org.apache.flink.api.common.typeutils.base.LocalTimeSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.array.BooleanPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.CharPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.DoublePrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.FloatPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.IntPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.LongPrimitiveArraySerializer;
import org.apache.flink.api.common.typeutils.base.array.ShortPrimitiveArraySerializer;
import org.apache.flink.api.java.io.CollectionInputFormat;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.core.memory.ByteArrayInputStreamWithPos;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.typeinfo.python.PickledByteArrayTypeInfo;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.serializers.python.BigDecSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.DateSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.StringSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.TimeSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.TimestampSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.table.typeutils.TimeIntervalTypeInfo;
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Array;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/** A util class for converting the given TypeInformation to other objects. */
@Internal
public class PythonTypeUtils {

    /** Get coder proto according to the given type information. */
    public static class TypeInfoToProtoConverter {

        private static final Set<FlinkFnApi.TypeInfo.TypeName> basicTypeNames =
                Sets.newHashSet(
                        FlinkFnApi.TypeInfo.TypeName.BOOLEAN,
                        FlinkFnApi.TypeInfo.TypeName.BYTE,
                        FlinkFnApi.TypeInfo.TypeName.STRING,
                        FlinkFnApi.TypeInfo.TypeName.SHORT,
                        FlinkFnApi.TypeInfo.TypeName.INT,
                        FlinkFnApi.TypeInfo.TypeName.LONG,
                        FlinkFnApi.TypeInfo.TypeName.FLOAT,
                        FlinkFnApi.TypeInfo.TypeName.DOUBLE,
                        FlinkFnApi.TypeInfo.TypeName.CHAR,
                        FlinkFnApi.TypeInfo.TypeName.BIG_INT,
                        FlinkFnApi.TypeInfo.TypeName.BIG_DEC,
                        FlinkFnApi.TypeInfo.TypeName.INSTANT);

        private static final Set<FlinkFnApi.TypeInfo.TypeName> primitiveArrayElementTypeNames =
                Sets.newHashSet(
                        FlinkFnApi.TypeInfo.TypeName.BOOLEAN,
                        FlinkFnApi.TypeInfo.TypeName.BYTE,
                        FlinkFnApi.TypeInfo.TypeName.SHORT,
                        FlinkFnApi.TypeInfo.TypeName.INT,
                        FlinkFnApi.TypeInfo.TypeName.LONG,
                        FlinkFnApi.TypeInfo.TypeName.FLOAT,
                        FlinkFnApi.TypeInfo.TypeName.DOUBLE,
                        FlinkFnApi.TypeInfo.TypeName.CHAR);

        private static final Set<FlinkFnApi.TypeInfo.TypeName> basicArrayElementTypeNames =
                Sets.newHashSet(
                        FlinkFnApi.TypeInfo.TypeName.BOOLEAN,
                        FlinkFnApi.TypeInfo.TypeName.BYTE,
                        FlinkFnApi.TypeInfo.TypeName.SHORT,
                        FlinkFnApi.TypeInfo.TypeName.INT,
                        FlinkFnApi.TypeInfo.TypeName.LONG,
                        FlinkFnApi.TypeInfo.TypeName.FLOAT,
                        FlinkFnApi.TypeInfo.TypeName.DOUBLE,
                        FlinkFnApi.TypeInfo.TypeName.CHAR,
                        FlinkFnApi.TypeInfo.TypeName.STRING);

        private static final Set<FlinkFnApi.TypeInfo.TypeName> sqlTimeTypeNames =
                Sets.newHashSet(
                        FlinkFnApi.TypeInfo.TypeName.SQL_DATE,
                        FlinkFnApi.TypeInfo.TypeName.SQL_TIME,
                        FlinkFnApi.TypeInfo.TypeName.SQL_TIMESTAMP);

        /**
         * userClassLoader could be null if no external class loading is needed, e.g. load Avro
         * classes.
         */
        public static FlinkFnApi.TypeInfo toTypeInfoProto(
                TypeInformation<?> typeInformation, @Nullable ClassLoader userClassLoader) {

            if (typeInformation instanceof BasicTypeInfo) {
                return buildBasicTypeProto((BasicTypeInfo<?>) typeInformation);
            }

            if (typeInformation instanceof PrimitiveArrayTypeInfo) {
                return buildPrimitiveArrayTypeProto(
                        (PrimitiveArrayTypeInfo<?>) typeInformation, userClassLoader);
            }

            if (typeInformation instanceof ObjectArrayTypeInfo) {
                return buildObjectArrayTypeProto(
                        (ObjectArrayTypeInfo<?, ?>) typeInformation, userClassLoader);
            }

            if (typeInformation instanceof SqlTimeTypeInfo) {
                return buildSqlTimeTypeProto((SqlTimeTypeInfo<?>) typeInformation);
            }

            if (typeInformation instanceof LocalTimeTypeInfo) {
                return buildLocalTimeTypeProto((LocalTimeTypeInfo<?>) typeInformation);
            }

            if (typeInformation instanceof RowTypeInfo) {
                return buildRowTypeProto((RowTypeInfo) typeInformation, userClassLoader);
            }

            if (typeInformation instanceof PickledByteArrayTypeInfo) {
                return FlinkFnApi.TypeInfo.newBuilder()
                        .setTypeName(getTypeName(typeInformation))
                        .build();
            }

            if (typeInformation instanceof TupleTypeInfo) {
                return buildTupleTypeProto((TupleTypeInfo<?>) typeInformation, userClassLoader);
            }

            if (typeInformation instanceof BasicArrayTypeInfo) {
                return buildBasicArrayTypeProto(
                        (BasicArrayTypeInfo<?, ?>) typeInformation, userClassLoader);
            }

            if (typeInformation instanceof MapTypeInfo) {
                return buildMapTypeProto((MapTypeInfo<?, ?>) typeInformation, userClassLoader);
            }

            if (typeInformation instanceof ListTypeInfo) {
                return buildListTypeProto((ListTypeInfo<?>) typeInformation, userClassLoader);
            }

            if (typeInformation instanceof ExternalTypeInfo) {
                return toTypeInfoProto(
                        LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(
                                ((ExternalTypeInfo<?>) typeInformation).getDataType()),
                        userClassLoader);
            }

            if (typeInformation instanceof InternalTypeInfo) {
                return buildInternalTypeProto(
                        (InternalTypeInfo<?>) typeInformation, userClassLoader);
            }

            if (typeInformation
                    .getClass()
                    .getCanonicalName()
                    .equals("org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo")) {
                try {
                    Preconditions.checkNotNull(userClassLoader);
                    return buildAvroTypeProto(typeInformation, userClassLoader);
                } catch (Exception e) {
                    throw new RuntimeException(
                            "Error when building proto for GenericRecordAvroTypeInfo", e);
                }
            }

            throw new UnsupportedOperationException(
                    String.format(
                            "The type information: %s is not supported in PyFlink currently.",
                            typeInformation.toString()));
        }

        private static FlinkFnApi.TypeInfo buildBasicTypeProto(BasicTypeInfo<?> basicTypeInfo) {
            FlinkFnApi.TypeInfo.TypeName typeName = getTypeName(basicTypeInfo);
            if (!basicTypeNames.contains(typeName)) {
                throw new UnsupportedOperationException(
                        String.format(
                                "The BasicTypeInfo: %s is not supported in PyFlink currently.",
                                basicTypeInfo.toString()));
            }

            return FlinkFnApi.TypeInfo.newBuilder().setTypeName(typeName).build();
        }

        private static FlinkFnApi.TypeInfo buildSqlTimeTypeProto(
                SqlTimeTypeInfo<?> sqlTimeTypeInfo) {
            FlinkFnApi.TypeInfo.TypeName typeName = getTypeName(sqlTimeTypeInfo);
            if (!sqlTimeTypeNames.contains(typeName)) {
                throw new UnsupportedOperationException(
                        String.format(
                                "The SqlTimeTypeInfo: %s is not supported in PyFlink currently.",
                                sqlTimeTypeInfo.toString()));
            }

            return FlinkFnApi.TypeInfo.newBuilder().setTypeName(typeName).build();
        }

        private static FlinkFnApi.TypeInfo buildLocalTimeTypeProto(
                LocalTimeTypeInfo<?> localTimeTypeInfo) {
            FlinkFnApi.TypeInfo.TypeName typeName = getTypeName(localTimeTypeInfo);
            return FlinkFnApi.TypeInfo.newBuilder().setTypeName(typeName).build();
        }

        private static FlinkFnApi.TypeInfo buildPrimitiveArrayTypeProto(
                PrimitiveArrayTypeInfo<?> primitiveArrayTypeInfo, ClassLoader userClassLoader) {
            FlinkFnApi.TypeInfo elementFieldType =
                    toTypeInfoProto(primitiveArrayTypeInfo.getComponentType(), userClassLoader);
            if (!primitiveArrayElementTypeNames.contains(elementFieldType.getTypeName())) {
                throw new UnsupportedOperationException(
                        String.format(
                                "The element type of PrimitiveArrayTypeInfo: %s is not supported in PyFlink currently.",
                                primitiveArrayTypeInfo.toString()));
            }

            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(getTypeName(primitiveArrayTypeInfo))
                    .setCollectionElementType(elementFieldType)
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildObjectArrayTypeProto(
                ObjectArrayTypeInfo<?, ?> objectArrayTypeInfo, ClassLoader userClassLoader) {
            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(getTypeName(objectArrayTypeInfo))
                    .setCollectionElementType(
                            toTypeInfoProto(
                                    objectArrayTypeInfo.getComponentInfo(), userClassLoader))
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildBasicArrayTypeProto(
                BasicArrayTypeInfo<?, ?> basicArrayTypeInfo, ClassLoader userClassLoader) {
            FlinkFnApi.TypeInfo elementFieldType =
                    toTypeInfoProto(basicArrayTypeInfo.getComponentInfo(), userClassLoader);
            if (!basicArrayElementTypeNames.contains(elementFieldType.getTypeName())) {
                throw new UnsupportedOperationException(
                        String.format(
                                "The element type of BasicArrayTypeInfo: %s is not supported in PyFlink currently.",
                                basicArrayTypeInfo.toString()));
            }

            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(getTypeName(basicArrayTypeInfo))
                    .setCollectionElementType(elementFieldType)
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildRowTypeProto(
                RowTypeInfo rowTypeInfo, ClassLoader userClassLoader) {
            FlinkFnApi.TypeInfo.RowTypeInfo.Builder rowTypeInfoBuilder =
                    FlinkFnApi.TypeInfo.RowTypeInfo.newBuilder();

            int arity = rowTypeInfo.getArity();
            for (int index = 0; index < arity; index++) {
                rowTypeInfoBuilder.addFields(
                        FlinkFnApi.TypeInfo.RowTypeInfo.Field.newBuilder()
                                .setFieldName(rowTypeInfo.getFieldNames()[index])
                                .setFieldType(
                                        toTypeInfoProto(
                                                rowTypeInfo.getTypeAt(index), userClassLoader))
                                .build());
            }

            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(getTypeName(rowTypeInfo))
                    .setRowTypeInfo(rowTypeInfoBuilder.build())
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildTupleTypeProto(
                TupleTypeInfo<?> tupleTypeInfo, ClassLoader userClassLoader) {
            FlinkFnApi.TypeInfo.TupleTypeInfo.Builder tupleTypeInfoBuilder =
                    FlinkFnApi.TypeInfo.TupleTypeInfo.newBuilder();

            int arity = tupleTypeInfo.getArity();
            for (int index = 0; index < arity; index++) {
                tupleTypeInfoBuilder.addFieldTypes(
                        toTypeInfoProto(tupleTypeInfo.getTypeAt(index), userClassLoader));
            }

            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(getTypeName(tupleTypeInfo))
                    .setTupleTypeInfo(tupleTypeInfoBuilder.build())
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildMapTypeProto(
                MapTypeInfo<?, ?> mapTypeInfo, ClassLoader userClassLoader) {
            FlinkFnApi.TypeInfo.MapTypeInfo.Builder mapTypeInfoBuilder =
                    FlinkFnApi.TypeInfo.MapTypeInfo.newBuilder();

            mapTypeInfoBuilder
                    .setKeyType(toTypeInfoProto(mapTypeInfo.getKeyTypeInfo(), userClassLoader))
                    .setValueType(toTypeInfoProto(mapTypeInfo.getValueTypeInfo(), userClassLoader));

            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(getTypeName(mapTypeInfo))
                    .setMapTypeInfo(mapTypeInfoBuilder.build())
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildListTypeProto(
                ListTypeInfo<?> listTypeInfo, ClassLoader userClassLoader) {
            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(getTypeName(listTypeInfo))
                    .setCollectionElementType(
                            toTypeInfoProto(listTypeInfo.getElementTypeInfo(), userClassLoader))
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildAvroTypeProto(
                TypeInformation<?> avroTypeInfo, ClassLoader userClassLoader) throws Exception {
            Class<?> clazz =
                    Class.forName(
                            "org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo",
                            true,
                            userClassLoader);
            Field schemaField = clazz.getDeclaredField("schema");
            schemaField.setAccessible(true);
            String schema = schemaField.get(avroTypeInfo).toString();
            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(FlinkFnApi.TypeInfo.TypeName.AVRO)
                    .setAvroTypeInfo(
                            FlinkFnApi.TypeInfo.AvroTypeInfo.newBuilder().setSchema(schema).build())
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildTableBinaryTypeProto() {
            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(FlinkFnApi.TypeInfo.TypeName.PRIMITIVE_ARRAY)
                    .setCollectionElementType(
                            FlinkFnApi.TypeInfo.newBuilder()
                                    .setTypeName(FlinkFnApi.TypeInfo.TypeName.BYTE)
                                    .build())
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildTableArrayTypeProto(
                InternalTypeInfo<?> internalTypeInfo, ClassLoader userClassLoader) {
            ArrayType arrayType = (ArrayType) internalTypeInfo.toLogicalType();
            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(FlinkFnApi.TypeInfo.TypeName.OBJECT_ARRAY)
                    .setCollectionElementType(
                            toTypeInfoProto(
                                    InternalTypeInfo.of(arrayType.getElementType()),
                                    userClassLoader))
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildTableMapTypeProto(
                InternalTypeInfo<?> internalTypeInfo, ClassLoader userClassLoader) {
            MapType mapType = (MapType) internalTypeInfo.toLogicalType();
            FlinkFnApi.TypeInfo.MapTypeInfo.Builder mapTypeInfoBuilder =
                    FlinkFnApi.TypeInfo.MapTypeInfo.newBuilder();
            mapTypeInfoBuilder
                    .setKeyType(
                            toTypeInfoProto(
                                    InternalTypeInfo.of(mapType.getKeyType()), userClassLoader))
                    .setValueType(
                            toTypeInfoProto(
                                    InternalTypeInfo.of(mapType.getValueType()), userClassLoader));
            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(FlinkFnApi.TypeInfo.TypeName.MAP)
                    .setMapTypeInfo(mapTypeInfoBuilder.build())
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildTableRowTypeProto(
                InternalTypeInfo<?> internalTypeInfo, ClassLoader userClassLoader) {
            RowType rowType = internalTypeInfo.toRowType();
            FlinkFnApi.TypeInfo.RowTypeInfo.Builder rowTypeInfoBuilder =
                    FlinkFnApi.TypeInfo.RowTypeInfo.newBuilder();

            int arity = rowType.getFieldCount();
            for (int index = 0; index < arity; index++) {
                rowTypeInfoBuilder.addFields(
                        FlinkFnApi.TypeInfo.RowTypeInfo.Field.newBuilder()
                                .setFieldName(rowType.getFieldNames().get(index))
                                .setFieldType(
                                        toTypeInfoProto(
                                                InternalTypeInfo.of(
                                                        rowType.getFields().get(index).getType()),
                                                userClassLoader))
                                .build());
            }

            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(FlinkFnApi.TypeInfo.TypeName.ROW)
                    .setRowTypeInfo(rowTypeInfoBuilder.build())
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildInternalTypeProto(
                InternalTypeInfo<?> internalTypeInfo, ClassLoader userClassLoader)
                throws UnsupportedOperationException {
            FlinkFnApi.TypeInfo.TypeName typeName;
            switch (internalTypeInfo.toLogicalType().getTypeRoot()) {
                case CHAR:
                case VARCHAR:
                    typeName = FlinkFnApi.TypeInfo.TypeName.STRING;
                    break;
                case BOOLEAN:
                    typeName = FlinkFnApi.TypeInfo.TypeName.BOOLEAN;
                    break;
                case BINARY:
                case VARBINARY:
                    return buildTableBinaryTypeProto();
                case DECIMAL:
                    typeName = FlinkFnApi.TypeInfo.TypeName.BIG_DEC;
                    break;
                case TINYINT:
                    typeName = FlinkFnApi.TypeInfo.TypeName.BYTE;
                    break;
                case SMALLINT:
                    typeName = FlinkFnApi.TypeInfo.TypeName.SHORT;
                    break;
                case INTEGER:
                case INTERVAL_YEAR_MONTH:
                    typeName = FlinkFnApi.TypeInfo.TypeName.INT;
                    break;
                case BIGINT:
                    typeName = FlinkFnApi.TypeInfo.TypeName.BIG_INT;
                    break;
                case INTERVAL_DAY_TIME:
                    typeName = FlinkFnApi.TypeInfo.TypeName.LONG;
                    break;
                case FLOAT:
                    typeName = FlinkFnApi.TypeInfo.TypeName.FLOAT;
                    break;
                case DOUBLE:
                    typeName = FlinkFnApi.TypeInfo.TypeName.DOUBLE;
                    break;
                case DATE:
                    typeName = FlinkFnApi.TypeInfo.TypeName.SQL_DATE;
                    break;
                case TIME_WITHOUT_TIME_ZONE:
                    typeName = FlinkFnApi.TypeInfo.TypeName.SQL_TIME;
                    break;
                case TIMESTAMP_WITHOUT_TIME_ZONE:
                    typeName = FlinkFnApi.TypeInfo.TypeName.SQL_TIMESTAMP;
                    break;
                case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                    typeName = FlinkFnApi.TypeInfo.TypeName.LOCAL_ZONED_TIMESTAMP;
                    break;
                case ARRAY:
                    return buildTableArrayTypeProto(internalTypeInfo, userClassLoader);
                case MAP:
                    return buildTableMapTypeProto(internalTypeInfo, userClassLoader);
                case ROW:
                    return buildTableRowTypeProto(internalTypeInfo, userClassLoader);
                default:
                    throw new UnsupportedOperationException(
                            String.format(
                                    "InternalTypeInfo %s is still not supported in PyFlink",
                                    internalTypeInfo));
            }
            return FlinkFnApi.TypeInfo.newBuilder().setTypeName(typeName).build();
        }

        private static FlinkFnApi.TypeInfo.TypeName getTypeName(TypeInformation<?> typeInfo) {
            if (typeInfo.equals(BasicTypeInfo.BOOLEAN_TYPE_INFO)) {
                return FlinkFnApi.TypeInfo.TypeName.BOOLEAN;
            }

            if (typeInfo.equals(BasicTypeInfo.BYTE_TYPE_INFO)) {
                return FlinkFnApi.TypeInfo.TypeName.BYTE;
            }

            if (typeInfo.equals(BasicTypeInfo.STRING_TYPE_INFO)) {
                return FlinkFnApi.TypeInfo.TypeName.STRING;
            }

            if (typeInfo.equals(BasicTypeInfo.SHORT_TYPE_INFO)) {
                return FlinkFnApi.TypeInfo.TypeName.SHORT;
            }

            if (typeInfo.equals(BasicTypeInfo.INT_TYPE_INFO)) {
                return FlinkFnApi.TypeInfo.TypeName.INT;
            }

            if (typeInfo.equals(BasicTypeInfo.LONG_TYPE_INFO)) {
                return FlinkFnApi.TypeInfo.TypeName.LONG;
            }

            if (typeInfo.equals(BasicTypeInfo.FLOAT_TYPE_INFO)) {
                return FlinkFnApi.TypeInfo.TypeName.FLOAT;
            }

            if (typeInfo.equals(BasicTypeInfo.DOUBLE_TYPE_INFO)) {
                return FlinkFnApi.TypeInfo.TypeName.DOUBLE;
            }

            if (typeInfo.equals(BasicTypeInfo.CHAR_TYPE_INFO)) {
                return FlinkFnApi.TypeInfo.TypeName.CHAR;
            }

            if (typeInfo.equals(BasicTypeInfo.BIG_INT_TYPE_INFO)) {
                return FlinkFnApi.TypeInfo.TypeName.BIG_INT;
            }

            if (typeInfo.equals(BasicTypeInfo.BIG_DEC_TYPE_INFO)) {
                return FlinkFnApi.TypeInfo.TypeName.BIG_DEC;
            }

            if (typeInfo.equals(BasicTypeInfo.INSTANT_TYPE_INFO)) {
                return FlinkFnApi.TypeInfo.TypeName.INSTANT;
            }

            if (typeInfo instanceof PrimitiveArrayTypeInfo) {
                return FlinkFnApi.TypeInfo.TypeName.PRIMITIVE_ARRAY;
            }

            if (typeInfo instanceof BasicArrayTypeInfo) {
                return FlinkFnApi.TypeInfo.TypeName.BASIC_ARRAY;
            }

            if (typeInfo instanceof ObjectArrayTypeInfo) {
                return FlinkFnApi.TypeInfo.TypeName.OBJECT_ARRAY;
            }

            if (typeInfo instanceof RowTypeInfo) {
                return FlinkFnApi.TypeInfo.TypeName.ROW;
            }

            if (typeInfo instanceof TupleTypeInfo) {
                return FlinkFnApi.TypeInfo.TypeName.TUPLE;
            }

            if (typeInfo instanceof MapTypeInfo) {
                return FlinkFnApi.TypeInfo.TypeName.MAP;
            }

            if (typeInfo instanceof ListTypeInfo) {
                return FlinkFnApi.TypeInfo.TypeName.LIST;
            }

            if (typeInfo instanceof PickledByteArrayTypeInfo) {
                return FlinkFnApi.TypeInfo.TypeName.PICKLED_BYTES;
            }

            if (typeInfo.equals(SqlTimeTypeInfo.DATE)) {
                return FlinkFnApi.TypeInfo.TypeName.SQL_DATE;
            }

            if (typeInfo.equals(SqlTimeTypeInfo.TIME)) {
                return FlinkFnApi.TypeInfo.TypeName.SQL_TIME;
            }

            if (typeInfo.equals(SqlTimeTypeInfo.TIMESTAMP)) {
                return FlinkFnApi.TypeInfo.TypeName.SQL_TIMESTAMP;
            }

            if (typeInfo.equals(LocalTimeTypeInfo.LOCAL_DATE)) {
                return FlinkFnApi.TypeInfo.TypeName.LOCAL_DATE;
            }

            if (typeInfo.equals(LocalTimeTypeInfo.LOCAL_TIME)) {
                return FlinkFnApi.TypeInfo.TypeName.LOCAL_TIME;
            }

            if (typeInfo.equals(LocalTimeTypeInfo.LOCAL_DATE_TIME)) {
                return FlinkFnApi.TypeInfo.TypeName.LOCAL_DATETIME;
            }

            throw new UnsupportedOperationException(
                    String.format(
                            "Type %s is still not supported in PyFlink.", typeInfo.toString()));
        }
    }

    /** Get serializers according to the given typeInformation. */
    public static class TypeInfoToSerializerConverter {
        private static final Map<Class, TypeSerializer> typeInfoToSerializerMap = new HashMap<>();

        static {
            typeInfoToSerializerMap.put(
                    BasicTypeInfo.BOOLEAN_TYPE_INFO.getTypeClass(), BooleanSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    BasicTypeInfo.INT_TYPE_INFO.getTypeClass(), IntSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    BasicTypeInfo.STRING_TYPE_INFO.getTypeClass(), StringSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    BasicTypeInfo.SHORT_TYPE_INFO.getTypeClass(), ShortSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    BasicTypeInfo.LONG_TYPE_INFO.getTypeClass(), LongSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    BasicTypeInfo.FLOAT_TYPE_INFO.getTypeClass(), FloatSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    BasicTypeInfo.DOUBLE_TYPE_INFO.getTypeClass(), DoubleSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    BasicTypeInfo.CHAR_TYPE_INFO.getTypeClass(), CharSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    BasicTypeInfo.BIG_INT_TYPE_INFO.getTypeClass(), BigIntSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    BasicTypeInfo.BIG_DEC_TYPE_INFO.getTypeClass(), BigDecSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    BasicTypeInfo.BYTE_TYPE_INFO.getTypeClass(), ByteSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    BasicTypeInfo.INSTANT_TYPE_INFO.getTypeClass(), InstantSerializer.INSTANCE);

            typeInfoToSerializerMap.put(
                    PrimitiveArrayTypeInfo.BOOLEAN_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
                    BooleanPrimitiveArraySerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
                    BytePrimitiveArraySerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    PrimitiveArrayTypeInfo.CHAR_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
                    CharPrimitiveArraySerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    PrimitiveArrayTypeInfo.DOUBLE_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
                    DoublePrimitiveArraySerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    PrimitiveArrayTypeInfo.FLOAT_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
                    FloatPrimitiveArraySerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    PrimitiveArrayTypeInfo.LONG_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
                    LongPrimitiveArraySerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    PrimitiveArrayTypeInfo.SHORT_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
                    ShortPrimitiveArraySerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    PrimitiveArrayTypeInfo.INT_PRIMITIVE_ARRAY_TYPE_INFO.getTypeClass(),
                    IntPrimitiveArraySerializer.INSTANCE);

            typeInfoToSerializerMap.put(
                    SqlTimeTypeInfo.DATE.getTypeClass(), DateSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    SqlTimeTypeInfo.TIME.getTypeClass(), TimeSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    SqlTimeTypeInfo.TIMESTAMP.getTypeClass(), new TimestampSerializer(3));

            typeInfoToSerializerMap.put(
                    LocalTimeTypeInfo.LOCAL_DATE.getTypeClass(), LocalDateSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    LocalTimeTypeInfo.LOCAL_TIME.getTypeClass(), LocalTimeSerializer.INSTANCE);
            typeInfoToSerializerMap.put(
                    LocalTimeTypeInfo.LOCAL_DATE_TIME.getTypeClass(),
                    LocalDateTimeSerializer.INSTANCE);
        }

        @SuppressWarnings("unchecked")
        public static <T> TypeSerializer<T> typeInfoSerializerConverter(
                TypeInformation<T> typeInformation) {
            TypeSerializer<T> typeSerializer =
                    typeInfoToSerializerMap.get(typeInformation.getTypeClass());
            if (typeSerializer != null) {
                return typeSerializer;
            } else {

                if (typeInformation instanceof PickledByteArrayTypeInfo) {
                    return (TypeSerializer<T>) BytePrimitiveArraySerializer.INSTANCE;
                }

                if (typeInformation instanceof RowTypeInfo) {
                    RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInformation;
                    TypeSerializer<?>[] fieldTypeSerializers =
                            Arrays.stream(rowTypeInfo.getFieldTypes())
                                    .map(f -> typeInfoSerializerConverter(f))
                                    .toArray(TypeSerializer[]::new);
                    return (TypeSerializer<T>) new RowSerializer(fieldTypeSerializers);
                }

                if (typeInformation instanceof TupleTypeInfo) {
                    TupleTypeInfo<?> tupleTypeInfo = (TupleTypeInfo<?>) typeInformation;
                    TypeInformation<?>[] typeInformations =
                            new TypeInformation[tupleTypeInfo.getArity()];
                    for (int idx = 0; idx < tupleTypeInfo.getArity(); idx++) {
                        typeInformations[idx] = tupleTypeInfo.getTypeAt(idx);
                    }

                    TypeSerializer<?>[] fieldTypeSerializers =
                            Arrays.stream(typeInformations)
                                    .map(TypeInfoToSerializerConverter::typeInfoSerializerConverter)
                                    .toArray(TypeSerializer[]::new);
                    return (TypeSerializer<T>)
                            new TupleSerializer<>(
                                    Tuple.getTupleClass(tupleTypeInfo.getArity()),
                                    fieldTypeSerializers);
                }

                if (typeInformation instanceof BasicArrayTypeInfo) {
                    BasicArrayTypeInfo<?, ?> basicArrayTypeInfo =
                            (BasicArrayTypeInfo<?, ?>) typeInformation;
                    return (TypeSerializer<T>)
                            new GenericArraySerializer(
                                    basicArrayTypeInfo.getComponentTypeClass(),
                                    typeInfoSerializerConverter(
                                            basicArrayTypeInfo.getComponentInfo()));
                }

                if (typeInformation instanceof ObjectArrayTypeInfo) {
                    ObjectArrayTypeInfo<?, ?> objectArrayTypeInfo =
                            (ObjectArrayTypeInfo<?, ?>) typeInformation;
                    return (TypeSerializer<T>)
                            new GenericArraySerializer(
                                    objectArrayTypeInfo.getComponentInfo().getTypeClass(),
                                    typeInfoSerializerConverter(
                                            objectArrayTypeInfo.getComponentInfo()));
                }

                if (typeInformation instanceof MapTypeInfo) {
                    return (TypeSerializer<T>)
                            new MapSerializer<>(
                                    typeInfoSerializerConverter(
                                            ((MapTypeInfo<?, ?>) typeInformation).getKeyTypeInfo()),
                                    typeInfoSerializerConverter(
                                            ((MapTypeInfo<?, ?>) typeInformation)
                                                    .getValueTypeInfo()));
                }

                if (typeInformation instanceof ListTypeInfo) {
                    return (TypeSerializer<T>)
                            new ListSerializer<>(
                                    typeInfoSerializerConverter(
                                            ((ListTypeInfo<?>) typeInformation)
                                                    .getElementTypeInfo()));
                }

                if (typeInformation instanceof ExternalTypeInfo) {
                    return (TypeSerializer<T>)
                            typeInfoSerializerConverter(
                                    LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(
                                            ((ExternalTypeInfo<?>) typeInformation).getDataType()));
                }

                if (typeInformation instanceof InternalTypeInfo) {
                    InternalTypeInfo<?> internalTypeInfo = (InternalTypeInfo<?>) typeInformation;
                    return org.apache.flink.table.runtime.typeutils.PythonTypeUtils
                            .toInternalSerializer(internalTypeInfo.toLogicalType());
                }

                if (typeInformation
                        .getClass()
                        .getCanonicalName()
                        .equals(
                                "org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo")) {
                    return typeInformation.createSerializer(new ExecutionConfig());
                }
            }

            throw new UnsupportedOperationException(
                    String.format(
                            "Could not find type serializer for current type [%s].",
                            typeInformation.toString()));
        }
    }

    /** Data Converter that converts the data to the format data which can be used in PemJa. */
    public abstract static class DataConverter<IN, OUT> implements Serializable {

        private static final long serialVersionUID = 1L;

        public abstract IN toInternal(OUT value);

        public abstract OUT toExternal(IN value);
    }

    /** Identity data converter. */
    public static final class IdentityDataConverter<T> extends DataConverter<T, T> {

        private static final long serialVersionUID = 1L;

        public static final IdentityDataConverter INSTANCE = new IdentityDataConverter<>();

        @Override
        public T toInternal(T value) {
            return value;
        }

        @Override
        public T toExternal(T value) {
            return value;
        }
    }

    /**
     * Python Long will be converted to Long in PemJa, so we need ByteDataConverter to convert Java
     * Long to internal Byte.
     */
    public static final class ByteDataConverter extends DataConverter<Byte, Long> {

        private static final long serialVersionUID = 1L;

        public static final ByteDataConverter INSTANCE = new ByteDataConverter();

        @Override
        public Byte toInternal(Long value) {
            if (value == null) {
                return null;
            }

            return value.byteValue();
        }

        @Override
        public Long toExternal(Byte value) {
            if (value == null) {
                return null;
            }

            return value.longValue();
        }
    }

    /**
     * Python Long will be converted to Long in PemJa, so we need ShortDataConverter to convert Java
     * Long to internal Short.
     */
    public static final class ShortDataConverter extends DataConverter<Short, Long> {

        private static final long serialVersionUID = 1L;

        public static final ShortDataConverter INSTANCE = new ShortDataConverter();

        @Override
        public Short toInternal(Long value) {
            if (value == null) {
                return null;
            }

            return value.shortValue();
        }

        @Override
        public Long toExternal(Short value) {
            if (value == null) {
                return null;
            }

            return value.longValue();
        }
    }

    /**
     * Python Long will be converted to Long in PemJa, so we need IntDataConverter to convert Java
     * Long to internal Integer.
     */
    public static final class IntDataConverter extends DataConverter<Integer, Long> {

        private static final long serialVersionUID = 1L;

        public static final IntDataConverter INSTANCE = new IntDataConverter();

        @Override
        public Integer toInternal(Long value) {
            if (value == null) {
                return null;
            }

            return value.intValue();
        }

        @Override
        public Long toExternal(Integer value) {
            if (value == null) {
                return null;
            }

            return value.longValue();
        }
    }

    /**
     * Python Float will be converted to Double in PemJa, so we need FloatDataConverter to convert
     * Java Double to internal Float.
     */
    public static final class FloatDataConverter extends DataConverter<Float, Double> {

        private static final long serialVersionUID = 1L;

        public static final FloatDataConverter INSTANCE = new FloatDataConverter();

        @Override
        public Float toInternal(Double value) {
            if (value == null) {
                return null;
            }

            return value.floatValue();
        }

        @Override
        public Double toExternal(Float value) {
            if (value == null) {
                return null;
            }

            return value.doubleValue();
        }
    }

    /**
     * Row Data will be converted to the Object Array [RowKind(as Long Object), Field Values(as
     * Object Array)].
     */
    public static final class RowDataConverter extends DataConverter<Row, Object[]> {

        private static final long serialVersionUID = 1L;

        private final DataConverter[] fieldDataConverters;
        private final Row reuseRow;
        private final Object[] reuseExternalRow;
        private final Object[] reuseExternalRowData;

        RowDataConverter(DataConverter[] dataConverters) {
            this.fieldDataConverters = dataConverters;
            this.reuseRow = new Row(fieldDataConverters.length);
            this.reuseExternalRowData = new Object[fieldDataConverters.length];
            this.reuseExternalRow = new Object[2];
            this.reuseExternalRow[1] = reuseExternalRowData;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Row toInternal(Object[] value) {
            if (value == null) {
                return null;
            }

            RowKind rowKind = RowKind.fromByteValue(((Long) value[0]).byteValue());
            reuseRow.setKind(rowKind);
            Object[] fieldValues = (Object[]) value[1];
            for (int i = 0; i < fieldValues.length; i++) {
                reuseRow.setField(i, fieldDataConverters[i].toInternal(fieldValues[i]));
            }
            return reuseRow;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object[] toExternal(Row value) {
            if (value == null) {
                return null;
            }

            reuseExternalRow[0] = (long) value.getKind().toByteValue();
            for (int i = 0; i < value.getArity(); i++) {
                reuseExternalRowData[i] = fieldDataConverters[i].toExternal(value.getField(i));
            }
            return reuseExternalRow;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final RowDataConverter other = (RowDataConverter) o;
            if (other.fieldDataConverters.length != this.fieldDataConverters.length) {
                return false;
            }

            for (int i = 0; i < other.fieldDataConverters.length; i++) {
                if (!other.fieldDataConverters[i].equals(this.fieldDataConverters[i])) {
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * RowData Data will be converted to the Object Array [RowKind(as Long Object), Field Values(as
     * Object Array)].
     */
    public static final class RowDataDataConverter extends DataConverter<RowData, Object[]> {

        private final RowDataConverter dataConverter;

        private final DataFormatConverters.DataFormatConverter<RowData, Row> dataFormatConverter;

        RowDataDataConverter(
                RowDataConverter dataConverter,
                DataFormatConverters.DataFormatConverter<RowData, Row> dataFormatConverter) {
            this.dataConverter = dataConverter;
            this.dataFormatConverter = dataFormatConverter;
        }

        @Override
        public RowData toInternal(Object[] value) {
            return dataFormatConverter.toInternal(dataConverter.toInternal(value));
        }

        @Override
        public Object[] toExternal(RowData value) {
            return dataConverter.toExternal(dataFormatConverter.toExternal(value));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final RowDataDataConverter other = (RowDataDataConverter) o;
            return this.dataConverter.equals(other.dataConverter);
        }
    }

    /** Tuple Data will be converted to the Object Array. */
    public static final class TupleDataConverter extends DataConverter<Tuple, Object[]> {

        private final DataConverter[] fieldDataConverters;
        private final Tuple reuseTuple;
        private final Object[] reuseExternalTuple;

        TupleDataConverter(DataConverter[] dataConverters) {
            this.fieldDataConverters = dataConverters;
            this.reuseTuple = Tuple.newInstance(dataConverters.length);
            this.reuseExternalTuple = new Object[dataConverters.length];
        }

        @SuppressWarnings("unchecked")
        @Override
        public Tuple toInternal(Object[] value) {
            if (value == null) {
                return null;
            }

            for (int i = 0; i < value.length; i++) {
                reuseTuple.setField(fieldDataConverters[i].toInternal(value[i]), i);
            }
            return reuseTuple;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object[] toExternal(Tuple value) {
            if (value == null) {
                return null;
            }

            for (int i = 0; i < value.getArity(); i++) {
                reuseExternalTuple[i] = fieldDataConverters[i].toExternal(value.getField(i));
            }
            return reuseExternalTuple;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final TupleDataConverter other = (TupleDataConverter) o;
            if (other.fieldDataConverters.length != this.fieldDataConverters.length) {
                return false;
            }

            for (int i = 0; i < other.fieldDataConverters.length; i++) {
                if (!other.fieldDataConverters[i].equals(this.fieldDataConverters[i])) {
                    return false;
                }
            }
            return true;
        }
    }

    /**
     * The element in the Object Array will be converted to the corresponding Data through element
     * DataConverter.
     */
    public static final class ArrayDataConverter<T> extends DataConverter<T[], Object[]> {

        private final DataConverter elementConverter;
        private final Class<T> componentClass;

        ArrayDataConverter(Class<T> componentClass, DataConverter elementConverter) {
            this.componentClass = componentClass;
            this.elementConverter = elementConverter;
        }

        @SuppressWarnings("unchecked")
        @Override
        public T[] toInternal(Object[] value) {
            if (value == null) {
                return null;
            }

            T[] array = (T[]) Array.newInstance(componentClass, value.length);

            for (int i = 0; i < value.length; i++) {
                array[i] = (T) elementConverter.toInternal(value[i]);
            }

            return array;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Object[] toExternal(T[] value) {
            if (value == null) {
                return null;
            }

            Object[] array = new Object[value.length];

            for (int i = 0; i < value.length; i++) {
                array[i] = elementConverter.toExternal(value[i]);
            }

            return array;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final ArrayDataConverter other = (ArrayDataConverter) o;
            return this.elementConverter.equals(other.elementConverter);
        }
    }

    /**
     * The element in the List will be converted to the corresponding Data through element
     * DataConverter.
     */
    public static final class ListDataConverter extends DataConverter<List, List> {

        private final DataConverter elementConverter;

        ListDataConverter(DataConverter elementConverter) {
            this.elementConverter = elementConverter;
        }

        @SuppressWarnings("unchecked")
        @Override
        public List toInternal(List value) {
            if (value == null) {
                return null;
            }

            List<Object> list = new LinkedList<>();

            for (Object item : value) {
                list.add(elementConverter.toInternal(item));
            }
            return list;
        }

        @SuppressWarnings("unchecked")
        @Override
        public List toExternal(List value) {
            if (value == null) {
                return null;
            }

            List<Object> list = new LinkedList<>();

            for (Object item : value) {
                list.add(elementConverter.toExternal(item));
            }
            return list;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final ListDataConverter other = (ListDataConverter) o;
            return this.elementConverter.equals(other.elementConverter);
        }
    }

    /**
     * The key/value in the Map will be converted to the corresponding Data through key/value
     * DataConverter.
     */
    public static final class MapDataConverter extends DataConverter<Map<?, ?>, Map<?, ?>> {

        private final DataConverter keyConverter;
        private final DataConverter valueConverter;

        MapDataConverter(DataConverter keyConverter, DataConverter valueConverter) {
            this.keyConverter = keyConverter;
            this.valueConverter = valueConverter;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<?, ?> toInternal(Map<?, ?> value) {
            if (value == null) {
                return null;
            }

            Map<Object, Object> map = new HashMap<>();

            for (Map.Entry<?, ?> entry : value.entrySet()) {
                map.put(
                        keyConverter.toInternal(entry.getKey()),
                        valueConverter.toInternal(entry.getValue()));
            }

            return map;
        }

        @SuppressWarnings("unchecked")
        @Override
        public Map<?, ?> toExternal(Map<?, ?> value) {
            if (value == null) {
                return null;
            }

            Map<Object, Object> map = new HashMap<>();

            for (Map.Entry<?, ?> entry : value.entrySet()) {
                map.put(
                        keyConverter.toExternal(entry.getKey()),
                        valueConverter.toExternal(entry.getValue()));
            }
            return map;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final MapDataConverter other = (MapDataConverter) o;
            return this.keyConverter.equals(other.keyConverter)
                    && this.valueConverter.equals(other.valueConverter);
        }
    }

    /** Get DataConverter according to the given typeInformation. */
    public static class TypeInfoToDataConverter {
        private static final Map<Class, DataConverter> typeInfoToDataConverterMap = new HashMap<>();

        static {
            typeInfoToDataConverterMap.put(
                    BasicTypeInfo.INT_TYPE_INFO.getTypeClass(), IntDataConverter.INSTANCE);
            typeInfoToDataConverterMap.put(
                    BasicTypeInfo.SHORT_TYPE_INFO.getTypeClass(), ShortDataConverter.INSTANCE);
            typeInfoToDataConverterMap.put(
                    BasicTypeInfo.FLOAT_TYPE_INFO.getTypeClass(), FloatDataConverter.INSTANCE);
            typeInfoToDataConverterMap.put(
                    BasicTypeInfo.BYTE_TYPE_INFO.getTypeClass(), ByteDataConverter.INSTANCE);
        }

        @SuppressWarnings("unchecked")
        public static <IN, OUT> DataConverter<IN, OUT> typeInfoDataConverter(
                TypeInformation<IN> typeInformation) {
            DataConverter<IN, OUT> dataConverter =
                    typeInfoToDataConverterMap.get(typeInformation.getTypeClass());

            if (dataConverter != null) {
                return dataConverter;
            } else {
                if (typeInformation instanceof PickledByteArrayTypeInfo) {
                    return IdentityDataConverter.INSTANCE;
                }

                if (typeInformation instanceof RowTypeInfo) {
                    DataConverter[] fieldDataConverters =
                            Arrays.stream(((RowTypeInfo) typeInformation).getFieldTypes())
                                    .map(TypeInfoToDataConverter::typeInfoDataConverter)
                                    .toArray(DataConverter[]::new);
                    return (DataConverter<IN, OUT>) new RowDataConverter(fieldDataConverters);
                }

                if (typeInformation instanceof TupleTypeInfo) {
                    Object[] fieldDataConverters =
                            Arrays.stream(((TupleTypeInfo) typeInformation).getFieldTypes())
                                    .map(TypeInfoToDataConverter::typeInfoDataConverter)
                                    .toArray(DataConverter[]::new);
                    return (DataConverter<IN, OUT>)
                            new TupleDataConverter((DataConverter[]) fieldDataConverters);
                }

                if (typeInformation instanceof BasicArrayTypeInfo) {
                    return (DataConverter<IN, OUT>)
                            new ArrayDataConverter(
                                    ((BasicArrayTypeInfo) typeInformation).getComponentTypeClass(),
                                    TypeInfoToDataConverter.typeInfoDataConverter(
                                            ((BasicArrayTypeInfo) typeInformation)
                                                    .getComponentInfo()));
                }

                if (typeInformation instanceof ObjectArrayTypeInfo) {
                    return (DataConverter<IN, OUT>)
                            new ArrayDataConverter(
                                    ((ObjectArrayTypeInfo) typeInformation)
                                            .getComponentInfo()
                                            .getTypeClass(),
                                    TypeInfoToDataConverter.typeInfoDataConverter(
                                            ((ObjectArrayTypeInfo) typeInformation)
                                                    .getComponentInfo()));
                }

                if (typeInformation instanceof ListTypeInfo) {
                    return (DataConverter<IN, OUT>)
                            new ListDataConverter(
                                    TypeInfoToDataConverter.typeInfoDataConverter(
                                            ((ListTypeInfo) typeInformation).getElementTypeInfo()));
                }

                if (typeInformation instanceof MapTypeInfo) {
                    return (DataConverter<IN, OUT>)
                            new MapDataConverter(
                                    TypeInfoToDataConverter.typeInfoDataConverter(
                                            ((MapTypeInfo) typeInformation).getKeyTypeInfo()),
                                    TypeInfoToDataConverter.typeInfoDataConverter(
                                            ((MapTypeInfo) typeInformation).getValueTypeInfo()));
                }

                if (typeInformation instanceof InternalTypeInfo) {

                    return ((InternalTypeInfo<IN>) typeInformation)
                            .toLogicalType()
                            .accept(LogicalTypeToDataConverter.INSTANCE);
                }

                if (typeInformation instanceof ExternalTypeInfo) {
                    return (DataConverter<IN, OUT>)
                            TypeInfoToDataConverter.typeInfoDataConverter(
                                    LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(
                                            ((ExternalTypeInfo<?>) typeInformation).getDataType()));
                }
            }

            return IdentityDataConverter.INSTANCE;
        }
    }

    private static class LogicalTypeToDataConverter
            extends LogicalTypeDefaultVisitor<DataConverter> {

        public static final LogicalTypeToDataConverter INSTANCE = new LogicalTypeToDataConverter();

        @Override
        public DataConverter visit(IntType intType) {
            return IntDataConverter.INSTANCE;
        }

        @Override
        public DataConverter visit(TinyIntType tinyIntType) {
            return ByteDataConverter.INSTANCE;
        }

        @Override
        public DataConverter visit(SmallIntType smallIntType) {
            return ShortDataConverter.INSTANCE;
        }

        @Override
        public DataConverter visit(FloatType floatType) {
            return FloatDataConverter.INSTANCE;
        }

        @Override
        public DataConverter visit(ArrayType arrayType) {
            LogicalType elementType = arrayType.getElementType();
            return new ArrayDataConverter<>(
                    elementType.getDefaultConversion(), elementType.accept(this));
        }

        @Override
        public DataConverter visit(MapType mapType) {
            return new MapDataConverter(
                    mapType.getKeyType().accept(this), mapType.getValueType().accept(this));
        }

        @SuppressWarnings("unchecked")
        @Override
        public DataConverter visit(RowType rowType) {
            final DataConverter[] fieldTypeDataConverters =
                    rowType.getFields().stream()
                            .map(f -> f.getType().accept(this))
                            .toArray(DataConverter[]::new);

            DataFormatConverters.DataFormatConverter dataFormatConverter =
                    DataFormatConverters.getConverterForDataType(
                            TypeConversions.fromLogicalToDataType(rowType));

            return new RowDataDataConverter(
                    new RowDataConverter(fieldTypeDataConverters), dataFormatConverter);
        }

        @Override
        protected DataConverter defaultMethod(LogicalType logicalType) {
            return IdentityDataConverter.INSTANCE;
        }
    }

    /**
     * Wrap the unpickled python data with an InputFormat. It will be passed to
     * StreamExecutionEnvironment.creatInput() to create an InputFormat later.
     *
     * @param data The unpickled python data.
     * @param dataType The python data type.
     * @param config The execution config used to create serializer.
     * @return An InputFormat containing the python data.
     */
    @SuppressWarnings("unchecked")
    public static <T> InputFormat<T, ?> getCollectionInputFormat(
            final List<T> data, final TypeInformation<T> dataType, final ExecutionConfig config) {
        Function<Object, Object> converter = converter(dataType, config);
        return new CollectionInputFormat<>(
                data.stream()
                        .map(objects -> (T) converter.apply(objects))
                        .collect(Collectors.toList()),
                dataType.createSerializer(config));
    }

    private static BiFunction<Integer, Function<Integer, Object>, Object> arrayConstructor(
            final TypeInformation<?> elementType, final boolean primitiveArray) {
        if (elementType.equals(BasicTypeInfo.BOOLEAN_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    boolean[] array = new boolean[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (boolean) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Boolean[] array = new Boolean[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Boolean) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.BYTE_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    byte[] array = new byte[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (byte) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Byte[] array = new Byte[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Byte) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.SHORT_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    short[] array = new short[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (short) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Short[] array = new Short[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Short) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.INT_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    int[] array = new int[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (int) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Integer[] array = new Integer[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Integer) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.LONG_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    long[] array = new long[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (long) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Long[] array = new Long[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Long) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.FLOAT_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    float[] array = new float[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (float) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Float[] array = new Float[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Float) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.DOUBLE_TYPE_INFO)) {
            if (primitiveArray) {
                return (length, elementGetter) -> {
                    double[] array = new double[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (double) elementGetter.apply(i);
                    }
                    return array;
                };
            } else {
                return (length, elementGetter) -> {
                    Double[] array = new Double[length];
                    for (int i = 0; i < length; i++) {
                        array[i] = (Double) elementGetter.apply(i);
                    }
                    return array;
                };
            }
        }
        if (elementType.equals(BasicTypeInfo.STRING_TYPE_INFO)) {
            return (length, elementGetter) -> {
                String[] array = new String[length];
                for (int i = 0; i < length; i++) {
                    array[i] = (String) elementGetter.apply(i);
                }
                return array;
            };
        }
        return (length, elementGetter) -> {
            Object[] array = new Object[length];
            for (int i = 0; i < length; i++) {
                array[i] = elementGetter.apply(i);
            }
            return array;
        };
    }

    private static Function<Object, Object> converter(
            final TypeInformation<?> dataType, final ExecutionConfig config) {
        if (dataType.equals(Types.BOOLEAN)) {
            return b -> b instanceof Boolean ? b : null;
        }
        if (dataType.equals(Types.BYTE)) {
            return c -> {
                if (c instanceof Byte) {
                    return c;
                }
                if (c instanceof Short) {
                    return ((Short) c).byteValue();
                }
                if (c instanceof Integer) {
                    return ((Integer) c).byteValue();
                }
                if (c instanceof Long) {
                    return ((Long) c).byteValue();
                }
                return null;
            };
        }
        if (dataType.equals(Types.SHORT)) {
            return c -> {
                if (c instanceof Byte) {
                    return ((Byte) c).shortValue();
                }
                if (c instanceof Short) {
                    return c;
                }
                if (c instanceof Integer) {
                    return ((Integer) c).shortValue();
                }
                if (c instanceof Long) {
                    return ((Long) c).shortValue();
                }
                return null;
            };
        }
        if (dataType.equals(Types.INT)) {
            return c -> {
                if (c instanceof Byte) {
                    return ((Byte) c).intValue();
                }
                if (c instanceof Short) {
                    return ((Short) c).intValue();
                }
                if (c instanceof Integer) {
                    return c;
                }
                if (c instanceof Long) {
                    return ((Long) c).intValue();
                }
                return null;
            };
        }
        if (dataType.equals(Types.LONG)) {
            return c -> {
                if (c instanceof Byte) {
                    return ((Byte) c).longValue();
                }
                if (c instanceof Short) {
                    return ((Short) c).longValue();
                }
                if (c instanceof Integer) {
                    return ((Integer) c).longValue();
                }
                if (c instanceof Long) {
                    return c;
                }
                return null;
            };
        }
        if (dataType.equals(Types.FLOAT)) {
            return c -> {
                if (c instanceof Float) {
                    return c;
                }
                if (c instanceof Double) {
                    return ((Double) c).floatValue();
                }
                return null;
            };
        }
        if (dataType.equals(Types.DOUBLE)) {
            return c -> {
                if (c instanceof Float) {
                    return ((Float) c).doubleValue();
                }
                if (c instanceof Double) {
                    return c;
                }
                return null;
            };
        }
        if (dataType.equals(Types.BIG_DEC)) {
            return c -> c instanceof BigDecimal ? c : null;
        }
        if (dataType.equals(Types.SQL_DATE)) {
            return c -> {
                if (c instanceof Integer) {
                    long millisLocal = ((Integer) c).longValue() * 86400000;
                    long millisUtc = millisLocal - getOffsetFromLocalMillis(millisLocal);
                    return new Date(millisUtc);
                }
                return null;
            };
        }

        if (dataType.equals(Types.SQL_TIME)) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? new Time(((Number) c).longValue() / 1000)
                            : null;
        }

        if (dataType.equals(Types.SQL_TIMESTAMP)) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? new Timestamp(((Number) c).longValue() / 1000)
                            : null;
        }

        if (dataType.equals(Types.LOCAL_DATE)) {
            return c -> {
                if (c instanceof Integer) {
                    long millisLocal = ((Integer) c).longValue() * 86400000;
                    long millisUtc = millisLocal - getOffsetFromLocalMillis(millisLocal);
                    return Instant.ofEpochMilli(millisUtc)
                            .atZone(ZoneId.systemDefault())
                            .toLocalDate();
                }
                return null;
            };
        }

        if (dataType.equals(Types.LOCAL_TIME)) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? Instant.ofEpochMilli(((Number) c).longValue() / 1000)
                                    .atZone(ZoneId.systemDefault())
                                    .toLocalTime()
                            : null;
        }

        if (dataType.equals(Types.LOCAL_DATE_TIME)) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? Instant.ofEpochMilli(((Number) c).longValue() / 1000)
                                    .atZone(ZoneId.systemDefault())
                                    .toLocalDateTime()
                            : null;
        }

        if (dataType.equals(org.apache.flink.api.common.typeinfo.Types.INSTANT)) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? Instant.ofEpochMilli(((Number) c).longValue() / 1000)
                            : null;
        }
        if (dataType.equals(TimeIntervalTypeInfo.INTERVAL_MILLIS)) {
            return c ->
                    c instanceof Integer || c instanceof Long
                            ? ((Number) c).longValue() / 1000
                            : null;
        }
        if (dataType.equals(Types.STRING)) {
            return c -> c != null ? c.toString() : null;
        }
        if (dataType.equals(PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO)) {
            return c -> {
                if (c instanceof String) {
                    return ((String) c).getBytes(StandardCharsets.UTF_8);
                }
                if (c instanceof byte[]) {
                    return c;
                }
                return null;
            };
        }
        if (dataType instanceof PrimitiveArrayTypeInfo
                || dataType instanceof BasicArrayTypeInfo
                || dataType instanceof ObjectArrayTypeInfo) {
            TypeInformation<?> elementType =
                    dataType instanceof PrimitiveArrayTypeInfo
                            ? ((PrimitiveArrayTypeInfo<?>) dataType).getComponentType()
                            : dataType instanceof BasicArrayTypeInfo
                                    ? ((BasicArrayTypeInfo<?, ?>) dataType).getComponentInfo()
                                    : ((ObjectArrayTypeInfo<?, ?>) dataType).getComponentInfo();
            boolean primitive = dataType instanceof PrimitiveArrayTypeInfo;
            Function<Object, Object> elementConverter = converter(elementType, config);
            BiFunction<Integer, Function<Integer, Object>, Object> arrayConstructor =
                    arrayConstructor(elementType, primitive);
            return c -> {
                int length = -1;
                Function<Integer, Object> elementGetter = null;
                if (c instanceof List) {
                    length = ((List<?>) c).size();
                    elementGetter = i -> elementConverter.apply(((List<?>) c).get(i));
                }
                if (c != null && c.getClass().isArray()) {
                    length = Array.getLength(c);
                    elementGetter = i -> elementConverter.apply(Array.get(c, i));
                }
                if (elementGetter != null) {
                    return arrayConstructor.apply(length, elementGetter);
                }
                return null;
            };
        }
        if (dataType instanceof MapTypeInfo) {
            Function<Object, Object> keyConverter =
                    converter(((MapTypeInfo<?, ?>) dataType).getKeyTypeInfo(), config);
            Function<Object, Object> valueConverter =
                    converter(((MapTypeInfo<?, ?>) dataType).getValueTypeInfo(), config);
            return c ->
                    c instanceof Map
                            ? ((Map<?, ?>) c)
                                    .entrySet().stream()
                                            .collect(
                                                    Collectors.toMap(
                                                            e -> keyConverter.apply(e.getKey()),
                                                            e ->
                                                                    valueConverter.apply(
                                                                            e.getValue())))
                            : null;
        }
        if (dataType instanceof RowTypeInfo) {
            TypeInformation<?>[] fieldTypes = ((RowTypeInfo) dataType).getFieldTypes();
            List<Function<Object, Object>> fieldConverters =
                    Arrays.stream(fieldTypes)
                            .map(x -> converter(x, config))
                            .collect(Collectors.toList());
            return c -> {
                if (c != null && c.getClass().isArray()) {
                    int length = Array.getLength(c);
                    if (length - 1 != fieldTypes.length) {
                        throw new IllegalStateException(
                                "Input row doesn't have expected number of values required by the schema. "
                                        + fieldTypes.length
                                        + " fields are required while "
                                        + (length - 1)
                                        + " values are provided.");
                    }

                    Row row = new Row(length - 1);
                    row.setKind(RowKind.fromByteValue(((Number) Array.get(c, 0)).byteValue()));

                    for (int i = 0; i < row.getArity(); i++) {
                        row.setField(i, fieldConverters.get(i).apply(Array.get(c, i + 1)));
                    }

                    return row;
                }
                return null;
            };
        }
        if (dataType instanceof TupleTypeInfo) {
            TypeInformation<?>[] fieldTypes = ((TupleTypeInfo<?>) dataType).getFieldTypes();
            List<Function<Object, Object>> fieldConverters =
                    Arrays.stream(fieldTypes)
                            .map(x -> converter(x, config))
                            .collect(Collectors.toList());
            return c -> {
                if (c != null && c.getClass().isArray()) {
                    int length = Array.getLength(c);
                    if (length != fieldTypes.length) {
                        throw new IllegalStateException(
                                "Input tuple doesn't have expected number of values required by the schema. "
                                        + fieldTypes.length
                                        + " fields are required while "
                                        + length
                                        + " values are provided.");
                    }

                    Tuple tuple = Tuple.newInstance(length);
                    for (int i = 0; i < tuple.getArity(); i++) {
                        tuple.setField(fieldConverters.get(i).apply(Array.get(c, i)), i);
                    }

                    return tuple;
                }
                return null;
            };
        }

        return c -> {
            if (c == null
                    || c.getClass() != byte[].class
                    || dataType instanceof PickledByteArrayTypeInfo) {
                return c;
            }

            // other typeinfos will use the corresponding serializer to deserialize data.
            byte[] b = (byte[]) c;
            TypeSerializer<?> dataSerializer = dataType.createSerializer(config);
            ByteArrayInputStreamWithPos bais = new ByteArrayInputStreamWithPos();
            DataInputViewStreamWrapper baisWrapper = new DataInputViewStreamWrapper(bais);
            bais.setBuffer(b, 0, b.length);
            try {
                return dataSerializer.deserialize(baisWrapper);
            } catch (IOException e) {
                throw new IllegalStateException(
                        "Failed to deserialize the object with datatype " + dataType, e);
            }
        };
    }

    private static int getOffsetFromLocalMillis(final long millisLocal) {
        TimeZone localZone = TimeZone.getDefault();
        int result = localZone.getRawOffset();
        // the actual offset should be calculated based on milliseconds in UTC
        int offset = localZone.getOffset(millisLocal - (long) result);
        if (offset != result) {
            // DayLight Saving Time
            result = localZone.getOffset(millisLocal - (long) offset);
            if (result != offset) {
                // fallback to do the reverse lookup using java.time.LocalDateTime
                // this should only happen near the start or end of DST
                LocalDate localDate = LocalDate.ofEpochDay(millisLocal / 86400000L);
                LocalTime localTime =
                        LocalTime.ofNanoOfDay(
                                Math.floorMod(millisLocal, 86400000L) * 1000L * 1000L);
                LocalDateTime localDateTime = LocalDateTime.of(localDate, localTime);
                long millisEpoch =
                        localDateTime.atZone(localZone.toZoneId()).toInstant().toEpochMilli();
                result = (int) (millisLocal - millisEpoch);
            }
        }

        return result;
    }
}
