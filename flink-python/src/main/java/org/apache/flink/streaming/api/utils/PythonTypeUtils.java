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
import org.apache.flink.api.common.typeinfo.BasicArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.PrimitiveArrayTypeInfo;
import org.apache.flink.api.common.typeinfo.SqlTimeTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.api.java.typeutils.ObjectArrayTypeInfo;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.api.java.typeutils.runtime.TupleSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.streaming.api.typeinfo.python.PickledByteArrayTypeInfo;
import org.apache.flink.table.runtime.typeutils.ExternalTypeInfo;
import org.apache.flink.table.runtime.typeutils.serializers.python.BigDecSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.DateSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.StringSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.TimeSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.TimestampSerializer;
import org.apache.flink.table.types.utils.LegacyTypeInfoDataTypeConverter;

import org.apache.flink.shaded.guava30.com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

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

        public static FlinkFnApi.TypeInfo toTypeInfoProto(TypeInformation<?> typeInformation) {

            if (typeInformation instanceof BasicTypeInfo) {
                return buildBasicTypeProto((BasicTypeInfo<?>) typeInformation);
            }

            if (typeInformation instanceof PrimitiveArrayTypeInfo) {
                return buildPrimitiveArrayTypeProto((PrimitiveArrayTypeInfo<?>) typeInformation);
            }

            if (typeInformation instanceof ObjectArrayTypeInfo) {
                return buildObjectArrayTypeProto((ObjectArrayTypeInfo<?, ?>) typeInformation);
            }

            if (typeInformation instanceof SqlTimeTypeInfo) {
                return buildSqlTimeTypeProto((SqlTimeTypeInfo<?>) typeInformation);
            }

            if (typeInformation instanceof RowTypeInfo) {
                return buildRowTypeProto((RowTypeInfo) typeInformation);
            }

            if (typeInformation instanceof PickledByteArrayTypeInfo) {
                return FlinkFnApi.TypeInfo.newBuilder()
                        .setTypeName(getTypeName(typeInformation))
                        .build();
            }

            if (typeInformation instanceof TupleTypeInfo) {
                return buildTupleTypeProto((TupleTypeInfo<?>) typeInformation);
            }

            if (typeInformation instanceof BasicArrayTypeInfo) {
                return buildBasicArrayTypeProto((BasicArrayTypeInfo<?, ?>) typeInformation);
            }

            if (typeInformation instanceof MapTypeInfo) {
                return buildMapTypeProto((MapTypeInfo<?, ?>) typeInformation);
            }

            if (typeInformation instanceof ListTypeInfo) {
                return buildListTypeProto((ListTypeInfo<?>) typeInformation);
            }

            if (typeInformation instanceof ExternalTypeInfo) {
                return toTypeInfoProto(
                        LegacyTypeInfoDataTypeConverter.toLegacyTypeInfo(
                                ((ExternalTypeInfo<?>) typeInformation).getDataType()));
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

        private static FlinkFnApi.TypeInfo buildPrimitiveArrayTypeProto(
                PrimitiveArrayTypeInfo<?> primitiveArrayTypeInfo) {
            FlinkFnApi.TypeInfo elementFieldType =
                    toTypeInfoProto(primitiveArrayTypeInfo.getComponentType());
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
                ObjectArrayTypeInfo<?, ?> objectArrayTypeInfo) {
            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(getTypeName(objectArrayTypeInfo))
                    .setCollectionElementType(
                            toTypeInfoProto(objectArrayTypeInfo.getComponentInfo()))
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildBasicArrayTypeProto(
                BasicArrayTypeInfo<?, ?> basicArrayTypeInfo) {
            FlinkFnApi.TypeInfo elementFieldType =
                    toTypeInfoProto(basicArrayTypeInfo.getComponentInfo());
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

        private static FlinkFnApi.TypeInfo buildRowTypeProto(RowTypeInfo rowTypeInfo) {
            FlinkFnApi.TypeInfo.RowTypeInfo.Builder rowTypeInfoBuilder =
                    FlinkFnApi.TypeInfo.RowTypeInfo.newBuilder();

            int arity = rowTypeInfo.getArity();
            for (int index = 0; index < arity; index++) {
                rowTypeInfoBuilder.addFields(
                        FlinkFnApi.TypeInfo.RowTypeInfo.Field.newBuilder()
                                .setFieldName(rowTypeInfo.getFieldNames()[index])
                                .setFieldType(toTypeInfoProto(rowTypeInfo.getTypeAt(index)))
                                .build());
            }

            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(getTypeName(rowTypeInfo))
                    .setRowTypeInfo(rowTypeInfoBuilder.build())
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildTupleTypeProto(TupleTypeInfo<?> tupleTypeInfo) {
            FlinkFnApi.TypeInfo.TupleTypeInfo.Builder tupleTypeInfoBuilder =
                    FlinkFnApi.TypeInfo.TupleTypeInfo.newBuilder();

            int arity = tupleTypeInfo.getArity();
            for (int index = 0; index < arity; index++) {
                tupleTypeInfoBuilder.addFieldTypes(toTypeInfoProto(tupleTypeInfo.getTypeAt(index)));
            }

            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(getTypeName(tupleTypeInfo))
                    .setTupleTypeInfo(tupleTypeInfoBuilder.build())
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildMapTypeProto(MapTypeInfo<?, ?> mapTypeInfo) {
            FlinkFnApi.TypeInfo.MapTypeInfo.Builder mapTypeInfoBuilder =
                    FlinkFnApi.TypeInfo.MapTypeInfo.newBuilder();

            mapTypeInfoBuilder
                    .setKeyType(toTypeInfoProto(mapTypeInfo.getKeyTypeInfo()))
                    .setValueType(toTypeInfoProto(mapTypeInfo.getValueTypeInfo()));

            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(getTypeName(mapTypeInfo))
                    .setMapTypeInfo(mapTypeInfoBuilder.build())
                    .build();
        }

        private static FlinkFnApi.TypeInfo buildListTypeProto(ListTypeInfo<?> listTypeInfo) {
            return FlinkFnApi.TypeInfo.newBuilder()
                    .setTypeName(getTypeName(listTypeInfo))
                    .setCollectionElementType(toTypeInfoProto(listTypeInfo.getElementTypeInfo()))
                    .build();
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
            }

            throw new UnsupportedOperationException(
                    String.format(
                            "Could not find type serializer for current type [%s].",
                            typeInformation.toString()));
        }
    }
}
