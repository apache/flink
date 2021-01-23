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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.BooleanSerializer;
import org.apache.flink.api.common.typeutils.base.ByteSerializer;
import org.apache.flink.api.common.typeutils.base.DoubleSerializer;
import org.apache.flink.api.common.typeutils.base.FloatSerializer;
import org.apache.flink.api.common.typeutils.base.GenericArraySerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.common.typeutils.base.MapSerializer;
import org.apache.flink.api.common.typeutils.base.ShortSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.java.typeutils.runtime.RowSerializer;
import org.apache.flink.fnexecution.v1.FlinkFnApi;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.runtime.typeutils.serializers.python.ArrayDataSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.BigDecSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.DateSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.DecimalDataSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.MapDataSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.StringSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.TimeSerializer;
import org.apache.flink.table.runtime.typeutils.serializers.python.TimestampSerializer;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LegacyTypeInformationType;
import org.apache.flink.table.types.logical.LocalZonedTimestampType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;

import org.apache.beam.model.pipeline.v1.RunnerApi;

import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.TimeZone;

/**
 * Utilities for converting Flink logical types, such as convert it to the related TypeSerializer or
 * ProtoType.
 */
@Internal
public final class PythonTypeUtils {

    private static final String EMPTY_STRING = "";

    /** The local time zone. */
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    /** The number of milliseconds in a day. */
    private static final long MILLIS_PER_DAY = 86400000L; // = 24 * 60 * 60 * 1000

    public static RunnerApi.Coder getRowCoderProto(RowType rowType, String coderUrn) {
        return RunnerApi.Coder.newBuilder()
                .setSpec(
                        RunnerApi.FunctionSpec.newBuilder()
                                .setUrn(coderUrn)
                                .setPayload(
                                        org.apache.beam.vendor.grpc.v1p26p0.com.google.protobuf
                                                .ByteString.copyFrom(
                                                toProtoType(rowType).getRowSchema().toByteArray()))
                                .build())
                .build();
    }

    public static FlinkFnApi.Schema.FieldType toProtoType(LogicalType logicalType) {
        return logicalType.accept(new PythonTypeUtils.LogicalTypeToProtoTypeConverter());
    }

    public static TypeSerializer toFlinkTypeSerializer(LogicalType logicalType) {
        return logicalType.accept(new LogicalTypeToTypeSerializerConverter());
    }

    public static TypeSerializer toBlinkTypeSerializer(LogicalType logicalType) {
        return logicalType.accept(new LogicalTypeToBlinkTypeSerializerConverter());
    }

    /**
     * Convert the specified bigDecimal according to the specified precision and scale. The
     * specified bigDecimal may be rounded to have the specified scale and then the specified
     * precision is checked. If precision overflow, it will return `null`.
     *
     * <p>Note: The implementation refers to {@link DecimalData#fromBigDecimal}.
     */
    public static BigDecimal fromBigDecimal(BigDecimal bigDecimal, int precision, int scale) {
        if (bigDecimal.scale() != scale || bigDecimal.precision() > precision) {
            // need adjust the precision and scale
            bigDecimal = bigDecimal.setScale(scale, RoundingMode.HALF_UP);
            if (bigDecimal.precision() > precision) {
                return null;
            }
        }
        return bigDecimal;
    }

    /**
     * Converts the internal representation of a SQL DATE (int) to the Java type used for UDF
     * parameters ({@link java.sql.Date}).
     *
     * <p>Note: The implementation refers to {@link SqlDateTimeUtils#internalToDate}.
     */
    public static java.sql.Date internalToDate(int v) {
        // note that, in this case, can't handle Daylight Saving Time
        final long t = v * MILLIS_PER_DAY;
        return new java.sql.Date(t - LOCAL_TZ.getOffset(t));
    }

    /**
     * Converts the Java type used for UDF parameters of SQL DATE type ({@link java.sql.Date}) to
     * internal representation (int).
     *
     * <p>Note: The implementation refers to {@link SqlDateTimeUtils#dateToInternal}.
     */
    public static int dateToInternal(java.sql.Date date) {
        long ts = date.getTime() + LOCAL_TZ.getOffset(date.getTime());
        return (int) (ts / MILLIS_PER_DAY);
    }

    /**
     * Converts the internal representation of a SQL TIMESTAMP (long) to the Java type used for UDF
     * parameters ({@link java.sql.Timestamp}).
     *
     * <p>Note: The implementation refers to {@link SqlDateTimeUtils#internalToTimestamp}.
     */
    public static java.sql.Timestamp internalToTimestamp(long v) {
        return new java.sql.Timestamp(v - LOCAL_TZ.getOffset(v));
    }

    /**
     * Converts the Java type used for UDF parameters of SQL TIMESTAMP type ({@link
     * java.sql.Timestamp}) to internal representation (long).
     *
     * <p>Note: The implementation refers to {@link SqlDateTimeUtils#timestampToInternal}.
     */
    public static long timestampToInternal(java.sql.Timestamp ts) {
        long time = ts.getTime();
        return time + LOCAL_TZ.getOffset(time);
    }

    /** Convert LogicalType to conversion class for flink planner. */
    public static class LogicalTypeToConversionClassConverter
            extends LogicalTypeDefaultVisitor<Class> {

        public static final LogicalTypeToConversionClassConverter INSTANCE =
                new LogicalTypeToConversionClassConverter();

        private LogicalTypeToConversionClassConverter() {}

        @Override
        public Class visit(DateType dateType) {
            return Date.class;
        }

        @Override
        public Class visit(TimeType timeType) {
            return Time.class;
        }

        @Override
        public Class visit(TimestampType timestampType) {
            return Timestamp.class;
        }

        @Override
        public Class visit(ArrayType arrayType) {
            Class elementClass = arrayType.getElementType().accept(this);
            return Array.newInstance(elementClass, 0).getClass();
        }

        @Override
        protected Class defaultMethod(LogicalType logicalType) {
            return logicalType.getDefaultConversion();
        }
    }

    private static class LogicalTypeToTypeSerializerConverter
            extends LogicalTypeDefaultVisitor<TypeSerializer> {
        @Override
        public TypeSerializer visit(BooleanType booleanType) {
            return BooleanSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(TinyIntType tinyIntType) {
            return ByteSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(SmallIntType smallIntType) {
            return ShortSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(IntType intType) {
            return IntSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(BigIntType bigIntType) {
            return LongSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(FloatType floatType) {
            return FloatSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(DoubleType doubleType) {
            return DoubleSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(BinaryType binaryType) {
            return BytePrimitiveArraySerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(VarBinaryType varBinaryType) {
            return BytePrimitiveArraySerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(VarCharType varCharType) {
            return StringSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(CharType charType) {
            return StringSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(DateType dateType) {
            return DateSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(TimeType timeType) {
            return TimeSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(TimestampType timestampType) {
            return new TimestampSerializer(timestampType.getPrecision());
        }

        @SuppressWarnings("unchecked")
        @Override
        public TypeSerializer visit(ArrayType arrayType) {
            LogicalType elementType = arrayType.getElementType();
            TypeSerializer<?> elementTypeSerializer = elementType.accept(this);
            Class<?> elementClass =
                    elementType.accept(LogicalTypeToConversionClassConverter.INSTANCE);
            return new GenericArraySerializer(elementClass, elementTypeSerializer);
        }

        @Override
        public TypeSerializer visit(MapType mapType) {
            TypeSerializer<?> keyTypeSerializer = mapType.getKeyType().accept(this);
            TypeSerializer<?> valueTypeSerializer = mapType.getValueType().accept(this);
            return new MapSerializer<>(keyTypeSerializer, valueTypeSerializer);
        }

        @Override
        public TypeSerializer visit(RowType rowType) {
            final TypeSerializer[] fieldTypeSerializers =
                    rowType.getFields().stream()
                            .map(f -> f.getType().accept(this))
                            .toArray(TypeSerializer[]::new);
            return new RowSerializer(fieldTypeSerializers);
        }

        @Override
        protected TypeSerializer defaultMethod(LogicalType logicalType) {
            if (logicalType instanceof LegacyTypeInformationType) {
                Class<?> typeClass =
                        ((LegacyTypeInformationType) logicalType)
                                .getTypeInformation()
                                .getTypeClass();
                if (typeClass == BigDecimal.class) {
                    return BigDecSerializer.INSTANCE;
                }
            }
            throw new UnsupportedOperationException(
                    String.format(
                            "Python UDF doesn't support logical type %s currently.",
                            logicalType.asSummaryString()));
        }
    }

    private static class LogicalTypeToBlinkTypeSerializerConverter
            extends LogicalTypeToTypeSerializerConverter {

        @Override
        public TypeSerializer visit(RowType rowType) {
            final TypeSerializer[] fieldTypeSerializers =
                    rowType.getFields().stream()
                            .map(f -> f.getType().accept(this))
                            .toArray(TypeSerializer[]::new);
            return new RowDataSerializer(
                    rowType.getChildren().toArray(new LogicalType[0]), fieldTypeSerializers);
        }

        @Override
        public TypeSerializer visit(VarCharType varCharType) {
            return StringDataSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(CharType charType) {
            return StringDataSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(DateType dateType) {
            return IntSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(TimeType timeType) {
            return IntSerializer.INSTANCE;
        }

        @Override
        public TypeSerializer visit(TimestampType timestampType) {
            return new TimestampDataSerializer(timestampType.getPrecision());
        }

        @Override
        public TypeSerializer visit(LocalZonedTimestampType localZonedTimestampType) {
            return new TimestampDataSerializer(localZonedTimestampType.getPrecision());
        }

        public TypeSerializer visit(ArrayType arrayType) {
            LogicalType elementType = arrayType.getElementType();
            TypeSerializer elementTypeSerializer = elementType.accept(this);
            return new ArrayDataSerializer(elementType, elementTypeSerializer);
        }

        @Override
        public TypeSerializer visit(MapType mapType) {
            LogicalType keyType = mapType.getKeyType();
            LogicalType valueType = mapType.getValueType();
            TypeSerializer<?> keyTypeSerializer = keyType.accept(this);
            TypeSerializer<?> valueTypeSerializer = valueType.accept(this);
            return new MapDataSerializer(
                    keyType, valueType, keyTypeSerializer, valueTypeSerializer);
        }

        @Override
        public TypeSerializer visit(DecimalType decimalType) {
            return new DecimalDataSerializer(decimalType.getPrecision(), decimalType.getScale());
        }
    }

    /** Converter That convert the logicalType to the related Prototype. */
    public static class LogicalTypeToProtoTypeConverter
            extends LogicalTypeDefaultVisitor<FlinkFnApi.Schema.FieldType> {
        @Override
        public FlinkFnApi.Schema.FieldType visit(BooleanType booleanType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.BOOLEAN)
                    .setNullable(booleanType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(TinyIntType tinyIntType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.TINYINT)
                    .setNullable(tinyIntType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(SmallIntType smallIntType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.SMALLINT)
                    .setNullable(smallIntType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(IntType intType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.INT)
                    .setNullable(intType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(BigIntType bigIntType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.BIGINT)
                    .setNullable(bigIntType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(FloatType floatType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.FLOAT)
                    .setNullable(floatType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(DoubleType doubleType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.DOUBLE)
                    .setNullable(doubleType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(BinaryType binaryType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.BINARY)
                    .setBinaryInfo(
                            FlinkFnApi.Schema.BinaryInfo.newBuilder()
                                    .setLength(binaryType.getLength()))
                    .setNullable(binaryType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(VarBinaryType varBinaryType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.VARBINARY)
                    .setVarBinaryInfo(
                            FlinkFnApi.Schema.VarBinaryInfo.newBuilder()
                                    .setLength(varBinaryType.getLength()))
                    .setNullable(varBinaryType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(CharType charType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.CHAR)
                    .setCharInfo(
                            FlinkFnApi.Schema.CharInfo.newBuilder().setLength(charType.getLength()))
                    .setNullable(charType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(VarCharType varCharType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.VARCHAR)
                    .setVarCharInfo(
                            FlinkFnApi.Schema.VarCharInfo.newBuilder()
                                    .setLength(varCharType.getLength()))
                    .setNullable(varCharType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(DateType dateType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.DATE)
                    .setNullable(dateType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(TimeType timeType) {
            return FlinkFnApi.Schema.FieldType.newBuilder()
                    .setTypeName(FlinkFnApi.Schema.TypeName.TIME)
                    .setTimeInfo(
                            FlinkFnApi.Schema.TimeInfo.newBuilder()
                                    .setPrecision(timeType.getPrecision()))
                    .setNullable(timeType.isNullable())
                    .build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(TimestampType timestampType) {
            FlinkFnApi.Schema.FieldType.Builder builder =
                    FlinkFnApi.Schema.FieldType.newBuilder()
                            .setTypeName(FlinkFnApi.Schema.TypeName.TIMESTAMP)
                            .setNullable(timestampType.isNullable());

            FlinkFnApi.Schema.TimestampInfo.Builder timestampInfoBuilder =
                    FlinkFnApi.Schema.TimestampInfo.newBuilder()
                            .setPrecision(timestampType.getPrecision());
            builder.setTimestampInfo(timestampInfoBuilder);
            return builder.build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(LocalZonedTimestampType localZonedTimestampType) {
            FlinkFnApi.Schema.FieldType.Builder builder =
                    FlinkFnApi.Schema.FieldType.newBuilder()
                            .setTypeName(FlinkFnApi.Schema.TypeName.LOCAL_ZONED_TIMESTAMP)
                            .setNullable(localZonedTimestampType.isNullable());

            FlinkFnApi.Schema.LocalZonedTimestampInfo.Builder dateTimeBuilder =
                    FlinkFnApi.Schema.LocalZonedTimestampInfo.newBuilder()
                            .setPrecision(localZonedTimestampType.getPrecision());
            builder.setLocalZonedTimestampInfo(dateTimeBuilder.build());
            return builder.build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(DecimalType decimalType) {
            FlinkFnApi.Schema.FieldType.Builder builder =
                    FlinkFnApi.Schema.FieldType.newBuilder()
                            .setTypeName(FlinkFnApi.Schema.TypeName.DECIMAL)
                            .setNullable(decimalType.isNullable());

            FlinkFnApi.Schema.DecimalInfo.Builder decimalInfoBuilder =
                    FlinkFnApi.Schema.DecimalInfo.newBuilder()
                            .setPrecision(decimalType.getPrecision())
                            .setScale(decimalType.getScale());
            builder.setDecimalInfo(decimalInfoBuilder);
            return builder.build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(ArrayType arrayType) {
            FlinkFnApi.Schema.FieldType.Builder builder =
                    FlinkFnApi.Schema.FieldType.newBuilder()
                            .setTypeName(FlinkFnApi.Schema.TypeName.BASIC_ARRAY)
                            .setNullable(arrayType.isNullable());

            FlinkFnApi.Schema.FieldType elementFieldType = arrayType.getElementType().accept(this);
            builder.setCollectionElementType(elementFieldType);
            return builder.build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(MapType mapType) {
            FlinkFnApi.Schema.FieldType.Builder builder =
                    FlinkFnApi.Schema.FieldType.newBuilder()
                            .setTypeName(FlinkFnApi.Schema.TypeName.MAP)
                            .setNullable(mapType.isNullable());

            FlinkFnApi.Schema.MapInfo.Builder mapBuilder =
                    FlinkFnApi.Schema.MapInfo.newBuilder()
                            .setKeyType(mapType.getKeyType().accept(this))
                            .setValueType(mapType.getValueType().accept(this));
            builder.setMapInfo(mapBuilder.build());
            return builder.build();
        }

        @Override
        public FlinkFnApi.Schema.FieldType visit(RowType rowType) {
            FlinkFnApi.Schema.FieldType.Builder builder =
                    FlinkFnApi.Schema.FieldType.newBuilder()
                            .setTypeName(FlinkFnApi.Schema.TypeName.ROW)
                            .setNullable(rowType.isNullable());

            FlinkFnApi.Schema.Builder schemaBuilder = FlinkFnApi.Schema.newBuilder();
            for (RowType.RowField field : rowType.getFields()) {
                schemaBuilder.addFields(
                        FlinkFnApi.Schema.Field.newBuilder()
                                .setName(field.getName())
                                .setDescription(field.getDescription().orElse(EMPTY_STRING))
                                .setType(field.getType().accept(this))
                                .build());
            }
            builder.setRowSchema(schemaBuilder.build());
            return builder.build();
        }

        @Override
        protected FlinkFnApi.Schema.FieldType defaultMethod(LogicalType logicalType) {
            if (logicalType instanceof LegacyTypeInformationType) {
                Class<?> typeClass =
                        ((LegacyTypeInformationType) logicalType)
                                .getTypeInformation()
                                .getTypeClass();
                if (typeClass == BigDecimal.class) {
                    FlinkFnApi.Schema.FieldType.Builder builder =
                            FlinkFnApi.Schema.FieldType.newBuilder()
                                    .setTypeName(FlinkFnApi.Schema.TypeName.DECIMAL)
                                    .setNullable(logicalType.isNullable());
                    // Because we can't get precision and scale from legacy BIG_DEC_TYPE_INFO,
                    // we set the precision and scale to default value compatible with python.
                    FlinkFnApi.Schema.DecimalInfo.Builder decimalTypeBuilder =
                            FlinkFnApi.Schema.DecimalInfo.newBuilder()
                                    .setPrecision(38)
                                    .setScale(18);
                    builder.setDecimalInfo(decimalTypeBuilder);
                    return builder.build();
                }
            }
            throw new UnsupportedOperationException(
                    String.format(
                            "Python UDF doesn't support logical type %s currently.",
                            logicalType.asSummaryString()));
        }
    }
}
