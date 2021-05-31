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

package org.apache.flink.formats.avro.typeutils;

import org.apache.flink.types.Row;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

/** Utility class that takes care of conversions between avro and flink types. */
public class AvroConversions {
    /** Used for time conversions from SQL types. */
    private static final TimeZone LOCAL_TZ = TimeZone.getDefault();

    public static GenericRecord convertRowToAvroRecord(Schema schema, Row row) {
        final List<Schema.Field> fields = schema.getFields();
        final int length = fields.size();
        final GenericRecord record = new GenericData.Record(schema);
        for (int i = 0; i < length; i++) {
            final Schema.Field field = fields.get(i);
            record.put(i, convertFlinkType(field.schema(), row.getField(i)));
        }
        return record;
    }

    private static Object convertFlinkType(Schema schema, Object object) {
        if (object == null) {
            return null;
        }
        switch (schema.getType()) {
            case RECORD:
                if (object instanceof Row) {
                    return convertRowToAvroRecord(schema, (Row) object);
                }
                throw new IllegalStateException("Row expected but was: " + object.getClass());
            case ENUM:
                return new GenericData.EnumSymbol(schema, object.toString());
            case ARRAY:
                final Schema elementSchema = schema.getElementType();
                final Object[] array = (Object[]) object;
                final GenericData.Array<Object> convertedArray =
                        new GenericData.Array<>(array.length, schema);
                for (Object element : array) {
                    convertedArray.add(convertFlinkType(elementSchema, element));
                }
                return convertedArray;
            case MAP:
                final Map<?, ?> map = (Map<?, ?>) object;
                final Map<Utf8, Object> convertedMap = new HashMap<>();
                for (Map.Entry<?, ?> entry : map.entrySet()) {
                    convertedMap.put(
                            new Utf8(entry.getKey().toString()),
                            convertFlinkType(schema.getValueType(), entry.getValue()));
                }
                return convertedMap;
            case UNION:
                final List<Schema> types = schema.getTypes();
                final int size = types.size();
                final Schema actualSchema;
                if (size == 2 && types.get(0).getType() == Schema.Type.NULL) {
                    actualSchema = types.get(1);
                } else if (size == 2 && types.get(1).getType() == Schema.Type.NULL) {
                    actualSchema = types.get(0);
                } else if (size == 1) {
                    actualSchema = types.get(0);
                } else {
                    // generic type
                    return object;
                }
                return convertFlinkType(actualSchema, object);
            case FIXED:
                // check for logical type
                if (object instanceof BigDecimal) {
                    return new GenericData.Fixed(
                            schema, convertFromDecimal(schema, (BigDecimal) object));
                }
                return new GenericData.Fixed(schema, (byte[]) object);
            case STRING:
                return new Utf8(object.toString());
            case BYTES:
                // check for logical type
                if (object instanceof BigDecimal) {
                    return ByteBuffer.wrap(convertFromDecimal(schema, (BigDecimal) object));
                }
                return ByteBuffer.wrap((byte[]) object);
            case INT:
                // check for logical types
                if (object instanceof Date) {
                    return convertFromDate(schema, (Date) object);
                } else if (object instanceof LocalDate) {
                    return convertFromDate(schema, Date.valueOf((LocalDate) object));
                } else if (object instanceof Time) {
                    return convertFromTimeMillis(schema, (Time) object);
                } else if (object instanceof LocalTime) {
                    return convertFromTimeMillis(schema, Time.valueOf((LocalTime) object));
                }
                return object;
            case LONG:
                // check for logical type
                if (object instanceof Timestamp) {
                    return convertFromTimestamp(schema, (Timestamp) object);
                } else if (object instanceof LocalDateTime) {
                    return convertFromTimestamp(schema, Timestamp.valueOf((LocalDateTime) object));
                } else if (object instanceof Time) {
                    return convertFromTimeMicros(schema, (Time) object);
                }
                return object;
            case FLOAT:
            case DOUBLE:
            case BOOLEAN:
                return object;
        }
        throw new RuntimeException("Unsupported Avro type:" + schema);
    }

    private static byte[] convertFromDecimal(Schema schema, BigDecimal decimal) {
        final LogicalType logicalType = schema.getLogicalType();
        if (logicalType instanceof LogicalTypes.Decimal) {
            final LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
            // rescale to target type
            final BigDecimal rescaled =
                    decimal.setScale(decimalType.getScale(), BigDecimal.ROUND_UNNECESSARY);
            // byte array must contain the two's-complement representation of the
            // unscaled integer value in big-endian byte order
            return decimal.unscaledValue().toByteArray();
        } else {
            throw new RuntimeException("Unsupported decimal type.");
        }
    }

    private static int convertFromDate(Schema schema, Date date) {
        final LogicalType logicalType = schema.getLogicalType();
        if (logicalType == LogicalTypes.date()) {
            // adopted from Apache Calcite
            final long converted = toEpochMillis(date);
            return (int) (converted / 86400000L);
        } else {
            throw new RuntimeException("Unsupported date type.");
        }
    }

    private static int convertFromTimeMillis(Schema schema, Time date) {
        final LogicalType logicalType = schema.getLogicalType();
        if (logicalType == LogicalTypes.timeMillis()) {
            // adopted from Apache Calcite
            final long converted = toEpochMillis(date);
            return (int) (converted % 86400000L);
        } else {
            throw new RuntimeException("Unsupported time type.");
        }
    }

    private static long convertFromTimeMicros(Schema schema, Time date) {
        final LogicalType logicalType = schema.getLogicalType();
        if (logicalType == LogicalTypes.timeMicros()) {
            // adopted from Apache Calcite
            final long converted = toEpochMillis(date);
            return (converted % 86400000L) * 1000L;
        } else {
            throw new RuntimeException("Unsupported time type.");
        }
    }

    private static long convertFromTimestamp(Schema schema, Timestamp date) {
        final LogicalType logicalType = schema.getLogicalType();
        if (logicalType == LogicalTypes.timestampMillis()) {
            // adopted from Apache Calcite
            final long time = date.getTime();
            return time + (long) LOCAL_TZ.getOffset(time);
        } else if (logicalType == LogicalTypes.timestampMicros()) {
            long millis = date.getTime();
            long micros = millis * 1000 + (date.getNanos() % 1_000_000 / 1000);
            long offset = LOCAL_TZ.getOffset(millis) * 1000L;
            return micros + offset;
        } else {
            throw new RuntimeException("Unsupported timestamp type.");
        }
    }

    private static long toEpochMillis(java.util.Date date) {
        final long time = date.getTime();
        return time + (long) LOCAL_TZ.getOffset(time);
    }
}
