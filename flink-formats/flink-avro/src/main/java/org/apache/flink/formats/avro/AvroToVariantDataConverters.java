/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantBuilder;

import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Internal
public class AvroToVariantDataConverters {
    private static final VariantBuilder SHARED_BUILDER = Variant.newBuilder();

    public static AvroToVariantDataConverter createVariantConverter(Schema schema) {
        return createNullableConverter(schema);
    }

    /** Creates a converter that directly maps Avro types to Variant types. */
    private static AvroToVariantDataConverter createAvroToVariantConverter(Schema schema) {
        LogicalType logicalType = schema.getLogicalType();

        switch (schema.getType()) {
            case RECORD:
                List<Schema.Field> fields = schema.getFields();
                String[] fieldNames = new String[fields.size()];
                AvroToVariantDataConverter[] fieldConverters =
                        new AvroToVariantDataConverter[fields.size()];

                for (int i = 0; i < fields.size(); i++) {
                    Schema.Field field = fields.get(i);
                    fieldNames[i] = field.name();
                    fieldConverters[i] = createNullableConverter(field.schema());
                }

                return (avroObject) -> {
                    GenericRecord record = (GenericRecord) avroObject;
                    VariantBuilder.VariantObjectBuilder variantObjectBuilder =
                            SHARED_BUILDER.object();

                    for (int i = 0; i < fieldNames.length; i++) {
                        String fieldName = fieldNames[i];
                        AvroToVariantDataConverter converter = fieldConverters[i];
                        Object fieldValue = record.get(fieldName);

                        variantObjectBuilder.add(fieldName, converter.convert(fieldValue));
                    }

                    return variantObjectBuilder.build();
                };

            case NULL:
                return (avroObject) -> SHARED_BUILDER.ofNull();

            case BOOLEAN:
                return (avroObject) -> SHARED_BUILDER.of((Boolean) avroObject);

            case INT:
                if (logicalType == LogicalTypes.date()) {
                    return (avroObject) -> SHARED_BUILDER.of(convertToDate((Integer) avroObject));
                } // Time-millis (logical type represents a time of day, with no reference to a
                // particular calendar).
                // Store it as LONG by converting millis to micros as the logical type millis or
                // micros is lost.
                else if (logicalType == LogicalTypes.timeMillis()) {
                    return (avroObject) -> SHARED_BUILDER.of((Integer) avroObject * 1000L);
                } else {
                    return (avroObject) -> SHARED_BUILDER.of((Integer) avroObject);
                }

            case LONG:
                if (logicalType == LogicalTypes.timestampMillis()
                        || logicalType == LogicalTypes.timestampMicros()) {
                    return (avroObject) ->
                            SHARED_BUILDER.of(
                                    convertToTimestamp(
                                            (Long) avroObject,
                                            LogicalTypes.timestampMicros() == logicalType));
                } else {
                    return (avroObject) -> SHARED_BUILDER.of((Long) avroObject);
                }

            case FLOAT:
                return (avroObject) -> SHARED_BUILDER.of((Float) avroObject);

            case DOUBLE:
                return (avroObject) -> SHARED_BUILDER.of((Double) avroObject);

            case BYTES:
            case FIXED:
                if (logicalType instanceof LogicalTypes.Decimal) {
                    LogicalTypes.Decimal decimalType = (LogicalTypes.Decimal) logicalType;
                    return createDecimalConverter(
                            decimalType.getPrecision(), decimalType.getScale());
                } else {
                    return object -> SHARED_BUILDER.of(convertToBytes(object));
                }

            case STRING:
            case ENUM:
                return (avroObject) -> SHARED_BUILDER.of(avroObject.toString());

            case ARRAY:
                Schema elementSchema = schema.getElementType();
                AvroToVariantDataConverter elementConverter =
                        createNullableConverter(elementSchema);
                return createArrayConverter(elementConverter);

            case MAP:
                Schema valueSchema = schema.getValueType();
                AvroToVariantDataConverter valueConverter = createNullableConverter(valueSchema);
                return createMapConverter(valueConverter);

            case UNION:
                // Handle nullable types (union with null)
                List<Schema> nonNullUnionType =
                        schema.getTypes().stream()
                                .filter(t -> t.getType() != Schema.Type.NULL)
                                .collect(Collectors.toList());

                if (nonNullUnionType.size() == 1) {
                    return createNullableConverter(nonNullUnionType.get(0));
                } else {
                    throw new UnsupportedOperationException(
                            "Avro Union with NULL type is only supported. Unsupported types: "
                                    + schema.getTypes());
                }

            default:
                throw new UnsupportedOperationException("Unsupported type: " + schema.getType());
        }
    }

    /** Creates an array converter that works directly with Avro elements. */
    private static AvroToVariantDataConverter createArrayConverter(
            AvroToVariantDataConverter elementConverter) {
        return (avroObject) -> {
            List<?> list = (List<?>) avroObject;
            VariantBuilder.VariantArrayBuilder variantArrayBuilder = SHARED_BUILDER.array();

            for (Object item : list) {
                Variant convertedItem = elementConverter.convert(item);
                variantArrayBuilder.add(convertedItem);
            }

            return variantArrayBuilder.build();
        };
    }

    /** Creates a map converter that works directly with Avro values. */
    private static AvroToVariantDataConverter createMapConverter(
            AvroToVariantDataConverter valueConverter) {
        return (avroObject) -> {
            Map<?, ?> map = (Map<?, ?>) avroObject;
            VariantBuilder.VariantObjectBuilder variantObjectBuilder = SHARED_BUILDER.object();

            for (Map.Entry<?, ?> entry : map.entrySet()) {
                String key = entry.getKey().toString(); // Avro maps always have string keys
                Variant convertedValue = valueConverter.convert(entry.getValue());
                variantObjectBuilder.add(key, convertedValue);
            }

            return variantObjectBuilder.build();
        };
    }

    /** Creates a decimal converter for the specified precision and scale. */
    private static AvroToVariantDataConverter createDecimalConverter(int precision, int scale) {
        return avroObject -> {
            if (avroObject instanceof BigDecimal) {
                return SHARED_BUILDER.of((BigDecimal) avroObject);
            }

            return SHARED_BUILDER.of(
                    DecimalData.fromUnscaledBytes(convertToBytes(avroObject), precision, scale)
                            .toBigDecimal());
        };
    }

    /*
    timestamp-millis logical type annotates an Avro long, where the long stores
    the number of milliseconds from the unix epoch, 1 January 1970 00:00:00.000 UTC.
     */
    private static LocalDateTime convertToTimestamp(Long object, boolean isMicros) {
        int nanos;
        long secs;

        if (isMicros) {
            secs = object / 1000_000;
            nanos = (int) (object - secs * 1000_000) * 1000;
        } else {
            secs = object / 1000L;
            nanos = (int) (object - secs * 1000L) * 1000_000;
        }

        return LocalDateTime.ofEpochSecond(secs, nanos, ZoneOffset.UTC);
    }

    /*
    The date logical type represents a date within the calendar, with no reference to a particular time zone or time of day.
    A date logical type annotates an Avro int, where the int stores the number of days from the unix epoch, 1 January 1970 (ISO calendar).
     */
    private static LocalDate convertToDate(Integer numOfDays) {
        return LocalDate.ofEpochDay(numOfDays);
    }

    private static byte[] convertToBytes(Object object) {
        if (object instanceof GenericFixed) {
            return ((GenericFixed) object).bytes();
        } else if (object instanceof ByteBuffer) {
            ByteBuffer byteBuffer = (ByteBuffer) object;
            byte[] bytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(bytes);
            return bytes;
        } else {
            return (byte[]) object;
        }
    }

    private static AvroToVariantDataConverter createNullableConverter(Schema schema) {
        final AvroToVariantDataConverter converter = createAvroToVariantConverter(schema);
        return avroObject -> {
            if (avroObject == null) {
                return SHARED_BUILDER.ofNull();
            }
            return converter.convert(avroObject);
        };
    }

    @FunctionalInterface
    public interface AvroToVariantDataConverter extends Serializable {
        Variant convert(Object object);
    }
}
