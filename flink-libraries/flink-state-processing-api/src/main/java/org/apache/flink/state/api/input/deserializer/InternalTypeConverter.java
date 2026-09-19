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

package org.apache.flink.state.api.input.deserializer;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.data.GenericMapData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.MultisetType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

/**
 * Converts external Java objects (as produced by DataStream serializers) to Flink table internal
 * types as expected by {@link GenericRowData}.
 *
 * <p>Conversion rules:
 *
 * <ul>
 *   <li>{@link String} → {@link StringData}
 *   <li>{@link BigDecimal} → {@link DecimalData} (precision/scale from {@link DecimalType})
 *   <li>{@link ByteBuffer} or {@code byte[]} → {@link DecimalData} (unscaled bytes)
 *   <li>{@link ByteBuffer} → {@code byte[]} for BINARY/VARBINARY
 *   <li>{@link java.sql.Date}, {@link LocalDate} → {@code int} (days since epoch)
 *   <li>{@link Timestamp}, {@link Instant}, {@link LocalDateTime} → {@link TimestampData}
 *   <li>{@link List}, arrays, {@link Iterable} → {@link GenericArrayData} (elements recursively
 *       converted)
 *   <li>{@link Map} or {@link Iterable} of {@link Map.Entry} → {@link GenericMapData} (keys/values
 *       recursively converted)
 *   <li>{@link Row} → {@link GenericRowData} (fields recursively converted)
 *   <li>{@link RowData} subtypes → passed through unchanged
 *   <li>Primitives (boxed) → passed through unchanged
 * </ul>
 */
@Internal
public final class InternalTypeConverter {

    private InternalTypeConverter() {}

    /**
     * Converts {@code value} to the Flink table internal representation dictated by {@code type}.
     *
     * @param value the raw Java object; may be null
     * @param type the target logical type; used to drive nested conversions
     * @return the converted value, or null if value is null
     */
    @Nullable
    public static Object toInternal(@Nullable Object value, LogicalType type) {
        if (value == null) {
            return null;
        }

        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                if (value instanceof StringData) {
                    return value;
                }
                return StringData.fromString(value.toString());

            case BOOLEAN:
            case TINYINT:
            case SMALLINT:
            case INTEGER:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case TIME_WITHOUT_TIME_ZONE:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_DAY_TIME:
                return value;

            case DECIMAL:
                if (value instanceof DecimalData) {
                    return value;
                }
                if (value instanceof BigDecimal) {
                    DecimalType dt = (DecimalType) type;
                    return DecimalData.fromBigDecimal(
                            (BigDecimal) value, dt.getPrecision(), dt.getScale());
                }
                if (value instanceof ByteBuffer) {
                    DecimalType dt = (DecimalType) type;
                    return DecimalData.fromUnscaledBytes(
                            toByteArray((ByteBuffer) value), dt.getPrecision(), dt.getScale());
                }
                if (value instanceof byte[]) {
                    DecimalType dt = (DecimalType) type;
                    return DecimalData.fromUnscaledBytes(
                            (byte[]) value, dt.getPrecision(), dt.getScale());
                }
                return value;

            case DATE:
                if (value instanceof Integer) {
                    return value;
                }
                if (value instanceof java.sql.Date) {
                    return (int) ((java.sql.Date) value).toLocalDate().toEpochDay();
                }
                if (value instanceof LocalDate) {
                    return (int) ((LocalDate) value).toEpochDay();
                }
                return value;

            case TIMESTAMP_WITHOUT_TIME_ZONE:
            case TIMESTAMP_WITH_TIME_ZONE:
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                if (value instanceof TimestampData) {
                    return value;
                }
                if (value instanceof Timestamp) {
                    return TimestampData.fromTimestamp((Timestamp) value);
                }
                if (value instanceof Instant) {
                    return TimestampData.fromInstant((Instant) value);
                }
                if (value instanceof LocalDateTime) {
                    return TimestampData.fromLocalDateTime((LocalDateTime) value);
                }
                return value;

            case BINARY:
            case VARBINARY:
                if (value instanceof ByteBuffer) {
                    return toByteArray((ByteBuffer) value);
                }
                return value;

            case NULL:
                return null;

            case ROW:
            case STRUCTURED_TYPE:
                if (value instanceof GenericRowData) {
                    return value;
                }
                if (value instanceof Row) {
                    return rowToGenericRowData((Row) value, (RowType) type);
                }
                return value;

            case ARRAY:
                if (value instanceof GenericArrayData) {
                    return value;
                }
                ArrayType at = (ArrayType) type;
                if (value instanceof Object[]) {
                    return objectArrayToArrayData((Object[]) value, at.getElementType());
                }
                if (value instanceof Iterable) {
                    return iterableToArrayData((Iterable<?>) value, at.getElementType());
                }
                return value;

            case MAP:
                if (value instanceof GenericMapData) {
                    return value;
                }
                MapType mt = (MapType) type;
                if (value instanceof Map) {
                    return mapToMapData((Map<?, ?>) value, mt.getKeyType(), mt.getValueType());
                }
                if (value instanceof Iterable) {
                    return mapEntryIterableToMapData(
                            (Iterable<?>) value, mt.getKeyType(), mt.getValueType());
                }
                return value;

            case MULTISET:
                // MultisetType is not a MapType: it has only an element type, represented
                // internally as Map<element, Integer> (element -> multiplicity).
                if (value instanceof GenericMapData) {
                    return value;
                }
                LogicalType elementType = ((MultisetType) type).getElementType();
                if (value instanceof Map) {
                    return mapToMapData((Map<?, ?>) value, elementType, new IntType());
                }
                if (value instanceof Iterable) {
                    return mapEntryIterableToMapData(
                            (Iterable<?>) value, elementType, new IntType());
                }
                return value;

            default:
                throw new UnsupportedOperationException(
                        "Cannot convert value of type '"
                                + value.getClass().getName()
                                + "' to internal representation for LogicalTypeRoot "
                                + type.getTypeRoot()
                                + ".");
        }
    }

    private static byte[] toByteArray(ByteBuffer bb) {
        byte[] bytes = new byte[bb.remaining()];
        bb.get(bytes);
        return bytes;
    }

    private static GenericRowData rowToGenericRowData(Row row, RowType rowType) {
        List<RowType.RowField> fields = rowType.getFields();
        GenericRowData out = new GenericRowData(row.getArity());
        out.setRowKind(row.getKind());
        for (int i = 0; i < row.getArity(); i++) {
            LogicalType fieldType = i < fields.size() ? fields.get(i).getType() : null;
            Object rawField = row.getField(i);
            out.setField(i, fieldType != null ? toInternal(rawField, fieldType) : rawField);
        }
        return out;
    }

    private static GenericArrayData objectArrayToArrayData(Object[] src, LogicalType elementType) {
        Object[] arr = new Object[src.length];
        for (int i = 0; i < src.length; i++) {
            arr[i] = toInternal(src[i], elementType);
        }
        return new GenericArrayData(arr);
    }

    private static GenericArrayData iterableToArrayData(
            Iterable<?> iterable, LogicalType elementType) {
        return new GenericArrayData(
                StreamSupport.stream(iterable.spliterator(), false)
                        .map(v -> toInternal(v, elementType))
                        .toArray());
    }

    private static GenericMapData mapToMapData(
            Map<?, ?> map, LogicalType keyType, LogicalType valueType) {
        LinkedHashMap<Object, Object> converted = new LinkedHashMap<>(map.size());
        for (Map.Entry<?, ?> entry : map.entrySet()) {
            converted.put(
                    toInternal(entry.getKey(), keyType), toInternal(entry.getValue(), valueType));
        }
        return new GenericMapData(converted);
    }

    private static GenericMapData mapEntryIterableToMapData(
            Iterable<?> iterable, LogicalType keyType, LogicalType valueType) {
        LinkedHashMap<Object, Object> converted = new LinkedHashMap<>();
        for (Object element : iterable) {
            if (!(element instanceof Map.Entry)) {
                throw new UnsupportedOperationException(
                        "Map conversion supports only Iterable<Map.Entry> but received: "
                                + iterable.getClass().getName());
            }
            Map.Entry<?, ?> entry = (Map.Entry<?, ?>) element;
            converted.put(
                    toInternal(entry.getKey(), keyType), toInternal(entry.getValue(), valueType));
        }
        return new GenericMapData(converted);
    }
}
