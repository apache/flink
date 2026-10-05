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

package org.apache.flink.table.data.utils;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.ArrayData;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.MapData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.StringUtf8Utils;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.DistinctType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.LogicalTypeFamily;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks;
import org.apache.flink.table.types.logical.utils.UuidUtils;
import org.apache.flink.types.variant.BinaryVariant;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder;
import org.apache.flink.types.variant.BinaryVariantInternalBuilder.FieldEntry;
import org.apache.flink.types.variant.BinaryVariantUtil;
import org.apache.flink.types.variant.Variant;
import org.apache.flink.types.variant.VariantTypeException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.table.utils.DateTimeUtils.MILLIS_PER_DAY;

/**
 * Converts internal data of a SQL type into a {@link Variant}. It implements {@code CAST(x AS
 * VARIANT)}, and a format can use it to store a value in a {@code VARIANT} the same way.
 *
 * <p>A leaf keeps the kind of its SQL type. For example, a {@code BIGINT} is stored as a {@code
 * BIGINT}, even when it would fit a smaller kind. A {@code TIMESTAMP(p)} or {@code
 * TIMESTAMP_LTZ(p)} is stored with microseconds up to {@code p = 6} and with nanoseconds above,
 * whatever the digits of the value. A {@code NaN} or infinite {@code FLOAT} or {@code DOUBLE} is
 * stored as is, although {@code PARSE_JSON} rejects it.
 *
 * <p>An {@code ARRAY} becomes a variant array. A {@code ROW} or {@code STRUCTURED} value becomes a
 * variant object keyed by its field names, and a {@code MAP} one keyed by its character string
 * keys. A {@code NULL} becomes a variant null, and a nested {@code VARIANT} is copied in as is.
 *
 * <p>The converter is created once per type and writes a whole value into one builder. A nested
 * {@code ARRAY}, {@code MAP} or {@code ROW} is written in place rather than built as a separate
 * variant first. The converter keeps no state between calls.
 */
@Internal
public final class ToVariantConverter implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The highest timestamp precision a {@code VARIANT} stores with microseconds. */
    public static final int TIMESTAMP_PRECISION = 6;

    /**
     * The largest string or binary value a {@code VARIANT} holds, in bytes. A {@code VARIANT} is
     * limited to 16 MiB, and a long string or binary value spends 5 bytes of that on its header.
     */
    public static final int MAX_PAYLOAD_BYTES =
            BinaryVariantUtil.SIZE_LIMIT - 1 - BinaryVariantUtil.U32_SIZE;

    private final String sourceType;
    private final ValueWriter writer;

    private ToVariantConverter(String sourceType, ValueWriter writer) {
        this.sourceType = sourceType;
        this.writer = writer;
    }

    /**
     * Creates a converter for a type that {@code LogicalTypeCasts} allows to cast to VARIANT.
     *
     * @throws IllegalArgumentException if the type cannot be cast to VARIANT
     */
    public static ToVariantConverter create(LogicalType type) {
        return new ToVariantConverter(type.asSummaryString(), createNullableWriter(type));
    }

    /**
     * Converts a value into a new {@code VARIANT}. A {@code null} value becomes a variant null.
     *
     * @throws TableRuntimeException if the value has a {@code NULL} map key, a time or timestamp
     *     outside the range of its variant kind, does not fit into the 16 MiB of a {@code VARIANT},
     *     or is nested too deeply
     * @throws VariantTypeException if a nested {@code VARIANT} is malformed
     */
    public Variant convert(Object value) {
        // A MAP can hold the same key twice at runtime. The builder keeps the last one, like the
        // MAP constructor does.
        final BinaryVariantInternalBuilder builder = new BinaryVariantInternalBuilder(true);
        try {
            writeTo(builder, value);
            return builder.build();
        } catch (VariantTypeException e) {
            if (e != BinaryVariantInternalBuilder.VARIANT_SIZE_LIMIT_EXCEPTION) {
                throw e;
            }
            throw new TableRuntimeException(
                    String.format(
                            "Cannot cast a value of type %s to VARIANT. A VARIANT is limited to "
                                    + "16 MiB.",
                            sourceType));
        } catch (StackOverflowError e) {
            // The builder copies an embedded VARIANT with one call per nesting level, so a deeply
            // nested one overflows the stack. A bare StackOverflowError would not say which cast
            // failed or why.
            throw new TableRuntimeException(
                    String.format(
                            "Cannot cast a value of type %s to VARIANT because it is nested too "
                                    + "deeply.",
                            sourceType));
        }
    }

    /**
     * Writes a value into a builder that the caller owns, for example as a field of a larger {@code
     * VARIANT}. A {@code null} value becomes a variant null.
     *
     * <p>It fails like {@link #convert} for a {@code NULL} map key, a time or timestamp out of
     * range, and a string or binary value that can never fit. The caller builds the result, so it
     * handles {@link BinaryVariantInternalBuilder#VARIANT_SIZE_LIMIT_EXCEPTION} and a {@link
     * StackOverflowError} from a deeply nested {@code VARIANT} itself. A repeated {@code MAP} key
     * keeps the last value if the caller's builder allows duplicate keys, and fails otherwise. The
     * builder holds partial data after a failure, so the caller must not use it any further.
     */
    public void writeTo(BinaryVariantInternalBuilder builder, Object value) {
        writer.write(builder, value);
    }

    @FunctionalInterface
    private interface ValueWriter extends Serializable {
        void write(BinaryVariantInternalBuilder builder, Object value);
    }

    private static ValueWriter createNullableWriter(LogicalType type) {
        final ValueWriter writer = createWriter(type);
        return (builder, value) -> {
            if (value == null) {
                builder.appendNull();
            } else {
                writer.write(builder, value);
            }
        };
    }

    private static ValueWriter createWriter(LogicalType type) {
        switch (type.getTypeRoot()) {
            case NULL:
                return (builder, value) -> builder.appendNull();
            case BOOLEAN:
                return (builder, value) -> builder.appendBoolean((Boolean) value);
            case TINYINT:
                return (builder, value) -> builder.appendByte((Byte) value);
            case SMALLINT:
                return (builder, value) -> builder.appendShort((Short) value);
            case INTEGER:
                return (builder, value) -> builder.appendInt((Integer) value);
            case BIGINT:
                return (builder, value) -> builder.appendLong((Long) value);
            case FLOAT:
                return (builder, value) -> builder.appendFloat((Float) value);
            case DOUBLE:
                return (builder, value) -> builder.appendDouble((Double) value);
            case DECIMAL:
                return (builder, value) -> appendDecimal(builder, (DecimalData) value);
            case CHAR:
            case VARCHAR:
                return (builder, value) -> appendString(builder, (StringData) value);
            case BINARY:
            case VARBINARY:
                return (builder, value) -> appendBinary(builder, (byte[]) value);
            case DATE:
                return (builder, value) -> builder.appendDate((Integer) value);
            case TIME_WITHOUT_TIME_ZONE:
                return (builder, value) -> builder.appendTime(timeMicros((Integer) value));
            case TIMESTAMP_WITHOUT_TIME_ZONE:
                {
                    final int precision = LogicalTypeChecks.getPrecision(type);
                    if (precision <= TIMESTAMP_PRECISION) {
                        return (builder, value) ->
                                builder.appendTimestamp(timestampMicros((TimestampData) value));
                    }
                    final String timestampType = "TIMESTAMP(" + precision + ")";
                    return (builder, value) ->
                            builder.appendTimestampNanos(
                                    timestampNanos((TimestampData) value, timestampType));
                }
            case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                {
                    final int precision = LogicalTypeChecks.getPrecision(type);
                    if (precision <= TIMESTAMP_PRECISION) {
                        return (builder, value) ->
                                builder.appendTimestampLtz(timestampMicros((TimestampData) value));
                    }
                    final String timestampType = "TIMESTAMP_LTZ(" + precision + ")";
                    return (builder, value) ->
                            builder.appendTimestampLtzNanos(
                                    timestampNanos((TimestampData) value, timestampType));
                }
            case UUID:
                return (builder, value) -> builder.appendUuid(UuidUtils.fromBytes((byte[]) value));
            case VARIANT:
                return (builder, value) -> builder.appendVariant((BinaryVariant) value);
            case ARRAY:
                return createArrayWriter((ArrayType) type);
            case MAP:
                return createMapWriter((MapType) type);
            case ROW:
            case STRUCTURED_TYPE:
                return createRowWriter(type);
            case DISTINCT_TYPE:
                return createWriter(((DistinctType) type).getSourceType());
            default:
                throw new IllegalArgumentException("Cannot cast " + type + " to VARIANT.");
        }
    }

    // A compact decimal is written from its unscaled long, which saves building a BigDecimal.
    private static void appendDecimal(BinaryVariantInternalBuilder builder, DecimalData value) {
        if (value.isCompact()) {
            builder.appendDecimal(value.toUnscaledLong(), value.scale());
        } else {
            builder.appendDecimal(value.toBigDecimal());
        }
    }

    private static void appendString(BinaryVariantInternalBuilder builder, StringData value) {
        // The variant spec only allows valid UTF-8, but a StringData may hold invalid bytes.
        final byte[] utf8 = StringUtf8Utils.toValidUtf8Bytes(value);
        try {
            builder.appendString(utf8);
        } catch (VariantTypeException e) {
            throw payloadTooLarge(e, "string", utf8.length);
        }
    }

    private static void appendBinary(BinaryVariantInternalBuilder builder, byte[] value) {
        try {
            builder.appendBinary(value);
        } catch (VariantTypeException e) {
            throw payloadTooLarge(e, "binary", value.length);
        }
    }

    // A string or binary value too large on its own gets a message with its size. When only the
    // value as a whole is too large, convert reports that instead.
    private static RuntimeException payloadTooLarge(
            VariantTypeException e, String kind, int bytes) {
        if (e != BinaryVariantInternalBuilder.VARIANT_SIZE_LIMIT_EXCEPTION
                || bytes <= MAX_PAYLOAD_BYTES) {
            return e;
        }
        return new TableRuntimeException(
                String.format(
                        "Cannot cast a %s value of %d bytes to VARIANT. A VARIANT is limited to "
                                + "16 MiB, so a string or binary value can have at most %d bytes.",
                        kind, bytes, MAX_PAYLOAD_BYTES));
    }

    /** Microseconds of the day. TIME is millisecond-of-day at runtime. */
    private static long timeMicros(int millisOfDay) {
        if (millisOfDay < 0 || millisOfDay >= MILLIS_PER_DAY) {
            throw new TableRuntimeException(
                    String.format(
                            "Cannot cast the TIME value %d to VARIANT. A TIME holds the "
                                    + "milliseconds of one day, from 0 to %d.",
                            millisOfDay, MILLIS_PER_DAY - 1));
        }
        return millisOfDay * 1_000L;
    }

    /** Microseconds since the epoch, which cover every year a {@link TimestampData} can hold. */
    private static long timestampMicros(TimestampData value) {
        return value.getMillisecond() * 1_000L + value.getNanoOfMillisecond() / 1_000;
    }

    /** Nanoseconds since the epoch, which only cover 1677-09-21 to 2262-04-11. */
    private static long timestampNanos(TimestampData value, String timestampType) {
        final long millis = value.getMillisecond();
        final int nanos = value.getNanoOfMillisecond();
        try {
            // Before the epoch, the milliseconds alone can underflow although the sum fits, so
            // one millisecond moves into the nanoseconds first.
            return millis >= 0
                    ? Math.addExact(Math.multiplyExact(millis, 1_000_000L), nanos)
                    : Math.addExact(Math.multiplyExact(millis + 1, 1_000_000L), nanos - 1_000_000L);
        } catch (ArithmeticException e) {
            throw new TableRuntimeException(
                    String.format(
                            "Cannot cast the %s value %s to VARIANT. A VARIANT timestamp with "
                                    + "nanosecond precision only covers 1677-09-21 to 2262-04-11. "
                                    + "Cast the value to a precision of 6 or less first.",
                            timestampType, value));
        }
    }

    private static ValueWriter createArrayWriter(ArrayType type) {
        final ArrayData.ElementGetter elementGetter = createElementGetter(type.getElementType());
        final ValueWriter elementWriter = createNullableWriter(type.getElementType());
        return (builder, value) -> {
            final ArrayData array = (ArrayData) value;
            final int size = array.size();
            final int start = builder.getWritePos();
            final ArrayList<Integer> offsets = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                offsets.add(builder.getWritePos() - start);
                elementWriter.write(builder, elementGetter.getElementOrNull(array, i));
            }
            builder.finishWritingArray(start, offsets);
        };
    }

    private static ValueWriter createMapWriter(MapType type) {
        if (!type.getKeyType().is(LogicalTypeFamily.CHARACTER_STRING)) {
            throw new IllegalArgumentException(
                    "Cannot cast " + type + " to VARIANT. A MAP key must be a character string.");
        }
        final ArrayData.ElementGetter keyGetter = createElementGetter(type.getKeyType());
        final ArrayData.ElementGetter valueGetter = createElementGetter(type.getValueType());
        final ValueWriter valueWriter = createNullableWriter(type.getValueType());
        final String mapType = type.asSummaryString();
        return (builder, value) -> {
            final MapData map = (MapData) value;
            final ArrayData keys = map.keyArray();
            final ArrayData values = map.valueArray();
            final int size = map.size();
            final int start = builder.getWritePos();
            final ArrayList<FieldEntry> fields = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                final Object key = keyGetter.getElementOrNull(keys, i);
                if (key == null) {
                    throw new TableRuntimeException(
                            String.format(
                                    "Cannot cast a value of type %s with a NULL key to VARIANT. "
                                            + "A VARIANT object key cannot be NULL.",
                                    mapType));
                }
                final String name = key.toString();
                fields.add(
                        new FieldEntry(name, builder.addKey(name), builder.getWritePos() - start));
                valueWriter.write(builder, valueGetter.getElementOrNull(values, i));
            }
            builder.finishWritingObject(start, fields);
        };
    }

    private static ValueWriter createRowWriter(LogicalType type) {
        final List<LogicalType> fieldTypes = LogicalTypeChecks.getFieldTypes(type);
        final String[] fieldNames = LogicalTypeChecks.getFieldNames(type).toArray(new String[0]);
        final int fieldCount = fieldTypes.size();
        final RowData.FieldGetter[] fieldGetters = new RowData.FieldGetter[fieldCount];
        final ValueWriter[] fieldWriters = new ValueWriter[fieldCount];
        for (int i = 0; i < fieldCount; i++) {
            fieldGetters[i] = createFieldGetter(fieldTypes.get(i), i);
            fieldWriters[i] = createNullableWriter(fieldTypes.get(i));
        }
        return (builder, value) -> {
            final RowData row = (RowData) value;
            final int start = builder.getWritePos();
            final ArrayList<FieldEntry> fields = new ArrayList<>(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                final String name = fieldNames[i];
                fields.add(
                        new FieldEntry(name, builder.addKey(name), builder.getWritePos() - start));
                fieldWriters[i].write(builder, fieldGetters[i].getFieldOrNull(row));
            }
            builder.finishWritingObject(start, fields);
        };
    }

    // A NULL type has no accessor, but its values are always NULL, which becomes a variant null.
    private static ArrayData.ElementGetter createElementGetter(LogicalType type) {
        return type.is(LogicalTypeRoot.NULL)
                ? (array, pos) -> null
                : ArrayData.createElementGetter(type);
    }

    private static RowData.FieldGetter createFieldGetter(LogicalType type, int pos) {
        return type.is(LogicalTypeRoot.NULL) ? row -> null : RowData.createFieldGetter(type, pos);
    }
}
