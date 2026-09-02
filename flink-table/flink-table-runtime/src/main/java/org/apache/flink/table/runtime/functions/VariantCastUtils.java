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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.data.binary.BinaryStringData;
import org.apache.flink.table.data.binary.BinaryStringDataUtil;
import org.apache.flink.table.data.binary.StringUtf8Utils;
import org.apache.flink.table.utils.DateTimeUtils;
import org.apache.flink.types.variant.Variant;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.TimeZone;

/**
 * Runtime helpers for casting a {@code VARIANT} value to a SQL type.
 *
 * <p>A cast never reinterprets the stored value: one kind is not read as another, and a numeric
 * value is never wrapped or rounded to make it fit. Any numeric kind therefore reaches an integer
 * target as long as the value is integral and in range. {@code FLOAT} and {@code DOUBLE} are the
 * exception to exactness: they are approximate by definition, so they accept any numeric kind and
 * reject only a magnitude they cannot represent at all.
 *
 * <p>A length or a precision is not part of the value in that sense, so it follows the rules of a
 * regular cast into the same type. A value longer than the target is trimmed, fractional seconds
 * beyond the target precision are truncated, and the fixed width targets {@code CHAR(n)} and {@code
 * BINARY(n)} pad a shorter value.
 */
@Internal
public final class VariantCastUtils {

    /**
     * The magnitude 2^63, the exclusive bound for a {@code double} that still fits a {@code long}.
     * Taken from {@link Long#MIN_VALUE} because that is exactly -2^63, whereas widening {@link
     * Long#MAX_VALUE} would reach the same number only by rounding up.
     */
    private static final double LONG_MAGNITUDE_LIMIT = -(double) Long.MIN_VALUE;

    /** A microsecond timestamp variant renders with six fractional-second digits. */
    private static final int TIMESTAMP_PRECISION = 6;

    /** A nanosecond timestamp variant renders with nine fractional-second digits. */
    private static final int TIMESTAMP_NANOS_PRECISION = 9;

    /**
     * A time variant keeps microseconds, but the runtime TIME representation is millisecond-of-day,
     * so it renders with three fractional-second digits, the same as a regular TIME to string cast.
     */
    private static final int TIME_PRECISION = 3;

    private VariantCastUtils() {}

    /**
     * Reads the size of an array variant, failing when the variant is not an array. A constructed
     * cast checks the shape at every level: only an array casts to {@code ARRAY}.
     */
    public static int arraySize(Variant variant, String targetType) {
        if (variant.isArray()) {
            return variant.getArraySize();
        }
        throw wrongShape(variant, targetType, "an array");
    }

    /**
     * Fails when the variant is not an object, so a cast to {@code ROW}, {@code STRUCTURED}, or
     * {@code MAP} reports a shape mismatch clearly. Only an object carries named fields.
     */
    public static void requireObject(Variant variant, String targetType) {
        if (!variant.isObject()) {
            throw wrongShape(variant, targetType, "an object");
        }
    }

    /**
     * Renders an object field name as the map key, trimming or padding it to a bounded target the
     * same way a regular cast into the key type would. Object keys are always strings, so no kind
     * conversion happens here.
     */
    public static BinaryStringData variantKey(String name, int targetLength, boolean charTarget) {
        final BinaryStringData key = BinaryStringData.fromString(name);
        final int length = key.numChars();
        if (length > targetLength) {
            return key.substring(0, targetLength);
        }
        if (charTarget && length < targetLength) {
            return BinaryStringDataUtil.concat(
                    key, BinaryStringData.blankString(targetLength - length));
        }
        return key;
    }

    private static TableRuntimeException wrongShape(
            Variant variant, String targetType, String required) {
        return new TableRuntimeException(
                String.format(
                        "Cannot cast a VARIANT %s value to %s because the target requires %s. Only "
                                + "a variant array casts to ARRAY and only a variant object casts "
                                + "to ROW, STRUCTURED, or MAP.",
                        variant.getType(), targetType, required));
    }

    /**
     * Reads a numeric variant as a {@code long} and checks it against the target range. An
     * approximate or decimal value is accepted only when it is already integral, so nothing is
     * rounded away.
     */
    public static long toIntegral(Variant variant, long min, long max, String targetType) {
        final long value;
        switch (variant.getType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                value = ((Number) variant.get()).longValue();
                break;
            case FLOAT:
            case DOUBLE:
                final double approximate = ((Number) variant.get()).doubleValue();
                // Below 2^63 the narrowing conversion stays exact. The comparison is negated so
                // that NaN fails it too.
                if (!(Math.abs(approximate) < LONG_MAGNITUDE_LIMIT)) {
                    throw overflow(approximate, targetType);
                }
                value = (long) approximate;
                if (value != approximate) {
                    throw lossyCast(approximate, targetType);
                }
                break;
            case DECIMAL:
                final BigDecimal decimal = variant.getDecimal();
                final BigDecimal integral;
                try {
                    // UNNECESSARY throws unless the value is already integral.
                    integral = decimal.setScale(0, RoundingMode.UNNECESSARY);
                } catch (ArithmeticException e) {
                    throw lossyCast(decimal, targetType);
                }
                try {
                    // longValueExact rejects a value that does not fit a long instead of returning
                    // its low-order bits.
                    value = integral.longValueExact();
                } catch (ArithmeticException e) {
                    throw overflow(decimal, targetType);
                }
                break;
            default:
                throw unsupportedKind(variant, targetType);
        }
        if (value < min || value > max) {
            throw overflow(value, targetType);
        }
        return value;
    }

    /**
     * Reads any numeric variant as a {@code float}. Dropping decimal digits is expected of an
     * approximate type, but a magnitude outside the {@code FLOAT} range is rejected.
     */
    public static float toFloat(Variant variant) {
        final float value = numeric(variant, "FLOAT").floatValue();
        if (!Float.isFinite(value)) {
            throw overflow(variant.get(), "FLOAT");
        }
        return value;
    }

    /** Reads any numeric variant as a {@code double}. See {@link #toFloat(Variant)}. */
    public static double toDouble(Variant variant) {
        final double value = numeric(variant, "DOUBLE").doubleValue();
        if (!Double.isFinite(value)) {
            throw overflow(variant.get(), "DOUBLE");
        }
        return value;
    }

    /**
     * Reads an integer or decimal variant as the target {@code DECIMAL}. The value has to fit the
     * precision and scale without rounding, although trailing zeros may be appended to reach the
     * scale.
     */
    public static DecimalData toDecimal(Variant variant, int precision, int scale) {
        final BigDecimal value;
        switch (variant.getType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                value = BigDecimal.valueOf(((Number) variant.get()).longValue());
                break;
            case DECIMAL:
                value = variant.getDecimal();
                break;
            default:
                throw unsupportedKind(variant, decimalTarget(precision, scale));
        }
        // The integral part must fit the digits the target reserves for it.
        if (value.precision() - value.scale() > precision - scale) {
            throw overflow(value, decimalTarget(precision, scale));
        }
        final BigDecimal rescaled;
        try {
            // UNNECESSARY throws unless the value fits the target scale exactly.
            rescaled = value.setScale(scale, RoundingMode.UNNECESSARY);
        } catch (ArithmeticException e) {
            throw lossyCast(value, decimalTarget(precision, scale));
        }
        final DecimalData decimal = DecimalData.fromBigDecimal(rescaled, precision, scale);
        if (decimal == null) {
            throw overflow(value, decimalTarget(precision, scale));
        }
        return decimal;
    }

    private static String decimalTarget(int precision, int scale) {
        return String.format("DECIMAL(%d, %d)", precision, scale);
    }

    /**
     * Reads a timestamp variant as the target {@code TIMESTAMP}. {@link Variant#getDateTime()}
     * already accepts both the microsecond ({@link Variant.Type#TIMESTAMP}) and nanosecond ({@link
     * Variant.Type#TIMESTAMP_NS}) encodings. Fractional seconds beyond the target precision are
     * truncated, the same as a regular {@code TIMESTAMP} to {@code TIMESTAMP(p)} cast.
     */
    public static TimestampData toTimestamp(Variant variant, int precision) {
        final Variant.Type type = variant.getType();
        if (type != Variant.Type.TIMESTAMP && type != Variant.Type.TIMESTAMP_NS) {
            throw unsupportedKind(variant, String.format("TIMESTAMP(%d)", precision));
        }
        return DateTimeUtils.truncate(
                TimestampData.fromLocalDateTime(variant.getDateTime()), precision);
    }

    /** Reads a timestamp with local time zone variant. See {@link #toTimestamp(Variant, int)}. */
    public static TimestampData toTimestampLtz(Variant variant, int precision) {
        final Variant.Type type = variant.getType();
        if (type != Variant.Type.TIMESTAMP_LTZ && type != Variant.Type.TIMESTAMP_LTZ_NS) {
            throw unsupportedKind(variant, String.format("TIMESTAMP_LTZ(%d)", precision));
        }
        return DateTimeUtils.truncate(TimestampData.fromInstant(variant.getInstant()), precision);
    }

    /**
     * Reads a time variant as the target {@code TIME}. The runtime TIME representation is
     * millisecond-of-day, so a variant's microseconds are dropped and any fractional seconds beyond
     * the target precision are then truncated, the same as a regular {@code TIME} to {@code
     * TIME(p)} cast.
     */
    public static int toTime(Variant variant, int precision) {
        if (variant.getType() != Variant.Type.TIME) {
            throw unsupportedKind(variant, String.format("TIME(%d)", precision));
        }
        return DateTimeUtils.applyTimePrecisionTruncation(
                DateTimeUtils.toInternal(variant.getTime()), precision);
    }

    /**
     * Reads a binary variant as the target binary type. {@code BINARY} is fixed width, so a shorter
     * value is padded with zero bytes, and either target truncates a value longer than {@code
     * targetLength}. This matches a regular cast into the same type.
     */
    public static byte[] toBytes(Variant variant, int targetLength, boolean fixedLength) {
        final byte[] value = variant.getBytes();
        if (fixedLength) {
            return value.length == targetLength ? value : Arrays.copyOf(value, targetLength);
        }
        return value.length <= targetLength ? value : Arrays.copyOf(value, targetLength);
    }

    /**
     * Casts a {@code VARIANT} to a character string, rendering the stored kind the way a regular
     * SQL cast would. An object or array renders like a {@code MAP} or {@code ARRAY} cast, an array
     * as {@code [e1, e2]} and an object as {@code {k1=v1, k2=v2}}, with a nested null shown as
     * {@code NULL}. Strings are never quoted, at any depth; this is a SQL rendering, not JSON, so
     * use {@code JSON_STRING} for the JSON form. A value longer than {@code targetLength} is
     * trimmed and a {@code CHAR} target pads a shorter one, both counted in code points rather than
     * UTF-16 units. A binary value must be well-formed UTF-8, and is rejected rather than decoded
     * into {@code U+FFFD}.
     *
     * @param sessionZone the session time zone, applied to a {@code TIMESTAMP_LTZ} value
     */
    public static BinaryStringData toStringValue(
            Variant variant, TimeZone sessionZone, int targetLength, boolean charTarget) {
        final String value = renderValue(variant, sessionZone, targetLength, charTarget);
        // numChars and substring both count code points, so a character outside the BMP fills one
        // position rather than the two UTF-16 units it occupies.
        return variantKey(value, targetLength, charTarget);
    }

    /**
     * Renders a variant as a character string. An array becomes {@code [e1, e2]} and an object
     * becomes {@code {k1=v1, k2=v2}}, matching how a regular {@code ARRAY} or {@code MAP} casts to
     * a string. Elements and field values recurse through the same rendering, so a string stays
     * unquoted at every depth. A scalar renders like a regular cast of its stored kind.
     */
    private static String renderValue(
            final Variant variant,
            final TimeZone sessionZone,
            final int targetLength,
            final boolean charTarget) {
        if (variant.isArray()) {
            final int size = variant.getArraySize();
            final StringBuilder sb = new StringBuilder();
            sb.append('[');
            for (int i = 0; i < size; i++) {
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append(renderElement(variant.getElement(i), sessionZone));
            }
            return sb.append(']').toString();
        }
        if (variant.isObject()) {
            final StringBuilder sb = new StringBuilder();
            sb.append('{');
            boolean first = true;
            for (final String fieldName : variant.getFieldNames()) {
                if (!first) {
                    sb.append(", ");
                }
                first = false;
                sb.append(fieldName)
                        .append('=')
                        .append(renderElement(variant.getField(fieldName), sessionZone));
            }
            return sb.append('}').toString();
        }
        return renderScalar(variant, sessionZone, targetLength, charTarget);
    }

    /** Renders one array element or object field value; a nested null shows as {@code NULL}. */
    private static String renderElement(final Variant element, final TimeZone sessionZone) {
        return element.isNull()
                ? "NULL"
                : renderValue(element, sessionZone, Integer.MAX_VALUE, false);
    }

    private static String renderScalar(
            final Variant variant,
            final TimeZone sessionZone,
            final int targetLength,
            final boolean charTarget) {
        final String value;
        switch (variant.getType()) {
            case BOOLEAN:
                value = variant.getBoolean() ? "TRUE" : "FALSE";
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
                value = variant.get().toString();
                break;
            case DECIMAL:
                // toPlainString rather than toString, so that a small scale is not rendered in
                // scientific notation, matching a regular DECIMAL to string cast.
                value = variant.getDecimal().toPlainString();
                break;
            case STRING:
                value = variant.getString();
                break;
            case BYTES:
                // SQL reads a binary value as UTF-8, the same as a regular BINARY to string cast.
                final byte[] utf8 = variant.getBytes();
                final int invalidAt =
                        StringUtf8Utils.firstInvalidUtf8ByteIndex(utf8, 0, utf8.length);
                if (invalidAt >= 0) {
                    final String targetDescription = characterTarget(targetLength, charTarget);
                    throw new TableRuntimeException(
                            String.format(
                                    "Cannot cast the VARIANT binary value to %s because it is not "
                                            + "valid UTF-8; the first invalid byte is at index %d "
                                            + "of %d. Cast to BYTES to inspect the raw value, or "
                                            + "wrap that in MAKE_VALID_UTF8 to replace every "
                                            + "invalid byte with the U+FFFD replacement character.",
                                    targetDescription, invalidAt, utf8.length));
                }
                value = new String(utf8, StandardCharsets.UTF_8);
                break;
            case DATE:
                value = DateTimeUtils.formatDate((int) variant.getDate().toEpochDay());
                break;
            case TIME:
                value =
                        DateTimeUtils.formatTimestampMillis(
                                DateTimeUtils.toInternal(variant.getTime()), TIME_PRECISION);
                break;
            case TIMESTAMP:
                // A wall-clock value needs no zone shift, which is what UTC_ZONE achieves here.
                value =
                        DateTimeUtils.formatTimestamp(
                                TimestampData.fromLocalDateTime(variant.getDateTime()),
                                DateTimeUtils.UTC_ZONE,
                                TIMESTAMP_PRECISION);
                break;
            case TIMESTAMP_NS:
                value =
                        DateTimeUtils.formatTimestamp(
                                TimestampData.fromLocalDateTime(variant.getDateTime()),
                                DateTimeUtils.UTC_ZONE,
                                TIMESTAMP_NANOS_PRECISION);
                break;
            case TIMESTAMP_LTZ:
                value =
                        DateTimeUtils.formatTimestamp(
                                TimestampData.fromInstant(variant.getInstant()),
                                sessionZone,
                                TIMESTAMP_PRECISION);
                break;
            case TIMESTAMP_LTZ_NS:
                value =
                        DateTimeUtils.formatTimestamp(
                                TimestampData.fromInstant(variant.getInstant()),
                                sessionZone,
                                TIMESTAMP_NANOS_PRECISION);
                break;
            case NULL:
                // Only reachable for a NOT NULL target. A nullable target maps a null-valued
                // variant to SQL NULL before this method is called.
                final String targetDescription = characterTarget(targetLength, charTarget);
                throw new TableRuntimeException(
                        String.format(
                                "Cannot cast a VARIANT null value to %s because the target does not "
                                        + "accept NULL.",
                                targetDescription));
            default:
                // Any remaining kind has no scalar string rendering; JSON_STRING serializes it.
                throw new TableRuntimeException(
                        String.format(
                                "Cannot cast a VARIANT %s value to a character string. Use the "
                                        + "JSON_STRING function to obtain its JSON representation.",
                                variant.getType()));
        }
        return value;
    }

    private static String characterTarget(int targetLength, boolean charTarget) {
        if (charTarget) {
            return String.format("%s(%d)", "CHAR", targetLength);
        }
        if (targetLength == Integer.MAX_VALUE) {
            return "STRING";
        }
        return String.format("%s(%d)", "VARCHAR", targetLength);
    }

    private static Number numeric(Variant variant, String targetType) {
        switch (variant.getType()) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
            case FLOAT:
            case DOUBLE:
            case DECIMAL:
                return (Number) variant.get();
            default:
                throw unsupportedKind(variant, targetType);
        }
    }

    private static TableRuntimeException unsupportedKind(Variant variant, String targetType) {
        return new TableRuntimeException(
                String.format(
                        "Cannot cast a VARIANT %s value to %s. A VARIANT cast does not change the "
                                + "type of the stored value, so cast it to its own type first and "
                                + "then convert with a regular cast.",
                        variant.getType(), targetType));
    }

    private static TableRuntimeException overflow(Object value, String targetType) {
        return new TableRuntimeException(
                String.format("Casting the VARIANT value %s to %s overflowed.", value, targetType));
    }

    private static TableRuntimeException lossyCast(Object value, String targetType) {
        return new TableRuntimeException(
                String.format(
                        "Casting the VARIANT value %s to %s would lose precision. Cast it to a type "
                                + "that holds the value exactly first, then narrow if needed.",
                        value, targetType));
    }
}
