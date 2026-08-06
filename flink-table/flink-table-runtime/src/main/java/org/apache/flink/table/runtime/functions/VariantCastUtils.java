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

    /** A variant stores a timestamp with microsecond precision. */
    private static final int TIMESTAMP_PRECISION = 6;

    private VariantCastUtils() {}

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
     * Reads a timestamp variant as the target {@code TIMESTAMP}. A variant keeps microseconds, so
     * fractional seconds beyond the target precision are truncated, the same as a regular {@code
     * TIMESTAMP} to {@code TIMESTAMP(p)} cast.
     */
    public static TimestampData toTimestamp(Variant variant, int precision) {
        if (variant.getType() != Variant.Type.TIMESTAMP) {
            throw unsupportedKind(variant, String.format("TIMESTAMP(%d)", precision));
        }
        return DateTimeUtils.truncate(
                TimestampData.fromLocalDateTime(variant.getDateTime()), precision);
    }

    /** Reads a timestamp with local time zone variant. See {@link #toTimestamp(Variant, int)}. */
    public static TimestampData toTimestampLtz(Variant variant, int precision) {
        if (variant.getType() != Variant.Type.TIMESTAMP_LTZ) {
            throw unsupportedKind(variant, String.format("TIMESTAMP_LTZ(%d)", precision));
        }
        return DateTimeUtils.truncate(TimestampData.fromInstant(variant.getInstant()), precision);
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
     * Casts a scalar {@code VARIANT} to a character string, rendering the value the way a regular
     * SQL cast of the stored kind would. A value longer than {@code targetLength} is trimmed, and a
     * {@code CHAR} target pads a shorter value to its fixed width. Both are measured in code
     * points, so a character outside the BMP fills a single position even though it occupies two
     * UTF-16 units.
     *
     * <p>A stored binary value has to be well-formed UTF-8, since a character string cannot carry
     * bytes that no character maps to. Invalid input is rejected rather than decoded into {@code
     * U+FFFD}, which would silently substitute a character the value never held.
     *
     * @param sessionZone the session time zone, applied to a {@code TIMESTAMP_LTZ} value
     */
    public static BinaryStringData toStringValue(
            Variant variant, TimeZone sessionZone, int targetLength, boolean charTarget) {
        final String value = getVariantTypeAsString(variant, sessionZone, targetLength, charTarget);
        // numChars and substring both count code points, so a character outside the BMP fills one
        // position rather than the two UTF-16 units it occupies.
        final BinaryStringData result = BinaryStringData.fromString(value);
        final int length = result.numChars();
        if (length > targetLength) {
            return result.substring(0, targetLength);
        }
        if (charTarget && length < targetLength) {
            return BinaryStringDataUtil.concat(
                    result, BinaryStringData.blankString(targetLength - length));
        }
        return result;
    }

    private static String getVariantTypeAsString(
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
                    throw new TableRuntimeException(
                            String.format(
                                    "Cannot cast the VARIANT binary value to %s because it is not "
                                            + "valid UTF-8; the first invalid byte is at index %d "
                                            + "of %d. Cast to BYTES to inspect the raw value, or "
                                            + "wrap that in MAKE_VALID_UTF8 to replace every "
                                            + "invalid byte with the U+FFFD replacement character.",
                                    characterTarget(targetLength, charTarget),
                                    invalidAt,
                                    utf8.length));
                }
                value = new String(utf8, StandardCharsets.UTF_8);
                break;
            case DATE:
                value = DateTimeUtils.formatDate((int) variant.getDate().toEpochDay());
                break;
            case TIMESTAMP:
                // A wall-clock value needs no zone shift, which is what UTC_ZONE achieves here. A
                // variant keeps microseconds, so the precision is always 6.
                value =
                        DateTimeUtils.formatTimestamp(
                                TimestampData.fromLocalDateTime(variant.getDateTime()),
                                DateTimeUtils.UTC_ZONE,
                                TIMESTAMP_PRECISION);
                break;
            case TIMESTAMP_LTZ:
                value =
                        DateTimeUtils.formatTimestamp(
                                TimestampData.fromInstant(variant.getInstant()),
                                sessionZone,
                                TIMESTAMP_PRECISION);
                break;
            case NULL:
                // Only reachable for a NOT NULL target. A nullable target maps a null-valued
                // variant to SQL NULL before this method is called.
                throw new TableRuntimeException(
                        String.format(
                                "Cannot cast a VARIANT null value to %s because the target does not "
                                        + "accept NULL.",
                                characterTarget(targetLength, charTarget)));
            default:
                // An object or array has no scalar rendering.
                throw new TableRuntimeException(
                        String.format(
                                "Cannot cast a VARIANT %s value to a character string. Use the "
                                        + "JSON_STRING function to obtain its JSON representation.",
                                variant.getType()));
        }
        return value;
    }

    private static String characterTarget(int targetLength, boolean charTarget) {
        return String.format("%s(%d)", charTarget ? "CHAR" : "VARCHAR", targetLength);
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
