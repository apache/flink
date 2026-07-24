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
import org.apache.flink.types.variant.Variant;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Runtime helpers for casting a {@code VARIANT} value to a SQL type.
 *
 * <p>Numeric targets are strict: a value that cannot be represented exactly, whether by integer or
 * decimal overflow or by floating-point precision loss, raises {@link TableRuntimeException}
 * instead of wrapping or rounding. Casting to a character string extracts the scalar value (a
 * string stays unquoted); a variant holding an object, array, or binary value is not castable and
 * raises {@link TableRuntimeException} (use {@code JSON_STRING} for its JSON representation).
 */
@Internal
public final class VariantCastUtils {

    private VariantCastUtils() {}

    public static byte toByteExact(Number value) {
        return (byte) toIntegralExact(value, Byte.MIN_VALUE, Byte.MAX_VALUE, "TINYINT");
    }

    public static short toShortExact(Number value) {
        return (short) toIntegralExact(value, Short.MIN_VALUE, Short.MAX_VALUE, "SMALLINT");
    }

    public static int toIntExact(Number value) {
        return (int) toIntegralExact(value, Integer.MIN_VALUE, Integer.MAX_VALUE, "INTEGER");
    }

    public static long toLongExact(Number value) {
        return toIntegralExact(value, Long.MIN_VALUE, Long.MAX_VALUE, "BIGINT");
    }

    public static float toFloatExact(Number value) {
        final float result = value.floatValue();
        if (!Float.isFinite(result)) {
            throw overflow(value, "FLOAT");
        }
        if (exactValue(value).compareTo(new BigDecimal(result)) != 0) {
            throw lossyCast(value, "FLOAT");
        }
        return result;
    }

    public static double toDoubleExact(Number value) {
        final double result = value.doubleValue();
        if (!Double.isFinite(result)) {
            throw overflow(value, "DOUBLE");
        }
        if (exactValue(value).compareTo(new BigDecimal(result)) != 0) {
            throw lossyCast(value, "DOUBLE");
        }
        return result;
    }

    public static DecimalData toDecimalExact(Number value, int precision, int scale) {
        final DecimalData decimal =
                DecimalData.fromBigDecimal(toBigDecimal(value), precision, scale);
        if (decimal == null) {
            throw new TableRuntimeException(
                    String.format(
                            "Casting the VARIANT value %s to DECIMAL(%d, %d) overflowed.",
                            value, precision, scale));
        }
        return decimal;
    }

    /**
     * Casts a scalar {@code VARIANT} to its raw string value, enforcing {@code targetLength}
     * strictly with no padding or truncation ({@code CHAR} requires an exact length, {@code
     * VARCHAR} an upper bound).
     */
    public static String toStringValue(Variant variant, int targetLength, boolean charTarget) {
        switch (variant.getType()) {
            case OBJECT:
            case ARRAY:
            case BYTES:
                throw new TableRuntimeException(
                        String.format(
                                "Cannot cast a VARIANT %s value to a character string. Use the "
                                        + "JSON_STRING function to obtain its JSON representation.",
                                variant.getType()));
            default:
                // Scalars are cast to their raw string value.
        }
        final String value = String.valueOf(variant.get());
        final int length = value.length();
        final boolean fits = charTarget ? length == targetLength : length <= targetLength;
        if (!fits) {
            throw new TableRuntimeException(
                    String.format(
                            "The VARIANT string value of length %d does not fit %s(%d); VARIANT "
                                    + "string casts do not pad or truncate.",
                            length, charTarget ? "CHAR" : "VARCHAR", targetLength));
        }
        return value;
    }

    private static long toIntegralExact(Number value, long min, long max, String targetType) {
        // toBigInteger truncates any fractional part toward zero, matching regular numeric casts.
        final BigInteger integral = toBigDecimal(value).toBigInteger();
        if (integral.compareTo(BigInteger.valueOf(min)) < 0
                || integral.compareTo(BigInteger.valueOf(max)) > 0) {
            throw overflow(value, targetType);
        }
        return integral.longValue();
    }

    private static BigDecimal toBigDecimal(Number value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        }
        if (value instanceof Float || value instanceof Double) {
            return BigDecimal.valueOf(value.doubleValue());
        }
        return BigDecimal.valueOf(value.longValue());
    }

    // The exact mathematical value of a number, used to detect precision loss in floating-point
    // casts. Unlike toBigDecimal, floats and doubles use their exact binary value rather than the
    // shortest round-trippable decimal, so 0.1d becomes 0.1000000000000000055..., not 0.1.
    private static BigDecimal exactValue(Number value) {
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof BigInteger) {
            return new BigDecimal((BigInteger) value);
        }
        if (value instanceof Float || value instanceof Double) {
            return new BigDecimal(value.doubleValue());
        }
        return BigDecimal.valueOf(value.longValue());
    }

    private static TableRuntimeException overflow(Number value, String targetType) {
        return new TableRuntimeException(
                String.format("Casting the VARIANT value %s to %s overflowed.", value, targetType));
    }

    private static TableRuntimeException lossyCast(Number value, String targetType) {
        return new TableRuntimeException(
                String.format(
                        "Casting the VARIANT value %s to %s would lose precision. Cast it to a type "
                                + "that holds the value exactly first, then narrow if needed.",
                        value, targetType));
    }
}
