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
 * <p>Numeric targets are range-checked and raise {@link TableRuntimeException} on overflow instead
 * of wrapping. Casting to a string extracts the scalar value (a string stays unquoted), while
 * objects and arrays use their JSON representation.
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
     * Casts a {@code VARIANT} to its SQL string value for a {@code CHAR(targetLength)} or {@code
     * VARCHAR(targetLength)} target. Scalars use their raw value (a string stays unquoted, unlike
     * {@code JSON_STRING}); objects and arrays use their JSON representation.
     *
     * <p>The target length is enforced strictly, with no padding or truncation: a {@code VARCHAR}
     * value must not be longer than {@code targetLength}, and a {@code CHAR} value must match it
     * exactly. A value that does not fit raises {@link TableRuntimeException}.
     */
    public static String toStringValue(Variant variant, int targetLength, boolean charTarget) {
        final String value;
        switch (variant.getType()) {
            case OBJECT:
            case ARRAY:
            case BYTES:
                value = variant.toJson();
                break;
            default:
                value = String.valueOf(variant.get());
        }
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
            throw new TableRuntimeException(
                    String.format(
                            "Casting the VARIANT value %s to %s overflowed.", value, targetType));
        }
        return integral.longValue();
    }

    private static BigDecimal toBigDecimal(Number value) {
        return value instanceof BigDecimal ? (BigDecimal) value : convertToBigDecimal(value);
    }

    private static BigDecimal convertToBigDecimal(Number number) {
        if (number == null) {
            return null;
        }
        if (number instanceof BigDecimal) {
            return (BigDecimal) number;
        }
        if (number instanceof BigInteger) {
            return new BigDecimal((BigInteger) number);
        }
        if (number instanceof Float || number instanceof Double) {
            return BigDecimal.valueOf(number.doubleValue());
        }
        return BigDecimal.valueOf(number.longValue());
    }
}
