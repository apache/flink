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

import org.apache.flink.table.api.JsonValueOnEmptyOrError;
import org.apache.flink.table.api.TableRuntimeException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericArrayData;
import org.apache.flink.table.types.logical.LogicalTypeRoot;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.stream.Stream;

import static org.apache.flink.table.api.JsonQueryOnEmptyOrError.EMPTY_ARRAY;
import static org.apache.flink.table.api.JsonQueryOnEmptyOrError.ERROR;
import static org.apache.flink.table.api.JsonQueryOnEmptyOrError.NULL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BIGINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.BOOLEAN;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DECIMAL;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.DOUBLE;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.FLOAT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.INTEGER;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.SMALLINT;
import static org.apache.flink.table.types.logical.LogicalTypeRoot.TINYINT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlJsonUtilsConversionTest {

    private static Object scalar(Object raw, LogicalTypeRoot type) {
        return SqlJsonUtils.convertJsonScalar(raw, type, 0, 0, JsonValueOnEmptyOrError.NULL, null);
    }

    private static Object scalar(
            Object raw,
            LogicalTypeRoot type,
            JsonValueOnEmptyOrError onError,
            Object defaultValue) {
        return SqlJsonUtils.convertJsonScalar(raw, type, 0, 0, onError, defaultValue);
    }

    private static Object scalarDecimal(Object raw, int precision, int scale) {
        return SqlJsonUtils.convertJsonScalar(
                raw, DECIMAL, precision, scale, JsonValueOnEmptyOrError.NULL, null);
    }

    private static GenericArrayData array(Object[] raw, LogicalTypeRoot elementType) {
        return SqlJsonUtils.convertJsonArray(raw, elementType, 0, 0, NULL);
    }

    private static GenericArrayData arrayDecimal(
            Object[] raw, int precision, int scale,
            org.apache.flink.table.api.JsonQueryOnEmptyOrError onError) {
        return SqlJsonUtils.convertJsonArray(raw, DECIMAL, precision, scale, onError);
    }

    private static final BigInteger HUGE = new BigInteger("18446744073709551616");

    static Stream<Arguments> validConversions() {
        return Stream.of(
                Arguments.of(42, TINYINT, (byte) 42),
                Arguments.of(127L, TINYINT, Byte.MAX_VALUE),
                Arguments.of(-128L, TINYINT, Byte.MIN_VALUE),
                Arguments.of(1000, SMALLINT, (short) 1000),
                Arguments.of((long) Short.MAX_VALUE, SMALLINT, Short.MAX_VALUE),
                Arguments.of(42L, INTEGER, 42),
                Arguments.of((long) Integer.MAX_VALUE, INTEGER, Integer.MAX_VALUE),
                Arguments.of(42, BIGINT, 42L),
                Arguments.of(9_999_999_999L, BIGINT, 9_999_999_999L),
                Arguments.of(
                        new BigDecimal("9223372036854775807.5"),
                        BIGINT,
                        Long.MAX_VALUE),
                Arguments.of(13.37, FLOAT, 13.37f),
                Arguments.of(13.37, DOUBLE, 13.37));
    }

    static Stream<Arguments> overflowCases() {
        return Stream.of(
                Arguments.of(128, TINYINT),
                Arguments.of(-129, TINYINT),
                Arguments.of(200L, TINYINT),
                Arguments.of(-200L, TINYINT),
                Arguments.of(HUGE, TINYINT),
                Arguments.of(HUGE.negate(), TINYINT),
                Arguments.of(new BigDecimal("18446744073709551616"), TINYINT),
                Arguments.of((long) Short.MAX_VALUE + 1, SMALLINT),
                Arguments.of((long) Short.MIN_VALUE - 1, SMALLINT),
                Arguments.of(40_000L, SMALLINT),
                Arguments.of(-40_000L, SMALLINT),
                Arguments.of(HUGE, SMALLINT),
                Arguments.of(HUGE.negate(), SMALLINT),
                Arguments.of(new BigDecimal("18446744073709551616"), SMALLINT),
                Arguments.of(9_999_999_999L, INTEGER),
                Arguments.of(-9_999_999_999L, INTEGER),
                Arguments.of(HUGE, INTEGER),
                Arguments.of(HUGE.negate(), INTEGER),
                Arguments.of(new BigDecimal("18446744073709551616"), INTEGER),
                Arguments.of(HUGE, BIGINT),
                Arguments.of(HUGE.negate(), BIGINT),
                Arguments.of(new BigDecimal("18446744073709551616"), BIGINT));
    }

    @Nested
    class RangeCheckedConversions {

        @ParameterizedTest(name = "{0} -> {1}")
        @MethodSource(
                "org.apache.flink.table.runtime.functions.SqlJsonUtilsConversionTest#validConversions")
        void convertsWithinRange(Number input, LogicalTypeRoot type, Object expected) {
            assertThat(scalar(input, type)).isEqualTo(expected);
        }

        @ParameterizedTest(name = "{0} -> {1}")
        @MethodSource(
                "org.apache.flink.table.runtime.functions.SqlJsonUtilsConversionTest#overflowCases")
        void overflowReturnsNull(Number input, LogicalTypeRoot type) {
            assertThat(scalar(input, type)).isNull();
        }

        @Test
        void intTruncatesFraction() {
            assertThat(scalar(13.37, INTEGER)).isEqualTo(13);
            assertThat(scalar(-13.99, INTEGER)).isEqualTo(-13);
        }

        @Test
        void bigIntegerOverflow() {
            BigInteger tooLarge = BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE);
            assertThat(scalar(tooLarge, BIGINT)).isNull();
        }

        @Test
        void negativeBigIntegerOverflow() {
            BigInteger tooSmall = BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE);
            assertThat(scalar(tooSmall, BIGINT)).isNull();
        }

        @Test
        void bigDecimalOverflow() {
            BigDecimal tooLarge = new BigDecimal(Long.MAX_VALUE).add(BigDecimal.ONE);
            assertThat(scalar(tooLarge, BIGINT)).isNull();
        }

        @Test
        void bigDecimalToBigintTruncatesFraction() {
            assertThat(scalar(new BigDecimal("9999999999.5"), BIGINT)).isEqualTo(9_999_999_999L);
            assertThat(scalar(new BigDecimal("-42.9"), BIGINT)).isEqualTo(-42L);
        }

        @Test
        void floatOverflowReturnsNull() {
            assertThat(scalar(1e40, FLOAT)).isNull();
            assertThat(scalar(-1e40, FLOAT)).isNull();
        }

        @Test
        void doubleOverflowReturnsNull() {
            assertThat(scalar(new BigDecimal("1E309"), DOUBLE)).isNull();
            assertThat(scalar(new BigDecimal("-1E309"), DOUBLE)).isNull();
        }
    }

    @Nested
    class DecimalConversion {

        @Test
        void validPrecision() {
            Object result = scalarDecimal(13.37, 10, 2);
            assertThat(result).isInstanceOf(DecimalData.class);
            assertThat(((DecimalData) result).toBigDecimal())
                    .isEqualByComparingTo(new BigDecimal("13.37"));
        }

        @Test
        void precisionOverflowReturnsNull() {
            assertThat(scalarDecimal(123456789.99, 5, 2)).isNull();
        }

        @Test
        void precisionOverflowWithStringDefault() {
            Object result =
                    SqlJsonUtils.convertJsonScalar(
                            123456789.99, DECIMAL, 5, 2, JsonValueOnEmptyOrError.DEFAULT, "0.00");
            assertThat(result).isInstanceOf(DecimalData.class);
            assertThat(((DecimalData) result).toBigDecimal())
                    .isEqualByComparingTo(new BigDecimal("0.00"));
        }

        @Test
        void scientificNotation() {
            DecimalData result = SqlJsonUtils.toCheckedDecimal("1.5E1", 10, 2);
            assertThat(result.toBigDecimal()).isEqualByComparingTo(new BigDecimal("15.00"));
        }
    }

    @Nested
    class ScalarErrorHandling {

        @Test
        void nullInputPassesThrough() {
            assertThat(scalar(null, INTEGER)).isNull();
        }

        @Test
        void numericStringParsedAsInteger() {
            assertThat(scalar("42", INTEGER)).isEqualTo(42);
        }

        @Test
        void numericStringParsedAsFloat() {
            assertThat(scalar("13.37", FLOAT)).isEqualTo(13.37f);
        }

        @Test
        void numericStringParsedAsBigint() {
            assertThat(scalar("9999999999", BIGINT)).isEqualTo(9_999_999_999L);
        }

        @Test
        void numericStringOverflowReturnsNull() {
            assertThat(scalar("9999999999", INTEGER)).isNull();
        }

        @Test
        void nonNumericStringReturnsDefault() {
            assertThat(scalar("notANumber", INTEGER, JsonValueOnEmptyOrError.DEFAULT, 42))
                    .isEqualTo(42);
        }

        @Test
        void emptyStringReturnsNull() {
            assertThat(scalar("", INTEGER)).isNull();
        }

        @Test
        void nanStringReturnsNull() {
            assertThat(scalar("NaN", FLOAT)).isNull();
            assertThat(scalar("NaN", DOUBLE)).isNull();
            assertThat(scalar("NaN", INTEGER)).isNull();
        }

        @Test
        void infinityStringReturnsNull() {
            assertThat(scalar("Infinity", FLOAT)).isNull();
            assertThat(scalar("-Infinity", DOUBLE)).isNull();
        }

        @Test
        void booleanStringCoerced() {
            assertThat(scalar("true", BOOLEAN)).isEqualTo(true);
            assertThat(scalar("false", BOOLEAN)).isEqualTo(false);
            assertThat(scalar("TRUE", BOOLEAN)).isEqualTo(true);
            assertThat(scalar("t", BOOLEAN)).isEqualTo(true);
            assertThat(scalar("f", BOOLEAN)).isEqualTo(false);
            assertThat(scalar("yes", BOOLEAN)).isEqualTo(true);
            assertThat(scalar("no", BOOLEAN)).isEqualTo(false);
            assertThat(scalar("1", BOOLEAN)).isEqualTo(true);
            assertThat(scalar("0", BOOLEAN)).isEqualTo(false);
        }

        @Test
        void invalidBooleanStringReturnsNull() {
            assertThat(scalar("maybe", BOOLEAN)).isNull();
            assertThat(scalar("2", BOOLEAN)).isNull();
        }

        @Test
        void typeMismatchThrows() {
            assertThatThrownBy(
                            () ->
                                    scalar(
                                            9_999_999_999L,
                                            INTEGER,
                                            JsonValueOnEmptyOrError.ERROR,
                                            null))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("Cannot cast");
        }

        @Test
        void booleanToIntReturnsNull() {
            assertThat(scalar(Boolean.TRUE, INTEGER)).isNull();
        }

        @Test
        void intZeroOneToBooleanConverts() {
            assertThat(scalar(0, BOOLEAN)).isEqualTo(false);
            assertThat(scalar(1, BOOLEAN)).isEqualTo(true);
            assertThat(scalar(0L, BOOLEAN)).isEqualTo(false);
            assertThat(scalar(1L, BOOLEAN)).isEqualTo(true);
        }

        @Test
        void otherIntToBooleanReturnsNull() {
            assertThat(scalar(42, BOOLEAN)).isNull();
            assertThat(scalar(-1, BOOLEAN)).isNull();
            assertThat(scalar(2, BOOLEAN)).isNull();
        }
    }

    @Nested
    class ArrayConversion {

        @Test
        void nullInputPassesThrough() {
            assertThat(SqlJsonUtils.convertJsonArray(null, INTEGER, 0, 0, NULL)).isNull();
        }

        @Test
        void convertsIntegerArray() {
            assertThat(array(new Object[] {1, 2, 3}, INTEGER).toObjectArray())
                    .containsExactly(1, 2, 3);
        }

        @Test
        void preservesNullElements() {
            assertThat(array(new Object[] {1, null, 3}, INTEGER).toObjectArray())
                    .containsExactly(1, null, 3);
        }

        @Test
        void convertsBooleanArray() {
            assertThat(array(new Object[] {true, false}, BOOLEAN).toObjectArray())
                    .containsExactly(true, false);
        }

        @Test
        void integerZeroOneToBooleanArray() {
            assertThat(array(new Object[] {0, 1}, BOOLEAN).toObjectArray())
                    .containsExactly(false, true);
        }

        @Test
        void numericStringArrayParsed() {
            assertThat(array(new Object[] {"1", "2", "3"}, INTEGER).toObjectArray())
                    .containsExactly(1, 2, 3);
        }

        @Test
        void emptyArrayStaysEmpty() {
            assertThat(array(new Object[0], INTEGER).size()).isZero();
        }

        @Test
        void typeMismatchReturnsNull() {
            assertThat(SqlJsonUtils.convertJsonArray(new Object[] {"a", "b"}, INTEGER, 0, 0, NULL))
                    .isNull();
        }

        @Test
        void typeMismatchReturnsEmptyArray() {
            GenericArrayData result =
                    SqlJsonUtils.convertJsonArray(
                            new Object[] {"a", "b"}, INTEGER, 0, 0, EMPTY_ARRAY);
            assertThat(result).isNotNull();
            assertThat(result.size()).isZero();
        }

        @Test
        void typeMismatchThrows() {
            assertThatThrownBy(
                            () ->
                                    SqlJsonUtils.convertJsonArray(
                                            new Object[] {"a", "b"}, INTEGER, 0, 0, ERROR))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("Array element type mismatch");
        }

        @Test
        void partialMismatchFailsAtomically() {
            assertThat(
                            SqlJsonUtils.convertJsonArray(
                                    new Object[] {1, "notANumber", 3}, INTEGER, 0, 0, NULL))
                    .isNull();
        }

        @Test
        void decimalOverflowTriggersOnError() {
            assertThat(SqlJsonUtils.convertJsonArray(new Object[] {999999}, DECIMAL, 2, 1, NULL))
                    .isNull();
        }

        @Test
        void decimalOverflowTriggersEmptyArray() {
            GenericArrayData result = arrayDecimal(new Object[] {999999}, 2, 1, EMPTY_ARRAY);
            assertThat(result).isNotNull();
            assertThat(result.size()).isZero();
        }

        @Test
        void decimalOverflowTriggersError() {
            assertThatThrownBy(() -> arrayDecimal(new Object[] {999999}, 2, 1, ERROR))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("Array element type mismatch");
        }

        @Test
        void emptyArrayWithDecimalType() {
            GenericArrayData result = arrayDecimal(new Object[0], 10, 2, NULL);
            assertThat(result).isNotNull();
            assertThat(result.size()).isZero();
        }
    }
}
