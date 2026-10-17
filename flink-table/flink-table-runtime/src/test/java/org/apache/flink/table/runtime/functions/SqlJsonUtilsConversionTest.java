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

    /** Prefix for {@link MethodSource} references so {@code @Nested} tests can see the sources. */
    private static final String SRC =
            "org.apache.flink.table.runtime.functions.SqlJsonUtilsConversionTest#";

    private static final BigInteger HUGE = new BigInteger("18446744073709551616");

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
        return SqlJsonUtils.convertJsonArray(raw, elementType, 0, 0, true, NULL);
    }

    private static GenericArrayData arrayDecimal(
            Object[] raw,
            int precision,
            int scale,
            org.apache.flink.table.api.JsonQueryOnEmptyOrError onError) {
        return SqlJsonUtils.convertJsonArray(raw, DECIMAL, precision, scale, true, onError);
    }

    // ------------------------------------------------------------------------
    //  Numeric range conversion
    // ------------------------------------------------------------------------

    static Stream<Arguments> numericValueConversions() {
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
                Arguments.of(new BigDecimal("9223372036854775807.5"), BIGINT, Long.MAX_VALUE),
                Arguments.of(13.37, FLOAT, 13.37f),
                Arguments.of(13.37, DOUBLE, 13.37),
                // fraction truncation
                Arguments.of(13.37, INTEGER, 13),
                Arguments.of(-13.99, INTEGER, -13),
                Arguments.of(new BigDecimal("9999999999.5"), BIGINT, 9_999_999_999L),
                Arguments.of(new BigDecimal("-42.9"), BIGINT, -42L),
                // silent precision loss when the target type cannot hold the value exactly
                Arguments.of(16777217, FLOAT, 16777216.0f),
                Arguments.of(9007199254740993L, DOUBLE, 9.007199254740992E15));
    }

    static Stream<Arguments> numericOverflowCases() {
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
                Arguments.of(new BigDecimal("18446744073709551616"), BIGINT),
                Arguments.of(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), BIGINT),
                Arguments.of(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE), BIGINT),
                Arguments.of(new BigDecimal(Long.MAX_VALUE).add(BigDecimal.ONE), BIGINT),
                Arguments.of(1e40, FLOAT),
                Arguments.of(-1e40, FLOAT),
                Arguments.of(new BigDecimal("1E309"), DOUBLE),
                Arguments.of(new BigDecimal("-1E309"), DOUBLE));
    }

    @Nested
    class NumericRangeConversion {

        @ParameterizedTest(name = "{0} -> {1}")
        @MethodSource(SRC + "numericValueConversions")
        void convertsToExpectedValue(Number input, LogicalTypeRoot type, Object expected) {
            assertThat(scalar(input, type)).isEqualTo(expected);
        }

        @ParameterizedTest(name = "{0} -> {1}")
        @MethodSource(SRC + "numericOverflowCases")
        void overflowReturnsNull(Number input, LogicalTypeRoot type) {
            assertThat(scalar(input, type)).isNull();
        }
    }

    // ------------------------------------------------------------------------
    //  Decimal conversion
    // ------------------------------------------------------------------------

    static Stream<Arguments> decimalConversions() {
        return Stream.of(
                Arguments.of(13.37, 10, 2, "13.37"),
                Arguments.of("99999999999999999999999999999999999999", 38, 0,
                        "99999999999999999999999999999999999999"),
                Arguments.of("123.456", 5, 2, "123.46"));
    }

    static Stream<Arguments> decimalOverflowCases() {
        return Stream.of(
                Arguments.of(123456789.99, 5, 2),
                Arguments.of("999999999999999999999999999999999999999", 38, 0),
                // 999.999 rounds to 1000.00 (precision 6) which overflows DECIMAL(5,2)
                Arguments.of("999.999", 5, 2));
    }

    @Nested
    class DecimalConversion {

        @ParameterizedTest(name = "{0} -> DECIMAL({1},{2})")
        @MethodSource(SRC + "decimalConversions")
        void convertsToDecimal(Object raw, int precision, int scale, String expected) {
            Object result = scalarDecimal(raw, precision, scale);
            assertThat(result).isInstanceOf(DecimalData.class);
            assertThat(((DecimalData) result).toBigDecimal())
                    .isEqualByComparingTo(new BigDecimal(expected));
        }

        @ParameterizedTest(name = "{0} -> DECIMAL({1},{2})")
        @MethodSource(SRC + "decimalOverflowCases")
        void overflowReturnsNull(Object raw, int precision, int scale) {
            assertThat(scalarDecimal(raw, precision, scale)).isNull();
        }

        @Test
        void scientificNotation() {
            DecimalData result = SqlJsonUtils.toCheckedDecimal("1.5E1", 10, 2);
            assertThat(result.toBigDecimal()).isEqualByComparingTo(new BigDecimal("15.00"));
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
    }

    // ------------------------------------------------------------------------
    //  Boolean conversion
    // ------------------------------------------------------------------------

    static Stream<Arguments> booleanConversions() {
        return Stream.of(
                Arguments.of(0, false),
                Arguments.of(1, true),
                Arguments.of(0L, false),
                Arguments.of(1L, true),
                Arguments.of(0.0, false),
                Arguments.of(1.0, true),
                Arguments.of("true", true),
                Arguments.of("false", false),
                Arguments.of("TRUE", true),
                Arguments.of("t", true),
                Arguments.of("f", false),
                Arguments.of("yes", true),
                Arguments.of("no", false),
                Arguments.of("1", true),
                Arguments.of("0", false),
                // parseStringAsBoolean trims before matching
                Arguments.of(" true ", true),
                Arguments.of(" FALSE ", false));
    }

    static Stream<Arguments> nonBooleanInputs() {
        return Stream.of(
                Arguments.of(42),
                Arguments.of(-1),
                Arguments.of(2),
                Arguments.of(1.9),
                Arguments.of(0.5),
                Arguments.of(-0.1),
                // exact BigDecimal comparison rejects values that doubleValue() would round to 0/1
                Arguments.of(new BigDecimal("1.00000000000000000001")),
                Arguments.of(new BigDecimal("0.99999999999999999999")),
                Arguments.of(new BigDecimal("1E-400")),
                Arguments.of("maybe"),
                Arguments.of("2"));
    }

    @Nested
    class BooleanConversion {

        @ParameterizedTest(name = "{0} -> {1}")
        @MethodSource(SRC + "booleanConversions")
        void convertsToBoolean(Object raw, Boolean expected) {
            assertThat(scalar(raw, BOOLEAN)).isEqualTo(expected);
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource(SRC + "nonBooleanInputs")
        void returnsNullForNonBoolean(Object raw) {
            assertThat(scalar(raw, BOOLEAN)).isNull();
        }
    }

    // ------------------------------------------------------------------------
    //  String input parsing
    // ------------------------------------------------------------------------

    static Stream<Arguments> stringConversions() {
        return Stream.of(
                Arguments.of("42", INTEGER, 42),
                Arguments.of("13.37", FLOAT, 13.37f),
                Arguments.of("9999999999", BIGINT, 9_999_999_999L),
                // CAST trims leading/trailing whitespace (matches PostgreSQL)
                Arguments.of(" 42 ", INTEGER, 42),
                Arguments.of(" 13.37 ", DOUBLE, 13.37));
    }

    static Stream<Arguments> invalidStringInputs() {
        return Stream.of(
                Arguments.of("9999999999", INTEGER),
                Arguments.of("", INTEGER),
                Arguments.of("NaN", FLOAT),
                Arguments.of("NaN", DOUBLE),
                Arguments.of("NaN", INTEGER),
                Arguments.of("Infinity", FLOAT),
                Arguments.of("-Infinity", DOUBLE));
    }

    @Nested
    class StringInputParsing {

        @ParameterizedTest(name = "{0} -> {1}")
        @MethodSource(SRC + "stringConversions")
        void parsesToExpectedValue(String raw, LogicalTypeRoot type, Object expected) {
            assertThat(scalar(raw, type)).isEqualTo(expected);
        }

        @ParameterizedTest(name = "{0} -> {1}")
        @MethodSource(SRC + "invalidStringInputs")
        void returnsNull(String raw, LogicalTypeRoot type) {
            assertThat(scalar(raw, type)).isNull();
        }
    }

    // ------------------------------------------------------------------------
    //  On-error behavior
    // ------------------------------------------------------------------------

    static Stream<Arguments> onErrorNullInputs() {
        return Stream.of(
                // null input passes straight through
                Arguments.of((Object) null, INTEGER),
                // a boolean cannot be converted to a number
                Arguments.of(Boolean.TRUE, INTEGER));
    }

    @Nested
    class OnErrorBehavior {

        @ParameterizedTest(name = "{0} -> {1}")
        @MethodSource(SRC + "onErrorNullInputs")
        void returnsNull(Object raw, LogicalTypeRoot type) {
            assertThat(scalar(raw, type)).isNull();
        }

        @Test
        void returnsDefaultForUnparsableString() {
            assertThat(scalar("notANumber", INTEGER, JsonValueOnEmptyOrError.DEFAULT, 42))
                    .isEqualTo(42);
        }

        @Test
        void throwsOnErrorMode() {
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
    }

    // ------------------------------------------------------------------------
    //  Array conversion
    // ------------------------------------------------------------------------

    static Stream<Arguments> arrayConversions() {
        return Stream.of(
                Arguments.of((Object) new Object[] {1, 2, 3}, INTEGER, (Object) new Object[] {1, 2, 3}),
                Arguments.of(
                        (Object) new Object[] {1, null, 3},
                        INTEGER,
                        (Object) new Object[] {1, null, 3}),
                Arguments.of(
                        (Object) new Object[] {true, false},
                        BOOLEAN,
                        (Object) new Object[] {true, false}),
                Arguments.of(
                        (Object) new Object[] {0, 1},
                        BOOLEAN,
                        (Object) new Object[] {false, true}),
                Arguments.of(
                        (Object) new Object[] {"1", "2", "3"},
                        INTEGER,
                        (Object) new Object[] {1, 2, 3}),
                Arguments.of(
                        (Object) new Object[] {"yes", "no"},
                        BOOLEAN,
                        (Object) new Object[] {true, false}),
                Arguments.of(
                        (Object) new Object[] {1, 9999999999L},
                        BIGINT,
                        (Object) new Object[] {1L, 9999999999L}));
    }

    static Stream<Arguments> arrayNullResultCases() {
        return Stream.of(
                Arguments.of((Object) new Object[] {"a", "b"}, INTEGER, 0, 0, true),
                Arguments.of((Object) new Object[] {1, "notANumber", 3}, INTEGER, 0, 0, true),
                Arguments.of((Object) new Object[] {"Y", "n"}, BOOLEAN, 0, 0, true),
                // element 2 is neither 0 nor 1, so conversion fails atomically
                Arguments.of((Object) new Object[] {1, 2, 3}, BOOLEAN, 0, 0, true),
                Arguments.of((Object) new Object[] {999999}, DECIMAL, 2, 1, true),
                // 999.999 rounds to 1000.00 which overflows DECIMAL(5,2); whole array fails
                Arguments.of((Object) new Object[] {1.5, 999.999}, DECIMAL, 5, 2, true),
                // a null element in a NOT NULL array fails
                Arguments.of((Object) new Object[] {1, null, 3}, INTEGER, 0, 0, false));
    }

    static Stream<Arguments> arrayEmptyResultCases() {
        return Stream.of(
                Arguments.of((Object) new Object[] {"a", "b"}, INTEGER, 0, 0),
                Arguments.of((Object) new Object[] {999999}, DECIMAL, 2, 1));
    }

    static Stream<Arguments> arrayThrowCases() {
        return Stream.of(
                Arguments.of((Object) new Object[] {"a", "b"}, INTEGER, 0, 0, true),
                Arguments.of((Object) new Object[] {999999}, DECIMAL, 2, 1, true),
                Arguments.of((Object) new Object[] {1, null, 3}, INTEGER, 0, 0, false));
    }

    static Stream<Arguments> emptyArrayInputs() {
        return Stream.of(Arguments.of(INTEGER, 0, 0), Arguments.of(DECIMAL, 10, 2));
    }

    @Nested
    class ArrayConversion {

        @ParameterizedTest(name = "{1}")
        @MethodSource(SRC + "arrayConversions")
        void convertsArray(Object[] raw, LogicalTypeRoot elementType, Object[] expected) {
            assertThat(array(raw, elementType).toObjectArray()).containsExactly(expected);
        }

        @Test
        void convertsDecimalArrayWithRounding() {
            GenericArrayData result = arrayDecimal(new Object[] {1.5, 123.456}, 5, 2, NULL);
            assertThat(result).isNotNull();
            Object[] elements = result.toObjectArray();
            assertThat(((DecimalData) elements[0]).toBigDecimal())
                    .isEqualByComparingTo(new BigDecimal("1.50"));
            assertThat(((DecimalData) elements[1]).toBigDecimal())
                    .isEqualByComparingTo(new BigDecimal("123.46"));
        }

        @ParameterizedTest(name = "{1}")
        @MethodSource(SRC + "arrayNullResultCases")
        void returnsNullOnErrorMode(
                Object[] raw, LogicalTypeRoot elementType, int precision, int scale,
                boolean nullable) {
            assertThat(
                            SqlJsonUtils.convertJsonArray(
                                    raw, elementType, precision, scale, nullable, NULL))
                    .isNull();
        }

        @ParameterizedTest(name = "{1}")
        @MethodSource(SRC + "arrayEmptyResultCases")
        void returnsEmptyArrayOnErrorMode(
                Object[] raw, LogicalTypeRoot elementType, int precision, int scale) {
            GenericArrayData result =
                    SqlJsonUtils.convertJsonArray(
                            raw, elementType, precision, scale, true, EMPTY_ARRAY);
            assertThat(result).isNotNull();
            assertThat(result.size()).isZero();
        }

        @ParameterizedTest(name = "{1}")
        @MethodSource(SRC + "arrayThrowCases")
        void throwsOnErrorMode(
                Object[] raw, LogicalTypeRoot elementType, int precision, int scale,
                boolean nullable) {
            assertThatThrownBy(
                            () ->
                                    SqlJsonUtils.convertJsonArray(
                                            raw, elementType, precision, scale, nullable, ERROR))
                    .isInstanceOf(TableRuntimeException.class)
                    .hasMessageContaining("Array element type mismatch");
        }

        @ParameterizedTest(name = "{0}")
        @MethodSource(SRC + "emptyArrayInputs")
        void emptyInputStaysEmpty(LogicalTypeRoot elementType, int precision, int scale) {
            GenericArrayData result =
                    SqlJsonUtils.convertJsonArray(
                            new Object[0], elementType, precision, scale, true, NULL);
            assertThat(result).isNotNull();
            assertThat(result.size()).isZero();
        }

        @Test
        void nullInputPassesThrough() {
            assertThat(SqlJsonUtils.convertJsonArray(null, INTEGER, 0, 0, true, NULL)).isNull();
        }
    }
}
