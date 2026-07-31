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

package org.apache.flink.table.expressions;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.types.DataType;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoField;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.AND;
import static org.apache.flink.table.functions.BuiltInFunctionDefinitions.EQUALS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link org.apache.flink.table.expressions.Expression} and its sub-classes. */
class ExpressionTest {

    private static final ScalarFunction DUMMY_FUNCTION = new ScalarFunction() {
                // dummy
            };

    private static final Expression TREE_WITH_NULL = createExpressionTree(null);

    private static final Expression TREE_WITH_VALUE = createExpressionTree(12);

    private static final Expression TREE_WITH_SAME_VALUE = createExpressionTree(12);

    private static final String TREE_WITH_NULL_STRING = "and(true, equals(field, dummy(null)))";

    @Test
    void testExpressionString() {
        assertThat(TREE_WITH_NULL.toString()).isEqualTo(TREE_WITH_NULL_STRING);
    }

    @Test
    void testExpressionEquality() {
        assertThat(TREE_WITH_SAME_VALUE).isEqualTo(TREE_WITH_VALUE);
    }

    @Test
    void testArrayValueLiteralEquality() {
        assertThat(new ValueLiteralExpression(new Integer[][] {null, null, {1, 2, 3}}))
                .isEqualTo(new ValueLiteralExpression(new Integer[][] {null, null, {1, 2, 3}}));

        assertThat(
                        new ValueLiteralExpression(
                                new String[][] {null, null, {"1", "2", "3", "Dog's"}},
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())).notNull()))
                .isEqualTo(
                        new ValueLiteralExpression(
                                new String[][] {null, null, {"1", "2", "3", "Dog's"}},
                                DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())).notNull()));

        assertThat(new ValueLiteralExpression("abc".getBytes(StandardCharsets.UTF_8)))
                .isEqualTo(new ValueLiteralExpression("abc".getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void testExpressionInequality() {
        assertThat(TREE_WITH_VALUE).isNotEqualTo(TREE_WITH_NULL);
    }

    @Test
    void testValueLiteralString() {
        assertThat(new ValueLiteralExpression(new Integer[][] {null, null, {1, 2, 3}}).toString())
                .isEqualTo("[null, null, [1, 2, 3]]");

        assertThat(
                        new ValueLiteralExpression(
                                        new String[][] {null, null, {"1", "2", "3", "Dog's"}},
                                        DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING()))
                                                .notNull())
                                .toString())
                .isEqualTo("[null, null, ['1', '2', '3', 'Dog''s']]");

        final Map<String, Integer> map = new LinkedHashMap<>();
        map.put("key1", 1);
        map.put("key2", 2);
        map.put("key3", 3);
        assertThat(
                        new ValueLiteralExpression(
                                        map,
                                        DataTypes.MAP(DataTypes.STRING(), DataTypes.INT())
                                                .notNull())
                                .toString())
                .isEqualTo("{key1=1, key2=2, key3=3}");
        assertThat(
                        new ValueLiteralExpression(
                                        map, DataTypes.MULTISET(DataTypes.STRING()).notNull())
                                .toString())
                .isEqualTo("{key1=1, key2=2, key3=3}");
    }

    @Test
    void testInvalidValueLiteral() {
        assertThatThrownBy(() -> new ValueLiteralExpression(12, DataTypes.TINYINT().notNull()))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining(
                        "does not support a value literal of class 'java.lang.Integer'");
    }

    @Test
    void testInvalidValueLiteralExtraction() {
        assertThatThrownBy(() -> new ValueLiteralExpression(this))
                .isInstanceOf(ValidationException.class)
                .hasMessageContaining("Cannot derive a data type");
    }

    @Test
    void testBigDecimalValueLiteralExtraction() {
        final float f = 2.44444444443f;
        assertThat(
                        new ValueLiteralExpression(f)
                                .getValueAs(BigDecimal.class)
                                .map(BigDecimal::floatValue)
                                .orElseThrow(AssertionError::new))
                .isEqualTo(f);
    }

    @Test
    void testLocalDateTimeValueLiteralExtraction() {
        final Timestamp sqlTimestamp = Timestamp.valueOf("2006-11-03 00:00:00.123456789");
        final LocalDateTime localDateTime = LocalDateTime.of(2006, 11, 3, 0, 0, 0, 123456789);

        assertThat(
                        new ValueLiteralExpression(sqlTimestamp)
                                .getValueAs(LocalDateTime.class)
                                .orElseThrow(AssertionError::new))
                .isEqualTo(localDateTime);
    }

    @Test
    void testLocalTimeValueLiteralExtraction() {
        final LocalTime localTime = LocalTime.of(12, 12, 12, 123456789);

        final long nanos = localTime.toNanoOfDay();

        final int millis = localTime.get(ChronoField.MILLI_OF_DAY);

        final Time sqlTime = Time.valueOf("12:12:12");

        assertThat(
                        new ValueLiteralExpression(sqlTime)
                                .getValueAs(LocalTime.class)
                                .orElseThrow(AssertionError::new))
                .isEqualTo(localTime.withNano(0));

        assertThat(
                        new ValueLiteralExpression(nanos)
                                .getValueAs(LocalTime.class)
                                .orElseThrow(AssertionError::new))
                .isEqualTo(localTime);

        assertThat(
                        new ValueLiteralExpression(millis)
                                .getValueAs(LocalTime.class)
                                .orElseThrow(AssertionError::new))
                .isEqualTo(localTime.minusNanos(456789));
    }

    @Test
    void testLocalDateValueLiteralExtraction() {
        final LocalDate localDate = LocalDate.of(2012, 12, 12);

        final int daysSinceEpoch = (int) localDate.toEpochDay();

        final Date sqlDate = Date.valueOf("2012-12-12");

        assertThat(
                        new ValueLiteralExpression(sqlDate)
                                .getValueAs(LocalDate.class)
                                .orElseThrow(AssertionError::new))
                .isEqualTo(localDate);

        assertThat(
                        new ValueLiteralExpression(daysSinceEpoch)
                                .getValueAs(LocalDate.class)
                                .orElseThrow(AssertionError::new))
                .isEqualTo(localDate);
    }

    @Test
    void testInstantValueLiteralExtraction() {
        final Instant instant = Instant.ofEpochMilli(100);

        final long millis = instant.toEpochMilli();

        final int seconds = (int) instant.toEpochMilli() / 1_000;

        assertThat(
                        new ValueLiteralExpression(millis)
                                .getValueAs(Instant.class)
                                .orElseThrow(AssertionError::new))
                .isEqualTo(instant);

        assertThat(
                        new ValueLiteralExpression(seconds)
                                .getValueAs(Instant.class)
                                .orElseThrow(AssertionError::new))
                .isEqualTo(instant.minusMillis(100));
    }

    @Test
    void testOffsetDateTimeValueLiteralExtraction() {
        final OffsetDateTime offsetDateTime =
                OffsetDateTime.of(
                        LocalDateTime.parse("2012-12-12T12:12:12"),
                        ZoneOffset.ofHours(1)); // Europe/Berlin equals GMT+1 on 2012-12-12

        final ZonedDateTime zonedDateTime =
                ZonedDateTime.of(
                        LocalDateTime.parse("2012-12-12T12:12:12"), ZoneId.of("Europe/Berlin"));

        assertThat(
                        new ValueLiteralExpression(zonedDateTime)
                                .getValueAs(OffsetDateTime.class)
                                .orElseThrow(AssertionError::new))
                .isEqualTo(offsetDateTime);
    }

    @Test
    void testSymbolValueLiteralExtraction() {
        final TimeIntervalUnit intervalUnit = TimeIntervalUnit.DAY_TO_MINUTE;

        assertThat(
                        new ValueLiteralExpression(intervalUnit)
                                .getValueAs(TimeIntervalUnit.class)
                                .orElseThrow(AssertionError::new))
                .isEqualTo(intervalUnit);
    }

    @Test
    void testPeriodValueLiteralExtraction() {
        Integer periodInInt = 10;
        final Period expected = Period.ofMonths(10);
        assertThat(
                        new ValueLiteralExpression(periodInInt)
                                .getValueAs(Period.class)
                                .orElseThrow(AssertionError::new))
                .isEqualTo(expected);
    }

    @ParameterizedTest(name = "precision {1}: {2}")
    @MethodSource("timestampLtzPrecisionTestCases")
    void testTimestampLtzPrecisionAsSerializableString(
            Instant instant, int precision, String expected) {
        assertThat(
                        new ValueLiteralExpression(
                                        instant, DataTypes.TIMESTAMP_LTZ(precision).notNull())
                                .asSerializableString(DefaultSqlFactory.INSTANCE))
                .isEqualTo(expected);
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("intervalLiteralTestCases")
    void testIntervalAsSerializableString(
            String caseName, Object value, DataType dataType, String expected) {
        assertThat(
                        new ValueLiteralExpression(value, dataType)
                                .asSerializableString(DefaultSqlFactory.INSTANCE))
                .isEqualTo(expected);
    }

    // --------------------------------------------------------------------------------------------

    private static Expression createExpressionTree(Integer nestedValue) {
        final ValueLiteralExpression nestedLiteral;
        if (nestedValue != null) {
            nestedLiteral = new ValueLiteralExpression(nestedValue, DataTypes.INT().notNull());
        } else {
            nestedLiteral = new ValueLiteralExpression(null, DataTypes.INT());
        }
        return CallExpression.permanent(
                AND,
                asList(
                        new ValueLiteralExpression(true),
                        CallExpression.permanent(
                                EQUALS,
                                asList(
                                        new FieldReferenceExpression(
                                                "field", DataTypes.INT(), 0, 0),
                                        CallExpression.anonymous(
                                                new ScalarFunctionDefinition(
                                                        "dummy", DUMMY_FUNCTION),
                                                singletonList(nestedLiteral),
                                                DataTypes.INT())),
                                DataTypes.BOOLEAN())),
                DataTypes.BOOLEAN());
    }

    private static Stream<Arguments> timestampLtzPrecisionTestCases() {
        return Stream.of(
                // Precision 0-2: numeric variant wrapped in CAST to match data type
                Arguments.of(
                        Instant.ofEpochSecond(1234),
                        0,
                        "CAST(TO_TIMESTAMP_LTZ(1234, 0) AS TIMESTAMP_LTZ(0))"),
                Arguments.of(
                        Instant.ofEpochSecond(1, 100_000_000),
                        1,
                        "CAST(TO_TIMESTAMP_LTZ(11, 1) AS TIMESTAMP_LTZ(1))"),
                Arguments.of(
                        Instant.ofEpochSecond(1, 120_000_000),
                        2,
                        "CAST(TO_TIMESTAMP_LTZ(112, 2) AS TIMESTAMP_LTZ(2))"),
                // Precision 3+: numeric variant without CAST
                Arguments.of(Instant.ofEpochMilli(1234567), 3, "TO_TIMESTAMP_LTZ(1234567, 3)"),
                Arguments.of(Instant.ofEpochSecond(1, 123400000), 4, "TO_TIMESTAMP_LTZ(11234, 4)"),
                Arguments.of(Instant.ofEpochSecond(1, 123450000), 5, "TO_TIMESTAMP_LTZ(112345, 5)"),
                Arguments.of(
                        Instant.ofEpochSecond(1, 123456000), 6, "TO_TIMESTAMP_LTZ(1123456, 6)"),
                Arguments.of(
                        Instant.ofEpochSecond(1, 123456700), 7, "TO_TIMESTAMP_LTZ(11234567, 7)"),
                Arguments.of(
                        Instant.ofEpochSecond(1, 123456780), 8, "TO_TIMESTAMP_LTZ(112345678, 8)"),
                Arguments.of(
                        Instant.ofEpochSecond(1, 123456789), 9, "TO_TIMESTAMP_LTZ(1123456789, 9)"),
                // Edge cases: large instants fall back to string variant to avoid long overflow
                Arguments.of(
                        Instant.parse("9999-12-31T23:59:59.999999999Z"),
                        9,
                        "TO_TIMESTAMP_LTZ('9999-12-31 23:59:59.999999999', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS', 'UTC')"),
                Arguments.of(
                        Instant.parse("2262-04-12T00:00:00Z"),
                        9,
                        "TO_TIMESTAMP_LTZ('2262-04-12 00:00:00.000000000', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS', 'UTC')"),
                Arguments.of(
                        Instant.ofEpochSecond(9223372036L, 900_000_000),
                        9,
                        "TO_TIMESTAMP_LTZ('2262-04-11 23:47:16.900000000', 'yyyy-MM-dd HH:mm:ss.SSSSSSSSS', 'UTC')"),
                Arguments.of(
                        Instant.ofEpochSecond(-9223372036L, 0),
                        9,
                        "TO_TIMESTAMP_LTZ(-9223372036000000000, 9)"));
    }

    private static Stream<Arguments> intervalLiteralTestCases() {
        return Stream.of(
                Arguments.of(
                        "YEAR_TO_MONTH maximum",
                        Period.of(9999, 11, 0),
                        DataTypes.INTERVAL(DataTypes.YEAR(4), DataTypes.MONTH()).notNull(),
                        "INTERVAL '9999-11' YEAR(4) TO MONTH"),
                Arguments.of(
                        "YEAR_TO_MONTH default precision",
                        Period.ofMonths(470),
                        DataTypes.INTERVAL(DataTypes.YEAR(), DataTypes.MONTH()).notNull(),
                        "INTERVAL '39-2' YEAR(2) TO MONTH"),
                Arguments.of(
                        "YEAR with months not dropped",
                        Period.of(5, 3, 0),
                        DataTypes.INTERVAL(DataTypes.YEAR()).notNull(),
                        "INTERVAL '5-3' YEAR(2) TO MONTH"),
                Arguments.of(
                        "YEAR only whole years",
                        Period.ofYears(120),
                        DataTypes.INTERVAL(DataTypes.YEAR(3)).notNull(),
                        "INTERVAL '120-0' YEAR(3) TO MONTH"),
                Arguments.of(
                        "MONTH only",
                        Period.ofMonths(50),
                        DataTypes.INTERVAL(DataTypes.MONTH()).notNull(),
                        "INTERVAL '4-2' YEAR(2) TO MONTH"),
                Arguments.of(
                        "DAY(3) three-digit value",
                        Duration.ofDays(100),
                        DataTypes.INTERVAL(DataTypes.DAY(3)).notNull(),
                        "INTERVAL '100 00:00:00.000000' DAY(3) TO SECOND(6)"),
                Arguments.of(
                        "DAY_TO_HOUR",
                        Duration.ofDays(5).plusHours(7),
                        DataTypes.INTERVAL(DataTypes.DAY(2), DataTypes.HOUR()).notNull(),
                        "INTERVAL '5 07:00:00.000000' DAY(2) TO SECOND(6)"),
                Arguments.of(
                        "DAY_TO_MINUTE",
                        Duration.ofDays(5).plusHours(7).plusMinutes(8),
                        DataTypes.INTERVAL(DataTypes.DAY(2), DataTypes.MINUTE()).notNull(),
                        "INTERVAL '5 07:08:00.000000' DAY(2) TO SECOND(6)"),
                Arguments.of(
                        "DAY_TO_SECOND fractional padding",
                        Duration.ofDays(1).plusMillis(5),
                        DataTypes.INTERVAL(DataTypes.DAY(2), DataTypes.SECOND(3)).notNull(),
                        "INTERVAL '1 00:00:00.005' DAY(2) TO SECOND(3)"),
                Arguments.of(
                        "DAY_TO_SECOND nanosecond precision (string only; sub-ms not round-trippable)",
                        Duration.ofDays(99).plusSeconds(34).plusNanos(999_000_001),
                        DataTypes.INTERVAL(DataTypes.DAY(2), DataTypes.SECOND(9)).notNull(),
                        "INTERVAL '99 00:00:34.999000001' DAY(2) TO SECOND(9)"),
                Arguments.of(
                        "DAY_TO_SECOND zero fractional precision",
                        Duration.ofDays(1).plusSeconds(30),
                        DataTypes.INTERVAL(DataTypes.DAY(2), DataTypes.SECOND(0)).notNull(),
                        "INTERVAL '1 00:00:30' DAY(2) TO SECOND(0)"),
                Arguments.of(
                        "HOUR only preserves whole value",
                        Duration.ofHours(30),
                        DataTypes.INTERVAL(DataTypes.HOUR()).notNull(),
                        "INTERVAL '1 06:00:00.000000' DAY(2) TO SECOND(6)"),
                Arguments.of(
                        "HOUR with minutes not dropped",
                        Duration.ofHours(5).plusMinutes(30),
                        DataTypes.INTERVAL(DataTypes.HOUR()).notNull(),
                        "INTERVAL '0 05:30:00.000000' DAY(2) TO SECOND(6)"),
                Arguments.of(
                        "HOUR_TO_MINUTE",
                        Duration.ofHours(30).plusMinutes(15),
                        DataTypes.INTERVAL(DataTypes.HOUR(), DataTypes.MINUTE()).notNull(),
                        "INTERVAL '1 06:15:00.000000' DAY(2) TO SECOND(6)"),
                Arguments.of(
                        "HOUR_TO_SECOND",
                        Duration.ofHours(30).plusMinutes(15).plusSeconds(20).plusNanos(500_000_000),
                        DataTypes.INTERVAL(DataTypes.HOUR(), DataTypes.SECOND(6)).notNull(),
                        "INTERVAL '1 06:15:20.500000' DAY(2) TO SECOND(6)"),
                Arguments.of(
                        "MINUTE only",
                        Duration.ofMinutes(45),
                        DataTypes.INTERVAL(DataTypes.MINUTE()).notNull(),
                        "INTERVAL '0 00:45:00.000000' DAY(2) TO SECOND(6)"),
                Arguments.of(
                        "MINUTE >= 100 (0 days, fits DAY(2))",
                        Duration.ofMinutes(100),
                        DataTypes.INTERVAL(DataTypes.MINUTE()).notNull(),
                        "INTERVAL '0 01:40:00.000000' DAY(2) TO SECOND(6)"),
                Arguments.of(
                        "MINUTE_TO_SECOND",
                        Duration.ofMinutes(45).plusSeconds(45).plusMillis(120),
                        DataTypes.INTERVAL(DataTypes.MINUTE(), DataTypes.SECOND(2)).notNull(),
                        "INTERVAL '0 00:45:45.12' DAY(2) TO SECOND(2)"),
                Arguments.of(
                        "SECOND standalone",
                        Duration.ofSeconds(45).plusMillis(750),
                        DataTypes.INTERVAL(DataTypes.SECOND(4)).notNull(),
                        "INTERVAL '0 00:00:45.7500' DAY(2) TO SECOND(4)"),
                Arguments.of(
                        "SECOND >= 100 (0 days, fits DAY(2))",
                        Duration.ofSeconds(150),
                        DataTypes.INTERVAL(DataTypes.SECOND(3)).notNull(),
                        "INTERVAL '0 00:02:30.000' DAY(2) TO SECOND(3)"),
                Arguments.of(
                        "Large day value under its natural DAY(3) type (= 10000 hours)",
                        Duration.ofDays(416).plusHours(16),
                        DataTypes.INTERVAL(DataTypes.DAY(3), DataTypes.HOUR()).notNull(),
                        "INTERVAL '416 16:00:00.000000' DAY(3) TO SECOND(6)"),
                Arguments.of(
                        "DAY maximum precision",
                        Duration.ofDays(999_999),
                        DataTypes.INTERVAL(DataTypes.DAY(6)).notNull(),
                        "INTERVAL '999999 00:00:00.000000' DAY(6) TO SECOND(6)"));
    }
}
