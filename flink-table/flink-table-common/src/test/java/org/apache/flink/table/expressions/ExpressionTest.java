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

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
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
}
