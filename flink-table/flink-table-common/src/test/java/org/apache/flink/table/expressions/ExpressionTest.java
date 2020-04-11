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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Tests for {@link org.apache.flink.table.expressions.Expression} and its sub-classes.
 */
public class ExpressionTest {

	private static final ScalarFunction DUMMY_FUNCTION = new ScalarFunction() {
		// dummy
	};

	private static final Expression TREE_WITH_NULL = createExpressionTree(null);

	private static final Expression TREE_WITH_VALUE = createExpressionTree(12);

	private static final Expression TREE_WITH_SAME_VALUE = createExpressionTree(12);

	private static final String TREE_WITH_NULL_STRING =
		"and(true, equals(field, dummy(null)))";

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testExpressionString() {
		assertEquals(TREE_WITH_NULL_STRING, TREE_WITH_NULL.toString());
	}

	@Test
	public void testExpressionEquality() {
		assertEquals(TREE_WITH_VALUE, TREE_WITH_SAME_VALUE);
	}

	@Test
	public void testArrayValueLiteralEquality() {
		assertEquals(
			new ValueLiteralExpression(new Integer[][]{null, null, {1, 2, 3}}),
			new ValueLiteralExpression(new Integer[][]{null, null, {1, 2, 3}}));

		assertEquals(
			new ValueLiteralExpression(
				new String[][]{null, null, {"1", "2", "3", "Dog's"}},
				DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())).notNull()),
			new ValueLiteralExpression(
				new String[][]{null, null, {"1", "2", "3", "Dog's"}},
				DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())).notNull())
		);

		assertEquals(
			new ValueLiteralExpression("abc".getBytes(StandardCharsets.UTF_8)),
			new ValueLiteralExpression("abc".getBytes(StandardCharsets.UTF_8))
		);
	}

	@Test
	public void testExpressionInequality() {
		assertNotEquals(TREE_WITH_NULL, TREE_WITH_VALUE);
	}

	@Test
	public void testValueLiteralString() {
		assertEquals(
			"[null, null, [1, 2, 3]]",
			new ValueLiteralExpression(new Integer[][]{null, null, {1, 2, 3}}).toString());

		assertEquals(
			"[null, null, ['1', '2', '3', 'Dog''s']]",
			new ValueLiteralExpression(
					new String[][]{null, null, {"1", "2", "3", "Dog's"}},
					DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())).notNull())
				.toString());

		final Map<String, Integer> map = new LinkedHashMap<>();
		map.put("key1", 1);
		map.put("key2", 2);
		map.put("key3", 3);
		assertEquals(
			"{key1=1, key2=2, key3=3}",
			new ValueLiteralExpression(
					map,
					DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()).notNull())
				.toString());
		assertEquals(
			"{key1=1, key2=2, key3=3}",
			new ValueLiteralExpression(
					map,
					DataTypes.MULTISET(DataTypes.STRING()).notNull())
				.toString());
	}

	@Test
	public void testInvalidValueLiteral() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("does not support a value literal of class 'java.lang.Integer'");

		new ValueLiteralExpression(12, DataTypes.TINYINT().notNull());
	}

	@Test
	public void testInvalidValueLiteralExtraction() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Cannot derive a data type");

		new ValueLiteralExpression(this);
	}

	@Test
	public void testBigDecimalValueLiteralExtraction() {
		final float f = 2.44444444443f;
		assertEquals(
			f,
			new ValueLiteralExpression(f).getValueAs(BigDecimal.class)
				.map(BigDecimal::floatValue)
				.orElseThrow(AssertionError::new),
			0);
	}

	@Test
	public void testLocalDateTimeValueLiteralExtraction() {
		final Timestamp sqlTimestamp = Timestamp.valueOf("2006-11-03 00:00:00.123456789");
		final LocalDateTime localDateTime = LocalDateTime.of(2006, 11, 3, 0, 0, 0, 123456789);

		assertEquals(
			localDateTime,
			new ValueLiteralExpression(sqlTimestamp).getValueAs(LocalDateTime.class)
				.orElseThrow(AssertionError::new));
	}

	@Test
	public void testLocalTimeValueLiteralExtraction() {
		final LocalTime localTime = LocalTime.of(12, 12, 12, 123456789);

		final long nanos = localTime.toNanoOfDay();

		final int millis = localTime.get(ChronoField.MILLI_OF_DAY);

		final Time sqlTime = Time.valueOf("12:12:12");

		assertEquals(
			localTime.withNano(0),
			new ValueLiteralExpression(sqlTime).getValueAs(LocalTime.class)
				.orElseThrow(AssertionError::new));

		assertEquals(
			localTime,
			new ValueLiteralExpression(nanos).getValueAs(LocalTime.class)
				.orElseThrow(AssertionError::new));

		assertEquals(
			localTime.minusNanos(456789),
			new ValueLiteralExpression(millis).getValueAs(LocalTime.class)
				.orElseThrow(AssertionError::new));
	}

	@Test
	public void testLocalDateValueLiteralExtraction() {
		final LocalDate localDate = LocalDate.of(2012, 12, 12);

		final int daysSinceEpoch = (int) localDate.toEpochDay();

		final Date sqlDate = Date.valueOf("2012-12-12");

		assertEquals(
			localDate,
			new ValueLiteralExpression(sqlDate).getValueAs(LocalDate.class)
				.orElseThrow(AssertionError::new));

		assertEquals(
			localDate,
			new ValueLiteralExpression(daysSinceEpoch).getValueAs(LocalDate.class)
				.orElseThrow(AssertionError::new));
	}

	@Test
	public void testInstantValueLiteralExtraction() {
		final Instant instant = Instant.ofEpochMilli(100);

		final long millis = instant.toEpochMilli();

		final int seconds = (int) instant.toEpochMilli() / 1_000;

		assertEquals(
			instant,
			new ValueLiteralExpression(millis).getValueAs(Instant.class)
				.orElseThrow(AssertionError::new));

		assertEquals(
			instant.minusMillis(100),
			new ValueLiteralExpression(seconds).getValueAs(Instant.class)
				.orElseThrow(AssertionError::new));
	}

	@Test
	public void testOffsetDateTimeValueLiteralExtraction() {
		final OffsetDateTime offsetDateTime = OffsetDateTime.of(
			LocalDateTime.parse("2012-12-12T12:12:12"),
			ZoneOffset.ofHours(1)); // Europe/Berlin equals GMT+1 on 2012-12-12

		final ZonedDateTime zonedDateTime = ZonedDateTime.of(
			LocalDateTime.parse("2012-12-12T12:12:12"),
			ZoneId.of("Europe/Berlin"));

		assertEquals(
			offsetDateTime,
			new ValueLiteralExpression(zonedDateTime).getValueAs(OffsetDateTime.class)
				.orElseThrow(AssertionError::new));
	}

	@Test
	public void testSymbolValueLiteralExtraction() {
		final TimeIntervalUnit intervalUnit = TimeIntervalUnit.DAY_TO_MINUTE;

		assertEquals(
			intervalUnit,
			new ValueLiteralExpression(intervalUnit).getValueAs(TimeIntervalUnit.class)
				.orElseThrow(AssertionError::new));
	}

	@Test
	public void testPeriodValueLiteralExtraction() {
		Integer periodInInt = 10;
		final Period expected = Period.ofMonths(10);
		assertEquals(
			expected,
			new ValueLiteralExpression(periodInInt).getValueAs(Period.class)
				.orElseThrow(AssertionError::new));
	}

	// --------------------------------------------------------------------------------------------

	private static Expression createExpressionTree(Integer nestedValue) {
		final ValueLiteralExpression nestedLiteral;
		if (nestedValue != null) {
			nestedLiteral = new ValueLiteralExpression(nestedValue, DataTypes.INT().notNull());
		} else {
			nestedLiteral = new ValueLiteralExpression(null, DataTypes.INT());
		}
		return new CallExpression(
			AND,
			asList(
				new ValueLiteralExpression(true),
				new CallExpression(
					EQUALS,
					asList(
						new FieldReferenceExpression("field", DataTypes.INT(), 0, 0),
						new CallExpression(
							new ScalarFunctionDefinition("dummy", DUMMY_FUNCTION),
							singletonList(nestedLiteral),
							DataTypes.INT()
						)
					),
					DataTypes.BOOLEAN()
				)
			),
			DataTypes.BOOLEAN()
		);
	}
}
