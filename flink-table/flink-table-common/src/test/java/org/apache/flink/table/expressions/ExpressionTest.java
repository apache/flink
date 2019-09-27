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
import java.sql.Timestamp;
import java.time.LocalDateTime;

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
					DataTypes.ARRAY(DataTypes.ARRAY(DataTypes.STRING())))
				.toString());
	}

	@Test
	public void testInvalidValueLiteral() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("does not support a value literal of class 'java.lang.Integer'");

		new ValueLiteralExpression(12, DataTypes.TINYINT());
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
	public void testSqlTimestampValueLiteralExtraction() {
		final Timestamp sqlTimestamp = Timestamp.valueOf("2006-11-03 00:00:00.123456789");
		final LocalDateTime localDateTime = LocalDateTime.of(2006, 11, 3, 0, 0, 0, 123456789);

		assertEquals(
			localDateTime,
			new ValueLiteralExpression(sqlTimestamp).getValueAs(LocalDateTime.class)
				.orElseThrow(AssertionError::new));

		assertEquals(
			sqlTimestamp,
			new ValueLiteralExpression(localDateTime).getValueAs(Timestamp.class)
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

	// --------------------------------------------------------------------------------------------

	private static Expression createExpressionTree(Integer nestedValue) {
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
							singletonList(new ValueLiteralExpression(nestedValue, DataTypes.INT())),
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
