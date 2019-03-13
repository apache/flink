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

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.ScalarFunction;

import org.junit.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.AND;
import static org.apache.flink.table.expressions.BuiltInFunctionDefinitions.EQUALS;
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

	private static Expression createExpressionTree(Integer nestedValue) {
		return new CallExpression(
			AND,
			asList(
				new ValueLiteralExpression(true),
				new CallExpression(
					EQUALS,
					asList(
						new FieldReferenceExpression("field", Types.INT, 0, 0),
						new CallExpression(
							new ScalarFunctionDefinition("dummy", DUMMY_FUNCTION),
							singletonList(new ValueLiteralExpression(nestedValue, Types.INT))
						)
					)
				)
			)
		);
	}
}
