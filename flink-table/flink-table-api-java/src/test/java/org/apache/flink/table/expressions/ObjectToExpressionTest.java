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
import org.apache.flink.types.Row;
import org.apache.flink.types.RowKind;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.flink.table.api.Expressions.array;
import static org.apache.flink.table.api.Expressions.lit;
import static org.apache.flink.table.api.Expressions.map;
import static org.apache.flink.table.api.Expressions.nullOf;
import static org.apache.flink.table.api.Expressions.row;
import static org.apache.flink.table.expressions.ApiExpressionUtils.objectToExpression;
import static org.apache.flink.table.expressions.ApiExpressionUtils.unwrapFromApi;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

/**
 * Tests for converting an object to a {@link Expression} via {@link ApiExpressionUtils#objectToExpression(Object)}.
 */
public class ObjectToExpressionTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testListConversion() {
		Expression expr = objectToExpression(asList(1, 2));

		assertThatEquals(expr, array(1, 2));
	}

	@Test
	public void testNestedListConversion() {
		Expression expr = objectToExpression(asList(singletonList(1), singletonList(2)));

		assertThatEquals(expr, array(array(1), array(2)));
	}

	@Test
	public void testMapConversion() {
		Map<String, List<Integer>> map = new HashMap<>();
		map.put("key1", singletonList(2));
		map.put("key2", asList(1, 2));

		Expression expr = objectToExpression(map);
		assertThatEquals(
			expr,
			map(
				"key1", array(2),
				"key2", array(1, 2)
			)
		);
	}

	@Test
	public void testRowConversion() {
		Expression expr = objectToExpression(Row.of(1, "ABC", new int[]{1, 2, 3}, new byte[]{1, 2, 3}, null));

		assertThatEquals(
			expr,
			row(
				1,
				"ABC",
				array(1, 2, 3),
				lit(new byte[]{1, 2, 3}, DataTypes.BINARY(3).notNull()),
				nullOf(DataTypes.NULL())));
	}

	@Test
	public void testRowWithDeleteKindConversion() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Unsupported kind 'DELETE' of a row [1]. " +
			"Only rows with 'INSERT' kind are supported when converting to an expression.");
		objectToExpression(Row.ofKind(RowKind.DELETE, 1));
	}

	@Test
	public void testRowWithUpdateBeforeKindConversion() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Unsupported kind 'UPDATE_BEFORE' of a row [1]. " +
			"Only rows with 'INSERT' kind are supported when converting to an expression.");
		objectToExpression(Row.ofKind(RowKind.UPDATE_BEFORE, 1));
	}

	@Test
	public void testRowWithUpdateAfterKindConversion() {
		thrown.expect(ValidationException.class);
		thrown.expectMessage("Unsupported kind 'UPDATE_AFTER' of a row [1]. " +
			"Only rows with 'INSERT' kind are supported when converting to an expression.");
		objectToExpression(Row.ofKind(RowKind.UPDATE_AFTER, 1));
	}

	private static void assertThatEquals(Expression actual, Expression expected) {
		assertThat(unwrapFromApi(actual), equalTo(unwrapFromApi(expected)));
	}
}
