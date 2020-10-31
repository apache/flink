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

package org.apache.calcite.rex;

import org.apache.flink.table.planner.calcite.FlinkRexBuilder;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.FlinkTypeSystem;

import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;

/**
 * Tests for {@link FlinkRexSimplifySearchUtil}.
 */
public class FlinkRexSimplifySearchUtilTest {

	private final FlinkTypeFactory typeFactory = new FlinkTypeFactory(new FlinkTypeSystem());
	private final RexBuilder rexBuilder = new FlinkRexBuilder(typeFactory);

	@Test
	public void testSimplifySearchInAnd() {
		RexInputRef a = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

		// a = 0 AND SEARCH(a, [0, 1]) -> a = 0
		RexNode equals = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, a, intLiteral(0));
		RexNode predicate = rexBuilder.makeCall(
			SqlStdOperatorTable.AND,
			equals,
			rexBuilder.makeIn(a, Arrays.asList(intLiteral(0), intLiteral(1))));

		assertSimplify(equals, predicate);
	}

	@Test
	public void testSimplifySearchInOr() {
		RexInputRef a = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

		// a > 0 OR SEARCH(a, [1, 2]) -> a > 0
		RexNode greaterThan = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, a, intLiteral(0));
		RexNode predicate = rexBuilder.makeCall(
			SqlStdOperatorTable.OR,
			greaterThan,
			rexBuilder.makeIn(a, Arrays.asList(intLiteral(1), intLiteral(2))));

		assertSimplify(greaterThan, predicate);
	}

	@Test
	public void testSimplifySearchInNestedAndsAndOrs() {
		RexInputRef a = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);

		// a > 0 AND (
		//   SEARCH(a, [0, 1]) OR (
		//     SEARCH(a, [1, 2] AND a < 2)))
		// -> a = 1
		RexNode layer2 = rexBuilder.makeCall(
			SqlStdOperatorTable.AND,
			rexBuilder.makeIn(a, Arrays.asList(intLiteral(1), intLiteral(2))),
			rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, a, intLiteral(2)));
		RexNode layer1 = rexBuilder.makeCall(
			SqlStdOperatorTable.OR,
			rexBuilder.makeIn(a, Arrays.asList(intLiteral(0), intLiteral(1))),
			layer2);
		RexNode predicate = rexBuilder.makeCall(
			SqlStdOperatorTable.AND,
			rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, a, intLiteral(0)),
			layer1);

		assertSimplify(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, a, intLiteral(1)), predicate);
	}

	@Test
	public void testNotSimplifyComplexSearch() {
		RexInputRef a = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
		RexInputRef b = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);

		// SEARCH(a, [0, 2, 4]) OR SEARCH(b, [6])
		RexNode search1 = rexBuilder.makeIn(a, Arrays.asList(intLiteral(0), intLiteral(2), intLiteral(4)));
		RexNode search2 = rexBuilder.makeIn(b, Collections.singletonList(intLiteral(6)));
		RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.OR, search1, search2);
		Assert.assertEquals(
			predicate.toString(),
			FlinkRexSimplifySearchUtil.simplify(rexBuilder, predicate).toString());
	}

	@Test
	public void testNotExtractSearch() {
		RexInputRef a = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 0);
		RexInputRef b = rexBuilder.makeInputRef(typeFactory.createSqlType(SqlTypeName.INTEGER), 1);

		// SEARCH(a, [0..3]) OR SEARCH(b, [4..7])
		RexSimplify calciteSimplifier = new RexSimplify(
			rexBuilder,
			RelOptPredicateList.EMPTY,
			true,
			RexUtil.EXECUTOR);

		RexNode search1 = calciteSimplifier.simplify(rexBuilder.makeCall(
			SqlStdOperatorTable.AND,
			rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, a, intLiteral(0)),
			rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, a, intLiteral(3))
		));
		Assert.assertTrue(search1.isA(SqlKind.SEARCH));

		RexNode search2 = calciteSimplifier.simplify(rexBuilder.makeCall(
			SqlStdOperatorTable.AND,
			rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, b, intLiteral(4)),
			rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, b, intLiteral(7))
		));
		Assert.assertTrue(search2.isA(SqlKind.SEARCH));

		RexNode predicate = rexBuilder.makeCall(SqlStdOperatorTable.OR, search1, search2);
		Assert.assertEquals(
			predicate.toString(),
			FlinkRexSimplifySearchUtil.simplify(rexBuilder, predicate).toString());
	}

	private void assertSimplify(RexNode expected, RexNode toSimplify) {
		RexNode actual = FlinkRexSimplifySearchUtil.simplify(rexBuilder, toSimplify);
		Assert.assertEquals(expected.toString(), actual.toString());

		RexSimplify calciteSimplifier = new RexSimplify(
			rexBuilder,
			RelOptPredicateList.EMPTY,
			true,
			RexUtil.EXECUTOR);
		RexNode calciteActual = calciteSimplifier.simplify(toSimplify);
		// this will fail when CALCITE-4364 is fixed,
		// at that time the commits for FLINK-19811 should be reverted
		Assert.assertNotEquals(expected.toString(), calciteActual.toString());
	}

	private RexLiteral intLiteral(int x) {
		return rexBuilder.makeExactLiteral(BigDecimal.valueOf(x));
	}
}
