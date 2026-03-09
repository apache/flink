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
package org.apache.flink.table.planner.codegen

import org.apache.calcite.jdbc.JavaTypeFactoryImpl
import org.apache.calcite.rel.`type`.RelDataTypeSystem
import org.apache.calcite.rex.RexBuilder
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.Test

/**
 * Unit test for [[CseUtils]] - the Common Sub-expression Elimination analyzer.
 *
 * <p>These tests verify that the CSE analyzer correctly identifies duplicate sub-expressions within
 * a list of RexNode expressions.
 */
class CseUtilsTest {

  private val typeFactory = new JavaTypeFactoryImpl(RelDataTypeSystem.DEFAULT)
  private val rexBuilder = new RexBuilder(typeFactory)
  private val intType = typeFactory.createSqlType(SqlTypeName.INTEGER)

  /**
   * Tests detection of duplicate sub-expressions across two projections. Given proj1 = ($0 + $1) *
   * 2 and proj2 = ($0 + $1) * 3, the common sub-expression ($0 + $1) should be identified.
   */
  @Test
  def testFindDuplicateSubExpressions(): Unit = {
    val input0 = rexBuilder.makeInputRef(intType, 0)
    val input1 = rexBuilder.makeInputRef(intType, 1)

    // Common sub-expression: $0 + $1
    val addExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, input0, input1)

    // proj1 = ($0 + $1) * 2
    val literal2 = rexBuilder.makeLiteral(2, intType, false)
    val proj1 = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, addExpr, literal2)

    // proj2 = ($0 + $1) * 3
    val literal3 = rexBuilder.makeLiteral(3, intType, false)
    val proj2 = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, addExpr, literal3)

    val duplicates = CseUtils.findDuplicateSubExpressions(Seq(proj1, proj2))

    // addExpr appears in both projections
    assertThat(duplicates.contains(addExpr.toString)).isTrue
    // Each MULTIPLY is unique
    assertThat(duplicates.contains(proj1.toString)).isFalse
    assertThat(duplicates.contains(proj2.toString)).isFalse
  }

  /** Tests that no duplicates are found when all expressions are unique. */
  @Test
  def testNoDuplicates(): Unit = {
    val input0 = rexBuilder.makeInputRef(intType, 0)
    val input1 = rexBuilder.makeInputRef(intType, 1)

    val proj1 = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, input0, input1)
    val proj2 = rexBuilder.makeCall(SqlStdOperatorTable.MINUS, input0, input1)

    val duplicates = CseUtils.findDuplicateSubExpressions(Seq(proj1, proj2))

    assertThat(duplicates.isEmpty).isTrue
  }

  /** Tests that identical top-level expressions are correctly detected. */
  @Test
  def testIdenticalTopLevelExpressions(): Unit = {
    val input0 = rexBuilder.makeInputRef(intType, 0)
    val input1 = rexBuilder.makeInputRef(intType, 1)

    val expr1 = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, input0, input1)
    val expr2 = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, input0, input1)

    val duplicates = CseUtils.findDuplicateSubExpressions(Seq(expr1, expr2))

    assertThat(duplicates.contains(expr1.toString)).isTrue
  }

  /**
   * Tests detection of deeply nested duplicate sub-expressions. Given proj1 = f(f($0)) * 2 and
   * proj2 = f(f($0)) * 3, both f($0) and f(f($0)) should be detected as duplicates.
   */
  @Test
  def testNestedDuplicates(): Unit = {
    val input0 = rexBuilder.makeInputRef(intType, 0)
    val literal1 = rexBuilder.makeLiteral(1, intType, false)

    // Inner common expr: $0 + 1
    val innerExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, input0, literal1)
    // Outer common expr: ($0 + 1) + 1
    val outerExpr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, innerExpr, literal1)

    val literal2 = rexBuilder.makeLiteral(2, intType, false)
    val literal3 = rexBuilder.makeLiteral(3, intType, false)

    // proj1 = (($0 + 1) + 1) * 2
    val proj1 = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, outerExpr, literal2)
    // proj2 = (($0 + 1) + 1) * 3
    val proj2 = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, outerExpr, literal3)

    val duplicates = CseUtils.findDuplicateSubExpressions(Seq(proj1, proj2))

    // Both innerExpr and outerExpr appear in both projections
    assertThat(duplicates.contains(innerExpr.toString)).isTrue
    assertThat(duplicates.contains(outerExpr.toString)).isTrue
  }

  /** Tests with empty projection list. */
  @Test
  def testEmptyProjections(): Unit = {
    val duplicates = CseUtils.findDuplicateSubExpressions(Seq.empty)
    assertThat(duplicates.isEmpty).isTrue
  }

  /** Tests with a single projection (no possible duplicates). */
  @Test
  def testSingleProjection(): Unit = {
    val input0 = rexBuilder.makeInputRef(intType, 0)
    val input1 = rexBuilder.makeInputRef(intType, 1)
    val expr = rexBuilder.makeCall(SqlStdOperatorTable.PLUS, input0, input1)

    val duplicates = CseUtils.findDuplicateSubExpressions(Seq(expr))
    assertThat(duplicates.isEmpty).isTrue
  }
}
