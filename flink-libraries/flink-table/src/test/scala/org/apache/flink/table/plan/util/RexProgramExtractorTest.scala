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

package org.apache.flink.table.plan.util

import java.math.BigDecimal

import org.apache.calcite.rex.{RexBuilder, RexProgramBuilder}
import org.apache.calcite.sql.SqlPostfixOperator
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.flink.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.table.validate.FunctionCatalog
import org.junit.Assert.{assertArrayEquals, assertEquals}
import org.junit.Test

import scala.collection.JavaConverters._

class RexProgramExtractorTest extends RexProgramTestBase {

  private val functionCatalog: FunctionCatalog = FunctionCatalog.withBuiltIns

  @Test
  def testExtractRefInputFields(): Unit = {
    val usedFields = RexProgramExtractor.extractRefInputFields(buildSimpleRexProgram())
    assertArrayEquals(usedFields, Array(2, 3, 1))
  }

  @Test
  def testExtractSimpleCondition(): Unit = {
    val builder: RexBuilder = new RexBuilder(typeFactory)
    val program = buildSimpleRexProgram()

    val firstExp = ExpressionParser.parseExpression("id > 6")
    val secondExp = ExpressionParser.parseExpression("amount * price < 100")
    val expected: Array[Expression] = Array(firstExp, secondExp)

    val (convertedExpressions, unconvertedRexNodes) =
      RexProgramExtractor.extractConjunctiveConditions(
        program,
        builder,
        functionCatalog)

    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testExtractSingleCondition(): Unit = {
    val inputRowType = typeFactory.createStructType(allFieldTypes, allFieldNames)
    val builder = new RexProgramBuilder(inputRowType, rexBuilder)

    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // id
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)

    // a = amount >= id
    val a = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, t0, t1))
    builder.addCondition(a)

    val program = builder.getProgram
    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexProgramExtractor.extractConjunctiveConditions(
        program,
        relBuilder,
        functionCatalog)

    val expected: Array[Expression] = Array(ExpressionParser.parseExpression("amount >= id"))
    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  // ((a AND b) OR c) AND (NOT d) => (a OR c) AND (b OR c) AND (NOT d)
  @Test
  def testExtractCnfCondition(): Unit = {
    val inputRowType = typeFactory.createStructType(allFieldTypes, allFieldNames)
    val builder = new RexProgramBuilder(inputRowType, rexBuilder)

    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // id
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)
    // price
    val t2 = rexBuilder.makeInputRef(allFieldTypes.get(3), 3)
    // 100
    val t3 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))

    // a = amount < 100
    val a = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, t0, t3))
    // b = id > 100
    val b = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t1, t3))
    // c = price == 100
    val c = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, t2, t3))
    // d = amount <= id
    val d = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, t0, t1))

    // a AND b
    val and = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.AND, List(a, b).asJava))
    // (a AND b) or c
    val or = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.OR, List(and, c).asJava))
    // not d
    val not = builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.NOT, List(d).asJava))

    // (a AND b) OR c) AND (NOT d)
    builder.addCondition(builder.addExpr(
      rexBuilder.makeCall(SqlStdOperatorTable.AND, List(or, not).asJava)))

    val program = builder.getProgram
    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexProgramExtractor.extractConjunctiveConditions(
        program,
        relBuilder,
        functionCatalog)

    val expected: Array[Expression] = Array(
      ExpressionParser.parseExpression("amount < 100 || price == 100"),
      ExpressionParser.parseExpression("id > 100 || price == 100"),
      ExpressionParser.parseExpression("!(amount <= id)"))
    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testExtractArithmeticConditions(): Unit = {
    val inputRowType = typeFactory.createStructType(allFieldTypes, allFieldNames)
    val builder = new RexProgramBuilder(inputRowType, rexBuilder)

    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // id
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)
    // 100
    val t2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))

    val condition = List(
      // amount < id
      builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, t0, t1)),
      // amount <= id
      builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, t0, t1)),
      // amount <> id
      builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, t0, t1)),
      // amount == id
      builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, t0, t1)),
      // amount >= id
      builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, t0, t1)),
      // amount > id
      builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t0, t1)),
      // amount + id == 100
      builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.PLUS, t0, t1), t2)),
      // amount - id == 100
      builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.MINUS, t0, t1), t2)),
      // amount * id == 100
      builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, t0, t1), t2)),
      // amount / id == 100
      builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.DIVIDE, t0, t1), t2)),
      // -amount == 100
      builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, t0), t2))
    ).asJava

    builder.addCondition(builder.addExpr(rexBuilder.makeCall(SqlStdOperatorTable.AND, condition)))
    val program = builder.getProgram
    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexProgramExtractor.extractConjunctiveConditions(
        program,
        relBuilder,
        functionCatalog)

    val expected: Array[Expression] = Array(
      ExpressionParser.parseExpression("amount < id"),
      ExpressionParser.parseExpression("amount <= id"),
      ExpressionParser.parseExpression("amount <> id"),
      ExpressionParser.parseExpression("amount == id"),
      ExpressionParser.parseExpression("amount >= id"),
      ExpressionParser.parseExpression("amount > id"),
      ExpressionParser.parseExpression("amount + id == 100"),
      ExpressionParser.parseExpression("amount - id == 100"),
      ExpressionParser.parseExpression("amount * id == 100"),
      ExpressionParser.parseExpression("amount / id == 100"),
      ExpressionParser.parseExpression("-amount == 100")
    )
    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testExtractPostfixConditions(): Unit = {
    testExtractSinglePostfixCondition(4, SqlStdOperatorTable.IS_NULL, "('flag).isNull")
    // IS_NOT_NULL will be eliminated since flag is not nullable
    // testExtractSinglePostfixCondition(SqlStdOperatorTable.IS_NOT_NULL, "('flag).isNotNull")
    testExtractSinglePostfixCondition(4, SqlStdOperatorTable.IS_TRUE, "('flag).isTrue")
    testExtractSinglePostfixCondition(4, SqlStdOperatorTable.IS_NOT_TRUE, "('flag).isNotTrue")
    testExtractSinglePostfixCondition(4, SqlStdOperatorTable.IS_FALSE, "('flag).isFalse")
    testExtractSinglePostfixCondition(4, SqlStdOperatorTable.IS_NOT_FALSE, "('flag).isNotFalse")
  }

  @Test
  def testExtractConditionWithFunctionCalls(): Unit = {
    val inputRowType = typeFactory.createStructType(allFieldTypes, allFieldNames)
    val builder = new RexProgramBuilder(inputRowType, rexBuilder)

    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // id
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)
    // 100
    val t2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))

    // sum(amount) > 100
    val condition1 = builder.addExpr(
      rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
        rexBuilder.makeCall(SqlStdOperatorTable.SUM, t0), t2))

    // min(id) == 100
    val condition2 = builder.addExpr(
      rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.MIN, t1), t2))

    builder.addCondition(builder.addExpr(
      rexBuilder.makeCall(SqlStdOperatorTable.AND, condition1, condition2)))

    val program = builder.getProgram
    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexProgramExtractor.extractConjunctiveConditions(
        program,
        relBuilder,
        functionCatalog)

    val expected: Array[Expression] = Array(
      ExpressionParser.parseExpression("sum(amount) > 100"),
      ExpressionParser.parseExpression("min(id) == 100")
    )
    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testExtractWithUnsupportedConditions(): Unit = {
    val inputRowType = typeFactory.createStructType(allFieldTypes, allFieldNames)
    val builder = new RexProgramBuilder(inputRowType, rexBuilder)

    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // id
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)
    // 100
    val t2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))

    // unsupported now: amount.cast(BigInteger)
    val cast = builder.addExpr(rexBuilder.makeCast(allFieldTypes.get(1), t0))

    // unsupported now: amount.cast(BigInteger) > 100
    val condition1 = builder.addExpr(
      rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, cast, t2))

    // amount <= id
    val condition2 = builder.addExpr(
      rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, t0, t1))

    // contains unsupported condition: (amount.cast(BigInteger) > 100 OR amount <= id)
    val condition3 = builder.addExpr(
      rexBuilder.makeCall(SqlStdOperatorTable.OR, condition1, condition2))

    // only condition2 can be translated
    builder.addCondition(
      rexBuilder.makeCall(SqlStdOperatorTable.AND, condition1, condition2, condition3))

    val program = builder.getProgram
    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexProgramExtractor.extractConjunctiveConditions(
        program,
        relBuilder,
        functionCatalog)

    val expected: Array[Expression] = Array(
      ExpressionParser.parseExpression("amount <= id")
    )
    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(2, unconvertedRexNodes.length)
    assertEquals(">(CAST($2):BIGINT NOT NULL, 100)", unconvertedRexNodes(0).toString)
    assertEquals("OR(>(CAST($2):BIGINT NOT NULL, 100), <=($2, $1))",
      unconvertedRexNodes(1).toString)
  }

  private def testExtractSinglePostfixCondition(
      fieldIndex: Integer,
      op: SqlPostfixOperator,
      expr: String) : Unit = {

    val inputRowType = typeFactory.createStructType(allFieldTypes, allFieldNames)
    val builder = new RexProgramBuilder(inputRowType, rexBuilder)
    rexBuilder = new RexBuilder(typeFactory)

    // flag
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(fieldIndex), fieldIndex)
    builder.addCondition(builder.addExpr(rexBuilder.makeCall(op, t0)))

    val program = builder.getProgram(false)
    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexProgramExtractor.extractConjunctiveConditions(
        program,
        relBuilder,
        functionCatalog)

    assertEquals(1, convertedExpressions.length)
    assertEquals(expr, convertedExpressions.head.toString)
    assertEquals(0, unconvertedRexNodes.length)
  }

  private def assertExpressionArrayEquals(
      expected: Array[Expression],
      actual: Array[Expression]) = {
    val sortedExpected = expected.sortBy(e => e.toString)
    val sortedActual = actual.sortBy(e => e.toString)

    assertEquals(sortedExpected.length, sortedActual.length)
    sortedExpected.zip(sortedActual).foreach {
      case (l, r) => assertEquals(l.toString, r.toString)
    }
  }
}
