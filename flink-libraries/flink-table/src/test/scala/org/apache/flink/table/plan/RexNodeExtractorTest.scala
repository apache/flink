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

package org.apache.flink.table.plan

import java.util.{List => JList}
import java.math.BigDecimal
import java.sql.{Date, Time, Timestamp}

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.SqlPostfixOperator
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.`type`.SqlTypeName.{BIGINT, INTEGER, VARCHAR}
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.util.{DateString, TimeString, TimestampString}
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.expressions._
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.plan.util.RexNodeExtractor
import org.apache.flink.table.util.InputTypeBuilder.inputOf
import org.apache.flink.table.validate.{BuiltInFunctionCatalog, FunctionCatalog}
import org.apache.flink.util.TimeConvertUtils
import org.hamcrest.CoreMatchers.is
import org.junit.Assert.{assertArrayEquals, assertEquals, assertThat}
import org.junit.Test

import scala.collection.JavaConverters._

class RexNodeExtractorTest extends RexNodeTestBase {

  private val functionCatalog: FunctionCatalog = BuiltInFunctionCatalog.withBuiltIns

  @Test
  def testExtractRefInputFields(): Unit = {
    val usedFields = RexNodeExtractor.extractRefInputFields(buildExprs())
    assertArrayEquals(usedFields, Array(2, 3, 1))
  }

  @Test
  def testExtractSimpleCondition(): Unit = {
    val builder: RexBuilder = new RexBuilder(typeFactory)
    val expr = buildConditionExpr()

    val firstExp = ExpressionParser.parseExpression("id > 6")
    val secondExp = ExpressionParser.parseExpression("amount * price < 100")
    val expected: Array[Expression] = Array(firstExp, secondExp)

    val (convertedExpressions, unconvertedRexNodes) =
      RexNodeExtractor.extractConjunctiveConditions(
        expr,
        -1,
        allFieldNames,
        builder,
        functionCatalog)

    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testExtractSingleCondition(): Unit = {
    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // id
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)

    // a = amount >= id
    val a = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, t0, t1)

    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexNodeExtractor.extractConjunctiveConditions(
        a,
        -1,
        allFieldNames,
        relBuilder,
        functionCatalog)

    val expected: Array[Expression] = Array(ExpressionParser.parseExpression("amount >= id"))
    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  // ((a AND b) OR c) AND (NOT d) => (a OR c) AND (b OR c) AND (NOT d)
  @Test
  def testExtractCnfCondition(): Unit = {
    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // id
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)
    // price
    val t2 = rexBuilder.makeInputRef(allFieldTypes.get(3), 3)
    // 100
    val t3 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))
    // 200
    val t4 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(200L))

    // a = amount < 100
    val a = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, t0, t3)
    // b = id > 100
    val b = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t1, t3)
    // c = price == 100
    val c = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, t2, t3)
    // d = amount <= id
    val d = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, t0, t1)
    // e = price == 200
    val e = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, t2, t4)

    // a AND b
    val and = rexBuilder.makeCall(SqlStdOperatorTable.AND, List(a, b).asJava)
    // (a AND b) OR c OR e
    val or = rexBuilder.makeCall(SqlStdOperatorTable.OR, List(and, c, e).asJava)
    // NOT d
    val not = rexBuilder.makeCall(SqlStdOperatorTable.NOT, List(d).asJava)

    // (a AND b) OR c OR e) AND (NOT d)
    val complexNode = rexBuilder.makeCall(SqlStdOperatorTable.AND, List(or, not).asJava)

    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexNodeExtractor.extractConjunctiveConditions(
        complexNode,
        -1,
        allFieldNames,
        relBuilder,
        functionCatalog)

    val expected: Array[Expression] = Array(
      ExpressionParser.parseExpression("amount < 100 || price == 100 || price == 200"),
      ExpressionParser.parseExpression("id > 100 || price == 100 || price == 200"),
      ExpressionParser.parseExpression("!(amount <= id)"))
    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testExtractANDExpressions(): Unit = {
    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // id
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)
    // price
    val t2 = rexBuilder.makeInputRef(allFieldTypes.get(3), 3)
    // 100
    val t3 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))

    // a = amount < 100
    val a = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, t0, t3)
    // b = id > 100
    val b = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t1, t3)
    // c = price == 100
    val c = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, t2, t3)
    // d = amount <= id
    val d = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, t0, t1)

    // a AND b AND c AND d
    val and = rexBuilder.makeCall(SqlStdOperatorTable.AND, List(a, b, c, d).asJava)

    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexNodeExtractor.extractConjunctiveConditions(
        and,
        -1,
        allFieldNames,
        relBuilder,
        functionCatalog)

    val expected: Array[Expression] = Array(
      ExpressionParser.parseExpression("amount < 100"),
      ExpressionParser.parseExpression("amount <= id"),
      ExpressionParser.parseExpression("id > 100"),
      ExpressionParser.parseExpression("price === 100")
    )

    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testNumericLiteralConversions(): Unit = {
    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // id
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)
    // price
    val t2 = rexBuilder.makeInputRef(allFieldTypes.get(3), 3)
    // 100
    val t3 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))
    // 200.1
    val t4 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(200.1))

    // a = amount < 100
    val a = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, t0, t3)
    // b = id > 200.1
    val b = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t1, t4)
    // c = price == 200.1
    val c = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, t2, t4)
    // d = amount <= id
    val d = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, t0, t1)

    // a AND b AND c AND d
    val and = rexBuilder.makeCall(SqlStdOperatorTable.AND, List(a, b, c, d).asJava)

    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexNodeExtractor.extractConjunctiveConditions(
        and,
        -1,
        allFieldNames,
        relBuilder,
        functionCatalog)

    val expected: Array[Expression] = Array(
      ExpressionParser.parseExpression("amount < 100"),
      ExpressionParser.parseExpression("amount <= id"),
      ExpressionParser.parseExpression("id > 200.1"),
      ExpressionParser.parseExpression("price === 200.1")
    )

    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testTimeLiteralConversions(): Unit = {
    val fieldNames = List("timestamp_col", "date_col", "time_col").asJava
    val fieldTypes = makeTypes(SqlTypeName.TIMESTAMP, SqlTypeName.DATE, SqlTypeName.TIME)

    val timestampString = new TimestampString("2017-09-10 14:23:01")
    val rexTimestamp = rexBuilder.makeTimestampLiteral(timestampString, 0)
    val rexDate = rexBuilder.makeDateLiteral(new DateString("2017-09-12"))
    val rexTime = rexBuilder.makeTimeLiteral(new TimeString("14:23:01"), 0)

    val allRexNodes = List(rexTimestamp, rexDate, rexTime)

    val condition = fieldTypes.asScala.zipWithIndex
        .map((t: (RelDataType, Int)) => rexBuilder.makeInputRef(t._1, t._2))
        .zip(allRexNodes)
        .map(t => rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, t._1, t._2))
        .asJava

    val and = rexBuilder.makeCall(SqlStdOperatorTable.AND, condition)

    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (converted, _) = RexNodeExtractor.extractConjunctiveConditions(
      and,
      -1,
      fieldNames,
      relBuilder,
      functionCatalog)

    val expected = Array[Expression](
      EqualTo(
        UnresolvedFieldReference("timestamp_col"),
        Literal(new Timestamp(TimeConvertUtils.timestampStringToUnixDate("2017-09-10 14:23:01")))
      ),
      EqualTo(
        UnresolvedFieldReference("date_col"),
        Literal(new Date(
          TimeConvertUtils.dateStringToUnixDate("2017-09-12") * TimeConvertUtils.MILLIS_PER_DAY))
      ),
      EqualTo(
        UnresolvedFieldReference("time_col"),
        Literal(new Time(TimeConvertUtils.timeStringToUnixDate("14:23:01")))
      )
    )

    assertExpressionArrayEquals(expected, converted)
  }

  @Test
  def testExtractArithmeticConditions(): Unit = {
    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // id
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)
    // 100
    val t2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))

    val condition = List(
      // amount < id
      rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, t0, t1),
      // amount <= id
      rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, t0, t1),
      // amount <> id
      rexBuilder.makeCall(SqlStdOperatorTable.NOT_EQUALS, t0, t1),
      // amount == id
      rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, t0, t1),
      // amount >= id
      rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, t0, t1),
      // amount > id
      rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t0, t1),
      // amount + id == 100
      rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.PLUS, t0, t1), t2),
      // amount - id == 100
      rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.MINUS, t0, t1), t2),
      // amount * id == 100
      rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, t0, t1), t2),
      // amount / id == 100
      rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(ScalarSqlFunctions.DIVIDE, t0, t1), t2),
      // -amount == 100
      rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, t0), t2)
    ).asJava

    val complexExpr = rexBuilder.makeCall(SqlStdOperatorTable.AND, condition)
    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexNodeExtractor.extractConjunctiveConditions(
        complexExpr,
        -1,
        allFieldNames,
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
    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // id
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)
    // 100
    val t2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))

    // sum(amount) > 100
    val condition1 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN,
        rexBuilder.makeCall(SqlStdOperatorTable.SUM, t0), t2)

    // min(id) == 100
    val condition2 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.MIN, t1), t2)

    val complexExpr = rexBuilder.makeCall(SqlStdOperatorTable.AND, condition1, condition2)

    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) = RexNodeExtractor.extractConjunctiveConditions(
        complexExpr,
        -1,
        allFieldNames,
        relBuilder,
        functionCatalog)

    val expected: Array[Expression] = Array(
      GreaterThan(Sum(UnresolvedFieldReference("amount")), Literal(100)),
      EqualTo(Min(UnresolvedFieldReference("id")), Literal(100))
    )
    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testExtractWithUnsupportedConditions(): Unit = {
    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // id
    val t1 = rexBuilder.makeInputRef(allFieldTypes.get(1), 1)
    // 100
    val t2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))

    // unsupported now: amount.cast(BigInteger)
    val cast = rexBuilder.makeCast(allFieldTypes.get(1), t0)

    // unsupported now: amount.cast(BigInteger) > 100
    val condition1 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, cast, t2)

    // amount <= id
    val condition2 = rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN_OR_EQUAL, t0, t1)

    // contains unsupported condition: (amount.cast(BigInteger) > 100 OR amount <= id)
    val condition3 = rexBuilder.makeCall(SqlStdOperatorTable.OR, condition1, condition2)

    // only condition2 can be translated
    val conditionExpr = rexBuilder.makeCall(
      SqlStdOperatorTable.AND,
      condition1,
      condition2,
      condition3)

    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexNodeExtractor.extractConjunctiveConditions(
        conditionExpr,
        -1,
        allFieldNames,
        relBuilder,
        functionCatalog)


    assertEquals(3, convertedExpressions.length)
    assertEquals("'amount.cast(LongType) > 100", convertedExpressions(0).toString)
    assertEquals("'amount <= 'id", convertedExpressions(1).toString)
    assertEquals("'amount.cast(LongType) > 100 || 'amount <= 'id",
                 convertedExpressions(2).toString)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testExtractRefNestedInputFields(): Unit = {
    val rexProgram = buildExprsWithNesting()

    val usedFields = RexNodeExtractor.extractRefInputFields(rexProgram)
    val usedNestedFields = RexNodeExtractor.extractRefNestedInputFields(rexProgram, usedFields)

    val expected = Array(Array("amount"), Array("*"))
    assertThat(usedNestedFields, is(expected))
  }

  @Test
  def testExtractRefNestedInputFieldsWithNoNesting(): Unit = {
    val exprs = buildExprs()

    val usedFields = RexNodeExtractor.extractRefInputFields(exprs)
    val usedNestedFields = RexNodeExtractor.extractRefNestedInputFields(exprs, usedFields)

    val expected = Array(Array("*"), Array("*"), Array("*"))
    assertThat(usedNestedFields, is(expected))
  }

  @Test
  def testExtractDeepRefNestedInputFields(): Unit = {
    val rexProgram = buildExprsWithDeepNesting()

    val usedFields = RexNodeExtractor.extractRefInputFields(rexProgram)
    val usedNestedFields = RexNodeExtractor.extractRefNestedInputFields(rexProgram, usedFields)

    val expected = Array(
      Array("amount"),
      Array("*"),
      Array("with.deeper.entry", "with.deep.entry"))

    assertThat(usedFields, is(Array(1, 0, 2)))
    assertThat(usedNestedFields, is(expected))
  }

  private def buildExprsWithDeepNesting(): JList[RexNode] = {

    // person input
    val passportRow = inputOf(typeFactory)
      .field("id", VARCHAR)
      .field("status", VARCHAR)
      .build

    val personRow = inputOf(typeFactory)
      .field("name", VARCHAR)
      .field("age", INTEGER)
      .nestedField("passport", passportRow)
      .build

    // payment input
    val paymentRow = inputOf(typeFactory)
      .field("id", BIGINT)
      .field("amount", INTEGER)
      .build

    // deep field input
    val deepRowType = inputOf(typeFactory)
      .field("entry", VARCHAR)
      .build

    val entryRowType = inputOf(typeFactory)
      .nestedField("inside", deepRowType)
      .build

    val deeperRowType = inputOf(typeFactory)
      .nestedField("entry", entryRowType)
      .build

    val withRowType = inputOf(typeFactory)
      .nestedField("deep", deepRowType)
      .nestedField("deeper", deeperRowType)
      .build

    val fieldRowType = inputOf(typeFactory)
      .nestedField("with", withRowType)
      .build

    // inputRowType
    //
    // [ persons:  [ name: VARCHAR, age:  INT, passport: [id: VARCHAR, status: VARCHAR ] ],
    //   payments: [ id: BIGINT, amount: INT ],
    //   field:    [ with: [ deep: [ entry: VARCHAR ],
    //                       deeper: [ entry: [ inside: [entry: VARCHAR ] ] ]
    //             ] ]
    // ]

    val t0 = rexBuilder.makeInputRef(personRow, 0)
    val t1 = rexBuilder.makeInputRef(paymentRow, 1)
    val t2 = rexBuilder.makeInputRef(fieldRowType, 2)
    val t3 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(10L))

    // person
    val person$pass = rexBuilder.makeFieldAccess(t0, "passport", false)
    val person$pass$stat = rexBuilder.makeFieldAccess(person$pass, "status", false)

    // payment
    val pay$amount = rexBuilder.makeFieldAccess(t1, "amount", false)
    val multiplyAmount = rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, pay$amount, t3)

    // field
    val field$with = rexBuilder.makeFieldAccess(t2, "with", false)
    val field$with$deep = rexBuilder.makeFieldAccess(field$with, "deep", false)
    val field$with$deeper = rexBuilder.makeFieldAccess(field$with, "deeper", false)
    val field$with$deep$entry = rexBuilder.makeFieldAccess(field$with$deep, "entry", false)
    val field$with$deeper$entry = rexBuilder.makeFieldAccess(field$with$deeper, "entry", false)
    val field$with$deeper$entry$inside = rexBuilder
      .makeFieldAccess(field$with$deeper$entry, "inside", false)
    val field$with$deeper$entry$inside$entry = rexBuilder
      .makeFieldAccess(field$with$deeper$entry$inside, "entry", false)

    // Program
    // (
    //   payments.amount * 10),
    //   persons.passport.status,
    //   field.with.deep.entry
    //   field.with.deeper.entry.inside.entry
    //   field.with.deeper.entry
    //   persons
    // )
    List(multiplyAmount, person$pass$stat, field$with$deep$entry,
      field$with$deeper$entry$inside$entry, field$with$deeper$entry, t0).asJava

  }

  private def buildExprsWithNesting(): JList[RexNode] = {

    val personRow = inputOf(typeFactory)
      .field("name", INTEGER)
      .field("age", VARCHAR)
      .build

    val paymentRow = inputOf(typeFactory)
      .field("id", BIGINT)
      .field("amount", INTEGER)
      .build

    val types = List(personRow, paymentRow).asJava

    val t0 = rexBuilder.makeInputRef(types.get(0), 0)
    val t1 = rexBuilder.makeInputRef(types.get(1), 1)
    val t2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))

    val payment$amount = rexBuilder.makeFieldAccess(t1, "amount", false)

    List(payment$amount, t0, t2).asJava
  }

  private def testExtractSinglePostfixCondition(
      fieldIndex: Integer,
      op: SqlPostfixOperator,
      expr: String) : Unit = {
    rexBuilder = new RexBuilder(typeFactory)

    // flag
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(fieldIndex), fieldIndex)
    val conditionExpr = rexBuilder.makeCall(op, t0)
    val relBuilder: RexBuilder = new RexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      RexNodeExtractor.extractConjunctiveConditions(
        conditionExpr,
        -1,
        allFieldNames,
        relBuilder,
        functionCatalog)

    assertEquals(1, convertedExpressions.length)
    assertEquals(expr, convertedExpressions.head.toString)
    assertEquals(0, unconvertedRexNodes.length)
  }

  private def assertExpressionArrayEquals(
      expected: Array[Expression],
      actual: Array[Expression]): Unit = {
    val sortedExpected = expected.sortBy(e => e.toString)
    val sortedActual = actual.sortBy(e => e.toString)

    assertEquals(sortedExpected.length, sortedActual.length)
    sortedExpected.zip(sortedActual).foreach {
      case (l, r) => assertEquals(l.toString, r.toString)
    }
  }
}
