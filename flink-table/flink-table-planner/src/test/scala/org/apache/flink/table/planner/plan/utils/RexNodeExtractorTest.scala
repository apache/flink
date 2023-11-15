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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.api.common.typeinfo.Types
import org.apache.flink.table.api._
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.expressions.ApiExpressionUtils.{unresolvedCall, unresolvedRef, valueLiteral}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.functions.{AggregateFunctionDefinition, FunctionIdentifier}
import org.apache.flink.table.functions.BuiltInFunctionDefinitions.{EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQUAL}
import org.apache.flink.table.module.ModuleManager
import org.apache.flink.table.planner.calcite.FlinkRexBuilder
import org.apache.flink.table.planner.expressions.utils.Func1
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.functions.utils.ScalarSqlFunction
import org.apache.flink.table.planner.utils.{DateTimeTestUtil, IntSumAggFunction}
import org.apache.flink.table.resource.ResourceManager
import org.apache.flink.table.utils.CatalogManagerMocks

import org.apache.calcite.avatica.util.ByteString
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.sql.`type`.SqlTypeName
import org.apache.calcite.sql.SqlPostfixOperator
import org.apache.calcite.sql.fun.{SqlStdOperatorTable, SqlTrimFunction}
import org.apache.calcite.util.{DateString, TimestampString, TimeString}
import org.hamcrest.CoreMatchers.is
import org.junit.Assert.{assertArrayEquals, assertEquals, assertThat, assertTrue}
import org.junit.Test

import java.math.BigDecimal
import java.net.URL
import java.time.ZoneId
import java.util.{Arrays, List => JList, TimeZone}

import scala.collection.JavaConverters._

/** Test for [[RexNodeExtractor]]. */
class RexNodeExtractorTest extends RexNodeTestBase {
  val tableConfig = TableConfig.getDefault
  val resourceManager = ResourceManager.createResourceManager(
    new Array[URL](0),
    Thread.currentThread.getContextClassLoader,
    tableConfig.getConfiguration)
  val catalogManager: CatalogManager = CatalogManagerMocks.createEmptyCatalogManager()
  val moduleManager = new ModuleManager
  private val functionCatalog =
    new FunctionCatalog(tableConfig, resourceManager, catalogManager, moduleManager)

  @Test
  def testExtractRefInputFields(): Unit = {
    val (exprs, _) = buildExprs()
    val usedFields = RexNodeExtractor.extractRefInputFields(exprs)
    assertArrayEquals(usedFields, Array(2, 3, 1))
  }

  @Test
  def testExtractRefNestedInputFields(): Unit = {
    val (rexProgram, _) = buildExprsWithNesting()

    val usedFields = RexNodeExtractor.extractRefInputFields(rexProgram)
    val usedNestedFields = RexNodeExtractor.extractRefNestedInputFields(rexProgram, usedFields)

    val expected = Array(Array(Arrays.asList("amount")), Array(Arrays.asList("*")))
    assertThat(usedNestedFields, is(expected))
  }

  @Test
  def testExtractRefNestedInputFieldsWithNoNesting(): Unit = {
    val (exprs, _) = buildExprs()

    val usedFields = RexNodeExtractor.extractRefInputFields(exprs)
    val usedNestedFields = RexNodeExtractor.extractRefNestedInputFields(exprs, usedFields)

    val expected =
      Array(Array(Arrays.asList("*")), Array(Arrays.asList("*")), Array(Arrays.asList("*")))
    assertThat(usedNestedFields, is(expected))
  }

  @Test
  def testExtractDeepRefNestedInputFields(): Unit = {
    val (rexProgram, _) = buildExprsWithDeepNesting()

    val usedFields = RexNodeExtractor.extractRefInputFields(rexProgram)
    val usedNestedFields = RexNodeExtractor.extractRefNestedInputFields(rexProgram, usedFields)

    val expected = Array(
      Array(Arrays.asList("amount")),
      Array(Arrays.asList("*")),
      Array(Arrays.asList("with", "deeper", "entry"), Arrays.asList("with", "deep", "entry")),
      Array(Arrays.asList("outer"), Arrays.asList("inner", "deep_array"))
    )

    assertThat(usedFields, is(Array(1, 0, 2, 3)))
    assertThat(usedNestedFields, is(expected))
  }

  @Test
  def testExtractSimpleCondition(): Unit = {
    val builder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val expr = buildConditionExpr()

    val firstExp = $"id" > 6
    val secondExp = $"amount" * $"price" < 100
    val expected: Array[Expression] = Array(firstExp, secondExp)

    val (convertedExpressions, unconvertedRexNodes) =
      extractConjunctiveConditions(expr, -1, allFieldNames, builder, functionCatalog)

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

    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      extractConjunctiveConditions(a, -1, allFieldNames, relBuilder, functionCatalog)

    val expected: Array[Expression] = Array($"amount" >= $"id")
    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testExtractConditionWithBinaryLiteral(): Unit = {
    // blob
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(5), 5)

    // X'616263'
    val t1 = rexBuilder.makeBinaryLiteral(ByteString.of("616263", 16))

    // blob = X'616263'
    val a = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, t0, t1)

    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      extractConjunctiveConditions(a, -1, allFieldNames, relBuilder, functionCatalog)

    val expected: Array[Expression] = Array($"blob" === Array[Byte](97, 98, 99))
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

    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      extractConjunctiveConditions(complexNode, -1, allFieldNames, relBuilder, functionCatalog)

    val expected: Array[Expression] = Array(
      $"amount" < 100 || $"price" === 100 || $"price" === 200,
      $"id" > 100 || $"price" === 100 || $"price" === 200,
      !($"amount" <= $"id")
    )
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

    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      extractConjunctiveConditions(and, -1, allFieldNames, relBuilder, functionCatalog)

    val expected: Array[Expression] = Array(
      $"amount" < 100,
      $"amount" <= $"id",
      $"id" > 100,
      $"price" === 100
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

    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      extractConjunctiveConditions(and, -1, allFieldNames, relBuilder, functionCatalog)

    val expected: Array[Expression] = Array(
      // amount < 100
      unresolvedCall(LESS_THAN, unresolvedRef("amount"), valueLiteral(100)),
      // amount <= id
      unresolvedCall(LESS_THAN_OR_EQUAL, unresolvedRef("amount"), unresolvedRef("id")),
      // id > 200.1
      unresolvedCall(GREATER_THAN, unresolvedRef("id"), valueLiteral(200.1)),
      // price === 200.1
      unresolvedCall(EQUALS, unresolvedRef("price"), valueLiteral(200.1))
    )

    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testTimeLiteralConversions(): Unit = {
    val fieldNames = List("timestamp_col", "date_col", "time_col").asJava
    val fieldTypes = makeTypes(SqlTypeName.TIMESTAMP, SqlTypeName.DATE, SqlTypeName.TIME)

    val timestampString = new TimestampString("2017-09-10 14:23:01")
    val rexTimestamp = rexBuilder.makeTimestampLiteral(timestampString, 3)
    val rexDate = rexBuilder.makeDateLiteral(new DateString("2017-09-12"))
    val rexTime = rexBuilder.makeTimeLiteral(new TimeString("14:23:01"), 0)

    val allRexNodes = List(rexTimestamp, rexDate, rexTime)

    val condition = fieldTypes.asScala.zipWithIndex
      .map((t: (RelDataType, Int)) => rexBuilder.makeInputRef(t._1, t._2))
      .zip(allRexNodes)
      .map(t => rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, t._1, t._2))
      .asJava

    val and = rexBuilder.makeCall(SqlStdOperatorTable.AND, condition)

    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val (converted, _) =
      extractConjunctiveConditions(and, -1, fieldNames, relBuilder, functionCatalog)

    val datetime = DateTimeTestUtil.localDateTime("2017-09-10 14:23:01")
    val date = DateTimeTestUtil.localDate("2017-09-12")
    val time = DateTimeTestUtil.localTime("14:23:01")

    {
      val expected = Array[Expression](
        // timestamp_col = '2017-09-10 14:23:01'
        unresolvedCall(EQUALS, unresolvedRef("timestamp_col"), valueLiteral(datetime)),
        // date_col = '2017-09-12'
        unresolvedCall(EQUALS, unresolvedRef("date_col"), valueLiteral(date)),
        // time_col = '14:23:01'
        unresolvedCall(EQUALS, unresolvedRef("time_col"), valueLiteral(time))
      )

      assertExpressionArrayEquals(expected, converted)
    }
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
      rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.PLUS, t0, t1),
        t2),
      // amount - id == 100
      rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.MINUS, t0, t1),
        t2),
      // amount * id == 100
      rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(SqlStdOperatorTable.MULTIPLY, t0, t1),
        t2),
      // amount / id == 100
      rexBuilder.makeCall(
        SqlStdOperatorTable.EQUALS,
        rexBuilder.makeCall(FlinkSqlOperatorTable.DIVIDE, t0, t1),
        t2)
      // TODO supports this case
      // -amount == 100
      // rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
      //  rexBuilder.makeCall(SqlStdOperatorTable.UNARY_MINUS, t0), t2)
    ).asJava

    val complexExpr = rexBuilder.makeCall(SqlStdOperatorTable.AND, condition)
    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      extractConjunctiveConditions(complexExpr, -1, allFieldNames, relBuilder, functionCatalog)

    val expected: Array[Expression] = Array(
      $"amount" < $"id",
      $"amount" <= $"id",
      $"amount" !== $"id",
      $"amount" === $"id",
      $"amount" >= $"id",
      $"amount" > $"id",
      $"amount" + $"id" === 100,
      $"amount" - $"id" === 100,
      $"amount" * $"id" === 100,
      $"amount" / $"id" === 100
    )
    assertExpressionArrayEquals(expected, convertedExpressions)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testExtractPostfixConditions(): Unit = {
    testExtractSinglePostfixCondition(4, SqlStdOperatorTable.IS_NULL, "isNull(flag)")
    // IS_NOT_NULL will be eliminated since flag is not nullable
    // testExtractSinglePostfixCondition(4, SqlStdOperatorTable.IS_NOT_NULL, "('flag).isNotNull")
    testExtractSinglePostfixCondition(4, SqlStdOperatorTable.IS_TRUE, "isTrue(flag)")
    testExtractSinglePostfixCondition(4, SqlStdOperatorTable.IS_NOT_TRUE, "isNotTrue(flag)")
    testExtractSinglePostfixCondition(4, SqlStdOperatorTable.IS_FALSE, "isFalse(flag)")
    testExtractSinglePostfixCondition(4, SqlStdOperatorTable.IS_NOT_FALSE, "isNotFalse(flag)")
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
    val condition1 = rexBuilder.makeCall(
      SqlStdOperatorTable.GREATER_THAN,
      rexBuilder.makeCall(SqlStdOperatorTable.SUM, t0),
      t2)

    // first_value(id) == 100
    val condition2 = rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      rexBuilder.makeCall(FlinkSqlOperatorTable.FIRST_VALUE, t1),
      t2)

    val complexExpr = rexBuilder.makeCall(SqlStdOperatorTable.AND, condition1, condition2)

    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      extractConjunctiveConditions(complexExpr, -1, allFieldNames, relBuilder, functionCatalog)

    {
      val expected: Array[Expression] = Array(
        // sum(amount) > 100
        unresolvedCall(
          GREATER_THAN,
          unresolvedCall(
            new AggregateFunctionDefinition("sum", new IntSumAggFunction, Types.INT, Types.INT),
            unresolvedRef("amount")),
          valueLiteral(100))
      )
      assertExpressionArrayEquals(expected, convertedExpressions)
      assertEquals(1, unconvertedRexNodes.length)
    }
  }

  @Test
  def testExtractWithCast(): Unit = {
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
    val conditionExpr =
      rexBuilder.makeCall(SqlStdOperatorTable.AND, condition1, condition2, condition3)

    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      extractConjunctiveConditions(conditionExpr, -1, allFieldNames, relBuilder, functionCatalog)

    assertEquals(3, convertedExpressions.length)
    assertEquals("greaterThan(cast(amount, BIGINT), 100)", convertedExpressions(0).toString)
    assertEquals("lessThanOrEqual(amount, id)", convertedExpressions(1).toString)
    assertEquals(
      "or(greaterThan(cast(amount, BIGINT), 100), lessThanOrEqual(amount, id))",
      convertedExpressions(2).toString)
    assertEquals(0, unconvertedRexNodes.length)

    assertExpressionArrayEquals(Array($"amount" <= $"id"), Array(convertedExpressions(1)))
  }

  @Test
  def testExtractWithTrim(): Unit = {
    // name
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(0), 0)
    val t1 = rexBuilder.makeLiteral("He")

    // trim(BOTH, ' ', name) = 'He'
    val trimBoth = rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      rexBuilder.makeCall(
        SqlStdOperatorTable.TRIM,
        rexBuilder.makeFlag(SqlTrimFunction.Flag.BOTH),
        rexBuilder.makeLiteral(" "),
        t0),
      t1
    )

    // trim(LEADING, ' ', name) = 'He'
    val trimLeading = rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      rexBuilder.makeCall(
        SqlStdOperatorTable.TRIM,
        rexBuilder.makeFlag(SqlTrimFunction.Flag.LEADING),
        rexBuilder.makeLiteral(" "),
        t0),
      t1
    )

    // trim(TRAILING, ' ', name) = 'He'
    val trimTrailing = rexBuilder.makeCall(
      SqlStdOperatorTable.EQUALS,
      rexBuilder.makeCall(
        SqlStdOperatorTable.TRIM,
        rexBuilder.makeFlag(SqlTrimFunction.Flag.TRAILING),
        rexBuilder.makeLiteral(" "),
        t0),
      t1
    )

    val and =
      rexBuilder.makeCall(SqlStdOperatorTable.AND, List(trimBoth, trimLeading, trimTrailing).asJava)

    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      extractConjunctiveConditions(and, -1, allFieldNames, relBuilder, functionCatalog)

    assertEquals(0, convertedExpressions.length)
    assertEquals(3, unconvertedRexNodes.length)
  }

  @Test
  def testExtractWithUdf(): Unit = {
    functionCatalog.registerTempSystemScalarFunction("myUdf", Func1)
    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // my_udf(amount)
    val t1 = rexBuilder.makeCall(
      new ScalarSqlFunction(FunctionIdentifier.of("MyUdf"), "myUdf", Func1, typeFactory),
      t0)
    // 100
    val t2 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))
    // my_udf(amount) >  100
    val condition = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t1, t2)

    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      extractConjunctiveConditions(condition, -1, allFieldNames, relBuilder, functionCatalog)

    assertEquals(1, convertedExpressions.length)
    assertEquals("greaterThan(MyUdf(amount), 100)", convertedExpressions(0).toString)
    assertEquals(0, unconvertedRexNodes.length)
  }

  @Test
  def testExtractPartitionPredicates(): Unit = {
    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 2)
    // 100
    val t1 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))
    val c1 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t0, t1)
    // name
    val t2 = rexBuilder.makeInputRef(allFieldTypes.get(0), 0)
    // 'test%'
    val t3 = rexBuilder.makeLiteral("test%")
    val c2 = rexBuilder.makeCall(SqlStdOperatorTable.LIKE, t2, t3)
    // amount > 100 and name like 'test%'
    val c3 = rexBuilder.makeCall(SqlStdOperatorTable.AND, c1, c2)

    val (partitionPredicate1, nonPartitionPredicate1) =
      RexNodeExtractor.extractPartitionPredicates(
        c3,
        -1,
        allFieldNames.asScala.toArray,
        rexBuilder,
        Array("amount", "name")
      )
    assertEquals(c3, partitionPredicate1)
    assertTrue(nonPartitionPredicate1.isAlwaysTrue)

    val (partitionPredicate2, nonPartitionPredicate2) =
      RexNodeExtractor.extractPartitionPredicates(
        c3,
        -1,
        allFieldNames.asScala.toArray,
        rexBuilder,
        Array("amount")
      )
    assertEquals(c1, partitionPredicate2)
    assertEquals(c2, nonPartitionPredicate2)

    val (partitionPredicate3, nonPartitionPredicate3) =
      RexNodeExtractor.extractPartitionPredicates(
        c3,
        -1,
        allFieldNames.asScala.toArray,
        rexBuilder,
        Array("id")
      )
    assertTrue(partitionPredicate3.isAlwaysTrue)
    assertEquals(c3, nonPartitionPredicate3)

    // amount > 100 or name like 'test%'
    val c4 = rexBuilder.makeCall(SqlStdOperatorTable.OR, c1, c2)
    val (partitionPredicate4, nonPartitionPredicate4) =
      RexNodeExtractor.extractPartitionPredicates(
        c4,
        -1,
        allFieldNames.asScala.toArray,
        rexBuilder,
        Array("amount", "name")
      )
    assertEquals(c4, partitionPredicate4)
    assertTrue(nonPartitionPredicate4.isAlwaysTrue)

    val (partitionPredicate5, nonPartitionPredicate5) =
      RexNodeExtractor.extractPartitionPredicates(
        c4,
        -1,
        allFieldNames.asScala.toArray,
        rexBuilder,
        Array("amount")
      )
    assertTrue(partitionPredicate5.isAlwaysTrue)
    assertEquals(c4, nonPartitionPredicate5)
  }

  @Test
  def testExtractPartitionPredicatesDate(): Unit = {
    // amount
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(2), 1)
    // 100
    val t1 = rexBuilder.makeExactLiteral(BigDecimal.valueOf(100L))
    val c1 = rexBuilder.makeCall(SqlStdOperatorTable.GREATER_THAN, t0, t1)
    // date
    val t2 = rexBuilder.makeInputRef(
      typeFactory.createFieldTypeFromLogicalType(DataTypes.DATE().getLogicalType),
      0)
    // 2019-04-14
    val t3 = rexBuilder.makeDateLiteral(DateString.fromDaysSinceEpoch(18000))
    val c2 = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, t2, t3)
    // amount > 100 and date = 2019-04-14
    val c3 = rexBuilder.makeCall(SqlStdOperatorTable.AND, c1, c2)
    val (partitionPredicate1, nonPartitionPredicate1) =
      RexNodeExtractor.extractPartitionPredicates(
        c3,
        -1,
        Array("date", "amount", "id"),
        rexBuilder,
        Array("date")
      )
    assertEquals(c2, partitionPredicate1)
    assertEquals(c1, nonPartitionPredicate1)

    val (partitionPredicate2, nonPartitionPredicate2) =
      RexNodeExtractor.extractPartitionPredicates(
        c3,
        -1,
        Array("date", "amount", "id"),
        rexBuilder,
        Array("date", "amount")
      )
    assertEquals(c3, partitionPredicate2)
    assertTrue(nonPartitionPredicate2.isAlwaysTrue)

    val (partitionPredicate3, nonPartitionPredicate3) =
      RexNodeExtractor.extractPartitionPredicates(
        c3,
        -1,
        Array("date", "amount", "id"),
        rexBuilder,
        Array("id", "amount")
      )
    assertEquals(c1, partitionPredicate3)
    assertEquals(c2, nonPartitionPredicate3)
  }

  @Test
  def testTimeLiteralWithLocalTimeZoneConversions(): Unit = {
    // TIMESTAMP(6), TIMESTAMP(6) WITH LOCAL TIME ZONE
    val fieldNames = List("timestamp_col", "instant_col").asJava
    val fieldTypes = makeTypes(SqlTypeName.TIMESTAMP, SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)

    val timestampString = new TimestampString("2017-09-10 14:23:01.123456")
    val rexTimestamp = rexBuilder.makeTimestampLiteral(timestampString, 6)
    val rexInstant = rexBuilder.makeTimestampWithLocalTimeZoneLiteral(timestampString, 6)

    val allRexNodes = List(rexTimestamp, rexInstant)

    val condition = fieldTypes.asScala.zipWithIndex
      .map((t: (RelDataType, Int)) => rexBuilder.makeInputRef(t._1, t._2))
      .zip(allRexNodes)
      .map(t => rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, t._1, t._2))
      .asJava

    val and = rexBuilder.makeCall(SqlStdOperatorTable.AND, condition)

    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)

    val shanghai = ZoneId.of("Asia/Shanghai")
    val (converted, _) = extractConjunctiveConditions(
      and,
      -1,
      fieldNames,
      relBuilder,
      functionCatalog,
      TimeZone.getTimeZone(shanghai))

    val datetime = DateTimeTestUtil.localDateTime("2017-09-10 14:23:01.123456")
    val instant = datetime.toInstant(shanghai.getRules.getOffset(datetime))

    {
      val expected = Array[Expression](
        // timestamp_col = '2017-09-10 14:23:01.123456'
        unresolvedCall(EQUALS, unresolvedRef("timestamp_col"), valueLiteral(datetime)),
        // instant_col = '2017-09-10T06:23:01.123456Z'
        unresolvedCall(EQUALS, unresolvedRef("instant_col"), valueLiteral(instant))
      )

      assertExpressionArrayEquals(expected, converted)
    }
  }

  private def testExtractSinglePostfixCondition(
      fieldIndex: Integer,
      op: SqlPostfixOperator,
      expr: String): Unit = {
    rexBuilder = new FlinkRexBuilder(typeFactory)

    // flag
    val t0 = rexBuilder.makeInputRef(allFieldTypes.get(fieldIndex), fieldIndex)
    val conditionExpr = rexBuilder.makeCall(op, t0)
    val relBuilder: RexBuilder = new FlinkRexBuilder(typeFactory)
    val (convertedExpressions, unconvertedRexNodes) =
      extractConjunctiveConditions(conditionExpr, -1, allFieldNames, relBuilder, functionCatalog)

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
    sortedExpected.zip(sortedActual).foreach { case (l, r) => assertEquals(l.toString, r.toString) }
  }

  private def extractConjunctiveConditions(
      expr: RexNode,
      maxCnfNodeCount: Int,
      inputFieldNames: JList[String],
      rexBuilder: RexBuilder,
      catalog: FunctionCatalog,
      tz: TimeZone = TimeZone.getDefault): (Array[Expression], Array[RexNode]) = {
    RexNodeExtractor.extractConjunctiveConditions(
      expr,
      maxCnfNodeCount,
      inputFieldNames,
      rexBuilder,
      catalog,
      catalogManager,
      tz)
  }

}
