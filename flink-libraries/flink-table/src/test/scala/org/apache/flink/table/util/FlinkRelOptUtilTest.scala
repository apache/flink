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

package org.apache.flink.table.util

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.calcite.{FlinkTypeFactory, FlinkTypeSystem}
import org.apache.flink.table.functions.sql.ScalarSqlFunctions
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.plan.util.FlinkRelOptUtil._

import org.apache.calcite.rex.{RexBuilder, RexLiteral, RexNode, RexUtil}
import org.apache.calcite.sql.`type`.SqlTypeName._
import org.apache.calcite.sql.fun.SqlStdOperatorTable
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.util.{DateString, TimeString, TimestampString}

import java.math.BigDecimal
import java.sql.{Date, Time, Timestamp}

import org.junit.Assert._
import org.junit.Test

class FlinkRelOptUtilTest {
  val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem())
  val rexBuilder = new RexBuilder(typeFactory)
  val (
      booleanType,
      tinyintType,
      smallintType,
      integerType,
      bigintType,
      floatType,
      doubleType,
      varcharType,
      charType,
      decimalType) = (
      typeFactory.createSqlType(BOOLEAN),
      typeFactory.createSqlType(TINYINT),
      typeFactory.createSqlType(SMALLINT),
      typeFactory.createSqlType(INTEGER),
      typeFactory.createSqlType(BIGINT),
      typeFactory.createSqlType(FLOAT),
      typeFactory.createSqlType(DOUBLE),
      typeFactory.createSqlType(VARCHAR),
      typeFactory.createSqlType(CHAR),
      typeFactory.createSqlType(DECIMAL))

  @Test
  def testGetLiteralValue(): Unit = {
    val nullLiteral = rexBuilder.makeBigintLiteral(null)
    assertNull(getLiteralValue(nullLiteral))
    assertLiteralValueEquals(true, rexBuilder.makeLiteral(true))
    val decimalValue = BigDecimal.valueOf(1L)
    assertLiteralValueEquals(1.toByte, rexBuilder.makeExactLiteral(decimalValue, tinyintType))
    assertLiteralValueEquals(1.toShort, rexBuilder.makeExactLiteral(decimalValue, smallintType))
    assertLiteralValueEquals(1, rexBuilder.makeExactLiteral(decimalValue, integerType))
    assertLiteralValueEquals(1L, rexBuilder.makeExactLiteral(decimalValue, bigintType))
    assertLiteralValueEquals(1.toFloat, rexBuilder.makeExactLiteral(decimalValue, floatType))
    assertLiteralValueEquals(1.toDouble, rexBuilder.makeExactLiteral(decimalValue, doubleType))
    assertLiteralValueEquals(decimalValue, rexBuilder.makeExactLiteral(decimalValue, decimalType))
    assertLiteralValueEquals("a",
      rexBuilder.makeLiteral("a", charType, true).asInstanceOf[RexLiteral])
    assertLiteralValueEquals("as",
      rexBuilder.makeLiteral("as", varcharType, true).asInstanceOf[RexLiteral])
    val timeString = new TimeString(10, 1, 59)
    assertEquals(getLiteralValue(rexBuilder.makeTimeLiteral(timeString, 0)),
      new Time(timeString.toCalendar.getTimeInMillis))
    val dateString = new DateString(2017, 10, 1)
    assertEquals(getLiteralValue(rexBuilder.makeDateLiteral(dateString)),
      new Date(dateString.getMillisSinceEpoch))
    val timestampString = new TimestampString(2017, 10, 1, 1, 0, 0)
    assertEquals(getLiteralValue(
      rexBuilder.makeTimestampLiteral(timestampString, 0)),
      new Timestamp(timestampString.getMillisSinceEpoch))
  }

  @Test
  def testPartition(): Unit = {
    val inputRefIdx = 1
    val isRelated = (r: RexNode)=> r.accept(new ColumnRelatedVisitor(inputRefIdx))

    // $1 <= 2
    val expr1 = rexBuilder.makeCall(
      LESS_THAN_OR_EQUAL,
      rexBuilder.makeInputRef(integerType, inputRefIdx),
      rexBuilder.makeExactLiteral(BigDecimal.valueOf(2), integerType))
    assertEquals((Option(expr1), None),
                 partition(expr1, rexBuilder, isRelated))
    // $1 > -1
    val expr2 = rexBuilder.makeCall(
      GREATER_THAN,
      rexBuilder.makeInputRef(integerType, inputRefIdx),
      rexBuilder.makeExactLiteral(BigDecimal.valueOf(-1), integerType))
    assertEquals((Option(expr2), None),
                 partition(expr2, rexBuilder, isRelated))
    // $2 is true
    val ref2 = rexBuilder.makeInputRef(booleanType, 2)
    val expr3 = rexBuilder.makeCall(IS_TRUE, ref2)
    assertEquals((None, Option(ref2)), partition(expr3, rexBuilder, isRelated))
    // $2 is false
    val expr3_1 = rexBuilder.makeCall(IS_FALSE, ref2)
    val expr3_2 = rexBuilder.makeCall(NOT, ref2)
    val (interested3, rest3) = partition(expr3_1, rexBuilder, isRelated)
    assertEquals(None, interested3)
    assertTrue(rest3.isDefined)
    assertTrue(RexUtil.eq(expr3_2, rest3.get))
    // $3 < 20
    val expr4 = rexBuilder.makeCall(LESS_THAN,
      rexBuilder.makeInputRef(integerType, 3),
      rexBuilder.makeExactLiteral(BigDecimal.valueOf(20), integerType))
    assertEquals((None, Option(expr4)), partition(expr4, rexBuilder, isRelated))
    // $1 <= 2 and $1 > -1
    val expr5 = rexBuilder.makeCall(AND, expr1, expr2)
    val (interested5, rest5) = partition(expr5, rexBuilder, isRelated)
    assertTrue(RexUtil.eq(expr5, interested5.get))
    assertTrue(rest5.isEmpty)
    // $1 <= 2 and not ($1 > -1)
    val expr6 = rexBuilder.makeCall(AND, expr1, rexBuilder.makeCall(NOT, expr2))
    val (interested6, rest6) = partition(expr6, rexBuilder, isRelated)
    assertEquals("AND(<=($1, 2), <=($1, -1))", interested6.get.toString)
    assertTrue(rest6.isEmpty)
    // $1 <= 2 and not ($1 > -1 and $2 is true)
    val expr7 = rexBuilder.makeCall(AND, expr1,
      rexBuilder.makeCall(NOT, rexBuilder.makeCall(AND, expr2, expr3)))
    val (interested7, rest7) = partition(expr7, rexBuilder, isRelated)
    assertEquals("<=($1, 2)", interested7.get.toString)
    assertEquals("OR(<=($1, -1), NOT(IS TRUE($2)))", rest7.get.toString)
    // $1 <= 2 and not ($1 > -1 or $2 is true)
    val expr8 = rexBuilder.makeCall(AND, expr1,
      rexBuilder.makeCall(NOT, rexBuilder.makeCall(OR, expr2, expr3)))
    val (interested8, rest8) = partition(expr8, rexBuilder, isRelated)
    assertEquals("AND(<=($1, 2), <=($1, -1))", interested8.get.toString)
    assertEquals("NOT(IS TRUE($2))", rest8.get.toString)
    // $1 <= 2 or $1 > -1
    val expr9 = rexBuilder.makeCall(OR, expr1, expr2)
    val (interested9, rest9) = partition(expr9, rexBuilder, isRelated)
    assertTrue(RexUtil.eq(expr9, interested9.get))
    assertTrue(rest9.isEmpty)
    // $1 <= 2 or $1 > -1 or $3 < 20
    val expr10 = rexBuilder.makeCall(OR, expr1, expr2, expr4)
    val (interested10, rest10) = partition(expr10, rexBuilder, isRelated)
    assertTrue(interested10.isEmpty)
    assertTrue(RexUtil.eq(expr10, rest10.get))
    // ($1 > -1 and $2 = true) or ($1 > -1 and $3 < 20)
    val expr11 = rexBuilder.makeCall(OR,
      rexBuilder.makeCall(AND, expr1, expr3),
      rexBuilder.makeCall(AND, expr1, expr4))
    val (interested11, rest11) = partition(expr11, rexBuilder, isRelated)
    assertEquals(expr1, interested11.get)
    // not ($1 <= 2 and $1 > -1)
    val expr12 = rexBuilder.makeCall(NOT, rexBuilder.makeCall(AND, expr1, expr2))
    val (interested12, rest12) = partition(expr12, rexBuilder, isRelated)
    assertEquals("OR(>($1, 2), <=($1, -1))", interested12.get.toString)
    assertTrue(rest12.isEmpty)
    // not ($1 <= 2 or $1 > -1 or $3 < 20)
    val expr13 = rexBuilder.makeCall(NOT, rexBuilder.makeCall(OR, expr1, expr2, expr4))
    val (interested13, rest13) = partition(expr13, rexBuilder, isRelated)
    assertEquals("AND(>($1, 2), <=($1, -1))", interested13.get.toString)
    assertEquals(">=($3, 20)", rest13.get.toString)
    // ($1 <= 2 and $1 > -1) and ($2 is true or not($3 < 20))
    val expr14 = rexBuilder.makeCall(AND,
      rexBuilder.makeCall(AND, expr1, expr2),
      rexBuilder.makeCall(OR, expr3, rexBuilder.makeCall(NOT, expr4)))
    val (interest14, rest14) = partition(expr14, rexBuilder, isRelated)
    assertEquals("AND(<=($1, 2), >($1, -1))", interest14.get.toString)
    assertEquals("OR(IS TRUE($2), >=($3, 20))", rest14.get.toString)
    // $1^2 <= 3
    val expr15 = rexBuilder.makeCall(
      SqlStdOperatorTable.POWER,
      rexBuilder.makeInputRef(integerType, 1),
      rexBuilder.makeExactLiteral(BigDecimal.valueOf(2), integerType))
    val (interest15, rest15) = partition(expr15, rexBuilder, isRelated)
    assertTrue(RexUtil.eq(expr15, interest15.get))
    assertTrue(rest15.isEmpty)
    // DIV($1, 2) > 3
    val expr16 = rexBuilder.makeCall(GREATER_THAN,
      rexBuilder.makeCall(ScalarSqlFunctions.DIV,
        rexBuilder.makeInputRef(integerType, 1),
        rexBuilder.makeExactLiteral(BigDecimal.valueOf(2), integerType)),
      rexBuilder.makeExactLiteral(BigDecimal.valueOf(3), integerType))
    val (interest16, rest16) = partition(expr16, rexBuilder, isRelated)
    assertTrue(RexUtil.eq(expr16, interest16.get))
    assertTrue(rest16.isEmpty)
  }

  @Test
  def testGetDigest(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getBatchTableEnvironment(env)
    tEnv.registerTableSource("MyTable", CommonTestData.get3Source(Array("a", "b", "c")))
    val table = tEnv.sqlQuery("select c from MyTable where a > 10")
    val node = tEnv.optimize(table.getRelNode)
    assertEquals("BatchExecCalc(select=[c], where=[>(a, 10)])", getDigest(node))
    assertTrue(getDigest(node, withInput = true).contains("input="))
  }

  private def assertLiteralValueEquals(expected: Any, actual: RexLiteral): Unit = {
    assertEquals(expected, getLiteralValue(actual))
  }
}
