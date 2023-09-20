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
package org.apache.flink.table.planner.expressions

import org.apache.flink.table.api.{DataTypes, LiteralStringExpression, UnresolvedFieldExpression}
import org.apache.flink.table.planner.expressions.utils.ScalarOperatorsTestBase

import org.junit.jupiter.api.Test

class ScalarOperatorsTest extends ScalarOperatorsTestBase {

  @Test
  def testIn(): Unit = {
    testSqlApi(
      "f2 IN (1, 2, 42)",
      "TRUE"
    )

    testSqlApi(
      "CAST (f0 AS DECIMAL) IN (42.0, 2.00, 3.01, 1.000000)", // SQL would downcast otherwise
      "TRUE"
    )

    testSqlApi(
      "f10 IN ('This is a test String.', 'String', 'Hello world', 'Comment#1')",
      "TRUE"
    )

    testSqlApi(
      "f14 IN ('This is a test String.', 'String', 'Hello world')",
      "NULL"
    )

    testSqlApi(
      "f15 IN (DATE '1996-11-10')",
      "TRUE"
    )

    testSqlApi(
      "f15 IN (DATE '1996-11-10', DATE '1996-11-11')",
      "TRUE"
    )

    testSqlApi(
      "f7 IN (f16, f17)",
      "TRUE"
    )
  }

  @Test
  def testCompareOperator(): Unit = {

    // f18 and f19 have same length.
    testSqlApi("f18 > f19", "TRUE")
    testSqlApi("f18 >= f19", "TRUE")
    testSqlApi("f18 < f19", "FALSE")
    testSqlApi("f18 <= f19", "FALSE")
    testSqlApi("f18 = f18", "TRUE")

    // f20's length is short than f19's, but great than it.
    testSqlApi("f19 < f20", "TRUE")

    testSqlApi("x'68656C6C6F20636F6465' < x'68656C6C6F2063617374'", "FALSE")
    testSqlApi("x'68656C6C6F20636F6465' > x'68656C6C6F2063617374'", "TRUE")

  }

  @Test
  def testFunctionWithBooleanExpression(): Unit = {
    // this test is to check if the `resultType` of the `GeneratedExpression`
    // of these boolean expression match their definition in `FlinkSqlOperatorTable`,
    // if not, exceptions will be thrown from BridgingFunctionGenUtil#verifyArgumentTypes

    // test comparison expression
    testSqlApi("IFNULL(f18 > f19, false)", "TRUE")
    testSqlApi("IFNULL(f18 >= f19, false)", "TRUE")
    testSqlApi("IFNULL(f18 < f19, true)", "FALSE")
    testSqlApi("IFNULL(f18 <= f19, true)", "FALSE")
    testSqlApi("IFNULL(f18 = f18, false)", "TRUE")
    // test logic expression
    testSqlApi("IFNULL((f6 is false) and f11, true) ", "FALSE")
    testSqlApi("IFNULL((f6 is true) or f11, false) ", "TRUE")
    testSqlApi("IFNULL(not f11, false) ", "TRUE")
    testSqlApi("IFNULL(f6 is true, false) ", "TRUE")
    testSqlApi("IFNULL(f6 is not true, true) ", "FALSE")
    testSqlApi("IFNULL(f6 is false, true) ", "FALSE")
    testSqlApi("IFNULL(f6 is not false, false) ", "TRUE")
  }

  @Test
  def testOtherExpressions(): Unit = {

    // nested field null type
    testSqlApi("CASE WHEN f13.f1 IS NULL THEN 'a' ELSE 'b' END", "a")
    testSqlApi("CASE WHEN f13.f1 IS NOT NULL THEN 'a' ELSE 'b' END", "b")
    testSqlApi("f13 IS NULL", "FALSE")
    testSqlApi("f13 IS NOT NULL", "TRUE")
    testSqlApi("f13.f0 IS NULL", "FALSE")
    testSqlApi("f13.f0 IS NOT NULL", "TRUE")
    testSqlApi("f13.f1 IS NULL", "TRUE")
    testSqlApi("f13.f1 IS NOT NULL", "FALSE")

    // boolean literals
    testSqlApi("true", "TRUE")

    testSqlApi("fAlse", "FALSE")

    testSqlApi("tRuE", "TRUE")

    // case when
    testSqlApi("CASE 11 WHEN 1 THEN 'a' ELSE 'b' END", "b")
    testSqlApi("CASE 2 WHEN 1 THEN 'a' ELSE 'b' END", "b")
    testSqlApi(
      "CASE 1 WHEN 1, 2 THEN '1 or 2' WHEN 2 THEN 'not possible' WHEN 3, 2 " +
        "THEN '3' ELSE 'none of the above' END",
      "1 or 2")
    testSqlApi(
      "CASE 2 WHEN 1, 2 THEN '1 or 2' WHEN 2 THEN 'not possible' WHEN 3, 2 " +
        "THEN '3' ELSE 'none of the above' END",
      "1 or 2")
    testSqlApi(
      "CASE 3 WHEN 1, 2 THEN '1 or 2' WHEN 2 THEN 'not possible' WHEN 3, 2 " +
        "THEN '3' ELSE 'none of the above' END",
      "3")
    testSqlApi(
      "CASE 4 WHEN 1, 2 THEN '1 or 2' WHEN 2 THEN 'not possible' WHEN 3, 2 " +
        "THEN '3' ELSE 'none of the above' END",
      "none of the above")
    testSqlApi("CASE WHEN 'a'='a' THEN 1 END", "1")
    testSqlApi("CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END", "bcd")
    testSqlApi("CASE 1 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END", "a")
    testSqlApi("CASE f2 WHEN 1 THEN 11 WHEN 2 THEN 4 ELSE NULL END", "11")
    testSqlApi("CASE f7 WHEN 1 THEN 11 WHEN 2 THEN 4 ELSE NULL END", "NULL")
    testSqlApi("CASE 42 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END", "NULL")
    testSqlApi("CASE 1 WHEN 1 THEN true WHEN 2 THEN false ELSE NULL END", "TRUE")
    testSqlApi("IF(true, TRY_CAST ('non-numeric' AS BIGINT), 0)", "NULL")
  }

  @Test
  def testUnaryPlusMinus(): Unit = {
    testSqlApi("-f0", "-1")
    testSqlApi("+f0", "1")
    testSqlApi("-f1", "-1")
    testSqlApi("+f1", "1")
    testSqlApi("-f2", "-1")
    testSqlApi("+f2", "1")
    testSqlApi("-f3", "-1")
    testSqlApi("+f3", "1")
    testSqlApi("-f4", "-1.0")
    testSqlApi("+f4", "1.0")
    testSqlApi("-f5", "-1.0")
    testSqlApi("+f5", "1.0")
    testSqlApi("-f17", "-10.0")
    testSqlApi("+f17", "10.0")
  }

  @Test
  def testTemporalTypeEqualsStringLiteral(): Unit = {
    testSqlApi("f15 = '1996-11-10'", "TRUE")
    testSqlApi("f15 = '1996-11-11'", "FALSE")
    testSqlApi("'1996-11-10' = f15", "TRUE")
    testSqlApi("'1996-11-11' = f15", "FALSE")

    testSqlApi("f21 = '12:34:56'", "TRUE")
    testSqlApi("f21 = '13:34:56'", "FALSE")
    testSqlApi("'12:34:56' = f21", "TRUE")
    testSqlApi("'13:34:56' = f21", "FALSE")

    testSqlApi("f22 = '1996-11-10 12:34:56'", "TRUE")
    testSqlApi("f22 = '1996-11-10 12:34:57'", "FALSE")
    testSqlApi("f22 = cast(null as string)", "NULL")
    testSqlApi("'1996-11-10 12:34:56' = f22", "TRUE")
    testSqlApi("'1996-11-10 12:34:57' = f22", "FALSE")
    testSqlApi("cast(null as string) = f22", "NULL")
  }

  @Test
  def testTemporalTypeEqualsStringType(): Unit = {
    testSqlApi("f15 = date_format(cast(f15 as timestamp), 'yyyy-MM-dd')", "TRUE")
    testSqlApi(
      "f15 = date_format(cast(f15 as timestamp) + interval '1' day, 'yyyy-MM-dd')",
      "FALSE")
    testSqlApi("f15 = uuid()", "NULL")
    testSqlApi("date_format(cast(f15 as timestamp), 'yyyy-MM-dd') = f15", "TRUE")
    testSqlApi(
      "date_format(cast(f15 as timestamp) + interval '1' day, 'yyyy-MM-dd') = f15",
      "FALSE")
    testSqlApi("uuid() = f15", "NULL")

    testSqlApi("f21 = date_format(cast(f21 as timestamp), 'HH:mm:ss')", "TRUE")
    testSqlApi("f21 = date_format(cast(f21 as timestamp) + interval '1' hour, 'HH:mm:ss')", "FALSE")
    testSqlApi("f21 = uuid()", "NULL")
    testSqlApi("date_format(cast(f21 as timestamp), 'HH:mm:ss') = f21", "TRUE")
    testSqlApi("date_format(cast(f21 as timestamp) + interval '1' hour, 'HH:mm:ss') = f21", "FALSE")
    testSqlApi("uuid() = f21", "NULL")

    testSqlApi("f22 = date_format(f22, 'yyyy-MM-dd HH:mm:ss')", "TRUE")
    testSqlApi("f22 = date_format(f22 + interval '1' second, 'yyyy-MM-dd HH:mm:ss')", "FALSE")
    testSqlApi("f22 = uuid()", "NULL")
    testSqlApi("date_format(f22, 'yyyy-MM-dd HH:mm:ss') = f22", "TRUE")
    testSqlApi("date_format(f22 + interval '1' second, 'yyyy-MM-dd HH:mm:ss') = f22", "FALSE")
    testSqlApi("uuid() = f22", "NULL")

    testSqlApi("cast(f22 as timestamp_ltz) = date_format(f22, 'yyyy-MM-dd HH:mm:ss')", "TRUE")
    testSqlApi(
      "cast(f22 as timestamp_ltz) = date_format(f22 + interval '1' second, 'yyyy-MM-dd HH:mm:ss')",
      "FALSE")
    testSqlApi("cast(f22 as timestamp_ltz) = uuid()", "NULL")
    testSqlApi("date_format(f22, 'yyyy-MM-dd HH:mm:ss') = cast(f22 as timestamp_ltz)", "TRUE")
    testSqlApi(
      "date_format(f22 + interval '1' second, 'yyyy-MM-dd HH:mm:ss') = cast(f22 as timestamp_ltz)",
      "FALSE")
    testSqlApi("uuid() = cast(f22 as timestamp_ltz)", "NULL")
  }

  @Test
  def testTryCast(): Unit = {
    testAllApis(
      "non-numeric".tryCast(DataTypes.BIGINT()),
      "TRY_CAST ('non-numeric' AS BIGINT)",
      "NULL")
    testAllApis('f10.tryCast(DataTypes.BIGINT()), "TRY_CAST (f10 AS BIGINT)", "NULL")
  }
}
