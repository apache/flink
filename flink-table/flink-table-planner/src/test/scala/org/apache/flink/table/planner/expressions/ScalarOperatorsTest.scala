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

    testSqlApi("TYPEOF(f22)", "TIMESTAMP(6)")
    testSqlApi("f22 = '1996-11-10 12:34:56'", "TRUE")
    testSqlApi("f22 = '1996-11-10 12:34:57'", "FALSE")
    testSqlApi("f22 = cast(null as string)", "NULL")
    testSqlApi("'1996-11-10 12:34:56' = f22", "TRUE")
    testSqlApi("'1996-11-10 12:34:57' = f22", "FALSE")
    testSqlApi("cast(null as string) = f22", "NULL")

    testSqlApi("TYPEOF(f23)", "TIMESTAMP_LTZ(6)")
    testSqlApi("f23 = '1996-11-10 12:34:56'", "TRUE")
    testSqlApi("f23 = '1996-11-10 12:34:57'", "FALSE")
    testSqlApi("f23 = cast(null as string)", "NULL")
    testSqlApi("'1996-11-10 12:34:56' = f23", "TRUE")
    testSqlApi("'1996-11-10 12:34:57' = f23", "FALSE")
    testSqlApi("cast(null as string) = f23", "NULL")
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

    testSqlApi("f23 = date_format(f23, 'yyyy-MM-dd HH:mm:ss')", "TRUE")
    testSqlApi("f23 = date_format(f23 + interval '1' second, 'yyyy-MM-dd HH:mm:ss')", "FALSE")
    testSqlApi("f23 = uuid()", "NULL")
    testSqlApi("date_format(f23, 'yyyy-MM-dd HH:mm:ss') = f23", "TRUE")
    testSqlApi("date_format(f23 + interval '1' second, 'yyyy-MM-dd HH:mm:ss') = f23", "FALSE")
    testSqlApi("uuid() = f23", "NULL")
  }

  @Test
  def testTimePointTypeNotEqualsString(): Unit = {
    testSqlApi("NOT(f15 = '1996-11-10')", "FALSE")
    testSqlApi("NOT(f15 = '1996-11-11')", "TRUE")
    testSqlApi("NOT('1996-11-10' = f15)", "FALSE")
    testSqlApi("NOT('1996-11-11' = f15)", "TRUE")

    testSqlApi("NOT(f21 = '12:34:56')", "FALSE")
    testSqlApi("NOT(f21 = '13:34:56')", "TRUE")
    testSqlApi("NOT('12:34:56' = f21)", "FALSE")
    testSqlApi("NOT('13:34:56' = f21)", "TRUE")

    testSqlApi("TYPEOF(f22)", "TIMESTAMP(6)")
    testSqlApi("NOT(f22 = '1996-11-10 12:34:56')", "FALSE")
    testSqlApi("NOT(f22 = '1996-11-10 12:34:57')", "TRUE")
    testSqlApi("NOT(f22 = cast(null as string))", "NULL")
    testSqlApi("NOT('1996-11-10 12:34:56' = f22)", "FALSE")
    testSqlApi("NOT('1996-11-10 12:34:57' = f22)", "TRUE")
    testSqlApi("NOT(cast(null as string) = f22)", "NULL")

    testSqlApi("TYPEOF(f23)", "TIMESTAMP_LTZ(6)")
    testSqlApi("f23 = '1996-11-10 12:34:56'", "TRUE")
    testSqlApi("f23 = '1996-11-10 12:34:57'", "FALSE")
    testSqlApi("f23 = cast(null as string)", "NULL")
    testSqlApi("'1996-11-10 12:34:56' = f23", "TRUE")
    testSqlApi("'1996-11-10 12:34:57' = f23", "FALSE")
    testSqlApi("cast(null as string) = f23", "NULL")

    testSqlApi("NOT(f15 = date_format(cast(f15 as timestamp), 'yyyy-MM-dd'))", "FALSE")
    testSqlApi(
      "NOT(f15 = date_format(cast(f15 as timestamp) + interval '1' day, 'yyyy-MM-dd'))",
      "TRUE")
    testSqlApi("NOT(f15 = uuid())", "NULL")
    testSqlApi("NOT(date_format(cast(f15 as timestamp), 'yyyy-MM-dd') = f15)", "FALSE")
    testSqlApi(
      "NOT(date_format(cast(f15 as timestamp) + interval '1' day, 'yyyy-MM-dd')) = f15",
      "TRUE")
    testSqlApi("NOT(uuid() = f15)", "NULL")

    testSqlApi("NOT(f21 = date_format(cast(f21 as timestamp), 'HH:mm:ss'))", "FALSE")
    testSqlApi(
      "NOT(f21 = date_format(cast(f21 as timestamp) + interval '1' hour, 'HH:mm:ss'))",
      "TRUE")
    testSqlApi("NOT(f21 = uuid())", "NULL")
    testSqlApi("NOT(date_format(cast(f21 as timestamp), 'HH:mm:ss') = f21)", "FALSE")
    testSqlApi(
      "NOT(date_format(cast(f21 as timestamp) + interval '1' hour, 'HH:mm:ss') = f21)",
      "TRUE")
    testSqlApi("NOT(uuid() = f21)", "NULL")

    testSqlApi("NOT(f22 = date_format(f22, 'yyyy-MM-dd HH:mm:ss'))", "FALSE")
    testSqlApi("NOT(f22 = date_format(f22 + interval '1' second, 'yyyy-MM-dd HH:mm:ss'))", "TRUE")
    testSqlApi("NOT(f22 = uuid())", "NULL")
    testSqlApi("NOT(date_format(f22, 'yyyy-MM-dd HH:mm:ss') = f22)", "FALSE")
    testSqlApi("NOT(date_format(f22 + interval '1' second, 'yyyy-MM-dd HH:mm:ss') = f22)", "TRUE")
    testSqlApi("NOT(uuid() = f22)", "NULL")

    testSqlApi("NOT(cast(f22 as timestamp_ltz) = date_format(f22, 'yyyy-MM-dd HH:mm:ss'))", "FALSE")
    testSqlApi(
      "NOT(cast(f22 as timestamp_ltz) = date_format(f22 + interval '1' second, 'yyyy-MM-dd HH:mm:ss'))",
      "TRUE")
    testSqlApi("NOT(cast(f22 as timestamp_ltz) = uuid())", "NULL")
    testSqlApi("NOT(date_format(f22, 'yyyy-MM-dd HH:mm:ss') = cast(f22 as timestamp_ltz))", "FALSE")
    testSqlApi(
      "NOT(date_format(f22 + interval '1' second, 'yyyy-MM-dd HH:mm:ss') = cast(f22 as timestamp_ltz))",
      "TRUE")
    testSqlApi("NOT(uuid() = cast(f22 as timestamp_ltz))", "NULL")

    testSqlApi("NOT(f23 = date_format(f23, 'yyyy-MM-dd HH:mm:ss'))", "FALSE")
    testSqlApi("NOT(f23 = date_format(f23 + interval '1' second, 'yyyy-MM-dd HH:mm:ss'))", "TRUE")
    testSqlApi("NOT(f23 = uuid())", "NULL")
    testSqlApi("NOT(date_format(f23, 'yyyy-MM-dd HH:mm:ss') = f23)", "FALSE")
    testSqlApi("NOT(date_format(f23 + interval '1' second, 'yyyy-MM-dd HH:mm:ss') = f23)", "TRUE")
    testSqlApi("NOT(uuid() = f23)", "NULL")
  }

  @Test
  def testMoreEqualAndNonEqual(): Unit = {
    // character string
    testSqlApi("f10 = 'String'", "TRUE")
    testSqlApi("f10 = 'string'", "FALSE")
    testSqlApi("f10 = NULL", "NULL")
    testSqlApi("f10 = CAST(NULL AS STRING)", "NULL")
    testSqlApi("'String' = f10", "TRUE")
    testSqlApi("'string' = f10", "FALSE")
    testSqlApi("NULL = f10", "NULL")
    testSqlApi("CAST(NULL AS STRING) = f10", "NULL")

    testSqlApi("NOT(f10 = 'String')", "FALSE")
    testSqlApi("NOT(f10 = 'string')", "TRUE")
    testSqlApi("NOT(f10 = NULL)", "NULL")
    testSqlApi("NOT(f10 = CAST(NULL AS STRING))", "NULL")
    testSqlApi("NOT('String' = f10)", "FALSE")
    testSqlApi("NOT('string' = f10)", "TRUE")
    testSqlApi("NOT(NULL = f10)", "NULL")
    testSqlApi("NOT(CAST(NULL AS STRING) = f10)", "NULL")

    // numeric types
    testSqlApi("f2 = 1", "TRUE")
    testSqlApi("f2 = 2", "FALSE")
    testSqlApi("f2 = NULL", "NULL")
    testSqlApi("f2 = CAST(NULL AS INT)", "NULL")
    testSqlApi("1 = f2", "TRUE")
    testSqlApi("2 = f2", "FALSE")
    testSqlApi("NULL = f2", "NULL")
    testSqlApi("CAST(NULL AS INT) = f2", "NULL")

    testSqlApi("NOT(f2 = 1)", "FALSE")
    testSqlApi("NOT(f2 = 2)", "TRUE")
    testSqlApi("NOT(f2 = NULL)", "NULL")
    testSqlApi("NOT(f2 = CAST(NULL AS INT))", "NULL")
    testSqlApi("NOT(1 = f2)", "FALSE")
    testSqlApi("NOT(2 = f2)", "TRUE")
    testSqlApi("NOT(NULL = f2)", "NULL")
    testSqlApi("NOT(CAST(NULL AS INT) = f2)", "NULL")

    // array
    testSqlApi("f24 = ARRAY['hello', 'world']", "TRUE")
    testSqlApi("f24 = ARRAY['hello1', 'world']", "FALSE")
    testSqlApi("f24 = NULL", "NULL")
    testSqlApi("f24 = CAST(NULL AS ARRAY<STRING>)", "NULL")
    testSqlApi("ARRAY['hello', 'world'] = f24", "TRUE")
    testSqlApi("ARRAY['hello1', 'world'] = f24", "FALSE")
    testSqlApi("NULL = f24", "NULL")
    testSqlApi("CAST(NULL AS ARRAY<STRING>) = f24", "NULL")

    testSqlApi("NOT(f24 = ARRAY['hello', 'world'])", "FALSE")
    testSqlApi("NOT(f24 = ARRAY['hello1', 'world'])", "TRUE")
    testSqlApi("NOT(f24 = NULL)", "NULL")
    testSqlApi("NOT(f24 = CAST(NULL AS ARRAY<STRING>))", "NULL")
    testSqlApi("NOT(ARRAY['hello', 'world'] = f24)", "FALSE")
    testSqlApi("NOT(ARRAY['hello1', 'world'] = f24)", "TRUE")
    testSqlApi("NOT(NULL = f24)", "NULL")
    testSqlApi("NOT(CAST(NULL AS ARRAY<STRING>)) = f24", "NULL")

    // map
    testSqlApi("f25 = MAP['a', 1, 'b', 2]", "TRUE")
    testSqlApi("f25 = MAP['a', 3, 'b', 2]", "FALSE")
    testSqlApi("f25 = NULL", "NULL")
    testSqlApi("f25 = CAST(NULL AS MAP<STRING, INT>)", "NULL")
    testSqlApi("MAP['a', 1, 'b', 2] = f25", "TRUE")
    testSqlApi("MAP['a', 3, 'b', 2] = f25", "FALSE")
    testSqlApi("NULL = f25", "NULL")
    testSqlApi("CAST(NULL AS MAP<STRING, INT>) = f25", "NULL")

    testSqlApi("NOT(f25 = MAP['a', 1, 'b', 2])", "FALSE")
    testSqlApi("NOT(f25 = MAP['a', 3, 'b', 2])", "TRUE")
    testSqlApi("NOT(f25 = NULL)", "NULL")
    testSqlApi("NOT(f25 = CAST(NULL AS MAP<STRING, INT>))", "NULL")
    testSqlApi("NOT(MAP['a', 1, 'b', 2] = f25)", "FALSE")
    testSqlApi("NOT(MAP['a', 3, 'b', 2] = f25)", "TRUE")
    testSqlApi("NOT(NULL = f25)", "NULL")
    testSqlApi("NOT(CAST(NULL AS MAP<STRING, INT>) = f25)", "NULL")

    // raw
    testSqlApi("f27 = f29", "TRUE")
    testSqlApi("f27 = f28", "FALSE")
    testSqlApi("f27 = NULL", "NULL")
    testSqlApi("f29 = f27", "TRUE")
    testSqlApi("f28 = f27", "FALSE")
    testSqlApi("NULL = f27", "NULL")

    testSqlApi("NOT(f27 = f29)", "FALSE")
    testSqlApi("NOT(f27 = f28)", "TRUE")
    testSqlApi("NOT(f27 = NULL)", "NULL")
    testSqlApi("NOT(f29 = f27)", "FALSE")
    testSqlApi("NOT(f28 = f27)", "TRUE")
    testSqlApi("NOT(NULL = f27)", "NULL")

    // non comparable types
    testSqlApi("f30 = ROW('abc', 'def')", "TRUE")
    testSqlApi("f30 = ROW('abc', 'xyz')", "FALSE")
    testSqlApi("f30 = NULL", "NULL")
    testSqlApi("f30 = CAST(NULL AS ROW<f0 STRING, f1 STRING>)", "NULL")
    testSqlApi("ROW('abc', 'def') = f30", "TRUE")
    testSqlApi("ROW('abc', 'xyz') = f30", "FALSE")
    testSqlApi("CAST(NULL AS ROW<f0 STRING, f1 STRING>) = f30", "NULL")

    testSqlApi("NOT(f30 = ROW('abc', 'def'))", "FALSE")
    testSqlApi("NOT(f30 = ROW('abc', 'xyz'))", "TRUE")
    testSqlApi("NOT(f30 = NULL)", "NULL")
    testSqlApi("NOT(f30 = CAST(NULL AS ROW<f0 STRING, f1 STRING>))", "NULL")
    testSqlApi("NOT(ROW('abc', 'def') = f30)", "FALSE")
    testSqlApi("NOT(ROW('abc', 'xyz') = f30)", "TRUE")
    testSqlApi("NOT(CAST(NULL AS ROW<f0 STRING, f1 STRING>) = f30)", "NULL")

    // time interval, comparable
    testSqlApi("f31 = f33", "TRUE")
    testSqlApi("f31 = f32", "FALSE")
    testSqlApi("f31 = NULL", "NULL")
    testSqlApi("f31 = f34", "NULL")
    testSqlApi("f31 = CAST(NULL AS INTERVAL DAY)", "NULL")
    testSqlApi("f33 = f31", "TRUE")
    testSqlApi("f32 = f31", "FALSE")
    testSqlApi("NULL = f31", "NULL")
    testSqlApi("f34 = f31", "NULL")
    testSqlApi("CAST(NULL AS INTERVAL DAY) = f31", "NULL")

    testSqlApi("NOT(f31 = f33)", "FALSE")
    testSqlApi("NOT(f31 = f32)", "TRUE")
    testSqlApi("NOT(f31 = NULL)", "NULL")
    testSqlApi("NOT(f31 = f34)", "NULL")
    testSqlApi("NOT(f31 = CAST(NULL AS INTERVAL DAY))", "NULL")
    testSqlApi("NOT(f33 = f31)", "FALSE")
    testSqlApi("NOT(f32 = f31)", "TRUE")
    testSqlApi("NOT(NULL = f31)", "NULL")
    testSqlApi("NOT(f34 = f31)", "NULL")
    testSqlApi("NOT(CAST(NULL AS INTERVAL DAY) = f31)", "NULL")
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
