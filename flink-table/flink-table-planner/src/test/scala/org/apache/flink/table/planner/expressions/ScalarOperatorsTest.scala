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

import org.apache.flink.table.planner.expressions.utils.ScalarOperatorsTestBase

import org.junit.Test

class ScalarOperatorsTest extends ScalarOperatorsTestBase {

  @Test
  def testIn(): Unit = {
    testSqlApi(
      "f2 IN (1, 2, 42)",
      "true"
    )

    testSqlApi(
      "CAST (f0 AS DECIMAL) IN (42.0, 2.00, 3.01, 1.000000)", // SQL would downcast otherwise
      "true"
    )

    testSqlApi(
      "f10 IN ('This is a test String.', 'String', 'Hello world', 'Comment#1')",
      "true"
    )

    testSqlApi(
      "f14 IN ('This is a test String.', 'String', 'Hello world')",
      "null"
    )

    testSqlApi(
      "f15 IN (DATE '1996-11-10')",
      "true"
    )

    testSqlApi(
      "f15 IN (DATE '1996-11-10', DATE '1996-11-11')",
      "true"
    )

    testSqlApi(
      "f7 IN (f16, f17)",
      "true"
    )
  }

  @Test
  def testCompareOperator(): Unit= {

    // f18 and f19 have same length.
    testSqlApi(
      "f18 > f19",
      "true")
    testSqlApi(
      "f18 >= f19",
      "true")
    testSqlApi(
      "f18 < f19",
      "false")
    testSqlApi(
      "f18 <= f19",
      "false")
    testSqlApi(
      "f18 = f18",
      "true")

    // f20's length is short than f19's, but great than it.
    testSqlApi(
      "f19 < f20",
      "true")

    testSqlApi(
      "x'68656C6C6F20636F6465' < x'68656C6C6F2063617374'",
      "false")
    testSqlApi(
      "x'68656C6C6F20636F6465' > x'68656C6C6F2063617374'",
      "true")

  }

  @Test
  def testOtherExpressions(): Unit = {

    // nested field null type
    testSqlApi("CASE WHEN f13.f1 IS NULL THEN 'a' ELSE 'b' END", "a")
    testSqlApi("CASE WHEN f13.f1 IS NOT NULL THEN 'a' ELSE 'b' END", "b")
    testSqlApi("f13 IS NULL", "false")
    testSqlApi("f13 IS NOT NULL", "true")
    testSqlApi("f13.f0 IS NULL", "false")
    testSqlApi("f13.f0 IS NOT NULL", "true")
    testSqlApi("f13.f1 IS NULL", "true")
    testSqlApi("f13.f1 IS NOT NULL", "false")

    // boolean literals
    testSqlApi(
      "true",
      "true")

    testSqlApi(
      "fAlse",
      "false")

    testSqlApi(
      "tRuE",
      "true")

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
    testSqlApi("CASE f7 WHEN 1 THEN 11 WHEN 2 THEN 4 ELSE NULL END", "null")
    testSqlApi("CASE 42 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END", "null")
    testSqlApi("CASE 1 WHEN 1 THEN true WHEN 2 THEN false ELSE NULL END", "true")
    testSqlApi("IF(true, CAST ('non-numeric' AS BIGINT), 0)", "null")
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
    testSqlApi("f15 = '1996-11-10'", "true")
    testSqlApi("f15 = '1996-11-11'", "false")
    testSqlApi("'1996-11-10' = f15", "true")
    testSqlApi("'1996-11-11' = f15", "false")

    testSqlApi("f21 = '12:34:56'", "true")
    testSqlApi("f21 = '13:34:56'", "false")
    testSqlApi("'12:34:56' = f21", "true")
    testSqlApi("'13:34:56' = f21", "false")

    testSqlApi("f22 = '1996-11-10 12:34:56'", "true")
    testSqlApi("f22 = '1996-11-10 12:34:57'", "false")
    testSqlApi("f22 = cast(null as string)", "null")
    testSqlApi("'1996-11-10 12:34:56' = f22", "true")
    testSqlApi("'1996-11-10 12:34:57' = f22", "false")
    testSqlApi("cast(null as string) = f22", "null")
  }

  @Test
  def testTemporalTypeEqualsStringType(): Unit = {
    testSqlApi("f15 = date_format(cast(f15 as timestamp), 'yyyy-MM-dd')", "true")
    testSqlApi(
      "f15 = date_format(cast(f15 as timestamp) + interval '1' day, 'yyyy-MM-dd')",
      "false")
    testSqlApi("f15 = uuid()", "null")
    testSqlApi("date_format(cast(f15 as timestamp), 'yyyy-MM-dd') = f15", "true")
    testSqlApi(
      "date_format(cast(f15 as timestamp) + interval '1' day, 'yyyy-MM-dd') = f15",
      "false")
    testSqlApi("uuid() = f15", "null")

    testSqlApi("f21 = date_format(cast(f21 as timestamp), 'HH:mm:ss')", "true")
    testSqlApi(
      "f21 = date_format(cast(f21 as timestamp) + interval '1' hour, 'HH:mm:ss')",
      "false")
    testSqlApi("f21 = uuid()", "null")
    testSqlApi("date_format(cast(f21 as timestamp), 'HH:mm:ss') = f21", "true")
    testSqlApi(
      "date_format(cast(f21 as timestamp) + interval '1' hour, 'HH:mm:ss') = f21",
      "false")
    testSqlApi("uuid() = f21", "null")

    testSqlApi("f22 = date_format(f22, 'yyyy-MM-dd HH:mm:ss')", "true")
    testSqlApi(
      "f22 = date_format(f22 + interval '1' second, 'yyyy-MM-dd HH:mm:ss')",
      "false")
    testSqlApi("f22 = uuid()", "null")
    testSqlApi("date_format(f22, 'yyyy-MM-dd HH:mm:ss') = f22", "true")
    testSqlApi(
      "date_format(f22 + interval '1' second, 'yyyy-MM-dd HH:mm:ss') = f22",
      "false")
    testSqlApi("uuid() = f22", "null")

    testSqlApi(
      "cast(f22 as timestamp_ltz) = date_format(f22, 'yyyy-MM-dd HH:mm:ss')",
      "true")
    testSqlApi(
      "cast(f22 as timestamp_ltz) = date_format(f22 + interval '1' second, 'yyyy-MM-dd HH:mm:ss')",
      "false")
    testSqlApi("cast(f22 as timestamp_ltz) = uuid()", "null")
    testSqlApi(
      "date_format(f22, 'yyyy-MM-dd HH:mm:ss') = cast(f22 as timestamp_ltz)",
      "true")
    testSqlApi(
      "date_format(f22 + interval '1' second, 'yyyy-MM-dd HH:mm:ss') = cast(f22 as timestamp_ltz)",
      "false")
    testSqlApi("uuid() = cast(f22 as timestamp_ltz)", "null")
  }
}
