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
  def testCast(): Unit = {

    // binary -> varchar
    testSqlApi(
      "CAST (f18 as varchar)",
      "hello world")
    testSqlApi(
      "CAST (CAST (x'68656C6C6F20636F6465' as binary) as varchar)",
      "hello code")

    // varbinary -> varchar
    testSqlApi(
      "CAST (f19 as varchar)",
      "hello flink")
    testSqlApi(
      "CAST (CAST (x'68656C6C6F2063617374' as varbinary) as varchar)",
      "hello cast")

    // null case
    testSqlApi("CAST (NULL AS INT)", "null")
    testSqlApi(
      "CAST (NULL AS VARCHAR) = ''",
      "null")
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
    testSqlApi("CASE 1 WHEN 1 THEN CAST ('a' as varchar(1)) WHEN 2 THEN " +
      "CAST ('bcd' as varchar(3)) END", "a")
    testSqlApi("CASE f2 WHEN 1 THEN 11 WHEN 2 THEN 4 ELSE NULL END", "11")
    testSqlApi("CASE f7 WHEN 1 THEN 11 WHEN 2 THEN 4 ELSE NULL END", "null")
    testSqlApi("CASE 42 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END", "null")
    testSqlApi("CASE 1 WHEN 1 THEN true WHEN 2 THEN false ELSE NULL END", "true")

    testSqlApi("CASE WHEN f2 = 1 THEN CAST ('' as INT) ELSE 0 END", "null")
    testSqlApi("IF(true, CAST ('non-numeric' AS BIGINT), 0)", "null")
  }
}
