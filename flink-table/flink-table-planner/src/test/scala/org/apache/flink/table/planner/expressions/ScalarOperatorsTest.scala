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

import org.apache.flink.table.api.DataTypes
import org.apache.flink.table.planner.expressions.utils.{ExpressionTestBase, ScalarOperatorsTestBase}
import org.apache.flink.table.types.AbstractDataType
import org.apache.flink.types.Row

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
    testSqlApi("f15 = cast(null as string)", "null")
    testSqlApi("'1996-11-10' = f15", "true")
    testSqlApi("'1996-11-11' = f15", "false")
    testSqlApi("cast(null as string) = f15", "null")

    testSqlApi("f21 = '12:34:56'", "true")
    testSqlApi("f21 = '13:34:56'", "false")
    testSqlApi("f21 = cast(null as string)", "null")
    testSqlApi("'12:34:56' = f21", "true")
    testSqlApi("'13:34:56' = f21", "false")
    testSqlApi("cast(null as string) = f21", "null")

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

class ScalarEqualityOperatorsTest extends ExpressionTestBase {
  // these tests are extracted into a specific test class because they need specific data

  @Test
  def testEqualityForNumericValues(): Unit = {
    // direct equality
    for (i <- 0 to 6) {
      testSqlApi(s"f${i * 2} = f${i * 2 + 1}", "true")
      testSqlApi(s"f${14 + i * 2} = f${14 + i * 2 + 1}", "true")
      testSqlApi(s"f${i * 2} = f${14 + i * 2}", "false")

      testSqlApi(s"f${i * 2} <> f${i * 2 + 1}", "false")
      testSqlApi(s"f${14 + i * 2} <> f${14 + i * 2 + 1}", "false")
      testSqlApi(s"f${i * 2} <> f${14 + i * 2}", "true")
    }

    // equality between primitive numeric types
    for (i <- 0 to 6) {
      for (j <- 0 to 6) {
        testSqlApi(s"f${i * 2} = f${j * 2}", "true")
        testSqlApi(s"f${i * 2} = f${14 + j * 2}", "false")

        testSqlApi(s"f${i * 2} <> f${j * 2}", "false")
        testSqlApi(s"f${i * 2} <> f${14 + j * 2}", "true")
      }
    }
    // byte is excluded in this test because 777 overflows its range
    for (i <- 1 to 6) {
      for (j <- 1 to 6) {
        testSqlApi(s"f${14 + i * 2} = f${14 + j * 2}", "true")
        testSqlApi(s"f${14 + i * 2} = f${j * 2}", "false")

        testSqlApi(s"f${14 + i * 2} <> f${14 + j * 2}", "false")
        testSqlApi(s"f${14 + i * 2} <> f${j * 2}", "true")
      }
    }

    // equality with boxed numeric types (LEAST will return boxed internal data type)
    for (i <- 0 to 6) {
      testSqlApi(s"LEAST(f${i * 2}) = LEAST(f${i * 2 + 1})", "true")
      testSqlApi(s"LEAST(f${i * 2}) = f${i * 2 + 1}", "true")
      testSqlApi(s"LEAST(f${i * 2}) = f${14 + i * 2}", "false")
      testSqlApi(s"f${i * 2} = LEAST(f${i * 2 + 1})", "true")
      testSqlApi(s"f${14 + i * 2} = LEAST(f${i * 2 + 1})", "false")

      testSqlApi(s"LEAST(f${i * 2}) <> LEAST(f${i * 2 + 1})", "false")
      testSqlApi(s"LEAST(f${i * 2}) <> f${i * 2 + 1}", "false")
      testSqlApi(s"LEAST(f${i * 2}) <> f${14 + i * 2}", "true")
      testSqlApi(s"f${i * 2} <> LEAST(f${i * 2 + 1})", "false")
      testSqlApi(s"f${14 + i * 2} <> LEAST(f${i * 2 + 1})", "true")
    }
    // byte is excluded in this test because 777 overflows its range
    for (i <- 1 to 6) {
      testSqlApi(s"LEAST(f${14 + i * 2}) = LEAST(f${14 + i * 2 + 1})", "true")
      testSqlApi(s"LEAST(f${i * 2}) = LEAST(f${14 + i * 2})", "false")
      testSqlApi(s"LEAST(f${14 + i * 2}) = LEAST(f${i * 2})", "false")
      testSqlApi(s"LEAST(f${14 + i * 2}) = f${14 + i * 2 + 1}", "true")
      testSqlApi(s"LEAST(f${14 + i * 2}) = f${i * 2 + 1}", "false")
      testSqlApi(s"f${14 + i * 2} = LEAST(f${14 + i * 2 + 1})", "true")
      testSqlApi(s"f${i * 2} = LEAST(f${14 + i * 2 + 1})", "false")

      testSqlApi(s"LEAST(f${14 + i * 2}) <> LEAST(f${14 + i * 2 + 1})", "false")
      testSqlApi(s"LEAST(f${i * 2}) <> LEAST(f${14 + i * 2})", "true")
      testSqlApi(s"LEAST(f${14 + i * 2}) <> LEAST(f${i * 2})", "true")
      testSqlApi(s"LEAST(f${14 + i * 2}) <> f${14 + i * 2 + 1}", "false")
      testSqlApi(s"LEAST(f${14 + i * 2}) <> f${i * 2 + 1}", "true")
      testSqlApi(s"f${14 + i * 2} <> LEAST(f${14 + i * 2 + 1})", "false")
      testSqlApi(s"f${i * 2} <> LEAST(f${14 + i * 2 + 1})", "true")
    }
  }

  @Test
  def testEqualityForTimeValues(): Unit = {
    // direct equality
    for (i <- 0 to 3) {
      testSqlApi(s"f${28 + i * 2} = f${28 + i * 2 + 1}", "true")
      testSqlApi(s"f${36 + i * 2} = f${36 + i * 2 + 1}", "true")
      testSqlApi(s"f${28 + i * 2} = f${36 + i * 2}", "false")

      testSqlApi(s"f${28 + i * 2} <> f${28 + i * 2 + 1}", "false")
      testSqlApi(s"f${36 + i * 2} <> f${36 + i * 2 + 1}", "false")
      testSqlApi(s"f${28 + i * 2} <> f${36 + i * 2}", "true")
    }

    // equality with boxed time types (LEAST will return boxed internal data type)
    for (i <- 0 to 3) {
      testSqlApi(s"LEAST(f${28 + i * 2}) = LEAST(f${28 + i * 2 + 1})", "true")
      testSqlApi(s"LEAST(f${36 + i * 2}) = LEAST(f${36 + i * 2 + 1})", "true")
      testSqlApi(s"LEAST(f${28 + i * 2}) = LEAST(f${36 + i * 2 + 1})", "false")

      testSqlApi(s"LEAST(f${28 + i * 2}) = f${28 + i * 2 + 1}", "true")
      testSqlApi(s"f${28 + i * 2} = LEAST(f${28 + i * 2 + 1})", "true")
      testSqlApi(s"LEAST(f${36 + i * 2}) = f${36 + i * 2 + 1}", "true")
      testSqlApi(s"f${36 + i * 2} = LEAST(f${36 + i * 2 + 1})", "true")
      testSqlApi(s"LEAST(f${28 + i * 2}) = f${36 + i * 2 + 1}", "false")
      testSqlApi(s"f${28 + i * 2} = LEAST(f${36 + i * 2 + 1})", "false")
      testSqlApi(s"LEAST(f${28 + i * 2}) = f${36 + i * 2 + 1}", "false")
      testSqlApi(s"f${28 + i * 2} = LEAST(f${36 + i * 2 + 1})", "false")

      testSqlApi(s"LEAST(f${28 + i * 2}) <> LEAST(f${28 + i * 2 + 1})", "false")
      testSqlApi(s"LEAST(f${36 + i * 2}) <> LEAST(f${36 + i * 2 + 1})", "false")
      testSqlApi(s"LEAST(f${28 + i * 2}) <> LEAST(f${36 + i * 2 + 1})", "true")

      testSqlApi(s"LEAST(f${28 + i * 2}) <> f${28 + i * 2 + 1}", "false")
      testSqlApi(s"f${28 + i * 2} <> LEAST(f${28 + i * 2 + 1})", "false")
      testSqlApi(s"LEAST(f${36 + i * 2}) <> f${36 + i * 2 + 1}", "false")
      testSqlApi(s"f${36 + i * 2} <> LEAST(f${36 + i * 2 + 1})", "false")
      testSqlApi(s"LEAST(f${28 + i * 2}) <> f${36 + i * 2 + 1}", "true")
      testSqlApi(s"f${28 + i * 2} <> LEAST(f${36 + i * 2 + 1})", "true")
      testSqlApi(s"LEAST(f${28 + i * 2}) <> f${36 + i * 2 + 1}", "true")
      testSqlApi(s"f${28 + i * 2} <> LEAST(f${36 + i * 2 + 1})", "true")
    }
  }

  @Test
  def testEqualityForBooleanValues(): Unit = {
    // direct equality
    testSqlApi("f44 = f45", "true")
    testSqlApi("f46 = f47", "true")
    testSqlApi("f44 = f46", "false")
    testSqlApi("f45 = f47", "false")

    testSqlApi("f44 <> f45", "false")
    testSqlApi("f46 <> f47", "false")
    testSqlApi("f44 <> f46", "true")
    testSqlApi("f45 <> f47", "true")

    // equality with boxed boolean types (LEAST will return boxed internal data type)
    testSqlApi("LEAST(f44) = LEAST(f45)", "true")
    testSqlApi("LEAST(f46) = LEAST(f47)", "true")
    testSqlApi("LEAST(f44) = LEAST(f47)", "false")
    testSqlApi("LEAST(f46) = LEAST(f45)", "false")

    testSqlApi("LEAST(f44) <> LEAST(f45)", "false")
    testSqlApi("LEAST(f46) <> LEAST(f47)", "false")
    testSqlApi("LEAST(f44) <> LEAST(f47)", "true")
    testSqlApi("LEAST(f46) <> LEAST(f45)", "true")

    testSqlApi("LEAST(f44) = f45", "true")
    testSqlApi("f44 = LEAST(f45)", "true")
    testSqlApi("LEAST(f46) = f47", "true")
    testSqlApi("f46 = LEAST(f47)", "true")
    testSqlApi("LEAST(f44) = f47", "false")
    testSqlApi("f44 = LEAST(f47)", "false")
    testSqlApi("LEAST(f46) = f45", "false")
    testSqlApi("f46 = LEAST(f45)", "false")

    testSqlApi("LEAST(f44) <> f45", "false")
    testSqlApi("f44 <> LEAST(f45)", "false")
    testSqlApi("LEAST(f46) <> f47", "false")
    testSqlApi("f46 <> LEAST(f47)", "false")
    testSqlApi("LEAST(f44) <> f47", "true")
    testSqlApi("f44 <> LEAST(f47)", "true")
    testSqlApi("LEAST(f46) <> f45", "true")
    testSqlApi("f46 <> LEAST(f45)", "true")
  }

  override def testData: Row = {
    Row.of(
      // numeric values in range [-128, 127]
      java.lang.Byte.valueOf("7"), java.lang.Byte.valueOf("7"),
      java.lang.Short.valueOf("7"), java.lang.Short.valueOf("7"),
      java.lang.Integer.valueOf(7), java.lang.Integer.valueOf(7),
      java.lang.Long.valueOf(7), java.lang.Long.valueOf(7),
      java.lang.Float.valueOf(7), java.lang.Float.valueOf(7),
      java.lang.Double.valueOf(7), java.lang.Double.valueOf(7),
      new java.math.BigDecimal("7"), new java.math.BigDecimal("7"),

      // numeric values out of range [-128, 127], except for bytes
      java.lang.Byte.valueOf("77"), java.lang.Byte.valueOf("77"),
      java.lang.Short.valueOf("777"), java.lang.Short.valueOf("777"),
      java.lang.Integer.valueOf(777), java.lang.Integer.valueOf(777),
      java.lang.Long.valueOf(777), java.lang.Long.valueOf(777),
      java.lang.Float.valueOf(777), java.lang.Float.valueOf(777),
      java.lang.Double.valueOf(777), java.lang.Double.valueOf(777),
      new java.math.BigDecimal("777"), new java.math.BigDecimal("777"),

      // time values whose internal data representation in range [-128, 127]
      java.time.LocalDate.ofEpochDay(7), java.time.LocalDate.ofEpochDay(7),
      // currently Flink SQL does not support time with precision > 0,
      // so the only integer second we can pick is 0
      java.time.LocalTime.ofSecondOfDay(0), java.time.LocalTime.ofSecondOfDay(0),
      java.time.LocalDateTime.ofEpochSecond(0, 7000000, java.time.ZoneOffset.UTC),
      java.time.LocalDateTime.ofEpochSecond(0, 7000000, java.time.ZoneOffset.UTC),
      java.time.Instant.ofEpochMilli(7), java.time.Instant.ofEpochMilli(7),

      // time values whose internal data representation out of range [-128, 127]
      java.time.LocalDate.ofEpochDay(7000), java.time.LocalDate.ofEpochDay(7000),
      java.time.LocalTime.ofSecondOfDay(7), java.time.LocalTime.ofSecondOfDay(7),
      java.time.LocalDateTime.ofEpochSecond(7, 0, java.time.ZoneOffset.UTC),
      java.time.LocalDateTime.ofEpochSecond(7, 0, java.time.ZoneOffset.UTC),
      java.time.Instant.ofEpochMilli(7000), java.time.Instant.ofEpochMilli(7000),

      // boolean values
      java.lang.Boolean.valueOf(true), java.lang.Boolean.valueOf(true),
      java.lang.Boolean.valueOf(false), java.lang.Boolean.valueOf(false))
  }

  override def testDataType: AbstractDataType[_] = {
    DataTypes.ROW(
      // numeric values in range [-128, 127]
      DataTypes.FIELD("f0", DataTypes.TINYINT()),
      DataTypes.FIELD("f1", DataTypes.TINYINT()),
      DataTypes.FIELD("f2", DataTypes.SMALLINT()),
      DataTypes.FIELD("f3", DataTypes.SMALLINT()),
      DataTypes.FIELD("f4", DataTypes.INT()),
      DataTypes.FIELD("f5", DataTypes.INT()),
      DataTypes.FIELD("f6", DataTypes.BIGINT()),
      DataTypes.FIELD("f7", DataTypes.BIGINT()),
      DataTypes.FIELD("f8", DataTypes.FLOAT()),
      DataTypes.FIELD("f9", DataTypes.FLOAT()),
      DataTypes.FIELD("f10", DataTypes.DOUBLE()),
      DataTypes.FIELD("f11", DataTypes.DOUBLE()),
      DataTypes.FIELD("f12", DataTypes.DECIMAL(10, 3)),
      DataTypes.FIELD("f13", DataTypes.DECIMAL(10, 3)),

      // numeric values out of range [-128, 127]
      DataTypes.FIELD("f14", DataTypes.TINYINT()),
      DataTypes.FIELD("f15", DataTypes.TINYINT()),
      DataTypes.FIELD("f16", DataTypes.SMALLINT()),
      DataTypes.FIELD("f17", DataTypes.SMALLINT()),
      DataTypes.FIELD("f18", DataTypes.INT()),
      DataTypes.FIELD("f19", DataTypes.INT()),
      DataTypes.FIELD("f20", DataTypes.BIGINT()),
      DataTypes.FIELD("f21", DataTypes.BIGINT()),
      DataTypes.FIELD("f22", DataTypes.FLOAT()),
      DataTypes.FIELD("f23", DataTypes.FLOAT()),
      DataTypes.FIELD("f24", DataTypes.DOUBLE()),
      DataTypes.FIELD("f25", DataTypes.DOUBLE()),
      DataTypes.FIELD("f26", DataTypes.DECIMAL(10, 3)),
      DataTypes.FIELD("f27", DataTypes.DECIMAL(10, 3)),

      // time values whose internal data representation in range [-128, 127]
      DataTypes.FIELD("f28", DataTypes.DATE()),
      DataTypes.FIELD("f29", DataTypes.DATE()),
      DataTypes.FIELD("f30", DataTypes.TIME(0)),
      DataTypes.FIELD("f31", DataTypes.TIME(0)),
      DataTypes.FIELD("f32", DataTypes.TIMESTAMP(3)),
      DataTypes.FIELD("f33", DataTypes.TIMESTAMP(3)),
      DataTypes.FIELD("f34", DataTypes.TIMESTAMP_LTZ(3)),
      DataTypes.FIELD("f35", DataTypes.TIMESTAMP_LTZ(3)),

      // time values whose internal data representation out of range [-128, 127]
      DataTypes.FIELD("f36", DataTypes.DATE()),
      DataTypes.FIELD("f37", DataTypes.DATE()),
      DataTypes.FIELD("f38", DataTypes.TIME(0)),
      DataTypes.FIELD("f39", DataTypes.TIME(0)),
      DataTypes.FIELD("f40", DataTypes.TIMESTAMP(3)),
      DataTypes.FIELD("f41", DataTypes.TIMESTAMP(3)),
      DataTypes.FIELD("f42", DataTypes.TIMESTAMP_LTZ(3)),
      DataTypes.FIELD("f43", DataTypes.TIMESTAMP_LTZ(3)),

      // boolean values
      DataTypes.FIELD("f44", DataTypes.BOOLEAN()),
      DataTypes.FIELD("f45", DataTypes.BOOLEAN()),
      DataTypes.FIELD("f46", DataTypes.BOOLEAN()),
      DataTypes.FIELD("f47", DataTypes.BOOLEAN())
    )
  }

  override def containsLegacyTypes: Boolean = false
}
