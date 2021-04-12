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

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.planner.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row

import org.junit.Test

import scala.util.Random

class DecimalCastTest extends ExpressionTestBase {

  val rnd = new Random()

  @Test
  def testCastFromNumeric(): Unit = {
    def test(t: String, max: Any, min: Any, rV: Any): Unit = {
      def value(i: Any) = s"CAST($i AS $t)"

      testSqlApi(s"CAST(${value(null)} AS DECIMAL)", "null")

      testSqlApi(s"CAST(${value(0)} AS DECIMAL)", "0")
      testSqlApi(s"CAST(${value(12)} AS DECIMAL)", "12")
      testSqlApi(s"CAST(${value(-12)} AS DECIMAL)", "-12")
      testSqlApi(s"CAST(${value(max)} AS DECIMAL(20, 0))", max.toString)
      testSqlApi(s"CAST(${value(min)} AS DECIMAL(20, 0))", min.toString)

      testSqlApi(s"CAST(${value(rV)} AS DECIMAL(20, 0))", rV.toString)

      testSqlApi(s"CAST(${value(100)} AS DECIMAL(2, 0))", "null")
    }

    test("TINYINT", Byte.MaxValue, Byte.MinValue, rnd.nextInt().toByte)
    test("SMALLINT", Short.MaxValue, Short.MinValue, rnd.nextInt().toShort)
    test("INT", Int.MaxValue, Int.MinValue, rnd.nextInt())
    test("BIGINT", Long.MaxValue, Long.MinValue, rnd.nextLong())
  }

  @Test
  def testCastFromFloat(): Unit = {
    def test(t: String, max: Any, min: Any, rV: Any): Unit = {
      def value(i: Any) = s"CAST($i AS $t)"

      testSqlApi(s"CAST(${value(null)} AS DECIMAL)", "null")

      testSqlApi(s"CAST(${value(0)} AS DECIMAL)", "0")
      testSqlApi(s"CAST(${value(12.2)} AS DECIMAL)", "12")
      testSqlApi(s"CAST(${value(-12.2)} AS DECIMAL)", "-12")
      testSqlApi(s"CAST(${value(max)} AS DECIMAL(38, 0))", "null")
      testSqlApi(s"CAST(${value(min)} AS DECIMAL(38, 0))", "null")

      testSqlApi(s"CAST(${value(rV)} AS DECIMAL(38, 0))", rV.toString)

      testSqlApi(s"CAST(${value(100)} AS DECIMAL(2, 0))", "null")

      testSqlApi(s"CAST(${value(6.8242f)} AS DECIMAL(10, 4))", "6.8242")

      // Test Round HALF_UP
      testSqlApi(s"CAST(${value(6.8242f)} AS DECIMAL(10, 3))", "6.824")
      testSqlApi(s"CAST(${value(6.8247f)} AS DECIMAL(10, 3))", "6.825")

      testSqlApi(s"CAST(${value(6.82f)} AS DECIMAL(10, 5))", "6.82000")
      testSqlApi(s"CAST(${value(6.82f)} AS DECIMAL(5, 5))", "null")
      testSqlApi(s"CAST(${value(66.82f)} AS DECIMAL(5, 4))", "null")
    }

    test("FLOAT", Float.MaxValue, Float.MinValue, rnd.nextInt())
    test("DOUBLE", Double.MaxValue, Double.MinValue, rnd.nextInt())
  }

  @Test
  def testCastFromString(): Unit = {
    testSqlApi(s"CAST(CAST(null AS VARCHAR) AS DECIMAL)", "null")

    testSqlApi(s"CAST('0' AS DECIMAL)", "0")
    testSqlApi(s"CAST('12.2' AS DECIMAL)", "12")
    testSqlApi(s"CAST('-12.2' AS DECIMAL)", "-12")

    val rV = rnd.nextInt()
    testSqlApi(s"CAST('$rV' AS DECIMAL(38, 0))", rV.toString)

    testSqlApi(s"CAST('100' AS DECIMAL(2, 0))", "null")
    testSqlApi(s"CAST('x100' AS DECIMAL(2, 0))", "null")
    testSqlApi(s"CAST('100x' AS DECIMAL(2, 0))", "null")
  }

  @Test
  def testCastFromBoolean(): Unit = {
    testSqlApi(s"CAST(CAST(null AS BOOLEAN) AS DECIMAL)", "null")
    testSqlApi(s"CAST(true AS DECIMAL)", "1")
    testSqlApi(s"CAST(false AS DECIMAL)", "0")
  }

  @Test
  def testCastToNumeric(): Unit = {
    def value(i: Any) = s"CAST('$i' AS DECIMAL(38, 18))"

    def test(t: String, max: Any, min: Any, rV: Any): Unit = {
      testSqlApi(s"CAST(${value(null)} AS $t)", "null")

      testSqlApi(s"CAST(${value(0)} AS $t)", "0")
      testSqlApi(s"CAST(${value(12)} AS $t)", "12")
      testSqlApi(s"CAST(${value(-12)} AS $t)", "-12")
      testSqlApi(s"CAST(${value(max)} AS $t)", max.toString)
      testSqlApi(s"CAST(${value(min)} AS $t)", min.toString)
      testSqlApi(s"CAST(${value(rV)} AS $t)", rV.toString)

      testSqlApi(s"CAST(${value(5.26)} AS $t)", "5")
    }

    test("TINYINT", Byte.MaxValue, Byte.MinValue, rnd.nextInt().toByte)
    test("SMALLINT", Short.MaxValue, Short.MinValue, rnd.nextInt().toShort)
    test("INT", Int.MaxValue, Int.MinValue, rnd.nextInt())
    test("BIGINT", Long.MaxValue, Long.MinValue, rnd.nextLong())

    // test cast overflow

    // 128 => -128
    testSqlApi(s"CAST(${value(Byte.MaxValue + 1)} AS TINYINT)", "-128")

    // 32768 => -32768
    testSqlApi(s"CAST(${value(Short.MaxValue + 1)} AS SMALLINT)", "-32768")

    // 2147483648 => -2147483648
    testSqlApi(s"CAST(${value(Int.MaxValue + 1L)} AS INT)", "-2147483648")

    // 9223372036854775808 => -9223372036854775808
    testSqlApi(
      s"CAST(${value(BigDecimal.apply(Long.MaxValue) + 1)} AS BIGINT)",
      "-9223372036854775808")
  }

  @Test
  def testCastToFloat(): Unit = {
    def value(i: Any) = s"CAST('$i' AS DECIMAL(38, 18))"

    def test(t: String): Unit = {
      testSqlApi(s"CAST(${value(null)} AS $t)", "null")
      testSqlApi(s"CAST(${value(0)} AS $t)", "0.0")
      testSqlApi(s"CAST(${value(12.2)} AS $t)", "12.2")
      testSqlApi(s"CAST(${value(-12.2)} AS $t)", "-12.2")
      testSqlApi(s"CAST(${value(5.26)} AS $t)", "5.26")
    }

    test("FLOAT")
    test("DOUBLE")
  }

  @Test
  def testCastToString(): Unit = {
    def value(i: Any) = s"CAST('$i' AS DECIMAL(38, 2))"
    testSqlApi(s"CAST(${value(null)} AS VARCHAR)", "null")
    testSqlApi(s"CAST(${value(0)} AS VARCHAR)", "0.00")
    testSqlApi(s"CAST(${value(12.2)} AS VARCHAR)", "12.20")
    testSqlApi(s"CAST(${value(-12.2)} AS VARCHAR)", "-12.20")
    testSqlApi(s"CAST(${value(5.26)} AS VARCHAR)", "5.26")
  }

  @Test
  def testCastToBoolean(): Unit = {
    def value(i: Any) = s"CAST('$i' AS DECIMAL(38, 2))"
    testSqlApi(s"CAST(${value(null)} AS BOOLEAN)", "null")
    testSqlApi(s"CAST(${value(0)} AS BOOLEAN)", "false")
    testSqlApi(s"CAST(${value(1)} AS BOOLEAN)", "true")
    testSqlApi(s"CAST(${value(12.2)} AS BOOLEAN)", "true")
    testSqlApi(s"CAST(${value(-12.2)} AS BOOLEAN)", "true")
  }

  @Test
  def testCastToDecimal(): Unit = {
    def value(i: Any) = s"CAST('$i' AS DECIMAL(38, 2))"
    testSqlApi(s"CAST(${value(null)} AS DECIMAL)", "null")
    testSqlApi(s"CAST(${value(0)} AS DECIMAL(1, 1))", "0.0")
    testSqlApi(s"CAST(${value(6.32)} AS DECIMAL(3, 2))", "6.32")
    testSqlApi(s"CAST(${value(236.2)} AS DECIMAL(2, 1))", "null")

    // Test Round HALF_UP
    testSqlApi(s"CAST(${value(5.22)} AS DECIMAL(2, 1))", "5.2")
    testSqlApi(s"CAST(${value(5.26)} AS DECIMAL(2, 1))", "5.3")
  }

  override def testData: Row = new Row(0)

  override def typeInfo: RowTypeInfo = new RowTypeInfo()
}
