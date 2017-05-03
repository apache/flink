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

package org.apache.flink.table.expressions

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.types.Row
import org.apache.flink.table.api.{Types, ValidationException}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.{ExpressionTestBase, ShouldNotExecuteFunc}
import org.apache.flink.table.functions.ScalarFunction
import org.junit.Test

class ScalarOperatorsTest extends ExpressionTestBase {

  @Test
  def testCasting(): Unit = {
    // test casting
    // * -> String
    testTableApi('f2.cast(Types.STRING), "f2.cast(STRING)", "1")
    testTableApi('f5.cast(Types.STRING), "f5.cast(STRING)", "1.0")
    testTableApi('f3.cast(Types.STRING), "f3.cast(STRING)", "1")
    testTableApi('f6.cast(Types.STRING), "f6.cast(STRING)", "true")
    // NUMERIC TYPE -> Boolean
    testTableApi('f2.cast(Types.BOOLEAN), "f2.cast(BOOLEAN)", "true")
    testTableApi('f7.cast(Types.BOOLEAN), "f7.cast(BOOLEAN)", "false")
    testTableApi('f3.cast(Types.BOOLEAN), "f3.cast(BOOLEAN)", "true")
    // NUMERIC TYPE -> NUMERIC TYPE
    testTableApi('f2.cast(Types.DOUBLE), "f2.cast(DOUBLE)", "1.0")
    testTableApi('f7.cast(Types.INT), "f7.cast(INT)", "0")
    testTableApi('f3.cast(Types.SHORT), "f3.cast(SHORT)", "1")
    // Boolean -> NUMERIC TYPE
    testTableApi('f6.cast(Types.DOUBLE), "f6.cast(DOUBLE)", "1.0")
    // identity casting
    testTableApi('f2.cast(Types.INT), "f2.cast(INT)", "1")
    testTableApi('f7.cast(Types.DOUBLE), "f7.cast(DOUBLE)", "0.0")
    testTableApi('f3.cast(Types.LONG), "f3.cast(LONG)", "1")
    testTableApi('f6.cast(Types.BOOLEAN), "f6.cast(BOOLEAN)", "true")
    // String -> BASIC TYPE (not String, Date, Void, Character)
    testTableApi('f2.cast(Types.BYTE), "f2.cast(BYTE)", "1")
    testTableApi('f2.cast(Types.SHORT), "f2.cast(SHORT)", "1")
    testTableApi('f2.cast(Types.INT), "f2.cast(INT)", "1")
    testTableApi('f2.cast(Types.LONG), "f2.cast(LONG)", "1")
    testTableApi('f3.cast(Types.DOUBLE), "f3.cast(DOUBLE)", "1.0")
    testTableApi('f3.cast(Types.FLOAT), "f3.cast(FLOAT)", "1.0")
    testTableApi('f5.cast(Types.BOOLEAN), "f5.cast(BOOLEAN)", "true")

    // numeric auto cast in arithmetic
    testTableApi('f0 + 1, "f0 + 1", "2")
    testTableApi('f1 + 1, "f1 + 1", "2")
    testTableApi('f2 + 1L, "f2 + 1L", "2")
    testTableApi('f3 + 1.0f, "f3 + 1.0f", "2.0")
    testTableApi('f3 + 1.0d, "f3 + 1.0d", "2.0")
    testTableApi('f5 + 1, "f5 + 1", "2.0")
    testTableApi('f3 + 1.0d, "f3 + 1.0d", "2.0")
    testTableApi('f4 + 'f0, "f4 + f0", "2.0")

    // numeric auto cast in comparison
    testTableApi(
      'f0 > 0 && 'f1 > 0 && 'f2 > 0L && 'f4 > 0.0f && 'f5 > 0.0d  && 'f3 > 0,
      "f0 > 0 && f1 > 0 && f2 > 0L && f4 > 0.0f && f5 > 0.0d  && f3 > 0",
      "true")
  }

  @Test
  def testArithmetic(): Unit = {
    // math arthmetic
    testTableApi('f8 - 5, "f8 - 5", "0")
    testTableApi('f8 + 5, "f8 + 5", "10")
    testTableApi('f8 / 2, "f8 / 2", "2")
    testTableApi('f8 * 2, "f8 * 2", "10")
    testTableApi('f8 % 2, "f8 % 2", "1")
    testTableApi(-'f8, "-f8", "-5")
    testTableApi( +'f8, "+f8", "5") // additional space before "+" required because of checkstyle
    testTableApi(3.toExpr + 'f8, "3 + f8", "8")

    // boolean arithmetic: AND
    testTableApi('f6 && true, "f6 && true", "true")      // true && true
    testTableApi('f6 && false, "f6 && false", "false")   // true && false
    testTableApi('f11 && true, "f11 && true", "false")   // false && true
    testTableApi('f11 && false, "f11 && false", "false") // false && false
    testTableApi('f6 && 'f12, "f6 && f12", "null")       // true && null
    testTableApi('f11 && 'f12, "f11 && f12", "false")    // false && null
    testTableApi('f12 && true, "f12 && true", "null")    // null && true
    testTableApi('f12 && false, "f12 && false", "false") // null && false
    testTableApi('f12 && 'f12, "f12 && f12", "null")     // null && null
    testTableApi('f11 && ShouldNotExecuteFunc('f10),     // early out
      "f11 && ShouldNotExecuteFunc(f10)", "false")
    testTableApi('f6 && 'f11 && ShouldNotExecuteFunc('f10),  // early out
      "f6 && f11 && ShouldNotExecuteFunc(f10)", "false")

    // boolean arithmetic: OR
    testTableApi('f6 || true, "f6 || true", "true")      // true || true
    testTableApi('f6 || false, "f6 || false", "true")    // true || false
    testTableApi('f11 || true, "f11 || true", "true")    // false || true
    testTableApi('f11 || false, "f11 || false", "false") // false || false
    testTableApi('f6 || 'f12, "f6 || f12", "true")       // true || null
    testTableApi('f11 || 'f12, "f11 || f12", "null")     // false || null
    testTableApi('f12 || true, "f12 || true", "true")    // null || true
    testTableApi('f12 || false, "f12 || false", "null")  // null || false
    testTableApi('f12 || 'f12, "f12 || f12", "null")     // null || null
    testTableApi('f6 || ShouldNotExecuteFunc('f10),      // early out
      "f6 || ShouldNotExecuteFunc(f10)", "true")
    testTableApi('f11 || 'f6 || ShouldNotExecuteFunc('f10),  // early out
      "f11 || f6 || ShouldNotExecuteFunc(f10)", "true")

    // boolean arithmetic: NOT
    testTableApi(!'f6, "!f6", "false")

    // comparison
    testTableApi('f8 > 'f2, "f8 > f2", "true")
    testTableApi('f8 >= 'f8, "f8 >= f8", "true")
    testTableApi('f8 < 'f2, "f8 < f2", "false")
    testTableApi('f8.isNull, "f8.isNull", "false")
    testTableApi('f8.isNotNull, "f8.isNotNull", "true")
    testTableApi(12.toExpr <= 'f8, "12 <= f8", "false")

    // string arithmetic
    testTableApi(42.toExpr + 'f10 + 'f9, "42 + f10 + f9", "42String10")
    testTableApi('f10 + 'f9, "f10 + f9", "String10")
  }

  @Test
  def testOtherExpressions(): Unit = {
    // null
    testAllApis(Null(Types.INT), "Null(INT)", "CAST(NULL AS INT)", "null")
    testAllApis(
      Null(Types.STRING) === "",
      "Null(STRING) === ''",
      "CAST(NULL AS VARCHAR) = ''",
      "null")

    // if
    testTableApi(('f6 && true).?("true", "false"), "(f6 && true).?('true', 'false')", "true")
    testTableApi(false.?("true", "false"), "false.?('true', 'false')", "false")
    testTableApi(
      true.?(true.?(true.?(10, 4), 4), 4),
      "true.?(true.?(true.?(10, 4), 4), 4)",
      "10")
    testTableApi(true, "?((f6 && true), 'true', 'false')", "true")
    testTableApi(
      If('f9 > 'f8, 'f9 - 1, 'f9),
      "If(f9 > f8, f9 - 1, f9)",
      "9"
    )
    testSqlApi("CASE 11 WHEN 1 THEN 'a' ELSE 'b' END", "b")
    testSqlApi("CASE 2 WHEN 1 THEN 'a' ELSE 'b' END", "b")
    testSqlApi(
      "CASE 1 WHEN 1, 2 THEN '1 or 2' WHEN 2 THEN 'not possible' WHEN 3, 2 THEN '3' " +
      "ELSE 'none of the above' END",
      "1 or 2           ")
    testSqlApi("CASE WHEN 'a'='a' THEN 1 END", "1")
    testSqlApi("CASE 2 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END", "bcd")
    testSqlApi("CASE f2 WHEN 1 THEN 11 WHEN 2 THEN 4 ELSE NULL END", "11")
    testSqlApi("CASE f7 WHEN 1 THEN 11 WHEN 2 THEN 4 ELSE NULL END", "null")
    testSqlApi("CASE 42 WHEN 1 THEN 'a' WHEN 2 THEN 'bcd' END", "null")
    testSqlApi("CASE 1 WHEN 1 THEN true WHEN 2 THEN false ELSE NULL END", "true")

    // case insensitive as
    testTableApi(5 as 'test, "5 As test", "5")

    // complex expressions
    testTableApi('f0.isNull.isNull, "f0.isNull().isNull", "false")
    testTableApi(
      'f8.abs() + 'f8.abs().abs().abs().abs(),
      "f8.abs() + f8.abs().abs().abs().abs()",
      "10")
    testTableApi(
      'f8.cast(Types.STRING) + 'f8.cast(Types.STRING),
      "f8.cast(STRING) + f8.cast(STRING)",
      "55")
    testTableApi('f8.isNull.cast(Types.INT), "CAST(ISNULL(f8), INT)", "0")
    testTableApi(
      'f8.cast(Types.INT).abs().isNull === false,
      "ISNULL(CAST(f8, INT).abs()) === false",
      "true")
    testTableApi(
      (((true === true) || false).cast(Types.STRING) + "X ").trim(),
      "((((true) === true) || false).cast(STRING) + 'X ').trim",
      "trueX")
    testTableApi(12.isNull, "12.isNull", "false")
  }

  @Test(expected = classOf[ValidationException])
  def testIfInvalidTypesScala(): Unit = {
    testTableApi(('f6 && true).?(5, "false"), "FAIL", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testIfInvalidTypesJava(): Unit = {
    testTableApi("FAIL", "(f8 && true).?(5, 'false')", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidStringComparison1(): Unit = {
    testTableApi("w" === 4, "FAIL", "FAIL")
  }

  @Test(expected = classOf[ValidationException])
  def testInvalidStringComparison2(): Unit = {
    testTableApi("w" > 4.toExpr, "FAIL", "FAIL")
  }

  // ----------------------------------------------------------------------------------------------

  def testData = {
    val testData = new Row(13)
    testData.setField(0, 1: Byte)
    testData.setField(1, 1: Short)
    testData.setField(2, 1)
    testData.setField(3, 1L)
    testData.setField(4, 1.0f)
    testData.setField(5, 1.0d)
    testData.setField(6, true)
    testData.setField(7, 0.0d)
    testData.setField(8, 5)
    testData.setField(9, 10)
    testData.setField(10, "String")
    testData.setField(11, false)
    testData.setField(12, null)
    testData
  }

  def typeInfo = {
    new RowTypeInfo(
      Types.BYTE,
      Types.SHORT,
      Types.INT,
      Types.LONG,
      Types.FLOAT,
      Types.DOUBLE,
      Types.BOOLEAN,
      Types.DOUBLE,
      Types.INT,
      Types.INT,
      Types.STRING,
      Types.BOOLEAN,
      Types.BOOLEAN
      ).asInstanceOf[TypeInformation[Any]]
  }

  override def functions: Map[String, ScalarFunction] = Map(
    "shouldNotExecuteFunc" -> ShouldNotExecuteFunc
  )

}
