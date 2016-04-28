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

package org.apache.flink.api.scala.expression

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.expression.utils.ExpressionEvaluator
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.expressions.{Expression, ExpressionParser}
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.junit.Assert.assertEquals
import org.junit.Test

class ScalarFunctionsTest {

  // ----------------------------------------------------------------------------------------------
  // String functions
  // ----------------------------------------------------------------------------------------------

  @Test
  def testSubstring(): Unit = {
    testFunction(
      'f0.substring(2),
      "f0.substring(2)",
      "SUBSTRING(f0, 2)",
      "his is a test String.")

    testFunction(
      'f0.substring(2, 5),
      "f0.substring(2, 5)",
      "SUBSTRING(f0, 2, 5)",
      "his i")

    testFunction(
      'f0.substring(1, 'f7),
      "f0.substring(1, f7)",
      "SUBSTRING(f0, 1, f7)",
      "Thi")
  }

  @Test
  def testTrim(): Unit = {
    testFunction(
      'f8.trim(),
      "f8.trim()",
      "TRIM(f8)",
      "This is a test String.")

    testFunction(
      'f8.trim(removeLeading = true, removeTrailing = true, " "),
      "trim(f8)",
      "TRIM(f8)",
      "This is a test String.")

    testFunction(
      'f8.trim(removeLeading = false, removeTrailing = true, " "),
      "f8.trim(TRAILING, ' ')",
      "TRIM(TRAILING FROM f8)",
      " This is a test String.")

    testFunction(
      'f0.trim(removeLeading = true, removeTrailing = true, "."),
      "trim(BOTH, '.', f0)",
      "TRIM(BOTH '.' FROM f0)",
      "This is a test String")
  }

  @Test
  def testCharLength(): Unit = {
    testFunction(
      'f0.charLength(),
      "f0.charLength()",
      "CHAR_LENGTH(f0)",
      "22")

    testFunction(
      'f0.charLength(),
      "charLength(f0)",
      "CHARACTER_LENGTH(f0)",
      "22")
  }

  @Test
  def testUpperCase(): Unit = {
    testFunction(
      'f0.upperCase(),
      "f0.upperCase()",
      "UPPER(f0)",
      "THIS IS A TEST STRING.")
  }

  @Test
  def testLowerCase(): Unit = {
    testFunction(
      'f0.lowerCase(),
      "f0.lowerCase()",
      "LOWER(f0)",
      "this is a test string.")
  }

  @Test
  def testInitCap(): Unit = {
    testFunction(
      'f0.initCap(),
      "f0.initCap()",
      "INITCAP(f0)",
      "This Is A Test String.")
  }

  @Test
  def testConcat(): Unit = {
    testFunction(
      'f0 + 'f0,
      "f0 + f0",
      "f0||f0",
      "This is a test String.This is a test String.")
  }

  @Test
  def testLike(): Unit = {
    testFunction(
      'f0.like("Th_s%"),
      "f0.like('Th_s%')",
      "f0 LIKE 'Th_s%'",
      "true")

    testFunction(
      'f0.like("%is a%"),
      "f0.like('%is a%')",
      "f0 LIKE '%is a%'",
      "true")
  }

  @Test
  def testNotLike(): Unit = {
    testFunction(
      !'f0.like("Th_s%"),
      "!f0.like('Th_s%')",
      "f0 NOT LIKE 'Th_s%'",
      "false")

    testFunction(
      !'f0.like("%is a%"),
      "!f0.like('%is a%')",
      "f0 NOT LIKE '%is a%'",
      "false")
  }

  @Test
  def testSimilar(): Unit = {
    testFunction(
      'f0.similar("_*"),
      "f0.similar('_*')",
      "f0 SIMILAR TO '_*'",
      "true")

    testFunction(
      'f0.similar("This (is)? a (test)+ Strin_*"),
      "f0.similar('This (is)? a (test)+ Strin_*')",
      "f0 SIMILAR TO 'This (is)? a (test)+ Strin_*'",
      "true")
  }

  @Test
  def testNotSimilar(): Unit = {
    testFunction(
      !'f0.similar("_*"),
      "!f0.similar('_*')",
      "f0 NOT SIMILAR TO '_*'",
      "false")

    testFunction(
      !'f0.similar("This (is)? a (test)+ Strin_*"),
      "!f0.similar('This (is)? a (test)+ Strin_*')",
      "f0 NOT SIMILAR TO 'This (is)? a (test)+ Strin_*'",
      "false")
  }

  @Test
  def testMod(): Unit = {
    testFunction(
      'f4.mod('f7),
      "f4.mod(f7)",
      "MOD(f4, f7)",
      "2")

    testFunction(
      'f4.mod(3),
      "mod(f4, 3)",
      "MOD(f4, 3)",
      "2")

    testFunction(
      'f4 % 3,
      "mod(44, 3)",
      "MOD(44, 3)",
      "2")

  }

  @Test
  def testExp(): Unit = {
    testFunction(
      'f2.exp(),
      "f2.exp()",
      "EXP(f2)",
      math.exp(42.toByte).toString)

    testFunction(
      'f3.exp(),
      "f3.exp()",
      "EXP(f3)",
      math.exp(43.toShort).toString)

    testFunction(
      'f4.exp(),
      "f4.exp()",
      "EXP(f4)",
      math.exp(44.toLong).toString)

    testFunction(
      'f5.exp(),
      "f5.exp()",
      "EXP(f5)",
      math.exp(4.5.toFloat).toString)

    testFunction(
      'f6.exp(),
      "f6.exp()",
      "EXP(f6)",
      math.exp(4.6).toString)

    testFunction(
      'f7.exp(),
      "exp(3)",
      "EXP(3)",
      math.exp(3).toString)
  }

  @Test
  def testLog10(): Unit = {
    testFunction(
      'f2.log10(),
      "f2.log10()",
      "LOG10(f2)",
      math.log10(42.toByte).toString)

    testFunction(
      'f3.log10(),
      "f3.log10()",
      "LOG10(f3)",
      math.log10(43.toShort).toString)

    testFunction(
      'f4.log10(),
      "f4.log10()",
      "LOG10(f4)",
      math.log10(44.toLong).toString)

    testFunction(
      'f5.log10(),
      "f5.log10()",
      "LOG10(f5)",
      math.log10(4.5.toFloat).toString)

    testFunction(
      'f6.log10(),
      "f6.log10()",
      "LOG10(f6)",
      math.log10(4.6).toString)
  }

  @Test
  def testPower(): Unit = {
    testFunction(
      'f2.power('f7),
      "f2.power(f7)",
      "POWER(f2, f7)",
      math.pow(42.toByte, 3).toString)

    testFunction(
      'f3.power('f6),
      "f3.power(f6)",
      "POWER(f3, f6)",
      math.pow(43.toShort, 4.6D).toString)

    testFunction(
      'f4.power('f5),
      "f4.power(f5)",
      "POWER(f4, f5)",
      math.pow(44.toLong, 4.5.toFloat).toString)
  }

  @Test
  def testLn(): Unit = {
    testFunction(
      'f2.ln(),
      "f2.ln()",
      "LN(f2)",
      math.log(42.toByte).toString)

    testFunction(
      'f3.ln(),
      "f3.ln()",
      "LN(f3)",
      math.log(43.toShort).toString)

    testFunction(
      'f4.ln(),
      "f4.ln()",
      "LN(f4)",
      math.log(44.toLong).toString)

    testFunction(
      'f5.ln(),
      "f5.ln()",
      "LN(f5)",
      math.log(4.5.toFloat).toString)

    testFunction(
      'f6.ln(),
      "f6.ln()",
      "LN(f6)",
      math.log(4.6).toString)
  }

  @Test
  def testAbs(): Unit = {
    testFunction(
      'f2.abs(),
      "f2.abs()",
      "ABS(f2)",
      "42")

    testFunction(
      'f3.abs(),
      "f3.abs()",
      "ABS(f3)",
      "43")

    testFunction(
      'f4.abs(),
      "f4.abs()",
      "ABS(f4)",
      "44")

    testFunction(
      'f5.abs(),
      "f5.abs()",
      "ABS(f5)",
      "4.5")

    testFunction(
      'f6.abs(),
      "f6.abs()",
      "ABS(f6)",
      "4.6")

    testFunction(
      'f9.abs(),
      "f9.abs()",
      "ABS(f9)",
      "42")

    testFunction(
      'f10.abs(),
      "f10.abs()",
      "ABS(f10)",
      "43")

    testFunction(
      'f11.abs(),
      "f11.abs()",
      "ABS(f11)",
      "44")

    testFunction(
      'f12.abs(),
      "f12.abs()",
      "ABS(f12)",
      "4.5")

    testFunction(
      'f13.abs(),
      "f13.abs()",
      "ABS(f13)",
      "4.6")
  }

  // ----------------------------------------------------------------------------------------------

  def testFunction(
      expr: Expression,
      exprString: String,
      sqlExpr: String,
      expected: String): Unit = {
    val testData = new Row(15)
    testData.setField(0, "This is a test String.")
    testData.setField(1, true)
    testData.setField(2, 42.toByte)
    testData.setField(3, 43.toShort)
    testData.setField(4, 44.toLong)
    testData.setField(5, 4.5.toFloat)
    testData.setField(6, 4.6)
    testData.setField(7, 3)
    testData.setField(8, " This is a test String. ")
    testData.setField(9, -42.toByte)
    testData.setField(10, -43.toShort)
    testData.setField(11, -44.toLong)
    testData.setField(12, -4.5.toFloat)
    testData.setField(13, -4.6)
    testData.setField(14, -3)

    val typeInfo = new RowTypeInfo(Seq(
      STRING_TYPE_INFO,
      BOOLEAN_TYPE_INFO,
      BYTE_TYPE_INFO,
      SHORT_TYPE_INFO,
      LONG_TYPE_INFO,
      FLOAT_TYPE_INFO,
      DOUBLE_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO,
      BYTE_TYPE_INFO,
      SHORT_TYPE_INFO,
      LONG_TYPE_INFO,
      FLOAT_TYPE_INFO,
      DOUBLE_TYPE_INFO,
      INT_TYPE_INFO)).asInstanceOf[TypeInformation[Any]]

    val exprResult = ExpressionEvaluator.evaluate(testData, typeInfo, expr)
    assertEquals(expected, exprResult)

    val exprStringResult = ExpressionEvaluator.evaluate(
      testData,
      typeInfo,
      ExpressionParser.parseExpression(exprString))
    assertEquals(expected, exprStringResult)

    val exprSqlResult = ExpressionEvaluator.evaluate(testData, typeInfo, sqlExpr)
    assertEquals(expected, exprSqlResult)
  }



}
