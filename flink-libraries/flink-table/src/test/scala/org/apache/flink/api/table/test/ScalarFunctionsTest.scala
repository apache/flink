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

package org.apache.flink.api.table.test

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.Row
import org.apache.flink.api.table.expressions.Expression
import org.apache.flink.api.table.parser.ExpressionParser
import org.apache.flink.api.table.test.utils.ExpressionEvaluator
import org.apache.flink.api.table.typeinfo.RowTypeInfo
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

  // ----------------------------------------------------------------------------------------------

  def testFunction(
      expr: Expression,
      exprString: String,
      sqlExpr: String,
      expected: String): Unit = {
    val testData = new Row(9)
    testData.setField(0, "This is a test String.")
    testData.setField(1, true)
    testData.setField(2, 42.toByte)
    testData.setField(3, 43.toShort)
    testData.setField(4, 44.toLong)
    testData.setField(5, 4.5.toFloat)
    testData.setField(6, 4.6)
    testData.setField(7, 3)
    testData.setField(8, " This is a test String. ")

    val typeInfo = new RowTypeInfo(Seq(
      STRING_TYPE_INFO,
      BOOLEAN_TYPE_INFO,
      BYTE_TYPE_INFO,
      SHORT_TYPE_INFO,
      LONG_TYPE_INFO,
      FLOAT_TYPE_INFO,
      DOUBLE_TYPE_INFO,
      INT_TYPE_INFO,
      STRING_TYPE_INFO)).asInstanceOf[TypeInformation[Any]]

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
