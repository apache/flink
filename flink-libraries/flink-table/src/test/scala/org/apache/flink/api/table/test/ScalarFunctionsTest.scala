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

  // ----------------------------------------------------------------------------------------------

  def testFunction(
      expr: Expression,
      exprString: String,
      sqlExpr: String,
      expected: String): Unit = {
    val testData = new Row(8)
    testData.setField(0, "This is a test String.")
    testData.setField(1, true)
    testData.setField(2, 42.toByte)
    testData.setField(3, 43.toShort)
    testData.setField(4, 44.toLong)
    testData.setField(5, 4.5.toFloat)
    testData.setField(6, 4.6)
    testData.setField(7, 3)

    val typeInfo = new RowTypeInfo(Seq(
      STRING_TYPE_INFO,
      BOOLEAN_TYPE_INFO,
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

    // TODO test SQL expression
  }



}
