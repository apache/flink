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

package org.apache.flink.api.scala.expression.utils

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.table.expressions.utils.ExpressionEvaluator
import org.apache.flink.api.table.expressions.{Expression, ExpressionParser}
import org.junit.Assert._

/**
  * Base test class for expression tests.
  */
abstract class ExpressionTestBase {

  def testData: Any

  def typeInfo: TypeInformation[Any]

  def testAllApis(
      expr: Expression,
      exprString: String,
      sqlExpr: String,
      expected: String): Unit = {
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

  def testTableApi(
      expr: Expression,
      exprString: String,
      expected: String): Unit = {
    val exprResult = ExpressionEvaluator.evaluate(testData, typeInfo, expr)
    assertEquals(expected, exprResult)

    val exprStringResult = ExpressionEvaluator.evaluate(
      testData,
      typeInfo,
      ExpressionParser.parseExpression(exprString))
    assertEquals(expected, exprStringResult)
  }

  def testSqlApi(
      sqlExpr: String,
      expected: String): Unit = {
    val exprSqlResult = ExpressionEvaluator.evaluate(testData, typeInfo, sqlExpr)
    assertEquals(expected, exprSqlResult)
  }

}
