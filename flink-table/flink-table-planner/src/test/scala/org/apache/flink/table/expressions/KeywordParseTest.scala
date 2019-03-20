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

import org.apache.flink.table.expressions.ApiExpressionUtils.{call, unresolvedFieldRef, unresolvedCall}
import org.junit.Assert.assertEquals
import org.junit.Test

/**
  * Tests keyword as suffix.
  */
class KeywordParseTest {

  @Test
  def testKeyword(): Unit = {
    assertEquals(
      call(BuiltInFunctionDefinitions.ORDER_ASC, unresolvedFieldRef("f0")),
      ExpressionParser.parseExpression("f0.asc"))
    assertEquals(
      call(BuiltInFunctionDefinitions.ORDER_ASC, unresolvedFieldRef("f0")),
      ExpressionParser.parseExpression("f0.asc()"))
  }

  @Test
  def testKeywordAsPrefixInFunctionName(): Unit = {
    assertEquals(
      unresolvedCall("ascii", unresolvedFieldRef("f0")),
      ExpressionParser.parseExpression("f0.ascii()"))
  }

  @Test
  def testKeywordAsInfixInFunctionName(): Unit = {
    assertEquals(
      unresolvedCall("iiascii", unresolvedFieldRef("f0")),
      ExpressionParser.parseExpression("f0.iiascii()"))
  }

  @Test
  def testKeywordAsSuffixInFunctionName(): Unit = {
    assertEquals(
      unresolvedCall("iiasc", unresolvedFieldRef("f0")),
      ExpressionParser.parseExpression("f0.iiasc()"))
  }
}
