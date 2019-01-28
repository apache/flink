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
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row
import org.junit.{Assert, Test}

/**
  * Tests keyword as suffix.
  */
class KeywordParseTest extends ExpressionTestBase {

  @Test
  def testKeyword(): Unit = {
    Assert.assertEquals(
      ExpressionParser.parseExpression("f0.asc"),
      Asc(UnresolvedFieldReference("f0")))
    Assert.assertEquals(
      ExpressionParser.parseExpression("f0.asc()"),
      Asc(UnresolvedFieldReference("f0")))
  }

  @Test
  def testKeywordAsPrefixInFunctionName(): Unit = {
    Assert.assertEquals(
      ExpressionParser.parseExpression("f0.ascii()").asInstanceOf[Call].functionName,
      "ASCII")
  }

  @Test
  def testKeywordAsInfixInFunctionName(): Unit = {
    Assert.assertEquals(
      ExpressionParser.parseExpression("f0.iiascii()").asInstanceOf[Call].functionName,
      "IIASCII")
  }

  @Test
  def testKeywordAsSuffixInFunctionName(): Unit = {
    Assert.assertEquals(
      ExpressionParser.parseExpression("f0.iiasc()").asInstanceOf[Call].functionName,
      "IIASC")
  }

  override def testData: Any = new Row(0)

  override def typeInfo: TypeInformation[Any] =
    new RowTypeInfo().asInstanceOf[TypeInformation[Any]]
}
