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
  def testFunctionNameContainsSuffixKeyword(): Unit = {
    // ASC / DESC (no parameter)
    Assert.assertEquals(
      ((ExpressionParser.parseExpression("f0.ascii()")).asInstanceOf[Call]).functionName,
      "ASCII")
    Assert.assertEquals(
      ((ExpressionParser.parseExpression("f0.iiascii()")).asInstanceOf[Call]).functionName,
      "IIASCII")
    Assert.assertEquals(
      ((ExpressionParser.parseExpression("f0.iiasc()")).asInstanceOf[Call]).functionName,
      "IIASC")
    Assert.assertEquals(
      ((ExpressionParser.parseExpression("f0.descabc()")).asInstanceOf[Call]).functionName,
      "DESCABC")
    Assert.assertEquals(
      ((ExpressionParser.parseExpression("f0.abcdescabc()")).asInstanceOf[Call]).functionName,
      "ABCDESCABC")
    Assert.assertEquals(
      ((ExpressionParser.parseExpression("f0.abcdesc()")).asInstanceOf[Call]).functionName,
      "ABCDESC")
    // LOG has parameter
    Assert.assertEquals(
      ((ExpressionParser.parseExpression("f0.logabc()")).asInstanceOf[Call]).functionName,
      "LOGABC")
    Assert.assertEquals(
      ((ExpressionParser.parseExpression("f0.abclogabc()")).asInstanceOf[Call]).functionName,
      "ABCLOGABC")
    Assert.assertEquals(
      ((ExpressionParser.parseExpression("f0.abclog()")).asInstanceOf[Call]).functionName,
      "ABCLOG")
    Assert.assertEquals(
      ((ExpressionParser.parseExpression("f0.log2()")).asInstanceOf[Call]).functionName,
      "LOG2")
    Assert.assertEquals(
      ((ExpressionParser.parseExpression("f0.log10()")).asInstanceOf[Call]).functionName,
      "LOG10")
  }

  override def testData: Any = new Row(0)

  override def typeInfo: TypeInformation[Any] =
    new RowTypeInfo().asInstanceOf[TypeInformation[Any]]
}
