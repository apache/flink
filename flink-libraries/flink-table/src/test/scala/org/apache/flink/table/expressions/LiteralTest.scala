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
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.utils.{ExpressionTestBase, Func3}
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.types.Row
import org.junit.Test

class LiteralTest extends ExpressionTestBase {

  @Test
  def testNonAsciiLiteral(): Unit = {
    testAllApis(
      'f0.like("%测试%"),
      "f0.like('%测试%')",
      "f0 LIKE '%测试%'",
      "true")

    testAllApis(
      "Абвгде" + "谢谢",
      "'Абвгде' + '谢谢'",
      "'Абвгде' || '谢谢'",
      "Абвгде谢谢")
  }

  @Test
  def testDoubleQuote(): Unit = {
    val hello = "\"<hello>\""
    testAllApis(
      Func3(42, hello),
      s"Func3(42, '$hello')",
      s"Func3(42, '$hello')",
      s"42 and $hello")
  }

  override def testData: Any = {
    val testData = new Row(1)
    testData.setField(0, "这是个测试字符串")
    testData
  }

  override def typeInfo: TypeInformation[Any] = {
    new RowTypeInfo(
      Types.STRING
    ).asInstanceOf[TypeInformation[Any]]
  }

  override def functions: Map[String, ScalarFunction] = Map(
    "Func3" -> Func3
  )
}
