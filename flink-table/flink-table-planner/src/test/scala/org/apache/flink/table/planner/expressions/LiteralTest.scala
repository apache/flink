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

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.expressions.utils.{ExpressionTestBase, Func3}
import org.apache.flink.types.Row

import org.junit.Test

class LiteralTest extends ExpressionTestBase {

  @Test
  def testFieldWithBooleanPrefix(): Unit = {

    testAllApis(
      'trUeX,
      "trUeX",
      "trUeX",
      "trUeX_value"
    )

    testAllApis(
      'FALSE_A,
      "FALSE_A",
      "FALSE_A",
      "FALSE_A_value"
    )

    testAllApis(
      'FALSE_AB,
      "FALSE_AB",
      "FALSE_AB",
      "FALSE_AB_value"
    )

    testAllApis(
      true,
      "trUe",
      "trUe",
      "true"
    )

    testAllApis(
      false,
      "FALSE",
      "FALSE",
      "false"
    )
  }

  @Test
  def testNonAsciiLiteral(): Unit = {
    testAllApis(
      'f4.like("%测试%"),
      "f4.like('%测试%')",
      "f4 LIKE '%测试%'",
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

  @Test
  def testStringLiterals(): Unit = {

    // these tests use Java/Scala escaping for non-quoting unicode characters

    testAllApis(
      ">\n<",
      "'>\n<'",
      "'>\n<'",
      ">\n<")

    testAllApis(
      ">\u263A<",
      "'>\u263A<'",
      "'>\u263A<'",
      ">\u263A<")

    testAllApis(
      ">\\<",
      "'>\\<'",
      "'>\\<'",
      ">\\<")

    testAllApis(
      ">'<",
      "'>''<'",
      "'>''<'",
      ">'<")

    testAllApis(
      " ",
      "' '",
      "' '",
      " ")

    testAllApis(
      "",
      "''",
      "''",
      "")

    testAllApis(
      ">foo([\\w]+)<",
      "'>foo([\\w]+)<'",
      "'>foo([\\w]+)<'",
      ">foo([\\w]+)<")

    testAllApis(
      ">\\'\n<",
      "\">\\'\n<\"",
      "'>\\''\n<'",
      ">\\'\n<")

    testAllApis(
      "It's me.",
      "'It''s me.'",
      "'It''s me.'",
      "It's me.")

    // these test use SQL for describing unicode characters

    testSqlApi(
      "U&'>\\263A<'", // default escape backslash
      ">\u263A<")

    testSqlApi(
      "U&'>#263A<' UESCAPE '#'", // custom escape '#'
      ">\u263A<")

    testSqlApi(
      """'>\\<'""",
      ">\\\\<")
  }

  override def functions: Map[String, ScalarFunction] = Map(
    "Func3" -> Func3
  )

  override def testData: Row = {
    val testData = new Row(4)
    testData.setField(0, "trUeX_value")
    testData.setField(1, "FALSE_A_value")
    testData.setField(2, "FALSE_AB_value")
    testData.setField(3, "这是个测试字符串")
    testData
  }

  override def typeInfo : RowTypeInfo = {
    new RowTypeInfo(
      Array(
        /* 0 */  Types.STRING,
        /* 1 */  Types.STRING,
        /* 2 */  Types.STRING,
        /* 3 */  Types.STRING
      ).asInstanceOf[Array[TypeInformation[_]]],
      Array("trUeX", "FALSE_A", "FALSE_AB", "f4")
    )
  }
}
