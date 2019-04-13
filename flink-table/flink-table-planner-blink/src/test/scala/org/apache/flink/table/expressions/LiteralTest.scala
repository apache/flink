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

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.expressions.utils.ExpressionTestBase
import org.apache.flink.types.Row
import org.junit.Test

class LiteralTest extends ExpressionTestBase {

  @Test
  def testFieldWithBooleanPrefix(): Unit = {

    testSqlApi(
      "trUeX",
      "trUeX_value"
    )

    testSqlApi(
      "FALSE_A",
      "FALSE_A_value"
    )

    testSqlApi(
      "FALSE_AB",
      "FALSE_AB_value"
    )

    testSqlApi(
      "trUe",
      "true"
    )

    testSqlApi(
      "FALSE",
      "false"
    )
  }

  @Test
  def testNonAsciiLiteral(): Unit = {
    testSqlApi(
      "f4 LIKE '%测试%'",
      "true")

    testSqlApi(
      "'Абвгде' || '谢谢'",
      "Абвгде谢谢")
  }

  @Test
  def testDoubleQuote(): Unit = {
    val hello = "\"<hello>\""
    testSqlApi(
      s"concat('a', ' ', '$hello')",
      s"a $hello")
  }

  @Test
  def testStringLiterals(): Unit = {

    // these tests use Java/Scala escaping for non-quoting unicode characters

    testSqlApi(
      "'>\n<'",
      ">\n<")

    testSqlApi(
      "'>\u263A<'",
      ">\u263A<")

    testSqlApi(
      "'>\u263A<'",
      ">\u263A<")

    testSqlApi(
      "'>\\<'",
      ">\\<")

    testSqlApi(
      "'>''<'",
      ">'<")

    testSqlApi(
      "' '",
      " ")

    testSqlApi(
      "''",
      "")

    testSqlApi(
      "'>foo([\\w]+)<'",
      ">foo([\\w]+)<")

    testSqlApi(
      "'>\\''\n<'",
      ">\\'\n<")

    testSqlApi(
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
