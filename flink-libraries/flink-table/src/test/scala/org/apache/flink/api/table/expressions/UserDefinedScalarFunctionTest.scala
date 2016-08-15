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

package org.apache.flink.api.table.expressions

import java.sql.{Date, Time, Timestamp}

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.expressions.utils._
import org.apache.flink.api.table.functions.UserDefinedFunction
import org.apache.flink.api.table.typeutils.RowTypeInfo
import org.apache.flink.api.table.{Row, Types}
import org.junit.Test

class UserDefinedScalarFunctionTest extends ExpressionTestBase {

  @Test
  def testParameters(): Unit = {
    testAllApis(
      Func0('f0),
      "Func0(f0)",
      "Func0(f0)",
      "42")

    testAllApis(
      Func1('f0),
      "Func1(f0)",
      "Func1(f0)",
      "43")

    testAllApis(
      Func2('f0, 'f1, 'f3),
      "Func2(f0, f1, f3)",
      "Func2(f0, f1, f3)",
      "42 and Test and SimplePojo(Bob,36)")

    testAllApis(
      Func0(123),
      "Func0(123)",
      "Func0(123)",
      "123")

    testAllApis(
      Func6('f4, 'f5, 'f6),
      "Func6(f4, f5, f6)",
      "Func6(f4, f5, f6)",
      "(1990-10-14,12:10:10,1990-10-14 12:10:10.0)")
  }

  @Test
  def testNullableParameters(): Unit = {
    testAllApis(
      Func3(Null(INT_TYPE_INFO), Null(STRING_TYPE_INFO)),
      "Func3(Null(INT), Null(STRING))",
      "Func3(NULL, NULL)",
      "null and null")

    testAllApis(
      Func3(Null(INT_TYPE_INFO), "Test"),
      "Func3(Null(INT), 'Test')",
      "Func3(NULL, 'Test')",
      "null and Test")

    testAllApis(
      Func3(42, Null(STRING_TYPE_INFO)),
      "Func3(42, Null(STRING))",
      "Func3(42, NULL)",
      "42 and null")

    testAllApis(
      Func0(Null(INT_TYPE_INFO)),
      "Func0(Null(INT))",
      "Func0(NULL)",
      "-1")
  }

  @Test
  def testResults(): Unit = {
    testAllApis(
      Func4(),
      "Func4()",
      "Func4()",
      "null")

    testAllApis(
      Func5(),
      "Func5()",
      "Func5()",
      "-1")
  }

  @Test
  def testNesting(): Unit = {
    testAllApis(
      Func0(Func0('f0)),
      "Func0(Func0(f0))",
      "Func0(Func0(f0))",
      "42")

    testAllApis(
      Func0(Func0('f0)),
      "Func0(Func0(f0))",
      "Func0(Func0(f0))",
      "42")

    testAllApis(
      Func7(Func7(Func7(1, 1), Func7(1, 1)), Func7(Func7(1, 1), Func7(1, 1))),
      "Func7(Func7(Func7(1, 1), Func7(1, 1)), Func7(Func7(1, 1), Func7(1, 1)))",
      "Func7(Func7(Func7(1, 1), Func7(1, 1)), Func7(Func7(1, 1), Func7(1, 1)))",
      "8")
  }

  @Test
  def testOverloadedParameters(): Unit = {
    testAllApis(
      Func8(1),
      "Func8(1)",
      "Func8(1)",
      "a")

    testAllApis(
      Func8(1, 1),
      "Func8(1, 1)",
      "Func8(1, 1)",
      "b")

    testAllApis(
      Func8("a", "a"),
      "Func8('a', 'a')",
      "Func8('a', 'a')",
      "c")
  }

  @Test
  def testTimePointsOnPrimitives(): Unit = {
    testAllApis(
      Func9('f4, 'f5, 'f6),
      "Func9(f4, f5, f6)",
      "Func9(f4, f5, f6)",
      "7591 and 43810000 and 655906210000")

    testAllApis(
      Func10('f6),
      "Func10(f6)",
      "Func10(f6)",
      "1990-10-14 12:10:10.0")
  }

  @Test
  def testTimeIntervalsOnPrimitives(): Unit = {
    testAllApis(
      Func11('f7, 'f8),
      "Func11(f7, f8)",
      "Func11(f7, f8)",
      "12 and 1000")

    testAllApis(
      Func12('f8),
      "Func12(f8)",
      "Func12(f8)",
      "+0 00:00:01.000")
  }

  // ----------------------------------------------------------------------------------------------

  override def testData: Any = {
    val testData = new Row(9)
    testData.setField(0, 42)
    testData.setField(1, "Test")
    testData.setField(2, null)
    testData.setField(3, SimplePojo("Bob", 36))
    testData.setField(4, Date.valueOf("1990-10-14"))
    testData.setField(5, Time.valueOf("12:10:10"))
    testData.setField(6, Timestamp.valueOf("1990-10-14 12:10:10"))
    testData.setField(7, 12)
    testData.setField(8, 1000L)
    testData
  }

  override def typeInfo: TypeInformation[Any] = {
    new RowTypeInfo(Seq(
      Types.INT,
      Types.STRING,
      Types.BOOLEAN,
      TypeInformation.of(classOf[SimplePojo]),
      Types.DATE,
      Types.TIME,
      Types.TIMESTAMP,
      Types.INTERVAL_MONTHS,
      Types.INTERVAL_MILLIS
    )).asInstanceOf[TypeInformation[Any]]
  }

  override def functions: Map[String, UserDefinedFunction] = Map(
    "Func0" -> Func0,
    "Func1" -> Func1,
    "Func2" -> Func2,
    "Func3" -> Func3,
    "Func4" -> Func4,
    "Func5" -> Func5,
    "Func6" -> Func6,
    "Func7" -> Func7,
    "Func8" -> Func8,
    "Func9" -> Func9,
    "Func10" -> Func10,
    "Func11" -> Func11,
    "Func12" -> Func12
  )
}

