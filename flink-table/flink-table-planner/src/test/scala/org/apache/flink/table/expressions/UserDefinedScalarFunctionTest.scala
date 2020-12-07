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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo._
import org.apache.flink.api.common.typeinfo.{BasicArrayTypeInfo, TypeInformation}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.expressions.utils._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.runtime.utils.JavaUserDefinedScalarFunctions._
import org.apache.flink.types.Row

import org.junit.Test

import java.lang.{Boolean => JBoolean}
import java.sql.{Date, Time, Timestamp}

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
      Func1('f11),
      "Func1(f11)",
      "Func1(f11)",
      "4")

    testAllApis(
      Func1('f12),
      "Func1(f12)",
      "Func1(f12)",
      "4")

    testAllApis(
      Func1('f13),
      "Func1(f13)",
      "Func1(f13)",
      "4.0")

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

    // function names containing keywords
    testAllApis(
      Func0('f0),
      "getFunc0(f0)",
      "getFunc0(f0)",
      "42")

    testAllApis(
      Func0('f0),
      "asAlways(f0)",
      "asAlways(f0)",
      "42")

    testAllApis(
      Func0('f0),
      "toWhatever(f0)",
      "toWhatever(f0)",
      "42")

    testAllApis(
      Func0('f0),
      "Nullable(f0)",
      "Nullable(f0)",
      "42")

    // test row type input
    testAllApis(
      Func19('f14),
      "Func19(f14)",
      "Func19(f14)",
      "12,true,1,2,3"
    )
  }

  @Test
  def testNullableParameters(): Unit = {
    testAllApis(
      Func3(nullOf(INT_TYPE_INFO), nullOf(STRING_TYPE_INFO)),
      "Func3(nullOf(INT), nullOf(STRING))",
      "Func3(NULL, NULL)",
      "null and null")

    testAllApis(
      Func3(nullOf(INT_TYPE_INFO), "Test"),
      "Func3(nullOf(INT), 'Test')",
      "Func3(NULL, 'Test')",
      "null and Test")

    testAllApis(
      Func3(42, nullOf(STRING_TYPE_INFO)),
      "Func3(42, nullOf(STRING))",
      "Func3(42, NULL)",
      "42 and null")

    testAllApis(
      Func0(nullOf(INT_TYPE_INFO)),
      "Func0(nullOf(INT))",
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

    testAllApis(
      Func21('f15),
      "Func21(f15)",
      "Func21(f15)",
      "student#Bob")

    testAllApis(
      Func22('f16),
      "Func22(f16)",
      "Func22(f16)",
      "student#Bob")
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
      "1990-10-14 12:10:10.000")
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
  
  @Test
  def testVariableArgs(): Unit = {
    testAllApis(
      Func14(1, 2, 3, 4),
      "Func14(1, 2, 3, 4)",
      "Func14(1, 2, 3, 4)",
      "10")

    // Test for empty arguments
    testAllApis(
      Func14(),
      "Func14()",
      "Func14()",
      "0")

    // Test for override
    testAllApis(
      Func15("Hello"),
      "Func15('Hello')",
      "Func15('Hello')",
      "Hello"
    )

    testAllApis(
      Func15('f1),
      "Func15(f1)",
      "Func15(f1)",
      "Test"
    )

    testAllApis(
      Func15("Hello", 1, 2, 3),
      "Func15('Hello', 1, 2, 3)",
      "Func15('Hello', 1, 2, 3)",
      "Hello3"
    )

    testAllApis(
      Func16('f9),
      "Func16(f9)",
      "Func16(f9)",
      "Hello, World"
    )

    try {
      testAllApis(
        Func17("Hello", "World"),
        "Func17('Hello', 'World')",
        "Func17('Hello', 'World')",
        "Hello, World"
      )
      throw new RuntimeException("Shouldn't be reached here!")
    } catch {
      case ex: ValidationException =>
        // ok
    }

    val JavaFunc2 = new JavaFunc2
    testAllApis(
      JavaFunc2("Hi", 1, 3, 5, 7),
      "JavaFunc2('Hi', 1, 3, 5, 7)",
      "JavaFunc2('Hi', 1, 3, 5, 7)",
      "Hi105")

    // test overloading
    val JavaFunc3 = new JavaFunc3
    testAllApis(
      JavaFunc3("Hi"),
      "JavaFunc3('Hi')",
      "JavaFunc3('Hi')",
      "Hi")

    testAllApis(
      JavaFunc3('f1),
      "JavaFunc3(f1)",
      "JavaFunc3(f1)",
      "Test")
  }

  @Test
  def testJavaBoxedPrimitives(): Unit = {
    val JavaFunc0 = new JavaFunc0()
    val JavaFunc1 = new JavaFunc1()
    val JavaFunc4 = new JavaFunc4()

    testAllApis(
      JavaFunc0('f8),
      "JavaFunc0(f8)",
      "JavaFunc0(f8)",
      "1001"
    )

    testTableApi(
      JavaFunc0(1000L),
      "JavaFunc0(1000L)",
      "1001"
    )

    testAllApis(
      JavaFunc1('f4, 'f5, 'f6),
      "JavaFunc1(f4, f5, f6)",
      "JavaFunc1(f4, f5, f6)",
      "7591 and 43810000 and 655906210000")

    testAllApis(
      JavaFunc1(nullOf(Types.SQL_TIME), 15, nullOf(Types.SQL_TIMESTAMP)),
      "JavaFunc1(nullOf(SQL_TIME), 15, nullOf(SQL_TIMESTAMP))",
      "JavaFunc1(NULL, 15, NULL)",
      "null and 15 and null")

    testAllApis(
      JavaFunc4('f10, array("a", "b", "c")),
      "JavaFunc4(f10, array('a', 'b', 'c'))",
      "JavaFunc4(f10, array['a', 'b', 'c'])",
      "[1, 2, null] and [a, b, c]"
    )
  }

  @Test
  def testRichFunctions(): Unit = {
    val richFunc0 = new RichFunc0
    val richFunc1 = new RichFunc1
    val richFunc2 = new RichFunc2
    testAllApis(
      richFunc0('f0),
      "RichFunc0(f0)",
      "RichFunc0(f0)",
      "43")

    testAllApis(
      richFunc1('f0),
      "RichFunc1(f0)",
      "RichFunc1(f0)",
      "42")

    testAllApis(
      richFunc2('f1),
      "RichFunc2(f1)",
      "RichFunc2(f1)",
      "#Test")
  }

  // ----------------------------------------------------------------------------------------------

  override def testData: Any = {
    val testData = new Row(17)
    testData.setField(0, 42)
    testData.setField(1, "Test")
    testData.setField(2, null)
    testData.setField(3, SimplePojo("Bob", 36))
    testData.setField(4, Date.valueOf("1990-10-14"))
    testData.setField(5, Time.valueOf("12:10:10"))
    testData.setField(6, Timestamp.valueOf("1990-10-14 12:10:10"))
    testData.setField(7, 12)
    testData.setField(8, 1000L)
    testData.setField(9, Seq("Hello", "World"))
    testData.setField(10, Array[Integer](1, 2, null))
    testData.setField(11, 3.toByte)
    testData.setField(12, 3.toShort)
    testData.setField(13, 3.toFloat)
    testData.setField(14, Row.of(
      12.asInstanceOf[Integer],
      true.asInstanceOf[JBoolean],
      Row.of(1.asInstanceOf[Integer], 2.asInstanceOf[Integer], 3.asInstanceOf[Integer]))
    )
    testData.setField(15, new GraduatedStudent("Bob"))
    testData.setField(16, Array(new GraduatedStudent("Bob")))
    testData
  }

  override def typeInfo: TypeInformation[Any] = {
    new RowTypeInfo(
      Types.INT,
      Types.STRING,
      Types.BOOLEAN,
      TypeInformation.of(classOf[SimplePojo]),
      Types.SQL_DATE,
      Types.SQL_TIME,
      Types.SQL_TIMESTAMP,
      Types.INTERVAL_MONTHS,
      Types.INTERVAL_MILLIS,
      TypeInformation.of(classOf[Seq[String]]),
      BasicArrayTypeInfo.INT_ARRAY_TYPE_INFO,
      Types.BYTE,
      Types.SHORT,
      Types.FLOAT,
      Types.ROW(Types.INT, Types.BOOLEAN, Types.ROW(Types.INT, Types.INT, Types.INT)),
      TypeInformation.of(classOf[GraduatedStudent]),
      TypeInformation.of(classOf[Array[GraduatedStudent]])
    ).asInstanceOf[TypeInformation[Any]]
  }

  override def functions: Map[String, ScalarFunction] = Map(
    "Func0" -> Func0,
    "getFunc0" -> Func0,
    "asAlways" -> Func0,
    "toWhatever" -> Func0,
    "Nullable" -> Func0,
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
    "Func12" -> Func12,
    "Func14" -> Func14,
    "Func15" -> Func15,
    "Func16" -> Func16,
    "Func17" -> Func17,
    "Func19" -> Func19,
    "Func21" -> Func21,
    "Func22" -> Func22,
    "JavaFunc0" -> new JavaFunc0,
    "JavaFunc1" -> new JavaFunc1,
    "JavaFunc2" -> new JavaFunc2,
    "JavaFunc3" -> new JavaFunc3,
    "JavaFunc4" -> new JavaFunc4,
    "RichFunc0" -> new RichFunc0,
    "RichFunc1" -> new RichFunc1,
    "RichFunc2" -> new RichFunc2
  )
}

