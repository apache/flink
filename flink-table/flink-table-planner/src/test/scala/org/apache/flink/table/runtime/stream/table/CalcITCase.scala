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

package org.apache.flink.table.runtime.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.expressions.utils._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData, UserDefinedFunctionTestUtils}
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit.{Ignore, Rule, Test}

import scala.collection.mutable

class CalcITCase extends AbstractTestBase {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  val settings: EnvironmentSettings = EnvironmentSettings.newInstance().useOldPlanner().build
  val tEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

  @Test
  def testSimpleSelectAll(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).select('_1, '_2, '_3)

    val results = ds.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
        "1,1,Hi",
        "2,2,Hello",
        "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSimpleSelectEmpty(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv)
      .select()
      .select(1.count)

    val results = ds.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("3")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testSelectStar(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmallNestedTupleDataStream(env).toTable(tEnv).select('*)

    val results = ds.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("(1,1),one", "(2,2),two", "(3,3),three")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSelectFirst(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).select('_1)

    val results = ds.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("1", "2", "3")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSimpleSelectWithNaming(): Unit = {
    // verify ProjectMergeRule.
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv)
      .select('_1 as 'a, '_2 as 'b, '_1 as 'c)
      .select('a, 'b)

    val results = ds.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "1,1", "2,2", "3,2", "4,3", "5,3", "6,3", "7,4",
      "8,4", "9,4", "10,4", "11,5", "12,5", "13,5", "14,5", "15,5",
      "16,6", "17,6", "18,6", "19,6", "20,6", "21,6")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSimpleSelectAllWithAs(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .select('a, 'b, 'c)

    val results = ds.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
        "1,1,Hi",
        "2,2,Hello",
        "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

 @Test
  def testSimpleFilter(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter('a === 3)
    val results = filterDs.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter(false)
    val results = filterDs.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    assertEquals(true, StreamITCase.testResults.isEmpty)
  }

  @Test
  def testAllPassingFilter(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter(true)
    val results = filterDs.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
        "1,1,Hi",
        "2,2,Hello",
        "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testFilterOnIntegerTupleField(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 === 0 )
      .where($"b" === 3 || $"b" === 4 || $"b" === 5)
    val results = filterDs.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "4,3,Hello world, how are you?", "6,3,Luke Skywalker",
      "8,4,Comment#2", "10,4,Comment#4", "12,5,Comment#6", "14,5,Comment#8")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testNotEquals(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 !== 0)
      .where(($"b" !== 1) && ($"b" !== 2) && ($"b" !== 3))
    val results = filterDs.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()
    val expected = mutable.MutableList(
      "7,4,Comment#1", "9,4,Comment#3",
      "11,5,Comment#5", "13,5,Comment#7", "15,5,Comment#9",
      "17,6,Comment#11", "19,6,Comment#13", "21,6,Comment#15")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testUserDefinedFunctionWithParameter(): Unit = {
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))

    StreamITCase.testResults = mutable.MutableList()

    val result = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .where(call("RichFunc2", $"c") === "ABC#Hello")
      .select('c)

    val results = result.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("Hello")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testMultipleUserDefinedFunctions(): Unit = {
    tEnv.registerFunction("RichFunc1", new RichFunc1)
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    StreamITCase.testResults = mutable.MutableList()

    val result = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .where(call("RichFunc2", $"c") === "Abc#Hello" ||
             (call("RichFunc1", $"a") === 3) &&
             ($"b" === 2))
      .select('c)

    val results = result.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("Hello", "Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testScalarFunctionConstructorWithParams(): Unit = {
    StreamITCase.testResults = mutable.MutableList()

    val testData = new mutable.MutableList[(Int, Long, String)]
    testData.+=((1, 1L, "Jack#22"))
    testData.+=((2, 2L, "John#19"))
    testData.+=((3, 2L, "Anna#44"))
    testData.+=((4, 3L, "nosharp"))

    val t = env.fromCollection(testData).toTable(tEnv).as("a", "b", "c")
    val func0 = new Func13("default")
    val func1 = new Func13("Sunny")
    val func2 = new Func13("kevin2")

    val result = t.select(func0('c), func1('c), func2('c))

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "default-Anna#44,Sunny-Anna#44,kevin2-Anna#44",
      "default-Jack#22,Sunny-Jack#22,kevin2-Jack#22",
      "default-John#19,Sunny-John#19,kevin2-John#19",
      "default-nosharp,Sunny-nosharp,kevin2-nosharp"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testInlineScalarFunction(): Unit = {
    StreamITCase.testResults = mutable.MutableList()

    val t = env.fromElements(1, 2, 3, 4).toTable(tEnv).as("a")

    val result = t.select(
      (new ScalarFunction() {
        def eval(i: Int, suffix: String): String = {
          suffix + i
        }
      })('a, ">>"))

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      ">>1",
      ">>2",
      ">>3",
      ">>4"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testNonStaticObjectScalarFunction(): Unit = {
    StreamITCase.testResults = mutable.MutableList()

    val t = env.fromElements(1, 2, 3, 4).toTable(tEnv).as("a")

    val result = t.select(NonStaticObjectScalarFunction('a, ">>"))

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      ">>1",
      ">>2",
      ">>3",
      ">>4"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  object NonStaticObjectScalarFunction extends ScalarFunction {
    def eval(i: Int, suffix: String): String = {
      suffix + i
    }
  }

  @Test(expected = classOf[ValidationException]) // see FLINK-15162
  def testNonStaticClassScalarFunction(): Unit = {
    StreamITCase.testResults = mutable.MutableList()

    val t = env.fromElements(1, 2, 3, 4).toTable(tEnv).as("a")

    val result = t.select(new NonStaticClassScalarFunction()('a, ">>"))

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      ">>1",
      ">>2",
      ">>3",
      ">>4"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  class NonStaticClassScalarFunction extends ScalarFunction {
    def eval(i: Int, suffix: String): String = {
      suffix + i
    }
  }

  @Test
  def testMapType(): Unit = {
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv)
      .select(map('_1, '_3))

    val results = ds.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "{10=Comment#4}",
      "{11=Comment#5}",
      "{12=Comment#6}",
      "{13=Comment#7}",
      "{14=Comment#8}",
      "{15=Comment#9}",
      "{16=Comment#10}",
      "{17=Comment#11}",
      "{18=Comment#12}",
      "{19=Comment#13}",
      "{1=Hi}",
      "{20=Comment#14}",
      "{21=Comment#15}",
      "{2=Hello}",
      "{3=Hello world}",
      "{4=Hello world, how are you?}",
      "{5=I am fine.}",
      "{6=Luke Skywalker}",
      "{7=Comment#1}",
      "{8=Comment#2}",
      "{9=Comment#3}")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testColumnOperation(): Unit = {
    StreamITCase.clear

    val testData = new mutable.MutableList[(Int, Long, String)]
    testData.+=((1, 1L, "Kevin"))
    testData.+=((2, 2L, "Sunny"))

    val t = env.fromCollection(testData).toTable(tEnv).as("a", "b", "c")

    val result = t
      // Adds simple column
      .addColumns("concat(c, 'sunny') as kid")
      // Adds columns by flattening
      .addColumns(row(1, "str").flatten())
      // If the added fields have duplicate field name, then the last one is used.
      .addOrReplaceColumns(concat('c, "_kid") as 'kid, concat('c, "kid") as 'kid)
      // Existing fields will be replaced.
      .addOrReplaceColumns("concat(c, ' is a kid') as kid")
      // Adds value literal column
      .addColumns("'last'")
      // Adds column without alias
      .addColumns('a + 2)
      // Renames columns
      .renameColumns('a as 'a2, 'b as 'b2)
      .renameColumns("c as c2")
      // Drops columns
      .dropColumns('b2)
      .dropColumns("c2")

    result.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "1,Kevin is a kid,1,str,last,3",
      "2,Sunny is a kid,1,str,last,4"
    )
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testMap(): Unit = {
    StreamITCase.testResults = mutable.MutableList()

    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .map(Func23('a, 'b, 'c)).as("a", "b", "c", "d")
      .map(Func24('a, 'b, 'c, 'd)).as("a", "b", "c", "d")
      .map(Func1('b))

    val results = ds.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList(
      "3",
      "4",
      "5")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Ignore("Will be open when FLINK-10834 has been fixed.")
  @Test
  def testNonDeterministic(): Unit = {
    StreamITCase.testResults = mutable.MutableList()

    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .map(Func25('a))

    val results = ds.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    StreamITCase.testResults.foreach { testResult =>
      val result = testResult.split(",")
      assertEquals(result(0), result(1))
    }
  }
}
