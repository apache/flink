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

package org.apache.flink.table.planner.runtime.stream.table

import java.util

import org.apache.flink.api.scala._
import org.apache.flink.table.annotation.{DataTypeHint, InputGroup}
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.expressions.utils._
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingAppendSink, TestingRetractSink, UserDefinedFunctionTestUtils}
import org.apache.flink.table.utils.LegacyRowResource
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.annotation.varargs
import scala.collection.{Seq, mutable}

@RunWith(classOf[Parameterized])
class CalcITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Rule
  def usesLegacyRows: LegacyRowResource = LegacyRowResource.INSTANCE

  @Test
  def testFunctionSplitWhenCodegenOverLengthLimit(): Unit = {
    // test function split
    val udfLen = TestUDFLength
    tEnv.registerFunction("RichFunc1", new RichFunc1)
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    val result = env.fromCollection(tupleData3)
      .toTable(tEnv, 'a, 'b, 'c)
      .where(call("RichFunc2", $"c") === "Abc#Hello" || call("RichFunc1", $"a") === 3 && $"b" === 2)
      .select('c, udfLen('c) as 'len)

    val sink = new TestingAppendSink
    result.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Hello,5", "Hello world,11")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSimpleSelectAll(): Unit = {
    val ds = env.fromCollection(smallTupleData3).toTable(tEnv)

    val sink = new TestingAppendSink
    ds.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,Hi",
      "2,2,Hello",
      "3,2,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSimpleSelectEmpty(): Unit = {
    val ds = env.fromCollection(smallTupleData3).toTable(tEnv)
      .select()
      .select(lit("1").count())

    val sink = new TestingRetractSink
    ds.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("3")
    assertEquals(expected.sorted, sink.getRetractResults.sorted)
  }

  @Test
  def testSelectStar(): Unit = {
    val ds = env.fromCollection(smallNestedTupleData).toTable(tEnv, '_1, '_2).select('*)

    val sink = new TestingAppendSink
    ds.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList("1,1,one", "2,2,two", "3,3,three")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSelectFirst(): Unit = {
    val ds = env.fromCollection(smallTupleData3).toTable(tEnv).select('_1)

    val sink = new TestingAppendSink
    ds.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList("1", "2", "3")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSimpleSelectWithNaming(): Unit = {

    // verify ProjectMergeRule.
    val ds = env.fromCollection(tupleData3).toTable(tEnv)
      .select('_1 as 'a, '_2 as 'b, '_1 as 'c)
      .select('a, 'b)

    val sink = new TestingAppendSink
    ds.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1", "2,2", "3,2", "4,3", "5,3", "6,3", "7,4",
      "8,4", "9,4", "10,4", "11,5", "12,5", "13,5", "14,5", "15,5",
      "16,6", "17,6", "18,6", "19,6", "20,6", "21,6")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSimpleSelectAllWithAs(): Unit = {
    val ds = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
      .select('a, 'b, 'c)

    val sink = new TestingAppendSink
    ds.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,Hi",
      "2,2,Hello",
      "3,2,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSimpleFilter(): Unit = {
    /*
     * Test simple filter
     */
    val ds = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)

    val sink = new TestingAppendSink
    ds.filter('a === 3).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList("3,2,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    /*
     * Test all-rejecting filter
     */
    val ds = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter(false)
    val sink = new TestingAppendSink
    filterDs.toAppendStream[Row].addSink(sink)
    env.execute()

    assertEquals(true, sink.getAppendResults.isEmpty)
  }

  @Test
  def testAllPassingFilter(): Unit = {
    /*
     * Test all-passing filter
     */
    val ds = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
    val sink = new TestingAppendSink
    ds.filter(true).toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1,Hi",
      "2,2,Hello",
      "3,2,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFilterOnIntegerTupleField(): Unit = {
    /*
     * Test filter on Integer tuple field.
     */
    val ds = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 === 0 )
      .where($"b" === 3 || $"b" === 4 || $"b" === 5)
    val sink = new TestingAppendSink
    filterDs.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "4,3,Hello world, how are you?", "6,3,Luke Skywalker",
      "8,4,Comment#2", "10,4,Comment#4", "12,5,Comment#6", "14,5,Comment#8")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNotEquals(): Unit = {
    /*
     * Test filter on Integer tuple field.
     */
    val ds = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 !== 0)
      .where(($"b" !== 1) && ($"b" !== 2) && ($"b" !== 3))
    val sink = new TestingAppendSink
    filterDs.toAppendStream[Row].addSink(sink)
    env.execute()
    val expected = mutable.MutableList(
      "7,4,Comment#1", "9,4,Comment#3",
      "11,5,Comment#5", "13,5,Comment#7", "15,5,Comment#9",
      "17,6,Comment#11", "19,6,Comment#13", "21,6,Comment#15")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUserDefinedFunctionWithParameter(): Unit = {
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))
    val ds = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
      .where(call("RichFunc2", $"c") === "ABC#Hello")
      .select('c)

    val sink = new TestingAppendSink
    ds.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Hello")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMultipleUserDefinedFunctions(): Unit = {
    tEnv.registerFunction("RichFunc1", new RichFunc1)
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    val result = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
      .where(call("RichFunc2", $"c") === "Abc#Hello" ||
             (call("RichFunc1", $"a") === 3) &&
             ($"b" === 2))
      .select('c)

    val sink = new TestingAppendSink
    result.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Hello", "Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testScalarFunctionConstructorWithParams(): Unit = {
    val testData = new mutable.MutableList[(Int, Long, String)]
    testData.+=((1, 1L, "Jack#22"))
    testData.+=((2, 2L, "John#19"))
    testData.+=((3, 2L, "Anna#44"))
    testData.+=((4, 3L, "nosharp"))

    val t = env.fromCollection(testData).toTable(tEnv).as("a", "b", "c")
    val func0 = new Func13("default")
    val func1 = new Func13("Sunny")
    val func2 = new Func13("kevin2")

    val sink = new TestingAppendSink
    val result = t.select(func0('c), func1('c), func2('c))
    result.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "default-Anna#44,Sunny-Anna#44,kevin2-Anna#44",
      "default-Jack#22,Sunny-Jack#22,kevin2-Jack#22",
      "default-John#19,Sunny-John#19,kevin2-John#19",
      "default-nosharp,Sunny-nosharp,kevin2-nosharp"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testInlineScalarFunction(): Unit = {
    val t = env.fromElements(1, 2, 3, 4).toTable(tEnv).as("a")

    val sink = new TestingAppendSink
    val result = t.select(
      (new ScalarFunction() {
        def eval(i: Int, suffix: String): String = {
          suffix + i
        }
      })('a, ">>"))
    result.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      ">>1",
      ">>2",
      ">>3",
      ">>4"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNonStaticObjectScalarFunction(): Unit = {
    val t = env.fromElements(1, 2, 3, 4).toTable(tEnv).as("a")

    val sink = new TestingAppendSink
    val result = t.select(NonStaticObjectScalarFunction('a, ">>"))

    result.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      ">>1",
      ">>2",
      ">>3",
      ">>4"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  object NonStaticObjectScalarFunction extends ScalarFunction {
    def eval(i: Int, suffix: String): String = {
      suffix + i
    }
  }

  @Test(expected = classOf[ValidationException]) // see FLINK-15162
  def testNonStaticClassScalarFunction(): Unit = {
    val t = env.fromElements(1, 2, 3, 4).toTable(tEnv).as("a")

    val sink = new TestingAppendSink
    val result = t.select(new NonStaticClassScalarFunction()('a, ">>"))

    result.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      ">>1",
      ">>2",
      ">>3",
      ">>4"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  class NonStaticClassScalarFunction extends ScalarFunction {
    def eval(i: Int, suffix: String): String = {
      suffix + i
    }
  }

  @Test
  def testCallFunctionWithStarArgument(): Unit = {
    val table = tEnv.fromDataStream(env.fromCollection(Seq(
      ("Foo", 0, 3),
      ("Bar", 1, 4),
      ("Error", -1, 2),
      ("Example", 3, 6)
    )), '_s, '_b, '_e).where(ValidSubStringFilter('*)).select(SubstringFunc('*))

    val sink = new TestingAppendSink
    table.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List("Foo", "mpl")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testOptimizeNestingInvokeScalarFunction(): Unit = {

    val t = env.fromElements(1, 2, 3, 4).toTable(tEnv).as("f1")
    tEnv.createTemporaryView("t1", t)
    tEnv.createTemporaryFunction("func", NestingFunc)
    tEnv.sqlQuery("select func(func(f1)) from t1")
        .toAppendStream[Row].addSink(new TestingAppendSink)
    env.execute()
  }

  @SerialVersionUID(1L)
  object ValidSubStringFilter extends ScalarFunction {
    @varargs
    def eval(@DataTypeHint(inputGroup = InputGroup.ANY) row: AnyRef*): Boolean = {
      val str = row(0).asInstanceOf[String]
      val begin = row(1).asInstanceOf[Int]
      val end = row(2).asInstanceOf[Int]
      begin >= 0 && begin <= end && str.length() >= end
    }
  }

  @SerialVersionUID(1L)
  object SubstringFunc extends ScalarFunction {
    def eval(str: String, begin: Integer, end: Integer): String = {
      str.substring(begin, end)
    }
  }

  @SerialVersionUID(1L)
  object NestingFunc extends ScalarFunction {
    val expected = new util.HashMap[Integer, Integer]()
    def eval(a: Integer): util.Map[Integer, Integer] = {
      expected
    }
    def eval(map: util.Map[Integer, Integer] ): util.Map[Integer, Integer] = {
      Assert.assertTrue(map.eq(expected))
      map
    }
  }

  @Test
  def testMapType(): Unit = {
    val ds = env.fromCollection(tupleData3).toTable(tEnv).select(map('_1, '_3))

    val sink = new TestingAppendSink
    ds.toAppendStream[Row].addSink(sink)
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
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testColumnOperation(): Unit = {
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

    val sink = new TestingAppendSink
    result.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,Kevin is a kid,1,str,last,3",
      "2,Sunny is a kid,1,str,last,4"
    )
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMap(): Unit = {
    val ds = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
      .map(Func23('a, 'b, 'c)).as("a", "b", "c", "d")
      .map(Func24('a, 'b, 'c, 'd)).as("a", "b", "c", "d")
      .map(Func1('b))

    val sink = new TestingAppendSink
    ds.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "3",
      "4",
      "5")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMapWithStarArgument(): Unit = {
    val ds = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
      .map(Func23('*)).as("a", "b", "c", "d")
      .map(Func24('*)).as("a", "b", "c", "d")
      .map(Func1('b))

    val sink = new TestingAppendSink
    ds.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "3",
      "4",
      "5")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Ignore("Will be open when FLINK-10834 has been fixed.")
  @Test
  def testNonDeterministic(): Unit = {
    val ds = env.fromCollection(smallTupleData3).toTable(tEnv, 'a, 'b, 'c)
      .map(Func25('a))

    val sink = new TestingAppendSink
    ds.toAppendStream[Row].addSink(sink)
    env.execute()

    sink.getAppendResults.foreach { testResult =>
      val result = testResult.split(",")
      assertEquals(result(0), result(1))
    }
  }

  @Test
  def testPrimitiveMapType(): Unit = {
    val ds = env.fromCollection(smallTupleData3)
      .toTable(tEnv)
      .select(map('_2, 30, 10L, '_1))

    val results = ds.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = List(
      "{1=30, 10=1}",
      "{2=30, 10=2}",
      "{2=30, 10=3}")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNonPrimitiveType(): Unit = {
    val ds = env.fromCollection(smallTupleData3)
      .toTable(tEnv)
      .select(map('_1, '_3))

    val results = ds.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = List(
      "{1=Hi}",
      "{2=Hello}",
      "{3=Hello world}")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSelectStarFromNestedTable(): Unit = {
    val table = tEnv.fromDataStream(env.fromCollection(Seq(
      ((0, 0), "0"),
      ((1, 1), "1"),
      ((2, 2), "2")
    )), '_1, '_2).select('*)

    val sink = new TestingAppendSink
    table.toAppendStream[Row].addSink(sink)
    env.execute()

    val expected = List("0,0,0", "1,1,1", "2,2,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}

@SerialVersionUID(1L)
object TestUDFLength extends ScalarFunction {

  // testing eval function with throws clause
  @throws(classOf[Exception])
  def eval(x: String): Int = {
    if (null == x || x.isEmpty) {
      0
    } else {
      x.length
    }
  }
}
