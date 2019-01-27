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
import org.apache.flink.table.api.{TableConfigOptions, TableException}
import org.apache.flink.table.api.functions.ScalarFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.expressions.Literal
import org.apache.flink.table.expressions.utils.{Func13, RichFunc1, RichFunc2}
import org.apache.flink.table.runtime.utils._
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.{Seq, mutable}

class CalcITCase extends StreamingTestBase {

  @Test
  def testSimpleSelectAll(): Unit = {

    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).select('_1, '_2, '_3)

    val sink = new TestingAppendSink
    val results = ds.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
        "1,1,Hi",
        "2,2,Hello",
        "3,2,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSelectFirst(): Unit = {

    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).select('_1)

    val sink = new TestingAppendSink
    val results = ds.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("1", "2", "3")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSimpleSelectWithNaming(): Unit = {

    // verify ProjectMergeRule.
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv)
      .select('_1 as 'a, '_2 as 'b, '_1 as 'c)
      .select('a, 'b)

    val sink = new TestingAppendSink
    val results = ds.toAppendStream[Row]
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1", "2,2", "3,2", "4,3", "5,3", "6,3", "7,4",
      "8,4", "9,4", "10,4", "11,5", "12,5", "13,5", "14,5", "15,5",
      "16,6", "17,6", "18,6", "19,6", "20,6", "21,6")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testSimpleSelectAllWithAs(): Unit = {

    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .select('a, 'b, 'c)

    val sink = new TestingAppendSink
    val results = ds.toAppendStream[Row]
    results.addSink(sink)
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
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter('a === 3)
    val results = filterDs.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("3,2,Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testAllRejectingFilter(): Unit = {
    /*
     * Test all-rejecting filter
     */
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( Literal(false) )
    tEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_EXEC_SOURCE_VALUES_INPUT_ENABLED, true) // enable values source input
    val results = filterDs.toAppendStream[Row]
    val sink = new TestingAppendSink

    results.addSink(sink)
    env.execute()

    assertEquals(true, sink.getAppendResults.isEmpty)
  }

  @Test(expected = classOf[TableException])
  def testAllRejectingFilterWhenDisableValuesSourceInput(): Unit = {
    /*
     * Test all-rejecting filter
     */
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( Literal(false) )
    // default disable values source input
    val results = filterDs.toAppendStream[Row]
    val sink = new TestingAppendSink

    results.addSink(sink)
    env.execute()
  }

  @Test
  def testAllPassingFilter(): Unit = {
    /*
     * Test all-passing filter
     */
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( Literal(true) )
    val results = filterDs.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
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
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 === 0 )
    val results = filterDs.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList(
      "2,2,Hello", "4,3,Hello world, how are you?",
      "6,3,Luke Skywalker", "8,4,Comment#2", "10,4,Comment#4",
      "12,5,Comment#6", "14,5,Comment#8", "16,6,Comment#10",
      "18,6,Comment#12", "20,6,Comment#14")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testNotEquals(): Unit = {
    /*
     * Test filter on Integer tuple field.
     */
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 !== 0)
    val results = filterDs.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()
    val expected = mutable.MutableList(
      "1,1,Hi", "3,2,Hello world",
      "5,3,I am fine.", "7,4,Comment#1", "9,4,Comment#3",
      "11,5,Comment#5", "13,5,Comment#7", "15,5,Comment#9",
      "17,6,Comment#11", "19,6,Comment#13", "21,6,Comment#15")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testUserDefinedFunctionWithParameter(): Unit = {
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))

    val result = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .where("RichFunc2(c)='ABC#Hello'")
      .select('c)

    val results = result.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Hello")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testMultipleUserDefinedFunctions(): Unit = {
    tEnv.registerFunction("RichFunc1", new RichFunc1)
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    val result = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .where("RichFunc2(c)='Abc#Hello' || RichFunc1(a)=3 && b=2")
      .select('c)

    val results = result.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Hello", "Hello world")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testFunctionSplitWhenCodegenOverLengthLimit(): Unit = {
    // test function split
    tEnv.getConfig.getConf.setInteger(TableConfigOptions.SQL_CODEGEN_LENGTH_MAX, 10)

    val udfLen = TestUDFLength
    tEnv.registerFunction("RichFunc1", new RichFunc1)
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    val result = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .where("RichFunc2(c)='Abc#Hello' || RichFunc1(a)=3 && b=2")
      .select('c, udfLen('c) as 'len)

    val results = result.toAppendStream[Row]
    val sink = new TestingAppendSink
    results.addSink(sink)
    env.execute()

    val expected = mutable.MutableList("Hello,5", "Hello world,11")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @Test
  def testScalarFunctionConstructorWithParams(): Unit = {
    val testData = new mutable.MutableList[(Int, Long, String)]
    testData.+=((1, 1L, "Jack#22"))
    testData.+=((2, 2L, "John#19"))
    testData.+=((3, 2L, "Anna#44"))
    testData.+=((4, 3L, "nosharp"))

    val t = env.fromCollection(testData).toTable(tEnv).as('a, 'b, 'c)
    val func0 = new Func13("default")
    val func1 = new Func13("Sunny")
    val func2 = new Func13("kevin2")

    val result = t.select(func0('c), func1('c), func2('c))

    val sink = new TestingAppendSink
    result.addSink(sink)
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
  def testPrimitiveMapType(): Unit = {
    val ds = StreamTestData.getSmall3TupleDataStream(env)
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
    val ds = StreamTestData.getSmall3TupleDataStream(env)
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
    )))

    val sink = new TestingAppendTableSink
    table.select('*).writeToSink(sink)
    env.execute()

    val expected = List("0,0,0", "1,1,1", "2,2,2")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }
}

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
