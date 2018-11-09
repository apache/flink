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
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.expressions.Literal
import org.apache.flink.table.expressions.utils.{Func13, RichFunc1, RichFunc2, SplitUDF}
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData, UserDefinedFunctionTestUtils}
import org.apache.flink.test.util.AbstractTestBase
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class CalcITCase extends AbstractTestBase {

  @Test
  def testSimpleSelectAll(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
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
  def testSelectStar(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
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

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
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
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
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

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
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
    /*
     * Test simple filter
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

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
    /*
     * Test all-rejecting filter
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( Literal(false) )
    val results = filterDs.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    assertEquals(true, StreamITCase.testResults.isEmpty)
  }

  @Test
  def testAllPassingFilter(): Unit = {
    /*
     * Test all-passing filter
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( Literal(true) )
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
    /*
     * Test filter on Integer tuple field.
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 === 0 )
      .where("b = 3 || b = 4 || b = 5")
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
    /*
     * Test filter on Integer tuple field.
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)

    val filterDs = ds.filter( 'a % 2 !== 0)
      .where("b != 1 && b != 2 && b != 3")
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
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "ABC"))

    StreamITCase.testResults = mutable.MutableList()

    val result = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .where("RichFunc2(c)='ABC#Hello'")
      .select('c)

    val results = result.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("Hello")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testMultipleUserDefinedFunctions(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    tEnv.registerFunction("RichFunc1", new RichFunc1)
    tEnv.registerFunction("RichFunc2", new RichFunc2)
    UserDefinedFunctionTestUtils.setJobParameters(env, Map("string.value" -> "Abc"))

    StreamITCase.testResults = mutable.MutableList()

    val result = StreamTestData.get3TupleDataStream(env)
      .toTable(tEnv, 'a, 'b, 'c)
      .where("RichFunc2(c)='Abc#Hello' || RichFunc1(a)=3 && b=2")
      .select('c)

    val results = result.toAppendStream[Row]
    results.addSink(new StreamITCase.StringSink[Row])
    env.execute()

    val expected = mutable.MutableList("Hello", "Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testScalarFunctionConstructorWithParams(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)

    StreamITCase.testResults = mutable.MutableList()

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
  def testMapType(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
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
}
