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

package org.apache.flink.api.scala.table.streaming.test

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.{TableEnvironment, Row}
import org.apache.flink.api.table.expressions.Literal
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.JavaConversions._
import org.junit.Test
import org.junit.Assert._
import org.apache.flink.api.scala.table.streaming.test.utils.StreamITCase
import org.apache.flink.api.scala.table.streaming.test.utils.StreamTestData

class FilterITCase extends StreamingMultipleProgramsTestBase {

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
    val results = filterDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
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
    val results = filterDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
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
    val results = filterDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
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
    val results = filterDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "2,2,Hello", "4,3,Hello world, how are you?",
      "6,3,Luke Skywalker", "8,4,Comment#2", "10,4,Comment#4",
      "12,5,Comment#6", "14,5,Comment#8", "16,6,Comment#10",
      "18,6,Comment#12", "20,6,Comment#14")
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
    val results = filterDs.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()
    val expected = mutable.MutableList(
      "1,1,Hi", "3,2,Hello world",
      "5,3,I am fine.", "7,4,Comment#1", "9,4,Comment#3",
      "11,5,Comment#5", "13,5,Comment#7", "15,5,Comment#9",
      "17,6,Comment#11", "19,6,Comment#13", "21,6,Comment#15")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
