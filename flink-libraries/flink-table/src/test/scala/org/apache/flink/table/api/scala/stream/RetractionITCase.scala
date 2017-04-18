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

package org.apache.flink.table.api.scala.stream

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{TableEnvironment, TableException}
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamingWithStateTestBase}
import org.apache.flink.types.Row
import org.junit.Assert._
import org.junit.Test
import org.apache.flink.table.api.scala._
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.table.runtime.types.CRow
import org.apache.flink.table.utils.TableFunc0

import scala.collection.mutable

/**
  * tests for retraction
  */
class RetractionITCase extends StreamingWithStateTestBase {
  // input data
  val data = List(
    ("Hello", 1),
    ("word", 1),
    ("Hello", 1),
    ("bark", 1),
    ("bark", 1),
    ("bark", 1),
    ("bark", 1),
    ("bark", 1),
    ("bark", 1),
    ("flink", 1)
  )

  // keyed groupby + keyed groupby
  @Test
  def testWordCount(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)
    env.setStateBackend(getStateBackend)

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'word, 'num)
    val resultTable = table
      .groupBy('word)
      .select('word as 'word, 'num.sum as 'count)
      .groupBy('count)
      .select('count, 'word.count as 'frequency)

    // to DataStream with CRow
    val results = resultTable.toDataStream[CRow]
    results.addSink(new StreamITCase.StringSinkWithCRow)
    env.execute()

    val expected = Seq("+1,1", "+1,2", "+1,1", "+2,1", "+1,2", "+1,1", "+2,2", "+2,1", "+3,1",
      "+3,0", "+4,1", "+4,0", "+5,1", "+5,0", "+6,1", "+1,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  // keyed groupby + non-keyed groupby
  @Test
  def testGroupByAndNonKeyedGroupBy(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)
    env.setStateBackend(getStateBackend)

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'word, 'num)
    val resultTable = table
      .groupBy('word)
      .select('word as 'word, 'num.sum as 'count)
      .select('count.sum)

    val results = resultTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("1", "2", "1", "3", "4", "3", "5", "3", "6", "3", "7", "3", "8", "3", "9",
      "10")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  // non-keyed groupby + keyed groupby
  @Test
  def testNonKeyedGroupByAndGroupBy(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)
    env.setStateBackend(getStateBackend)

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'word, 'num)
    val resultTable = table
      .select('num.sum as 'count)
      .groupBy('count)
      .select('count, 'count.count)

    val results = resultTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("1,1", "1,0", "2,1", "2,0", "3,1", "3,0", "4,1", "4,0", "5,1", "5,0", "6," +
      "1", "6,0", "7,1", "7,0", "8,1", "8,0", "9,1", "9,0", "10,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  // test unique process, if the current output message of unbounded groupby equals the
  // previous message, unbounded groupby will ignore the current one.
  @Test
  def testUniqueProcess(): Unit = {
    // data input
    val data = List(
      (1, 1L),
      (2, 2L),
      (3, 3L),
      (3, 3L),
      (4, 1L),
      (4, 0L),
      (4, 0L),
      (4, 0L),
      (5, 1L),
      (6, 6L),
      (6, 6L),
      (6, 6L),
      (7, 8L)
    )
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)
    env.setStateBackend(getStateBackend)

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'pk, 'value)
    val resultTable = table
      .groupBy('pk)
      .select('pk as 'pk, 'value.sum as 'sum)
      .groupBy('sum)
      .select('sum, 'pk.count as 'count)

    val results = resultTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq("1,1", "2,1", "3,1", "3,0", "6,1", "1,2", "1,3", "6,2", "6,1", "12,1","12," +
      "0", "18,1", "8,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  // correlate should handle retraction messages correctly
  @Test
  def testCorrelate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    env.setParallelism(1)
    env.setStateBackend(getStateBackend)

    val func0 = new TableFunc0

    val stream = env.fromCollection(data)
    val table = stream.toTable(tEnv, 'word, 'num)
    val resultTable = table
      .groupBy('word)
      .select('word as 'word, 'num.sum as 'count)
      .leftOuterJoin(func0('word))
      .groupBy('count)
      .select('count, 'word.count as 'frequency)

    val results = resultTable.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = Seq(
      "1,1", "1,2", "1,1", "2,1", "1,2", "1,1", "2,2", "2,1", "3,1", "3,0", "4,1", "4,0", "5,1",
      "5,0", "6,1", "1,2")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

}
