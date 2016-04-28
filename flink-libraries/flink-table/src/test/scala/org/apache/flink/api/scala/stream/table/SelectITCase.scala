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

package org.apache.flink.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.stream.utils.{StreamTestData, StreamITCase}
import org.apache.flink.api.scala.table._
import org.apache.flink.api.table.{Row, TableEnvironment}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable

class SelectITCase extends StreamingMultipleProgramsTestBase {

  @Test
  def testSimpleSelectAll(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).select('_1, '_2, '_3)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
        "1,1,Hi",
        "2,2,Hello",
        "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testSelectFirst(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.getSmall3TupleDataStream(env).toTable(tEnv).select('_1)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
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

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
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

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
        "1,1,Hi",
        "2,2,Hello",
        "3,2,Hello world")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAsWithToFewFields(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("no")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAsWithToManyFields(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("no")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testAsWithAmbiguousFields(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'b)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("no")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }


  @Test(expected = classOf[IllegalArgumentException])
  def testOnlyFieldRefInAs(): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.testResults = mutable.MutableList()
    val ds = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b as 'c, 'd)

    val results = ds.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList("no")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
