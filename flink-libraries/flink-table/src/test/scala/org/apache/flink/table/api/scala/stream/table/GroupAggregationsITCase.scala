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

package org.apache.flink.table.api.scala.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.table.api.TableEnvironment
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

/**
  * Tests of groupby (without window) aggregations
  */
class GroupAggregationsITCase extends StreamingWithStateTestBase {

  @Test
  def testNonKeyedGroupAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
            .select('a.sum, 'b.sum)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1", "3,3", "6,5", "10,8", "15,11", "21,14", "28,18", "36,22", "45,26", "55,30", "66,35",
      "78,40", "91,45", "105,50", "120,55", "136,61", "153,67", "171,73", "190,79", "210,85",
      "231,91")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testGroupAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.sum)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1,1", "2,2", "2,5", "3,4", "3,9", "3,15", "4,7", "4,15",
      "4,24", "4,34", "5,11", "5,23", "5,36", "5,50", "5,65", "6,16", "6,33", "6,51", "6,70",
      "6,90", "6,111")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testDoubleGroupAggregation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('a.sum as 'd, 'b)
      .groupBy('b, 'd)
      .select('b)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "1",
      "2", "2",
      "3", "3", "3",
      "4", "4", "4", "4",
      "5", "5", "5", "5", "5",
      "6", "6", "6", "6", "6", "6")

    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }

  @Test
  def testGroupAggregateWithExpression(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    env.setParallelism(1)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .groupBy('e, 'b % 3)
      .select('c.min, 'e, 'a.avg, 'd.count)

    val results = t.toDataStream[Row]
    results.addSink(new StreamITCase.StringSink)
    env.execute()

    val expected = mutable.MutableList(
      "0,1,1,1", "1,2,2,1", "2,1,2,1", "3,2,3,1", "1,2,2,2",
      "5,3,3,1", "3,2,3,2", "7,1,4,1", "2,1,3,2", "3,2,3,3", "7,1,4,2", "5,3,4,2", "12,3,5,1",
      "1,2,3,3", "14,2,5,1")
    assertEquals(expected.sorted, StreamITCase.testResults.sorted)
  }
}
