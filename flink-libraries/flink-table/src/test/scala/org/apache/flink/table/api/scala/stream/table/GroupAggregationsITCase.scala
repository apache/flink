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

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.scala.stream.utils.{StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.table.api.{StreamQueryConfig, TableEnvironment}
import org.apache.flink.table.api.scala.stream.utils.StreamITCase.RetractingSink
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.mutable

/**
  * Tests of groupby (without window) aggregations
  */
class GroupAggregationsITCase extends StreamingWithStateTestBase {
  private val queryConfig = new StreamQueryConfig()
  queryConfig.withIdleStateRetentionTime(Time.hours(1), Time.hours(2))


  @Test
  def testDistinct(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .select('b).distinct()

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("1", "2", "3", "4", "5", "6")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testDistinctAfterAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .groupBy('e).select('e, 'a.count).distinct()

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = mutable.MutableList("1,5", "2,7", "3,3")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testNonKeyedGroupAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
            .select('a.sum, 'b.sum)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List("231,91")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testGroupAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('b, 'a.sum)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new StreamITCase.RetractingSink)
    env.execute()

    val expected = List("1,1", "2,5", "3,15", "4,34", "5,65", "6,111")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testDoubleGroupAggregation(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .select('a.count as 'cnt, 'b)
      .groupBy('cnt)
      .select('cnt, 'b.count as 'freq)

    val results = t.toRetractStream[Row](queryConfig)

    results.addSink(new RetractingSink)
    env.execute()
    val expected = List("1,1", "2,1", "3,1", "4,1", "5,1", "6,1")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testGroupAggregateWithExpression(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val tEnv = TableEnvironment.getTableEnvironment(env)
    StreamITCase.clear

    val t = StreamTestData.get5TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c, 'd, 'e)
      .groupBy('e, 'b % 3)
      .select('c.min, 'e, 'a.avg, 'd.count)

    val results = t.toRetractStream[Row](queryConfig)
    results.addSink(new RetractingSink)
    env.execute()

    val expected = mutable.MutableList(
      "0,1,1,1", "7,1,4,2", "2,1,3,2",
      "3,2,3,3", "1,2,3,3", "14,2,5,1",
      "12,3,5,1", "5,3,4,2")
    assertEquals(expected.sorted, StreamITCase.retractedResults.sorted)
  }
}
