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
import org.apache.flink.table.runtime.utils.{StreamITCase, StreamTestData, StreamingWithStateTestBase}
import org.apache.flink.table.utils.{Top3, Top3WithEmitRetractValue, Top3WithMapView}
import org.apache.flink.types.Row

import org.junit.Assert.assertEquals
import org.junit.{Before, Test}

/**
  * Tests of groupby (without window) table aggregations
  */
class TableAggregateITCase extends StreamingWithStateTestBase {

  @Before
  def setup(): Unit = {
    StreamITCase.clear
  }

  @Test
  def testGroupByFlatAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val top3 = new Top3
    val source = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source.groupBy('b)
      .flatAggregate(top3('a))
      .select('b, 'f0, 'f1)
      .as("category", "v1", "v2")

    val results = resultTable.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1,1",
      "2,2,2",
      "2,3,3",
      "3,4,4",
      "3,5,5",
      "3,6,6",
      "4,10,10",
      "4,9,9",
      "4,8,8",
      "5,15,15",
      "5,14,14",
      "5,13,13",
      "6,21,21",
      "6,20,20",
      "6,19,19"
    ).sorted
    assertEquals(expected, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testEmitRetractValueIncrementally(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val top3 = new Top3WithEmitRetractValue
    val source = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source.groupBy('b)
      .flatAggregate(top3('a))
      .select('b, 'f0, 'f1)
      .as("category", "v1", "v2")

    val results = resultTable.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1,1",
      "2,2,2",
      "2,3,3",
      "3,4,4",
      "3,5,5",
      "3,6,6",
      "4,10,10",
      "4,9,9",
      "4,8,8",
      "5,15,15",
      "5,14,14",
      "5,13,13",
      "6,21,21",
      "6,20,20",
      "6,19,19"
    ).sorted
    assertEquals(expected, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testNonkeyedFlatAggregate(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val top3 = new Top3
    val source = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source
      .flatAggregate(top3('a))
      .select('f0, 'f1)
      .as("v1", "v2")

    val results = resultTable.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List(
      "19,19",
      "20,20",
      "21,21"
    ).sorted
    assertEquals(expected, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testWithMapViewAndInputWithRetraction(): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val top3 = new Top3WithMapView
    val source = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source
      .groupBy('b)
      .select('b, 'a.sum as 'a)
      .flatAggregate(top3('a) as ('v1, 'v2))
      .select('v1, 'v2)

    val results = resultTable.toRetractStream[Row]
    results.addSink(new StreamITCase.RetractingSink).setParallelism(1)
    env.execute()

    val expected = List(
      "111,111",
      "65,65",
      "34,34"
    ).sorted
    assertEquals(expected, StreamITCase.retractedResults.sorted)
  }

  @Test
  def testTableAggFunctionWithoutRetractionMethod(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Function class " +
      "'org.apache.flink.table.utils.Top3' does not implement at least one method " +
      "named 'retract' which is public, not abstract and (in case of table functions) not static.")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(getStateBackend)
    val settings = EnvironmentSettings.newInstance().useOldPlanner().build()
    val tEnv = StreamTableEnvironment.create(env, settings)

    val top3 = new Top3
    val source = StreamTestData.get3TupleDataStream(env).toTable(tEnv, 'a, 'b, 'c)
    source
      .groupBy('b)
      .select('b, 'a.sum as 'a)
      .flatAggregate(top3('a) as ('v1, 'v2))
      .select('v1, 'v2)
      .toRetractStream[Row]

    env.execute()
  }
}
