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

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils.TestData.tupleData3
import org.apache.flink.table.planner.runtime.utils.{StreamingWithStateTestBase, TestingRetractSink}
import org.apache.flink.table.planner.utils.{EmptyTableAggFuncWithoutEmit, TableAggSum, Top3, Top3WithMapView, Top3WithRetractInput}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

/**
  * Tests of groupby (without window) table aggregations
  */
@RunWith(classOf[Parameterized])
class TableAggregateITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Before
  override def before(): Unit = {
    super.before()
    tEnv.getConfig.setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
  }

  @Test
  def testGroupByFlatAggregate(): Unit = {
    val top3 = new Top3

    val resultTable = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .flatAggregate(top3('a))
      .select('b, 'f0, 'f1)
      .as('category, 'v1, 'v2)

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
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
    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testNonkeyedFlatAggregate(): Unit = {

    val top3 = new Top3
    val source = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source
      .flatAggregate(top3('a))
      .select('f0, 'f1)
      .as('v1, 'v2)

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "19,19",
      "20,20",
      "21,21"
    ).sorted
    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testAggregateAfterTableAggregate(): Unit = {
    val top3 = new Top3

    val resultTable = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .flatAggregate(top3('a))
      .select('b, 'f0, 'f1)
      .as('category, 'v1, 'v2)
      .groupBy('category)
      .select('category, 'v1.max)

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "1,1",
      "2,3",
      "3,6",
      "4,10",
      "5,15",
      "6,21"
    ).sorted
    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testGroupByFlatAggregateWithMapView(): Unit = {
    val top3 = new Top3WithMapView

    val resultTable = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
      .groupBy('b)
      .flatAggregate(top3('a))
      .select('b, 'f0, 'f1)
      .as('category, 'v1, 'v2)

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
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
    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testInputWithRetraction(): Unit = {

    val top3 = new Top3WithRetractInput
    val source = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source
      .groupBy('b)
      .select('b, 'a.sum as 'a)
      .flatAggregate(top3('a) as ('v1, 'v2))
      .select('v1, 'v2)

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List(
      "111,111",
      "65,65",
      "34,34"
    ).sorted
    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testInternalAccumulatorType(): Unit = {
    val tableAggSum = new TableAggSum
    val source = failingDataSource(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    val resultTable = source
      .groupBy('b)
      .flatAggregate(tableAggSum('a) as 'sum)
      .select('b, 'sum)

    val sink = new TestingRetractSink()
    resultTable.toRetractStream[Row].addSink(sink).setParallelism(1)
    env.execute()

    val expected = List("6,111", "6,111", "5,65", "5,65", "4,34", "4,34", "3,15", "3,15",
      "2,5", "2,5", "1,1", "1,1").sorted
    assertEquals(expected, sink.getRetractResults.sorted)
  }

  @Test
  def testTableAggFunctionWithoutRetractionMethod(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Function class 'org.apache.flink.table.planner.utils.Top3'" +
      " does not implement at least one method named 'retract' which is public, " +
      "not abstract and (in case of table functions) not static.")

    val top3 = new Top3
    val source = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    source
      .groupBy('b)
      .select('b, 'a.sum as 'a)
      .flatAggregate(top3('a) as ('v1, 'v2))
      .select('v1, 'v2)
      .toRetractStream[Row]

    env.execute()
  }

  @Test
  def testTableAggFunctionWithoutEmitValueMethod(): Unit = {
    expectedException.expect(classOf[ValidationException])
    expectedException.expectMessage("Function class " +
      "'org.apache.flink.table.planner.utils.EmptyTableAggFuncWithoutEmit' does not " +
      "implement at least one method named 'emitValue' which is public, " +
      "not abstract and (in case of table functions) not static.")

    val func = new EmptyTableAggFuncWithoutEmit
    val source = env.fromCollection(tupleData3).toTable(tEnv, 'a, 'b, 'c)
    source
      .flatAggregate(func('a) as ('v1, 'v2))
      .select('v1, 'v2)
      .toRetractStream[Row]

    env.execute()
  }
}
