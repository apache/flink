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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecSort
import org.apache.flink.table.planner.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.planner.runtime.utils._
import org.apache.flink.types.Row

import org.junit.Assert._
import org.junit._
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class SortITCase(mode: StateBackendMode) extends StreamingWithStateTestBase(mode) {

  @Test
  def testDisableSortNonTemporalField(): Unit = {
    val sqlQuery = "SELECT * FROM a ORDER BY a2"
    val data = new mutable.MutableList[(String, String)]
    data.+=(("0", "4"))
    data.+=(("3", "3"))
    data.+=(("1", "2"))
    data.+=(("5", "1"))

    val da = env.fromCollection(data).toTable(tEnv, 'a1, 'a2)
    tEnv.registerTable("a", da)

    thrown.expect(classOf[TableException])
    thrown.expectMessage("Sort on a non-time-attribute field is not supported.")
    tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
  }

  @Test
  def testSort(): Unit = {
    val sqlQuery = "SELECT * FROM a ORDER BY a2"
    val data = new mutable.MutableList[(String, String)]
    data.+=(("0", "4"))
    data.+=(("3", "3"))
    data.+=(("1", "2"))
    data.+=(("5", "1"))

    val da = failingDataSource(data).toTable(tEnv, 'a1, 'a2)
    tEnv.registerTable("a", da)

    val sink = new TestingRetractSink
    tEnv.getConfig.getConfiguration.setBoolean(
      StreamExecSort.TABLE_EXEC_SORT_NON_TEMPORAL_ENABLED, true)
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "5,1",
      "1,2",
      "3,3",
      "0,4")

    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testSortOrderByDesc(): Unit = {
    val sqlQuery = "SELECT * FROM a ORDER BY a1 DESC"

    val data = new mutable.MutableList[(String, String)]
    data.+=(("0", "4"))
    data.+=(("3", "3"))
    data.+=(("1", "2"))
    data.+=(("5", "1"))

    val da = failingDataSource(data).toTable(tEnv, 'a1, 'a2)
    tEnv.registerTable("a", da)

    val sink = new TestingRetractSink
    tEnv.getConfig.getConfiguration.setBoolean(
      StreamExecSort.TABLE_EXEC_SORT_NON_TEMPORAL_ENABLED, true)
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "5,1",
      "3,3",
      "1,2",
      "0,4")

    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testSortOrderByMultipleFields(): Unit = {
    val sqlQuery = "SELECT * FROM a ORDER BY a1, a2"

    val data = new mutable.MutableList[(String, String)]
    data.+=(("5", "1"))
    data.+=(("0", "4"))
    data.+=(("1", "7"))
    data.+=(("1", "2"))

    val da = failingDataSource(data).toTable(tEnv, 'a1, 'a2)
    tEnv.registerTable("a", da)

    val sink = new TestingRetractSink
    tEnv.getConfig.getConfiguration.setBoolean(
      StreamExecSort.TABLE_EXEC_SORT_NON_TEMPORAL_ENABLED, true)
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "0,4",
      "1,2",
      "1,7",
      "5,1")

    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testSortOrderByRepeatedFields(): Unit = {
    val sqlQuery = "SELECT * FROM a ORDER BY a1, a1"

    val data = new mutable.MutableList[(String, String)]
    data.+=(("5", "1"))
    data.+=(("0", "4"))
    data.+=(("1", "7"))
    data.+=(("2", "2"))

    val da = failingDataSource(data).toTable(tEnv, 'a1, 'a2)
    tEnv.registerTable("a", da)

    val sink = new TestingRetractSink
    tEnv.getConfig.getConfiguration.setBoolean(
      StreamExecSort.TABLE_EXEC_SORT_NON_TEMPORAL_ENABLED, true)
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "0,4",
      "1,7",
      "2,2",
      "5,1")

    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testSortOrderByWithRetract(): Unit = {
    val sqlQuery = "SELECT a1, count(*) as c FROM a GROUP BY a1 ORDER BY c"

    val data = new mutable.MutableList[(String, String)]
    data.+=(("1", "1"))
    data.+=(("2", "1"))
    data.+=(("3", "1"))
    data.+=(("3", "4"))
    data.+=(("6", "1"))
    data.+=(("1", "2"))
    data.+=(("1", "3"))
    data.+=(("3", "2"))
    data.+=(("3", "3"))
    data.+=(("6", "2"))

    val da = failingDataSource(data).toTable(tEnv, 'a1, 'a2)
    tEnv.registerTable("a", da)

    val sink = new TestingRetractSink
    tEnv.getConfig.getConfiguration.setBoolean(
      StreamExecSort.TABLE_EXEC_SORT_NON_TEMPORAL_ENABLED, true)
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "2,1",
      "6,2",
      "1,3",
      "3,4")

    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testSortWithWhere(): Unit = {
    val sqlQuery =
      s"""
         |select * from a where a1 < all (select a1 * 2 from a) order by a1 desc
       """.stripMargin

    val data = new mutable.MutableList[(Int, Int)]
    data.+=((8, 1))
    data.+=((7, 2))
    data.+=((6, 3))
    data.+=((5, 4))
    data.+=((4, 5))

    val da = failingDataSource(data).toTable(tEnv, 'a1)
    tEnv.registerTable("a", da)

    val sink = new TestingRetractSink
    tEnv.getConfig.getConfiguration.setBoolean(
      StreamExecSort.TABLE_EXEC_SORT_NON_TEMPORAL_ENABLED, true)
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "7",
      "6",
      "5",
      "4")

    assertEquals(expected, sink.getRetractResults)
  }
}
