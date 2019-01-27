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

package org.apache.flink.table.runtime.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.runtime.utils.StreamingWithStateTestBase.StateBackendMode
import org.apache.flink.table.runtime.utils.{StreamingWithStateTestBase, TestingRetractSink}
import org.apache.flink.types.Row
import org.junit.Assert.assertEquals
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.Test

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class SortITCase(mode: StateBackendMode)
  extends StreamingWithStateTestBase(mode) {

  @Test
  def testSort(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM a ORDER BY a2
       """.stripMargin


    val dataA = new mutable.MutableList[(String, String)]
    dataA.+=(("0", "4"))
    dataA.+=(("3", "3"))
    dataA.+=(("1", "2"))
    dataA.+=(("5", "1"))

    val da = failingDataSource(dataA).toTable(tEnv, 'a1, 'a2)
    tEnv.registerTable("a", da)

    val sink = new TestingRetractSink
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_NON_TEMPORAL_ENABLED, true)
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "5,1", "1,2", "3,3", "0,4"
    )

    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testSortOrderByDesc(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM a ORDER BY a1 DESC
       """.stripMargin


    val dataA = new mutable.MutableList[(String, String)]
    dataA.+=(("0", "4"))
    dataA.+=(("3", "3"))
    dataA.+=(("1", "2"))
    dataA.+=(("5", "1"))

    val da = failingDataSource(dataA).toTable(tEnv, 'a1, 'a2)
    tEnv.registerTable("a", da)

    val sink = new TestingRetractSink
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_NON_TEMPORAL_ENABLED, true)
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "5,1", "3,3", "1,2", "0,4"
    )

    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testSortOrderByMultipleFields(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM a ORDER BY a1, a2
       """.stripMargin


    val dataA = new mutable.MutableList[(String, String)]
    dataA.+=(("5", "1"))
    dataA.+=(("0", "4"))
    dataA.+=(("1", "7"))
    dataA.+=(("1", "2"))

    val da = failingDataSource(dataA).toTable(tEnv, 'a1, 'a2)
    tEnv.registerTable("a", da)

    val sink = new TestingRetractSink
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_NON_TEMPORAL_ENABLED, true)
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "0,4", "1,2", "1,7", "5,1"
    )

    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testSortOrderByRepeatedFields(): Unit = {
    val sqlQuery =
      s"""
         |SELECT * FROM a ORDER BY a1, a1
       """.stripMargin


    val dataA = new mutable.MutableList[(String, String)]
    dataA.+=(("5", "1"))
    dataA.+=(("0", "4"))
    dataA.+=(("1", "7"))
    dataA.+=(("2", "2"))

    val da = failingDataSource(dataA).toTable(tEnv, 'a1, 'a2)
    tEnv.registerTable("a", da)

    val sink = new TestingRetractSink
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_NON_TEMPORAL_ENABLED, true)
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "0,4", "1,7", "2,2", "5,1"
    )

    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testSortOrderByWithRetract(): Unit = {
    val sqlQuery =
      s"""
         |SELECT a1, count(*) as c FROM a GROUP BY a1 ORDER BY c
       """.stripMargin


    val dataA = new mutable.MutableList[(String, String)]
    dataA.+=(("1", "1"))
    dataA.+=(("2", "1"))
    dataA.+=(("3", "1"))
    dataA.+=(("3", "4"))
    dataA.+=(("6", "1"))
    dataA.+=(("1", "2"))
    dataA.+=(("1", "3"))
    dataA.+=(("3", "2"))
    dataA.+=(("3", "3"))
    dataA.+=(("6", "2"))

    val da = failingDataSource(dataA).toTable(tEnv, 'a1, 'a2)
    tEnv.registerTable("a", da)

    val sink = new TestingRetractSink
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_NON_TEMPORAL_ENABLED, true)
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "2,1", "6,2", "1,3", "3,4"
    )

    assertEquals(expected, sink.getRetractResults)
  }

  @Test
  def testSortWithWhere(): Unit = {
    val sqlQuery =
      s"""
         |select * from a where a1 < all (select a1 * 2 from a) order by a1 desc
       """.stripMargin

    val dataA = new mutable.MutableList[(Int, Int)]
    dataA.+=((8, 1))
    dataA.+=((7, 2))
    dataA.+=((6, 3))
    dataA.+=((5, 4))
    dataA.+=((4, 5))

    val da = failingDataSource(dataA).toTable(tEnv, 'a1)
    tEnv.registerTable("a", da)

    val sink = new TestingRetractSink
    tEnv.getConfig.getConf.setBoolean(TableConfigOptions.SQL_EXEC_SORT_NON_TEMPORAL_ENABLED, true)
    val results = tEnv.sqlQuery(sqlQuery).toRetractStream[Row]
    results.addSink(sink).setParallelism(1)
    env.execute()

    val expected = Seq(
      "7", "6", "5", "4"
    )

    assertEquals(expected, sink.getRetractResults)
  }
}
