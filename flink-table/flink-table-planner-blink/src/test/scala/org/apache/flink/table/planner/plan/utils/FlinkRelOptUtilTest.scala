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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.{StreamTableEnvironment, _}
import org.apache.flink.table.planner.plan.`trait`.{MiniBatchInterval, MiniBatchMode}
import org.apache.flink.table.planner.utils.TableTestUtil

import org.apache.calcite.sql.SqlExplainLevel
import org.junit.Assert.assertEquals
import org.junit.Test

class FlinkRelOptUtilTest {

  @Test
  def testToString(): Unit = {
    val env  = StreamExecutionEnvironment.createLocalEnvironment()
    val tableEnv = StreamTableEnvironment.create(env, TableTestUtil.STREAM_SETTING)

    val table = env.fromElements[(Int, Long, String)]().toTable(tableEnv, 'a, 'b, 'c)
    tableEnv.registerTable("MyTable", table)

    val sqlQuery =
      """
        |WITH t1 AS (SELECT a, c FROM MyTable WHERE b > 50),
        |     t2 AS (SELECT a * 2 AS a, c FROM MyTable WHERE b < 50)
        |
        |SELECT * FROM t1 JOIN t2 ON t1.a = t2.a
      """.stripMargin
    val result = tableEnv.sqlQuery(sqlQuery)
    val rel = TableTestUtil.toRelNode(result)

    val expected1 =
      """
        |LogicalProject(a=[$0], c=[$1], a0=[$2], c0=[$3])
        |+- LogicalJoin(condition=[=($0, $2)], joinType=[inner])
        |   :- LogicalProject(a=[$0], c=[$2])
        |   :  +- LogicalFilter(condition=[>($1, 50)])
        |   :     +- LogicalTableScan(table=[[default_catalog, default_database, MyTable]])
        |   +- LogicalProject(a=[*($0, 2)], c=[$2])
        |      +- LogicalFilter(condition=[<($1, 50)])
        |         +- LogicalTableScan(table=[[default_catalog, default_database, MyTable]])
      """.stripMargin
    assertEquals(expected1.trim, FlinkRelOptUtil.toString(rel).trim)

    val expected2 =
      """
        |LogicalProject
        |+- LogicalJoin
        |   :- LogicalProject
        |   :  +- LogicalFilter
        |   :     +- LogicalTableScan
        |   +- LogicalProject
        |      +- LogicalFilter
        |         +- LogicalTableScan
      """.stripMargin
    assertEquals(expected2.trim, FlinkRelOptUtil.toString(rel, SqlExplainLevel.NO_ATTRIBUTES).trim)
  }

  @Test
  def testMergeRowTimeAndNone(): Unit = {
    val none = MiniBatchInterval.NONE
    val rowtime = MiniBatchInterval(1000L, MiniBatchMode.RowTime)
    val mergedResult = FlinkRelOptUtil.mergeMiniBatchInterval(none, rowtime)
    assertEquals(rowtime, mergedResult)
  }

  @Test
  def testMergeProcTimeAndNone(): Unit = {
    val none = MiniBatchInterval.NONE
    val proctime = MiniBatchInterval(1000L, MiniBatchMode.ProcTime)
    val mergedResult = FlinkRelOptUtil.mergeMiniBatchInterval(none, proctime)
    assertEquals(proctime, mergedResult)
  }

  @Test
  def testMergeRowTimeTAndProcTime1(): Unit = {
    val rowtime = MiniBatchInterval(4000L, MiniBatchMode.RowTime)
    val proctime = MiniBatchInterval(1000L, MiniBatchMode.ProcTime)
    val mergedResult = FlinkRelOptUtil.mergeMiniBatchInterval(rowtime, proctime)
    assertEquals(rowtime, mergedResult)
  }

  @Test
  def testMergeRowTimeTAndProcTime2(): Unit = {
    val rowtime = MiniBatchInterval(0L, MiniBatchMode.RowTime)
    val proctime = MiniBatchInterval(1000L, MiniBatchMode.ProcTime)
    val mergedResult = FlinkRelOptUtil.mergeMiniBatchInterval(rowtime, proctime)
    assertEquals(MiniBatchInterval(1000L, MiniBatchMode.RowTime), mergedResult)
  }

  @Test
  def testMergeRowTimeAndRowtime(): Unit = {
    val rowtime1 = MiniBatchInterval(3000L, MiniBatchMode.RowTime)
    val rowtime2 = MiniBatchInterval(5000L, MiniBatchMode.RowTime)
    val mergedResult = FlinkRelOptUtil.mergeMiniBatchInterval(rowtime1, rowtime2)
    assertEquals(MiniBatchInterval(1000L, MiniBatchMode.RowTime), mergedResult)
  }

  @Test
  def testMergeWithNoneMiniBatch(): Unit = {
    assertEquals(MiniBatchInterval.NO_MINIBATCH,
      FlinkRelOptUtil.mergeMiniBatchInterval(
        MiniBatchInterval.NO_MINIBATCH, MiniBatchInterval.NONE))
    assertEquals(MiniBatchInterval.NO_MINIBATCH,
      FlinkRelOptUtil.mergeMiniBatchInterval(
        MiniBatchInterval.NONE, MiniBatchInterval.NO_MINIBATCH))
    assertEquals(MiniBatchInterval.NO_MINIBATCH,
      FlinkRelOptUtil.mergeMiniBatchInterval(
        MiniBatchInterval.NO_MINIBATCH, MiniBatchInterval.NO_MINIBATCH))
    val rowtime = MiniBatchInterval(3000L, MiniBatchMode.RowTime)
    assertEquals(MiniBatchInterval.NO_MINIBATCH,
      FlinkRelOptUtil.mergeMiniBatchInterval(MiniBatchInterval.NO_MINIBATCH, rowtime))
    assertEquals(MiniBatchInterval.NO_MINIBATCH,
      FlinkRelOptUtil.mergeMiniBatchInterval(rowtime, MiniBatchInterval.NO_MINIBATCH))
    val proctime = MiniBatchInterval(1000L, MiniBatchMode.ProcTime)
    assertEquals(MiniBatchInterval.NO_MINIBATCH,
      FlinkRelOptUtil.mergeMiniBatchInterval(MiniBatchInterval.NO_MINIBATCH, proctime))
    assertEquals(MiniBatchInterval.NO_MINIBATCH,
      FlinkRelOptUtil.mergeMiniBatchInterval(proctime, MiniBatchInterval.NO_MINIBATCH))
  }

}
