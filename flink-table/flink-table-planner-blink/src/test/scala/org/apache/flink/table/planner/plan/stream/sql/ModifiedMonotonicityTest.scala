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

package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.functions.ScalarFunction
import org.apache.flink.table.planner.plan.`trait`.RelModifiedMonotonicity
import org.apache.flink.table.planner.plan.metadata.FlinkRelMetadataQuery
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.planner.utils.{StreamTableTestUtil, TableTestBase, TableTestUtil}

import org.apache.calcite.sql.validate.SqlMonotonicity.{CONSTANT, DECREASING, INCREASING, NOT_MONOTONIC}
import org.junit.Assert.assertEquals
import org.junit.Test

class ModifiedMonotonicityTest extends TableTestBase {

  val util: StreamTableTestUtil = streamTestUtil()
  util.addDataStream[(Int, Long, Long)]("A", 'a1, 'a2, 'a3)
  util.addDataStream[(Int, Long, Long)]("B", 'b1, 'b2, 'b3)
  util.addDataStream[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
  util.addFunction("weightedAvg", new WeightedAvgWithMerge)
  util.addTableSource[(Int, Long, String)]("AA", 'a1, 'a2, 'a3)
  util.addTableSource[(Int, Long, Int, String, Long)]("BB", 'b1, 'b2, 'b3, 'b4, 'b5)

  @Test
  def testMaxWithRetractOptimize(): Unit = {
    val query =
      "SELECT a1, MAX(a3) FROM (SELECT a1, a2, MAX(a3) AS a3 FROM A GROUP BY a1, a2) t GROUP BY a1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMinWithRetractOptimize(): Unit = {
    val query =
      "SELECT a1, MIN(a3) FROM (SELECT a1, a2, MIN(a3) AS a3 FROM A GROUP BY a1, a2) t GROUP BY a1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMinCanNotOptimize(): Unit = {
    val query =
      "SELECT a1, MIN(a3) FROM (SELECT a1, a2, MAX(a3) AS a3 FROM A GROUP BY a1, a2) t GROUP BY a1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMaxWithRetractOptimizeWithLocalGlobal(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration
      .setString(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "100 ms")
    val query = "SELECT a1, max(a3) from (SELECT a1, a2, max(a3) as a3 FROM A GROUP BY a1, a2) " +
      "group by a1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMinWithRetractOptimizeWithLocalGlobal(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration
      .setString(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "100 ms")
    val query = "SELECT min(a3) from (SELECT a1, a2, min(a3) as a3 FROM A GROUP BY a1, a2)"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMinCanNotOptimizeWithLocalGlobal(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration
      .setString(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "100 ms")
    val query =
      "SELECT a1, MIN(a3) FROM (SELECT a1, a2, MAX(a3) AS a3 FROM A GROUP BY a1, a2) t GROUP BY a1"
    util.verifyPlan(query, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testTumbleFunAndRegularAggFunInGroupBy(): Unit = {
    val sql = "SELECT b, d, weightedAvg(c, a) FROM " +
      " (SELECT a, b, c, count(*) d," +
      "  TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as ping_start " +
      " FROM MyTable " +
      "  GROUP BY a, b, c, TUMBLE(rowtime, INTERVAL '15' MINUTE)) AS t1 " +
      "GROUP BY b, d, ping_start"
    verifyMonotonicity(sql, new RelModifiedMonotonicity(Array(CONSTANT, CONSTANT, NOT_MONOTONIC)))
  }

  @Test
  def testAntiJoin(): Unit = {
    val sql = "SELECT * FROM AA WHERE NOT EXISTS (SELECT b1 from BB WHERE a1 = b1)"
    verifyMonotonicity(sql, null)
  }

  @Test
  def testSemiJoinWithNonEqual(): Unit = {
    val query1 = "SELECT MAX(a2) AS a2, a1 FROM AA group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM BB group by b1"
    val sql = s"SELECT * FROM ($query1) WHERE a1 in (SELECT b1 from ($query2) WHERE a2 < b2)"
    verifyMonotonicity(sql, null)
  }

  @Test
  def testSemiJoin(): Unit = {
    val query1 = "SELECT MAX(a2) AS a2, a1 FROM AA group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM BB group by b1"
    val sql = s"SELECT a1, a2 FROM ($query1) WHERE a1 in (SELECT b1 from ($query2))"
    verifyMonotonicity(sql, new RelModifiedMonotonicity(Array(CONSTANT, INCREASING)))
  }

  @Test
  def testInnerJoinOnAggResult(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM AA group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM BB group by b1"
    val sql = s"SELECT a1, a2, b1, b2 FROM ($query1) JOIN ($query2) ON a2 = b2"
    verifyMonotonicity(sql, null)
  }

  @Test
  def testInnerJoin(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM AA group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM BB group by b1"
    val sql = s"SELECT a1, a2, b1, b2 FROM ($query1) JOIN ($query2) ON a1 = b1"
    verifyMonotonicity(
      sql,
      new RelModifiedMonotonicity(Array(CONSTANT, NOT_MONOTONIC, CONSTANT, NOT_MONOTONIC)))
  }

  @Test
  def testUnionAll(): Unit = {
    val query1 = "SELECT SUM(a2) AS a2, a1 FROM AA group by a1"
    val query2 = "SELECT SUM(b2) AS b2, b1 FROM BB group by b1"
    val sql = s"SELECT a1, a2 FROM ($query1) union all ($query2)"
    verifyMonotonicity(sql, new RelModifiedMonotonicity(Array(NOT_MONOTONIC, NOT_MONOTONIC)))
  }

  @Test
  def testOver(): Unit = {
    val sql = "SELECT a, " +
      "  SUM(c) OVER (" +
      "    PARTITION BY a ORDER BY proctime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW), " +
      "  MIN(c) OVER (" +
      "    PARTITION BY a ORDER BY proctime ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) " +
      "FROM MyTable"
    verifyMonotonicity(sql, new RelModifiedMonotonicity(Array(CONSTANT, CONSTANT, CONSTANT)))
  }

  @Test
  def testMultiOperandsForCalc(): Unit = {
    util.addFunction("func1", new Func1)
    val sql = "SELECT func1(func1(a1, a3)) from " +
      "(SELECT last_value(a1) as a1, last_value(a3) as a3 FROM AA group by a2) "
    verifyMonotonicity(sql, new RelModifiedMonotonicity(Array(NOT_MONOTONIC)))
  }

  @Test
  def testTopNDesc(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT b, c, a,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC, c ASC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num <= 10
      """.stripMargin
    verifyMonotonicity(
      sql,
      new RelModifiedMonotonicity(Array(INCREASING, NOT_MONOTONIC, CONSTANT, CONSTANT)))
  }

  @Test
  def testTopNAsc(): Unit = {
    val sql =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b ASC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num <= 10
      """.stripMargin
    verifyMonotonicity(
      sql,
      new RelModifiedMonotonicity(Array(CONSTANT, DECREASING, NOT_MONOTONIC, CONSTANT)))
  }

  @Test
  def testTopNAfterGroupBy(): Unit = {
    val subquery =
      """
        |SELECT a, b, COUNT(*) as count_c
        |FROM MyTable
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, count_c,
         |      ROW_NUMBER() OVER (PARTITION BY b ORDER BY count_c DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin
    verifyMonotonicity(
      sql,
      new RelModifiedMonotonicity(Array(NOT_MONOTONIC, CONSTANT, INCREASING, CONSTANT)))
  }

  @Test
  def testTopNAfterGroupBy2(): Unit = {
    val subquery =
      """
        |SELECT a, b, COUNT(*) as count_c
        |FROM MyTable
        |GROUP BY a, b
      """.stripMargin

    val sql =
      s"""
         |SELECT *
         |FROM (
         |  SELECT a, b, count_c,
         |      ROW_NUMBER() OVER (PARTITION BY count_c ORDER BY a DESC) as rank_num
         |  FROM ($subquery))
         |WHERE rank_num <= 10
      """.stripMargin
    verifyMonotonicity(sql, null)
  }

  def verifyMonotonicity(sql: String, expect: RelModifiedMonotonicity): Unit = {
    val table = util.tableEnv.sqlQuery(sql)
    val relNode = TableTestUtil.toRelNode(table)
    val optimized = util.getPlanner.optimize(relNode)

    val actualMono = FlinkRelMetadataQuery.reuseOrCreate(optimized.getCluster.getMetadataQuery)
      .getRelModifiedMonotonicity(optimized)
    assertEquals(expect, actualMono)
  }
}

@SerialVersionUID(1L)
class Func1 extends ScalarFunction {
  def eval(str: String): String = {
    s"$str"
  }

  def eval(index: Integer, str: String): String = {
    s"$index and $str"
  }
}
