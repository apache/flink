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

package org.apache.flink.table.planner.plan.stream.sql.agg

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedScalarFunctions.OverAgg0
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}

import org.junit.Assert.assertEquals
import org.junit.Test

class OverAggregateTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addDataStream[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)

  def verifyPlanIdentical(sql1: String, sql2: String): Unit = {
    val table1 = util.tableEnv.sqlQuery(sql1)
    val table2 = util.tableEnv.sqlQuery(sql2)
    val optimized1 = util.getPlanner.optimize(TableTestUtil.toRelNode(table1))
    val optimized2 = util.getPlanner.optimize(TableTestUtil.toRelNode(table2))
    assertEquals(FlinkRelOptUtil.toString(optimized1), FlinkRelOptUtil.toString(optimized2))
  }

  /**
    * All aggregates must be computed on the same window.
    */
  @Test(expected = classOf[TableException])
  def testMultiWindow(): Unit = {
    val sqlQuery =
      """
        |SELECT c,
        |    COUNT(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED PRECEDING),
        |    SUM(a) OVER (PARTITION BY b ORDER BY proctime RANGE UNBOUNDED PRECEDING)
        |from MyTable
      """.stripMargin

    util.verifyPlan(sqlQuery)
  }

  /**
    * OVER clause is necessary for [[OverAgg0]] window function.
    */
  @Test(expected = classOf[ValidationException])
  def testInvalidOverAggregation(): Unit = {
    util.addFunction("overAgg", new OverAgg0)
    util.verifyPlan("SELECT overAgg(c, a) FROM MyTable")
  }

  /**
    * OVER clause is necessary for [[OverAgg0]] window function.
    */
  @Test(expected = classOf[ValidationException])
  def testInvalidOverAggregation2(): Unit = {
    util.addFunction("overAgg", new OverAgg0)
    util.verifyPlan("SELECT overAgg(c, a) FROM MyTable")
  }

  @Test
  def testProctimeBoundedDistinctWithNonDistinctPartitionedRowOver(): Unit = {
    val sqlQuery1 =
      """
        |SELECT b,
        |    COUNT(a) OVER (PARTITION BY b ORDER BY proctime
        |        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cnt1,
        |    SUM(a) OVER (PARTITION BY b ORDER BY proctime
        |        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum1,
        |    COUNT(DISTINCT a) OVER (PARTITION BY b ORDER BY proctime
        |        ROWS BETWEEN 2 preceding AND CURRENT ROW) AS cnt2,
        |    sum(DISTINCT c) OVER (PARTITION BY b ORDER BY proctime
        |        ROWS BETWEEN 2 preceding AND CURRENT ROW) AS sum2
        |FROM MyTable
      """.stripMargin

    val sqlQuery2 =
      """
        |SELECT b,
        |    COUNT(a) OVER w AS cnt1,
        |    SUM(a) OVER w AS sum1,
        |    COUNT(DISTINCT a) OVER w AS cnt2,
        |    SUM(DISTINCT c) OVER w AS sum2
        |FROM MyTable
        |    WINDOW w AS (PARTITION BY b ORDER BY proctime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
      """.stripMargin

    verifyPlanIdentical(sqlQuery1, sqlQuery2)
    util.verifyPlan(sqlQuery1)
  }

  @Test
  def testProctimeBoundedDistinctPartitionedRowOver(): Unit = {
    val sqlQuery1 =
      """
        |SELECT c,
        |    COUNT(DISTINCT a) OVER (PARTITION BY c ORDER BY proctime
        |        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cnt1,
        |    SUM(DISTINCT a) OVER (PARTITION BY c ORDER BY proctime
        |        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum1
        |FROM MyTable
      """.stripMargin

    val sqlQuery2 =
      """
        |SELECT c,
        |    COUNT(DISTINCT a) OVER w AS cnt1,
        |    SUM(DISTINCT a) OVER (PARTITION BY c ORDER BY proctime
        |        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum1
        |FROM MyTable
        |    WINDOW w AS (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
      """.stripMargin

    verifyPlanIdentical(sqlQuery1, sqlQuery2)
    util.verifyPlan(sqlQuery1)
  }

  @Test
  def testProcTimeBoundedPartitionedRowsOver(): Unit = {
    val sqlQuery1 =
      """
        |SELECT c,
        |    COUNT(a) OVER (PARTITION BY c ORDER BY proctime
        |        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cnt1,
        |    SUM(a) OVER (PARTITION BY c ORDER BY proctime
        |        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS sum1
        |FROM MyTable
      """.stripMargin

    val sqlQuery2 =
      """
        |SELECT c,
        |    COUNT(a) OVER (PARTITION BY c ORDER BY proctime
        |        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS cnt1,
        |    SUM(a) OVER w AS sum1
        |FROM MyTable
        |    WINDOW w AS (PARTITION BY c ORDER BY proctime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
      """.stripMargin

    verifyPlanIdentical(sqlQuery1, sqlQuery2)
    util.verifyPlan(sqlQuery1)
  }

  @Test
  def testProcTimeBoundedPartitionedRangeOver(): Unit = {
    val sqlQuery1 =
      """
        |SELECT a,
        |    AVG(c) OVER (PARTITION BY a ORDER BY proctime
        |        RANGE BETWEEN INTERVAL '2' HOUR PRECEDING AND CURRENT ROW) AS avgA
        |FROM MyTable
      """.stripMargin

    val sqlQuery2 =
      """
        |SELECT a,  AVG(c) OVER w AS avgA FROM MyTable WINDOW w AS (
        |    PARTITION BY a ORDER BY proctime
        |        RANGE BETWEEN INTERVAL '2' HOUR PRECEDING AND CURRENT ROW)
      """.stripMargin

    verifyPlanIdentical(sqlQuery1, sqlQuery2)
    util.verifyPlan(sqlQuery1)
  }

  @Test
  def testProcTimeBoundedNonPartitionedRangeOver(): Unit = {
    val sqlQuery1 =
      """
        |SELECT a,
        |    COUNT(c) OVER (ORDER BY proctime
        |        RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW)
        | FROM MyTable
      """.stripMargin

    val sqlQuery2 =
      """
        |SELECT a, COUNT(c) OVER w FROM MyTable WINDOW w AS (
        |    ORDER BY proctime RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW)
      """.stripMargin

    verifyPlanIdentical(sqlQuery1, sqlQuery2)
    util.verifyPlan(sqlQuery1)
  }

  @Test
  def testProcTimeBoundedNonPartitionedRowsOver(): Unit = {
    val sqlQuery1 =
      """
        |SELECT c,
        |    COUNT(a) OVER (ORDER BY proctime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
        |FROM MyTable
      """.stripMargin

    val sqlQuery2 =
      """
        |SELECT c, COUNT(a) OVER w FROM MyTable WINDOW w AS (
        |    ORDER BY proctime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
      """.stripMargin

    verifyPlanIdentical(sqlQuery1, sqlQuery2)
    util.verifyPlan(sqlQuery1)
  }

  @Test
  def testProcTimeUnboundedPartitionedRangeOver(): Unit = {
    val sql =
      """
        |SELECT c,
        |    COUNT(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED PRECEDING) AS cnt1,
        |    SUM(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED PRECEDING) AS cnt2
        |FROM MyTable
      """.stripMargin

    val sql2 =
      """
        |SELECT c,
        |    COUNT(a) OVER w AS cnt1,
        |    SUM(a) OVER (PARTITION BY c ORDER BY proctime RANGE UNBOUNDED PRECEDING) AS cnt2
        |FROM MyTable WINDOW w AS (
        |    PARTITION BY c ORDER BY proctime RANGE UNBOUNDED PRECEDING)
      """.stripMargin

    verifyPlanIdentical(sql, sql2)
    util.verifyPlan(sql)
  }

  @Test
  def testProcTimeUnboundedPartitionedRowsOver(): Unit = {
    val sql =
      """
        |SELECT c,
        |    COUNT(a) OVER (PARTITION BY c ORDER BY proctime
        |        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        |FROM MyTable
      """.stripMargin

    val sql2 =
      """
        |SELECT c, COUNT(a) OVER w FROM MyTable WINDOW w AS (
        |    PARTITION BY c ORDER BY proctime ROWS UNBOUNDED PRECEDING)
      """.stripMargin

    verifyPlanIdentical(sql, sql2)
    util.verifyPlan(sql)
  }

  @Test
  def testProcTimeUnboundedNonPartitionedRangeOver(): Unit = {
    val sql =
      """
        |SELECT c,
        |    COUNT(a) OVER (ORDER BY proctime RANGE UNBOUNDED PRECEDING) AS cnt1,
        |    SUM(a) OVER (ORDER BY proctime RANGE UNBOUNDED PRECEDING) AS cnt2
        |FROM MyTable
      """.stripMargin

    val sql2 =
      """
        |SELECT c,
        |    COUNT(a) OVER (ORDER BY proctime RANGE UNBOUNDED PRECEDING) AS cnt1,
        |    sum(a) OVER w AS cnt2
        |FROM MyTable WINDOW w AS(
        |    ORDER BY proctime RANGE UNBOUNDED PRECEDING)
      """.stripMargin

    verifyPlanIdentical(sql, sql2)
    util.verifyPlan(sql)
  }

  @Test
  def testProcTimeUnboundedNonPartitionedRowsOver(): Unit = {
    val sql =
      """
        |SELECT c,
        |    COUNT(a) OVER (ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
        |FROM MyTable
      """.stripMargin

    val sql2 =
      """
        |SELECT c, COUNT(a) OVER w FROM MyTable WINDOW w AS (
        |    ORDER BY proctime ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
      """.stripMargin

    verifyPlanIdentical(sql, sql2)
    util.verifyPlan(sql)
  }

  @Test
  def testRowTimeBoundedPartitionedRowsOver(): Unit = {
    val sql =
      """
        |SELECT c,
        |    COUNT(a) OVER (PARTITION BY c ORDER BY rowtime
        |        ROWS BETWEEN 5 preceding AND CURRENT ROW)
        |FROM MyTable
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRowTimeBoundedPartitionedRangeOver(): Unit = {
    val sql =
      """
        |SELECT c,
        |    COUNT(a) OVER (PARTITION BY c ORDER BY rowtime
        |        RANGE BETWEEN INTERVAL '1' SECOND  PRECEDING AND CURRENT ROW)
        |    FROM MyTable
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRowTimeBoundedNonPartitionedRowsOver(): Unit = {
    val sql =
      """
        |SELECT c,
        |    COUNT(a) OVER (ORDER BY rowtime ROWS BETWEEN 5 PRECEDING AND CURRENT ROW)
        |FROM MyTable
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRowTimeBoundedNonPartitionedRangeOver(): Unit = {
    val sql =
      """
        |SELECT c,
        |    COUNT(a) OVER (ORDER BY rowtime
        |        RANGE BETWEEN INTERVAL '1' SECOND  PRECEDING AND CURRENT ROW) AS cnt1
        |FROM MyTable
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRowTimeUnboundedPartitionedRangeOver(): Unit = {
    val sql =
      """
        |SELECT c,
        |    COUNT(a) OVER (PARTITION BY c ORDER BY rowtime RANGE UNBOUNDED PRECEDING) AS cnt1,
        |    SUM(a) OVER (PARTITION BY c ORDER BY rowtime RANGE UNBOUNDED PRECEDING) AS cnt2
        |FROM MyTable
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRowTimeUnboundedPartitionedRowsOver(): Unit = {
    val sql =
      """
        |SELECT c,
        |    COUNT(a) OVER (PARTITION BY c ORDER BY rowtime ROWS UNBOUNDED PRECEDING) AS cnt1,
        |    SUM(a) OVER (PARTITION BY c ORDER BY rowtime ROWS UNBOUNDED PRECEDING) AS cnt2
        |FROM MyTable
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRowTimeUnboundedNonPartitionedRangeOver(): Unit = {
    val sql =
      """
        |SELECT c,
        |    COUNT(a) OVER (ORDER BY rowtime RANGE UNBOUNDED PRECEDING) AS cnt1,
        |    SUM(a) OVER (ORDER BY rowtime RANGE UNBOUNDED PRECEDING) AS cnt2
        |FROM MyTable
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testRowTimeUnboundedNonPartitionedRowsOver(): Unit = {
    val sql =
      """
        |SELECT c,
        |    COUNT(a) OVER (ORDER BY rowtime ROWS UNBOUNDED PRECEDING) AS cnt1,
        |    SUM(a) OVER (ORDER BY rowtime ROWS UNBOUNDED preceding) AS cnt2
        |FROM MyTable
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test(expected = classOf[TableException])
  def testProcTimeBoundedPartitionedRowsOverDifferentWindows(): Unit = {
    val sql =
      """
        |SELECT a,
        |    SUM(c) OVER (PARTITION BY a ORDER BY proctime
        |       ROWS BETWEEN 3 PRECEDING AND CURRENT ROW),
        |    MIN(c) OVER (PARTITION BY a ORDER BY proctime
        |       ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
        |FROM MyTable
      """.stripMargin

    val sql2 = "SELECT " +
      "a, " +
      "SUM(c) OVER w1, " +
      "MIN(c) OVER w2 " +
      "FROM MyTable " +
      "WINDOW w1 AS (PARTITION BY a ORDER BY proctime ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)," +
      "w2 AS (PARTITION BY a ORDER BY proctime ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)"

    verifyPlanIdentical(sql, sql2)
    util.verifyPlan(sql)
  }
}
