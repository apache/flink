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

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy.{TABLE_EXEC_EMIT_LATE_FIRE_DELAY, TABLE_EXEC_EMIT_LATE_FIRE_ENABLED, TABLE_EXEC_EMIT_ALLOW_LATENESS}
import org.apache.flink.table.planner.utils.TableTestBase

import java.time.Duration

import org.junit.Test

class GroupWindowTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addDataStream[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
  util.addTemporarySystemFunction("weightedAvg", classOf[WeightedAvgWithMerge])
  util.tableEnv.executeSql(
    s"""
       |create table MyTable1 (
       |  a int,
       |  b bigint,
       |  c as proctime()
       |) with (
       |  'connector' = 'COLLECTION'
       |)
       |""".stripMargin)

  @Test(expected = classOf[TableException])
  def testTumbleWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB FROM MyTable " +
        "GROUP BY TUMBLE(proctime, INTERVAL '2' HOUR, TIME '10:00:00')"
    util.verifyExecPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testHopWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB FROM MyTable " +
        "GROUP BY HOP(proctime, INTERVAL '1' HOUR, INTERVAL '2' HOUR, TIME '10:00:00')"
    util.verifyExecPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testSessionWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB FROM MyTable " +
        "GROUP BY SESSION(proctime, INTERVAL '2' HOUR, TIME '10:00:00')"
    util.verifyExecPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testVariableWindowSize(): Unit = {
    val sql = "SELECT COUNT(*) FROM MyTable GROUP BY TUMBLE(proctime, c * INTERVAL '1' MINUTE)"
    util.verifyExecPlan(sql)
  }

  @Test(expected = classOf[ValidationException])
  def testWindowUdAggInvalidArgs(): Unit = {
    val sqlQuery = "SELECT SUM(a) AS sumA, weightedAvg(a, b) AS wAvg FROM MyTable " +
      "GROUP BY TUMBLE(proctime(), INTERVAL '2' HOUR, TIME '10:00:00')"
    util.verifyExecPlan(sqlQuery)
  }

  @Test(expected = classOf[AssertionError])
  def testWindowAggWithGroupSets(): Unit = {
    // TODO supports group sets
    // currently, the optimized plan is not collect, and an exception will be thrown in code-gen
    val sql =
    """
      |SELECT COUNT(*),
      |    TUMBLE_END(rowtime, INTERVAL '15' MINUTE) + INTERVAL '1' MINUTE
      |FROM MyTable
      |    GROUP BY rollup(TUMBLE(rowtime, INTERVAL '15' MINUTE), b)
    """.stripMargin
    util.verifyRelPlanNotExpected(sql, "TUMBLE(rowtime")
  }

  @Test
  def testWindowWrongWindowParameter1(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Window aggregate only support SECOND, MINUTE, HOUR, DAY as the time unit. " +
        "MONTH and YEAR time unit are not supported yet.")

    val sqlQuery =
      "SELECT COUNT(*) FROM MyTable GROUP BY TUMBLE(proctime, INTERVAL '1' MONTH)"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testWindowWrongWindowParameter2(): Unit = {
    expectedException.expect(classOf[TableException])
    expectedException.expectMessage(
      "Window aggregate only support SECOND, MINUTE, HOUR, DAY as the time unit. " +
        "MONTH and YEAR time unit are not supported yet.")

    val sqlQuery =
      "SELECT COUNT(*) FROM MyTable GROUP BY TUMBLE(proctime, INTERVAL '2-10' YEAR TO MONTH)"

    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testIntervalDay(): Unit = {
    val sqlQuery =
      "SELECT COUNT(*) FROM MyTable GROUP BY TUMBLE(proctime, INTERVAL '35' DAY)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testTumbleFunction(): Unit = {
    val sql =
      """
        |SELECT COUNT(*),
        |    weightedAvg(c, a) AS wAvg,
        |    TUMBLE_START(rowtime, INTERVAL '15' MINUTE),
        |    TUMBLE_END(rowtime, INTERVAL '15' MINUTE)
        |FROM MyTable
        |    GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testWindowGroupByOnConstant(): Unit = {
    val sql =
      """
        |SELECT COUNT(*),
        |    weightedAvg(c, a) AS wAvg,
        |    TUMBLE_START(rowtime, INTERVAL '15' MINUTE),
        |    TUMBLE_END(rowtime, INTERVAL '15' MINUTE)
        |FROM MyTable
        |    GROUP BY 'a', TUMBLE(rowtime, INTERVAL '15' MINUTE)
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testTumblingWindowWithProctime(): Unit = {
    val sql = "select sum(a), max(b) from MyTable1 group by TUMBLE(c, INTERVAL '1' SECOND)"
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultiHopWindows(): Unit = {
    val sql =
      """
        |SELECT
        |   HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' HOUR),
        |   HOP_END(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' HOUR),
        |   count(*),
        |   sum(c)
        |FROM MyTable
        |GROUP BY HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' HOUR)
        |UNION ALL
        |SELECT
        |   HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' DAY),
        |   HOP_END(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' DAY),
        |   count(*),
        |   sum(c)
        |FROM MyTable
        |GROUP BY HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' DAY)
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultiHopWindowsJoin(): Unit = {
    val sql =
      """
        |SELECT * FROM
        | (SELECT
        |   HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' HOUR) as hs1,
        |   HOP_END(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' HOUR) as he1,
        |   count(*) as c1,
        |   sum(c) as s1
        | FROM MyTable
        | GROUP BY HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' HOUR)) t1
        |JOIN
        | (SELECT
        |   HOP_START(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' DAY) as hs2,
        |   HOP_END(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' DAY) as he2,
        |   count(*) as c2,
        |   sum(c) as s2
        | FROM MyTable
        | GROUP BY HOP(rowtime, INTERVAL '1' MINUTE, INTERVAL '1' DAY)) t2 ON t1.he1 = t2.he2
        |WHERE t1.s1 IS NOT NULL
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testHoppingFunction(): Unit = {
    val sql =
      """
        |SELECT COUNT(*),
        |    weightedAvg(c, a) AS wAvg,
        |    HOP_START(proctime, INTERVAL '15' MINUTE, INTERVAL '1' HOUR),
        |    HOP_END(proctime, INTERVAL '15' MINUTE, INTERVAL '1' HOUR)
        |FROM MyTable
        |    GROUP BY HOP(proctime, INTERVAL '15' MINUTE, INTERVAL '1' HOUR)
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testHopWindowWithProctime(): Unit = {
    val sql =
      s"""
         |select sum(a), max(b)
         |from MyTable1
         |group by HOP(c, INTERVAL '1' SECOND, INTERVAL '1' MINUTE)
         |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testSessionFunction(): Unit = {
    val sql =
      """
        |SELECT
        |    COUNT(*), weightedAvg(c, a) AS wAvg,
        |    SESSION_START(proctime, INTERVAL '15' MINUTE),
        |    SESSION_END(proctime, INTERVAL '15' MINUTE)
        |FROM MyTable
        |    GROUP BY SESSION(proctime, INTERVAL '15' MINUTE)
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testSessionWindowWithProctime(): Unit = {
    val sql =
      s"""
         |select sum(a), max(b)
         |from MyTable1
         |group by SESSION(c, INTERVAL '1' MINUTE)
         |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testExpressionOnWindowAuxFunction(): Unit = {
    val sql =
      """
        |SELECT COUNT(*),
        |    TUMBLE_END(rowtime, INTERVAL '15' MINUTE) + INTERVAL '1' MINUTE
        |FROM MyTable
        |    GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultiWindowSqlWithAggregation(): Unit = {
    val sql =
      """
        |SELECT
        |  TUMBLE_ROWTIME(zzzzz, INTERVAL '0.004' SECOND),
        |  TUMBLE_END(zzzzz, INTERVAL '0.004' SECOND),
        |  COUNT(`a`) AS `a`
        |FROM (
        |  SELECT
        |    COUNT(`a`) AS `a`,
        |    TUMBLE_ROWTIME(rowtime, INTERVAL '0.002' SECOND) AS `zzzzz`
        |  FROM MyTable
        |  GROUP BY TUMBLE(rowtime, INTERVAL '0.002' SECOND)
        |)
        |GROUP BY TUMBLE(zzzzz, INTERVAL '0.004' SECOND)
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testTumbleFunInGroupBy(): Unit = {
    val sql =
      """
        |SELECT weightedAvg(c, a) FROM
        |    (SELECT a, b, c,
        |        TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as ping_start
        |     FROM MyTable
        |         GROUP BY a, b, c, TUMBLE(rowtime, INTERVAL '15' MINUTE)
        |     ) AS t1
        | GROUP BY b, ping_start
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testTumbleFunNotInGroupBy(): Unit = {
    val sql =
      """
        |SELECT weightedAvg(c, a) FROM
        |    (SELECT a, b, c,
        |        TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as ping_start
        |     FROM MyTable
        |         GROUP BY a, b, c, TUMBLE(rowtime, INTERVAL '15' MINUTE)) AS t1
        |GROUP BY b
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testTumbleFunAndRegularAggFunInGroupBy(): Unit = {
    val sql =
      """
        |SELECT weightedAvg(c, a) FROM
        |    (SELECT a, b, c, count(*) d,
        |        TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as ping_start
        |     FROM MyTable
        |         GROUP BY a, b, c, TUMBLE(rowtime, INTERVAL '15' MINUTE)) AS t1
        |GROUP BY b, d, ping_start
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testRegularAggFunInGroupByAndTumbleFunAndNotInGroupBy(): Unit = {
    val sql =
      """
        |SELECT weightedAvg(c, a) FROM
        |    (SELECT a, b, c, count(*) d,
        |        TUMBLE_START(rowtime, INTERVAL '15' MINUTE) as ping_start
        |     FROM MyTable
        |         GROUP BY a, b, c, TUMBLE(rowtime, INTERVAL '15' MINUTE)) AS t1
        |GROUP BY b, d
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testDecomposableAggFunctions(): Unit = {
    val sql =
      """
        |SELECT
        |    VAR_POP(c),
        |    VAR_SAMP(c),
        |    STDDEV_POP(c),
        |    STDDEV_SAMP(c),
        |    TUMBLE_START(rowtime, INTERVAL '15' MINUTE),
        |    TUMBLE_END(rowtime, INTERVAL '15' MINUTE)
        |FROM MyTable
        |    GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testExpressionOnWindowHavingFunction(): Unit = {
    val sql =
      """
        |SELECT COUNT(*),
        |    HOP_START(rowtime, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE)
        | FROM MyTable
        |     GROUP BY HOP(rowtime, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE)
        |     HAVING SUM(a) > 0 AND
        |         QUARTER(HOP_START(rowtime, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE)) = 1
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testReturnTypeInferenceForWindowAgg(): Unit = {

    val sql =
      """
        |SELECT
        |  SUM(correct) AS s,
        |  AVG(correct) AS a,
        |  TUMBLE_START(rowtime, INTERVAL '15' MINUTE) AS wStart
        |FROM (
        |  SELECT CASE a
        |      WHEN 1 THEN 1
        |      ELSE 99
        |    END AS correct, rowtime
        |  FROM MyTable
        |)
        |GROUP BY TUMBLE(rowtime, INTERVAL '15' MINUTE)
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testWindowAggregateWithDifferentWindows(): Unit = {
    // This test ensures that the LogicalWindowAggregate node' digest contains the window specs.
    // This allows the planner to make the distinction between similar aggregations using different
    // windows (see FLINK-15577).
    val sql =
    """
      |WITH window_1h AS (
      |    SELECT 1
      |    FROM MyTable
      |    GROUP BY HOP(`rowtime`, INTERVAL '1' HOUR, INTERVAL '1' HOUR)
      |),
      |
      |window_2h AS (
      |    SELECT 1
      |    FROM MyTable
      |    GROUP BY HOP(`rowtime`, INTERVAL '1' HOUR, INTERVAL '2' HOUR)
      |)
      |
      |(SELECT * FROM window_1h)
      |UNION ALL
      |(SELECT * FROM window_2h)
      |""".stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testWindowAggregateWithLateFire(): Unit = {
    util.conf.getConfiguration.setBoolean(TABLE_EXEC_EMIT_LATE_FIRE_ENABLED, true)
    util.conf.getConfiguration.set(TABLE_EXEC_EMIT_LATE_FIRE_DELAY, Duration.ofSeconds(5))
    util.conf.setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    val sql =
      """
        |SELECT TUMBLE_START(`rowtime`, INTERVAL '1' SECOND), COUNT(*) cnt
        |FROM MyTable
        |GROUP BY TUMBLE(`rowtime`, INTERVAL '1' SECOND)
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testWindowAggregateWithAllowLateness(): Unit = {
    util.conf.getConfiguration.setBoolean(TABLE_EXEC_EMIT_LATE_FIRE_ENABLED, true)
    util.conf.getConfiguration.set(TABLE_EXEC_EMIT_LATE_FIRE_DELAY, Duration.ofSeconds(5))
    util.conf.getConfiguration.set(TABLE_EXEC_EMIT_ALLOW_LATENESS, Duration.ofHours(1))
    val sql =
      """
        |SELECT TUMBLE_START(`rowtime`, INTERVAL '1' SECOND), COUNT(*) cnt
        |FROM MyTable
        |GROUP BY TUMBLE(`rowtime`, INTERVAL '1' SECOND)
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testWindowAggregateWithInvalidAllowLateness(): Unit = {
    util.conf.getConfiguration.setBoolean(TABLE_EXEC_EMIT_LATE_FIRE_ENABLED, true)
    util.conf.getConfiguration.set(TABLE_EXEC_EMIT_LATE_FIRE_DELAY, Duration.ofSeconds(5))
    util.conf.getConfiguration.set(TABLE_EXEC_EMIT_ALLOW_LATENESS, Duration.ofSeconds(1))
    val sql =
      """
        |SELECT TUMBLE_START(`rowtime`, INTERVAL '1' SECOND), COUNT(*) cnt
        |FROM MyTable
        |GROUP BY TUMBLE(`rowtime`, INTERVAL '1' SECOND)
        |""".stripMargin
    thrown.expect(classOf[TableException])
    thrown.expectMessage("Allow-lateness [1000ms] should not be smaller than " +
      "Late-fire delay [5000ms] when enable late-fire emit strategy.")
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testWindowAggregateWithAllowLatenessOnly(): Unit = {
    util.conf.setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    val sql =
      """
        |SELECT TUMBLE_START(`rowtime`, INTERVAL '1' SECOND), COUNT(*) cnt
        |FROM MyTable
        |GROUP BY TUMBLE(`rowtime`, INTERVAL '1' SECOND)
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testWindowAggregateOnRetractStream(): Unit = {
    val sql =
      """
        |SELECT TUMBLE_START(`rowtime`, INTERVAL '1' SECOND), COUNT(*) cnt
        |FROM  (
        | SELECT a, b, c, rowtime
        | FROM (
        |   SELECT *,
        |   ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rowNum
        |   FROM MyTable
        | )
        | WHERE rowNum = 1
        |)
        |GROUP BY TUMBLE(`rowtime`, INTERVAL '1' SECOND)
        |""".stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testWindowAggregateOnUpsertSource(): Unit = {
    util.addTable(
      """
        |CREATE TABLE src (
        |  ts AS PROCTIME(),
        |  a INT,
        |  b DOUBLE,
        |  PRIMARY KEY (a) NOT ENFORCED
        |) WITH (
        |  'connector' = 'values',
        |  'changelog-mode' = 'UA,D'
        |)
      """.stripMargin)
    val query =
      """
        |SELECT TUMBLE_START(ts, INTERVAL '10' SECOND), COUNT(*)
        |FROM src
        |GROUP BY TUMBLE(ts, INTERVAL '10' SECOND)
        |""".stripMargin
    util.verifyRelPlan(query, ExplainDetail.CHANGELOG_MODE)
  }
}
