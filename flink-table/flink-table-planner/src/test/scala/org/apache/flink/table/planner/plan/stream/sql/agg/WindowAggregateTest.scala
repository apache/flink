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

import org.apache.flink.configuration.Configuration
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.{WeightedAvgWithMerge, WeightedAvg}
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy.{TABLE_EXEC_EMIT_EARLY_FIRE_DELAY, TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED, TABLE_EXEC_EMIT_LATE_FIRE_DELAY, TABLE_EXEC_EMIT_LATE_FIRE_ENABLED}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.TestPythonAggregateFunction
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.JavaTableFunc1
import org.apache.flink.table.planner.utils.{AggregatePhaseStrategy, TableTestBase}
import org.junit.Assume.assumeTrue
import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.util

/**
 * Tests for window aggregates based on window table-valued function.
 */
@RunWith(classOf[Parameterized])
class WindowAggregateTest(aggPhaseEnforcer: AggregatePhaseStrategy) extends TableTestBase {

  private val util = streamTestUtil()

  /**
   * Some tests are not need to test on both mode, because they can't apply two-phase optimization.
   * In order to reduce xml plan size, we can just test on the default mode,
   * i.e. TWO_PHASE (on streaming).
   */
  private val isTwoPhase = aggPhaseEnforcer == AggregatePhaseStrategy.TWO_PHASE

  @Before
  def before(): Unit = {
    util.addTemporarySystemFunction("weightedAvg", classOf[WeightedAvgWithMerge])
    util.addTemporarySystemFunction("weightedAvgWithoutMerge", classOf[WeightedAvg])
    util.tableEnv.executeSql(
      s"""
         |CREATE TABLE MyTable (
         |  a INT,
         |  b BIGINT,
         |  c STRING NOT NULL,
         |  d DECIMAL(10, 3),
         |  e BIGINT,
         |  rowtime TIMESTAMP(3),
         |  proctime as PROCTIME(),
         |  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
         |) with (
         |  'connector' = 'values'
         |)
         |""".stripMargin)

    // set agg-phase strategy
    util.tableEnv.getConfig.getConfiguration.setString(
      OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY,
      aggPhaseEnforcer.toString)
  }

  @Test
  def testTumble_OnRowtime(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_OnProctime(): Unit = {
    assumeTrue(isTwoPhase)
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_CalcOnTVF(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM (
        |  SELECT window_start, rowtime, d, proctime, e, b, c, window_end, window_time, a
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  WHERE b > 1000
        |)
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_WindowColumnsAtEnd(): Unit = {
    // there shouldn't be any Calc on the WindowAggregate,
    // because fields order are align with WindowAggregate schema
    val sql =
      """
        |SELECT
        |   a,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv,
        |   window_start,
        |   window_end
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_GroupMultipleWindowColumns(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   ws,
        |   window_end,
        |   window_time,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM (
        |  SELECT *, window_start as ws
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |)
        |GROUP BY a, window_start, window_end, ws, window_time
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_GroupMultipleKeys(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY window_start, a, window_end, b
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_GroupOnlyWindowColumns(): Unit = {
    val sql =
      """
        |SELECT
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_GroupOnLiteralValue(): Unit = {
    val sql =
      """
        |SELECT
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY 'literal', window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_ProjectionPushDown(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d)
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_CascadingWindow(): Unit = {
    util.tableEnv.executeSql(
      """
        |CREATE VIEW window1 AS
        |SELECT
        |   a,
        |   b,
        |   window_time as rowtime,
        |   count(*) as cnt,
        |   sum(d) as sum_d,
        |   max(d) as max_d
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE))
        |GROUP BY a, window_start, window_end, window_time, b
      """.stripMargin)

    val sql =
      """
        |SELECT
        |  a,
        |  window_start,
        |  window_end,
        |  sum(cnt),
        |  sum(sum_d),
        |  max(max_d)
        |FROM TABLE(TUMBLE(TABLE window1, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE))
        |GROUP BY a, window_start, window_end
        |""".stripMargin

    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_DistinctSplitEnabled(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_DistinctOnWindowColumns(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
    // window_time is used in agg arg, thus we shouldn't merge WindowTVF into WindowAggregate.
    // actually, after expanded, there's HASH_CODE(window_time),
    // and thus we shouldn't transpose WindowTVF and Expand too.
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   max(d) filter (where b > 1000),
        |   count(distinct window_time) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_DoNotSplitProcessingTimeWindow(): Unit = {
    assumeTrue(isTwoPhase)
    // the processing-time window aggregate with distinct shouldn't be split into two-level agg
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_NotOutputWindowColumns(): Unit = {
    val sql =
      """
        |SELECT
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_UdafWithoutMerge(): Unit = {
    assumeTrue(isTwoPhase)
    // the window aggregation shouldn't be split into local-global window aggregation
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvgWithoutMerge(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulate_OnRowtime(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulate_OnProctime(): Unit = {
    assumeTrue(isTwoPhase)
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulate_DistinctSplitEnabled(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testHop_OnRowtime(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |   HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testHop_OnProctime(): Unit = {
    assumeTrue(isTwoPhase)
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |   HOP(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testHop_DistinctSplitEnabled(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   count(distinct c) AS uv
        |FROM TABLE(
        |   HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testMultipleAggregateOnSameWindowTVF(): Unit = {
    util.tableEnv.executeSql(
      """
        |CREATE VIEW tvf AS
        |SELECT * FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |""".stripMargin)
    val statementSet = util.tableEnv.createStatementSet()
    util.tableEnv.executeSql(
      """
        |CREATE TABLE s1 (
        |  wstart TIMESTAMP(3),
        |  wend TIMESTAMP(3),
        |  `result` BIGINT
        |) WITH (
        |  'connector' = 'values'
        |)
        |""".stripMargin)

    statementSet.addInsertSql(
      """
        |INSERT INTO s1
        |SELECT
        |   window_start,
        |   window_end,
        |   weightedAvg(b, e) AS wAvg
        |FROM tvf
        |GROUP BY window_start, window_end
        |""".stripMargin)

    statementSet.addInsertSql(
      """
        |INSERT INTO s1
        |SELECT
        |   window_start,
        |   window_end,
        |   count(*)
        |FROM tvf
        |GROUP BY window_start, window_end
        |""".stripMargin)

    util.verifyExecPlan(statementSet)
  }

  // ----------------------------------------------------------------------------------------
  // Tests for queries can't be translated to window aggregate for now
  // ----------------------------------------------------------------------------------------

  @Test
  def testCantMergeWindowTVF_FilterOnWindowStart(): Unit = {
    val sql =
      """
        |SELECT
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM (
        |  SELECT window_start, rowtime, d, proctime, e, b, c, window_end, window_time, a
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |  WHERE window_start >= TIMESTAMP '2021-01-01 10:10:00.000'
        |)
        |GROUP BY window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCantMergeWindowTVF_UdtfOnWindowTVF(): Unit = {
    util.tableEnv.createTemporaryFunction("len_udtf", classOf[JavaTableFunc1])
    val sql =
      """
        |SELECT
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(len),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM (
        |  SELECT *
        |  FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE)),
        |  LATERAL TABLE(len_udtf(c)) AS T(len)
        |)
        |GROUP BY window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCantTranslateToWindowAgg_GroupOnOnlyStart(): Unit = {
    assumeTrue(isTwoPhase)
    val sql =
      """
        |SELECT
        |   window_start,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY window_start
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCantTranslateToWindowAgg_PythonAggregateCall(): Unit = {
    assumeTrue(isTwoPhase)
    util.tableEnv.createTemporaryFunction("python_agg", classOf[TestPythonAggregateFunction])
    val sql =
      """
        |SELECT
        |   window_start,
        |   window_end,
        |   python_agg(1, 1)
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testUnsupportedException_EarlyFire(): Unit = {
    val conf = new Configuration()
    conf.setString(TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED.key(), "true")
    conf.setString(TABLE_EXEC_EMIT_EARLY_FIRE_DELAY.key(), "5s")
    util.tableEnv.getConfig.addConfiguration(conf)

    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin

    thrown.expect(classOf[TableException])
    thrown.expectMessage("Currently, window table function based aggregate doesn't support " +
      "early-fire and late-fire configuration 'table.exec.emit.early-fire.enabled' and " +
      "'table.exec.emit.late-fire.enabled'.")
    util.verifyExecPlan(sql)
  }

  @Test
  def testUnsupportedException_LateFire(): Unit = {
    val conf = new Configuration()
    conf.setString(TABLE_EXEC_EMIT_LATE_FIRE_ENABLED.key(), "true")
    conf.setString(TABLE_EXEC_EMIT_LATE_FIRE_DELAY.key(), "5s")
    util.tableEnv.getConfig.addConfiguration(conf)

    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin

    thrown.expect(classOf[TableException])
    thrown.expectMessage("Currently, window table function based aggregate doesn't support " +
      "early-fire and late-fire configuration 'table.exec.emit.early-fire.enabled' and " +
      "'table.exec.emit.late-fire.enabled'.")
    util.verifyExecPlan(sql)
  }

  @Test
  def testUnsupportedException_HopSizeNonDivisible(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*)
        |FROM TABLE(
        |   HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '4' MINUTE, INTERVAL '10' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin

    thrown.expect(classOf[TableException])
    thrown.expectMessage("HOP table function based aggregate requires size must be an " +
      "integral multiple of slide, but got size 600000 ms and slide 240000 ms")
    util.verifyExplain(sql)
  }

  @Test
  def testUnsupportedException_CumulateSizeNonDivisible(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   window_start,
        |   window_end,
        |   count(*)
        |FROM TABLE(
        |   CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '25' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY a, window_start, window_end
      """.stripMargin

    thrown.expect(classOf[TableException])
    thrown.expectMessage("CUMULATE table function based aggregate requires maxSize " +
      "must be an integral multiple of step, but got maxSize 3600000 ms and step 1500000 ms")
    util.verifyExplain(sql)
  }

  @Test
  def testCantTranslateToWindowAgg_GroupingSetsWithoutWindowStartEnd(): Unit = {
    assumeTrue(isTwoPhase)
    // Cannot translate to window aggregate because group keys don't contain both window_start
    // and window_end
    val sql =
      """
        |SELECT
        |   a,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY GROUPING SETS ((a), (window_start), (window_end))
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCantTranslateToWindowAgg_GroupingSetsOnlyWithWindowStart(): Unit = {
    assumeTrue(isTwoPhase)
    val sql =
      """
        |SELECT
        |   a,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY GROUPING SETS ((a, window_start), (window_start))
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_GroupingSets(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY GROUPING SETS ((a, window_start, window_end), (b, window_start, window_end))
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_GroupingSets1(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY GROUPING SETS ((a), (b)), window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_GroupingSetsDistinctSplitEnabled(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY GROUPING SETS ((a), (b)), window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCantTranslateToWindowAgg_CubeWithoutWindowStartEnd(): Unit = {
    assumeTrue(isTwoPhase)
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY CUBE (a, b, window_start, window_end)
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCantTranslateToWindowAgg_RollupWithoutWindowStartEnd(): Unit = {
    assumeTrue(isTwoPhase)
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY ROLLUP (a, b, window_start, window_end)
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testTumble_Rollup(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(distinct c) AS uv
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY ROLLUP (a, b), window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCantMergeWindowTVF_GroupingSetsDistinctOnWindowColumns(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
    // window_time is used in agg arg, thus we shouldn't merge WindowTVF into WindowAggregate.
    // actually, after expanded, there's HASH_CODE(window_time),
    // and thus we shouldn't transpose WindowTVF and Expand too.
    val sql =
    """
      |SELECT
      |   a,
      |   b,
      |   count(*),
      |   max(d) filter (where b > 1000),
      |   count(distinct window_time) AS uv
      |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
      |GROUP BY GROUPING SETS ((a), (b)), window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testHop_GroupingSets(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |   HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |GROUP BY GROUPING SETS ((a), (b)), window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testHop_GroupingSets_DistinctSplitEnabled(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(*),
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |GROUP BY GROUPING SETS ((a), (b)), window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testHop_Cube(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |GROUP BY CUBE (a, b), window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testHop_Rollup(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  HOP(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |GROUP BY ROLLUP (a, b), window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulate_GroupingSets(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |   CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '25' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY GROUPING SETS ((a), (b)), window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulate_GroupingSets_DistinctSplitEnabled(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(*),
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY GROUPING SETS ((a), (b)), window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulate_Cube(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY CUBE (a, b), window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testCumulate_Rollup(): Unit = {
    val sql =
      """
        |SELECT
        |   a,
        |   b,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  CUMULATE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY ROLLUP (a, b), window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @Test
  def testFieldNameConflict(): Unit = {
    val sql =
      """
        |SELECT window_time,
        |  MIN(rowtime) as start_time,
        |  MAX(rowtime) as end_time
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY window_start, window_end, window_time
      """.stripMargin
    util.verifyRelPlan(sql)
  }
}

object WindowAggregateTest {
  @Parameterized.Parameters(name = "aggPhaseEnforcer={0}")
  def parameters(): util.Collection[Array[Any]] = {
    util.Arrays.asList(
      Array(AggregatePhaseStrategy.ONE_PHASE),
      Array(AggregatePhaseStrategy.TWO_PHASE)
    )
  }
}
