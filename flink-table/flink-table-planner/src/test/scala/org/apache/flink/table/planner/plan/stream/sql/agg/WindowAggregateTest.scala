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
import org.apache.flink.table.api.{ExplainDetail, TableException, ValidationException}
import org.apache.flink.table.api.config.{AggregatePhaseStrategy, OptimizerConfigOptions}
import org.apache.flink.table.planner.plan.utils.JavaUserDefinedAggFunctions.{WeightedAvg, WeightedAvgWithMerge}
import org.apache.flink.table.planner.plan.utils.WindowEmitStrategy.{TABLE_EXEC_EMIT_EARLY_FIRE_DELAY, TABLE_EXEC_EMIT_EARLY_FIRE_ENABLED, TABLE_EXEC_EMIT_LATE_FIRE_DELAY, TABLE_EXEC_EMIT_LATE_FIRE_ENABLED}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.TestPythonAggregateFunction
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.{JavaTableFunc1, StringSplit}
import org.apache.flink.table.planner.utils.TableTestBase
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}

import org.assertj.core.api.Assertions.assertThatThrownBy
import org.assertj.core.api.Assumptions.assumeThat
import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.util

/** Tests for window aggregates based on window table-valued function. */
@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class WindowAggregateTest(aggPhaseEnforcer: AggregatePhaseStrategy) extends TableTestBase {

  private val util = streamTestUtil()

  /**
   * Some tests are not need to test on both mode, because they can't apply two-phase optimization.
   * In order to reduce xml plan size, we can just test on the default mode,
   * i.e. TWO_PHASE (on streaming).
   */
  private val isTwoPhase = aggPhaseEnforcer == AggregatePhaseStrategy.TWO_PHASE

  @BeforeEach
  def before(): Unit = {
    util.addTemporarySystemFunction("weightedAvg", classOf[WeightedAvgWithMerge])
    util.addTemporarySystemFunction("weightedAvgWithoutMerge", classOf[WeightedAvg])
    util.tableEnv.executeSql(s"""
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

    util.tableEnv.executeSql(s"""
                                |CREATE TABLE MyCDCTable (
                                |  a INT,
                                |  b BIGINT,
                                |  c STRING NOT NULL,
                                |  d DECIMAL(10, 3),
                                |  e BIGINT,
                                |  rowtime TIMESTAMP(3),
                                |  proctime as PROCTIME(),
                                |  WATERMARK FOR rowtime AS rowtime - INTERVAL '1' SECOND
                                |) with (
                                |  'connector' = 'values',
                                |  'changelog-mode' = 'I,UA,UB,D'
                                |)
                                |""".stripMargin)

    util.tableEnv.executeSql(
      """
        |CREATE VIEW proctime_win AS
        |SELECT
        |   a,
        |   b,
        |   window_start as ws,
        |   window_end as we,
        |   window_time as wt,
        |   proctime() as new_proctime,
        |   count(*) as cnt,
        |   sum(d) as sum_d,
        |   max(d) as max_d
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '5' MINUTE))
        |GROUP BY a, window_start, window_end, window_time, b
      """.stripMargin)

    // set agg-phase strategy
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, aggPhaseEnforcer)
  }

  @TestTemplate
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

  @TestTemplate
  def testTumble_OnRowtimeWithCDCSource(): Unit = {
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
        |FROM TABLE(TUMBLE(TABLE MyCDCTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @TestTemplate
  def testTumble_OnProctime(): Unit = {
    assumeThat(isTwoPhase).isTrue
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

  @TestTemplate
  def testTumble_OnProctimeWithCDCSource(): Unit = {
    assumeThat(isTwoPhase).isTrue
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
        |FROM TABLE(TUMBLE(TABLE MyCDCTable, DESCRIPTOR(proctime), INTERVAL '15' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  def testTumble_CascadingWindow_RelaxForm(): Unit = {
    // a relax form of cascaded rowtime window which is actually supported
    util.verifyRelPlan(
      """
        |SELECT
        |  a,
        |  window_start,
        |  window_end,
        |  COUNT(*)
        |  FROM
        |  (
        |    SELECT
        |    a,
        |    window_start,
        |    window_end,
        |    COUNT(DISTINCT c) AS cnt
        |    FROM TABLE(
        |      TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '1' DAY, INTERVAL '8' HOUR))
        |    GROUP BY a, b, window_start, window_end
        |) GROUP BY a, window_start, window_end
      """.stripMargin)
  }

  @TestTemplate
  def testTumble_DistinctSplitEnabled(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, Boolean.box(true))
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

  @TestTemplate
  def testTumble_DistinctOnWindowColumns(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, Boolean.box(true))
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

  @TestTemplate
  def testTumble_DoNotSplitProcessingTimeWindow(): Unit = {
    assumeThat(isTwoPhase).isTrue
    // the processing-time window aggregate with distinct shouldn't be split into two-level agg
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, Boolean.box(true))
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

  @TestTemplate
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

  @TestTemplate
  def testTumble_UdafWithoutMerge(): Unit = {
    assumeThat(isTwoPhase).isTrue
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

  @TestTemplate
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

  @TestTemplate
  def testCumulate_OnRowtimeWithCDCSource(): Unit = {
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
        |  CUMULATE(TABLE MyCDCTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @TestTemplate
  def testCumulate_OnProctime(): Unit = {
    assumeThat(isTwoPhase).isTrue
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

  @TestTemplate
  def testCumulate_OnProctimeWithCDCSource(): Unit = {
    assumeThat(isTwoPhase).isTrue
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
        |  CUMULATE(TABLE MyCDCTable, DESCRIPTOR(proctime), INTERVAL '10' MINUTE, INTERVAL '1' HOUR))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @TestTemplate
  def testCumulate_DistinctSplitEnabled(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, Boolean.box(true))
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

  @TestTemplate
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

  @TestTemplate
  def testHop_OnRowtimeWithCDCSource(): Unit = {
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
        |   HOP(TABLE MyCDCTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @TestTemplate
  def testHop_OnProctime(): Unit = {
    assumeThat(isTwoPhase).isTrue
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

  @TestTemplate
  def testHop_OnProctimeWithCDCSource(): Unit = {
    assumeThat(isTwoPhase).isTrue
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
        |   HOP(TABLE MyCDCTable, DESCRIPTOR(proctime), INTERVAL '5' MINUTE, INTERVAL '10' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql, ExplainDetail.CHANGELOG_MODE)
  }

  @TestTemplate
  def testHop_DistinctSplitEnabled(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, Boolean.box(true))
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

  @TestTemplate
  def testMultipleAggregateOnSameWindowTVF(): Unit = {
    util.tableEnv.executeSql(
      """
        |CREATE VIEW tvf AS
        |SELECT * FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '15' MINUTE))
        |""".stripMargin)
    val statementSet = util.tableEnv.createStatementSet()
    util.tableEnv.executeSql("""
                               |CREATE TABLE s1 (
                               |  wstart TIMESTAMP(3),
                               |  wend TIMESTAMP(3),
                               |  `result` BIGINT
                               |) WITH (
                               |  'connector' = 'values'
                               |)
                               |""".stripMargin)

    statementSet.addInsertSql("""
                                |INSERT INTO s1
                                |SELECT
                                |   window_start,
                                |   window_end,
                                |   weightedAvg(b, e) AS wAvg
                                |FROM tvf
                                |GROUP BY window_start, window_end
                                |""".stripMargin)

    statementSet.addInsertSql("""
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  def testCantTranslateToWindowAgg_GroupOnOnlyStart(): Unit = {
    assumeThat(isTwoPhase).isTrue
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

  @TestTemplate
  def testCantTranslateToWindowAgg_PythonAggregateCall(): Unit = {
    assumeThat(isTwoPhase).isTrue
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

  @TestTemplate
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

    assertThatThrownBy(() => util.verifyExecPlan(sql))
      .hasMessageContaining(
        "Currently, window table function based aggregate doesn't support " +
          "early-fire and late-fire configuration 'table.exec.emit.early-fire.enabled' and " +
          "'table.exec.emit.late-fire.enabled'.")
      .isInstanceOf[TableException]
  }

  @TestTemplate
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

    assertThatThrownBy(() => util.verifyExecPlan(sql))
      .hasMessageContaining(
        "Currently, window table function based aggregate doesn't support " +
          "early-fire and late-fire configuration 'table.exec.emit.early-fire.enabled' and " +
          "'table.exec.emit.late-fire.enabled'.")
      .isInstanceOf[TableException]
  }

  @TestTemplate
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

    assertThatThrownBy(() => util.verifyExplain(sql))
      .hasMessageContaining("HOP table function based aggregate requires size must be an " +
        "integral multiple of slide, but got size 600000 ms and slide 240000 ms")
      .isInstanceOf[TableException]
  }

  @TestTemplate
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

    assertThatThrownBy(() => util.verifyExplain(sql))
      .hasMessageContaining("CUMULATE table function based aggregate requires maxSize " +
        "must be an integral multiple of step, but got maxSize 3600000 ms and step 1500000 ms")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testCantTranslateToWindowAgg_GroupingSetsWithoutWindowStartEnd(): Unit = {
    assumeThat(isTwoPhase).isTrue
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

  @TestTemplate
  def testCantTranslateToWindowAgg_GroupingSetsOnlyWithWindowStart(): Unit = {
    assumeThat(isTwoPhase).isTrue
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  def testTumble_GroupingSetsDistinctSplitEnabled(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, Boolean.box(true))
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

  @TestTemplate
  def testCantTranslateToWindowAgg_CubeWithoutWindowStartEnd(): Unit = {
    assumeThat(isTwoPhase).isTrue
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

  @TestTemplate
  def testCantTranslateToWindowAgg_RollupWithoutWindowStartEnd(): Unit = {
    assumeThat(isTwoPhase).isTrue
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

  @TestTemplate
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

  @TestTemplate
  def testCantMergeWindowTVF_GroupingSetsDistinctOnWindowColumns(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, Boolean.box(true))
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

  @TestTemplate
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

  @TestTemplate
  def testHop_GroupingSets_DistinctSplitEnabled(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, Boolean.box(true))
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  def testCumulate_GroupingSets_DistinctSplitEnabled(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, Boolean.box(true))
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
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

  @TestTemplate
  def testProctimeWindowWithFilter(): Unit = {
    util.tableEnv.executeSql(s"""
                                |CREATE TEMPORARY TABLE source (
                                |  a INT,
                                |  b BIGINT,
                                |  c STRING NOT NULL,
                                |  d BIGINT,
                                |  proctime as PROCTIME()
                                |) with (
                                |  'connector' = 'values'
                                |)
                                |""".stripMargin)

    util.tableEnv.executeSql("""
                               |CREATE TEMPORARY TABLE sink(
                               |    ws TIMESTAMP,
                               |    we TIMESTAMP,
                               |    b bigint,
                               |    c bigint
                               |)
                               |WITH (
                               |    'connector' = 'values'
                               |)
                               |""".stripMargin)

    util.verifyExecPlanInsert(
      """
        |insert into sink
        |    select
        |        window_start,
        |        window_end,
        |        b,
        |        COALESCE(sum(case
        |            when a = 11
        |            then 1
        |        end), 0) c
        |    from
        |        TABLE(
        |            TUMBLE(TABLE source, DESCRIPTOR(proctime), INTERVAL '10' SECONDS)
        |        )
        |    where
        |        a in (1, 5, 7, 9, 11)
        |    GROUP BY
        |        window_start, window_end, b
        |""".stripMargin)
  }

  @TestTemplate
  def testTumble_CascadingWindow_OnIndividualProctime(): Unit = {
    assumeThat(isTwoPhase).isFalse
    // a standard cascaded proctime window
    util.verifyExecPlan(
      """
        |SELECT
        |  window_start,
        |  window_end,
        |  sum(cnt),
        |  count(*)
        |FROM TABLE(TUMBLE(TABLE proctime_win, DESCRIPTOR(new_proctime), INTERVAL '10' MINUTE))
        |GROUP BY a, window_start, window_end
        |""".stripMargin)
  }

  @TestTemplate
  def testTumble_CascadingWindow_OnInheritProctime(): Unit = {
    assumeThat(isTwoPhase).isFalse
    // a standard cascaded proctime window
    util.verifyExecPlan(
      """
        |SELECT
        |  window_start,
        |  window_end,
        |  sum(cnt),
        |  count(*)
        |FROM TABLE(TUMBLE(TABLE proctime_win, DESCRIPTOR(wt), INTERVAL '10' MINUTE))
        |GROUP BY a, window_start, window_end
        |""".stripMargin)
  }

  @TestTemplate
  def testInvalidRelaxFormCascadeProctimeWindow(): Unit = {
    assumeThat(isTwoPhase).isFalse
    // a relax form of cascaded proctime window unsupported for now, will be translated to group agg
    util.verifyRelPlan("""
                         |SELECT
                         |  a,
                         |  ws,
                         |  we,
                         |  COUNT(*)
                         |FROM proctime_win
                         |GROUP BY a, ws, we
      """.stripMargin)
  }

  @TestTemplate
  def testTumble_CascadeProctimeWindow_OnWindowRank(): Unit = {
    assumeThat(isTwoPhase).isFalse
    // create window top10
    createProctimeWindowTopN("proctime_winrank", 10)

    util.verifyRelPlan(
      """
        |SELECT
        |  a,
        |  window_start,
        |  window_end,
        |  COUNT(*)
        |FROM TABLE(TUMBLE(TABLE proctime_winrank, DESCRIPTOR(new_proctime), INTERVAL '5' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin)
  }

  private def createProctimeWindowTopN(viewName: String, topNum: Int): Unit = {
    util.tableEnv.executeSql(
      s"""
         |CREATE VIEW $viewName AS
         |SELECT *
         |FROM(
         | SELECT
         |    a,
         |    b,
         |    window_start as ws,
         |    window_end as we,
         |    window_time as wt,
         |    proctime() as new_proctime,
         |    ROW_NUMBER() OVER (PARTITION BY window_start, window_end ORDER BY proctime DESC) AS rn
         | FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '5' MINUTE))
         |) WHERE rn <= $topNum
     """.stripMargin)
  }

  @TestTemplate
  def testInvalidRelaxFormCascadeProctimeWindow_OnWindowRank(): Unit = {
    assumeThat(isTwoPhase).isFalse
    // create window top10
    createProctimeWindowTopN("proctime_winrank", 10)

    // a relax form of cascaded proctime window on a window rank is unsupported for now, will be translated to group agg
    util.verifyRelPlan("""
                         |SELECT
                         |  a,
                         |  ws,
                         |  we,
                         |  COUNT(*)
                         |FROM proctime_winrank
                         |GROUP BY a, ws, we
      """.stripMargin)
  }

  @TestTemplate
  def testTumble_CascadeProctimeWindow_OnWindowDedup(): Unit = {
    assumeThat(isTwoPhase).isFalse
    // create window dedup(top1)
    createProctimeWindowTopN("proctime_windedup", 1)

    // a relax form of cascaded proctime window on a window dedup is unsupported for now, will be translated to group agg
    util.verifyRelPlan(
      """
        |SELECT
        |  a,
        |  window_start,
        |  window_end,
        |  COUNT(*)
        |FROM TABLE(TUMBLE(TABLE proctime_windedup, DESCRIPTOR(new_proctime), INTERVAL '5' MINUTE))
        |GROUP BY a, window_start, window_end
  """.stripMargin)
  }

  @TestTemplate
  def testInvalidRelaxFormCascadeProctimeWindow_OnWindowDedup(): Unit = {
    assumeThat(isTwoPhase).isFalse
    // create window dedup(top1)
    createProctimeWindowTopN("proctime_windedup", 1)

    // a relax form of cascaded proctime window unsupported for now, will be translated to group agg
    util.verifyRelPlan("""
                         |SELECT
                         |  a,
                         |  ws,
                         |  we,
                         |  COUNT(*)
                         |FROM proctime_windedup
                         |GROUP BY a, ws, we
      """.stripMargin)
  }

  @TestTemplate
  def testTumble_CascadeProctimeWindow_OnWindowJoin(): Unit = {
    assumeThat(isTwoPhase).isFalse
    createWindowJoin

    util.verifyRelPlan(
      """
        |SELECT
        |  a,
        |  window_start,
        |  window_end,
        |  COUNT(*)
        |FROM TABLE(TUMBLE(TABLE win_join, DESCRIPTOR(new_proctime), INTERVAL '5' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin)
  }

  private def createWindowJoin(): Unit = {
    util.tableEnv.executeSql(
      """
        |CREATE VIEW proctime_window AS
        |SELECT
        |   a,
        |   b,
        |   window_start,
        |   window_end
        |FROM TABLE(TUMBLE(TABLE MyTable, DESCRIPTOR(proctime), INTERVAL '5' MINUTE))
    """.stripMargin)

    util.tableEnv.executeSql(
      """
        |CREATE VIEW win_join AS
        |SELECT
        |   w1.a as a,
        |   w1.b as b,
        |   COALESCE(w1.window_start, w2.window_start) as ws,
        |   COALESCE(w1.window_end, w2.window_end) as we,
        |   proctime() as new_proctime
        |FROM proctime_window w1 join proctime_window w2
        |ON w1.window_start = w2.window_start AND w1.window_end = w2.window_end
    """.stripMargin)
  }

  @TestTemplate
  def testInvalidRelaxFormCascadeProctimeWindow_OnWindowJoin(): Unit = {
    assumeThat(isTwoPhase).isFalse
    createWindowJoin

    // a relax form of cascaded proctime window on a window join is unsupported for now, will be translated to group agg
    util.verifyRelPlan("""
                         |SELECT
                         |  a,
                         |  ws,
                         |  we,
                         |  COUNT(*)
                         |FROM win_join
                         |GROUP BY a, ws, we
      """.stripMargin)
  }

  @TestTemplate
  def testSession_OnRowtime(): Unit = {
    // Session window does not support two-phase optimization
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
        |  SESSION(TABLE MyTable PARTITION BY a, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @TestTemplate
  def testSession_OnProctime(): Unit = {
    assumeThat(isTwoPhase).isTrue
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
        |  SESSION(TABLE MyTable PARTITION BY a, DESCRIPTOR(proctime), INTERVAL '5' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @TestTemplate
  def testSession_DistinctSplitEnabled(): Unit = {
    // Session window does not support split-distinct optimization
    util.tableEnv.getConfig.getConfiguration
      .setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)
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
        |  SESSION(TABLE MyTable PARTITION BY a, DESCRIPTOR(proctime), INTERVAL '5' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @TestTemplate
  def testSessionWindowWithTwoPartitionKeys(): Unit = {
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
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  SESSION(TABLE MyTable PARTITION BY (b, a), DESCRIPTOR(rowtime), INTERVAL '5' MINUTE))
        |GROUP BY b, a, window_start, window_end
      """.stripMargin

    util.verifyExplain(sql)
  }

  @TestTemplate
  def testGroupKeyMoreThanPartitionKeyInSessionWindow(): Unit = {
    // the aggregate will not be converted to window aggregate
    // TODO Support session window table function in ExecWindowTableFunction. See
    //  more at FLINK-34100
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
        |  SESSION(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin

    util.verifyExplain(sql)
  }

  @TestTemplate
  def testGroupKeyLessThanPartitionKeyInSessionWindow(): Unit = {
    val sql = {
      """
        |SELECT
        |   window_start,
        |   window_end,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM TABLE(
        |  SESSION(TABLE MyTable PARTITION BY (b, a), DESCRIPTOR(rowtime), INTERVAL '5' MINUTE))
        |GROUP BY b, window_start, window_end
      """.stripMargin
    }

    util.verifyExplain(sql)
  }

  @TestTemplate
  def testDeprecatedSyntaxAboutPartitionKeyInSessionWindow(): Unit = {
    // Session window does not support two-phase optimization
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
        |  SESSION(TABLE MyTable, DESCRIPTOR(proctime), DESCRIPTOR(a), INTERVAL '5' MINUTE))
        |GROUP BY a, window_start, window_end
      """.stripMargin

    assertThatThrownBy(() => util.verifyExplain(sql))
      .hasMessageContaining(
        "Invalid number of arguments to function 'SESSION'. Was expecting 3 arguments")
      .isInstanceOf[ValidationException]
  }

  @TestTemplate
  def testGroupKeysIndicesChangesInSessionWindow(): Unit = {
    val sql =
      """
        |SELECT
        |   window_start,
        |   window_end,
        |   count(*),
        |   a
        |FROM TABLE(
        |  SESSION(TABLE MyTable partition by a, DESCRIPTOR(proctime), INTERVAL '10' MINUTE))
        |GROUP BY window_start, window_end, a
      """.stripMargin

    util.verifyExplain(sql)
  }

  @TestTemplate
  def testSessionWindowTVFWithPartitionKeyWhenCantMerge(): Unit = {
    val sql =
      """
        |SELECT
        |   window_start,
        |   window_end,
        |   a,
        |   count(*),
        |   sum(d),
        |   max(d) filter (where b > 1000),
        |   weightedAvg(b, e) AS wAvg,
        |   count(distinct c) AS uv
        |FROM (
        |  SELECT window_start, rowtime, d, proctime, e, b, c, window_end, window_time, a
        |  FROM TABLE(SESSION(TABLE MyTable PARTITION BY a, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE))
        |  WHERE window_start >= TIMESTAMP '2021-01-01 10:10:00.000'
        |)
        |GROUP BY a, window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @TestTemplate
  def testSessionWindowTVFWithoutPartitionKeyWhenCantMerge(): Unit = {
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
        |  FROM TABLE(SESSION(TABLE MyTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTE))
        |  WHERE window_start >= TIMESTAMP '2021-01-01 10:10:00.000'
        |)
        |GROUP BY window_start, window_end
      """.stripMargin
    util.verifyRelPlan(sql)
  }

  @TestTemplate
  def testProctimeWindowTVFWithCalcOnWindowColumnWhenCantMerge(): Unit = {
    util.verifyRelPlan(
      """
        |select c, count(a)
        |from
        | TABLE(CUMULATE(table MyTable, DESCRIPTOR(proctime), interval '10' seconds, interval '5' minutes))
        |where window_start <> '123'
        |group by window_start, window_end, c, window_time
        |""".stripMargin)
  }

  @TestTemplate
  def testProctimeWindowTVFWithRankWhenCantMerge(): Unit = {
    util.verifyRelPlan(
      """
        |select c, count(a)
        |from (
        | select *, row_number() over (partition by c order by proctime desc) as rn
        | from
        |  TABLE(CUMULATE(table MyTable, DESCRIPTOR(proctime), interval '10' seconds, interval '5' minutes))
        |)
        |where rn = 2
        |group by window_start, window_end, c, window_time
        |""".stripMargin)
  }

  @TestTemplate
  def testProctimeWindowTVFWithDedupWhenCantMerge(): Unit = {
    util.verifyRelPlan(
      """
        |select c, count(a)
        |from (
        | select *, row_number() over (partition by c order by proctime desc) as rn
        | from
        |  TABLE(CUMULATE(table MyTable, DESCRIPTOR(proctime), interval '10' seconds, interval '5' minutes))
        |)
        |where rn = 1
        |group by window_start, window_end, c, window_time
        |""".stripMargin)
  }

  @TestTemplate
  def testProctimeWindowTVFWithOverAggWhenCantMerge(): Unit = {
    util.verifyRelPlan(
      """
        |select c, max(c1), count(a)
        |from (
        | select *, count(*) over (partition by c order by proctime desc) as c1
        | from
        |  TABLE(CUMULATE(table MyTable, DESCRIPTOR(proctime), interval '10' seconds, interval '5' minutes))
        |)
        |group by window_start, window_end, c, window_time
        |""".stripMargin)
  }

  @TestTemplate
  def testProctimeWindowTVFWithJoinWhenCantMerge(): Unit = {
    util.verifyRelPlan(
      """
        |select t.c, max(t2.e), count(t.a)
        |from (
        |  TABLE(CUMULATE(table MyTable, DESCRIPTOR(proctime), interval '10' seconds, interval '5' minutes)) AS t
        |  join MyTable t2 on t2.a = t.a
        |)
        |group by window_start, window_end, t.c, window_time
        |""".stripMargin)
  }

  @TestTemplate
  def testProctimeWindowTVFWithCorrelateWhenCantMerge(): Unit = {
    util.addTemporarySystemFunction("str_split", new StringSplit())
    util.verifyRelPlan(
      """
        |select t.c, max(t2.x), count(t.a)
        |from (
        |  TABLE(CUMULATE(table MyTable, DESCRIPTOR(proctime), interval '10' seconds, interval '5' minutes)) AS t
        |  Left JOIN LATERAL TABLE(str_split('Jack,John', ',')) AS t2(x) ON TRUE
        |)
        |group by window_start, window_end, t.c, window_time
        |""".stripMargin)
  }

  @TestTemplate
  def testProctimeWindowTVFWithUnionWhenCantMerge(): Unit = {
    util.verifyRelPlan(
      """
        |select c, count(a)
        |from (
        |  select * from
        |  TABLE(TUMBLE(table MyTable, DESCRIPTOR(proctime), interval '10' seconds))
        |  union all
        |  select * from
        |  TABLE(TUMBLE(table MyTable, DESCRIPTOR(proctime), interval '5' seconds))
        |) t
        |group by window_start, window_end, c, window_time
        |""".stripMargin)
  }
}

object WindowAggregateTest {
  @Parameters(name = "aggPhaseEnforcer={0}")
  def parameters(): util.Collection[Array[Any]] = {
    util.Arrays.asList(
      Array(AggregatePhaseStrategy.ONE_PHASE),
      Array(AggregatePhaseStrategy.TWO_PHASE)
    )
  }
}
