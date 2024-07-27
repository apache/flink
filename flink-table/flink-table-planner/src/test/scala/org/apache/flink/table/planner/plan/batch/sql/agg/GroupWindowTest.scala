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
package org.apache.flink.table.planner.plan.batch.sql.agg

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{AggregatePhaseStrategy, OptimizerConfigOptions}
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.WeightedAvgWithMerge
import org.apache.flink.table.planner.utils.{CountAggFunction, TableTestBase}
import org.apache.flink.testutils.junit.extensions.parameterized.{ParameterizedTestExtension, Parameters}

import org.assertj.core.api.Assertions.{assertThatExceptionOfType, assertThatThrownBy}
import org.junit.jupiter.api.{BeforeEach, TestTemplate}
import org.junit.jupiter.api.extension.ExtendWith

import java.sql.Timestamp
import java.util

import scala.collection.JavaConversions._

@ExtendWith(Array(classOf[ParameterizedTestExtension]))
class GroupWindowTest(aggStrategy: AggregatePhaseStrategy) extends TableTestBase {

  private val util = batchTestUtil()

  @BeforeEach
  def before(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, aggStrategy)
    util.addTemporarySystemFunction("countFun", new CountAggFunction)
    util.addTableSource[(Int, Timestamp, Int, Long)]("MyTable", 'a, 'b, 'c, 'd)
    util.addTableSource[(Timestamp, Long, Int, String)]("MyTable1", 'ts, 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String, Int, Timestamp)]("MyTable2", 'a, 'b, 'c, 'd, 'ts)
    util.tableEnv.executeSql(s"""
                                |create table MyTable3 (
                                |  a int,
                                |  b bigint,
                                |  c as proctime()
                                |) with (
                                |  'connector' = 'COLLECTION'
                                |)
                                |""".stripMargin)
  }

  @TestTemplate
  def testHopWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB FROM MyTable2 " +
        "GROUP BY HOP(ts, INTERVAL '1' HOUR, INTERVAL '2' HOUR, TIME '10:00:00')"
    assertThatThrownBy(() => util.verifyExecPlan(sqlQuery))
      .hasMessageContaining("HOP window with alignment is not supported yet.")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testSessionWindowNoOffset(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB FROM MyTable2 " +
        "GROUP BY SESSION(ts, INTERVAL '2' HOUR, TIME '10:00:00')"
    assertThatThrownBy(() => util.verifyExecPlan(sqlQuery))
      .hasMessageContaining("SESSION window with alignment is not supported yet.")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testVariableWindowSize(): Unit = {
    assertThatThrownBy(
      () =>
        util.verifyExecPlan(
          "SELECT COUNT(*) FROM MyTable2 GROUP BY TUMBLE(ts, b * INTERVAL '1' MINUTE)"))
      .hasMessageContaining("Only constant window descriptors are supported")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testTumbleWindowWithInvalidUdAggArgs(): Unit = {
    val weightedAvg = new WeightedAvgWithMerge
    util.addTemporarySystemFunction("weightedAvg", weightedAvg)

    val sql = "SELECT weightedAvg(c, a) AS wAvg FROM MyTable2 " +
      "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE)"

    assertThatThrownBy(() => util.verifyExecPlan(sql))
      .hasMessageContaining(
        "SQL validation failed. Invalid function call:\nweightedAvg(STRING, INT)")
      .isInstanceOf[ValidationException]
  }

  @TestTemplate
  def testWindowProctime(): Unit = {
    val sqlQuery =
      "SELECT TUMBLE_PROCTIME(ts, INTERVAL '4' MINUTE) FROM MyTable2 " +
        "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE), c"
    assertThatThrownBy(() => util.verifyExecPlan(sqlQuery))
      .hasMessageContaining("PROCTIME window property is not supported in batch queries.")
      .isInstanceOf[ValidationException]
  }

  @TestTemplate
  def testWindowAggWithGroupSets(): Unit = {
    // TODO supports group sets
    // currently, the optimized plan is not collect, and an exception will be thrown in code-gen
    val sql =
      """
        |SELECT COUNT(*),
        |    TUMBLE_END(ts, INTERVAL '15' MINUTE) + INTERVAL '1' MINUTE
        |FROM MyTable1
        |    GROUP BY rollup(TUMBLE(ts, INTERVAL '15' MINUTE), b)
    """.stripMargin

    assertThatExceptionOfType(classOf[AssertionError])
      .isThrownBy(() => util.verifyRelPlanNotExpected(sql, "TUMBLE(ts"))
  }

  @TestTemplate
  def testNoGroupingTumblingWindow(): Unit = {
    val sqlQuery = "SELECT AVG(c), SUM(a) FROM MyTable GROUP BY TUMBLE(b, INTERVAL '3' SECOND)"
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testTumblingWindowSortAgg1(): Unit = {
    val sqlQuery = "SELECT MAX(c) FROM MyTable1 GROUP BY a, TUMBLE(ts, INTERVAL '3' SECOND)"
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testTumblingWindowSortAgg2(): Unit = {
    val sqlQuery = "SELECT AVG(c), countFun(a) FROM MyTable " +
      "GROUP BY a, d, TUMBLE(b, INTERVAL '3' SECOND)"
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testTumblingWindowHashAgg1(): Unit = {
    val sqlQuery = "SELECT COUNT(c) FROM MyTable1 GROUP BY a, TUMBLE(ts, INTERVAL '3' SECOND)"
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testTumblingWindowHashAgg2(): Unit = {
    val sql = "SELECT AVG(c), COUNT(a) FROM MyTable GROUP BY a, d, TUMBLE(b, INTERVAL '3' SECOND)"
    util.verifyExecPlan(sql)
  }

  @TestTemplate
  def testNonPartitionedTumblingWindow(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB FROM MyTable2 GROUP BY TUMBLE(ts, INTERVAL '2' HOUR)"
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testPartitionedTumblingWindow(): Unit = {
    val sqlQuery =
      """
        |SELECT TUMBLE_START(ts, INTERVAL '4' MINUTE),
        |    TUMBLE_END(ts, INTERVAL '4' MINUTE),
        |    TUMBLE_ROWTIME(ts, INTERVAL '4' MINUTE),
        |    c,
        |    SUM(a) AS sumA,
        |    MIN(b) AS minB
        |FROM MyTable2
        |    GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE), c
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testTumblingWindowWithUdAgg(): Unit = {
    util.addTemporarySystemFunction("weightedAvg", new WeightedAvgWithMerge)
    val sql = "SELECT weightedAvg(b, a) AS wAvg FROM MyTable2 " +
      "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE)"
    util.verifyExecPlan(sql)
  }

  @TestTemplate
  def testTumblingWindowWithProctime(): Unit = {
    val sql = "select sum(a), max(b) from MyTable3 group by TUMBLE(c, INTERVAL '1' SECOND)"
    assertThatThrownBy(() => util.verifyExecPlan(sql))
      .hasMessageContaining(
        "Window can not be defined over a proctime attribute column for batch mode")
      .isInstanceOf[ValidationException]
  }

  @TestTemplate
  def testNoGroupingSlidingWindow(): Unit = {
    val sqlQuery =
      """
        |SELECT SUM(a),
        |    HOP_START(b, INTERVAL '3' SECOND, INTERVAL '3' SECOND),
        |    HOP_END(b, INTERVAL '3' SECOND, INTERVAL '3' SECOND)
        |FROM MyTable
        |    GROUP BY HOP(b, INTERVAL '3' SECOND, INTERVAL '3' SECOND)
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testSlidingWindowSortAgg1(): Unit = {
    val sqlQuery = "SELECT MAX(c) FROM MyTable1 " +
      "GROUP BY a, HOP(ts, INTERVAL '3' SECOND, INTERVAL '1' HOUR)"
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testSlidingWindowSortAgg2(): Unit = {
    val sqlQuery = "SELECT MAX(c) FROM MyTable1 " +
      "GROUP BY b, HOP(ts, INTERVAL '0.111' SECOND(1,3), INTERVAL '1' SECOND)"
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testSlidingWindowSortAgg3(): Unit = {
    val sqlQuery = "SELECT countFun(c) FROM MyTable " +
      " GROUP BY a, d, HOP(b, INTERVAL '3' SECOND, INTERVAL '1' HOUR)"
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testSlidingWindowSortAggWithPaneOptimization(): Unit = {
    val sqlQuery = "SELECT COUNT(c) FROM MyTable1 " +
      "GROUP BY a, HOP(ts, INTERVAL '3' SECOND, INTERVAL '1' HOUR)"
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testSlidingWindowHashAgg(): Unit = {
    val sqlQuery = "SELECT count(c) FROM MyTable1 " +
      "GROUP BY b, HOP(ts, INTERVAL '3' SECOND, INTERVAL '1' HOUR)"
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testNonPartitionedSlidingWindow(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS sumA, COUNT(b) AS cntB " +
        "FROM MyTable2 " +
        "GROUP BY HOP(ts, INTERVAL '15' MINUTE, INTERVAL '90' MINUTE)"

    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testPartitionedSlidingWindow(): Unit = {
    val sqlQuery =
      "SELECT " +
        "  c, " +
        "  HOP_END(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), " +
        "  HOP_START(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), " +
        "  HOP_ROWTIME(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), " +
        "  SUM(a) AS sumA, " +
        "  AVG(b) AS avgB " +
        "FROM MyTable2 " +
        "GROUP BY HOP(ts, INTERVAL '1' HOUR, INTERVAL '3' HOUR), d, c"

    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testSlidingWindowWithProctime(): Unit = {
    val sql =
      s"""
         |select sum(a), max(b)
         |from MyTable3
         |group by HOP(c, INTERVAL '1' SECOND, INTERVAL '1' MINUTE)
         |""".stripMargin
    assertThatThrownBy(() => util.verifyExecPlan(sql))
      .hasMessageContaining("Window can not be defined over a proctime attribute column for " +
        "batch mode")
      .isInstanceOf[ValidationException]
  }

  @TestTemplate
  // TODO session window is not supported now
  def testNonPartitionedSessionWindow(): Unit = {
    val sqlQuery = "SELECT COUNT(*) AS cnt FROM MyTable2 GROUP BY SESSION(ts, INTERVAL '30' MINUTE)"
    assertThatThrownBy(() => util.verifyExecPlan(sqlQuery))
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  // TODO session window is not supported now
  def testPartitionedSessionWindow(): Unit = {
    val sqlQuery =
      """
        |SELECT c, d,
        |    SESSION_START(ts, INTERVAL '12' HOUR),
        |    SESSION_END(ts, INTERVAL '12' HOUR),
        |    SESSION_ROWTIME(ts, INTERVAL '12' HOUR),
        |    SUM(a) AS sumA,
        |    MIN(b) AS minB
        |FROM MyTable2
        |    GROUP BY SESSION(ts, INTERVAL '12' HOUR), c, d
      """.stripMargin
    assertThatThrownBy(() => util.verifyExecPlan(sqlQuery))
      .hasMessageContaining("Cannot generate a valid execution plan for the given query")
      .isInstanceOf[TableException]
  }

  @TestTemplate
  def testSessionWindowWithProctime(): Unit = {
    val sql =
      s"""
         |select sum(a), max(b)
         |from MyTable3
         |group by SESSION(c, INTERVAL '1' MINUTE)
         |""".stripMargin
    assertThatThrownBy(() => util.verifyExecPlan(sql))
      .hasMessageContaining(
        "Window can not be defined over a proctime attribute column for batch mode")
      .isInstanceOf[ValidationException]
  }

  @TestTemplate
  def testWindowEndOnly(): Unit = {
    val sqlQuery =
      "SELECT TUMBLE_END(ts, INTERVAL '4' MINUTE) FROM MyTable2 " +
        "GROUP BY TUMBLE(ts, INTERVAL '4' MINUTE), c"
    util.verifyExecPlan(sqlQuery)
  }

  @TestTemplate
  def testExpressionOnWindowHavingFunction(): Unit = {
    val sql =
      """
        |SELECT COUNT(*),
        |    HOP_START(ts, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE)
        |FROM MyTable2
        |    GROUP BY HOP(ts, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE)
        |    HAVING
        |     SUM(a) > 0 AND
        |     QUARTER(HOP_START(ts, INTERVAL '15' MINUTE, INTERVAL '1' MINUTE)) = 1
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  @TestTemplate
  def testDecomposableAggFunctions(): Unit = {
    val sql =
      """
        |SELECT VAR_POP(b),
        |    VAR_SAMP(b),
        |    STDDEV_POP(b),
        |    STDDEV_SAMP(b),
        |    TUMBLE_START(ts, INTERVAL '15' MINUTE),
        |    TUMBLE_END(ts, INTERVAL '15' MINUTE)
        |FROM MyTable1
        |    GROUP BY TUMBLE(ts, INTERVAL '15' MINUTE)
      """.stripMargin
    util.verifyExecPlan(sql)
  }

  // TODO: fix the plan regression when FLINK-19668 is fixed.
  @TestTemplate
  def testReturnTypeInferenceForWindowAgg() = {

    val sql =
      """
        |SELECT
        |  SUM(correct) AS s,
        |  AVG(correct) AS a,
        |  TUMBLE_START(b, INTERVAL '15' MINUTE) AS wStart
        |FROM (
        |  SELECT CASE a
        |      WHEN 1 THEN 1
        |      ELSE 99
        |    END AS correct, b
        |  FROM MyTable
        |)
        |GROUP BY TUMBLE(b, INTERVAL '15' MINUTE)
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @TestTemplate
  def testWindowAggregateWithDifferentWindows(): Unit = {
    // This test ensures that the LogicalWindowAggregate node' digest contains the window specs.
    // This allows the planner to make the distinction between similar aggregations using different
    // windows (see FLINK-15577).
    val sql =
      """
        |WITH window_1h AS (
        |    SELECT 1
        |    FROM MyTable2
        |    GROUP BY HOP(`ts`, INTERVAL '1' HOUR, INTERVAL '1' HOUR)
        |),
        |
        |window_2h AS (
        |    SELECT 1
        |    FROM MyTable2
        |    GROUP BY HOP(`ts`, INTERVAL '1' HOUR, INTERVAL '2' HOUR)
        |)
        |
        |(SELECT * FROM window_1h)
        |UNION ALL
        |(SELECT * FROM window_2h)
        |""".stripMargin

    util.verifyExecPlan(sql)
  }
}

object GroupWindowTest {

  @Parameters(name = "aggStrategy={0}")
  def parameters(): util.Collection[AggregatePhaseStrategy] = {
    Seq[AggregatePhaseStrategy](
      AggregatePhaseStrategy.AUTO,
      AggregatePhaseStrategy.ONE_PHASE,
      AggregatePhaseStrategy.TWO_PHASE
    )
  }
}
