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
package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.common.BatchShuffleMode
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.connector.source.Boundedness
import org.apache.flink.api.connector.source.mocks.MockSource
import org.apache.flink.api.scala._
import org.apache.flink.configuration.{ExecutionOptions, JobManagerOptions}
import org.apache.flink.configuration.JobManagerOptions.SchedulerType
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}

import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters

@RunWith(classOf[Parameterized])
class MultipleInputCreationTest(shuffleMode: BatchShuffleMode, schedulerType: SchedulerType)
  extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.addTableSource[(Int, Long, String, Int)]("x", 'a, 'b, 'c, 'nx)
    util.addTableSource[(Int, Long, String, Int)]("y", 'd, 'e, 'f, 'ny)
    util.addTableSource[(Int, Long, String, Int)]("z", 'g, 'h, 'i, 'nz)
    util.addDataStream[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.tableConfig.set(ExecutionOptions.BATCH_SHUFFLE_MODE, shuffleMode)
    util.tableConfig.set(JobManagerOptions.SCHEDULER, schedulerType)
  }

  @Test
  def testBasicMultipleInput(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sql =
      """
        |SELECT * FROM
        |  (SELECT a FROM x INNER JOIN y ON x.a = y.d) T1
        |  INNER JOIN
        |  (SELECT d FROM y INNER JOIN t ON y.d = t.a) T2
        |  ON T1.a = T2.d
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testManyMultipleInputs(): Unit = {
    //       y    z         t    y             t
    //       |    |         |    |             |
    // x -> [J -> J] ----> [J -> J] -> [Agg -> J -\
    //                \                            -> U]
    //                 \-> [J -> J] -> [Agg -> J -/
    //                      |    |             |
    //                      y    t             y
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, Boolean.box(false))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin,SortAgg")
    val sql =
      """
        |WITH
        |  T1 AS (
        |    SELECT a, ny, nz FROM x
        |      LEFT JOIN y ON x.a = y.ny
        |      LEFT JOIN z ON x.a = z.nz),
        |  T2 AS (
        |    SELECT T1.a AS a, t.b AS b, d, T1.ny AS ny, nz FROM T1
        |      LEFT JOIN t ON T1.a = t.a
        |      INNER JOIN y ON T1.a = y.d),
        |  T3 AS (
        |    SELECT T1.a AS a, t.b AS b, d, T1.ny AS ny, nz FROM T1
        |      LEFT JOIN y ON T1.a = y.d
        |      INNER JOIN t ON T1.a = t.a),
        |  T4 AS (SELECT b, SUM(d) AS sd, SUM(ny) AS sy, SUM(nz) AS sz FROM T2 GROUP BY b),
        |  T5 AS (SELECT b, SUM(d) AS sd, SUM(ny) AS sy, SUM(nz) AS sz FROM T3 GROUP BY b)
        |SELECT * FROM
        |  (SELECT t.b, sd, sy, sz FROM T4 LEFT JOIN t ON T4.b = t.b)
        |  UNION ALL
        |  (SELECT y.e, sd, sy, sz FROM T5 LEFT JOIN y ON T5.b = y.e)
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinWithAggAsProbe(): Unit = {
    util.tableEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
      "NestedLoopJoin,SortMergeJoin,SortAgg")
    val sql =
      """
        |WITH T AS (SELECT a, d FROM x INNER JOIN y ON x.a = y.d)
        |SELECT * FROM
        |  (SELECT a, COUNT(*) AS cnt FROM T GROUP BY a) T1
        |  LEFT JOIN
        |  (SELECT d, SUM(a) AS sm FROM T GROUP BY d) T2
        |  ON T1.a = T2.d
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testKeepMultipleInputWithOneMemberForChainableSource(): Unit = {
    createChainableTableSource()
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sql = "SELECT * FROM chainable LEFT JOIN x ON chainable.a = x.a"
    util.verifyExecPlan(sql)
  }

  @Test
  def testAvoidIncludingUnionFromInputSide(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sql =
      """
        |SELECT * FROM
        |  (SELECT a FROM (SELECT a FROM x) UNION ALL (SELECT a FROM t)) T1
        |  LEFT JOIN y ON T1.a = y.d
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testIncludeUnionForChainableSource(): Unit = {
    createChainableTableSource()
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sql =
      """
        |SELECT * FROM
        |  (SELECT a FROM (SELECT a FROM chainable) UNION ALL (SELECT a FROM t)) T1
        |  LEFT JOIN y ON T1.a = y.d
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAvoidIncludingCalcAfterNonChainableSource(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sql =
      """
        |SELECT * FROM x
        |  LEFT JOIN y ON x.a = y.d
        |  LEFT JOIN t ON x.a = t.a
        |  WHERE x.b > 10
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testIncludeCalcForChainableSource(): Unit = {
    createChainableTableSource()
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sql =
      """
        |SELECT * FROM chainable
        |  LEFT JOIN y ON chainable.a = y.d
        |  LEFT JOIN t ON chainable.a = t.a
        |  WHERE chainable.a > 10
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAvoidIncludingSingleton(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin,HashAgg")
    val sql =
      """
        |WITH
        |  T1 AS (SELECT COUNT(*) AS cnt FROM z),
        |  T2 AS (
        |    SELECT a FROM
        |      (SELECT a FROM x INNER JOIN y ON x.a = y.d)
        |      UNION ALL
        |      (SELECT a FROM t FULL JOIN T1 ON t.a > T1.cnt))
        |SELECT a FROM T2 LEFT JOIN z ON T2.a = z.g
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testNoPriorityConstraint(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")
    val sql =
      """
        |SELECT * FROM x
        |  INNER JOIN y ON x.a = y.d
        |  INNER JOIN t ON x.a = t.a
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testRelatedInputs(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sql =
      """
        |WITH
        |  T1 AS (SELECT x.a AS a, y.d AS b FROM y LEFT JOIN x ON y.d = x.a),
        |  T2 AS (
        |    SELECT a, b FROM
        |      (SELECT a, b FROM T1)
        |      UNION ALL
        |      (SELECT x.a AS a, x.b AS b FROM x))
        |SELECT * FROM T2 LEFT JOIN t ON T2.a = t.a
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testRelatedInputsWithAgg(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin,SortAgg")
    val sql =
      """
        |WITH
        |  T1 AS (SELECT x.a AS a, y.d AS b FROM y LEFT JOIN x ON y.d = x.a),
        |  T2 AS (
        |    SELECT a, b FROM
        |      (SELECT a, b FROM T1)
        |      UNION ALL
        |      (SELECT COUNT(x.a) AS a, x.b AS b FROM x GROUP BY x.b))
        |SELECT * FROM T2 LEFT JOIN t ON T2.a = t.a
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testRemoveRedundantUnion(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, Boolean.box(false))
    util.tableEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
      "NestedLoopJoin,SortMergeJoin,SortAgg")
    val sql =
      """
        |WITH
        |  T1 AS (SELECT COUNT(*) AS cnt FROM x GROUP BY a),
        |  T2 AS (SELECT COUNT(*) AS cnt FROM y GROUP BY d),
        |  T3 AS (SELECT a AS cnt FROM x INNER JOIN y ON x.a = y.d),
        |  T4 AS (SELECT b AS cnt FROM x INNER JOIN y ON x.b = y.e)
        |SELECT cnt FROM
        |  (SELECT cnt FROM (SELECT cnt FROM T1) UNION ALL (SELECT cnt FROM T2))
        |  UNION ALL
        |  (SELECT cnt FROM (SELECT cnt FROM T3) UNION ALL (SELECT cnt FROM T4))
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testRemoveOneInputOperatorFromRoot(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, Boolean.box(false))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sql =
      """
        |WITH
        |  T1 AS (SELECT a FROM x INNER JOIN y ON x.a = y.d),
        |  T2 AS (SELECT b FROM x INNER JOIN y ON x.b = y.e)
        |SELECT * FROM
        |  (SELECT a, b FROM T1 LEFT JOIN T2 ON T1.a = T2.b)
        |  UNION ALL
        |  (SELECT a, b FROM x)
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testCleanUpMultipleInputWithOneMember(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sql =
      """
        |WITH
        |  T1 AS (SELECT a FROM x INNER JOIN y ON x.a = y.d)
        |SELECT * FROM
        |  (SELECT a, a + 1 FROM T1)
        |  UNION ALL
        |  (SELECT a, b FROM x)
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testKeepUsefulUnion(): Unit = {
    createChainableTableSource()
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sql =
      """
        |WITH
        |  T1 AS (SELECT chainable.a AS a FROM chainable LEFT JOIN x ON chainable.a = x.a),
        |  T2 AS (SELECT chainable.a AS a FROM chainable LEFT JOIN y ON chainable.a = y.d)
        |SELECT * FROM
        |  (SELECT a FROM T1)
        |  UNION ALL
        |  (SELECT a FROM T2)
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testDeadlockCausedByExchangeInAncestor(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sql =
      """
        |WITH T1 AS (
        |  SELECT x1.*, x2.a AS k, (x1.b + x2.b) AS v
        |  FROM x x1 LEFT JOIN x x2 ON x1.a = x2.a WHERE x2.a > 0)
        |SELECT x.a, x.b, T1.* FROM x LEFT JOIN T1 ON x.a = T1.k WHERE x.a > 0 AND T1.v = 0
        |""".stripMargin
    util.verifyExecPlan(sql)
  }

  def createChainableTableSource(): Unit = {
    val dataStream = util.getStreamEnv.fromSource(
      new MockSource(Boundedness.BOUNDED, 1),
      WatermarkStrategy.noWatermarks[Integer],
      "chainable")
    TableTestUtil
      .createTemporaryView[Integer](util.tableEnv, "chainable", dataStream, Some(Array('a)))
  }
}

object MultipleInputCreationTest {

  @Parameters(name = "shuffleMode: {0}, schedulerType: {1}")
  def parameters(): Array[Array[java.lang.Object]] = {
    Array(
      Array(BatchShuffleMode.ALL_EXCHANGES_BLOCKING, SchedulerType.AdaptiveBatch),
      Array(BatchShuffleMode.ALL_EXCHANGES_BLOCKING, SchedulerType.Default),
      Array(BatchShuffleMode.ALL_EXCHANGES_PIPELINED, SchedulerType.Default)
    )
  }
}
