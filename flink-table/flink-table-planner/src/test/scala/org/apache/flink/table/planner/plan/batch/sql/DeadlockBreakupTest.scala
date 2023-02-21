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
import org.apache.flink.api.scala._
import org.apache.flink.configuration.ExecutionOptions
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

class DeadlockBreakupTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig
      .set(ExecutionOptions.BATCH_SHUFFLE_MODE, BatchShuffleMode.ALL_EXCHANGES_PIPELINED)
    util.addTableSource[(Int, Long, String)]("x", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("y", 'd, 'e, 'f)
    util.addDataStream[(Int, Long, String)]("t", 'a, 'b, 'c)
  }

  @Test
  def testSubplanReuse_SetExchangeAsBatch(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, Boolean.box(true))
    val sqlQuery =
      """
        |WITH t AS (SELECT x.a AS a, x.b AS b, y.d AS d, y.e AS e FROM x, y WHERE x.a = y.d)
        |SELECT t1.*, t2.* FROM t t1, t t2 WHERE t1.b = t2.e AND t1.a < 10 AND t2.a > 5
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_AddExchangeAsBatch_HashJoin(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a FROM x LIMIT 10)
        |SELECT r1.a FROM r r1, r r2 WHERE r1.a = r2.a
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_AddExchangeAsBatch_NestedLoopJoin(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, Boolean.box(false))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a FROM x LIMIT 10)
        |SELECT r1.a FROM r r1, r r2 WHERE r1.a = r2.a
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_SetExchangeAsBatch_SortMergeJoin(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,HashAgg")
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, Long.box(-1))
    val sqlQuery =
      """
        |
        |WITH v1 AS (SELECT a, SUM(b) AS b, MAX(c) AS c FROM x GROUP BY a),
        |     v2 AS (SELECT * FROM v1 r1, v1 r2 WHERE r1.a = r2.a AND r1.b > 10),
        |     v3 AS (SELECT * FROM v1 r1, v1 r2 WHERE r1.a = r2.a AND r1.b < 5)
        |
        |select * from v2, v3 where v2.c = v3.c
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_AddExchangeAsBatch_BuildLeftSemiHashJoin(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, Long.box(-1))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b FROM x LIMIT 10)
        |SELECT r1.a, r2.b FROM x r1,
        |(SELECT a, b FROM r WHERE a in (select a from x where b > 5)) r2
        |WHERE r1.a = r2.a and r1.b = 5
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_SetExchangeAsBatch_OverAgg(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(true))
    val sqlQuery =
      """
        |WITH r1 AS (SELECT SUM(a) OVER (PARTITION BY b ORDER BY b) AS a, b, c FROM x),
        | r2 AS (SELECT MAX(a) OVER (PARTITION BY b ORDER BY b
        | RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS a, b, c FROM r1),
        | r3 AS (SELECT MIN(a) OVER (PARTITION BY b ORDER BY b
        | RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS a, b, c FROM r1)
        |
        |SELECT * FROM r2, r3 WHERE r2.c = r3.c
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testReusedNodeIsBarrierNode(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, Boolean.box(false))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin,SortAgg")
    val sqlQuery =
      """
        |WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM x GROUP BY c)
        |    SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r2.a > 1
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testDataStreamReuse_SetExchangeAsBatch(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(false))
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, Long.box(-1))
    val sqlQuery = "SELECT * FROM t t1, t t2 WHERE t1.a = t2.a AND t1.b > 10 AND t2.c LIKE 'Test%'"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testDataStreamReuse_AddExchangeAsBatch_NestedLoopJoin(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(false))
    val sqlQuery = "SELECT * FROM t t1, t t2 WHERE t1.a = t2.b"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testDataStreamReuse_AddExchangeAsBatch_HashJoin(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(false))
    val sqlQuery = "SELECT * FROM t INTERSECT SELECT * FROM t"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_BuildAndProbeNoCommonSuccessors_HashJoin(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig.set(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
      "NestedLoopJoin,SortMergeJoin,SortAgg")
    val sqlQuery =
      s"""
         |WITH
         |  T1 AS (SELECT a, COUNT(*) AS cnt1 FROM x GROUP BY a),
         |  T2 AS (SELECT d, COUNT(*) AS cnt2 FROM y GROUP BY d)
         |SELECT * FROM
         |  (SELECT cnt1, cnt2 FROM T1 LEFT JOIN T2 ON a = d)
         |  UNION ALL
         |  (SELECT cnt1, cnt2 FROM T2 LEFT JOIN T1 ON d = a)
         |""".stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_AddSingletonExchange(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin,HashAgg")
    val sqlQuery =
      s"""
         |WITH
         |  T1 AS (SELECT COUNT(*) AS cnt FROM x),
         |  T2 AS (SELECT cnt FROM T1 WHERE cnt > 3),
         |  T3 AS (SELECT cnt FROM T1 WHERE cnt < 5)
         |SELECT * FROM T2 FULL JOIN T3 ON T2.cnt <> T3.cnt
         |""".stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_DeadlockCausedByReusingExchange(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      s"""
         |WITH T1 AS (SELECT a FROM x)
         |SELECT * FROM T1
         |  INNER JOIN T1 AS T2 ON T1.a = T2.a
         |""".stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_DeadlockCausedByReusingExchangeInAncestor(): Unit = {
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, Boolean.box(true))
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_MULTIPLE_INPUT_ENABLED, Boolean.box(false))
    util.tableEnv.getConfig
      .set(ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH T1 AS (
        |  SELECT x1.*, x2.a AS k, (x1.b + x2.b) AS v
        |  FROM x x1 LEFT JOIN x x2 ON x1.a = x2.a WHERE x2.a > 0)
        |SELECT x.a, x.b, T1.* FROM x LEFT JOIN T1 ON x.a = T1.k WHERE x.a > 0 AND T1.v = 0
        |""".stripMargin
    util.verifyExecPlan(sqlQuery)
  }
}
