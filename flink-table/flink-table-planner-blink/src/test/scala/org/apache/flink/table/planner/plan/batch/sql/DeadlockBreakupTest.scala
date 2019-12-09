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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

class DeadlockBreakupTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.addTableSource[(Int, Long, String)]("x", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("y", 'd, 'e, 'f)
    util.addDataStream[(Int, Long, String)]("t", 'a, 'b, 'c)
  }

  @Test
  def testSubplanReuse_SetExchangeAsBatch(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, true)
    val sqlQuery =
      """
        |WITH t AS (SELECT x.a AS a, x.b AS b, y.d AS d, y.e AS e FROM x, y WHERE x.a = y.d)
        |SELECT t1.*, t2.* FROM t t1, t t2 WHERE t1.b = t2.e AND t1.a < 10 AND t2.a > 5
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_AddExchangeAsBatch_HashJoin(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a FROM x LIMIT 10)
        |SELECT r1.a FROM r r1, r r2 WHERE r1.a = r2.a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_AddExchangeAsBatch_NestedLoopJoin(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a FROM x LIMIT 10)
        |SELECT r1.a FROM r r1, r r2 WHERE r1.a = r2.a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_SetExchangeAsBatch_SortMergeJoin(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin")
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery =
      """
        |
        |WITH v1 AS (SELECT a, SUM(b) AS b, MAX(c) AS c FROM x GROUP BY a),
        |     v2 AS (SELECT * FROM v1 r1, v1 r2 WHERE r1.a = r2.a AND r1.b > 10),
        |     v3 AS (SELECT * FROM v1 r1, v1 r2 WHERE r1.a = r2.a AND r1.b < 5)
        |
        |select * from v2, v3 where v2.c = v3.c
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_AddExchangeAsBatch_BuildLeftSemiHashJoin(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b FROM x LIMIT 10)
        |SELECT r1.a, r2.b FROM x r1,
        |(SELECT a, b FROM r WHERE a in (select a from x where b > 5)) r2
        |WHERE r1.a = r2.a and r1.b = 5
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSubplanReuse_SetExchangeAsBatch_OverAgg(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
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
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testReusedNodeIsBarrierNode(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false)
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM x GROUP BY c)
        |    SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r2.a > 1
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDataStreamReuse_SetExchangeAsBatch(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, false)
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery = "SELECT * FROM t t1, t t2 WHERE t1.a = t2.a AND t1.b > 10 AND t2.c LIKE 'Test%'"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDataStreamReuse_AddExchangeAsBatch_NestedLoopJoin(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, false)
    val sqlQuery = "SELECT * FROM t t1, t t2 WHERE t1.a = t2.b"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDataStreamReuse_AddExchangeAsBatch_HashJoin(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, false)
    val sqlQuery = "SELECT * FROM t INTERSECT SELECT * FROM t"
    util.verifyPlan(sqlQuery)
  }

}
