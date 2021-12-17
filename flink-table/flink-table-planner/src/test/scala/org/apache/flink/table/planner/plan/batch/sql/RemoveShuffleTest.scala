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

import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala._
import org.apache.flink.table.api.Types
import org.apache.flink.table.api.config.{ExecutionConfigOptions, OptimizerConfigOptions}
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.planner.plan.rules.physical.batch.{BatchPhysicalJoinRuleBase, BatchPhysicalSortMergeJoinRule}
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.utils.{TableFunc1, TableTestBase}

import org.junit.{Before, Test}

class RemoveShuffleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.addTableSource("x",
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING),
      Array("a", "b", "c"),
      FlinkStatistic.builder().tableStats(new TableStats(100L)).build()
    )
    util.addTableSource("y",
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING),
      Array("d", "e", "f"),
      FlinkStatistic.builder().tableStats(new TableStats(100L)).build()
    )
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, false)
  }

  @Test
  def testRemoveHashShuffle_OverWindowAgg(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin,SortAgg")
    val sqlQuery =
      """
        | SELECT
        |   SUM(b) sum_b,
        |   AVG(SUM(b)) OVER (PARTITION BY c) avg_b,
        |   RANK() OVER (PARTITION BY c ORDER BY c) rn,
        |   c
        | FROM x
        | GROUP BY c
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_MultiOverWindowAgg(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin,SortAgg")
    val sqlQuery =
      """
        | SELECT
        |   SUM(b) sum_b,
        |   AVG(SUM(b)) OVER (PARTITION BY a, c) avg_b,
        |   RANK() OVER (PARTITION BY c ORDER BY a, c) rn,
        |   c
        | FROM x
        | GROUP BY a, c
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_OverWindowAgg_PartialKey(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin,SortAgg")
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      BatchPhysicalJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED, true)
    // push down HashExchange[c] into HashAgg
    val sqlQuery =
      """
        | SELECT
        |   SUM(b) sum_b,
        |   AVG(SUM(b)) OVER (PARTITION BY c) avg_b,
        |   RANK() OVER (PARTITION BY c ORDER BY c) rn,
        |   c
        | FROM x
        | GROUP BY a, c
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_Agg_PartialKey(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin,SortAgg")
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      BatchPhysicalJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED, true)
    // push down HashExchange[c] into HashAgg
    val sqlQuery =
      """
        | WITH r AS (SELECT a, c, count(b) as cnt FROM x GROUP BY a, c)
        | SELECT count(cnt) FROM r group by c
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_HashAggregate(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortMergeJoin,NestedLoopJoin,SortAgg")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by a
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_HashAggregate_1(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortMergeJoin,NestedLoopJoin,SortAgg")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by a, d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_HashAggregate_2(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortMergeJoin,NestedLoopJoin,SortAgg")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_SortAggregate(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortMergeJoin,NestedLoopJoin,HashAgg")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by a
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_SortAggregate_1(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortMergeJoin,NestedLoopJoin,HashAgg")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by a, d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_SortAggregate_2(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortMergeJoin,NestedLoopJoin,HashAgg")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_SortMergeJoin(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      BatchPhysicalSortMergeJoinRule.TABLE_OPTIMIZER_SMJ_REMOVE_SORT_ENABLED, true)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_SortMergeJoin_LOJ(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      BatchPhysicalSortMergeJoinRule.TABLE_OPTIMIZER_SMJ_REMOVE_SORT_ENABLED, true)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x left join (SELECT * FROM y WHERE e = 2) r on a = d)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_SortMergeJoin_ROJ(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      BatchPhysicalSortMergeJoinRule.TABLE_OPTIMIZER_SMJ_REMOVE_SORT_ENABLED, true)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x right join (SELECT * FROM y WHERE e = 2) r on a = d)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_SortMergeJoin_FOJ(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x full join (SELECT * FROM y WHERE e = 2) r on a = d)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_HashJoin(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_BroadcastHashJoin(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_HashJoin_LOJ(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x left join (SELECT * FROM y WHERE e = 2) r on a = d)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_HashJoin_ROJ(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x right join (SELECT * FROM y WHERE e = 2) r on a = d)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_HashJoin_FOJ(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x full join (SELECT * FROM y WHERE e = 2) r on a = d)
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_HashJoin_1(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r1 AS (SELECT a, c, sum(b) FROM x group by a, c),
        |r2 AS (SELECT a, c, sum(b) FROM x group by a, c)
        |SELECT * FROM r1, r2 WHERE r1.a = r2.a and r1.c = r2.c
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_NestedLoopJoin(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT * FROM r r1, r r2 WHERE r1.a = r2.d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_Join_PartialKey(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortMergeJoin,NestedLoopJoin,SortAgg")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      BatchPhysicalJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED, true)
    val sqlQuery =
      """
        |WITH r AS (SELECT d, count(f) as cnt FROM y GROUP BY d)
        |SELECT * FROM x, r WHERE x.a = r.d AND x.b = r.cnt
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveSingleExchange_Agg(): Unit = {
    val sqlQuery = "SELECT avg(b) FROM x GROUP BY c  HAVING sum(b) > (SELECT sum(b) * 0.1 FROM x)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_Union(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin,SortAgg")
    val sqlQuery =
      """
        |WITH r AS (
        |SELECT count(a) as cnt, c FROM x WHERE b > 10 group by c
        |UNION ALL
        |SELECT count(d) as cnt, f FROM y WHERE e < 100 group by f)
        |SELECT r1.c, r1.cnt, r2.c, r2.cnt FROM r r1, r r2 WHERE r1.c = r2.c and r1.cnt < 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_Rank(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER(PARTITION BY a ORDER BY b) rk FROM (
        |   SELECT a, SUM(b) AS b FROM x GROUP BY a
        | )
        |) WHERE rk <= 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_Rank_PartialKey1(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      BatchPhysicalJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED, true)
    val sqlQuery =
      """
        |SELECT a, SUM(b) FROM (
        | SELECT * FROM (
        |   SELECT a, b, c, RANK() OVER(PARTITION BY a, c ORDER BY b) rk FROM x)
        | WHERE rk <= 10
        |) GROUP BY a
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_Rank_PartialKey2(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      BatchPhysicalJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED, false)
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, c, RANK() OVER(PARTITION BY a, c ORDER BY b) rk FROM (
        |   SELECT a, SUM(b) AS b, COUNT(c) AS c FROM x GROUP BY a
        | )
        |) WHERE rk <= 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_Rank_PartialKey3(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")
    util.tableEnv.getConfig.getConfiguration.setBoolean(
      BatchPhysicalJoinRuleBase.TABLE_OPTIMIZER_SHUFFLE_BY_PARTIAL_KEY_ENABLED, true)
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, c, RANK() OVER(PARTITION BY a, c ORDER BY b) rk FROM (
        |   SELECT a, SUM(b) AS b, COUNT(c) AS c FROM x GROUP BY a
        | )
        |) WHERE rk <= 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_Rank_Singleton1(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER(ORDER BY b) rk FROM (
        |   SELECT COUNT(a) AS a, SUM(b) AS b FROM x
        | )
        |) WHERE rk <= 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_Rank_Singleton2(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortAgg")
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, RANK() OVER(PARTITION BY a ORDER BY b) rk FROM (
        |   SELECT COUNT(a) AS a, SUM(b) AS b FROM x
        | )
        |) WHERE rk <= 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_Correlate1(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortMergeJoin,NestedLoopJoin,SortAgg")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    util.addFunction("split", new TableFunc1)
    val sqlQuery =
      """
        |WITH r AS (SELECT f, count(f) as cnt FROM y GROUP BY f),
        |     v as (SELECT f1, f, cnt FROM r, LATERAL TABLE(split(f)) AS T(f1))
        |SELECT * FROM x, v WHERE c = f
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_Correlate2(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortMergeJoin,NestedLoopJoin,SortAgg")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    util.addFunction("split", new TableFunc1)
    val sqlQuery =
      """
        |WITH r AS (SELECT f, count(f) as cnt FROM y GROUP BY f),
        |     v as (SELECT f, f1 FROM r, LATERAL TABLE(split(f)) AS T(f1))
        |SELECT * FROM x, v WHERE c = f AND f LIKE '%llo%'
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveHashShuffle_Correlate3(): Unit = {
    // do not remove shuffle
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "SortMergeJoin,NestedLoopJoin,SortAgg")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConfiguration.setLong(
      OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, -1)
    util.addFunction("split", new TableFunc1)
    val sqlQuery =
      """
        |WITH r AS (SELECT f, count(f) as cnt FROM y GROUP BY f),
        |     v as (SELECT f1 FROM r, LATERAL TABLE(split(f)) AS T(f1))
        |SELECT * FROM x, v WHERE c = f1
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }
}
