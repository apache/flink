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
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalSortMergeJoinRule
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalSortRule.TABLE_EXEC_RANGE_SORT_ENABLED
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.StringSplit
import org.apache.flink.table.planner.utils.{TableFunc1, TableTestBase}

import com.google.common.collect.ImmutableSet
import org.junit.{Before, Test}

class RemoveCollationTest extends TableTestBase {

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
    util.addTableSource("t1",
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING),
      Array("a1", "b1", "c1"),
      FlinkStatistic.builder().tableStats(new TableStats(100L)).build()
    )
    util.addTableSource("t2",
      Array[TypeInformation[_]](Types.INT, Types.LONG, Types.STRING),
      Array("d1", "e1", "f1"),
      FlinkStatistic.builder().tableStats(new TableStats(100L)).build()
    )

    util.tableEnv.getConfig.getConfiguration.setBoolean(
      BatchPhysicalSortMergeJoinRule.TABLE_OPTIMIZER_SMJ_REMOVE_SORT_ENABLED, true)
  }

  @Test
  def testRemoveCollation_OverWindowAgg(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "NestedLoopJoin,SortMergeJoin,HashAgg")
    val sqlQuery =
      """
        | SELECT
        |   SUM(b) sum_b,
        |   AVG(SUM(b)) OVER (PARTITION BY a order by a) avg_b,
        |   RANK() OVER (PARTITION BY a ORDER BY a) rn
        | FROM x
        | GROUP BY a
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Aggregate(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by a
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Aggregate_1(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by d
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Sort(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setBoolean(TABLE_EXEC_RANGE_SORT_ENABLED, true)
    val sqlQuery =
      """
        |WITH r AS (SELECT a, b, COUNT(c) AS cnt FROM x GROUP BY a, b)
        |SELECT * FROM r ORDER BY a
      """.stripMargin
    // exec node does not support range sort yet, so we verify rel plan here
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Aggregate_3(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg")
    util.tableEnv.getConfig.getConfiguration.setBoolean(TABLE_EXEC_RANGE_SORT_ENABLED, true)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x ORDER BY a, b)
        |SELECT a, b, COUNT(c) AS cnt FROM r GROUP BY a, b
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Rank_1(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg")
    val sqlQuery =
      """
        |SELECT a, SUM(b) FROM (
        | SELECT * FROM (
        |   SELECT a, b, RANK() OVER(PARTITION BY a ORDER BY b) rk FROM x)
        | WHERE rk <= 10
        |) GROUP BY a
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Rank_2(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg")
    val sqlQuery =
      """
        |SELECT a, b, MAX(c) FROM (
        | SELECT * FROM (
        |   SELECT a, b, c, RANK() OVER(PARTITION BY a ORDER BY b) rk FROM x)
        | WHERE rk <= 10
        |) GROUP BY a, b
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Rank_3(): Unit = {
    // TODO remove local rank for single distribution input
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, c, RANK() OVER(PARTITION BY a ORDER BY b) rk FROM (
        |   SELECT a, b, c FROM x ORDER BY a, b
        | )
        |) WHERE rk <= 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Rank_4(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg")
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, c, RANK() OVER(PARTITION BY a ORDER BY a) rk FROM (
        |   SELECT a, COUNT(c) AS c FROM x GROUP BY a
        | )
        |) WHERE rk <= 10
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_Rank_Singleton(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashAgg")
    val sqlQuery =
      """
        |SELECT COUNT(a), SUM(b) FROM (
        | SELECT * FROM (
        |   SELECT a, b, RANK() OVER(ORDER BY b) rk FROM x)
        | WHERE rk <= 10
        |)
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testRemoveCollation_MultipleSortMergeJoins1(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")

    val sql =
      """
        |select * from
        |   x join y on a = d
        |   join t1 on a = a1
        |   left outer join t2 on a = d1
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testRemoveCollation_MultipleSortMergeJoins_MultiJoinKeys1(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")

    val sql =
      """
        |select * from
        |   x join y on a = d and b = e
        |   join t1 on a = a1 and b = b1
        |   left outer join t2 on a = d1 and b = e1
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testRemoveCollation_MultipleSortMergeJoins2(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")

    val sql =
      """
        |select * from
        |   x join y on a = d
        |   join t1 on d = a1
        |   left outer join t2 on a1 = d1
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testRemoveCollation_MultipleSortMergeJoins_MultiJoinKeys2(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")

    val sql =
      """
        |select * from
        |   x join y on a = d and b = e
        |   join t1 on d = a1 and e = b1
        |   left outer join t2 on a1 = d1 and b1 = e1
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testRemoveCollation_MultipleSortMergeJoins3(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin")
    util.addTableSource("tb1",
      Array[TypeInformation[_]](
        Types.STRING, Types.STRING, Types.STRING, Types.STRING, Types.STRING),
      Array("id", "key", "tb2_ids", "tb3_ids", "name"),
      FlinkStatistic.builder().uniqueKeys(ImmutableSet.of(ImmutableSet.of("id"))).build()
    )
    util.addTableSource("tb2",
      Array[TypeInformation[_]](Types.STRING, Types.STRING),
      Array("id",  "name"),
      FlinkStatistic.builder().uniqueKeys(ImmutableSet.of(ImmutableSet.of("id"))).build()
    )
    util.addTableSource("tb3",
      Array[TypeInformation[_]](Types.STRING, Types.STRING),
      Array("id",  "name"),
      FlinkStatistic.builder().uniqueKeys(ImmutableSet.of(ImmutableSet.of("id"))).build()
    )
    util.addTableSource("tb4",
      Array[TypeInformation[_]](Types.STRING, Types.STRING),
      Array("id",  "name"),
      FlinkStatistic.builder().uniqueKeys(ImmutableSet.of(ImmutableSet.of("id"))).build()
    )
    util.addTableSource("tb5",
      Array[TypeInformation[_]](Types.STRING, Types.STRING),
      Array("id",  "name"),
      FlinkStatistic.builder().uniqueKeys(ImmutableSet.of(ImmutableSet.of("id"))).build()
    )
    util.addFunction("split", new StringSplit())

    val sql =
      """
        |with v1 as (
        | select id, tb2_id from tb1, LATERAL TABLE(split(tb2_ids)) AS T(tb2_id)
        |),
        |v2 as (
        | select id, tb3_id from tb1, LATERAL TABLE(split(tb3_ids)) AS T(tb3_id)
        |),
        |
        |join_tb2 as (
        | select tb1_id, LISTAGG(tb2_name, ',') as tb2_names
        | from (
        |  select v1.id as tb1_id, tb2.name as tb2_name
        |   from v1 left outer join tb2 on tb2_id = tb2.id
        | ) group by tb1_id
        |),
        |
        |join_tb3 as (
        | select tb1_id, LISTAGG(tb3_name, ',') as tb3_names
        | from (
        |  select v2.id as tb1_id, tb3.name as tb3_name
        |   from v2 left outer join tb3 on tb3_id = tb3.id
        | ) group by tb1_id
        |)
        |
        |select
        |   tb1.id,
        |   tb1.tb2_ids,
        |   tb1.tb3_ids,
        |   tb1.name,
        |   tb2_names,
        |   tb3_names,
        |   tb4.name,
        |   tb5.name
        | from tb1
        |   left outer join join_tb2 on tb1.id = join_tb2.tb1_id
        |   left outer join join_tb3 on tb1.id = join_tb3.tb1_id
        |   left outer join tb4 on tb1.key = tb4.id
        |   left outer join tb5 on tb1.key = tb5.id
      """.stripMargin

    util.verifyExecPlan(sql)
  }

  @Test
  def testRemoveCollation_Correlate1(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin,HashAgg")
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
  def testRemoveCollation_Correlate2(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin,HashAgg")
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
  def testRemoveCollation_Correlate3(): Unit = {
    // do not remove shuffle
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS, "HashJoin,NestedLoopJoin,HashAgg")
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

