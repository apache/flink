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
package org.apache.flink.table.plan.rules.physical.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{PlannerConfigOptions, TableConfigOptions}
import org.apache.flink.table.util.TableTestBase

import org.junit.{Before, Test}

/**
  * Test for [[RemoveRedundantLocalHashAggRule]].
  */
class RemoveRedundantLocalHashAggRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.addTableSource[(Int, Long, String)]("x", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("y", 'd, 'e, 'f)
  }

  @Test
  def testRemoveRedundantLocalHashAgg_ShuffleKeyFromJoin(): Unit = {
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "SortMergeJoin,NestedLoopJoin,SortAgg")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConf.setLong(
      PlannerConfigOptions.SQL_OPTIMIZER_HASH_JOIN_BROADCAST_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT SUM(b) FROM r GROUP BY a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRemoveRedundantLocalHashAgg_ShuffleKeyFromRank(): Unit = {
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "SortAgg")
    util.tableEnv.getConfig.getConf.setBoolean(
      PlannerConfigOptions.SQL_OPTIMIZER_SHUFFLE_PARTIAL_KEY_ENABLED, true)
    val sqlQuery =
      """
        |SELECT a, SUM(b) FROM (
        | SELECT * FROM (
        |   SELECT a, b, c, RANK() OVER (PARTITION BY a, c ORDER BY b) rk FROM x)
        | WHERE rk <= 10
        |) GROUP BY a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

}
