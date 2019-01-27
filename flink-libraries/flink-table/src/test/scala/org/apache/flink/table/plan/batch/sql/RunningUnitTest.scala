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

package org.apache.flink.table.plan.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.{BatchTableTestUtil, TableTestBase}
import org.junit.{Before, Test}

/**
  * Test for NodeRunningUnit.
  */
class RunningUnitTest extends TableTestBase {
  private var util: BatchTableTestUtil = _

  @Before
  def before(): Unit = {
    util = batchTestUtil()
    util.addTable[(Int, Long, String)]("t", 'a, 'b, 'c)
    util.addTable("x", CommonTestData.get3Source(Array("a", "b", "c")))
    util.addTable("y", CommonTestData.get3Source(Array("d", "e", "f")))
    util.tableEnv.alterTableStats("x", Some(TableStats(100L)))
    util.tableEnv.alterTableStats("y", Some(TableStats(100L)))
    util.tableEnv.getConfig.setSubsectionOptimization(false)
  }

  @Test
  def testBroadcastJoin(): Unit = {
    val sqlQuery = "SELECT sum(b)  FROM x, y WHERE a = d"
    util.verifyPlanWithRunningUnit(sqlQuery)
  }

  @Test
  def testSortMergeJoin(): Unit = {
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "NestedLoopJoin, HashJoin")
    val sqlQuery = "SELECT sum(b)  FROM x, y WHERE a = d"
    util.verifyPlanWithRunningUnit(sqlQuery)
  }

  @Test
  def testLeftSemi(): Unit = {
    util.disableBroadcastHashJoin()
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "NestedLoopJoin")
    util.tableEnv.alterTableStats("x", Some(TableStats(2L)))
    util.tableEnv.alterTableStats("y", Some(TableStats(200000L)))
    val sqlQuery = "SELECT * FROM x WHERE a IN (SELECT d FROM y)"
    util.verifyPlanWithRunningUnit(sqlQuery)
  }

  @Test
  def testReusedNodeIsBarrierNode(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED, false)
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "HashJoin,SortMergeJoin")
    val sqlQuery =
      """
        |WITH r AS (SELECT c, SUM(a) a, SUM(b) b FROM x GROUP BY c)
        |    SELECT * FROM r r1, r r2 WHERE r1.a = r2.b AND r2.a > 1
      """.stripMargin
    util.verifyPlanWithRunningUnit(sqlQuery)
  }

  @Test
  def testReuseSubPlan_SetExchangeAsBatch(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_SUB_PLAN_ENABLED, true)
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_REUSE_TABLE_SOURCE_ENABLED, true)
    val sqlQuery =
      """
        |WITH t AS (SELECT x.a AS a, x.b AS b, y.d AS d, y.e AS e FROM x, y WHERE x.a = y.d)
        |SELECT t1.*, t2.* FROM t t1, t t2 WHERE t1.b = t2.e AND t1.a < 10 AND t2.a > 5
      """.stripMargin
    util.tableEnv.alterTableStats("x", Some(TableStats(100000000L)))
    util.tableEnv.alterTableStats("y", Some(TableStats(1000000000L)))
    util.verifyPlanWithRunningUnit(sqlQuery)
  }

  @Test
  def testUnionAll(): Unit = {
    util.addTable("z", CommonTestData.get3Source(Array("a", "b", "c")))
    val sqlQuery = "SELECT sum(a) FROM (" +
        "SELECT a, c FROM x UNION ALL (SELECT a, c FROM z))" +
        "GROUP BY c"
    util.verifyPlanWithRunningUnit(sqlQuery)
  }

  @Test
  def testUnionAllWithExternalShuffle(): Unit = {
    util.tableEnv.config.getConf.setBoolean(
      TableConfigOptions.SQL_EXEC_DATA_EXCHANGE_MODE_ALL_BATCH, true)
    util.addTable("z", CommonTestData.get3Source(Array("a", "b", "c")))
    val sqlQuery = "SELECT sum(a) FROM (" +
        "SELECT a, c FROM x UNION ALL (SELECT a, c FROM z))" +
        "GROUP BY c"
    util.verifyPlanWithRunningUnit(sqlQuery)
  }
}
