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

import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.runtime.utils.CommonTestData
import org.apache.flink.table.util.{BatchTableTestUtil, TableTestBase}
import org.junit.{Before, Test}

class RemoveRedundantLocalHashAggRuleTest extends TableTestBase {

  private var util: BatchTableTestUtil = _

  @Before
  def before(): Unit = {
    util = batchTestUtil()
    util.addTable("x", CommonTestData.get3Source(Array("a", "b", "c")))
    util.addTable("y", CommonTestData.get3Source(Array("d", "e", "f")))  }

  @Test
  def testRemoveRedundantLocalHashAgg(): Unit = {
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "SortMergeJoin,NestedLoopJoin,SortAgg")
    // disable BroadcastHashJoin
    util.tableEnv.getConfig.getConf.setLong(
      TableConfigOptions.SQL_EXEC_HASH_JOIN_BROADCAST_THRESHOLD, -1)
    val sqlQuery =
      """
        |WITH r AS (SELECT * FROM x, y WHERE a = d AND c LIKE 'He%')
        |SELECT sum(b) FROM r group by a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRemoveRedundantLocalHashAgg1(): Unit = {
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "SortAgg")
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_SHUFFLE_PARTIAL_KEY_ENABLED, true)
    val sqlQuery =
      """
        |SELECT a, SUM(b) FROM (
        | SELECT * FROM (
        |   SELECT a, b, c, RANK() OVER(PARTITION BY a, c ORDER BY b) rk FROM x)
        | WHERE rk <= 10
        |) GROUP BY a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

}
