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
package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

/**
  * Tests for [[RemoveRedundantLocalRankRule]].
  */
class RemoveRedundantLocalRankRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.addTableSource[(Int, Long, String)]("x", 'a, 'b, 'c)
  }

  @Test
  def testSameRankRange(): Unit = {
    val sqlQuery =
      """
        |SELECT a FROM (
        | SELECT a, RANK() OVER(PARTITION BY a ORDER BY SUM(b)) rk FROM x GROUP BY a
        |) WHERE rk <= 5
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testDiffRankRange(): Unit = {
    val sqlQuery =
      """
        |SELECT a FROM (
        | SELECT a, RANK() OVER(PARTITION BY a ORDER BY SUM(b)) rk FROM x GROUP BY a
        |) WHERE rk <= 5 and rk >= 2
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiRanks(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b, rk, RANK() OVER(PARTITION BY a ORDER BY b) rk1 FROM (
        |   SELECT a, b, RANK() OVER(PARTITION BY a ORDER BY b) rk FROM x
        | ) WHERE rk <= 5
        |) WHERE rk1 <= 5
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

}
