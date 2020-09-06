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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

/**
  * Test for [[RankNumberColumnRemoveRule]].
  */
class RankNumberColumnRemoveRuleTest extends TableTestBase {
  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    util.buildStreamProgram(FlinkStreamProgram.PHYSICAL)

    util.addDataStream[(Int, String, Long)]("MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
  }

  @Test
  def testCannotRemoveRankNumberColumn1(): Unit = {
    val sql =
      """
        |SELECT a, rank_num FROM (
        |  SELECT *,
        |      RANK() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num >= 1 AND rank_num < 2
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testCannotRemoveRankNumberColumn2(): Unit = {
    val sql =
      """
        |SELECT a, rank_num FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num >= 1 AND rank_num < 3
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testCannotRemoveRankNumberColumn3(): Unit = {
    // the Rank does not output rank number, so this rule will not be matched
    val sql =
      """
        |SELECT a FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num >= 1 AND rank_num < 2
      """.stripMargin

    util.verifyPlan(sql)
  }

  @Test
  def testCouldRemoveRankNumberColumn(): Unit = {
    val sql =
      """
        |SELECT a, rank_num FROM (
        |  SELECT *,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY rowtime DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num >= 1 AND rank_num < 2
      """.stripMargin

    util.verifyPlan(sql)
  }
}
