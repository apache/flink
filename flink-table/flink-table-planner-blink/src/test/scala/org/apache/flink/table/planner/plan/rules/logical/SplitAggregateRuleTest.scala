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
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.plan.optimize.program.FlinkStreamProgram
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

/**
  * Test for [[SplitAggregateRule]].
  */
class SplitAggregateRuleTest extends TableTestBase {
  private val util = streamTestUtil()
  util.addTableSource[(Long, Int, String)]("MyTable", 'a, 'b, 'c)
  util.buildStreamProgram(FlinkStreamProgram.PHYSICAL)
  util.tableEnv.getConfig.getConfiguration.setBoolean(
    OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED, true)

  @Test
  def testSingleDistinctAgg(): Unit = {
    util.verifyPlan("SELECT COUNT(DISTINCT c) FROM MyTable")
  }

  @Test
  def testSingleMinAgg(): Unit = {
    // does not contain distinct agg
    util.verifyPlan("SELECT MIN(c) FROM MyTable")
  }

  @Test
  def testSingleFirstValueAgg(): Unit = {
    // does not contain distinct agg
    util.verifyPlan("SELECT FIRST_VALUE(c) FROM MyTable GROUP BY a")
  }

  @Test
  def testMultiDistinctAggs(): Unit = {
    util.verifyPlan("SELECT COUNT(DISTINCT a), SUM(DISTINCT b) FROM MyTable")
  }

  @Test
  def testSingleMaxWithDistinctAgg(): Unit = {
    val sqlQuery =
      """
        |SELECT a, COUNT(DISTINCT b), MAX(c)
        |FROM MyTable
        |GROUP BY a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleFirstValueWithDistinctAgg(): Unit = {
    util.verifyPlan("SELECT a, FIRST_VALUE(c), COUNT(DISTINCT b) FROM MyTable GROUP BY a")
  }

  @Test
  def testSingleLastValueWithDistinctAgg(): Unit = {
    util.verifyPlan("SELECT a, LAST_VALUE(c), COUNT(DISTINCT b) FROM MyTable GROUP BY a")
  }

  @Test
  def testSingleListAggWithDistinctAgg(): Unit = {
    util.verifyPlan("SELECT a, LISTAGG(c), COUNT(DISTINCT b) FROM MyTable GROUP BY a")
  }

  @Test
  def testSingleDistinctAggWithAllNonDistinctAgg(): Unit = {
    val sqlQuery =
      """
        |SELECT a, COUNT(DISTINCT c), SUM(b), AVG(b), MAX(b), MIN(b), COUNT(b), COUNT(*)
        |FROM MyTable
        |GROUP BY a
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctAggWithGroupBy(): Unit = {
    util.verifyPlan("SELECT a, COUNT(DISTINCT c) FROM MyTable GROUP BY a")
  }

  @Test
  def testSingleDistinctAggWithAndNonDistinctAggOnSameColumn(): Unit = {
    util.verifyPlan("SELECT a, COUNT(DISTINCT b), SUM(b), AVG(b) FROM MyTable GROUP BY a")
  }

  @Test
  def testSomeColumnsBothInDistinctAggAndGroupBy(): Unit = {
    // TODO: the COUNT(DISTINCT a) can be optimized to literal 1
    util.verifyPlan("SELECT a, COUNT(DISTINCT a), COUNT(b) FROM MyTable GROUP BY a")
  }

  @Test
  def testAggWithFilterClause(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  a,
         |  COUNT(DISTINCT b) FILTER (WHERE NOT b = 2),
         |  SUM(b) FILTER (WHERE NOT b = 5),
         |  SUM(b) FILTER (WHERE NOT b = 2)
         |FROM MyTable
         |GROUP BY a
       """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiGroupBys(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  c, MIN(b), MAX(b), SUM(b), COUNT(*), COUNT(DISTINCT a)
         |FROM(
         |  SELECT
         |    a, AVG(b) as b, MAX(c) as c
         |  FROM MyTable
         |  GROUP BY a
         |) GROUP BY c
       """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAggWithJoin(): Unit = {
    val sqlQuery =
      s"""
         |SELECT *
         |FROM(
         |  SELECT
         |    c, SUM(b) as b, SUM(b) as d, COUNT(DISTINCT a) as a
         |  FROM(
         |    SELECT
         |      a, COUNT(DISTINCT b) as b, SUM(b) as c, SUM(b) as d
         |    FROM MyTable
         |    GROUP BY a)
         |  GROUP BY c
         |) as MyTable1 JOIN MyTable ON MyTable1.b = MyTable.a
       """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testBucketsConfiguration(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setInteger(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_BUCKET_NUM, 100)
    val sqlQuery = "SELECT COUNT(DISTINCT c) FROM MyTable"
    util.verifyPlan(sqlQuery)
  }
}
