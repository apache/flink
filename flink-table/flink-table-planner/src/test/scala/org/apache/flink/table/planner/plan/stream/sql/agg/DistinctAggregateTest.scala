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
package org.apache.flink.table.planner.plan.stream.sql.agg

import org.apache.flink.api.common.time.Time
import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.plan.rules.physical.stream.IncrementalAggregateRule
import org.apache.flink.table.planner.utils.{AggregatePhaseStrategy, StreamTableTestUtil, TableTestBase}

import org.junit.{Before, Test}
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.util

@RunWith(classOf[Parameterized])
class DistinctAggregateTest(
    splitDistinctAggEnabled: Boolean,
    aggPhaseEnforcer: AggregatePhaseStrategy)
  extends TableTestBase {

  protected val util: StreamTableTestUtil = streamTestUtil()
  util.addTableSource[(Int, Long, String)]("MyTable", 'a, 'b, 'c)

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.setIdleStateRetentionTime(Time.hours(1), Time.hours(2))
    util.enableMiniBatch()
    util.tableEnv.getConfig
      .set(OptimizerConfigOptions.TABLE_OPTIMIZER_AGG_PHASE_STRATEGY, aggPhaseEnforcer.toString)
    util.tableEnv.getConfig.set(
      OptimizerConfigOptions.TABLE_OPTIMIZER_DISTINCT_AGG_SPLIT_ENABLED,
      Boolean.box(splitDistinctAggEnabled))
    // disable incremental agg
    util.tableEnv.getConfig
      .set(IncrementalAggregateRule.TABLE_OPTIMIZER_INCREMENTAL_AGG_ENABLED, Boolean.box(false))
  }

  @Test
  def testSingleDistinctAgg(): Unit = {
    util.verifyExecPlan("SELECT COUNT(DISTINCT c) FROM MyTable")
  }

  @Test
  def testMultiDistinctAggs(): Unit = {
    util.verifyExecPlan("SELECT COUNT(DISTINCT a), SUM(DISTINCT b) FROM MyTable")
  }

  @Test
  def testSingleMaxWithDistinctAgg(): Unit = {
    val sqlQuery =
      """
        |SELECT a, COUNT(DISTINCT b), MAX(c)
        |FROM MyTable
        |GROUP BY a
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSingleFirstValueWithDistinctAgg(): Unit = {
    // FIRST_VALUE is not mergeable, so the final plan does not contain local agg
    util.verifyExecPlan("SELECT a, FIRST_VALUE(c), COUNT(DISTINCT b) FROM MyTable GROUP BY a")
  }

  @Test
  def testSingleLastValueWithDistinctAgg(): Unit = {
    // LAST_VALUE is not mergeable, so the final plan does not contain local agg
    util.verifyExecPlan("SELECT a, LAST_VALUE(c), COUNT(DISTINCT b) FROM MyTable GROUP BY a")
  }

  @Test
  def testSingleListAggWithDistinctAgg(): Unit = {
    util.verifyExecPlan("SELECT a, LISTAGG(c), COUNT(DISTINCT b) FROM MyTable GROUP BY a")
  }

  @Test
  def testSingleDistinctAggWithAllNonDistinctAgg(): Unit = {
    val sqlQuery =
      """
        |SELECT a, COUNT(DISTINCT c), SUM(b), AVG(b), MAX(b), MIN(b), COUNT(b), COUNT(*)
        |FROM MyTable
        |GROUP BY a
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testTwoDistinctAggregateWithNonDistinctAgg(): Unit = {
    util.verifyExecPlan(
      "SELECT c, SUM(DISTINCT a), SUM(a), COUNT(DISTINCT b) FROM MyTable GROUP BY c")
  }

  @Test
  def testSingleDistinctAggWithGroupBy(): Unit = {
    util.verifyExecPlan("SELECT a, COUNT(DISTINCT c) FROM MyTable GROUP BY a")
  }

  @Test
  def testSingleDistinctAggWithAndNonDistinctAggOnSameColumn(): Unit = {
    util.verifyExecPlan("SELECT a, COUNT(DISTINCT b), SUM(b), AVG(b) FROM MyTable GROUP BY a")
  }

  @Test
  def testSomeColumnsBothInDistinctAggAndGroupBy(): Unit = {
    // TODO: the COUNT(DISTINCT a) can be optimized to literal 1
    util.verifyExecPlan("SELECT a, COUNT(DISTINCT a), COUNT(b) FROM MyTable GROUP BY a")
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
    util.verifyExecPlan(sqlQuery)
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
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testSingleDistinctWithRetraction(): Unit = {
    val sqlQuery =
      """
        |SELECT a, COUNT(DISTINCT b), COUNT(1)
        |FROM (
        |  SELECT c, AVG(a) as a, AVG(b) as b
        |  FROM MyTable
        |  GROUP BY c
        |) GROUP BY a
      """.stripMargin
    util.verifyRelPlan(sqlQuery, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testSumCountWithSingleDistinctAndRetraction(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  b, SUM(b1), COUNT(DISTINCT b1), COUNT(1)
         |FROM(
         |   SELECT
         |     a, COUNT(b) as b, MAX(b) as b1
         |   FROM MyTable
         |   GROUP BY a
         |) GROUP BY b
       """.stripMargin
    util.verifyRelPlan(sqlQuery, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testMinMaxWithRetraction(): Unit = {
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
    util.verifyRelPlan(sqlQuery, ExplainDetail.CHANGELOG_MODE)
  }

  @Test
  def testFirstValueLastValueWithRetraction(): Unit = {
    val sqlQuery =
      s"""
         |SELECT
         |  b, FIRST_VALUE(c), LAST_VALUE(c), COUNT(DISTINCT c)
         |FROM(
         |  SELECT
         |    a, COUNT(DISTINCT b) as b, MAX(b) as c
         |  FROM MyTable
         |  GROUP BY a
         |) GROUP BY b
       """.stripMargin
    util.verifyRelPlan(sqlQuery, ExplainDetail.CHANGELOG_MODE)
  }
}

object DistinctAggregateTest {
  @Parameterized.Parameters(name = "splitDistinctAggEnabled={0}, aggPhaseEnforcer={1}")
  def parameters(): util.Collection[Array[Any]] = {
    util.Arrays.asList(
      Array(true, AggregatePhaseStrategy.ONE_PHASE),
      Array(true, AggregatePhaseStrategy.TWO_PHASE),
      Array(false, AggregatePhaseStrategy.ONE_PHASE),
      Array(false, AggregatePhaseStrategy.TWO_PHASE)
    )
  }
}
