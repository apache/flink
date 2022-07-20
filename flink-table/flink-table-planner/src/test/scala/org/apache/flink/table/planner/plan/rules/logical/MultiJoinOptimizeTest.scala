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

import org.apache.flink.table.catalog.ObjectPath
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.planner.plan.optimize.program._
import org.apache.flink.table.planner.utils.{TableConfigUtils, TableTestBase}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules.CoreRules
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
 * Plan tests for join order using rule [[org.apache.calcite.rel.rules.LoptOptimizeJoinRule]] after
 * joins converts to multi join set by
 * [[org.apache.flink.table.planner.plan.rules.logical.FlinkJoinToMultiJoinRule]].
 */
class MultiJoinOptimizeTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val tEnv = util.tableEnv

    util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "merge join into multi join",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(
          RuleSets.ofList(
            FlinkJoinToMultiJoinRule.INSTANCE,
            CoreRules.PROJECT_MULTI_JOIN_MERGE,
            CoreRules.FILTER_MULTI_JOIN_MERGE))
        .build()
    )
    calciteConfig.getBatchProgram.get.addLast(
      "join reorder",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets
          .ofList(RewriteMultiJoinConditionRule.INSTANCE, CoreRules.MULTI_JOIN_OPTIMIZE))
        .build()
    )

    tEnv.executeSql("""
                      |CREATE TABLE T1 (
                      | a int,
                      | b bigint
                      | ) with (
                      |   'connector' = 'values',
                      |   'bounded' = 'true'
                      | )
                      |""".stripMargin)

    tEnv.executeSql("""
                      |CREATE TABLE T2 (
                      | c int,
                      | d bigint
                      | ) with (
                      |   'connector' = 'values',
                      |   'bounded' = 'true'
                      | )
                      |""".stripMargin)

    tEnv.executeSql("""
                      |CREATE TABLE T3 (
                      | e int,
                      | f bigint
                      | ) with (
                      |   'connector' = 'values',
                      |   'bounded' = 'true'
                      | )
                      |""".stripMargin)

    // Adding source table statistics.
    tEnv
      .getCatalog(tEnv.getCurrentCatalog)
      .get()
      .alterTableStatistics(
        new ObjectPath(tEnv.getCurrentDatabase, "T1"),
        new CatalogTableStatistics(1000L, 1000, 1000L, 1000L),
        false)

    tEnv
      .getCatalog(tEnv.getCurrentCatalog)
      .get()
      .alterTableStatistics(
        new ObjectPath(tEnv.getCurrentDatabase, "T2"),
        new CatalogTableStatistics(100L, 100, 100L, 100L),
        false)

    tEnv
      .getCatalog(tEnv.getCurrentCatalog)
      .get()
      .alterTableStatistics(
        new ObjectPath(tEnv.getCurrentDatabase, "T3"),
        new CatalogTableStatistics(10L, 10, 10L, 10L),
        false)

  }

  @Test
  def testInnerJoinToMultiJoin(): Unit = {
    // Can translate join to multi join, T2 and T3 will join first.
    val sqlQuery =
      "SELECT * FROM T1 JOIN T2 ON a = c JOIN (SELECT * FROM T3 ) ON a = e AND e > 10"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoinLeftOuterJoinToMultiJoin(): Unit = {
    // Can translate join to multi join. T1 and T3  will join first.
    val sqlQuery =
      "SELECT * FROM T1 LEFT OUTER JOIN T2 ON a = c LEFT OUTER JOIN (SELECT * FROM T3) ON a = e AND e > 10"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testDoesNotMatchLeftOuterJoinRightOuterJoin(): Unit = {
    // Cannot translate join to multi join.
    val sqlQuery =
      "SELECT * FROM T1 RIGHT OUTER JOIN T2 ON a = c LEFT OUTER JOIN (SELECT * FROM T3) ON a = e AND e > 10"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testDoesNotMatchFullOuterJoin(): Unit = {
    // Cannot translate join to multi join.
    val sqlQuery = "SELECT * FROM T1 FULL OUTER JOIN T2 ON a = c, T3 WHERE a = e AND e > 10"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testDoesNotMatchSemiJoin(): Unit = {
    val sqlQuery =
      "SELECT * FROM (SELECT * FROM T1 JOIN T2 ON a = c) t WHERE a IN (SELECT e FROM T3)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testDoesNotMatchAntiJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (SELECT * FROM T1 JOIN T2 ON a = c) t
        |WHERE NOT EXISTS (SELECT e FROM T3  WHERE a = e)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }
}
