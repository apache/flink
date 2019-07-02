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

package org.apache.flink.table.plan.rules.logical

import org.apache.flink.api.scala._
import org.apache.flink.table.calcite.CalciteConfig
import org.apache.flink.table.plan.optimize.program.{FlinkChainedProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE, StreamOptimizeContext}
import org.apache.flink.table.util.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
  * Test for [[FlinkAggregateJoinTransposeRule]].
  * this class only test left/right outer join.
  */
class FlinkAggregateOuterJoinTransposeRuleTest extends TableTestBase {

  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    val program = new FlinkChainedProgram[StreamOptimizeContext]()
    program.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          FlinkFilterJoinRule.FILTER_ON_JOIN,
          FlinkFilterJoinRule.JOIN,
          FilterAggregateTransposeRule.INSTANCE,
          FilterProjectTransposeRule.INSTANCE,
          FilterMergeRule.INSTANCE,
          AggregateProjectMergeRule.INSTANCE,
          FlinkAggregateJoinTransposeRule.LEFT_RIGHT_OUTER_JOIN_EXTENDED
        ))
        .build()
    )
    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceStreamProgram(program).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.addTableSource[(Int, Long, String, Int)]("T", 'a, 'b, 'c, 'd)
  }

  @Test
  def testPushCountAggThroughJoinOverUniqueColumn(): Unit = {
    util.verifyPlan("SELECT COUNT(A.a) FROM (SELECT DISTINCT a FROM T) AS A JOIN T AS B ON A.a=B.a")
  }

  @Test
  def testPushSumAggThroughJoinOverUniqueColumn(): Unit = {
    util.verifyPlan("SELECT SUM(A.a) FROM (SELECT DISTINCT a FROM T) AS A JOIN T AS B ON A.a=B.a")
  }

  @Test
  def testPushCountAggThroughLeftJoinOverUniqueColumn(): Unit = {
    val sqlQuery = "SELECT COUNT(A.a) FROM (SELECT DISTINCT a FROM T) AS A " +
      "LEFT OUTER JOIN T AS B ON A.a=B.a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPushSumAggThroughLeftJoinOverUniqueColumn(): Unit = {
    val sqlQuery = "SELECT SUM(A.a) FROM (SELECT DISTINCT a FROM T) AS A " +
      "LEFT OUTER JOIN T AS B ON A.a=B.a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPushCountAllAggThroughLeftJoinOverUniqueColumn(): Unit = {
    val sqlQuery = "SELECT COUNT(*) FROM (SELECT DISTINCT a FROM T) AS A " +
      "LEFT OUTER JOIN T AS B ON A.a=B.a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPushCountAggThroughLeftJoinOverUniqueColumnAndGroupByLeft(): Unit = {
    val sqlQuery = "SELECT COUNT(B.b) FROM (SELECT DISTINCT a FROM T) AS A " +
      "LEFT OUTER JOIN T AS B ON A.a=B.a GROUP BY A.a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPushCountAggThroughLeftJoinOverUniqueColumnAndGroupByRight(): Unit = {
    val sqlQuery = "SELECT COUNT(B.b) FROM (SELECT DISTINCT a FROM T) AS A " +
      "LEFT OUTER JOIN T AS B ON A.a=B.a GROUP BY B.a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPushCountAggThroughLeftJoinAndGroupByLeft(): Unit = {
    val sqlQuery = "SELECT COUNT(B.b) FROM (SELECT a FROM T) AS A " +
      "LEFT OUTER JOIN T AS B ON A.a=B.a GROUP BY A.a"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testPushCountAggThroughRightJoin(): Unit = {
    val sqlQuery = "SELECT COUNT(B.b) FROM T AS B RIGHT OUTER JOIN (SELECT a FROM T) AS A " +
      "ON A.a=B.a GROUP BY A.a"
    util.verifyPlan(sqlQuery)
  }

}
