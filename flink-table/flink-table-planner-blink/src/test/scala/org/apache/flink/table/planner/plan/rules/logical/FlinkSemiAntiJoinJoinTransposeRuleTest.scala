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
import org.apache.flink.table.planner.plan.optimize.program.{FlinkBatchProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.{TableConfigUtils, TableTestBase}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
  * Test for [[FlinkSemiAntiJoinJoinTransposeRule]].
  */
class FlinkSemiAntiJoinJoinTransposeRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_COLLECTION)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          FlinkSemiAntiJoinProjectTransposeRule.INSTANCE,
          FlinkSemiAntiJoinFilterTransposeRule.INSTANCE,
          FlinkSemiAntiJoinJoinTransposeRule.INSTANCE))
        .build()
    )

    util.addTableSource[(Int, Long, String)]("x", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("y", 'd, 'e, 'f)
    util.addTableSource[(Int, Long, String)]("z", 'i, 'j, 'k)
  }

  @Test
  def testIN_EquiCondition_InnerJoin1(): Unit = {
    // keys of semi-join are from join's right
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "y.d IN (SELECT z.i FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_EquiCondition_InnerJoin2(): Unit = {
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "y.d IN (SELECT z.i FROM z WHERE y.e = z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_EquiCondition_InnerJoin3(): Unit = {
    // keys of semi-join are from join's left
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "x.a IN (SELECT z.i FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_EquiCondition_InnerJoin4(): Unit = {
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "x.a IN (SELECT z.i FROM z WHERE x.b = z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_EquiCondition_InnerJoin5(): Unit = {
    // keys of semi-join are from both join's right and join's left
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "(x.a, y.e) IN (SELECT z.i, z.j FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_EquiCondition_InnerJoin6(): Unit = {
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "x.a IN (SELECT z.i FROM z WHERE y.e = z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_NonEquiCondition_InnerJoin1(): Unit = {
    // keys of semi-join are from join's right
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "y.d IN (SELECT z.i FROM z WHERE y.e > z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_NonEquiCondition_InnerJoin2(): Unit = {
    // keys of semi-join are from join's left
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "x.a IN (SELECT z.i FROM z WHERE x.b > z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_NonEquiCondition_InnerJoin3(): Unit = {
    // keys of semi-join are from both join's right and join's left
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "x.a IN (SELECT z.i FROM z WHERE y.e > z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_NonEquiCondition_InnerJoin4(): Unit = {
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "y.d IN (SELECT z.i FROM z WHERE x.b > z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testEXISTS_EquiCondition_InnerJoin1(): Unit = {
    // no keys
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "EXISTS (SELECT * FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testEXISTS_EquiCondition_InnerJoin2(): Unit = {
    // keys of semi-join are from join's left
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "EXISTS (SELECT * FROM z WHERE z.i = x.a AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testEXISTS_EquiCondition_InnerJoin3(): Unit = {
    // keys of semi-join are from join's right
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "EXISTS (SELECT * FROM z WHERE z.i = y.d AND z.j < 50)"
    util.verifyPlan(sqlQuery)

    // keys of semi-join are from both join's right and join's left
    // calcite does not support below sql:
    // "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
    //   "EXISTS (SELECT * FROM z WHERE z.i = y.d AND x.b = z.j AND z.j < 50)"
  }

  @Test
  def testEXISTS_NonEquiCondition_InnerJoin1(): Unit = {
    // keys of semi-join are from join's left
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "EXISTS (SELECT * FROM z WHERE z.i > x.a AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testEXISTS_NonEquiCondition_InnerJoin2(): Unit = {
    // keys of semi-join are from join's right
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "EXISTS (SELECT * FROM z WHERE z.i < y.d AND z.j < 50)"
    util.verifyPlan(sqlQuery)

    // keys of semi-join are from both join's right and join's left
    // calcite does not support below sql:
    // "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
    //   "EXISTS (SELECT * FROM z WHERE z.i < y.d AND x.b = z.j AND z.j < 50)"
  }

  @Test
  def testNOT_IN_EquiCondition_InnerJoin1(): Unit = {
    // keys of anti-join are from join's right
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "y.d NOT IN (SELECT z.i FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)

  }

  @Test
  def testNOT_IN_EquiCondition_InnerJoin2(): Unit = {
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "y.d NOT IN (SELECT z.i FROM z WHERE y.e = z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNOT_IN_EquiCondition_InnerJoin3(): Unit = {
    // keys of semi-join are from join's left
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "x.a NOT IN (SELECT z.i FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNOT_IN_EquiCondition_InnerJoin4(): Unit = {
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "x.a NOT IN (SELECT z.i FROM z WHERE x.b = z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNOT_IN_EquiCondition_InnerJoin5(): Unit = {
    // keys of semi-join are from both join's right and join's left
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "(x.a, y.e) NOT IN (SELECT z.i, z.j FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNOT_IN_EquiCondition_InnerJoin6(): Unit = {
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "x.a NOT IN (SELECT z.i FROM z WHERE y.e = z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNOT_IN_NonEquiCondition_InnerJoin1(): Unit = {
    // keys of semi-join are from join's right
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "y.d NOT IN (SELECT z.i FROM z WHERE y.e > z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNOT_IN_NonEquiCondition_InnerJoin2(): Unit = {
    // keys of semi-join are from join's left
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "x.a NOT IN (SELECT z.i FROM z WHERE x.b > z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNOT_IN_NonEquiCondition_InnerJoin3(): Unit = {
    // keys of semi-join are from both join's right and join's left
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "x.a NOT IN (SELECT z.i FROM z WHERE y.e > z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNOT_IN_NonEquiCondition_InnerJoin4(): Unit = {
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "y.d NOT IN (SELECT z.i FROM z WHERE x.b > z.j AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNOT_EXISTS_EquiCondition_InnerJoin1(): Unit = {
    // no keys
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "NOT EXISTS (SELECT * FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNOT_EXISTS_EquiCondition_InnerJoin2(): Unit = {
    // keys of semi-join are from join's left
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "NOT EXISTS (SELECT * FROM z WHERE z.i = x.a AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNOT_EXISTS_EquiCondition_InnerJoin3(): Unit = {
    // keys of semi-join are from join's right
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "NOT EXISTS (SELECT * FROM z WHERE z.i = y.d AND z.j < 50)"
    util.verifyPlan(sqlQuery)

    // keys of semi-join are from both join's right and join's left
    // calcite does not support below sql:
    // "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
    //   "NOT EXISTS (SELECT * FROM z WHERE z.i = y.d AND x.b = z.j AND z.j < 50)"
  }

  @Test
  def testNOT_EXISTS_NonEquiCondition_InnerJoin1(): Unit = {
    // keys of semi-join are from join's left
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "NOT EXISTS (SELECT * FROM z WHERE z.i > x.a AND z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNOT_EXISTS_NonEquiCondition_InnerJoin2(): Unit = {
    // keys of semi-join are from join's right
    val sqlQuery = "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
      "NOT EXISTS (SELECT * FROM z WHERE z.i < y.d AND z.j < 50)"
    util.verifyPlan(sqlQuery)

    // keys of semi-join are from both join's right and join's left
    // calcite does not support below sql:
    // "SELECT a, f FROM x, y WHERE x.c = y.f AND y.e > 100 AND " +
    //   "NOT EXISTS (SELECT * FROM z WHERE z.i < y.d AND x.b = z.j AND z.j < 50)"
  }

  @Test
  def testIN_LeftJoin1(): Unit = {
    // keys of semi-join are from join's right
    val sqlQuery = "SELECT a, f FROM (SELECT * FROM x LEFT JOIN y ON x.c = y.f) xy " +
      "WHERE xy.e > 100 AND xy.d IN (SELECT z.i FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_LeftJoin2(): Unit = {
    // keys of semi-join are from join's left
    val sqlQuery = "SELECT a, f FROM (SELECT * FROM x LEFT JOIN y ON x.c = y.f) xy " +
      "WHERE xy.e > 100 AND xy.a IN (SELECT z.i FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_RightJoin1(): Unit = {
    // keys of semi-join are from join's right
    val sqlQuery = "SELECT a, f FROM (SELECT * FROM x RIGHT JOIN y ON x.c = y.f) xy " +
      "WHERE xy.e > 100 AND xy.d IN (SELECT z.i FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_RightJoin2(): Unit = {
    // keys of semi-join are from join's left
    val sqlQuery = "SELECT a, f FROM (SELECT * FROM x RIGHT JOIN y ON x.c = y.f) xy " +
      "WHERE xy.e > 100 AND xy.a IN (SELECT z.i FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_FullJoin1(): Unit = {
    // keys of semi-join are from join's right
    val sqlQuery = "SELECT a, f FROM (SELECT * FROM x FULL JOIN y ON x.c = y.f) xy " +
      "WHERE xy.e > 100 AND xy.d IN (SELECT z.i FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testIN_FullJoin2(): Unit = {
    // keys of semi-join are from join's left
    val sqlQuery = "SELECT a, f FROM (SELECT * FROM x FULL JOIN y ON x.c = y.f) xy " +
      "WHERE xy.e > 100 AND xy.a IN (SELECT z.i FROM z WHERE z.j < 50)"
    util.verifyPlan(sqlQuery)
  }

}

