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
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.StringSplit
import org.apache.flink.table.planner.utils.{TableConfigUtils, TableTestBase}

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
  * Test for [[ProjectSemiAntiJoinTransposeRule]].
  */
class ProjectSemiAntiJoinTransposeRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    util.buildBatchProgram(FlinkBatchProgram.DEFAULT_REWRITE)
    val calciteConfig = TableConfigUtils.getCalciteConfig(util.tableEnv.getConfig)
    calciteConfig.getBatchProgram.get.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(ProjectSemiAntiJoinTransposeRule.INSTANCE))
        .build()
    )

    util.addTableSource[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("MyTable2", 'd, 'e, 'f)
  }

  @Test
  def testNotNeedTransposeProject_Semi1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 WHERE a IN (SELECT d FROM MyTable2)")
  }

  @Test
  def testNotNeedTransposeProject_Semi2(): Unit = {
    util.verifyPlan("SELECT b + 1, c FROM MyTable1 WHERE a IN (SELECT d FROM MyTable2)")
  }

  @Test
  def testNotNeedTransposeProject_Semi3(): Unit = {
    util.verifyPlan("SELECT c FROM MyTable1 WHERE a IN (SELECT d FROM MyTable2 WHERE b = e)")
  }

  @Test
  def testNotNeedTransposeProject_Anti1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable1 WHERE a NOT IN (SELECT d FROM MyTable2)")
  }

  @Test
  def testNotNeedTransposeProject_Anti2(): Unit = {
    util.verifyPlan("SELECT b + 1, c FROM MyTable1 WHERE a NOT IN (SELECT d FROM MyTable2)")
  }

  @Test
  def testNotNeedTransposeProject_Anti3(): Unit = {
    val sqlQuery =
      "SELECT c FROM MyTable1 WHERE a NOT IN (SELECT d FROM MyTable2 WHERE b = e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_Semi1(): Unit = {
    util.verifyPlan("SELECT a FROM MyTable1 WHERE a IN (SELECT d FROM MyTable2)")
  }

  @Test
  def testTransposeProject_Semi2(): Unit = {
    util.verifyPlan("SELECT a + 1 FROM MyTable1 WHERE a IN (SELECT d FROM MyTable2)")
  }

  @Test
  def testTransposeProject_Semi3(): Unit = {
    util.verifyPlan("SELECT a, b FROM MyTable1 WHERE a IN (SELECT d FROM MyTable2)")
  }

  @Test
  def testTransposeProject_Semi4(): Unit = {
    util.verifyPlan("SELECT b, a FROM MyTable1 WHERE a IN (SELECT d FROM MyTable2)")
  }

  @Test
  def testTransposeProject_Semi5(): Unit = {
    val sqlQuery = "SELECT d FROM (SELECT a, b, c, d FROM MyTable1, MyTable2) t " +
      "WHERE a IN (SELECT d FROM MyTable2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_Anti1(): Unit = {
    util.verifyPlan("SELECT a FROM MyTable1 WHERE a NOT IN (SELECT d FROM MyTable2)")
  }

  @Test
  def testTransposeProject_Anti2(): Unit = {
    util.verifyPlan( "SELECT a + 1 FROM MyTable1 WHERE a NOT IN (SELECT d FROM MyTable2)")
  }

  @Test
  def testTransposeProject_Anti3(): Unit = {
    util.verifyPlan("SELECT a, b FROM MyTable1 WHERE a NOT IN (SELECT d FROM MyTable2)")
  }

  @Test
  def testTransposeProject_Anti4(): Unit = {
    util.verifyPlan("SELECT b, a FROM MyTable1 WHERE a NOT IN (SELECT d FROM MyTable2)")
  }

  @Test
  def testTransposeProject_Anti5(): Unit = {
    val sqlQuery = "SELECT d FROM (SELECT a, b, c, d FROM MyTable1, MyTable2) t " +
      "WHERE a NOT IN (SELECT d FROM MyTable2)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_EmptyProject(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable3", 'i, 'j, 'k)
    util.addFunction("table_func", new StringSplit)

    val sqlQuery = "SELECT * FROM MyTable1 WHERE EXISTS (" +
      "SELECT * FROM MyTable2, " +
      "LATERAL TABLE(table_func(f)) AS T(f1) WHERE EXISTS (SELECT * FROM MyTable3))"
    util.verifyPlan(sqlQuery)
  }
}
