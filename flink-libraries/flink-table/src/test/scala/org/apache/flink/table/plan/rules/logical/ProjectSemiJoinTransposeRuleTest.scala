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
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.scala._
import org.apache.flink.table.calcite.CalciteConfig

import org.apache.calcite.tools.RuleSets
import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.{Before, Test}

@RunWith(classOf[Parameterized])
class ProjectSemiJoinTransposeRuleTest(fieldsNullable: Boolean)
  extends SubQueryTestBase(fieldsNullable) {

  @Before
  def setup(): Unit = {
    // update programs inited in parent class
    val programs = util.getTableEnv.getConfig.getCalciteConfig.getBatchPrograms
      .getOrElse(throw new TableException("optimize programs is not set in parent class"))
    programs.getFlinkRuleSetProgram("semi_join").get
      .add(RuleSets.ofList(ProjectSemiJoinTransposeRule.INSTANCE))
    val calciteConfig = CalciteConfig.createBuilder(util.tableEnv.getConfig.getCalciteConfig)
      .replaceBatchPrograms(programs).build()
    util.tableEnv.getConfig.setCalciteConfig(calciteConfig)

    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
  }

  @Test
  def testNotNeedTransposeProject_Semi1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE a IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotNeedTransposeProject_Semi2(): Unit = {
    val sqlQuery = "SELECT b + 1, c FROM l WHERE a IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotNeedTransposeProject_Semi3(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE a IN (SELECT d FROM r WHERE l.b = r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotNeedTransposeProject_Anti1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE a NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotNeedTransposeProject_Anti2(): Unit = {
    val sqlQuery = "SELECT b + 1, c FROM l WHERE a NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotNeedTransposeProject_Anti3(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE a NOT IN (SELECT d FROM r WHERE l.b = r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_Semi1(): Unit = {
    val sqlQuery = "SELECT a FROM l WHERE a IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_Semi2(): Unit = {
    val sqlQuery = "SELECT a + 1 FROM l WHERE a IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_Semi3(): Unit = {
    val sqlQuery = "SELECT a, b FROM l WHERE a IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_Semi4(): Unit = {
    val sqlQuery = "SELECT b, a FROM l WHERE a IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_Semi5(): Unit = {
    val sqlQuery = "SELECT d FROM (SELECT a, b, c, d FROM l, r) lr WHERE a IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_Anti1(): Unit = {
    val sqlQuery = "SELECT a FROM l WHERE a NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_Anti2(): Unit = {
    val sqlQuery = "SELECT a + 1 FROM l WHERE a NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_Anti3(): Unit = {
    val sqlQuery = "SELECT a, b FROM l WHERE a NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_Anti4(): Unit = {
    val sqlQuery = "SELECT b, a FROM l WHERE a NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testTransposeProject_Anti5(): Unit = {
    val sqlQuery = "SELECT d FROM (SELECT a, b, c, d FROM l, r) lr " +
      "WHERE a NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }
}

object ProjectSemiJoinTransposeRuleTest {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}
