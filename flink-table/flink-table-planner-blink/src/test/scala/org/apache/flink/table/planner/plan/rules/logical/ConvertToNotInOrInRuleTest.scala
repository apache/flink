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
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.plan.optimize.program._
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
  * Test for [[ConvertToNotInOrInRule]].
  */
class ConvertToNotInOrInRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(ConvertToNotInOrInRule.INSTANCE))
        .build())
    util.replaceBatchProgram(programs)

    util.addTableSource[(Int, Long, Float, Double, String)]("MyTable", 'a, 'b, 'c, 'd, 'e)
  }

  @Test
  def testConvertToIn_LessThanThreshold_Int(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a = 1 OR a = 2 OR a = 3")
  }

  @Test
  def testConvertToIn_EqualsToThreshold_Int(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a = 1 OR a = 2 OR a = 3 OR a = 4")
  }

  @Test
  def testConvertToIn_GreaterThanThreshold_Int(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a = 1 OR a = 2 OR a = 3 OR a = 4 OR a = 5")
  }

  @Test
  def testConvertToIn_LessThanThreshold_Double(): Unit = {
    val where = (1 until 20).map(i => s"d = $i").mkString(" OR ")
    util.verifyPlan(s"SELECT * FROM MyTable WHERE $where")
  }

  @Test
  def testConvertToIn_GreaterThanThreshold_Double(): Unit = {
    val where = (1 until 21).map(i => s"d = $i").mkString(" OR ")
    util.verifyPlan(s"SELECT * FROM MyTable WHERE $where")
  }

  @Test
  def testConvertToIn_WithOr1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a = 1 OR a = 2 OR a = 3 OR a = 4 OR b = 1")
  }

  @Test
  def testConvertToIn_WithOr2(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a = 1 OR a = 2 OR b = 1 OR a = 3 OR a = 4")
  }

  @Test
  def testConvertToIn_WithAnd1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE (a = 1 OR a = 2 OR a = 3 OR a = 4) AND b = 1")
  }

  @Test
  def testConvertToIn_WithAnd2(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a = 1 OR a = 2 OR a = 3 OR a = 4 AND b = 1")
  }

  @Test
  def testConvertToNotIn_LessThanThreshold_Int(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a <> 1 AND a <> 2 AND a <> 3")
  }

  @Test
  def testConvertToNotIn_EqualsToThreshold_Int(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a <> 1 AND a <> 2 AND a <> 3 AND a <> 4")
  }

  @Test
  def testConvertToNotIn_GreaterThanThreshold_Int(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a <> 1 AND a <> 2 AND a <> 3 AND a <> 4 AND a = 5")
  }

  @Test
  def testConvertToNotIn_LessThanThreshold_Double(): Unit = {
    val where = (1 until 20).map(i => s"d <> $i").mkString(" AND ")
    util.verifyPlan(s"SELECT * FROM MyTable WHERE $where")
  }

  @Test
  def testConvertToNotIn_GreaterThanThreshold_Double(): Unit = {
    val where = (1 until 21).map(i => s"d <> $i").mkString(" AND ")
    util.verifyPlan(s"SELECT * FROM MyTable WHERE $where")
  }

  @Test
  def testConvertToNotIn_WithOr1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE (a <> 1 AND a <> 2 AND a <> 3 AND a <> 4) OR b =" +
      " 1")
  }

  @Test
  def testConvertToNotIn_WithOr2(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a <> 1 AND a <> 2 AND a <> 3 AND a <> 4 OR b = 1")
  }

  @Test
  def testConvertToNotIn_WithOr3(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a <> 1 OR a <> 2 OR a <> 3 OR a <> 4 OR b = 1")
  }

  @Test
  def testConvertToNotIn_WithAnd1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a <> 1 AND a <> 2 AND a <> 3 AND a <> 4 AND b = 1")
  }

  @Test
  def testConvertToNotIn_WithAnd2(): Unit = {
    val sqlQuery = "SELECT * FROM MyTable WHERE a <> 1 AND a <> 2  AND b = 1 AND a <> 3 AND a <> 4"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testConvertToInAndNotIn1(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE a = 1 OR a = 2 OR a = 3 OR a = 4 OR b = 1 " +
      "OR (a <> 1 AND a <> 2 AND a <> 3 AND a <> 4)")
  }

  @Test
  def testConvertToInAndNotIn2(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE b = 1 OR a = 1 OR a = 2 OR a = 3 OR a = 4  " +
      "AND (a <> 1 AND a <> 2 AND a <> 3 AND a <> 4)")
  }

  @Test
  def testConvertToInAndNotIn3(): Unit = {
    util.verifyPlan("SELECT * FROM MyTable WHERE b = 1 OR b = 2 OR (a <> 1 AND a <> 2 AND a <> 3 " +
      "AND a <> 4 AND c = 1) OR b = 3 OR b = 4 OR c = 1")
  }
}
