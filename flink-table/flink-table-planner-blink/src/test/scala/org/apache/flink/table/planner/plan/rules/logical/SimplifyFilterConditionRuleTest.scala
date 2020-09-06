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
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkChainedProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.tools.RuleSets
import org.junit.{Before, Test}

/**
  * Tests for [[SimplifyFilterConditionRule]].
  */
class SimplifyFilterConditionRuleTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "FilterSimplifyExpressions",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(SimplifyFilterConditionRule.EXTENDED))
        .build()
    )
    util.replaceBatchProgram(programs)

    util.addTableSource[(Int, Long, String)]("x", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("y", 'd, 'e, 'f)
    util.addTableSource[(Int, Long, String)]("z", 'i, 'j, 'k)
  }

  @Test
  def testSimpleCondition(): Unit = {
    util.verifyPlan("SELECT * FROM x WHERE (a = 1 AND b = 2) OR (NOT(a <> 1) AND c = 3) AND true")
  }

  @Test
  def testSimplifyConditionInSubQuery1(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE EXISTS " +
      "(SELECT * FROM y WHERE (d = 1 AND e = 2) OR (NOT (d <> 1) AND e = 3)) AND true"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSimplifyConditionInSubQuery2(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE (a = 1 AND b = 2) OR (NOT (a <> 1) AND b = 3) " +
      "AND true AND EXISTS (SELECT * FROM y WHERE d > 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSimplifyConditionInSubQuery3(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE EXISTS " +
      "(SELECT * FROM y WHERE d IN " +
      "(SELECT i FROM z WHERE (i = 1 AND j = 2) OR (NOT (i <> 1) AND j = 3) AND true) AND e > 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testComplexCondition1(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE " +
      "(a = 1 AND b = 2) OR (NOT(a <> 1) AND c = 3) AND true AND EXISTS " +
      "(SELECT * FROM y WHERE x.a = y.d AND 2=2 AND " +
      "(SELECT count(*) FROM z WHERE i = 5 AND j = 6) > 0)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testComplexCondition2(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE " +
      "(a = 1 AND b = 2) OR (NOT(a <> 1) AND c = 3) AND true AND EXISTS " +
      "(SELECT * FROM y WHERE x.a = y.d AND " +
      "(SELECT count(*) FROM z WHERE (i = 5 AND j = 6) OR (NOT (i <> 5) AND j = 7) AND true) > 0)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testComplexCondition3(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE " +
      "(a = 1 AND b = 2) OR (NOT(a <> 1) AND c = 3) AND true AND EXISTS " +
      "(SELECT * FROM y WHERE x.a = y.d AND 2=2 AND " +
      "(SELECT count(*) FROM z WHERE (i = 5 AND j = 6) OR (NOT (i <> 5) AND j = 7) AND true) > 0)"
    util.verifyPlan(sqlQuery)
  }

}
