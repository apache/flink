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

import org.apache.flink.table.api._
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkChainedProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.apache.calcite.rel.rules._
import org.apache.calcite.tools.RuleSets
import org.junit.jupiter.api.{BeforeEach, Test}

import java.sql.Timestamp

/**
 * Tests for [[ProjectJoinJoinRemoveRule]], [[ProjectJoinRemoveRule]],
 * [[AggregateJoinJoinRemoveRule]]， [[AggregateJoinRemoveRule]].
 */
class JoinRemoveRulesTest extends TableTestBase {

  private val util = batchTestUtil()

  @BeforeEach
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "rules",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(RuleSets.ofList(
          CoreRules.AGGREGATE_PROJECT_MERGE,
          CoreRules.PROJECT_JOIN_JOIN_REMOVE,
          CoreRules.PROJECT_JOIN_REMOVE,
          CoreRules.AGGREGATE_JOIN_JOIN_REMOVE,
          CoreRules.AGGREGATE_JOIN_REMOVE
        ))
        .build()
    )
    util.replaceBatchProgram(programs)

    util.addTableSource[(Int, String, String, Int, Timestamp, Int, Int, Int, Boolean)](
      "emp",
      'empno,
      'ename,
      'job,
      'mgr,
      'hiredate,
      'sal,
      'comm,
      'deptno,
      'slacker)
    util.addTableSource[(Int, String)]("dept", 'deptno, 'name)
  }

  @Test
  def testAggregateJoinRemoveRule1(): Unit = {
    val sqlQuery =
      s"""
         |select count(distinct sal) from emp e
         |left outer join dept d1 on e.job = d1.name
         |left outer join dept d2 on e.job = d2.name
         |group by e.job
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateJoinRemoveRule2(): Unit = {
    val sqlQuery =
      s"""
         |select count(distinct sal) from emp e
         |left outer join dept d1 on e.job = d1.name
         |left outer join dept d2 on e.job = d2.name
         |group by e.job
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateJoinRemove1(): Unit = {
    val sqlQuery =
      s"""
         |select distinct e.deptno from emp e
         |left outer join dept d on e.deptno = d.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateJoinRemove2(): Unit = {
    val sqlQuery =
      s"""
         |select e.deptno, count(distinct e.job) from emp e
         |left outer join dept d on e.deptno = d.deptno
         |group by e.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateJoinRemove3(): Unit = {
    val sqlQuery =
      s"""
         |select e.deptno, count(distinct d.name) from emp e
         |left outer join dept d on e.deptno = d.deptno
         |group by e.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateJoinRemove4(): Unit = {
    val sqlQuery =
      s"""
         |select distinct d.deptno from emp e
         |right outer join dept d on e.deptno = d.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateJoinRemove5(): Unit = {
    val sqlQuery =
      s"""
         |select d.deptno, count(distinct d.name) from emp e
         |right outer join dept d on e.deptno = d.deptno
         |group by d.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateJoinRemove6(): Unit = {
    val sqlQuery =
      s"""
         |select d.deptno, count(distinct e.job) from emp e
         |right outer join dept d on e.deptno = d.deptno
         |group by d.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateJoinRemove7(): Unit = {
    val sqlQuery =
      s"""
         |SELECT distinct e.deptno
         |FROM emp e
         |LEFT JOIN dept d1 ON e.deptno = d1.deptno
         |LEFT JOIN dept d2 ON e.deptno = d2.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateJoinRemove8(): Unit = {
    val sqlQuery =
      s"""
         |SELECT e.deptno, COUNT(DISTINCT d2.name)
         |FROM emp e
         |LEFT JOIN dept d1 ON e.deptno = d1.deptno
         |LEFT JOIN dept d2 ON e.deptno = d2.deptno
         |GROUP BY e.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateJoinRemove9(): Unit = {
    val sqlQuery =
      s"""
         |SELECT distinct e.deptno, d2.name
         |FROM emp e
         |LEFT JOIN dept d1 ON e.deptno = d1.deptno
         |LEFT JOIN dept d2 ON e.deptno = d2.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testAggregateJoinRemove10(): Unit = {
    val sqlQuery =
      s"""
         |SELECT e.deptno, COUNT(DISTINCT d1.name, d2.name)
         |FROM emp e
         |LEFT JOIN dept d1 ON e.deptno = d1.deptno
         |LEFT JOIN dept d2 ON e.deptno = d2.deptno
         |GROUP BY e.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testProjectJoinRemove1(): Unit = {
    val sqlQuery =
      s"""
         |SELECT e.deptno, d2.deptno
         |FROM emp e
         |LEFT JOIN dept d1 ON e.deptno = d1.deptno
         |LEFT JOIN dept d2 ON e.deptno = d2.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testProjectJoinRemove2(): Unit = {
    val sqlQuery =
      s"""
         |SELECT e.deptno, d1.deptno
         |FROM emp e
         |LEFT JOIN dept d1 ON e.deptno = d1.deptno
         |LEFT JOIN dept d2 ON e.deptno = d2.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testProjectJoinRemove3(): Unit = {
    val sqlQuery =
      s"""
         |SELECT e1.deptno, d.deptno
         |FROM emp e1
         |LEFT JOIN emp e2 ON e1.deptno = e2.deptno
         |LEFT JOIN dept d ON e1.deptno = d.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testProjectJoinRemove4(): Unit = {
    val sqlQuery =
      s"""
         |SELECT e.deptno
         |FROM emp e
         |LEFT JOIN dept d ON e.deptno = d.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testProjectJoinRemove5(): Unit = {
    val sqlQuery =
      s"""
         |SELECT e1.deptno
         |FROM emp e1
         |LEFT JOIN emp e2 ON e1.deptno = e2.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testProjectJoinRemove6(): Unit = {
    val sqlQuery =
      s"""
         |SELECT e.deptno, d.name
         |FROM emp e
         |LEFT JOIN dept d ON e.deptno = d.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testProjectJoinRemove7(): Unit = {
    val sqlQuery =
      s"""
         |SELECT e.deptno
         |FROM dept d
         |RIGHT JOIN emp e ON e.deptno = d.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testProjectJoinRemove8(): Unit = {
    val sqlQuery =
      s"""
         |SELECT e2.deptno
         |FROM emp e1
         |LEFT JOIN emp e2 ON e1.deptno = e2.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testProjectJoinRemove9(): Unit = {
    val sqlQuery =
      s"""
         |SELECT e.deptno, d.name
         |FROM dept d
         |RIGHT JOIN emp e ON e.deptno = d.deptno
       """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }
}
