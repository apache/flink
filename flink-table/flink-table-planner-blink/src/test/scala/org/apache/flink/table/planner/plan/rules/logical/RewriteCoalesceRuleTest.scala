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
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.codegen.CodeGenException
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkChainedProgram, FlinkHepRuleSetProgramBuilder, HEP_RULES_EXECUTION_TYPE}
import org.apache.flink.table.planner.plan.rules.FlinkBatchRuleSets
import org.apache.flink.table.planner.utils.TableTestBase

import org.apache.calcite.plan.hep.HepMatchOrder
import org.junit.{Before, Test}

import java.sql.Date

/**
  * Test for [[RewriteCoalesceRule]].
  */
class RewriteCoalesceRuleTest extends TableTestBase {
  private val util = batchTestUtil()

  @Before
  def setup(): Unit = {
    val programs = new FlinkChainedProgram[BatchOptimizeContext]()
    programs.addLast(
      "default_rewrite",
      FlinkHepRuleSetProgramBuilder.newBuilder
        .setHepRulesExecutionType(HEP_RULES_EXECUTION_TYPE.RULE_SEQUENCE)
        .setHepMatchOrder(HepMatchOrder.BOTTOM_UP)
        .add(FlinkBatchRuleSets.DEFAULT_REWRITE_RULES)
        .build())
    util.replaceBatchProgram(programs)

    util.addTableSource[(Int, String, String, Int, Date, Double, Double, Int)](
      "scott_emp", 'empno, 'ename, 'job, 'mgr, 'hiredate, 'sal, 'comm, 'deptno)
    util.addTableSource[(Int, String, String)]("scott_dept", 'deptno, 'dname, 'loc)
  }

  @Test
  def testCalcite1018(): Unit = {
    val sqlQuery =
      """
        |select * from (select * from scott_emp) e left join (
        |    select * from scott_dept d) using (deptno)
        |    order by empno limit 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCoalesceConstantReduce(): Unit = {
    val sqlQuery =
      """
        |select * from lateral (select * from scott_emp) as e
        |    join (table scott_dept) using (deptno)
        |    where e.deptno = 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[CodeGenException])
  // TODO remove expected exception after [FLINK-12371] merged
  def testCalcite864_1(): Unit = {
    val sqlQuery =
      """
        |select *
        |    from scott_emp as e
        |    join scott_dept as d using (deptno)
        |    where sal = (
        |      select max(sal)
        |      from scott_emp as e2
        |      join scott_dept as d2 using (deptno)
        |      where d2.deptno = d.deptno)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[CodeGenException])
  // TODO remove expected exception after [FLINK-12371] merged
  def testCalcite864_3(): Unit = {
    val sqlQuery =
      """
        |select *
        |    from scott_emp as e
        |    join scott_dept as d using (deptno)
        |    where d.dname = (
        |      select max(dname)
        |      from scott_dept as d2
        |      where d2.deptno = d.deptno)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNaturalJoinWithPredicates(): Unit = {
    val sqlQuery =
      """
        |select * from scott_dept natural join scott_emp where empno = 1
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNaturalJoinLeftOuter(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM scott_dept
        |    natural left join scott_emp
        |    order by scott_dept.deptno, scott_emp.deptno
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNaturalJoinRightOuter(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM scott_dept
        |    natural right join scott_emp
        |    order by scott_dept.deptno, scott_emp.deptno
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }
}
