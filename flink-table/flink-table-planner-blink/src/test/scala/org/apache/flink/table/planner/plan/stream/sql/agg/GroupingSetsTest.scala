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

import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil
import org.apache.flink.table.planner.utils.{TableTestBase, TableTestUtil}

import org.junit.Assert.assertEquals
import org.junit.Test

import java.sql.Date


class GroupingSetsTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
  util.addTableSource[(String, Int, String)]("emp", 'ename, 'deptno, 'gender)
  util.addTableSource[(Int, String)]("dept", 'deptno, 'dname)
  util.addTableSource[(Long, String, Int, String, String, Long, Int, Boolean, Boolean, Date)](
    "emps",
    'empno, 'name, 'deptno, 'gender, 'city, 'empid, 'age, 'slacker, 'manager, 'joinedat)
  util.addTableSource[(Int, String, String, Int, Date, Double, Double, Int)](
    "scott_emp", 'empno, 'ename, 'job, 'mgr, 'hiredate, 'sal, 'comm, 'deptno)

  @Test
  def testGroupingSets(): Unit = {
    val sqlQuery =
      """
        |SELECT b, c, AVG(a) AS a, GROUP_ID() AS g FROM MyTable
        |GROUP BY GROUPING SETS (b, c)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingSets2(): Unit = {
    util.verifyPlan("SELECT b, c, AVG(a) AS a FROM MyTable GROUP BY GROUPING SETS (b, c, ())")
  }

  @Test
  def testGroupingSets3(): Unit = {
    val sqlQuery =
      """
        |SELECT b, c,
        |    AVG(a) AS a,
        |    GROUP_ID() AS g,
        |    GROUPING(b) AS gb,
        |    GROUPING(c) AS gc,
        |    GROUPING_ID(b) AS gib,
        |    GROUPING_ID(c) AS gic,
        |    GROUPING_ID(b, c) AS gid,
        |    COUNT(*) AS cnt
        |FROM MyTable
        |     GROUP BY GROUPING SETS (b, c, ())
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCube(): Unit = {
    val sqlQuery =
      """
        |SELECT b, c,
        |    AVG(a) AS a,
        |    GROUP_ID() AS g,
        |    GROUPING(b) AS gb,
        |    GROUPING(c) AS gc,
        |    GROUPING_ID(b) AS gib,
        |    GROUPING_ID(c) AS gic,
        |    GROUPING_ID(b, c) AS gid
        |FROM MyTable
        |    GROUP BY CUBE (b, c)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRollup(): Unit = {
    val sqlQuery =
      """
        |SELECT b, c,
        |    AVG(a) AS a,
        |    GROUP_ID() AS g,
        |    GROUPING(b) AS gb,
        |    GROUPING(c) AS gc,
        |    GROUPING_ID(b) AS gib,
        |    GROUPING_ID(c) AS gic,
        |    GROUPING_ID(b, c) as gid
        |FROM MyTable
        |     GROUP BY ROLLUP (b, c)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingSetsWithOneGrouping(): Unit = {
    val sqlQuery =
      """
        |SELECT deptno,
        |    AVG(age) AS a,
        |    GROUP_ID() AS g,
        |    GROUPING(deptno) AS gb,
        |    GROUPING_ID(deptno) AS gib
        |FROM emps GROUP BY GROUPING SETS (deptno)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testBasicGroupingSets(): Unit = {
    util.verifyPlan("SELECT deptno, COUNT(*) AS c FROM emps GROUP BY GROUPING SETS ((), (deptno))")
  }

  @Test
  def testGroupingSetsOnExpression(): Unit = {
    val sqlQuery =
      """
        |SELECT deptno + 1, COUNT(*) AS c FROM emps GROUP BY GROUPING SETS ((), (deptno + 1))
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testSimpleCube(): Unit = {
    util.verifyPlan("SELECT deptno + 1, COUNT(*) AS c FROM emp GROUP BY CUBE(deptno, gender)")
  }

  @Test
  def testRollupOn1Column(): Unit = {
    util.verifyPlan("SELECT deptno + 1, COUNT(*) AS c FROM emp GROUP BY ROLLUP(deptno)")
  }

  @Test
  def testRollupOn2Column(): Unit = {
    val sqlQuery =
      """
        |SELECT gender, deptno + 1, COUNT(*) AS c FROM emp GROUP BY ROLLUP(deptno, gender)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRollupOnColumnWithNulls(): Unit = {
    // Note the two rows with NULL key (one represents ALL)
    // TODO check plan after null type supported
    util.verifyPlan("SELECT gender, COUNT(*) AS c FROM emp GROUP BY ROLLUP(gender)")
  }

  @Test
  def testRollupPlusOrderBy(): Unit = {
    util.verifyPlan("SELECT gender, COUNT(*) AS c FROM emp GROUP BY ROLLUP(gender) ORDER BY c DESC")
  }

  @Test
  def testRollupCartesianProduct(): Unit = {
    util.verifyPlan("SELECT deptno, COUNT(*) AS c FROM emp GROUP BY ROLLUP(deptno), ROLLUP(gender)")
  }

  @Test
  def testRollupCartesianProductOfWithTupleWithExpression(): Unit = {
    val sqlQuery =
      """
        |SELECT deptno / 2 + 1 AS half1, COUNT(*) AS c FROM emp
        |GROUP BY ROLLUP(deptno / 2, gender), ROLLUP(substring(ename FROM 1 FOR 1))
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRollupWithHaving(): Unit = {
    val sqlQuery =
      """
        |SELECT deptno + 1 AS d1, COUNT(*) AS c FROM emp GROUP BY ROLLUP(deptno) HAVING COUNT(*) > 3
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCubeAndDistinct(): Unit = {
    util.verifyPlan("SELECT DISTINCT COUNT(*) FROM emp GROUP BY CUBE(deptno, gender)")
  }

  @Test
  def testCubeAndJoin(): Unit = {
    val sqlQuery =
      """
        |SELECT e.deptno, e.gender,
        |    MIN(e.ename) AS min_name
        |FROM emp AS e JOIN dept AS d USING (deptno)
        |    GROUP BY CUBE(e.deptno, d.deptno, e.gender)
        |    HAVING COUNT(*) > 2 OR gender = 'M' AND e.deptno = 10
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingInSelectClauseOfGroupByQuery(): Unit = {
    util.verifyPlan("SELECT COUNT(*) AS c, GROUPING(deptno) AS g FROM emp GROUP BY deptno")
  }

  @Test
  def testGroupingInSelectClauseOfCubeQuery(): Unit = {
    val sqlQuery =
      """
        |SELECT deptno, job,
        |    COUNT(*) AS c,
        |    GROUPING(deptno) AS d,
        |    GROUPING(job) j,
        |    GROUPING(deptno, job) AS x
        |FROM scott_emp GROUP BY CUBE(deptno, job)
        |
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingGroup_idGrouping_idInSelectClauseOfGroupByQuery(): Unit = {
    val sqlQuery =
      """
        |SELECT COUNT(*) as c,
        |    GROUPING(deptno) AS g,
        |    GROUP_ID() AS gid,
        |    GROUPING_ID(deptno) AS gd,
        |    GROUPING_ID(gender) AS gg,
        |    GROUPING_ID(gender, deptno) AS ggd,
        |    GROUPING_ID(deptno, gender) AS gdg
        |FROM emp GROUP BY ROLLUP(deptno, gender)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingAcceptsMultipleArgumentsGivesSameResultAsGrouping_id(): Unit = {
    val sqlQuery =
      """
        |SELECT COUNT(*) AS c,
        |    GROUPING(deptno) AS gd,
        |    GROUPING_ID(deptno) AS gid,
        |    GROUPING(deptno, gender, deptno) AS gdgd,
        |    GROUPING_ID(deptno, gender, deptno) AS gidgd
        |FROM emp
        |    GROUP BY ROLLUP(deptno, gender)
        |    HAVING GROUPING(deptno) <= GROUPING_ID(deptno, gender, deptno)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingInOrderByClause(): Unit = {
    val sqlQuery =
      """
        |SELECT COUNT(*) AS c FROM emp GROUP BY ROLLUP(deptno) ORDER BY GROUPING(deptno), c
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testDuplicateArgumentToGrouping_id(): Unit = {
    val sqlQuery =
      """
        |SELECT deptno, gender,
        |    GROUPING_ID(deptno, gender, deptno),
        |    COUNT(*) AS c
        |FROM emp WHERE deptno = 10
        |    GROUP BY ROLLUP(gender, deptno)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingInSelectClauseOfRollupQuery(): Unit = {
    val sqlQuery =
      """
        |SELECT COUNT(*) AS c, deptno, GROUPING(deptno) AS g FROM emp GROUP BY ROLLUP(deptno)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testGroupingGrouping_idAndGroup_id(): Unit = {
    val sqlQuery =
      """
        |SELECT deptno, gender,
        |    GROUPING(deptno) gd,
        |    GROUPING(gender) gg,
        |    GROUPING_ID(deptno, gender) dg,
        |    GROUPING_ID(gender, deptno) gd,
        |    GROUP_ID() gid,
        |    COUNT(*) c
        |FROM emp
        |    GROUP BY CUBE(deptno, gender)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAllowExpressionInRollup1(): Unit = {
    val sqlQuery =
      """
        |SELECT deptno + 1 AS d1, deptno + 1 - 1 AS d0, COUNT(*) AS c
        |FROM emp GROUP BY ROLLUP (deptno + 1)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAllowExpressionInCube1(): Unit = {
    val sqlQuery =
      """
        |SELECT MOD(deptno, 20) AS d, COUNT(*) AS c, gender AS g
        |FROM emp GROUP BY CUBE(MOD(deptno, 20), gender)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAllowExpressionInRollup2(): Unit = {
    val sqlQuery =
      """
        |select MOD(deptno, 20) AS d, COUNT(*) AS c, gender AS g
        |FROM emp GROUP BY ROLLUP(MOD(deptno, 20), gender)
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testAllowExpressionInCube2(): Unit = {
    util.verifyPlan("SELECT COUNT(*) AS c FROM emp GROUP BY CUBE(1)")
  }

  @Test
  def testAllowExpressionInRollup3(): Unit = {
    util.verifyPlan("SELECT COUNT(*) AS c FROM emp GROUP BY ROLLUP(1)")
  }

  @Test
  def testCALCITE1824(): Unit = {
    val sqlQuery =
      """
        |SELECT deptno, GROUP_ID() AS g, COUNT(*) AS c
        |FROM scott_emp GROUP BY GROUPING SETS (deptno, (), ())
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFromBlogspot(): Unit = {
    // From http://rwijk.blogspot.com/2008/12/groupid.html
    val sqlQuery =
      """
        |SELECT deptno, job, empno, ename, SUM(sal) sumsal,
        |    CASE GROUPING_ID(deptno, job, empno)
        |    WHEN 0 THEN CAST('grouped by deptno,job,empno,ename' as varchar)
        |    WHEN 1 THEN CAST('grouped by deptno,job' as varchar)
        |    WHEN 3 THEN CAST('grouped by deptno' as varchar)
        |    WHEN 7 THEN CAST('grouped by ()' as varchar)
        |    END gr_text
        |from scott_emp
        |    GROUP BY ROLLUP(deptno, job, (empno,ename))
        |    ORDER BY deptno, job, empno
      """.stripMargin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testCubeAsGroupingSets(): Unit = {
    val cubeQuery =
      """
        |SELECT b, c,
        |    AVG(a) AS a,
        |    GROUP_ID() AS g,
        |    GROUPING(b) AS gb,
        |    GROUPING(c) AS gc,
        |    GROUPING_ID(b) AS gib,
        |    GROUPING_ID(c) as gic,
        |    GROUPING_ID(b, c) AS gid
        |FROM MyTable
        |    GROUP BY CUBE (b, c)
      """.stripMargin
    val groupingSetsQuery =
      """
        |SELECT b, c,
        |    AVG(a) AS a,
        |    GROUP_ID() AS g,
        |    GROUPING(b) AS gb,
        |    GROUPING(c) AS gc,
        |    GROUPING_ID(b) AS gib,
        |    GROUPING_ID(c) as gic,
        |    GROUPING_ID(b, c) AS gid
        |FROM MyTable
        |    GROUP BY GROUPING SETS ((b, c), (b), (c), ())
      """.stripMargin

    verifyPlanIdentical(cubeQuery, groupingSetsQuery)
    util.verifyPlan(cubeQuery)
  }

  @Test
  def testRollupAsGroupingSets(): Unit = {
    val rollupQuery =
      """
        |SELECT b, c,
        |    AVG(a) AS a,
        |    GROUP_ID() AS g,
        |    GROUPING(b) AS gb,
        |    GROUPING(c) AS gc,
        |    GROUPING_ID(b) AS gib,
        |    GROUPING_ID(c) as gic,
        |    GROUPING_ID(b, c) AS gid
        |FROM MyTable
        |    GROUP BY ROLLUP (b, c)
      """.stripMargin
    val groupingSetsQuery =
      """
        |SELECT b, c,
        |    AVG(a) AS a,
        |    GROUP_ID() AS g,
        |    GROUPING(b) AS gb,
        |    GROUPING(c) AS gc,
        |    GROUPING_ID(b) AS gib,
        |    GROUPING_ID(c) as gic,
        |    GROUPING_ID(b, c) AS gid
        |FROM MyTable
        |    GROUP BY GROUPING SETS ((b, c), (b), ())
      """.stripMargin

    verifyPlanIdentical(rollupQuery, groupingSetsQuery)
    util.verifyPlan(rollupQuery)
  }

  def verifyPlanIdentical(sql1: String, sql2: String): Unit = {
    val table1 = util.tableEnv.sqlQuery(sql1)
    val table2 = util.tableEnv.sqlQuery(sql2)
    val optimized1 = util.getPlanner.optimize(TableTestUtil.toRelNode(table1))
    val optimized2 = util.getPlanner.optimize(TableTestUtil.toRelNode(table2))
    assertEquals(FlinkRelOptUtil.toString(optimized1), FlinkRelOptUtil.toString(optimized2))
  }

}
