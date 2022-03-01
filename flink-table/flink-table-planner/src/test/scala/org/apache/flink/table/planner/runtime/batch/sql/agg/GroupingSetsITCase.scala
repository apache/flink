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

package org.apache.flink.table.planner.runtime.batch.sql.agg

import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.table.api.Types
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.BatchTestBase.row
import org.apache.flink.table.planner.runtime.utils.TestData._
import org.apache.flink.table.planner.utils.DateTimeTestUtil._

import org.junit.{Before, Test}

import scala.collection.Seq

class GroupingSetsITCase extends BatchTestBase {

  private val TABLE_NAME = "MyTable"
  private val TABLE_WITH_NULLS_NAME = "MyTableWithNulls"

  private val TABLE_NAME_EMPS = "emps"
  private val empsTypes = new RowTypeInfo(Types.LONG, Types.STRING, Types.INT, Types.STRING,
    Types.STRING, Types.LONG, Types.INT, Types.BOOLEAN, Types.BOOLEAN, Types.LOCAL_DATE)
  private val empsNames =
    "empno, name, deptno, gender, city, empid, age, slacker, manager, joinedat"
  private val nullableOfEmps: Array[Boolean] =
    Array(true, true, true, true, true, true, true, true, true, true)
  private lazy val empsData = Seq(
    row(100L, "Fred", 10, null, null, 40L, 25, true, false, localDate("1996-08-03")),
    row(110L, "Eric", 20, "M", "San Francisco", 3L, 80, null, false, localDate("2001-01-01")),
    row(110L, "John", 40, "M", "Vancouver", 2L, null, false, true, localDate("2002-05-03")),
    row(120L, "Wilma", 20, "F", null, 1L, 5, null, true, localDate("2005-09-07")),
    row(130L, "Alice", 40, "F", "Vancouver", 2L, null, false, true, localDate("2007-01-01"))
  )

  private val TABLE_NAME_EMP = "emp"
  private val empTypes = new RowTypeInfo(Types.STRING, Types.INT, Types.STRING)
  private val empNames = "ename, deptno, gender"
  private val nullableOfEmp = Array(true, true, true)
  private lazy val empData = Seq(
    row("Adam", 50, "M"),
    row("Alice", 30, "F"),
    row("Bob", 10, "M"),
    row("Eric", 20, "M"),
    row("Eve", 50, "F"),
    row("Grace", 60, "F"),
    row("Jane", 10, "F"),
    row("Susan", 30, "F"),
    row("Wilma", null, "F")
  )

  private val TABLE_NAME_DEPT = "dept"
  private val deptTypes = new RowTypeInfo(Types.INT, Types.STRING)
  private val deptNames = "deptno, dname"
  private val nullableOfDept = Array(true, true)
  private lazy val deptData = Seq(
    row(10, "Sales"),
    row(20, "Marketing"),
    row(30, "Engineering"),
    row(40, "Empty")
  )

  private val TABLE_NAME_SCOTT_EMP = "scott_emp"
  private val scottEmpTypes = new RowTypeInfo(Types.INT, Types.STRING, Types.STRING, Types.INT,
    Types.LOCAL_DATE, Types.DOUBLE, Types.DOUBLE, Types.INT)
  private val scottEmpNames = "empno, ename, job, mgr, hiredate, sal, comm, deptno"
  private val nullableOfScottEmp = Array(true, true, true, true, true, true, true, true)
  private lazy val scottEmpData = Seq(
    row(7369, "SMITH", "CLERK", 7902, localDate("1980-12-17"), 800.00, null, 20),
    row(7499, "ALLEN", "SALESMAN", 7698, localDate("1981-02-20"), 1600.00, 300.00, 30),
    row(7521, "WARD", "SALESMAN", 7698, localDate("1981-02-22"), 1250.00, 500.00, 30),
    row(7566, "JONES", "MANAGER", 7839, localDate("1981-02-04"), 2975.00, null, 20),
    row(7654, "MARTIN", "SALESMAN", 7698, localDate("1981-09-28"), 1250.00, 1400.00, 30),
    row(7698, "BLAKE", "MANAGER", 7839, localDate("1981-01-05"), 2850.00, null, 30),
    row(7782, "CLARK", "MANAGER", 7839, localDate("1981-06-09"), 2450.00, null, 10),
    row(7788, "SCOTT", "ANALYST", 7566, localDate("1987-04-19"), 3000.00, null, 20),
    row(7839, "KING", "PRESIDENT", null, localDate("1981-11-17"), 5000.00, null, 10),
    row(7844, "TURNER", "SALESMAN", 7698, localDate("1981-09-08"), 1500.00, 0.00, 30),
    row(7876, "ADAMS", "CLERK", 7788, localDate("1987-05-23"), 1100.00, null, 20),
    row(7900, "JAMES", "CLERK", 7698, localDate("1981-12-03"), 950.00, null, 30),
    row(7902, "FORD", "ANALYST", 7566, localDate("1981-12-03"), 3000.00, null, 20),
    row(7934, "MILLER", "CLERK", 7782, localDate("1982-01-23"), 1300.00, null, 10)
  )

  @Before
  override def before(): Unit = {
    super.before()
    registerCollection(TABLE_NAME, data3, type3, "f0, f1, f2", nullablesOfData3)
    val nullableData3 = data3.map { r =>
      val newField2 = if (r.getField(2).asInstanceOf[String].contains("world")) {
        null.asInstanceOf[String]
      } else {
        r.getField(2)
      }
      row(r.getField(0), r.getField(1), newField2)
    }
    val nullablesOfNullsData3 = Array(true, true, true)
    registerCollection(TABLE_WITH_NULLS_NAME, nullableData3, type3, "f0, f1, f2",
      nullablesOfNullsData3)

    registerCollection(TABLE_NAME_EMPS, empsData, empsTypes, empsNames, nullableOfEmps)
    registerCollection(TABLE_NAME_EMP, empData, empTypes, empNames, nullableOfEmp)
    registerCollection(TABLE_NAME_DEPT, deptData, deptTypes, deptNames, nullableOfDept)
    registerCollection(TABLE_NAME_SCOTT_EMP, scottEmpData, scottEmpTypes, scottEmpNames,
      nullableOfScottEmp)
  }

  @Test
  def testGroupingSetsWithOneGrouping(): Unit = {
    checkResult(
      "select deptno, avg(age) as a, group_id() as g," +
        " grouping(deptno) as gb, grouping_id(deptno)as gib" +
        " from emps group by grouping sets (deptno)",
      Seq(row(10, 25, 0, 0, 0), row(20, 42, 0, 0, 0), row(40, null, 0, 0, 0))
    )
  }

  @Test
  def testBasicGroupingSets(): Unit = {
    checkResult(
      "select deptno, count(*) as c from emps group by grouping sets ((), (deptno))",
      Seq(row(10, 1), row(20, 2), row(40, 2), row(null, 5))
    )
  }

  @Test
  def testGroupingSetsOnExpression(): Unit = {
    checkResult(
      "select deptno + 1, count(*) as c from emps group by grouping sets ((), (deptno + 1))",
      Seq(row(11, 1), row(21, 2), row(41, 2), row(null, 5))
    )
  }

  @Test
  def testBooleanColumnOnGroupingSets(): Unit = {
    checkResult(
      s"""
         |select
         |  gender, city, manager, count(*) as cnt
         |from emps group by grouping sets ((city), (gender, city, manager))
         |""".stripMargin,
      Seq(
        row("F", "Vancouver", true, 1),
        row("F", null, true, 1),
        row("M", "San Francisco", false, 1),
        row("M", "Vancouver", true, 1),
        row(null, "San Francisco", null, 1),
        row(null, "Vancouver", null, 2),
        row(null, null, false, 1),
        row(null, null, null, 2)))
  }

  @Test
  def testCoalesceOnGroupingSets(): Unit = {
    checkResult(
      s"""
         |select
         |  gender, city, coalesce(deptno, -1) as deptno, count(*) as cnt
         |from emps group by grouping sets ((gender, city), (gender, city, deptno))
         |""".stripMargin,
      Seq(
        row("F", "Vancouver", -1, 1),
        row("F", "Vancouver", 40, 1),
        row("F", null, -1, 1),
        row("F", null, 20, 1),
        row("M", "San Francisco", -1, 1),
        row("M", "San Francisco", 20, 1),
        row("M", "Vancouver", -1, 1),
        row("M", "Vancouver", 40, 1),
        row(null, null, -1, 1),
        row(null, null, 10, 1)))
  }

  @Test
  def testCube(): Unit = {
    checkResult(
      "select deptno + 1, count(*) as c from emp group by cube(deptno, gender)",
      Seq(row(11, 1), row(11, 1), row(11, 2), row(21, 1), row(21, 1), row(31, 2), row(31, 2),
        row(51, 1), row(51, 1), row(51, 2), row(61, 1), row(61, 1), row(null, 1), row(null, 1),
        row(null, 3), row(null, 6), row(null, 9))
    )
  }

  @Test
  def testRollupOn1Column(): Unit = {
    checkResult(
      "select deptno + 1, count(*) as c from emp group by rollup(deptno)",
      Seq(row(11, 2), row(21, 1), row(31, 2), row(51, 2), row(61, 1), row(null, 1), row(null, 9))
    )
  }

  @Test
  def testRollupOn2Column(): Unit = {
    checkResult(
      "select gender, deptno + 1, count(*) as c from emp group by rollup(deptno, gender)",
      Seq(row("M", 21, 1), row("F", 11, 1), row("F", 31, 2), row("F", 51, 1),
        row("F", 61, 1), row("F", null, 1), row("M", 11, 1), row("M", 51, 1),
        row(null, 11, 2), row(null, 21, 1), row(null, 31, 2), row(null, 51, 2),
        row(null, 61, 1), row(null, null, 1), row(null, null, 9))
    )
  }

  @Test
  def testRollupOnColumnWithNulls(): Unit = {
    //Note the two rows with NULL key (one represents ALL)
    checkResult(
      "select gender, count(*) as c from emp group by rollup(gender)",
      Seq(row("F", 6), row("M", 3), row(null, 9))
    )
  }

  @Test
  def testRollupPlusOrderBy(): Unit = {
    checkResult(
      "select gender, count(*) as c from emp group by rollup(gender) order by c desc",
      Seq(row(null, 9), row("F", 6), row("M", 3))
    )
  }

  @Test
  def testRollupCartesianProduct(): Unit = {
    checkResult(
      "select deptno, count(*) as c from emp group by rollup(deptno), rollup(gender)",
      Seq(row("10", 1), row("10", 1), row("20", 1), row("20", 1), row(null, 1), row("10", 2),
        row("30", 2), row("30", 2), row("50", 1), row("50", 1), row("50", 2), row("60", 1),
        row("60", 1), row(null, 1), row(null, 3), row(null, 6), row(null, 9))
    )
  }

  @Test
  def testRollupCartesianProductOfWithTupleWithExpression(): Unit = {
    checkResult(
      "select deptno / 2 + 1 as half1, count(*) as c from emp " +
        "group by rollup(deptno / 2, gender), rollup(substring(ename FROM 1 FOR 1))",
      Seq(row(11, 1), row(11, 1), row(11, 1), row(11, 1), row(16, 1), row(16, 1),
        row(16, 1), row(16, 1), row(16, 2), row(16, 2), row(26, 1), row(26, 1),
        row(26, 1), row(26, 1), row(26, 1), row(26, 1), row(26, 2), row(31, 1),
        row(31, 1), row(31, 1), row(31, 1), row(6, 1), row(6, 1), row(6, 1),
        row(6, 1), row(6, 1), row(6, 1), row(6, 2), row(null, 1), row(null, 1),
        row(null, 1), row(null, 1), row(null, 1), row(null, 1), row(null, 1),
        row(null, 1), row(null, 1), row(null, 2), row(null, 2), row(null, 9))
    )
  }

  @Test
  def testRollupWithHaving(): Unit = {
    checkResult(
      "select deptno + 1 as d1, count(*) as c from emp " +
        "group by rollup(deptno)having count(*) > 3",
      Seq(row(null, 9))
    )
  }

  @Test
  def testCubeAndDistinct(): Unit = {
    checkResult(
      "select distinct count(*) from emp group by cube(deptno, gender)",
      Seq(row(1), row(2), row(3), row(6), row(9))
    )
  }

  @Test
  def testCubeAndJoin(): Unit = {
    checkResult(
      "select e.deptno, e.gender, min(e.ename) as min_name " +
        "from emp as e join dept as d using (deptno) " +
        "group by cube(e.deptno, d.deptno, e.gender) " +
        "having count(*) > 2 or gender = 'M' and e.deptno = 10",
      Seq(row(10, "M", "Bob"), row(10, "M", "Bob"),
        row(null, "F", "Alice"), row(null, null, "Alice"))
    )
  }

  @Test
  def testGroupingInSelectClauseOfGroupByQuery(): Unit = {
    checkResult(
      "select count(*) as c, grouping(deptno) as g from emp group by deptno",
      Seq(row(1, 0), row(1, 0), row(1, 0), row(2, 0), row(2, 0), row(2, 0))
    )
  }

  @Test
  def testGroupingInSelectClauseOfCubeQuery(): Unit = {
    checkResult(
      "select deptno, job, count(*) as c, grouping(deptno) as d, grouping(job) j, " +
        "grouping(deptno, job) as x from scott_emp group by cube(deptno, job)",
      Seq(row(10, "CLERK", 1, 0, 0, 0),
        row(10, "MANAGER", 1, 0, 0, 0),
        row(10, "PRESIDENT", 1, 0, 0, 0),
        row(10, null, 3, 0, 1, 1),
        row(20, "ANALYST", 2, 0, 0, 0),
        row(20, "CLERK", 2, 0, 0, 0),
        row(20, "MANAGER", 1, 0, 0, 0),
        row(20, null, 5, 0, 1, 1),
        row(30, "CLERK", 1, 0, 0, 0),
        row(30, "MANAGER", 1, 0, 0, 0),
        row(30, "SALESMAN", 4, 0, 0, 0),
        row(30, null, 6, 0, 1, 1),
        row(null, "ANALYST", 2, 1, 0, 2),
        row(null, "CLERK", 4, 1, 0, 2),
        row(null, "MANAGER", 3, 1, 0, 2),
        row(null, "PRESIDENT", 1, 1, 0, 2),
        row(null, "SALESMAN", 4, 1, 0, 2),
        row(null, null, 14, 1, 1, 3))
    )
  }

  @Test
  def testGroupingGroup_idGrouping_idInSelectClauseOfGroupByQuery(): Unit = {
    checkResult(
      "select count(*) as c, grouping(deptno) as g, group_id() as gid, " +
        "grouping_id(deptno) as gd, grouping_id(gender) as gg, " +
        "grouping_id(gender, deptno) as ggd, grouping_id(deptno, gender) as gdg " +
        "from emp group by rollup(deptno, gender)",
      Seq(row(1, 0, 0, 0, 0, 0, 0), row(1, 0, 0, 0, 0, 0, 0), row(1, 0, 0, 0, 0, 0, 0),
        row(1, 0, 0, 0, 0, 0, 0), row(1, 0, 0, 0, 0, 0, 0), row(1, 0, 0, 0, 0, 0, 0),
        row(1, 0, 0, 0, 0, 0, 0), row(2, 0, 0, 0, 0, 0, 0), row(9, 1, 0, 1, 1, 3, 3),
        row(1, 0, 0, 0, 1, 2, 1), row(1, 0, 0, 0, 1, 2, 1), row(1, 0, 0, 0, 1, 2, 1),
        row(2, 0, 0, 0, 1, 2, 1), row(2, 0, 0, 0, 1, 2, 1), row(2, 0, 0, 0, 1, 2, 1))
    )
  }

  @Test
  def testGroupingAcceptsMultipleArgumentsGivesSameResultAsGrouping_id(): Unit = {
    checkResult(
      "select count(*) as c, grouping(deptno) as gd, " +
        "grouping_id(deptno) as gid, " +
        "grouping(deptno, gender, deptno) as gdgd, " +
        "grouping_id(deptno, gender, deptno) as gidgd " +
        "from emp group by rollup(deptno, gender) " +
        "having grouping(deptno) <= grouping_id(deptno, gender, deptno)",
      Seq(row(1, 0, 0, 0, 0), row(1, 0, 0, 0, 0), row(1, 0, 0, 0, 0),
        row(1, 0, 0, 0, 0), row(1, 0, 0, 0, 0), row(1, 0, 0, 0, 0),
        row(1, 0, 0, 0, 0), row(2, 0, 0, 0, 0), row(1, 0, 0, 2, 2),
        row(1, 0, 0, 2, 2), row(1, 0, 0, 2, 2), row(2, 0, 0, 2, 2),
        row(2, 0, 0, 2, 2), row(2, 0, 0, 2, 2), row(9, 1, 1, 7, 7))
    )
  }

  @Test
  def testGroupingInOrderByClause(): Unit = {
    checkResult(
      "select count(*) as c from emp group by rollup(deptno) order by grouping(deptno), c",
      Seq(row(1), row(1), row(1), row(2), row(2), row(2), row(9))
    )
  }

  @Test
  def testDuplicateArgumentToGrouping_id(): Unit = {
    checkResult(
      "select deptno, gender, grouping_id(deptno, gender, deptno), count(*) as c " +
        "from emp where deptno = 10 group by rollup(gender, deptno)",
      Seq(row(10, "F", 0, 1), row(10, "M", 0, 1), row(null, "F", 5, 1),
        row(null, "M", 5, 1), row(null, null, 7, 2))
    )
  }

  @Test
  def testGroupingInSelectClauseOfRollupQuery(): Unit = {
    checkResult(
      "select count(*) as c, deptno, grouping(deptno) as g from emp group by rollup(deptno)",
      Seq(row(1, 20, 0), row(1, 60, 0), row(1, null, 0), row(2, 10, 0),
        row(2, 30, 0), row(2, 50, 0), row(9, null, 1))
    )
  }

  @Test
  def testGroupingGrouping_idAndGroup_id(): Unit = {
    checkResult(
      "select deptno, gender, grouping(deptno) gd, grouping(gender) gg, " +
        "grouping_id(deptno, gender) dg, grouping_id(gender, deptno) gd, " +
        "group_id() gid, count(*) c from emp group by cube(deptno, gender)",
      Seq(row(10, "F", 0, 0, 0, 0, 0, 1), row(10, "M", 0, 0, 0, 0, 0, 1),
        row(20, "M", 0, 0, 0, 0, 0, 1), row(30, "F", 0, 0, 0, 0, 0, 2),
        row(50, "F", 0, 0, 0, 0, 0, 1), row(50, "M", 0, 0, 0, 0, 0, 1),
        row(60, "F", 0, 0, 0, 0, 0, 1), row(null, "F", 0, 0, 0, 0, 0, 1),
        row(null, null, 1, 1, 3, 3, 0, 9), row(10, null, 0, 1, 1, 2, 0, 2),
        row(20, null, 0, 1, 1, 2, 0, 1), row(30, null, 0, 1, 1, 2, 0, 2),
        row(50, null, 0, 1, 1, 2, 0, 2), row(60, null, 0, 1, 1, 2, 0, 1),
        row(null, "F", 1, 0, 2, 1, 0, 6), row(null, "M", 1, 0, 2, 1, 0, 3),
        row(null, null, 0, 1, 1, 2, 0, 1))
    )
  }

  @Test
  def testAllowExpressionInCubeAndRollup(): Unit = {
    checkResult(
      "select deptno + 1 as d1, deptno + 1 - 1 as d0, count(*) as c " +
        "from emp group by rollup (deptno + 1)",
      Seq(row(11, 10, 2), row(21, 20, 1), row(31, 30, 2), row(51, 50, 2),
        row(61, 60, 1), row(null, null, 1), row(null, null, 9))
    )

    checkResult(
      "select mod(deptno, 20) as d, count(*) as c, gender as g " +
        "from emp group by cube(mod(deptno, 20), gender)",
      Seq(row(0, 1, "F"), row(0, 1, "M"), row(0, 2, null), row(10, 2, "M"),
        row(10, 4, "F"), row(10, 6, null), row(null, 1, "F"), row(null, 1, null),
        row(null, 3, "M"), row(null, 6, "F"), row(null, 9, null))
    )

    checkResult(
      "select mod(deptno, 20) as d, count(*) as c, gender as g " +
        "from emp group by rollup(mod(deptno, 20), gender)",
      Seq(row(0, 1, "F"), row(0, 1, "M"), row(0, 2, null), row(10, 2, "M"), row(10, 4, "F"),
        row(10, 6, null), row(null, 1, "F"), row(null, 1, null), row(null, 9, null))
    )

    checkResult(
      "select count(*) as c from emp group by cube(1)",
      Seq(row(9), row(9))
    )

    checkResult(
      "select count(*) as c from emp group by cube(1)",
      Seq(row(9), row(9))
    )
  }

  @Test
  def testCALCITE1824(): Unit = {
    checkResult(
      "select deptno, group_id() as g, count(*) as c " +
        "from scott_emp group by grouping sets (deptno, (), ())",
      Seq(row(10, 0, 3),
        row(10, 1, 3),
        row(20, 0, 5),
        row(20, 1, 5),
        row(30, 0, 6),
        row(30, 1, 6),
        row(null, 0, 14))
    )
  }

  @Test
  def testFromBlogspot(): Unit = {
    // From http://rwijk.blogspot.com/2008/12/groupid.html
    checkResult(
      "select deptno, job, empno, ename, sum(sal) sumsal, " +
        "case grouping_id(deptno, job, empno)" +
        " when 0 then cast('grouped by deptno,job,empno,ename' as varchar)" +
        " when 1 then cast('grouped by deptno,job' as varchar)" +
        " when 3 then cast('grouped by deptno' as varchar)" +
        " when 7 then cast('grouped by ()' as varchar)" +
        "end gr_text " +
        "from scott_emp group by rollup(deptno, job, (empno,ename)) " +
        "order by deptno, job, empno",
      Seq(row(10, "CLERK", 7934, "MILLER", 1300.00, "grouped by deptno,job,empno,ename"),
        row(10, "CLERK", null, null, 1300.00, "grouped by deptno,job"),
        row(10, "MANAGER", 7782, "CLARK", 2450.00, "grouped by deptno,job,empno,ename"),
        row(10, "MANAGER", null, null, 2450.00, "grouped by deptno,job"),
        row(10, "PRESIDENT", 7839, "KING", 5000.00, "grouped by deptno,job,empno,ename"),
        row(10, "PRESIDENT", null, null, 5000.00, "grouped by deptno,job"),
        row(10, null, null, null, 8750.00, "grouped by deptno"),
        row(20, "ANALYST", 7788, "SCOTT", 3000.00, "grouped by deptno,job,empno,ename"),
        row(20, "ANALYST", 7902, "FORD", 3000.00, "grouped by deptno,job,empno,ename"),
        row(20, "ANALYST", null, null, 6000.00, "grouped by deptno,job"),
        row(20, "CLERK", 7369, "SMITH", 800.00, "grouped by deptno,job,empno,ename"),
        row(20, "CLERK", 7876, "ADAMS", 1100.00, "grouped by deptno,job,empno,ename"),
        row(20, "CLERK", null, null, 1900.00, "grouped by deptno,job"),
        row(20, "MANAGER", 7566, "JONES", 2975.00, "grouped by deptno,job,empno,ename"),
        row(20, "MANAGER", null, null, 2975.00, "grouped by deptno,job"),
        row(20, null, null, null, 10875.00, "grouped by deptno"),
        row(30, "CLERK", 7900, "JAMES", 950.00, "grouped by deptno,job,empno,ename"),
        row(30, "CLERK", null, null, 950.00, "grouped by deptno,job"),
        row(30, "MANAGER", 7698, "BLAKE", 2850.00, "grouped by deptno,job,empno,ename"),
        row(30, "MANAGER", null, null, 2850.00, "grouped by deptno,job"),
        row(30, "SALESMAN", 7499, "ALLEN", 1600.00, "grouped by deptno,job,empno,ename"),
        row(30, "SALESMAN", 7521, "WARD", 1250.00, "grouped by deptno,job,empno,ename"),
        row(30, "SALESMAN", 7654, "MARTIN", 1250.00, "grouped by deptno,job,empno,ename"),
        row(30, "SALESMAN", 7844, "TURNER", 1500.00, "grouped by deptno,job,empno,ename"),
        row(30, "SALESMAN", null, null, 5600.00, "grouped by deptno,job"),
        row(30, null, null, null, 9400.00, "grouped by deptno"),
        row(null, null, null, null, 29025.00, "grouped by ()"))
    )
  }

  @Test
  def testGroupingSets(): Unit = {
    val query =
      "SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, " +
        " GROUPING(f1) as gf1, GROUPING(f2) as gf2, " +
        " GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, " +
        " GROUPING_ID(f1, f2) as gid, " +
        " COUNT(*) as cnt" +
        " FROM " + TABLE_NAME + " GROUP BY " +
        " GROUPING SETS (f1, f2, ())"

    val expected = Seq(
      row(1, null, 1, 0, 0, 1, 0, 1, 1, 1),
      row(6, null, 18, 0, 0, 1, 0, 1, 1, 6),
      row(2, null, 2, 0, 0, 1, 0, 1, 1, 2),
      row(4, null, 8, 0, 0, 1, 0, 1, 1, 4),
      row(5, null, 13, 0, 0, 1, 0, 1, 1, 5),
      row(3, null, 5, 0, 0, 1, 0, 1, 1, 3),
      row(null, "Comment#11", 17, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#8", 14, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#2", 8, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#1", 7, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#14", 20, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#7", 13, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#6", 12, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#3", 9, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#12", 18, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#5", 11, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#15", 21, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#4", 10, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Hi", 1, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#10", 16, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Hello world", 3, 0, 1, 0, 1, 0, 2, 1),
      row(null, "I am fine.", 5, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Hello world, how are you?", 4, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#9", 15, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Comment#13", 19, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Luke Skywalker", 6, 0, 1, 0, 1, 0, 2, 1),
      row(null, "Hello", 2, 0, 1, 0, 1, 0, 2, 1),
      row(null, null, 11, 0, 1, 1, 1, 1, 3, 21))
    checkResult(query, expected)
  }

  @Test
  def testGroupingSetsWithNulls(): Unit = {
    val query = "SELECT f1, f2, avg(f0) as a, GROUP_ID() as g FROM " +
      TABLE_WITH_NULLS_NAME + " GROUP BY GROUPING SETS (f1, f2)"
    val expected = Seq(
      row(6, null, 18, 0),
      row(5, null, 13, 0),
      row(4, null, 8, 0),
      row(3, null, 5, 0),
      row(2, null, 2, 0),
      row(1, null, 1, 0),
      row(null, "Luke Skywalker", 6, 0),
      row(null, "I am fine.", 5, 0),
      row(null, "Hi", 1, 0),
      row(null, null, 3, 0),
      row(null, "Hello", 2, 0),
      row(null, "Comment#9", 15, 0),
      row(null, "Comment#8", 14, 0),
      row(null, "Comment#7", 13, 0),
      row(null, "Comment#6", 12, 0),
      row(null, "Comment#5", 11, 0),
      row(null, "Comment#4", 10, 0),
      row(null, "Comment#3", 9, 0),
      row(null, "Comment#2", 8, 0),
      row(null, "Comment#15", 21, 0),
      row(null, "Comment#14", 20, 0),
      row(null, "Comment#13", 19, 0),
      row(null, "Comment#12", 18, 0),
      row(null, "Comment#11", 17, 0),
      row(null, "Comment#10", 16, 0),
      row(null, "Comment#1", 7, 0))
    checkResult(query, expected)
  }

  @Test
  def testCubeAsGroupingSets(): Unit = {
    val cubeQuery = "SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, " +
      " GROUPING(f1) as gf1, GROUPING(f2) as gf2, " +
      " GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, " +
      " GROUPING_ID(f1, f2) as gid " + " FROM " +
      TABLE_NAME + " GROUP BY CUBE (f1, f2)"

    val groupingSetsQuery = "SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, " +
      " GROUPING(f1) as gf1, GROUPING(f2) as gf2, " +
      " GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, " +
      " GROUPING_ID(f1, f2) as gid " +
      " FROM " + TABLE_NAME + " GROUP BY GROUPING SETS ((f1, f2), (f1), (f2), ())"

    val expected = executeQuery(parseQuery(groupingSetsQuery))
    checkResult(cubeQuery, expected)
  }

  @Test
  def testRollupAsGroupingSets(): Unit = {
    val rollupQuery = "SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, " +
      " GROUPING(f1) as gf1, GROUPING(f2) as gf2, " +
      " GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, " +
      " GROUPING_ID(f1, f2) as gid " +
      " FROM " + TABLE_NAME + " GROUP BY ROLLUP (f1, f2)"

    val groupingSetsQuery = "SELECT f1, f2, avg(f0) as a, GROUP_ID() as g, " +
      " GROUPING(f1) as gf1, GROUPING(f2) as gf2, " +
      " GROUPING_ID(f1) as gif1, GROUPING_ID(f2) as gif2, " +
      " GROUPING_ID(f1, f2) as gid " +
      " FROM " + TABLE_NAME + " GROUP BY GROUPING SETS ((f1, f2), (f1), ())"

    val expected = executeQuery(parseQuery(groupingSetsQuery))
    checkResult(rollupQuery, expected)
  }

}
