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

package org.apache.flink.table.planner.plan.rules.logical.subquery

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.StringSplit

import org.junit.Test

/**
  * Tests for [[org.apache.flink.table.planner.plan.rules.logical.FlinkSubQueryRemoveRule]],
  * this class only tests IN and EXISTS queries.
  */
class SubQuerySemiJoinTest extends SubQueryTestBase {
  util.addTableSource[(Int, Long, String)]("l", 'a, 'b, 'c)
  util.addTableSource[(Int, Long, String)]("r", 'd, 'e, 'f)
  util.addTableSource[(Int, Long, String)]("t", 'i, 'j, 'k)
  util.addTableSource[(Int, Long)]("x", 'a, 'b)
  util.addTableSource[(Int, Long)]("y", 'c, 'd)
  util.addTableSource[(Int, Long)]("z", 'e, 'f)
  util.addFunction("table_func", new StringSplit)

  @Test
  def testInOnWhere_NotSubQuery(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE a IN (1, 2, 3, 4)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithUncorrelatedOnWhere1(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a IN (SELECT c FROM y)")
  }

  @Test
  def testInWithUncorrelatedOnWhere2(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a IN (SELECT c FROM y WHERE d < 100) AND b > 10")
  }

  @Test
  def testInWithUncorrelatedOnWhere3(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a + 1 IN (SELECT c FROM y)")
  }

  @Test
  def testInWithUncorrelatedOnWhere4(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a * b IN (SELECT d FROM y)")
  }

  @Test
  def testInWithUncorrelatedOnWhere5(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE CAST(a AS BIGINT) IN (SELECT d FROM y)")
  }

  @Test
  def testInWithUncorrelatedOnWhere6(): Unit = {
    util.verifyRelPlan("SELECT a FROM x x1 WHERE a IN (SELECT a FROM x WHERE a < 3 GROUP BY a)")
  }

  @Test
  def testInWithUncorrelatedOnWhere_ComplexCondition1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' AND a IN (SELECT d FROM r))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithUncorrelatedOnWhere_ComplexCondition2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' OR a NOT IN (SELECT d FROM r))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ComplexCondition3(): Unit = {
    // join
    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(SELECT d FROM r FULL JOIN (SELECT j FROM t WHERE i > 10) t2 ON r.e = t2.j)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ComplexCondition4(): Unit = {
    // aggregate
    val sqlQuery = "SELECT a FROM l WHERE b IN (SELECT MAX(e) FROM r WHERE d < 3 GROUP BY f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ComplexCondition5(): Unit = {
    // union
    val sqlQuery = "SELECT a FROM l WHERE b IN " +
      "(SELECT e FROM r WHERE d > 10 UNION SELECT i FROM t WHERE i < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ComplexCondition6(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (a IN (SELECT d FROM y)) IS TRUE")
  }

  @Test
  def testInWithUncorrelatedOnWhere_UnsupportedCondition1(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    // these queries will not be converted to joinType=[semi]
    val sqlQuery = "SELECT * FROM x WHERE a IN (SELECT c FROM y WHERE x.b IN (SELECT e FROM z))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithUncorrelatedOnWhere_UnsupportedCondition2(): Unit = {
    val sqlQuery1 = "SELECT * FROM x WHERE a IN (SELECT c FROM y) OR b > 10"
    util.verifyRelPlanNotExpected(sqlQuery1, "joinType=[semi]")

    val sqlQuery2 = "SELECT * FROM x, y WHERE x.a = y.c AND (y.d IN (SELECT d FROM y) OR x.a >= 1)"
    util.verifyRelPlanNotExpected(sqlQuery2, "joinType=[semi]")

    val sqlQuery3 = "SELECT * FROM x WHERE a IN (SELECT x.b FROM r)"
    util.verifyRelPlanNotExpected(sqlQuery3, "joinType=[semi]")
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case1(): Unit = {
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a > 10 THEN 1 ELSE 2 END) IN (SELECT d FROM r)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case2(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT 1 FROM t1) THEN 1 ELSE 2 END) IN (SELECT d FROM r)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case3(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT i FROM t1) THEN 1 ELSE 2 END) IN (SELECT d FROM r)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case4(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)
    util.addTableSource[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT i FROM t1) THEN 1 WHEN a IN (SELECT j FROM t2) THEN 2 ELSE 3 END)" +
      " IN (SELECT d FROM r)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case5(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)
    util.addTableSource[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE (b, " +
      "(CASE WHEN a IN (SELECT i FROM t1) THEN 1 WHEN a IN (SELECT j FROM t2) THEN 2 ELSE 3 END))" +
      " IN (SELECT e, d FROM r)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case6(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)
    util.addTableSource[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT c FROM l WHERE (" +
      " (CASE WHEN a IN (SELECT i FROM t1) THEN 1 ELSE 2 END), " +
      " (CASE WHEN b IN (SELECT j FROM t2) THEN 3 ELSE 4 END)) " +
      " IN (SELECT d, e FROM r)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case7(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)
    util.addTableSource[(Int)]("t2", 'j)

    // TODO some bugs in SubQueryRemoveRule
    thrown.expect(classOf[RuntimeException])

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT i FROM t1 WHERE l.a = t1.i) THEN 1 ELSE 2 END) IN (SELECT d FROM r)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithUncorrelatedOnWhere_ScalarQuery1(): Unit = {
    val sqlQuery = "SELECT a FROM x WHERE (SELECT MAX(d) FROM y WHERE c > 0) IN (SELECT f FROM z)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ScalarQuery2(): Unit = {
    // TODO convert IN in scalar_query to joinType=[semi] ???
    val sqlQuery = "SELECT a FROM x WHERE " +
      "(SELECT MAX(d) FROM y WHERE c IN (SELECT e FROM z)) IN (SELECT f FROM z)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ScalarQuery3(): Unit = {
    val sqlQuery = "SELECT b FROM x WHERE a IN (SELECT c FROM y WHERE d > 10) " +
      "AND b > (SELECT 0.5 * SUM(e) FROM z WHERE z.f < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ScalarQuery4(): Unit = {
    val sqlQuery = "SELECT b FROM x WHERE a IN (SELECT c FROM y WHERE d > 10) " +
      "AND b > (SELECT 0.5 * SUM(e) FROM z WHERE x.a = z.e AND z.f < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ScalarQuery5(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    val sqlQuery = "SELECT b FROM x WHERE a IN (SELECT c FROM y WHERE d > " +
      "(SELECT 0.5 * SUM(e) FROM z WHERE x.a = z.e AND z.f < 100))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithUncorrelatedOnWhere_ScalarQuery6(): Unit = {
    val sqlQuery = "SELECT b FROM x WHERE a IN (SELECT c FROM y WHERE d > " +
      "(SELECT SUM(e) FROM z WHERE y.c = z.e AND z.f < 100))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Aggregate1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT MAX(r.e) FROM r WHERE r.d < 3 GROUP BY r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Aggregate2(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM l WHERE a IN (SELECT d * 5 FROM (SELECT SUM(d) AS d FROM r) r1)")
  }

  @Test
  def testInWithUncorrelatedOnWhere_Aggregate3(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE b IN (SELECT COUNT(*) FROM r)")
  }

  @Test
  def testInWithUncorrelatedOnWhere_Aggregate4(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE (a, b) IN (SELECT d, COUNT(*) FROM r GROUP BY d)")
  }

  @Test
  def testInWithUncorrelatedOnWhere_Aggregate5(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE b IN (SELECT MAX(e) FROM r GROUP BY d, true, 1)")
  }

  @Test
  def testInWithUncorrelatedOnWhere_Aggregate6(): Unit = {
    val sqlQuery =
      "SELECT * FROM l WHERE (b, a) IN (SELECT COUNT(*), d FROM r GROUP BY d, true, e, 1)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Over1(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE b IN (SELECT MAX(r.e) OVER() FROM r)")
  }

  @Test
  def testInWithUncorrelatedOnWhere_Over2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (a, b) IN " +
      "(SELECT MAX(r.d) OVER(), MIN(r.e) OVER(PARTITION BY f ORDER BY d) FROM r)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_AggregateOver(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE b IN (SELECT MAX(r.e) OVER() FROM r GROUP BY r.e)")
  }

  @Test
  def testInWithUncorrelatedOnWhere_MultiFields1(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE (a, c) IN (SELECT d, f FROM r)")
  }

  @Test
  def testInWithUncorrelatedOnWhere_MultiFields2(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE (a + 1, c) IN (SELECT d, f FROM r)")
  }

  @Test
  def testInWithUncorrelatedOnWhere_MultiFields3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "(a + 10, SUBSTRING(c, 1, 5)) IN (SELECT d + 100, SUBSTRING(f, 1, 5) FROM r)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelatedOnWhere1(): Unit = {
    // multi IN in AND condition
    util.verifyRelPlan("SELECT * FROM x WHERE a IN ( SELECT c FROM y) AND b IN (SELECT e FROM z)")
  }

  @Test
  def testMultiInWithUncorrelatedOnWhere2(): Unit = {
    // nested IN
    util.verifyRelPlan("SELECT * FROM x WHERE a IN (SELECT c FROM y WHERE d IN (SELECT f FROM z))")
  }

  @Test
  def testMultiInWithUncorrelatedOnWhere3(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE " +
      "a + 1 IN ( SELECT c FROM y WHERE d > 10) AND b * 2 IN (SELECT e FROM z WHERE f < 10)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelatedOnWhere_OR(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE a IN (SELECT c FROM y) OR b IN (SELECT e FROM z)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithUncorrelatedOnLateralTable1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnLateralTable2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE d IN (SELECT i FROM t))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnLateralTable3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM (SELECT * FROM r WHERE d IN (" +
      "SELECT i FROM t)) m, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnLateralTable4(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM (SELECT * FROM r LEFT JOIN LATERAL TABLE(table_func(f)) AS T(f1) ON TRUE))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere1(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a IN (SELECT c FROM y WHERE x.b = y.d)")
  }

  @Test
  def testInWithCorrelatedOnWhere2(): Unit = {
    val sqlQuery =
      "SELECT * FROM x WHERE b > 1 AND a IN (SELECT c FROM y WHERE x.b = y.d AND y.c > 10)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere3(): Unit = {
    val sqlQuery =
      "SELECT * FROM x WHERE b > 1 AND a + 1 IN (SELECT c FROM y WHERE x.b = y.d AND y.c > 10)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere4(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a IN (SELECT c FROM y WHERE x.b > y.d)")
  }

  @Test
  def testInWithCorrelatedOnWhere5(): Unit = {
    val sqlQuery =
      "SELECT a FROM x x1 WHERE a IN (SELECT a FROM x WHERE a < 3 AND x1.b = x.b GROUP BY a)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere6(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM x WHERE a IN (SELECT c FROM y where CAST(x.b AS INTEGER) = y.c)")
  }

  @Test
  def testInWithCorrelatedOnWhere7(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a IN (SELECT c FROM y WHERE x.b > 10)")
  }

  @Test
  def testInWithCorrelatedOnWhere_ComplexCondition1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE a IN " +
      "(SELECT CAST(e AS INTEGER) FROM r where CAST(l.b AS INTEGER) = CAST(r.d AS INTEGER))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ComplexCondition2(): Unit = {
    val sqlQuery = "SELECT a + 10, c FROM l WHERE " +
      "NOT(NOT(substring(c, 1, 5) IN (SELECT substring(f, 1, 5) FROM r WHERE l.b + 1 = r.e)))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ComplexCondition3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' AND a IN (SELECT d FROM r where l.b = r.e))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_ComplexCondition4(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' OR a NOT IN (SELECT d FROM r WHERE l.b = r.e))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ComplexCondition5(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (a IN (SELECT d FROM y WHERE y.d = x.b)) IS TRUE")
  }

  @Test
  def testInWithCorrelatedOnWhere_UnsupportedCondition1(): Unit = {
    // this query will not be converted to joinType=[semi]
    val sqlQuery1 = "SELECT * FROM x WHERE b IN (SELECT d FROM y WHERE x.a = y.c OR y.c = 10)"
    util.verifyRelPlanNotExpected(sqlQuery1, "joinType=[semi]")

    val sqlQuery2 = "SELECT * FROM x WHERE b IN (SELECT d FROM y WHERE x.a = y.c OR x.b = 10)"
    util.verifyRelPlanNotExpected(sqlQuery2, "joinType=[semi]")

    val sqlQuery3 = "SELECT * FROM x WHERE b IN (SELECT d FROM y WHERE x.a = y.c) OR x.a = 10"
    util.verifyRelPlanNotExpected(sqlQuery3, "joinType=[semi]")

    val sqlQuery4 = "SELECT * FROM x WHERE a IN (SELECT x.a FROM y WHERE y.d = x.b)"
    util.verifyRelPlanNotExpected(sqlQuery4, "joinType=[semi]")
  }

  @Test(expected = classOf[AssertionError])
  def testInWithCorrelatedOnWhere_UnsupportedCondition2(): Unit = {
    // TODO java.lang.RuntimeException: While invoking method
    // 'public RelDecorrelator$Frame RelDecorrelator.decorrelateRel(LogicalProject)'
    val sqlQuery = "SELECT * FROM l WHERE a IN (SELECT d FROM r WHERE l.b IN (SELECT j FROM t) " +
      "AND l.c = r.f)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_Case1(): Unit = {
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a > 10 THEN 1 ELSE 2 END) IN (SELECT d FROM r WHERE l.a = r.d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Case2(): Unit = {
    util.addTableSource[(Int, Long)]("t1", 'i, 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT 1 FROM t1) THEN 1 ELSE 2 END) IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Case3(): Unit = {
    util.addTableSource[(Int, Long)]("t1", 'i, 'j)

    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT i FROM t1) THEN 1 ELSE 2 END) IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Case4(): Unit = {
    util.addTableSource[(Int, Long)]("t1", 'i, 'j)
    util.addTableSource[(Int, Long)]("t2", 'i, 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT i FROM t1) THEN 1 WHEN a IN (SELECT j FROM t2) THEN 2 ELSE 3 END)" +
      " IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Case5(): Unit = {
    util.addTableSource[(Int, Long)]("t1", 'i, 'j)
    util.addTableSource[(Int, Long)]("t2", 'i, 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE (b, " +
      "(CASE WHEN a IN (SELECT i FROM t1) THEN 1 WHEN a IN (SELECT j FROM t2) THEN 2 ELSE 3 END))" +
      " IN (SELECT e, d FROM r WHERE l.c = r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Case6(): Unit = {
    util.addTableSource[(Int, Long)]("t1", 'i, 'j)
    util.addTableSource[(Int, Long)]("t2", 'i, 'j)

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT b FROM l WHERE (CASE WHEN a IN (SELECT i FROM t1 WHERE l.b = t1.j) " +
      "THEN 1 ELSE 2 END) IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery1(): Unit = {
    val sqlQuery = "SELECT a FROM x WHERE " +
      "(SELECT MAX(d) FROM y WHERE c > 0) IN (SELECT f FROM z WHERE z.e = x.a)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery2(): Unit = {
    // TODO convert IN in scalar_query to joinType=[semi] ???
    val sqlQuery = "SELECT a FROM x WHERE " +
      "(SELECT MAX(d) FROM y WHERE c IN (SELECT e FROM z)) IN (SELECT f FROM z WHERE z.e = x.a)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery3(): Unit = {
    // TODO convert IN in scalar_query to joinType=[semi] ???
    val sqlQuery = "SELECT a FROM x WHERE " +
      "(SELECT MAX(d) FROM y WHERE c IN (SELECT e FROM z WHERE y.d = z.f))" +
      " IN (SELECT f FROM z WHERE z.e = x.a)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery4(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    // nested correlation can not be converted joinType=[semi] now
    val sqlQuery = "SELECT a FROM x WHERE " +
      "(SELECT MAX(d) FROM y WHERE c IN (SELECT e FROM z WHERE x.b = z.f))" +
      " IN (SELECT f FROM z WHERE z.e = x.a)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery5(): Unit = {
    val sqlQuery = "SELECT b FROM x WHERE a IN (SELECT c FROM y WHERE x.b = y.d AND c > 10) " +
      "AND b > (SELECT 0.5 * SUM(e) FROM z WHERE z.f < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery6(): Unit = {
    val sqlQuery = "SELECT b FROM x WHERE a IN (SELECT c FROM y WHERE x.b = y.d AND c > 10) " +
      "AND b > (SELECT 0.5 * SUM(e) FROM z WHERE x.a = z.e AND z.f < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery7(): Unit = {
    val sqlQuery = "SELECT b FROM x WHERE a IN (SELECT c FROM y WHERE x.b = y.d AND c > " +
      "(SELECT SUM(e) FROM z WHERE y.c = z.e AND z.f < 100))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test(expected = classOf[AssertionError])
  def testInWithCorrelatedOnWhere_ScalarQuery8(): Unit = {
    // nested correlation can not be converted joinType=[semi] now
    // TODO There are some bugs when decorrelating in RelDecorrelator
    val sqlQuery = "SELECT b FROM x WHERE a IN (SELECT c FROM y WHERE x.b = y.d AND c > " +
      "(SELECT 0.5 * SUM(e) FROM z WHERE x.a = z.e AND z.f < 100))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) FROM r WHERE l.c = r.f AND r.d < 3 GROUP BY r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE a IN " +
      "(SELECT d * 5 FROM (SELECT SUM(d) AS d FROM r WHERE l.b = r.e) r1)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT COUNT(*) FROM r WHERE l.c = r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate4(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (a, b) IN " +
      "(SELECT d, COUNT(*) FROM r WHERE l.c = r.f GROUP BY d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate5(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(e) FROM r WHERE l.c = r.f GROUP BY d, true, 1)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate6(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (b, a) IN " +
      "(SELECT COUNT(*), d FROM r WHERE l.c = r.f GROUP BY d, true, e, 1)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate7(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT min(e) FROM " +
      "(SELECT d, e, RANK() OVER(PARTITION BY d ORDER BY e) AS rk FROM r) t " +
      "WHERE rk < 2 AND l.a = t.d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate8(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT AVG(e) FROM r WHERE l.c = r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate9(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT d FROM (SELECT MAX(d) AS d, f FROM r GROUP BY f) t WHERE l.c > t.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Over1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c = r.f AND r.d < 3)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Over2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (a, b) IN " +
      "(SELECT MAX(r.d) OVER(), MIN(r.e) OVER(PARTITION BY f ORDER BY d) FROM r WHERE l.c = r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Over3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c <> r.f AND r.d < 3)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_Over4(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (a, b) IN " +
      "(SELECT MAX(r.d) OVER(), MIN(r.e) OVER(PARTITION BY f ORDER BY d) FROM r WHERE l.c > r.f)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_Over5(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT rk FROM " +
      "(SELECT d, e, RANK() OVER(PARTITION BY d ORDER BY e) AS rk FROM r) t " +
      "WHERE l.a <> t.d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_AggregateOver1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c = r.f GROUP BY r.e)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_AggregateOver2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c < r.f GROUP BY r.e)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_UnsupportedAggregate1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT MIN(e) FROM r WHERE l.c > r.f)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_UnsupportedAggregate2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT MIN(e) FROM r WHERE l.c = r.f AND " +
      "r.d IN (SELECT MAX(i) FROM t WHERE r.e > t.j))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_UnsupportedAggregate3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT MIN(e) FROM r WHERE l.c > r.f AND " +
      "r.d IN (SELECT MAX(i) FROM t WHERE r.e = t.j))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_InnerJoin1(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(SELECT d FROM r INNER JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r.e = t2.j)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_InnerJoin2(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e, f FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT t.j FROM r1 INNER JOIN t ON r1.f = t.k)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_InnerJoin3(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT e FROM r1 INNER JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r1.e = t2.j)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_LeftJoin1(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(SELECT d FROM r LEFT JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r.e = t2.j)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_LeftJoin2(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e, f FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT t.j FROM r1 LEFT JOIN t ON r1.f = t.k)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_LeftJoin3(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT e FROM r1 LEFT JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r1.e = t2.j)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_RightJoin1(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(SELECT d FROM r RIGHT JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r.e = t2.j)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_RightJoin2(): Unit = {
    val sqlQuery1 = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e, f FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT t.j FROM r1 RIGHT JOIN t ON r1.f = t.k)"
    util.verifyRelPlanNotExpected(sqlQuery1, "joinType=[semi]")

    val sqlQuery2 = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT e FROM r1 RIGHT JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r1.e = t2.j)"
    util.verifyRelPlanNotExpected(sqlQuery2, "joinType=[semi]")

  }

  @Test
  def testInWithCorrelatedOnWhere_FullJoin(): Unit = {
    val sqlQuery1 = "SELECT c FROM l WHERE b IN " +
      "(SELECT d FROM r FULL JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r.e = t2.j)"
    util.verifyRelPlanNotExpected(sqlQuery1, "joinType=[semi]")

    val sqlQuery2 = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e, f FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT t.j FROM r1 FULL JOIN t ON r1.f = t.k)"
    util.verifyRelPlanNotExpected(sqlQuery2, "joinType=[semi]")

    val sqlQuery3 = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT e FROM r1 FULL JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r1.e = t2.j)"
    util.verifyRelPlanNotExpected(sqlQuery3, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_Union1(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    // UNION with correlation can not be converted to semi-join
    val sqlQuery = "SELECT a FROM l WHERE b IN " +
      "(SELECT e FROM r WHERE l.a = r.d AND d > 10 " +
      "UNION " +
      "SELECT i FROM t WHERE i < 100)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_Union2(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    val sqlQuery = "SELECT a FROM l WHERE b IN " +
      "(SELECT e FROM r WHERE l.a = r.d AND d > 10 " +
      "UNION " +
      "SELECT i FROM t WHERE l.c = t.k AND i < 100)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnWhere_MultiFields1(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE (a, c) IN (SELECT d, f FROM r WHERE l.b = r.e)")
  }

  @Test
  def testInWithCorrelatedOnWhere_MultiFields2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (a, SUBSTRING(c, 1, 5)) IN " +
      "(SELECT d, SUBSTRING(f, 1, 5) FROM r WHERE l.b = r.e)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_MultiFields3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (a + 10, SUBSTRING(c, 1, 5)) IN " +
      "(SELECT d + 100, SUBSTRING(f, 1, 5) FROM r WHERE l.b = r.e)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiInWithCorrelatedOnWhere1(): Unit = {
    // multi IN in AND condition with same correlation-vars
    val sqlQuery = "SELECT * FROM l WHERE " +
      "a IN (SELECT d FROM r WHERE c = f) AND b IN (SELECT j FROM t WHERE c = k)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiInWithCorrelatedOnWhere2(): Unit = {
    // multi IN in AND condition with different correlation-vars
    val sqlQuery = "SELECT * FROM l WHERE " +
      "a IN (SELECT d FROM r WHERE c = f) AND b IN (SELECT j FROM t WHERE a = i AND k <> 'test')"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiInWithCorrelatedOnWhere3(): Unit = {
    // nested IN
    val sqlQuery = "SELECT * FROM l WHERE a IN (" +
      "SELECT r.d FROM r WHERE l.b = r.e AND r.f IN (SELECT t.k FROM t WHERE r.e = t.j))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiInWithCorrelatedOnWhere_OR(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE " +
      "a IN (SELECT c FROM y WHERE x.b = y.d) OR b IN (SELECT e FROM z WHERE x.a = z.f)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testMultiInWithCorrelatedOnWhere_NestedCorrelation(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b > 1 AND a IN " +
      "(SELECT d FROM r WHERE l.b = r.e AND f IN " +
      "(SELECT k FROM t WHERE l.a = t.i AND r.e = t.j))"
    // nested correlation can not be converted joinType=[semi] now
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testMultiInWithUncorrelatedAndCorrelatedOnWhere1(): Unit = {
    // two IN in AND condition, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT a FROM x WHERE" +
      " a IN (SELECT c FROM y WHERE x.b = y.d) AND b IN (SELECT f FROM z)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelatedAndCorrelatedOnWhere2(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE " +
      "a IN ( SELECT c FROM y) AND b IN (SELECT e FROM z WHERE z.f = x.b)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelatedAndCorrelatedOnWhere3(): Unit = {
    // two nested IN, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT * FROM x WHERE a IN (" +
      "SELECT c FROM y WHERE d IN (SELECT f FROM z) AND x.b = y.d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelatedAndCorrelatedOnWhere4(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE a IN (" +
      "SELECT c FROM y WHERE d IN (SELECT f FROM z WHERE z.e = y.c))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnHaving1(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS s FROM x GROUP BY b HAVING COUNT(*) > 2 AND b IN (SELECT d FROM y)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnHaving2(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS s FROM x GROUP BY b HAVING COUNT(*) > 2 AND MAX(b) IN (SELECT d FROM y)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnHaving(): Unit = {
    // TODO There are some bugs when converting SqlNode to RelNode:
    val sqlQuery = "SELECT SUM(a) AS s FROM x GROUP BY b " +
      "HAVING MAX(a) IN (SELECT d FROM y WHERE y.d = x.b)"

    // the logical plan is:
    //
    // LogicalProject(s=[$1])
    //  LogicalFilter(condition=[IN($2, {
    //   LogicalProject(d=[$1])
    //    LogicalFilter(condition=[=($1, $cor0.b)])
    //     LogicalTableScan(table=[[builtin, default, r]])
    //  })])
    //   LogicalAggregate(group=[{0}], s=[SUM($1)], agg#1=[MAX($1)])
    //    LogicalProject(b=[$1], a=[$0])
    //     LogicalTableScan(table=[[builtin, default, l]])
    //
    // LogicalFilter lost variablesSet information.

    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnLateralTable1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE a = d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnLateralTable2(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1) " +
      "WHERE d IN (SELECT i FROM t WHERE l.b = t.j))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnLateralTable3(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM (SELECT * FROM r WHERE d IN (" +
      "SELECT i FROM t WHERE t.j = l.b)) m, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInWithCorrelatedOnLateralTable4(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM (SELECT * FROM r LEFT JOIN LATERAL TABLE(table_func(f)) AS T(f1) ON TRUE " +
      "WHERE d IN (SELECT i FROM t WHERE l.b = t.j)))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithUncorrelatedOnWhere1(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE EXISTS (SELECT * FROM y)")
  }

  @Test
  def testExistsWithUncorrelatedOnWhere2(): Unit = {
    val sqlQuery = "SELECT * FROM (SELECT a, b, d FROM x, y) xy " +
      "WHERE EXISTS (SELECT * FROM y where c > 0) AND a < 100"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere3(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE NOT (NOT EXISTS (SELECT * FROM y)) AND x.b = 10")
  }

  @Test
  def testExistsWithUncorrelatedOnWhere4(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE EXISTS (SELECT * FROM y) OR x.b = 10"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithUncorrelatedOnWhere5(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (EXISTS (SELECT d FROM y)) IS TRUE")
  }

  @Test
  def testExistsWithUncorrelatedOnWhere6(): Unit = {
    // TODO the result of SqlToRelConverter does not contain any field `b` info in SubQuery
    util.verifyRelPlan("SELECT a FROM x WHERE EXISTS (SELECT x.b IS NULL FROM y)")
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND EXISTS (SELECT CAST(e AS INTEGER), 1 FROM r WHERE e > 0)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition2(): Unit = {
    val sqlQuery = "SELECT a + 10, c FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' OR NOT EXISTS (SELECT d FROM r))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' AND EXISTS (SELECT d FROM r))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition4(): Unit = {
    // join
    val sqlQuery = "SELECT c FROM l WHERE EXISTS " +
      "(SELECT d, j + 1 FROM r FULL JOIN (SELECT j FROM t WHERE i > 10) t2 ON r.e = t2.j)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition5(): Unit = {
    // aggregate
    val sqlQuery = "SELECT a FROM l WHERE " +
      "EXISTS (SELECT MAX(e), MIN(d) FROM r WHERE d < 3 GROUP BY f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition6(): Unit = {
    // union
    val sqlQuery = "SELECT a FROM l WHERE EXISTS " +
      "(SELECT e, f FROM r WHERE d > 10 UNION SELECT j, k FROM t WHERE i < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition7(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE (EXISTS (SELECT d FROM r)) = true")
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_UnsupportedCondition(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (EXISTS (SELECT d FROM r)) IS NOT NULL"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testMultiExistsWithUncorrelatedOnWhere_AND1(): Unit = {
    val sqlQuery = "SELECT * FROM x, y WHERE x.a = y.c " +
      "AND EXISTS (SELECT * FROM z z1 WHERE z1.e > 50) " +
      "AND b >= 1 " +
      "AND EXISTS (SELECT * FROM z z2 WHERE z2.f < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithUncorrelatedOnWhere_AND2(): Unit = {
    val sqlQuery = "SELECT * FROM x, y WHERE x.a = y.c " +
      "AND EXISTS (SELECT * FROM z z1 WHERE z1.e > 50) " +
      "AND NOT (b >= 1 " +
      "AND EXISTS (SELECT * FROM z z2 WHERE z2.f < 100))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testMultiExistsWithUncorrelatedOnWhere_OR(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND (EXISTS (SELECT * FROM t t1 WHERE t1.k > 50) " +
      "OR EXISTS (SELECT * FROM t t2 WHERE t2.j < 100))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithUncorrelatedOnLateralTable1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnLateralTable2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE EXISTS (SELECT * FROM t))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnLateralTable3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM (SELECT * FROM r WHERE EXISTS (" +
      "SELECT * FROM t)) m, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnLateralTable4(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM (SELECT * FROM r LEFT JOIN LATERAL TABLE(table_func(f)) AS T(f1) ON TRUE))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere1(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE EXISTS (SELECT * FROM y WHERE a = c)")
  }

  @Test
  def testExistsWithCorrelatedOnWhere2(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE EXISTS (SELECT * FROM y WHERE a = c) AND x.a > 2")
  }

  @Test
  def testExistsWithCorrelatedOnWhere3(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM x WHERE EXISTS (SELECT 1, c + d, c + 2, d FROM y WHERE a = c)")
  }

  @Test
  def testExistsWithCorrelatedOnWhere4(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE EXISTS (SELECT * FROM y WHERE x.a = c and x.b > 10)")
  }

  @Test
  def testExistsWithCorrelatedOnWhere5(): Unit = {
    val sqlQuery = "SELECT x1.a FROM x x1, y WHERE x1.b = y.d AND x1.a < 10 AND y.c < 15 " +
      " AND EXISTS (SELECT * FROM x x2 WHERE x1.b = x2.b)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere6(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE EXISTS (SELECT * FROM y WHERE x.a < y.d)")
  }

  @Test
  def testExistsWithCorrelatedOnWhere7(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (EXISTS (SELECT d FROM y WHERE y.d = x.b)) IS TRUE")
  }

  @Test
  def testExistsWithCorrelatedOnWhere8(): Unit = {
    // TODO the result of SqlToRelConverter does not contain any field `a` info in SubQuery
    util.verifyRelPlan("SELECT a FROM x WHERE EXISTS (SELECT x.a IS NULL FROM y WHERE y.d = x.b)")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_ComplexCondition1(): Unit = {
    // TODO only d0 is required in LogicalProject(d=[$0], d0=[CAST($0):INTEGER])
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT CAST(e AS INTEGER), 1 FROM r where CAST(l.b AS INTEGER) = CAST(r.d AS INTEGER))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_ComplexCondition2(): Unit = {
    val sqlQuery = "SELECT a + 10, c FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' OR NOT EXISTS (SELECT d FROM r where l.b + 1 = r.e))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_ComplexCondition3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' AND EXISTS (SELECT d FROM r where l.b = r.e))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_ComplexCondition4(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (EXISTS (SELECT d FROM y WHERE y.d = x.b)) = true")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_UnsupportedCondition1(): Unit = {
    val sqlQuery1 = "SELECT * FROM x WHERE EXISTS (SELECT * FROM y WHERE x.a = y.c OR y.c = 10)"
    util.verifyRelPlanNotExpected(sqlQuery1, "joinType=[semi]")

    val sqlQuery2 = "SELECT * FROM x WHERE EXISTS (SELECT * FROM y WHERE x.a = y.c OR x.b = 10)"
    util.verifyRelPlanNotExpected(sqlQuery2, "joinType=[semi]")

    val sqlQuery3 = "SELECT * FROM x WHERE EXISTS (SELECT * FROM y WHERE x.a = y.c) OR x.b = 10"
    util.verifyRelPlanNotExpected(sqlQuery3, "joinType=[semi]")

    val sqlQuery4 = "SELECT * FROM x WHERE" +
      " (EXISTS (SELECT d FROM y WHERE y.d = x.b)) IS NOT NULL"
    util.verifyRelPlanNotExpected(sqlQuery4, "joinType=[semi]")
  }

  @Test(expected = classOf[AssertionError])
  def testExistsWithCorrelatedOnWhere_UnsupportedCondition2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      " (SELECT * FROM (SELECT * FROM r WHERE r.d = l.a AND r.e > 100) s " +
      "LEFT JOIN t ON s.f = t.k AND l.b = t.j)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Aggregate1(): Unit = {
    util.addTableSource[(Int, Long)]("l1", 'a, 'b)
    util.addTableSource[(Int, Long, Double, String)]("r1", 'c, 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l1 WHERE EXISTS " +
      "(SELECT COUNT(1) FROM r1 WHERE l1.b = r1.d AND c < 100 GROUP BY f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Aggregate2(): Unit = {
    util.addTableSource[(Int, Long)]("l1", 'a, 'b)
    util.addTableSource[(Int, Long, Double, String)]("r1", 'c, 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l1 WHERE EXISTS " +
      "(SELECT MAX(e) FROM r1 WHERE l1.b = r1.d AND c < 100 AND l1.a = r1.c GROUP BY c, true, f, 1)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Aggregate3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT d FROM (SELECT MAX(d) AS d, f FROM r GROUP BY f) t WHERE l.c > t.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Over1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c = r.f AND r.d < 3)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Over2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT SUM(r.e) OVER() FROM r WHERE l.c < r.f AND r.d < 3)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_AggregateOver1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c = r.f GROUP BY r.e)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_AggregateOver2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c <> r.f GROUP BY r.e)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_UnsupportedAggregate1(): Unit = {
    util.addTableSource[(Int, Long)]("l1", 'a, 'b)
    util.addTableSource[(Int, Long, Double, String)]("r1", 'c, 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l1 WHERE EXISTS " +
      "(SELECT COUNT(*) FROM r1 WHERE l1.b > r1.d AND c < 100 GROUP BY f)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_UnsupportedAggregate2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT MIN(e) FROM r WHERE l.c = r.f AND " +
      "EXISTS (SELECT MAX(i) FROM t WHERE r.e > t.j))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_UnsupportedAggregate3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT MIN(e) FROM r WHERE l.c = r.f AND " +
      "EXISTS (SELECT MAX(i) FROM t WHERE r.e > t.j))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_InnerJoin1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 INNER JOIN t ON r1.f = t.k)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_InnerJoin2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM r INNER JOIN (SELECT * FROM t WHERE t.j = l.b AND i < 50) t1 ON r.f = t1.k)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_InnerJoin3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 " +
      "INNER JOIN " +
      "(SELECT i, k FROM t WHERE t.j = l.b AND i < 50) t1 ON r1.f = t1.k)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_LeftJoin1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 LEFT JOIN t ON r1.f = t.k)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_LeftJoin2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM r LEFT JOIN (SELECT * FROM t WHERE t.j = l.b AND i < 50) t1 ON r.f = t1.k)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_LeftJoin3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 " +
      "LEFT JOIN " +
      "(SELECT i, k FROM t WHERE t.j = l.b AND i < 50) t1 ON r1.f = t1.k)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_RightJoin1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 RIGHT JOIN t ON r1.f = t.k)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_RightJoin2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM r RIGHT JOIN (SELECT * FROM t WHERE t.j = l.b AND i < 50) t1 ON r.f = t1.k)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_RightJoin3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 " +
      "RIGHT JOIN " +
      "(SELECT i, k FROM t WHERE t.j = l.b AND i < 50) t1 ON r1.f = t1.k)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_FullJoin(): Unit = {
    val sqlQuery1 = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 FULL JOIN t ON r1.f = t.k)"
    util.verifyRelPlanNotExpected(sqlQuery1, "joinType=[semi]")

    val sqlQuery2 = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM r FULL JOIN (SELECT * FROM t WHERE t.j = l.b AND i < 50) t1 ON r.f = t1.k)"
    util.verifyRelPlanNotExpected(sqlQuery2, "joinType=[semi]")

    val sqlQuery3 = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 " +
      "FULL JOIN " +
      "(SELECT i, k FROM t WHERE t.j = l.b AND i < 50) t1 ON r1.f = t1.k)"
    util.verifyRelPlanNotExpected(sqlQuery3, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Union1(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    // UNION with correlation is not supported
    val sqlQuery = "SELECT a FROM l WHERE EXISTS " +
      "(SELECT e FROM r WHERE l.a = r.d AND d > 10 " +
      "UNION " +
      "SELECT i FROM t WHERE i < 100)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Union2(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    val sqlQuery = "SELECT a FROM l WHERE EXISTS " +
      "(SELECT e FROM r WHERE l.a = r.d AND d > 10 " +
      "UNION " +
      "SELECT i FROM t WHERE l.c = t.k AND i < 100)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_InsideWith1(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE EXISTS (" +
      "WITH y2 AS (SELECT * FROM y WHERE y.c = x.a) SELECT 1 FROM y2 WHERE y2.d = x.b)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_InsideWith2(): Unit = {
    val sqlQuery = "WITH t (a, b) AS (SELECT * FROM (VALUES (1, 2))) " +
      "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM y WHERE y.c = t.a)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Limit1(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE EXISTS (SELECT 1 FROM y WHERE x.a = y.c LIMIT 1)")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Limit2(): Unit = {
    val sqlQuery = "SELECT a FROM x WHERE EXISTS " +
      "(SELECT 1 FROM (SELECT c FROM y LIMIT 1) y2 WHERE x.a = y2.c)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_SortLimit1(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM x WHERE EXISTS (SELECT 1 FROM y WHERE a = c ORDER BY d LIMIT 1)")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_SortLimit2(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM x WHERE EXISTS (SELECT 1 FROM y WHERE a = c ORDER BY c LIMIT 1)")
  }

  @Test
  def testMultiExistsWithCorrelatedOnWhere_NestedCorrelation(): Unit = {
    // SQL from Calcite Test
    // however, parse result is not correct
    val sqlQuery =
    """
      |SELECT * FROM (SELECT 2 + a d2, 3 + b d3 FROM l) e
      |    WHERE EXISTS (SELECT 1 FROM (SELECT d + 1 d1 FROM r) d
      |    WHERE d1 = e.d2 AND EXISTS (SELECT 2 FROM (SELECT i + 4 d4, i + 5 d5, i + 6 d6 FROM t)
      |    WHERE d4 = d.d1 AND d5 = d.d1 AND d6 = e.d3))
    """.stripMargin
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testMultiExistsWithCorrelatedOnWhere_AND1(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND c >= 1 " +
      "AND EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithCorrelatedOnWhere_AND2(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND (c >= 1 " +
      "AND EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithCorrelatedOnWhere_AND3(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND NOT (c >= 1 " +
      "AND EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testMultiExistsWithCorrelatedOnWhere_OR(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND (EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "OR EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testMultiExistsWithUncorrelatedAndCorrelatedOnWhere1(): Unit = {
    // two EXISTS in AND condition, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT * FROM l WHERE " +
      "EXISTS (SELECT * FROM r) AND EXISTS (SELECT * FROM t WHERE l.a = t.i AND t.j < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithUncorrelatedAndCorrelatedOnWhere2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "EXISTS (SELECT * FROM r WHERE l.a <> r.d) AND EXISTS (SELECT * FROM t WHERE j < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithUncorrelatedAndCorrelatedOnWhere3(): Unit = {
    // two nested EXISTS, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r WHERE EXISTS (SELECT * FROM t) AND l.a = r.d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithUncorrelatedAndCorrelatedOnWhere4(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r WHERE EXISTS (SELECT * FROM t WHERE r.d = t.i))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnHaving(): Unit = {
    // TODO There are some bugs when converting SqlNode to RelNode:
    val sqlQuery1 =
      "SELECT SUM(a) AS s FROM x GROUP BY b HAVING EXISTS (SELECT * FROM y WHERE y.d = x.b)"

    // the logical plan is:
    //
    // LogicalProject(s=[$1])
    //  LogicalFilter(condition=[IN($2, {
    //   LogicalProject(d=[$1])
    //    LogicalFilter(condition=[=($1, $cor0.b)])
    //     LogicalTableScan(table=[[builtin, default, r]])
    //  })])
    //   LogicalAggregate(group=[{0}], s=[SUM($1)], agg#1=[MAX($1)])
    //    LogicalProject(b=[$1], a=[$0])
    //     LogicalTableScan(table=[[builtin, default, l]])
    //
    // LogicalFilter lost variablesSet information.

    util.verifyRelPlanNotExpected(sqlQuery1, "joinType=[semi]")

    val sqlQuery2 = "SELECT MAX(a) FROM x GROUP BY 1 HAVING EXISTS (SELECT 1 FROM y WHERE d < b)"
    util.verifyRelPlanNotExpected(sqlQuery2, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnLateralTable1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE a = d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnLateralTable2(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1) " +
      "WHERE EXISTS (SELECT * FROM t WHERE l.b = t.j))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnLateralTable3(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM (SELECT * FROM r WHERE EXISTS (" +
      "SELECT * FROM t WHERE t.j = l.b)) m, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testExistsWithCorrelatedOnLateralTable4(): Unit = {
    thrown.expect(classOf[TableException])
    // correlate variable id is unstable, ignore here
    thrown.expectMessage("unexpected correlate variable $cor")

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM (SELECT * FROM r LEFT JOIN LATERAL TABLE(table_func(f)) AS T(f1) ON TRUE " +
      "WHERE EXISTS (SELECT i FROM t WHERE l.b = t.j)))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

  @Test
  def testInExists1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE l.a = r.d)" +
      " AND a IN (SELECT i FROM t WHERE l.b = t.j)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInExists2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT j FROM t) " +
      " AND EXISTS (SELECT * FROM r WHERE l.a = r.d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInExists3(): Unit = {
    util.addTableSource[(Int, Long, String)]("t2", 'l, 'm, 'n)

    // TODO some bugs in SubQueryRemoveRule
    //  the result RelNode (LogicalJoin(condition=[=($1, $8)], joinType=[left]))
    //  after SubQueryRemoveRule is unexpected
    thrown.expect(classOf[AssertionError])

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT c FROM l WHERE (" +
      " (CASE WHEN EXISTS (SELECT * FROM t WHERE l.a = t.i) THEN 1 ELSE 2 END), " +
      " (CASE WHEN b IN (SELECT m FROM t2) THEN 3 ELSE 4 END)) " +
      " IN (SELECT d, e FROM r)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[semi]")
  }

}
