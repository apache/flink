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
  * this class only tests NOT IN and NOT EXISTS queries.
  */
class SubQueryAntiJoinTest extends SubQueryTestBase {
  util.addTableSource[(Int, Long, String)]("l", 'a, 'b, 'c)
  util.addTableSource[(Int, Long, String)]("r", 'd, 'e, 'f)
  util.addTableSource[(Int, Long, String)]("t", 'i, 'j, 'k)
  util.addTableSource[(Int, Long)]("x", 'a, 'b)
  util.addTableSource[(Int, Long)]("y", 'c, 'd)
  util.addTableSource[(Int, Long)]("z", 'e, 'f)
  util.addFunction("table_func", new StringSplit)

  @Test
  def testNotInOnWhere_NotSubQuery(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE a NOT IN (1, 2, 3, 4)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere1(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a NOT IN (SELECT c FROM y)")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere2(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a NOT IN (SELECT c FROM y WHERE d < 100) AND b > 10")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere3(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a + 1 NOT IN (SELECT c FROM y)")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere4(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a * b NOT IN (SELECT d FROM y)")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere5(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE CAST(a AS BIGINT) NOT IN (SELECT d FROM y)")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere6(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a NOT IN (SELECT c FROM y WHERE c IS NOT NULL)")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere7(): Unit = {
    util.verifyRelPlan("SELECT a FROM x x1 WHERE a NOT IN (SELECT a FROM x WHERE a < 3 GROUP BY a)")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_ComplexCondition1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b > 10 AND NOT (c like 'abc' AND a IN (SELECT d FROM r))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_ComplexCondition2(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE NOT (a IN (SELECT d FROM r))")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_ComplexCondition3(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE NOT ( NOT ( a NOT IN (SELECT d FROM r)))")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_ComplexCondition4(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE NOT (a IN (SELECT d FROM r) OR b > 10)")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_ComplexCondition5(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND (c like 'abc' AND a NOT IN (SELECT d FROM r))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_ComplexCondition6(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (a NOT IN (SELECT c FROM y)) IS TRUE")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_ComplexCondition7(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (a NOT IN (SELECT c FROM y) = TRUE)")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_UnsupportedCondition(): Unit = {
    // these queries will not be converted to joinType=[anti]
    val sqlQuery1 = "SELECT * FROM x WHERE a NOT IN (SELECT c FROM y) OR b = 10"
    util.verifyRelPlanNotExpected(sqlQuery1, "joinType=[anti]")

    val sqlQuery2 = "SELECT * FROM x WHERE a NOT IN " +
      "(SELECT c FROM y WHERE x.b NOT IN (SELECT e FROM z))"
    util.verifyRelPlanNotExpected(sqlQuery2, "joinType=[anti]")

    val sqlQuery3 = "SELECT * FROM x, y WHERE " +
      "x.a = y.c AND (y.d NOT IN (SELECT d FROM y) OR x.a >= 1)"
    util.verifyRelPlanNotExpected(sqlQuery3, "joinType=[anti]")

    val sqlQuery4 = "SELECT * FROM x WHERE " +
      "a NOT IN (SELECT c FROM y) OR b NOT IN (SELECT e FROM z)"
    util.verifyRelPlanNotExpected(sqlQuery4, "joinType=[anti]")

    val sqlQuery5 = "SELECT * FROM x WHERE a NOT IN (SELECT x.b FROM y)"
    util.verifyRelPlanNotExpected(sqlQuery5, "joinType=[anti]")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case1(): Unit = {
    val sqlQuery =
      "SELECT b FROM l WHERE (CASE WHEN a > 10 THEN 1 ELSE 2 END) NOT IN (SELECT d FROM r)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case2(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a NOT IN (SELECT 1 FROM t1) THEN 1 ELSE 2 END) NOT IN (SELECT d FROM r)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case3(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1 ELSE 2 END) NOT IN (SELECT d FROM r)"
    util.verifyRelPlan(sqlQuery)

  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case4(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)
    util.addTableSource[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1" +
      " WHEN a NOT IN (SELECT j FROM t2) THEN 2 ELSE 3 END)" +
      " NOT IN (SELECT d FROM r)"
    util.verifyRelPlan(sqlQuery)

  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case5(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)
    util.addTableSource[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE (b, " +
      "(CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1" +
      " WHEN a NOT IN (SELECT j FROM t2) THEN 2 ELSE 3 END))" +
      " NOT IN (SELECT e, d FROM r)"
    util.verifyRelPlan(sqlQuery)

  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case6(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)
    util.addTableSource[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT c FROM l WHERE (" +
      " (CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1 ELSE 2 END), " +
      " (CASE WHEN b NOT IN (SELECT j FROM t2) THEN 3 ELSE 4 END)) " +
      " NOT IN (SELECT d, e FROM r)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case7(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)

    // TODO some bugs in SubQueryRemoveRule
    thrown.expect(classOf[RuntimeException])

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT b FROM l WHERE " +
      "(CASE WHEN a NOT IN (SELECT i FROM t1 WHERE l.a = t1.i) THEN 1 ELSE 2 END) " +
      "NOT IN (SELECT d FROM r)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_MultiFields1(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE (a, c) NOT IN (SELECT d, f FROM r)")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_MultiFields2(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE (a + 1, c) NOT IN (SELECT d, f FROM r)")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_MultiFields3(): Unit = {
    val sqlQuery =
      "SELECT * FROM l WHERE (a + 10, TRIM(c)) NOT IN (SELECT d + 100, SUBSTRING(f, 1, 5) FROM r)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiNotInWithUncorrelatedOnWhere_OR(): Unit = {
    // NOT IN in OR condition
    val sqlQuery = "SELECT * FROM x WHERE " +
      "a NOT IN (SELECT c FROM y) OR b NOT IN (SELECT f FROM z WHERE f IS NOT NULL)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testNotInWithUncorrelatedOnLateralTable(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE c NOT IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere1(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a NOT IN (SELECT c FROM y where x.b = y.d)")
  }

  @Test
  def testNotInWithCorrelatedOnWhere2(): Unit = {
    val sqlQuery =
      "SELECT * FROM x WHERE b > 1 AND a NOT IN (SELECT c FROM y WHERE x.b = y.d AND y.c > 10)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere3(): Unit = {
    val sqlQuery =
      "SELECT * FROM x WHERE b > 1 AND a + 1 NOT IN (SELECT c FROM y WHERE x.b = y.d AND y.c > 10)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere4(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE a NOT IN " +
      "(SELECT c1 FROM (SELECT d * 2 as d1, c + 1 as c1 FROM y) y1 WHERE x.b = y1.d1 AND d1 > 10)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere5(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a NOT IN (SELECT c FROM y WHERE x.b > y.d)")
  }

  @Test
  def testNotInWithCorrelatedOnWhere6(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE a NOT IN (SELECT c FROM y WHERE x.b > 10)")
  }

  @Test
  def testNotInWithCorrelatedOnWhere_Case1(): Unit = {
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a > 10 THEN 1 ELSE 2 END) NOT IN (SELECT d FROM r WHERE l.a = r.d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_Case2(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT 1 FROM t1) THEN 1 ELSE 2 END)" +
      " NOT IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_Case3(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)

    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1 ELSE 2 END)" +
      " NOT IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_Case4(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)
    util.addTableSource[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1" +
      " WHEN a NOT IN (SELECT j FROM t2) THEN 2 ELSE 3 END)" +
      " NOT IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_Case5(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)
    util.addTableSource[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE (b, " +
      "(CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1" +
      " WHEN a NOT IN (SELECT j FROM t2) THEN 2 ELSE 3 END))" +
      " NOT IN (SELECT e, d FROM r WHERE l.c = r.f)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_Case6(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)
    util.addTableSource[(Int)]("t2", 'j)

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT b FROM l WHERE " +
      "(CASE WHEN a NOT IN (SELECT i FROM t1 WHERE l.a = t1.i) THEN 1 ELSE 2 END)" +
      " NOT IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testNotInWithCorrelatedOnWhere_ComplexCondition1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE a NOT IN " +
      "(SELECT CAST(e AS INTEGER) FROM r where CAST(l.b AS INTEGER) = CAST(r.d AS INTEGER))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_ComplexCondition2(): Unit = {
    val sqlQuery =
      "SELECT a + 10, c FROM l WHERE a + 10 NOT IN (SELECT d + 1 FROM r WHERE l.b + 1 = r.e)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_ComplexCondition3(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE c NOT IN (SELECT TRIM(f) FROM r where l.a = r.d + 1)")
  }

  @Test
  def testNotInWithCorrelatedOnWhere_ComplexCondition4(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' AND a IN (SELECT d FROM r where l.b = r.e))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testNotInWithCorrelatedOnWhere_ComplexCondition5(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND (c like 'abc' AND a NOT IN (SELECT d FROM r where l.b = r.e))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_ComplexCondition6(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b NOT IN " +
      "(SELECT e * 5 FROM (SELECT SUM(e) AS e FROM r WHERE l.a = r.d) r1)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_UnsupportedCondition(): Unit = {
    // these queries will not be converted to joinType=[anti]
    val sqlQuery1 = "SELECT * FROM x WHERE b NOT IN (SELECT d FROM y WHERE x.a = y.c OR y.c = 10)"
    util.verifyRelPlanNotExpected(sqlQuery1, "joinType=[anti]")

    val sqlQuery2 = "SELECT * FROM x WHERE b NOT IN (SELECT d FROM y WHERE x.a = y.c OR x.b = 10)"
    util.verifyRelPlanNotExpected(sqlQuery2, "joinType=[anti]")

    val sqlQuery3 = "SELECT * FROM x WHERE b NOT IN (SELECT d FROM y WHERE x.a = y.c) OR x.a = 10"
    util.verifyRelPlanNotExpected(sqlQuery3, "joinType=[anti]")

    val sqlQuery4 = "SELECT * FROM x WHERE a NOT IN (SELECT x.b FROM y WHERE x.a = y.c)"
    util.verifyRelPlanNotExpected(sqlQuery4, "joinType=[anti]")

    val sqlQuery5 = "SELECT * FROM l WHERE a NOT IN " +
      "(SELECT d FROM r WHERE l.b NOT IN (SELECT j FROM t) AND l.c = r.f)"
    util.verifyRelPlanNotExpected(sqlQuery5, "joinType=[anti]")
  }

  @Test
  def testNotInWithCorrelatedOnWhere_MultiFields1(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE (a, c) NOT IN (SELECT d, f FROM r WHERE l.b = r.e)")
  }

  @Test
  def testNotInWithCorrelatedOnWhere_MultiFields2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (a, SUBSTRING(c, 1, 5)) NOT IN " +
      "(SELECT d, TRIM(f) FROM r WHERE l.b = r.e)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_MultiFields3(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (a + 10, SUBSTRING(c, 1, 5)) NOT IN " +
      "(SELECT d + 100, TRIM(f) FROM r WHERE l.b = r.e)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedAndCorrelatedOnWhere1(): Unit = {
    // two NOT IN in AND condition, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT a FROM x WHERE" +
      " a NOT IN (SELECT c FROM y WHERE x.b = y.d) AND b NOT IN (SELECT f FROM z)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedAndCorrelatedOnWhere2(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE " +
      "a NOT IN (SELECT c FROM y) AND b NOT IN (SELECT e FROM z WHERE z.f = x.b)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedAndCorrelatedOnWhere3(): Unit = {
    // two nested IN, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT * FROM x WHERE a NOT IN (" +
      "SELECT c FROM y WHERE d NOT IN (SELECT f FROM z) AND x.b = y.d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedAndCorrelatedOnWhere4(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE a NOT IN (" +
      "SELECT c FROM y WHERE d NOT IN (SELECT f FROM z WHERE z.e = y.c))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnLateralTable(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE c NOT IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE a = d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere1(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE NOT EXISTS (SELECT * FROM y)")
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere2(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE NOT EXISTS (SELECT * FROM y where c > 0) AND a < 100")
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere3(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM x WHERE NOT (NOT (NOT EXISTS (SELECT * FROM y))) AND x.b = 10")
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere4(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE NOT EXISTS (SELECT * FROM y) OR x.b = 10"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere5(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE (NOT EXISTS (SELECT d FROM y)) IS TRUE")
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere6(): Unit = {
    // TODO the result of SqlToRelConverter does not contain any field `b` info in SubQuery
    util.verifyRelPlan("SELECT a FROM x WHERE NOT EXISTS (SELECT x.b IS NULL FROM y)")
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere7(): Unit = {
    util.verifyRelPlan("SELECT * FROM l WHERE (NOT EXISTS (SELECT d FROM r)) = true")
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere_UnsupportedCondition(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (NOT EXISTS (SELECT d FROM r)) IS NOT NULL"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testMultiNotExistsWithUncorrelatedOnWhere_AND1(): Unit = {
    val sqlQuery = "SELECT * FROM x, y WHERE x.a = y.c " +
      "AND NOT EXISTS (SELECT * FROM z z1 WHERE z1.e > 50) " +
      "AND b >= 1 " +
      "AND NOT EXISTS (SELECT * FROM z z2 WHERE z2.f < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithUncorrelatedOnWhere_AND2(): Unit = {
    val sqlQuery = "SELECT * FROM x, y WHERE x.a = y.c " +
      "AND NOT EXISTS (SELECT * FROM z z1 WHERE z1.e > 50) " +
      "AND NOT (b >= 1 " +
      "AND NOT EXISTS (SELECT * FROM z z2 WHERE z2.f < 100))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testMultiNotExistsWithUncorrelatedOnWhere_OR(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND (NOT EXISTS (SELECT * FROM t t1 WHERE t1.k > 50) " +
      "OR NOT EXISTS (SELECT * FROM t t2 WHERE t2.j < 100))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testNotExistsWithUncorrelatedOnLateralTable(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere1(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE NOT EXISTS (SELECT * FROM y WHERE a = c)")
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere2(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE NOT EXISTS (SELECT * FROM y WHERE a = c) AND x.a > 2")
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere3(): Unit = {
    val sqlQuery = "SELECT * FROM x WHERE NOT EXISTS (SELECT 1, c + d, c + 2, d FROM y WHERE a = c)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere4(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM x WHERE NOT EXISTS (SELECT * FROM y WHERE x.a = c and x.b > 10)")
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere5(): Unit = {
    val sqlQuery = "SELECT x1.a FROM x x1, y WHERE x1.b = y.d AND x1.a < 10 AND y.c < 15 " +
      " AND NOT EXISTS (SELECT * FROM x x2 WHERE x1.b = x2.b)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere6(): Unit = {
    util.verifyRelPlan("SELECT * FROM x WHERE NOT EXISTS (SELECT * FROM y WHERE x.a < y.c)")
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere7(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM x WHERE (NOT EXISTS (SELECT d FROM y WHERE y.d = x.b)) IS TRUE")
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere8(): Unit = {
    // TODO the result of SqlToRelConverter does not contain any field `a` info in SubQuery
    util.verifyRelPlan(
      "SELECT a FROM x WHERE NOT EXISTS (SELECT x.a IS NULL FROM y WHERE y.d = x.b)")
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere9(): Unit = {
    util.verifyRelPlan(
      "SELECT * FROM x WHERE (NOT EXISTS (SELECT d FROM y WHERE y.d = x.b)) = true")
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere_UnsupportedCondition(): Unit = {
    val sqlQuery1 = "SELECT * FROM x WHERE NOT EXISTS (SELECT * FROM y WHERE x.a = y.c OR y.c = 10)"
    util.verifyRelPlanNotExpected(sqlQuery1, "joinType=[anti]")

    val sqlQuery2 = "SELECT * FROM x WHERE NOT EXISTS (SELECT * FROM y WHERE x.a = y.c OR x.b = 10)"
    util.verifyRelPlanNotExpected(sqlQuery2, "joinType=[anti]")

    val sqlQuery3 = "SELECT * FROM x WHERE NOT EXISTS (SELECT * FROM y WHERE x.a = y.c) OR x.b = 10"
    util.verifyRelPlanNotExpected(sqlQuery3, "joinType=[anti]")

    val sqlQuery4 = "SELECT * FROM x WHERE" +
      " (NOT EXISTS (SELECT d FROM y WHERE y.d = x.b)) IS NOT NULL"
    util.verifyRelPlanNotExpected(sqlQuery4, "joinType=[anti]")
  }

  @Test(expected = classOf[AssertionError])
  def testNotExistsWithCorrelatedOnWhere_UnsupportedCondition2(): Unit = {
    // TODO Calcite decorrelateRel error
    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS " +
      " (SELECT * FROM (SELECT * FROM r WHERE r.d = l.a AND r.e > 100) s " +
      "LEFT JOIN t ON s.f = t.k AND l.b = t.j)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithCorrelatedOnWhere_NestedCorrelation(): Unit = {
    // SQL from Calcite Test
    // however, parse result is not correct
    val sqlQuery = "SELECT * FROM (SELECT 2 + a d2, 3 + b d3 FROM l) e" +
      " WHERE NOT EXISTS (SELECT 1 FROM (SELECT d + 1 d1 FROM r) d" +
      " WHERE d1 = e.d2 AND NOT EXISTS (SELECT 2 FROM " +
      "(SELECT i + 4 d4, i + 5 d5, i + 6 d6 FROM t)" +
      " WHERE d4 = d.d1 AND d5 = d.d1 AND d6 = e.d3))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testMultiNotExistsWithCorrelatedOnWhere_AND1(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND NOT EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND c >= 1 " +
      "AND NOT EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithCorrelatedOnWhere_AND2(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND NOT EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND (c >= 1 " +
      "AND NOT EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithCorrelatedOnWhere_AND3(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND NOT EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND NOT (c >= 1 " +
      "AND NOT EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testMultiNotExistsWithCorrelatedOnWhere_OR(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND (NOT EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "OR NOT EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100))"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testMultiNotExistsWithUncorrelatedAndCorrelatedOnWhere1(): Unit = {
    // two EXISTS in AND condition, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT * FROM l WHERE " +
      "NOT EXISTS (SELECT * FROM r) AND NOT EXISTS (SELECT * FROM t WHERE l.a = t.i AND t.j < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithUncorrelatedAndCorrelatedOnWhere2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "NOT EXISTS (SELECT * FROM r WHERE l.a <> r.d) AND NOT EXISTS (SELECT * FROM t WHERE j < 100)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithUncorrelatedAndCorrelatedOnWhere3(): Unit = {
    // two nested EXISTS, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (" +
      "SELECT * FROM r WHERE NOT EXISTS (SELECT * FROM t) AND l.a = r.d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithUncorrelatedAndCorrelatedOnWhere4(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (" +
      "SELECT * FROM r WHERE NOT EXISTS (SELECT * FROM t WHERE r.d = t.i))"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnLateralTable(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE a = d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInNotExists1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE l.a = r.d)" +
      " AND a NOT IN (SELECT i FROM t WHERE l.b = t.j)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInNotExists2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b NOT IN (SELECT j FROM t) " +
      " AND NOT EXISTS (SELECT * FROM r WHERE l.a = r.d)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testNotInNotExists3(): Unit = {
    util.addTableSource[(Int, Long, String)]("t2", 'l, 'm, 'n)

    // TODO some bugs in SubQueryRemoveRule
    //  the result RelNode (LogicalJoin(condition=[=($1, $11)], joinType=[left]))
    //  after SubQueryRemoveRule is unexpected
    thrown.expect(classOf[AssertionError])

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT c FROM l WHERE (" +
      " (CASE WHEN NOT EXISTS (SELECT * FROM t WHERE l.a = t.i) THEN 1 ELSE 2 END), " +
      " (CASE WHEN b NOT IN (SELECT m FROM t2) THEN 3 ELSE 4 END)) " +
      "  NOT IN (SELECT d, e FROM r)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

  @Test
  def testInNotInExistsNotExists1(): Unit = {
    util.addTableSource[(Int, Long, String)]("t2", 'l, 'm, 'n)

    // FilterSimplifyExpressionsRule will reorder the conditions
    val sqlQuery = "SELECT * FROM l WHERE a IN (SELECT d FROM r) " +
      " AND b NOT IN (SELECT i FROM t WHERE l.c = t.k AND i > 10) " +
      " AND EXISTS (SELECT COUNT(l) FROM t2 where n like 'Test' GROUP BY l)" +
      " AND NOT EXISTS (SELECT * FROM r WHERE l.b <> r.e)"
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testInNotInExistsNotExists2(): Unit = {
    util.addTableSource[(Int, Long, String)]("t2", 'l, 'm, 'n)

    // TODO some bugs in SubQueryRemoveRule
    thrown.expect(classOf[RuntimeException])

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT c FROM l WHERE (" +
      " (CASE WHEN b IN (SELECT j FROM t WHERE l.a = t.i) THEN 1 ELSE 2 END), " +
      " (CASE WHEN NOT EXISTS (SELECT m FROM t2) THEN 3 " +
      "       WHEN EXISTS (select i FROM t) THEN 4 ELSE 5 END)) " +
      "  NOT IN (SELECT d, e FROM r)"
    util.verifyRelPlanNotExpected(sqlQuery, "joinType=[anti]")
  }

}

