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
package org.apache.flink.table.planner.plan.stream.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedTableFunctions.StringSplit
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class SemiAntiJoinTest extends TableTestBase {

  private val util = streamTestUtil()
  util.addTableSource[(Int, Long, String)]("l", 'a, 'b, 'c)
  util.addTableSource[(Int, Long, String)]("r", 'd, 'e, 'f)
  util.addTableSource[(Int, Long, String)]("t", 'i, 'j, 'k)
  util.addFunction("table_func", new StringSplit)

  @Test
  def testInWithUncorrelated_SimpleCondition1(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a IN (SELECT d FROM r)")
  }

  @Test
  def testInWithUncorrelated_SimpleCondition2(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a IN (SELECT d FROM r WHERE e < 100) AND b > 10")
  }

  @Test
  def testInWithUncorrelated_SimpleCondition3(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a + 1 IN (SELECT d FROM r)")
  }

  @Test
  def testInWithUncorrelated_ComplexCondition1(): Unit = {
    val sqlQuery =
      "SELECT * FROM l WHERE b > 10 AND NOT (c like 'abc' OR a NOT IN (SELECT d FROM r))"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelated_ComplexCondition2(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE (a IN (SELECT d FROM r)) IS TRUE")
  }

  @Test
  def testInWithUncorrelated_ComplexCondition3(): Unit = {
    val sqlQuery =
      """
        |SELECT b FROM l WHERE (CASE WHEN a IN
        |    (SELECT 1 FROM t) THEN 1 ELSE 2 END) IN (SELECT d FROM r)
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelated_ComplexCondition4(): Unit = {
    util.verifyExecPlan(
      "SELECT a FROM l WHERE (SELECT MAX(e) FROM r WHERE d > 0) IN (SELECT j FROM t)")
  }

  @Test
  def testInWithUncorrelated_ComplexCondition5(): Unit = {
    val sqlQuery = "SELECT b FROM l WHERE a IN (SELECT d FROM r WHERE e > 10) " +
      "AND b > (SELECT 0.5 * SUM(j) FROM t WHERE t.i < 100)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelated_AggInSubQuery1(): Unit = {
    util.verifyExecPlan("SELECT a FROM l WHERE b IN (SELECT MAX(e) FROM r WHERE d < 3 GROUP BY f)")
  }

  @Test
  def testInWithUncorrelated_AggInSubQuery2(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE (a, b) IN(SELECT d, COUNT(*) FROM r GROUP BY d)")
  }

  @Test
  def testInWithUncorrelated_JoinInSubQuery(): Unit = {
    val sqlQuery =
      """
        |SELECT c FROM l WHERE a IN
        |    (SELECT d FROM r FULL JOIN (SELECT i FROM t WHERE i > 10) t2 ON r.e = t2.i)
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelated_UnionInSubQuery(): Unit = {
    val sqlQuery =
      """
        |SELECT a FROM l WHERE b IN
        |    (SELECT e FROM r WHERE d > 10 UNION SELECT i FROM t WHERE i < 100)
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelated_OverInSubQuery(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM l WHERE (a, b) IN
        |    (SELECT MAX(r.d) OVER(PARTITION BY f ORDER BY d),
        |            MIN(r.e) OVER(PARTITION BY f ORDER BY d) FROM r)
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelated_LateralTableInSubQuery(): Unit = {
    val sqlQuery =
      "SELECT * FROM l WHERE c IN (SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyExecPlan(sqlQuery)
  }


  @Test
  def testInWithUncorrelated_Having(): Unit = {
    val sqlQuery =
      "SELECT SUM(a) AS s FROM l GROUP BY b HAVING COUNT(*) > 2 AND MAX(b) IN (SELECT e FROM r)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelated_MultiFields(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM l WHERE
        |    (a + 10, SUBSTRING(c, 1, 5)) IN (SELECT d + 100, SUBSTRING(f, 1, 5) FROM r)
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelated1(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a IN (SELECT d FROM r) AND b IN (SELECT j FROM t)")
  }

  @Test
  def testMultiInWithUncorrelated2(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a IN (SELECT d FROM r WHERE e IN (SELECT j FROM t))")
  }

  @Test
  def testInWithCorrelated_SimpleCondition1(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a IN (SELECT d FROM r WHERE l.b = r.e)")
  }

  @Test
  def testInWithCorrelated_SimpleCondition2(): Unit = {
    val sqlQuery =
      "SELECT * FROM l WHERE b > 1 AND a IN (SELECT d FROM r WHERE l.b = r.e AND r.d > 10)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelated_SimpleCondition3(): Unit = {
    util.verifyExecPlan(
      "SELECT * FROM l WHERE a IN (SELECT d FROM r where CAST(l.b AS INTEGER) = r.d)")
  }

  @Test
  def testInWithCorrelated_NonEquiCondition1(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a IN (SELECT d FROM r WHERE l.b > r.e)")
  }

  @Test
  def testInWithCorrelated_NonEquiCondition2(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a IN (SELECT d FROM r WHERE l.b > 10)")
  }

  @Test
  def testInWithCorrelated_ComplexCondition1(): Unit = {
    val sqlQuery = "SELECT a + 10, c FROM l WHERE " +
      "NOT(NOT(substring(c, 1, 5) IN (SELECT substring(f, 1, 5) FROM r WHERE l.b + 1 = r.e)))"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelated_ComplexCondition2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' OR a NOT IN (SELECT d FROM r WHERE l.b = r.e))"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelated_ComplexCondition3(): Unit = {
    val sqlQuery = "SELECT b FROM l WHERE " +
      " (CASE WHEN a IN (SELECT 1 FROM t) THEN 1 ELSE 2 END) IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelated_ScalarQuery(): Unit = {
    val sqlQuery = "SELECT a FROM l WHERE " +
      "(SELECT MAX(d) FROM r WHERE e IN (SELECT j FROM t)) IN (SELECT i FROM t WHERE t.k = l.c)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelated_AggInSubQuery1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) FROM r WHERE l.c = r.f AND r.d < 3 GROUP BY r.f)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelated_AggInSubQuery2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (b, a) IN " +
      "(SELECT COUNT(*), d FROM r WHERE l.c = r.f GROUP BY d, true, e, 1)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelated_OverInSubQuery1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT rk FROM " +
      "(SELECT d, e, RANK() OVER(PARTITION BY d ORDER BY e) AS rk FROM r) t " +
      "WHERE l.a <> t.d)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelated_JoinInSubQuery1(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT e FROM r1 INNER JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r1.e = t2.j)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelated_JoinInSubQuery2(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e, f FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT t.j FROM r1 LEFT JOIN t ON r1.f = t.k)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelated_JoinInSubQuery3(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE a IN " +
      "(SELECT d FROM r RIGHT JOIN (SELECT i FROM t WHERE l.c = t.k AND i > 10) t2 ON r.d = t2.i)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelated_LateralTableInSubQuery(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE a = d)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelated_MultiFields(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE (a, SUBSTRING(c, 1, 5)) IN " +
      "(SELECT d, SUBSTRING(f, 1, 5) FROM r WHERE l.b = r.e)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultiInWithCorrelated1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "a IN (SELECT d FROM r WHERE c = f) AND b IN (SELECT j FROM t WHERE a = i AND k <> 'test')"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultiInWithCorrelated2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE a IN (" +
      "SELECT r.d FROM r WHERE l.b = r.e AND r.f IN (SELECT t.k FROM t WHERE r.e = t.j))"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultiInWithCorrelated3(): Unit = {
    val sqlQuery =
      "SELECT * FROM l WHERE a IN (SELECT d FROM r) AND b IN (SELECT j FROM t WHERE t.k = l.c)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelated_SimpleCondition1(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE EXISTS (SELECT * FROM r)")
  }

  @Test
  def testExistsWithUncorrelated_SimpleCondition2(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE EXISTS (SELECT * FROM r) AND b > 10")
  }

  @Test
  def testExistsWithUncorrelated_ComplexCondition(): Unit = {
    val sqlQuery =
      "SELECT a + 10, c FROM l WHERE b > 10 AND NOT (c like 'abc' OR NOT EXISTS (SELECT d FROM r))"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelated_JoinInSubQuery(): Unit = {
    val sqlQuery = "SELECT c FROM l WHERE EXISTS " +
      "(SELECT d, j + 1 FROM r FULL JOIN (SELECT j FROM t WHERE i > 10) t2 ON r.e = t2.j)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelated_LateralTableInSubQuery(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE EXISTS (SELECT * FROM t))"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelated_UnionInSubQuery(): Unit = {
    val sqlQuery = "SELECT a FROM l WHERE EXISTS " +
      "(SELECT e, f FROM r WHERE d > 10 UNION SELECT j, k FROM t WHERE i < 100)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithUncorrelated(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND EXISTS (SELECT * FROM t t1 WHERE t1.i > 50) " +
      "AND b >= 1 " +
      "AND EXISTS (SELECT * FROM t t2 WHERE t2.j < 100)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelated_SimpleCondition(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE a = d)")
  }

  @Test
  def testExistsWithCorrelated_NonEquiCondition1(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE l.a < r.d)")
  }

  @Test
  def testExistsWithCorrelated_NonEquiCondition2(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE l.a < 10)")
  }

  @Test
  def testExistsWithCorrelated_AggInSubQuery(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT MAX(e) FROM r WHERE l.b = r.e AND d < 100 AND l.c = r.f GROUP BY d, true, f, 1)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelated_OverInSubQuery(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c = r.f GROUP BY r.e)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelated_LateralTableInSubQuery(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE a = d)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithCorrelate1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "EXISTS (SELECT * FROM r WHERE l.a <> r.d) AND EXISTS (SELECT * FROM t WHERE j < 100)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithCorrelate2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r WHERE EXISTS (SELECT * FROM t) AND l.a = r.d)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInExists1(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE l.a = r.d)" +
      " AND a IN (SELECT i FROM t WHERE l.b = t.j)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInExists2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT j FROM t) " +
      " AND EXISTS (SELECT * FROM r WHERE l.a = r.d)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelated_SimpleCondition1(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a NOT IN (SELECT d FROM r)")
  }

  @Test
  def testNotInWithUncorrelated_SimpleCondition2(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a NOT IN (SELECT d FROM r WHERE e < 100) AND b > 10")
  }

  @Test
  def testNotInWithUncorrelated_SimpleCondition3(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a * b NOT IN (SELECT d FROM r)")
  }

  @Test
  def testNotInWithUncorrelated_ComplexCondition1(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE NOT ( NOT ( a NOT IN (SELECT d FROM r)))")
  }

  @Test
  def testNotInWithUncorrelated_ComplexCondition2(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND (c like 'abc' AND a NOT IN (SELECT d FROM r))"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelated_ComplexCondition3(): Unit = {
    util.addTableSource[(Int)]("t1", 'i)
    util.addTableSource[(Int)]("t2", 'j)

    val sqlQuery = "SELECT b FROM l WHERE (b, " +
      "(CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1" +
      " WHEN a NOT IN (SELECT j FROM t2) THEN 2 ELSE 3 END))" +
      " NOT IN (SELECT e, d FROM r)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelated_MultiFields(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE (a, c) NOT IN (SELECT d, f FROM r)")
  }

  @Test
  def testNotInWithCorrelated_SimpleCondition1(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a NOT IN (SELECT d FROM r WHERE l.b = r.e)")
  }

  @Test
  def testNotInWithCorrelated_SimpleCondition2(): Unit = {
    val sqlQuery =
      "SELECT * FROM l WHERE b > 1 AND a NOT IN (SELECT d FROM r WHERE l.b = r.e AND r.d > 10)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelated_NonEquiCondition1(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a NOT IN (SELECT d FROM r WHERE l.b > r.e)")
  }

  @Test
  def testNotInWithCorrelated_NonEquiCondition2(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE a NOT IN (SELECT d FROM r WHERE l.b > 10)")
  }

  @Test
  def testNotInWithCorrelated_ComplexCondition1(): Unit = {
    val sqlQuery = "SELECT a + 10, c FROM l WHERE " +
      "NOT(NOT(substring(c, 1, 5) NOT IN (SELECT substring(f, 1, 5) FROM r WHERE l.b + 1 = r.e)))"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelated_ComplexCondition2(): Unit = {
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a NOT IN (SELECT i FROM t) THEN 1" +
      " WHEN a NOT IN (SELECT CAST(j AS INTEGER) FROM t) THEN 2 ELSE 3 END)" +
      " NOT IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelated_MultiFields(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE (a, c) NOT IN (SELECT d, f FROM r WHERE l.b = r.e)")
  }

  @Test
  def testMultiNotInWithCorrelated(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE a NOT IN (" +
      "SELECT d FROM r WHERE e NOT IN (SELECT j FROM t WHERE t.i = r.d))"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithUncorrelated_SimpleCondition1(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r)")
  }

  @Test
  def testNotExistsWithUncorrelated_SimpleCondition2(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r) AND b > 10")
  }

  @Test
  def testNotExistsWithUncorrelated_ComplexCondition(): Unit = {
    val sqlQuery =
      "SELECT a + 10, c FROM l WHERE " +
        "NOT( b > 10 OR (c like 'abc' OR NOT EXISTS (SELECT d FROM r)))"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithUncorrelated(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND NOT EXISTS (SELECT * FROM t t1 WHERE t1.i > 50) " +
      "AND b >= 1 " +
      "AND NOT EXISTS (SELECT * FROM t t2 WHERE t2.j < 100)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelated_SimpleCondition(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE a = d)")
  }

  @Test
  def testNotExistsWithCorrelated_NonEquiCondition1(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE l.a < r.d)")
  }

  @Test
  def testNotExistsWithCorrelated_NonEquiCondition2(): Unit = {
    util.verifyExecPlan("SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE l.a < 10)")
  }

  @Test
  def testMultiNotExistsWithCorrelate(): Unit = {
    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND NOT EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND c >= 1 " +
      "AND NOT EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testExistsAndNotExists(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE " +
      "NOT EXISTS (SELECT * FROM r) AND NOT EXISTS (SELECT * FROM t WHERE l.a = t.i AND t.j < 100)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testNotInNotExists(): Unit = {
    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE l.a = r.d)" +
      " AND a NOT IN (SELECT i FROM t WHERE l.b = t.j)"
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testInNotInExistsNotExists(): Unit = {
    util.addTableSource[(Int, Long, String)]("t2", 'l, 'm, 'n)

    val sqlQuery = "SELECT * FROM l WHERE a IN (SELECT d FROM r) " +
      " AND b NOT IN (SELECT i FROM t WHERE l.c = t.k AND i > 10) " +
      " AND EXISTS (SELECT COUNT(l) FROM t2 where n like 'Test' GROUP BY l)" +
      " AND NOT EXISTS (SELECT * FROM r WHERE l.b <> r.e)"
    util.verifyExecPlan(sqlQuery)
  }
}
