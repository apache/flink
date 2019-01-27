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

package org.apache.flink.table.plan.batch.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api.{TableException, ValidationException}
import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.rules.logical.SubQueryTestBase
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

/**
  * Tests for IN and EXISTS.
  */
@RunWith(classOf[Parameterized])
class SubQuerySemiJoinTest(fieldsNullable: Boolean) extends SubQueryTestBase(fieldsNullable) {

  @Test
  def testInOnWhere_NotSubQuery(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    val sqlQuery = "SELECT * FROM l WHERE a IN (1, 2, 3, 4)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithUncorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a IN (SELECT c FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a IN (SELECT c FROM r WHERE d < 100) AND b > 10"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a + 1 IN (SELECT c FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere4(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a * b IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere5(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE CAST(a AS BIGINT) IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere6(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT a FROM l l1 WHERE a IN (SELECT a FROM l WHERE a < 3 GROUP BY a)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ComplexCondition1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' AND a IN (SELECT d FROM r))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithUncorrelatedOnWhere_ComplexCondition2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' OR a NOT IN (SELECT d FROM r))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ComplexCondition3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // join
    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(SELECT d FROM r FULL JOIN (SELECT j FROM t WHERE i > 10) t2 ON r.e = t2.j)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ComplexCondition4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // aggregate
    val sqlQuery = "SELECT a FROM l WHERE b IN (SELECT MAX(e) FROM r WHERE d < 3 GROUP BY f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ComplexCondition5(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // union
    val sqlQuery = "SELECT a FROM l WHERE b IN " +
      "(SELECT e FROM r WHERE d > 10 UNION SELECT i FROM t WHERE i < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_UnsupportedCondition(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // these queries will not be converted to SemiJoin
    val sqlQuery1 = "SELECT * FROM l WHERE a IN (SELECT c FROM r WHERE l.b IN (select e from t))"
    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT * FROM l WHERE a IN (SELECT c FROM r) OR b > 10"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")

    val sqlQuery3 = "SELECT * FROM l, r WHERE l.a = r.c AND (r.d IN (SELECT d FROM r) OR l.a >= 1)"
    util.verifySqlNotExpected(sqlQuery3, "SemiJoin")

    val sqlQuery4 = "SELECT * FROM l WHERE (a IN (SELECT c FROM r)) IS TRUE"
    if (fieldsNullable) {
      util.verifySqlNotExpected(sqlQuery4, "SemiJoin")
    } else {
      // FilterSimplifyExpressionsRule will reduce `(a IN (SELECT c FROM r)) IS TRUE`
      // to `a IN (SELECT c FROM r)`
      util.verifyPlan(sqlQuery4)
    }

    val sqlQuery5 = "SELECT * FROM l WHERE (a IN (SELECT c FROM r)) = true"
    util.verifySqlNotExpected(sqlQuery5, "SemiJoin")

    val sqlQuery6 = "SELECT * FROM l WHERE a IN (SELECT l.b FROM r)"
    util.verifySqlNotExpected(sqlQuery6, "SemiJoin")
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a > 10 THEN 1 ELSE 2 END) IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT 1 FROM t1) THEN 1 ELSE 2 END) IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT i FROM t1) THEN 1 ELSE 2 END) IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT i FROM t1) THEN 1 WHEN a IN (SELECT j FROM t2) THEN 2 ELSE 3 END)" +
      " IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case5(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE (b, " +
      "(CASE WHEN a IN (SELECT i FROM t1) THEN 1 WHEN a IN (SELECT j FROM t2) THEN 2 ELSE 3 END))" +
      " IN (SELECT e, d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case6(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT c FROM l WHERE (" +
      " (CASE WHEN a IN (SELECT i FROM t1) THEN 1 ELSE 2 END), " +
      " (CASE WHEN b IN (SELECT j FROM t2) THEN 3 ELSE 4 END)) " +
      " IN (SELECT d, e FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Case7(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT i FROM t1 WHERE l.a = t1.i) THEN 1 ELSE 2 END) IN (SELECT d FROM r)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithUncorrelatedOnWhere_ScalarQuery1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT a FROM l WHERE (SELECT MAX(d) FROM r WHERE c > 0) IN (SELECT f FROM t)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ScalarQuery2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // TODO convert IN in scalar_query to SemiJoin ???
    val sqlQuery = "SELECT a FROM l WHERE " +
      "(SELECT MAX(d) FROM r WHERE c IN (SELECT e FROM t)) IN (SELECT f FROM t)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ScalarQuery3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT b FROM l WHERE a IN (SELECT c FROM r WHERE d > 10) " +
      "AND b > (SELECT 0.5 * SUM(e) FROM t WHERE t.f < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ScalarQuery4(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT b FROM l WHERE a IN (SELECT c FROM r WHERE d > 10) " +
      "AND b > (SELECT 0.5 * SUM(e) FROM t WHERE l.a = t.e AND t.f < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_ScalarQuery5(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT b FROM l WHERE a IN (SELECT c FROM r WHERE d > " +
      "(SELECT 0.5 * SUM(e) FROM t WHERE l.a = t.e AND t.f < 100))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithUncorrelatedOnWhere_ScalarQuery6(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT b FROM l WHERE a IN (SELECT c FROM r WHERE d > " +
      "(SELECT SUM(e) FROM t WHERE r.c = t.e AND t.f < 100))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Aggregate1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) FROM r WHERE r.d < 3 GROUP BY r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Aggregate2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE a IN " +
      "(SELECT d * 5 FROM (SELECT SUM(d) AS d FROM r) r1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Aggregate3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT COUNT(*) FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Aggregate4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a, b) IN " +
      "(SELECT d, COUNT(*) FROM r GROUP BY d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Aggregate5(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(e) FROM r GROUP BY d, true, 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Aggregate6(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (b, a) IN " +
      "(SELECT COUNT(*), d FROM r GROUP BY d, true, e, 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Over1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) OVER() FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_Over2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a, b) IN " +
      "(SELECT MAX(r.d) OVER(), MIN(r.e) OVER(PARTITION BY f ORDER BY d) FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_AggregateOver(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) OVER() FROM r GROUP BY r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_MultiFields1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a, c) IN (SELECT d, f FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_MultiFields2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a + 1, c) IN (SELECT d, f FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnWhere_MultiFields3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "(a + 10, SUBSTRING(c, 1, 5)) IN (SELECT d + 100, SUBSTRING(f, 1, 5) FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // multi IN in AND condition
    val sqlQuery = "SELECT * FROM l WHERE " +
      "a IN ( SELECT c FROM r) AND b IN (SELECT e FROM t)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // nested IN
    val sqlQuery = "SELECT * FROM l WHERE a IN (" +
      "SELECT c FROM r WHERE d IN (SELECT f FROM t))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "a + 1 IN ( SELECT c FROM r WHERE d > 10) AND b * 2 IN (SELECT e FROM t WHERE f < 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelatedOnWhere_OR(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "a IN (SELECT c FROM r) OR b IN (SELECT e FROM t)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithUncorrelatedOnLateralTable1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnLateralTable2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("x", 'i, 'j, 'k)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE d IN (SELECT i FROM x))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnLateralTable3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("x", 'i, 'j, 'k)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM (SELECT * FROM r WHERE d IN (" +
      "SELECT i FROM x)) t, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnLateralTable4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM (SELECT * FROM r LEFT JOIN LATERAL TABLE(table_func(f)) AS T(f1) ON TRUE))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a IN (SELECT c FROM r where l.b = r.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE b > 1 AND a IN " +
      "(SELECT c FROM r WHERE l.b = r.d AND r.c > 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE b > 1 AND a + 1 IN " +
      "(SELECT c FROM r WHERE l.b = r.d AND r.c > 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere4(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a IN (SELECT c FROM r WHERE l.b > r.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere5(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT a FROM l l1 WHERE a IN " +
      "(SELECT a FROM l WHERE a < 3 AND l1.b = l.b GROUP BY a)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere6(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a IN (SELECT c FROM r where CAST(l.b AS INTEGER) = r.c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere7(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a IN (SELECT c FROM r WHERE l.b > 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ComplexCondition1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE a IN " +
      "(SELECT CAST(e AS INTEGER) FROM r where CAST(l.b AS INTEGER) = CAST(r.d AS INTEGER))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ComplexCondition2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT a + 10, c FROM l WHERE " +
      "NOT(NOT(substring(c, 1, 5) IN (SELECT substring(f, 1, 5) FROM r WHERE l.b + 1 = r.e)))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ComplexCondition3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' AND a IN (SELECT d FROM r where l.b = r.e))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_ComplexCondition4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' OR a NOT IN (SELECT d FROM r where l.b = r.e))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_UnsupportedCondition1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    // this query will not be converted to SemiJoin
    val sqlQuery1 = "SELECT * FROM l WHERE b IN (SELECT d FROM r WHERE l.a = r.c OR r.c = 10)"
    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT * FROM l WHERE b IN (SELECT d FROM r WHERE l.a = r.c OR l.b = 10)"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")

    val sqlQuery3 = "SELECT * FROM l WHERE b IN (SELECT d FROM r WHERE l.a = r.c) OR l.a = 10"
    util.verifySqlNotExpected(sqlQuery3, "SemiJoin")

    val sqlQuery4 = "SELECT * FROM l WHERE (a IN (SELECT d FROM r WHERE r.d = l.b)) IS TRUE"
    if (fieldsNullable) {
      util.verifySqlNotExpected(sqlQuery4, "SemiJoin")
    } else {
      // FilterSimplifyExpressionsRule will reduce `(a IN (SELECT c FROM r)) IS TRUE`
      // to `a IN (SELECT c FROM r)`
      util.verifyPlan(sqlQuery4)
    }

    val sqlQuery5 = "SELECT * FROM l WHERE (a IN (SELECT d FROM r WHERE r.d = l.b)) = true"
    util.verifySqlNotExpected(sqlQuery5, "SemiJoin")

    val sqlQuery6 = "SELECT * FROM l WHERE a IN (SELECT l.a FROM r WHERE r.d = l.b)"
    util.verifySqlNotExpected(sqlQuery6, "SemiJoin")
  }

  @Test(expected = classOf[TableException])
  def testInWithCorrelatedOnWhere_UnsupportedCondition2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long)]("t", 'i, 'j, 'k)

    // TODO java.lang.RuntimeException: While invoking method
    // 'public RelDecorrelator$Frame RelDecorrelator.decorrelateRel(LogicalProject)'
    val sqlQuery = "SELECT * FROM l WHERE a IN " +
      "(SELECT d FROM r WHERE l.b IN (SELECT j FROM t) WHERE l.c = r.f)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_Case1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long)]("t1", 'i, 'j)
    util.addTable[(Int, Long)]("t2", 'i, 'j)

    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a > 10 THEN 1 ELSE 2 END) IN (SELECT d FROM r WHERE l.a = r.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Case2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long)]("t1", 'i, 'j)
    util.addTable[(Int, Long)]("t2", 'i, 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT 1 FROM t1) THEN 1 ELSE 2 END) IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Case3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long)]("t1", 'i, 'j)
    util.addTable[(Int, Long)]("t2", 'i, 'j)

    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT i FROM t1) THEN 1 ELSE 2 END) IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Case4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long)]("t1", 'i, 'j)
    util.addTable[(Int, Long)]("t2", 'i, 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT i FROM t1) THEN 1 WHEN a IN (SELECT j FROM t2) THEN 2 ELSE 3 END)" +
      " IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Case5(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long)]("t1", 'i, 'j)
    util.addTable[(Int, Long)]("t2", 'i, 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE (b, " +
      "(CASE WHEN a IN (SELECT i FROM t1) THEN 1 WHEN a IN (SELECT j FROM t2) THEN 2 ELSE 3 END))" +
      " IN (SELECT e, d FROM r WHERE l.c = r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Case6(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long)]("t1", 'i, 'j)
    util.addTable[(Int, Long)]("t2", 'i, 'j)

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT b FROM l WHERE (CASE WHEN a IN (SELECT i FROM t1 WHERE l.b = t1.j) " +
      "THEN 1 ELSE 2 END) IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT a FROM l WHERE " +
      "(SELECT MAX(d) FROM r WHERE c > 0) IN (SELECT f FROM t WHERE t.e = l.a)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // TODO convert IN in scalar_query to SemiJoin ???
    val sqlQuery = "SELECT a FROM l WHERE " +
      "(SELECT MAX(d) FROM r WHERE c IN (SELECT e FROM t)) IN (SELECT f FROM t WHERE t.e = l.a)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // TODO convert IN in scalar_query to SemiJoin ???
    val sqlQuery = "SELECT a FROM l WHERE " +
      "(SELECT MAX(d) FROM r WHERE c IN (SELECT e FROM t WHERE r.d = t.f))" +
      " IN (SELECT f FROM t WHERE t.e = l.a)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery4(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // nested correlation can not be converted SemiJoin now
    val sqlQuery = "SELECT a FROM l WHERE " +
      "(SELECT MAX(d) FROM r WHERE c IN (SELECT e FROM t WHERE l.b = t.f))" +
      " IN (SELECT f FROM t WHERE t.e = l.a)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery5(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT b FROM l WHERE a IN (SELECT c FROM r WHERE l.b = r.d AND c > 10) " +
      "AND b > (SELECT 0.5 * SUM(e) FROM t WHERE t.f < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery6(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT b FROM l WHERE a IN (SELECT c FROM r WHERE l.b = r.d AND c > 10) " +
      "AND b > (SELECT 0.5 * SUM(e) FROM t WHERE l.a = t.e AND t.f < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_ScalarQuery7(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT b FROM l WHERE a IN (SELECT c FROM r WHERE l.b = r.d AND c > " +
      "(SELECT SUM(e) FROM t WHERE r.c = t.e AND t.f < 100))"
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testInWithCorrelatedOnWhere_ScalarQuery8(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // nested correlation can not be converted SemiJoin now
    // TODO There are some bugs when decorrelating in RelDecorrelator
    val sqlQuery = "SELECT b FROM l WHERE a IN (SELECT c FROM r WHERE l.b = r.d AND c > " +
      "(SELECT 0.5 * SUM(e) FROM t WHERE l.a = t.e AND t.f < 100))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) FROM r WHERE l.c = r.f AND r.d < 3 GROUP BY r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE a IN " +
      "(SELECT d * 5 FROM (SELECT SUM(d) AS d FROM r WHERE l.b = r.e) r1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT COUNT(*) FROM r WHERE l.c = r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a, b) IN " +
      "(SELECT d, COUNT(*) FROM r WHERE l.c = r.f GROUP BY d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate5(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(e) FROM r WHERE l.c = r.f GROUP BY d, true, 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate6(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (b, a) IN " +
      "(SELECT COUNT(*), d FROM r WHERE l.c = r.f GROUP BY d, true, e, 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate7(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT min(e) FROM " +
      "(SELECT d, e, RANK() OVER(PARTITION BY d ORDER BY e) AS rk FROM r) t " +
      "WHERE rk < 2 AND l.a = t.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate8(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT AVG(e) FROM r WHERE l.c = r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Aggregate9(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT d FROM (SELECT MAX(d) AS d, f FROM r GROUP BY f) t WHERE l.c > t.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Over1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c = r.f AND r.d < 3)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Over2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a, b) IN " +
      "(SELECT MAX(r.d) OVER(), MIN(r.e) OVER(PARTITION BY f ORDER BY d) FROM r WHERE l.c = r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_Over3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c <> r.f AND r.d < 3)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_Over4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a, b) IN " +
      "(SELECT MAX(r.d) OVER(), MIN(r.e) OVER(PARTITION BY f ORDER BY d) FROM r WHERE l.c > r.f)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_Over5(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT rk FROM " +
      "(SELECT d, e, RANK() OVER(PARTITION BY d ORDER BY e) AS rk FROM r) t " +
      "WHERE l.a <> t.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_AggregateOver1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c = r.f GROUP BY r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_AggregateOver2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c < r.f GROUP BY r.e)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_UnsupportedAggregate1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT MIN(e) FROM r WHERE l.c > r.f)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_UnsupportedAggregate2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT MIN(e) FROM r WHERE l.c = r.f AND " +
      "r.d IN (SELECT MAX(i) FROM t WHERE r.e > t.j))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_UnsupportedAggregate3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT MIN(e) FROM r WHERE l.c > r.f AND " +
      "r.d IN (SELECT MAX(i) FROM t WHERE r.e = t.j))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_InnerJoin1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(SELECT d FROM r INNER JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r.e = t2.j)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_InnerJoin2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e, f FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT t.j FROM r1 INNER JOIN t ON r1.f = t.k)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_InnerJoin3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT e FROM r1 INNER JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r1.e = t2.j)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_LeftJoin1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(SELECT d FROM r LEFT JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r.e = t2.j)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_LeftJoin2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e, f FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT t.j FROM r1 LEFT JOIN t ON r1.f = t.k)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_LeftJoin3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT e FROM r1 LEFT JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r1.e = t2.j)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_RightJoin1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT c FROM l WHERE b IN " +
      "(SELECT d FROM r RIGHT JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r.e = t2.j)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_RightJoin2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery1 = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e, f FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT t.j FROM r1 RIGHT JOIN t ON r1.f = t.k)"
    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT e FROM r1 RIGHT JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r1.e = t2.j)"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")

  }

  @Test
  def testInWithCorrelatedOnWhere_FullJoin(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery1 = "SELECT c FROM l WHERE b IN " +
      "(SELECT d FROM r FULL JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r.e = t2.j)"
    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e, f FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT t.j FROM r1 FULL JOIN t ON r1.f = t.k)"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")

    val sqlQuery3 = "SELECT c FROM l WHERE b IN " +
      "(WITH r1 AS (SELECT e FROM r WHERE l.a = r.d AND r.e < 50) " +
      "SELECT e FROM r1 FULL JOIN (SELECT j FROM t WHERE l.c = t.k AND i > 10) t2 ON r1.e = t2.j)"
    util.verifySqlNotExpected(sqlQuery3, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_Union(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // UNION with correlation can not be converted to semi-join
    val sqlQuery1 = "SELECT a FROM l WHERE b IN " +
      "(SELECT e FROM r WHERE l.a = r.d AND d > 10 " +
      "UNION " +
      "SELECT i FROM t WHERE i < 100)"
    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT a FROM l WHERE b IN " +
      "(SELECT e FROM r WHERE l.a = r.d AND d > 10 " +
      "UNION " +
      "SELECT i FROM t WHERE l.c = t.k AND i < 100)"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnWhere_MultiFields1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a, c) IN (SELECT d, f FROM r WHERE l.b = r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_MultiFields2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a, SUBSTRING(c, 1, 5)) IN " +
      "(SELECT d, SUBSTRING(f, 1, 5) FROM r WHERE l.b = r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnWhere_MultiFields3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a + 10, SUBSTRING(c, 1, 5)) IN " +
      "(SELECT d + 100, SUBSTRING(f, 1, 5) FROM r WHERE l.b = r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiInWithCorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // multi IN in AND condition with same correlation-vars
    val sqlQuery = "SELECT * FROM l WHERE " +
      "a IN (SELECT d FROM r WHERE c = f) AND b IN (SELECT j FROM t WHERE c = k)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiInWithCorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // multi IN in AND condition with different correlation-vars
    val sqlQuery = "SELECT * FROM l WHERE " +
      "a IN (SELECT d FROM r WHERE c = f) AND b IN (SELECT j FROM t WHERE a = i AND k <> 'test')"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiInWithCorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // nested IN
    val sqlQuery = "SELECT * FROM l WHERE a IN (" +
      "SELECT r.d FROM r WHERE l.b = r.e AND r.f IN (SELECT t.k FROM t WHERE r.e = t.j))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiInWithCorrelatedOnWhere_OR(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "a IN (SELECT c FROM r WHERE l.b = r.d) OR b IN (SELECT e FROM t WHERE l.a = t.f)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testMultiInWithCorrelatedOnWhere_NestedCorrelation(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE b > 1 AND a IN " +
      "(SELECT d FROM r WHERE l.b = r.e AND f IN " +
      "(SELECT k FROM t WHERE l.a = t.i AND r.e = t.j))"
    // nested correlation can not be converted SemiJoin now
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testMultiInWithUncorrelatedAndCorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // two IN in AND condition, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT a FROM l WHERE" +
      " a IN (SELECT c FROM r WHERE l.b = r.d) AND b IN (SELECT f FROM t)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelatedAndCorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "a IN ( SELECT c FROM r) AND b IN (SELECT e FROM t WHERE t.f = l.b)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelatedAndCorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // two nested IN, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT * FROM l WHERE a IN (" +
      "SELECT c FROM r WHERE d IN (SELECT f FROM t) AND l.b = r.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiInWithUncorrelatedAndCorrelatedOnWhere4(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE a IN (" +
      "SELECT c FROM r WHERE d IN (SELECT f FROM t WHERE t.e = r.c))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnHaving1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT SUM(a) AS s FROM l GROUP BY b " +
      "HAVING COUNT(*) > 2 AND b IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithUncorrelatedOnHaving2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT SUM(a) AS s FROM l GROUP BY b " +
      "HAVING COUNT(*) > 2 AND MAX(b) IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnHaving(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    // TODO There are some bugs when converting SqlNode to RelNode:
    val sqlQuery = "SELECT SUM(a) AS s FROM l GROUP BY b " +
      "HAVING MAX(a) IN (SELECT d FROM r WHERE r.d = l.b)"

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

    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnLateralTable1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE a = d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInWithCorrelatedOnLateralTable2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("x", 'i, 'j, 'k)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1) " +
      "WHERE d IN (SELECT i FROM x WHERE l.b = x.j))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnLateralTable3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("x", 'i, 'j, 'k)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM (SELECT * FROM r WHERE d IN (" +
      "SELECT i FROM x WHERE x.j = l.b)) t, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInWithCorrelatedOnLateralTable4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("x", 'i, 'j, 'k)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE c IN (" +
      "SELECT f1 FROM (SELECT * FROM r LEFT JOIN LATERAL TABLE(table_func(f)) AS T(f1) ON TRUE " +
      "WHERE d IN (SELECT i FROM x WHERE l.b = x.j)))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithUncorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT * FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM (SELECT a, b, d FROM l, r) lr " +
      "WHERE EXISTS (SELECT * FROM r where c > 0) AND a < 100"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE NOT (NOT EXISTS (SELECT * FROM r)) AND l.b = 10"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere4(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT * FROM r) OR l.b = 10"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithUncorrelatedOnWhere5(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE (EXISTS (SELECT d FROM r)) IS TRUE"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere6(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    // TODO the result of SqlToRelConverter does not contain any field `b` info in SubQuery
    val sqlQuery = "SELECT a FROM l WHERE EXISTS (SELECT l.b IS NULL FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND EXISTS (SELECT CAST(e AS INTEGER), 1 FROM r WHERE e > 0)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT a + 10, c FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' OR NOT EXISTS (SELECT d FROM r))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' AND EXISTS (SELECT d FROM r))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // join
    val sqlQuery = "SELECT c FROM l WHERE EXISTS " +
      "(SELECT d, j + 1 FROM r FULL JOIN (SELECT j FROM t WHERE i > 10) t2 ON r.e = t2.j)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition5(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // aggregate
    val sqlQuery = "SELECT a FROM l WHERE " +
      "EXISTS (SELECT MAX(e), MIN(d) FROM r WHERE d < 3 GROUP BY f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_ComplexCondition6(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // union
    val sqlQuery = "SELECT a FROM l WHERE EXISTS " +
      "(SELECT e, f FROM r WHERE d > 10 UNION SELECT j, k FROM t WHERE i < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnWhere_UnsupportedCondition(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery1 = "SELECT * FROM l WHERE (EXISTS (SELECT d FROM r)) IS NOT NULL"
    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT * FROM l WHERE (EXISTS (SELECT d FROM r)) = true"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")
  }

  @Test
  def testMultiExistsWithUncorrelatedOnWhere_AND1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.c " +
      "AND EXISTS (SELECT * FROM t t1 WHERE t1.e > 50) " +
      "AND b >= 1 " +
      "AND EXISTS (SELECT * FROM t t2 WHERE t2.f < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithUncorrelatedOnWhere_AND2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.c " +
      "AND EXISTS (SELECT * FROM t t1 WHERE t1.e > 50) " +
      "AND NOT (b >= 1 " +
      "AND EXISTS (SELECT * FROM t t2 WHERE t2.f < 100))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testMultiExistsWithUncorrelatedOnWhere_OR(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND (EXISTS (SELECT * FROM t t1 WHERE t1.k > 50) " +
      "OR EXISTS (SELECT * FROM t t2 WHERE t2.j < 100))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithUncorrelatedOnLateralTable1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnLateralTable2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("x", 'i, 'j, 'k)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE EXISTS (SELECT * FROM x))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnLateralTable3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("x", 'i, 'j, 'k)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM (SELECT * FROM r WHERE EXISTS (" +
      "SELECT * FROM x)) t, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithUncorrelatedOnLateralTable4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM (SELECT * FROM r LEFT JOIN LATERAL TABLE(table_func(f)) AS T(f1) ON TRUE))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE a = c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE a = c) AND l.a > 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT 1, c + d, c + 2, d FROM r WHERE a = c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere4(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE l.a = c and l.b > 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere5(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT l1.a FROM l l1, r WHERE l1.b = r.d AND l1.a < 10 AND r.c < 15 " +
      " AND EXISTS (SELECT * FROM l l2 WHERE l1.b = l2.b)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere6(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE l.a < r.c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere7(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE (EXISTS (SELECT d FROM r WHERE r.d = l.b)) IS TRUE"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere8(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    // TODO the result of SqlToRelConverter does not contain any field `a` info in SubQuery
    val sqlQuery = "SELECT a FROM l WHERE EXISTS (SELECT l.a IS NULL FROM r WHERE r.d = l.b)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_ComplexCondition1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    // TODO only d0 is required in LogicalProject(d=[$0], d0=[CAST($0):INTEGER])
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT CAST(e AS INTEGER), 1 FROM r where CAST(l.b AS INTEGER) = CAST(r.d AS INTEGER))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_ComplexCondition2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT a + 10, c FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' OR NOT EXISTS (SELECT d FROM r where l.b + 1 = r.e))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_ComplexCondition3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' AND EXISTS (SELECT d FROM r where l.b = r.e))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_UnsupportedCondition1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery1 = "SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE l.a = r.c OR r.c = 10)"
    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE l.a = r.c OR l.b = 10)"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")

    val sqlQuery3 = "SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE l.a = r.c) OR l.b = 10"
    util.verifySqlNotExpected(sqlQuery3, "SemiJoin")

    val sqlQuery4 = "SELECT * FROM l WHERE" +
      " (EXISTS (SELECT d FROM r WHERE r.d = l.b)) IS NOT NULL"
    util.verifySqlNotExpected(sqlQuery4, "SemiJoin")

    val sqlQuery5 = "SELECT * FROM l WHERE (EXISTS (SELECT d FROM r WHERE r.d = l.b)) = true"
    util.verifySqlNotExpected(sqlQuery5, "SemiJoin")
  }

  @Test(expected = classOf[RuntimeException])
  def testExistsWithCorrelatedOnWhere_UnsupportedCondition2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // TODO Calcite decorrelateRel error
    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      " (SELECT * FROM (SELECT * FROM r WHERE r.d = l.a AND r.e > 100) s " +
      "LEFT JOIN t ON s.f = t.k AND l.b = t.j)"
    util.printSql(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Aggregate1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long, Double, String)]("r", 'c, 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT COUNT(1) FROM r WHERE l.b = r.d AND c < 100 GROUP BY f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Aggregate2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long, Double, String)]("r", 'c, 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT MAX(e) FROM r WHERE l.b = r.d AND c < 100 AND l.a = r.c GROUP BY c, true, f, 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Aggregate3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT d FROM (SELECT MAX(d) AS d, f FROM r GROUP BY f) t WHERE l.c > t.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Over1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c = r.f AND r.d < 3)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Over2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT SUM(r.e) OVER() FROM r WHERE l.c < r.f AND r.d < 3)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_AggregateOver1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c = r.f GROUP BY r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_AggregateOver2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT MAX(r.e) OVER() FROM r WHERE l.c <> r.f GROUP BY r.e)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_UnsupportedAggregate1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long, Double, String)]("r", 'c, 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT COUNT(*) FROM r WHERE l.b > r.d AND c < 100 GROUP BY f)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_UnsupportedAggregate2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT MIN(e) FROM r WHERE l.c = r.f AND " +
      "EXISTS (SELECT MAX(i) FROM t WHERE r.e > t.j))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_UnsupportedAggregate3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT MIN(e) FROM r WHERE l.c = r.f AND " +
      "EXISTS (SELECT MAX(i) FROM t WHERE r.e > t.j))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_InnerJoin1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 INNER JOIN t ON r1.f = t.k)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_InnerJoin2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM r INNER JOIN (SELECT * FROM t WHERE t.j = l.b AND i < 50) t1 ON r.f = t1.k)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_InnerJoin3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 " +
      "INNER JOIN " +
      "(SELECT i, k FROM t WHERE t.j = l.b AND i < 50) t1 ON r1.f = t1.k)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_LeftJoin1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 LEFT JOIN t ON r1.f = t.k)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_LeftJoin2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM r LEFT JOIN (SELECT * FROM t WHERE t.j = l.b AND i < 50) t1 ON r.f = t1.k)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_LeftJoin3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 " +
      "LEFT JOIN " +
      "(SELECT i, k FROM t WHERE t.j = l.b AND i < 50) t1 ON r1.f = t1.k)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_RightJoin1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 RIGHT JOIN t ON r1.f = t.k)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_RightJoin2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM r RIGHT JOIN (SELECT * FROM t WHERE t.j = l.b AND i < 50) t1 ON r.f = t1.k)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_RightJoin3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 " +
      "RIGHT JOIN " +
      "(SELECT i, k FROM t WHERE t.j = l.b AND i < 50) t1 ON r1.f = t1.k)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_FullJoin(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery1 = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 FULL JOIN t ON r1.f = t.k)"
    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM r FULL JOIN (SELECT * FROM t WHERE t.j = l.b AND i < 50) t1 ON r.f = t1.k)"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")

    val sqlQuery3 = "SELECT * FROM l WHERE EXISTS " +
      "(SELECT * FROM (SELECT f FROM r WHERE r.d = l.a AND r.e > 10) r1 " +
      "FULL JOIN " +
      "(SELECT i, k FROM t WHERE t.j = l.b AND i < 50) t1 ON r1.f = t1.k)"
    util.verifySqlNotExpected(sqlQuery3, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Union(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // UNION with correlation is not supported
    val sqlQuery1 = "SELECT a FROM l WHERE EXISTS " +
      "(SELECT e FROM r WHERE l.a = r.d AND d > 10 " +
      "UNION " +
      "SELECT i FROM t WHERE i < 100)"
    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT a FROM l WHERE EXISTS " +
      "(SELECT e FROM r WHERE l.a = r.d AND d > 10 " +
      "UNION " +
      "SELECT i FROM t WHERE l.c = t.k AND i < 100)"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnWhere_InsideWith1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "WITH r2 AS (SELECT * FROM r WHERE r.c = l.a) SELECT 1 FROM r2 WHERE r2.d = l.b)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_InsideWith2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "WITH t (a, b) AS (SELECT * FROM (VALUES (1, 2))) " +
      "SELECT * FROM t WHERE EXISTS (SELECT 1 FROM r WHERE r.c = t.a)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Limit1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT 1 FROM r WHERE l.a = r.c LIMIT 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_Limit2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT a FROM l WHERE EXISTS " +
      "(SELECT 1 FROM (SELECT c FROM r LIMIT 1) r2 WHERE l.a = r2.c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_SortLimit1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT 1 FROM r WHERE a = c ORDER BY d LIMIT 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnWhere_SortLimit2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT 1 FROM r WHERE a = c ORDER BY c LIMIT 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithCorrelatedOnWhere_NestedCorrelation(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // SQL from Calcite Test
    // however, parse result is not correct
    val sqlQuery = "SELECT * FROM (SELECT 2 + a d2, 3 + b d3 FROM l) e"
    +" WHERE EXISTS (SELECT 1 FROM (SELECT d + 1 d1 FROM r) d"
    +" WHERE d1 = e.d2 AND EXISTS (SELECT 2 FROM (SELECT i + 4 d4, i + 5 d5, i + 6 d6 FROM t)"
    +" WHERE d4 = d.d1 AND d5 = d.d1 AND d6 = e.d3))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testMultiExistsWithCorrelatedOnWhere_AND1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND c >= 1 " +
      "AND EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithCorrelatedOnWhere_AND2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND (c >= 1 " +
      "AND EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithCorrelatedOnWhere_AND3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND NOT (c >= 1 " +
      "AND EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testMultiExistsWithCorrelatedOnWhere_OR(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND (EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "OR EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testMultiExistsWithUncorrelatedAndCorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // two EXISTS in AND condition, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT * FROM l WHERE " +
      "EXISTS (SELECT * FROM r) AND EXISTS (SELECT * FROM t WHERE l.a = t.i AND t.j < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithUncorrelatedAndCorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "EXISTS (SELECT * FROM r WHERE l.a <> r.d) AND EXISTS (SELECT * FROM t WHERE j < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithUncorrelatedAndCorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // two nested EXISTS, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r WHERE EXISTS (SELECT * FROM t) AND l.a = r.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiExistsWithUncorrelatedAndCorrelatedOnWhere4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r WHERE EXISTS (SELECT * FROM t WHERE r.d = t.i))"
    util.verifyPlan(sqlQuery)
  }

  @Test(expected = classOf[ValidationException])
  def testExistsWithCorrelatedOnHaving(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    // TODO There are some bugs when converting SqlNode to RelNode:
    val sqlQuery1 = "SELECT SUM(a) AS s FROM l GROUP BY b " +
      "HAVING EXISTS (SELECT * FROM r WHERE r.d = l.b)"

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

    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT MAX(a) FROM l GROUP BY 1 HAVING EXISTS (SELECT 1 FROM r WHERE d < b)"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnLateralTable1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE a = d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testExistsWithCorrelatedOnLateralTable2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("x", 'i, 'j, 'k)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1) " +
      "WHERE EXISTS (SELECT * FROM x WHERE l.b = x.j))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnLateralTable3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("x", 'i, 'j, 'k)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM (SELECT * FROM r WHERE EXISTS (" +
      "SELECT * FROM x WHERE x.j = l.b)) t, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testExistsWithCorrelatedOnLateralTable4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("x", 'i, 'j, 'k)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE EXISTS (" +
      "SELECT * FROM (SELECT * FROM r LEFT JOIN LATERAL TABLE(table_func(f)) AS T(f1) ON TRUE " +
      "WHERE EXISTS (SELECT i FROM x WHERE l.b = x.j)))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInExists1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t1", 'i, 'j, 'k)
    util.addTable[(Int, Long, String)]("t2", 'l, 'm, 'n)

    val sqlQuery = "SELECT * FROM l WHERE EXISTS (SELECT * FROM r WHERE l.a = r.d)" +
      " AND a IN (SELECT i FROM t1 WHERE l.b = t1.j)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInExists2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t1", 'i, 'j, 'k)
    util.addTable[(Int, Long, String)]("t2", 'l, 'm, 'n)

    val sqlQuery = "SELECT * FROM l WHERE b IN (SELECT j FROM t1) " +
      " AND EXISTS (SELECT * FROM r WHERE l.a = r.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInExists3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t1", 'i, 'j, 'k)
    util.addTable[(Int, Long, String)]("t2", 'l, 'm, 'n)

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT c FROM l WHERE (" +
      " (CASE WHEN EXISTS (SELECT * FROM t1 WHERE l.a = t1.i) THEN 1 ELSE 2 END), " +
      " (CASE WHEN b IN (SELECT m FROM t2) THEN 3 ELSE 4 END)) " +
      " IN (SELECT d, e FROM r)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

}

object SubQuerySemiJoinTest {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}

class TableFunc extends TableFunction[String] {
  def eval(str: String): Unit = {
    if (str.contains("#")){
      str.split("#").foreach(collect)
    }
  }
}
