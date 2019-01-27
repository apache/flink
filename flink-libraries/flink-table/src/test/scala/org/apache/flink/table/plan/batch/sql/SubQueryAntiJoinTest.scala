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
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.functions.TableFunction
import org.apache.flink.table.api.scala._
import org.apache.flink.table.plan.rules.logical.SubQueryTestBase
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

/**
  * Tests for NOT IN and NOT EXISTS.
  */
@RunWith(classOf[Parameterized])
class SubQueryAntiJoinTest(fieldsNullable: Boolean) extends SubQueryTestBase(fieldsNullable) {

  @Test
  def testNotInOnWhere_NotSubQuery(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    val sqlQuery = "SELECT * FROM l WHERE a NOT IN (1, 2, 3, 4)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a NOT IN (SELECT c FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a NOT IN (SELECT c FROM r WHERE d < 100) AND b > 10"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a + 1 NOT IN (SELECT c FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere4(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a * b NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere5(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE CAST(a AS BIGINT) NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere6(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a NOT IN (SELECT c FROM r WHERE c IS NOT NULL)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere7(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT a FROM l l1 WHERE a NOT IN (SELECT a FROM l WHERE a < 3 GROUP BY a)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_ComplexCondition1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' AND a IN (SELECT d FROM r))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_ComplexCondition2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE NOT (a IN (SELECT d FROM r))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_ComplexCondition3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE NOT ( NOT ( a NOT IN (SELECT d FROM r)))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_ComplexCondition4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE NOT (a IN (SELECT d FROM r) OR b > 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_ComplexCondition5(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND (c like 'abc' AND a NOT IN (SELECT d FROM r))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_UnsupportedCondition(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // these queries will not be converted to SemiJoin
    val sqlQuery1 = "SELECT * FROM l WHERE a NOT IN (SELECT c FROM r) OR b = 10"
    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT * FROM l WHERE a NOT IN " +
      "(SELECT c FROM r WHERE l.b NOT IN (SELECT e FROM t))"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")

    val sqlQuery3 = "SELECT * FROM l, r WHERE " +
      "l.a = r.c AND (r.d NOT IN (SELECT d FROM r) OR l.a >= 1)"
    util.verifySqlNotExpected(sqlQuery3, "SemiJoin")

    val sqlQuery4 = "SELECT * FROM l WHERE (a NOT IN (SELECT c FROM r)) IS TRUE"
    if (fieldsNullable) {
      util.verifySqlNotExpected(sqlQuery4, "SemiJoin")
    } else {
      // FilterSimplifyExpressionsRule will reduce `(a NOT IN (SELECT c FROM r)) IS TRUE`
      // to `a NOT IN (SELECT c FROM r)`
      util.verifyPlan(sqlQuery4)
    }

    val sqlQuery5 = "SELECT * FROM l WHERE (a NOT IN (SELECT c FROM r) = true)"
    util.verifySqlNotExpected(sqlQuery5, "SemiJoin")

    val sqlQuery6 = "SELECT * FROM l WHERE " +
      "a NOT IN (SELECT c FROM r) OR b NOT IN (SELECT e FROM t)"
    util.verifySqlNotExpected(sqlQuery6, "SemiJoin")

    val sqlQuery7 = "SELECT * FROM l WHERE a NOT IN (SELECT l.b FROM r)"
    util.verifySqlNotExpected(sqlQuery7, "SemiJoin")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a > 10 THEN 1 ELSE 2 END) NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a NOT IN (SELECT 1 FROM t1) THEN 1 ELSE 2 END) NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1 ELSE 2 END) NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)

  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1" +
      " WHEN a NOT IN (SELECT j FROM t2) THEN 2 ELSE 3 END)" +
      " NOT IN (SELECT d FROM r)"
    util.verifyPlan(sqlQuery)

  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case5(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE (b, " +
      "(CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1" +
      " WHEN a NOT IN (SELECT j FROM t2) THEN 2 ELSE 3 END))" +
      " NOT IN (SELECT e, d FROM r)"
    util.verifyPlan(sqlQuery)

  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case6(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT c FROM l WHERE (" +
      " (CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1 ELSE 2 END), " +
      " (CASE WHEN b NOT IN (SELECT j FROM t2) THEN 3 ELSE 4 END)) " +
      " NOT IN (SELECT d, e FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_Case7(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT b FROM l WHERE " +
      "(CASE WHEN a NOT IN (SELECT i FROM t1 WHERE l.a = t1.i) THEN 1 ELSE 2 END) " +
      "NOT IN (SELECT d FROM r)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_MultiFields1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a, c) NOT IN (SELECT d, f FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_MultiFields2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a + 1, c) NOT IN (SELECT d, f FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedOnWhere_MultiFields3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "(a + 10, TRIM(c)) NOT IN (SELECT d + 100, SUBSTRING(f, 1, 5) FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiNotInWithUncorrelatedOnWhere_OR(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // NOT IN in OR condition
    val sqlQuery = "SELECT * FROM l WHERE " +
      "a NOT IN (SELECT c FROM r) OR b NOT IN (SELECT f FROM t WHERE f IS NOT NULL)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testNotInWithUncorrelatedOnLateralTable(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE c NOT IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a NOT IN (SELECT c FROM r where l.b = r.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE b > 1 AND a NOT IN " +
      "(SELECT c FROM r WHERE l.b = r.d AND r.c > 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE b > 1 AND a + 1 NOT IN " +
      "(SELECT c FROM r WHERE l.b = r.d AND r.c > 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere4(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a NOT IN " +
      "(SELECT c1 FROM (SELECT d * 2 as d1, c + 1 as c1 FROM r) r1 WHERE l.b = r1.d1 AND d1 > 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere5(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a NOT IN (SELECT c FROM r WHERE l.b > r.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere6(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE a NOT IN (SELECT c FROM r WHERE l.b > 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_Case1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a > 10 THEN 1 ELSE 2 END) NOT IN (SELECT d FROM r WHERE l.a = r.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_Case2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a IN (SELECT 1 FROM t1) THEN 1 ELSE 2 END)" +
      " NOT IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_Case3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1 ELSE 2 END)" +
      " NOT IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_Case4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE" +
      " (CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1" +
      " WHEN a NOT IN (SELECT j FROM t2) THEN 2 ELSE 3 END)" +
      " NOT IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_Case5(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO currently, FlinkSubQueryRemoveRule does not support SubQuery on Project.
    val sqlQuery = "SELECT b FROM l WHERE (b, " +
      "(CASE WHEN a NOT IN (SELECT i FROM t1) THEN 1" +
      " WHEN a NOT IN (SELECT j FROM t2) THEN 2 ELSE 3 END))" +
      " NOT IN (SELECT e, d FROM r WHERE l.c = r.f)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_Case6(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int)]("t1", 'i)
    util.addTable[(Int)]("t2", 'j)

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT b FROM l WHERE " +
      "(CASE WHEN a NOT IN (SELECT i FROM t1 WHERE l.a = t1.i) THEN 1 ELSE 2 END)" +
      " NOT IN (SELECT d FROM r WHERE l.c = r.f)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testNotInWithCorrelatedOnWhere_ComplexCondition1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE a NOT IN " +
      "(SELECT CAST(e AS INTEGER) FROM r where CAST(l.b AS INTEGER) = CAST(r.d AS INTEGER))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_ComplexCondition2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT a + 10, c FROM l WHERE " +
      "a + 10 NOT IN (SELECT d + 1 FROM r WHERE l.b + 1 = r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_ComplexCondition3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE" +
      " c NOT IN (SELECT TRIM(f) FROM r where l.a = r.d + 1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_ComplexCondition4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND NOT (c like 'abc' AND a IN (SELECT d FROM r where l.b = r.e))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testNotInWithCorrelatedOnWhere_ComplexCondition5(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "b > 10 AND (c like 'abc' AND a NOT IN (SELECT d FROM r where l.b = r.e))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_ComplexCondition6(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE b NOT IN " +
      "(SELECT e * 5 FROM (SELECT SUM(e) AS e FROM r WHERE l.a = r.d) r1)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_UnsupportedCondition1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    // these queries will not be converted to SemiJoin
    val sqlQuery2 = "SELECT * FROM l WHERE b NOT IN (SELECT d FROM r WHERE l.a = r.c OR r.c = 10)"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")

    val sqlQuery3 = "SELECT * FROM l WHERE b NOT IN (SELECT d FROM r WHERE l.a = r.c OR l.b = 10)"
    util.verifySqlNotExpected(sqlQuery3, "SemiJoin")

    val sqlQuery4 = "SELECT * FROM l WHERE b NOT IN (SELECT d FROM r WHERE l.a = r.c) OR l.a = 10"
    util.verifySqlNotExpected(sqlQuery4, "SemiJoin")

    val sqlQuery7 = "SELECT * FROM l WHERE a NOT IN (SELECT l.b FROM r WHERE l.a = r.c)"
    util.verifySqlNotExpected(sqlQuery7, "SemiJoin")
  }

  @Test(expected = classOf[TableException])
  def testNotInWithCorrelatedOnWhere_UnsupportedCondition2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long)]("t", 'i, 'j, 'k)

    // TODO java.lang.RuntimeException: While invoking method
    // 'public RelDecorrelator$Frame RelDecorrelator.decorrelateRel(LogicalProject)'
    val sqlQuery = "SELECT * FROM l WHERE a NOT IN " +
      "(SELECT d FROM r WHERE l.b NOT IN (SELECT j FROM t) WHERE l.c = r.f)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testNotInWithCorrelatedOnWhere_MultiFields1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a, c) NOT IN (SELECT d, f FROM r WHERE l.b = r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_MultiFields2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a, SUBSTRING(c, 1, 5)) NOT IN " +
      "(SELECT d, TRIM(f) FROM r WHERE l.b = r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnWhere_MultiFields3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE (a + 10, SUBSTRING(c, 1, 5)) NOT IN " +
      "(SELECT d + 100, TRIM(f) FROM r WHERE l.b = r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedAndCorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // two NOT IN in AND condition, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT a FROM l WHERE" +
      " a NOT IN (SELECT c FROM r WHERE l.b = r.d) AND b NOT IN (SELECT f FROM t)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedAndCorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "a NOT IN ( SELECT c FROM r) AND b NOT IN (SELECT e FROM t WHERE t.f = l.b)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedAndCorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    // two nested IN, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT * FROM l WHERE a NOT IN (" +
      "SELECT c FROM r WHERE d NOT IN (SELECT f FROM t) AND l.b = r.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithUncorrelatedAndCorrelatedOnWhere4(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT * FROM l WHERE a NOT IN (" +
      "SELECT c FROM r WHERE d NOT IN (SELECT f FROM t WHERE t.e = r.c))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInWithCorrelatedOnLateralTable(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE c NOT IN (" +
      "SELECT f1 FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE a = d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r where c > 0) AND a < 100"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE NOT (NOT (NOT EXISTS (SELECT * FROM r))) AND l.b = 10"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere4(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r) OR l.b = 10"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere5(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE (NOT EXISTS (SELECT d FROM r)) IS TRUE"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere6(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    // TODO the result of SqlToRelConverter does not contain any field `b` info in SubQuery
    val sqlQuery = "SELECT a FROM l WHERE NOT EXISTS (SELECT l.b IS NULL FROM r)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithUncorrelatedOnWhere_UnsupportedCondition(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)

    val sqlQuery1 = "SELECT * FROM l WHERE (NOT EXISTS (SELECT d FROM r)) IS NOT NULL"
    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT * FROM l WHERE (NOT EXISTS (SELECT d FROM r)) = true"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")
  }

  @Test
  def testMultiNotExistsWithUncorrelatedOnWhere_AND1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.c " +
      "AND NOT EXISTS (SELECT * FROM t t1 WHERE t1.e > 50) " +
      "AND b >= 1 " +
      "AND NOT EXISTS (SELECT * FROM t t2 WHERE t2.f < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithUncorrelatedOnWhere_AND2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)
    util.addTable[(Int, Long)]("t", 'e, 'f)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.c " +
      "AND NOT EXISTS (SELECT * FROM t t1 WHERE t1.e > 50) " +
      "AND NOT (b >= 1 " +
      "AND NOT EXISTS (SELECT * FROM t t2 WHERE t2.f < 100))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testMultiNotExistsWithUncorrelatedOnWhere_OR(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND (NOT EXISTS (SELECT * FROM t t1 WHERE t1.k > 50) " +
      "OR NOT EXISTS (SELECT * FROM t t2 WHERE t2.j < 100))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testNotExistsWithUncorrelatedOnLateralTable(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE a = c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE a = c) AND l.a > 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE" +
      " NOT EXISTS (SELECT 1, c + d, c + 2, d FROM r WHERE a = c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere4(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE l.a = c and l.b > 10)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere5(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT l1.a FROM l l1, r WHERE l1.b = r.d AND l1.a < 10 AND r.c < 15 " +
      " AND NOT EXISTS (SELECT * FROM l l2 WHERE l1.b = l2.b)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere6(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE l.a < r.c)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere7(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery = "SELECT * FROM l WHERE (NOT EXISTS (SELECT d FROM r WHERE r.d = l.b)) IS TRUE"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere8(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    // TODO the result of SqlToRelConverter does not contain any field `a` info in SubQuery
    val sqlQuery = "SELECT a FROM l WHERE NOT EXISTS (SELECT l.a IS NULL FROM r WHERE r.d = l.b)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnWhere_UnsupportedCondition(): Unit = {
    util.addTable[(Int, Long)]("l", 'a, 'b)
    util.addTable[(Int, Long)]("r", 'c, 'd)

    val sqlQuery1 = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE l.a = r.c OR r.c = 10)"
    util.verifySqlNotExpected(sqlQuery1, "SemiJoin")

    val sqlQuery2 = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE l.a = r.c OR l.b = 10)"
    util.verifySqlNotExpected(sqlQuery2, "SemiJoin")

    val sqlQuery3 = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE l.a = r.c) OR l.b = 10"
    util.verifySqlNotExpected(sqlQuery3, "SemiJoin")

    val sqlQuery4 = "SELECT * FROM l WHERE" +
      " (NOT EXISTS (SELECT d FROM r WHERE r.d = l.b)) IS NOT NULL"
    util.verifySqlNotExpected(sqlQuery4, "SemiJoin")

    val sqlQuery5 = "SELECT * FROM l WHERE (NOT EXISTS (SELECT d FROM r WHERE r.d = l.b)) = true"
    util.verifySqlNotExpected(sqlQuery5, "SemiJoin")
  }

  @Test(expected = classOf[RuntimeException])
  def testNotExistsWithCorrelatedOnWhere_UnsupportedCondition2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // TODO Calcite decorrelateRel error
    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS " +
      " (SELECT * FROM (SELECT * FROM r WHERE r.d = l.a AND r.e > 100) s " +
      "LEFT JOIN t ON s.f = t.k AND l.b = t.j)"
    util.printSql(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testMultiNotExistsWithCorrelatedOnWhere_NestedCorrelation(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // SQL from Calcite Test
    // however, parse result is not correct
    val sqlQuery = "SELECT * FROM (SELECT 2 + a d2, 3 + b d3 FROM l) e" +
    " WHERE NOT EXISTS (SELECT 1 FROM (SELECT d + 1 d1 FROM r) d" +
    " WHERE d1 = e.d2 AND NOT EXISTS (SELECT 2 FROM " +
    "(SELECT i + 4 d4, i + 5 d5, i + 6 d6 FROM t)" +
    " WHERE d4 = d.d1 AND d5 = d.d1 AND d6 = e.d3))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testMultiNotExistsWithCorrelatedOnWhere_AND1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND NOT EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND c >= 1 " +
      "AND NOT EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithCorrelatedOnWhere_AND2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND NOT EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND (c >= 1 " +
      "AND NOT EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithCorrelatedOnWhere_AND3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND NOT EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "AND NOT (c >= 1 " +
      "AND NOT EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testMultiNotExistsWithCorrelatedOnWhere_OR(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l, r WHERE l.a = r.d " +
      "AND (NOT EXISTS (SELECT * FROM t t1 WHERE l.b = t1.j AND t1.k > 50) " +
      "OR NOT EXISTS (SELECT * FROM t t2 WHERE l.a = t2.i AND t2.j < 100))"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testMultiNotExistsWithUncorrelatedAndCorrelatedOnWhere1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // two EXISTS in AND condition, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT * FROM l WHERE " +
      "NOT EXISTS (SELECT * FROM r) AND NOT EXISTS (SELECT * FROM t WHERE l.a = t.i AND t.j < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithUncorrelatedAndCorrelatedOnWhere2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE " +
      "NOT EXISTS (SELECT * FROM r WHERE l.a <> r.d) AND NOT EXISTS (SELECT * FROM t WHERE j < 100)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithUncorrelatedAndCorrelatedOnWhere3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    // two nested EXISTS, one is uncorrelation subquery, another is correlation subquery
    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (" +
      "SELECT * FROM r WHERE NOT EXISTS (SELECT * FROM t) AND l.a = r.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testMultiNotExistsWithUncorrelatedAndCorrelatedOnWhere4(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t", 'i, 'j, 'k)

    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (" +
      "SELECT * FROM r WHERE NOT EXISTS (SELECT * FROM t WHERE r.d = t.i))"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotExistsWithCorrelatedOnLateralTable(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addFunction("table_func", new TableFunc)
    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (" +
      "SELECT * FROM r, LATERAL TABLE(table_func(f)) AS T(f1) WHERE a = d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInNotExists1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t1", 'i, 'j, 'k)
    util.addTable[(Int, Long, String)]("t2", 'l, 'm, 'n)

    val sqlQuery = "SELECT * FROM l WHERE NOT EXISTS (SELECT * FROM r WHERE l.a = r.d)" +
      " AND a NOT IN (SELECT i FROM t1 WHERE l.b = t1.j)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInNotExists2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t1", 'i, 'j, 'k)
    util.addTable[(Int, Long, String)]("t2", 'l, 'm, 'n)

    val sqlQuery = "SELECT * FROM l WHERE b NOT IN (SELECT j FROM t1) " +
      " AND NOT EXISTS (SELECT * FROM r WHERE l.a = r.d)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testNotInNotExists3(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t1", 'i, 'j, 'k)
    util.addTable[(Int, Long, String)]("t2", 'l, 'm, 'n)

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT c FROM l WHERE (" +
      " (CASE WHEN NOT EXISTS (SELECT * FROM t1 WHERE l.a = t1.i) THEN 1 ELSE 2 END), " +
      " (CASE WHEN b NOT IN (SELECT m FROM t2) THEN 3 ELSE 4 END)) " +
      "  NOT IN (SELECT d, e FROM r)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

  @Test
  def testInNotInExistsNotExists1(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t1", 'i, 'j, 'k)
    util.addTable[(Int, Long, String)]("t2", 'l, 'm, 'n)

    // FilterSimplifyExpressionsRule will reorder the conditions
    val sqlQuery = "SELECT * FROM l WHERE a IN (SELECT d FROM r) " +
      " AND b NOT IN (SELECT i FROM t1 WHERE l.c = t1.k AND i > 10) " +
      " AND EXISTS (SELECT COUNT(l) FROM t2 where n like 'Test' GROUP BY l)" +
      " AND NOT EXISTS (SELECT * FROM r WHERE l.b <> r.e)"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInNotInExistsNotExists2(): Unit = {
    util.addTable[(Int, Long, String)]("l", 'a, 'b, 'c)
    util.addTable[(Int, Long, String)]("r", 'd, 'e, 'f)
    util.addTable[(Int, Long, String)]("t1", 'i, 'j, 'k)
    util.addTable[(Int, Long, String)]("t2", 'l, 'm, 'n)

    // TODO Calcite does not support project with correlated expressions.
    val sqlQuery = "SELECT c FROM l WHERE (" +
      " (CASE WHEN b IN (SELECT j FROM t1 WHERE l.a = t1.i) THEN 1 ELSE 2 END), " +
      " (CASE WHEN NOT EXISTS (SELECT m FROM t2) THEN 3 " +
      "       WHEN EXISTS (select i FROM t1) THEN 4 ELSE 5 END)) " +
      "  NOT IN (SELECT d, e FROM r)"
    util.verifySqlNotExpected(sqlQuery, "SemiJoin")
  }

}

object SubQueryAntiJoinTest {
  @Parameterized.Parameters(name = "{0}")
  def parameters(): java.util.Collection[Boolean] = {
    java.util.Arrays.asList(true, false)
  }
}

