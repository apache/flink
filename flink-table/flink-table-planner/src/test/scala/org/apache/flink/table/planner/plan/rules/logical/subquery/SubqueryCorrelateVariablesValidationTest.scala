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

import org.junit.Test

import java.sql.{Date, Timestamp}

class SubqueryCorrelateVariablesValidationTest extends SubQueryTestBase {

  util.addTableSource[(String, Short, Int, Long, Float, Double, BigDecimal, Timestamp, Date)](
    "t1",
    't1a,
    't1b,
    't1c,
    't1d,
    't1e,
    't1f,
    't1g,
    't1h,
    't1i)
  util.addTableSource[(String, Short, Int, Long, Float, Double, BigDecimal, Timestamp, Date)](
    "t2",
    't2a,
    't2b,
    't2c,
    't2d,
    't2e,
    't2f,
    't2g,
    't2h,
    't2i)
  util.addTableSource[(String, Short, Int, Long, Float, Double, BigDecimal, Timestamp, Date)](
    "t3",
    't3a,
    't3b,
    't3c,
    't3d,
    't3e,
    't3f,
    't3g,
    't3h,
    't3i)

  @Test
  def testWithProjectProjectCorrelate(): Unit = {
    val sqlQuery =
      """
        |SELECT (SELECT min(t1.t1d) FROM t3 WHERE t3.t3a = 'test') min_t1d
        |FROM   t1
        |WHERE  t1a = 'test'
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testWithProjectFilterCorrelate(): Unit = {
    val sqlQuery =
      """
        |SELECT (SELECT min(t3d) FROM t3 WHERE t3.t3a = t1.t1a) min_t3d,
        |       (SELECT max(t2h) FROM t2 WHERE t2.t2a = t1.t1a) max_t2h
        |FROM   t1
        |    WHERE  t1a = 'test'
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test(expected = classOf[RuntimeException])
  def testWithProjectJoinCorrelate(): Unit = {
    val sqlQuery =
      """
        |SELECT (SELECT max(t2h) FROM t2
        |    LEFT OUTER JOIN t1 ttt
        |    ON t2.t2a=t1.t1a) max_t2h
        |FROM t1
        |    WHERE  t1a = 'val1b'
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test
  def testWithFilterJoinCorrelate(): Unit = {
    val sqlQuery =
      """
        |SELECT t1a
        |FROM   t1
        |WHERE  EXISTS (SELECT max(t2h) FROM t2
        |               LEFT OUTER JOIN t1 ttt
        |               ON t2.t2a=t1.t1a)
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testWithFilterInCorrelate(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM t1
        |WHERE t1a
        |IN (SELECT t3a
        |    FROM t3
        |    WHERE t1.t1e
        |    IN (select t2e from t2))
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test(expected = classOf[TableException])
  def testWithFilterExistsCorrelate(): Unit = {
    val sqlQuery =
      """
        |SELECT *
        |FROM t1
        |WHERE EXISTS (SELECT *
        |              FROM t3
        |              WHERE EXISTS(select * from t3 WHERE t1.t1a = t3.t3a))
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

  @Test(expected = classOf[AssertionError])
  // TODO some bugs in RelDecorrelator.AdjustProjectForCountAggregateRule
  def testWithProjectCaseWhenCorrelate(): Unit = {
    val sqlQuery =
      """
        |SELECT
        |    (CASE WHEN EXISTS (SELECT min(t3d)
        |                       FROM t3
        |                       WHERE t3.t3a = t1.t1a)
        |     THEN 1 ELSE 2 END)
        |FROM   t1
        |    WHERE  t1a = 'test'
      """.stripMargin
    util.verifyRelPlan(sqlQuery)
  }

}
