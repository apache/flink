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
package org.apache.flink.table.planner.plan.stream.sql

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.{Before, Test}

class RemoveShuffleTest extends TableTestBase {

  private val util = streamTestUtil()

  @Before
  def setup(): Unit = {
    util.addTableSource[(Int, Long, String)]("T1", 'a1, 'b1, 'c1)
    util.addTableSource[(Int, Long, String)]("T2", 'a2, 'b2, 'c2)
    util.addTableSource[(Int, Long, String)]("T3", 'a3, 'b3, 'c3)
    util.addTableSource[(Int, Long, String)]("T4", 'a4, 'b4, 'c4)
    util.addTableSource[(Int, Long, String)]("T5", 'a5, 'b5, 'c5, 'proctime.proctime)
    util.addTable("""
                    |CREATE TABLE L (
                    |  `id` INT,
                    |  `name` STRING,
                    |  `age` INT
                    |) WITH (
                    |  'connector' = 'values',
                    |  'lookup.cache' = 'LRU',
                    |  'lookup.cache.partitioned' = 'true'
                    |)
                    |""".stripMargin)
  }

  @Test
  def testMultipleInnerJoins(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4
         |WHERE a1 = a2 AND a2 = a3 AND a3 = a4
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleInnerJoinsWithMultipleKeys(): Unit = {
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4
         |WHERE a1 = a2 AND b1 = b2 AND a2 = a3 AND b3 = b2 AND a3 = a4 AND b3 = b4
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleInnerJoinsWithPartialKey(): Unit = {
    // partial keys can not be pushed down
    val sql =
      s"""
         |SELECT * FROM T1, T2, T3, T4
         |WHERE a1 = a2 AND a2 = a3 AND a3 = a4 AND b3 = b4
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleLeftJoins(): Unit = {
    val sql =
      s"""
         |SELECT * FROM
         | (SELECT * FROM
         |   (SELECT * FROM T1 LEFT JOIN T2 ON a1 = a2) TMP1
         |     LEFT JOIN T3 ON a1 = a3) TMP2
         | LEFT JOIN T4 ON a1 = a4
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleLeftJoinsWithJoinKeyWithRightSide(): Unit = {
    // join key from outer side will not be pushed down
    val sql =
      s"""
         |SELECT * FROM
         |  (SELECT * FROM T1 LEFT JOIN T2 ON a1 = a2) TMP
         |    LEFT JOIN T3 ON a2 = a3
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleRightJoinsWithJoinKeyWithLeftSide(): Unit = {
    // join key from outer side will not be pushed down
    val sql =
      s"""
         |SELECT * FROM
         |  (SELECT * FROM T1 RIGHT JOIN T2 ON a1 = a2) TMP
         |    RIGHT JOIN T3 ON a1 = a3
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleFullJoins(): Unit = {
    // join key from outer side will not be pushed down
    val sql =
      s"""
         |SELECT * FROM
         |  (SELECT * FROM T1 FULL JOIN T2 ON a1 = a2) TMP
         |    FULL JOIN T3 ON a1 = a3
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleGroupAggWithEmptyKey(): Unit = {
    // singleton distribution will not be pushed down
    val sql =
      s"""
         |SELECT SUM(b) FROM (SELECT COUNT(b1) AS b FROM T1) t
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testMultipleGroupAggWithSameKeys(): Unit = {
    val sql =
      s"""
         |SELECT a1, count(*) FROM (SELECT a1 FROM T1 GROUP BY a1) t GROUP BY a1
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testJoinWithAggs(): Unit = {
    val query1 = "SELECT a1, SUM(b1) AS b1 FROM T1 GROUP BY a1"
    val query2 = "SELECT a2, SUM(b2) AS b2 FROM T2 GROUP BY a2"
    val query = s"SELECT * FROM ($query1) LEFT JOIN ($query2) ON a1 = a2"
    util.verifyExecPlan(query)
  }

  @Test
  def testJoinWithAggsWithCalc(): Unit = {
    // Calc also support required distribution push down
    val query1 = "SELECT SUM(b1) AS b1, a1 FROM T1 GROUP BY a1"
    val query2 = "SELECT SUM(b2) AS b2, a2 FROM T2 GROUP BY a2"
    val query = s"SELECT * FROM ($query1) LEFT JOIN ($query2) ON a1 = a2"
    util.verifyExecPlan(query)
  }

  @Test
  def testAggWithJoinFromLeftKeys(): Unit = {
    val sql =
      s"""
         |SELECT a1, b1, COUNT(a2), SUM(b2) FROM
         |  (SELECT a1, b1, a2, b2 FROM T1 JOIN T2 ON a1 = a2 AND b1 = b2) t
         |GROUP BY a1, b1
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAggWithJoinFromRightKeys(): Unit = {
    val sql =
      s"""
         |SELECT a2, b2, COUNT(a1), SUM(b1) FROM
         |  (SELECT a1, b1, a2, b2 FROM T1 JOIN T2 ON a1 = a2 AND b1 = b2) t
         |GROUP BY b2, a2
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAggWithJoinWithPartialKey(): Unit = {
    // partial keys can not be pushed down
    val sql =
      s"""
         |SELECT a1, MAX(b1), COUNT(a2), SUM(b2) FROM
         |  (SELECT a1, b1, a2, b2 FROM T1 JOIN T2 ON a1 = a2 AND b1 = b2) t
         |GROUP BY a1
         """.stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAggWithSemiJoin(): Unit = {
    val sql =
      s"""
         |SELECT a1, MAX(b1), COUNT(c1)
         |FROM T1
         |WHERE EXISTS
         |  (SELECT 1 FROM T2 WHERE T1.a1 = T2.a2)
         |GROUP BY a1
         |""".stripMargin
    util.verifyExecPlan(sql)
  }

  @Test
  def testAggWithAntiJoin(): Unit = {
    val sql =
      s"""
         |SELECT a1, MAX(b1), COUNT(c1)
         |FROM T1
         |WHERE NOT EXISTS
         |  (SELECT 1 FROM T2 WHERE T1.a1 = T2.a2)
         |GROUP BY a1
         |""".stripMargin
    util.verifyExecPlan(sql)
  }
}
