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

package org.apache.flink.table.planner.plan.batch.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class SingleRowJoinTest extends TableTestBase {

  @Test
  def testSingleRowCrossJoin(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Int, Int)]("A", 'a1, 'a2)
    util.verifyExecPlan("SELECT a1, a_sum FROM A, (SELECT SUM(a1) + SUM(a2) AS a_sum FROM A)")
  }

  @Test
  def testSingleRowNotEquiJoin(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Int, String)]("A", 'a1, 'a2)
    util.verifyExecPlan("SELECT a1, a2 FROM A, (SELECT COUNT(a1) AS cnt FROM A) WHERE a1 < cnt")
  }

  @Test
  def testSingleRowJoinWithComplexPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Int, Long)]("A", 'a1, 'a2)
    util.addTableSource[(Int, Long)]("B", 'b1, 'b2)
    val query =
      """
        |SELECT a1, a2, b1, b2 FROM A,
        |  (SELECT min(b1) AS b1, max(b2) AS b2 FROM B)
        |WHERE a1 < b1 AND a2 = b2
      """.stripMargin
    util.verifyExecPlan(query)
  }

  @Test
  def testRightSingleLeftJoinEqualPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Long, Int)]("A", 'a1, 'a2)
    util.addTableSource[(Int, Int)]("B", 'b1, 'b2)
    util.verifyExecPlan(
      "SELECT a2 FROM A LEFT JOIN (SELECT COUNT(*) AS cnt FROM B) AS x  ON a1 = cnt")
  }

  @Test
  def testRightSingleLeftJoinNotEqualPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Long, Int)]("A", 'a1, 'a2)
    util.addTableSource[(Int, Int)]("B", 'b1, 'b2)
    util.verifyExecPlan(
      "SELECT a2 FROM A LEFT JOIN (SELECT COUNT(*) AS cnt FROM B) AS x ON a1 > cnt")
  }

  @Test
  def testLeftSingleRightJoinEqualPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Long, Long)]("A", 'a1, 'a2)
    util.addTableSource[(Long, Long)]("B", 'b1, 'b2)
    util.verifyExecPlan("SELECT a1 FROM (SELECT COUNT(*) AS cnt FROM B) RIGHT JOIN A ON cnt = a2")
  }

  @Test
  def testLeftSingleRightJoinNotEqualPredicate(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Long, Long)]("A", 'a1, 'a2)
    util.addTableSource[(Long, Long)]("B", 'b1, 'b2)
    util.verifyExecPlan("SELECT a1 FROM (SELECT COUNT(*) AS cnt FROM B) RIGHT JOIN A ON cnt < a2")
  }

  @Test
  def testSingleRowJoinInnerJoin(): Unit = {
    val util = batchTestUtil()
    util.addTableSource[(Int, Int)]("A", 'a1, 'a2)
    val sql = "SELECT a2, SUM(a1) FROM A GROUP BY a2 HAVING SUM(a1) > (SELECT SUM(a1) * 0.1 FROM A)"
    util.verifyExecPlan(sql)
  }
}
