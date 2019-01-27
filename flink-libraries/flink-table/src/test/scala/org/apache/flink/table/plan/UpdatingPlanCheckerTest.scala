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

package org.apache.flink.table.plan

import org.apache.flink.table.api.Table
import org.apache.flink.table.api.stream.sql.StreamPlanTestBase
import org.apache.flink.table.plan.util.UpdatingPlanChecker
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

import org.junit.Assert.assertEquals
import org.junit.Test

class UpdatingPlanCheckerTest extends StreamPlanTestBase {

  streamUtil.addTable[(Int, String, Long)](
    "MyTable", 'a, 'b, 'c, 'proctime.proctime, 'rowtime.rowtime)
  streamUtil.addTable[(Int, Long, String)](
    "AA", 'a1, 'a2, 'a3)
  streamUtil.addTable[(Int, Long, Int, String, Long)](
    "BB", 'b1, 'b2, 'b3, 'b4, 'b5)

  @Test
  def testAppend(): Unit = {
    val query = "SELECT * from AA "
    verifySqlAppendOnly(query, true)
  }

  @Test
  def testGroupBy(): Unit = {
    val query = "SELECT a1, max(a3) from (SELECT a1, a2, max(a3) as a3 FROM A GROUP BY a1, a2) " +
      "group by a1"
    verifySqlAppendOnly(query, false)
  }

  @Test
  def testInnerJoin(): Unit = {
    val query = "SELECT a1, b1 FROM AA JOIN BB ON a1 = b1"
    verifySqlAppendOnly(query, true)
  }

  @Test
  def testLeftJoin(): Unit = {
    val query = "SELECT a1, b1 FROM AA LEFT JOIN BB ON a1 = b1 AND a2 > b2"
    verifySqlAppendOnly(query, false)
  }

  @Test
  def testRightJoin(): Unit = {
    val query = "SELECT a1, b1 FROM AA RIGHT JOIN BB ON a1 = b1 AND a2 > b2"
    verifySqlAppendOnly(query, false)
  }

  @Test
  def testFullJoin(): Unit = {
    val query = "SELECT a1, b1 FROM AA FULL JOIN BB ON a1 = b1 AND a2 > b2"
    verifySqlAppendOnly(query, false)
  }

  @Test
  def testAntiJoin(): Unit = {
    val query = "SELECT * FROM A WHERE NOT EXISTS (SELECT b1 from B WHERE a1 = b1)"
    verifySqlAppendOnly(query, false)
  }

  @Test
  def testSemiJoin(): Unit = {
    val query = "SELECT * FROM A WHERE EXISTS (SELECT b1 from B WHERE a1 = b1)"
    verifySqlAppendOnly(query, true)
  }

  @Test
  def testRank(): Unit = {
    val query =
      """
        |SELECT *
        |FROM (
        |  SELECT a, b, c,
        |      ROW_NUMBER() OVER (PARTITION BY a ORDER BY b DESC) as rank_num
        |  FROM MyTable)
        |WHERE rank_num <= 10
      """.stripMargin
    verifySqlAppendOnly(query, false)
  }

  def verifySqlAppendOnly(query: String, isUpdating: Boolean): Unit = {
    verifyTableAppendOnly(streamUtil.tableEnv.sqlQuery(query), isUpdating)
  }

  def verifyTableAppendOnly(resultTable: Table, expected: Boolean): Unit = {
    val relNode = resultTable.getRelNode
    val optimized = streamUtil.tableEnv.optimize(relNode, updatesAsRetraction = false)
    assertEquals(expected, UpdatingPlanChecker.isAppendOnly(optimized))
  }
}
