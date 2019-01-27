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
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBase

import org.junit.{Before, Test}

class BroadcastHashJoinTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.getTableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, true)
  }

  @Test
  def testInnerJoin(): Unit = {
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE a = d"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInnerJoinWithFilter(): Unit = {
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3, Table5 WHERE a = d AND d < 2"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table5 INNER JOIN Table3 ON a = d AND d < 2 AND b < h"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testJoinWithMultipleKeys(): Unit = {
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table5 INNER JOIN Table3 ON a = d AND b = e"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testFullOuterJoin(): Unit = {
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3 FULL OUTER JOIN Table5 ON b = e"
    // full outer join cannot be converted to BroadcastJoin
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3 LEFT OUTER JOIN Table5 ON b = e"
    util.verifyPlan(sqlQuery)
  }

  @Test
  def testRightOuterJoin(): Unit = {
    util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val sqlQuery = "SELECT c, g FROM Table3 RIGHT OUTER JOIN Table5 ON b = e"
    util.verifyPlan(sqlQuery)
  }
}
