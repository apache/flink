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

class SingleRowJoinTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.getTableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, true)
    util.addTable[(Int, Long)]("A", 'a1, 'a2)
    util.addTable[(Int, Long)]("B", 'b1, 'b2)
  }

  @Test
  def testSingleRowCrossJoin(): Unit = {
    val query = "SELECT a1, asum FROM A, (SELECT sum(a1) + sum(a2) AS asum FROM A)"
    util.verifyPlan(query)
  }

  @Test
  def testSingleRowEquiJoin(): Unit = {
    val query = "SELECT a1, a2 FROM A, (SELECT count(a1) AS cnt FROM A) WHERE a1 = cnt"
    util.verifyPlan(query)
  }

  @Test
  def testSingleRowJoinWithComplexPredicate(): Unit = {
    val query = "SELECT a1, a2, b1, b2 FROM A, (SELECT min(b1) AS b1, max(b2) AS b2 FROM B) " +
      "WHERE a1 < b1 AND a2 = b2"
    util.verifyPlan(query)
  }

  @Test
  def testRightSingleLeftJoinEqualPredicate(): Unit = {
    val queryLeftJoin = "SELECT a2 FROM A LEFT JOIN " +
      "(SELECT COUNT(*) AS cnt FROM B) AS x ON a1 = cnt"
    util.verifyPlan(queryLeftJoin)
  }

  @Test
  def testSingleRowJoinInnerJoin(): Unit = {
    val query = "SELECT a2, sum(a1) FROM A GROUP BY a2 " +
      "HAVING sum(a1) > (SELECT sum(a1) * 0.1 FROM A)"
    util.verifyPlan(query)
  }
}
