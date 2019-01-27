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

package org.apache.flink.table.plan.batch.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api.TableConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.util.TableTestBase
import org.junit.{Before, Test}

class BroadcastHashJoinTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getConf.setBoolean(
      TableConfigOptions.SQL_OPTIMIZER_JOIN_REORDER_ENABLED, true)
    util.tableEnv.getConfig.getConf.setString(
      TableConfigOptions.SQL_EXEC_DISABLED_OPERATORS, "SortMergeJoin")
  }

  @Test
  def testInnerJoin(): Unit = {
    val table1 = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    val table2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val result = table1.join(table2).where("a === d").select('c, 'g)
    util.verifyPlan(result)
  }

  @Test
  def testInnerJoinWithFilter(): Unit = {
    val table1 = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    val table2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val result = table1.join(table2).where("a === d && d < 2").select('c, 'g)
    util.verifyPlan(result)
  }

  @Test
  def testInnerJoinWithNonEquiJoinPredicate(): Unit = {
    val table1 = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    val table2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val result = table1.join(table2).where("a === d && d < 2 && b < h").select('c, 'g)
    util.verifyPlan(result)
  }

  @Test
  def testJoinWithMultipleKeys(): Unit = {
    val table1 = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    val table2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val result = table1.join(table2).where("a === d && b === e").select('c, 'g)
    util.verifyPlan(result)
  }

  @Test
  def testFullOuterJoin(): Unit = {
    val table1 = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    val table2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val result = table1.fullOuterJoin(table2, "b === e").select('c, 'g)
    // full outer join cannot be converted to BroadcastJoin
    util.verifyPlan(result)
  }

  @Test
  def testLeftOuterJoin(): Unit = {
    val table1 = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    val table2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val result = table1.leftOuterJoin(table2, "b === e").select('c, 'g)
    util.verifyPlan(result)
  }

  @Test
  def testRightOuterJoin(): Unit = {
    val table1 = util.addTable[(Int, Long, String)]("Table3", 'a, 'b, 'c)
    val table2 = util.addTable[(Int, Long, Int, String, Long)]("Table5", 'd, 'e, 'f, 'g, 'h)

    val result = table1.rightOuterJoin(table2, "b === e").select('c, 'g)
    // join-reorder will change "RightOuterJoin" to "LeftOuterJoin"
    util.verifyPlan(result)
  }
}
