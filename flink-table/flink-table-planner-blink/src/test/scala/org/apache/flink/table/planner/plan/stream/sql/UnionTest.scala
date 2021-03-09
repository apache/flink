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

// TODO add more union case after aggregation and join supported
class UnionTest extends TableTestBase {

  private val util = batchTestUtil()

  @Before
  def before(): Unit = {
    util.addTableSource[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("MyTable2", 'a, 'b, 'c)
    util.addTableSource[(Int, Long, String)]("MyTable3", 'a, 'b, 'c)
  }

  @Test
  def testUnionAll(): Unit = {
    val sqlQuery =
      """
        |SELECT a, c FROM (
        | SELECT a, c FROM MyTable1
        | UNION ALL
        | SELECT a, c FROM MyTable2
        | UNION ALL
        | SELECT a, c FROM MyTable3
        |) WHERE a > 2
      """.stripMargin
    util.verifyExecPlan(sqlQuery)
  }

  @Test
  def testUnionAllDiffType(): Unit = {
    val sqlQuery =
      """
        |SELECT * FROM (
        | SELECT a, b FROM MyTable1
        | UNION ALL
        | SELECT a, CAST(0 aS DECIMAL(2, 1)) FROM MyTable2)
      """.stripMargin
    util.verifyRelPlanWithType(sqlQuery)
  }

}
