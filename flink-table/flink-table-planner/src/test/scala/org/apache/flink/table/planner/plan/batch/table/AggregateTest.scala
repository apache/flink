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
package org.apache.flink.table.planner.plan.batch.table

import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.jupiter.api.Test

/** Test for testing aggregate plans. */
class AggregateTest extends TableTestBase {

  @Test
  def testGroupAggregateWithFilter(): Unit = {

    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val resultTable = sourceTable
      .groupBy('a)
      .select('a, 'a.avg, 'b.sum, 'c.count)
      .where('a === 1)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testAggregate(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val resultTable = sourceTable.select('a.avg, 'b.sum, 'c.count)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testAggregateWithFilter(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)

    val resultTable = sourceTable
      .select('a, 'b, 'c)
      .where('a === 1)
      .select('a.avg, 'b.sum, 'c.count)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testAggregateWithFilterOnNestedFields(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, (Int, Long))]("MyTable", 'a, 'b, 'c)

    val resultTable = sourceTable
      .select('a, 'b, 'c)
      .where('a === 1)
      .select('a.avg, 'b.sum, 'c.count, 'c.get("_1").sum)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testOrderByThenGlobalAggregate(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, String)]("MyTable", 'a, 'b)
    val resultTable = sourceTable.orderBy('b.asc).select('a.max)

    util.verifyRelPlan(resultTable)
  }

  @Test
  def testOrderByWithFetchThenGlobalAggregate(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, String)]("MyTable", 'a, 'b)
    val resultTable = sourceTable.orderBy('b.asc).fetch(10).select('a.max)

    util.verifyRelPlan(resultTable)
  }

  @Test
  def testOrderByExprWithFetchThenGlobalAggregate(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, String)]("MyTable", 'a, 'b)
    val resultTable = sourceTable.orderBy(('a + 1).asc).fetch(10).select('a.max)

    util.verifyRelPlan(resultTable)
  }

  @Test
  def testGroupAggregateWithHaving(): Unit = {
    // Filter on an aggregate result (HAVING semantics).
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, String)]("MyTable", 'a, 'b)
    val resultTable = sourceTable.groupBy('b).select('b, 'a.sum.as("s")).where('s > 3)

    util.verifyRelPlan(resultTable)
  }

}
