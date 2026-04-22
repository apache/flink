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
  def testGroupAggregateAfterOrderBy(): Unit = {
    // order_by before a group_by that prunes the sort-key column must not
    // leave a stale collation on the local hash aggregate.
    val util = batchTestUtil()
    val sourceTable = util
      .addTableSource[(Int, Long, Int, String, Double)]("MyTable", 'a, 'b, 'c, 'd, 'e)

    val resultTable = sourceTable
      .orderBy('c)
      .groupBy('d)
      .select('e.count.as('cnt))

    util.verifyExecPlan(resultTable)
  }
}
