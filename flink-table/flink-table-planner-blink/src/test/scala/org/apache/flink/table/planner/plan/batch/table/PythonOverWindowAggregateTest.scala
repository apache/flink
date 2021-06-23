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

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.{PandasAggregateFunction, TestPythonAggregateFunction}
import org.apache.flink.table.planner.utils.TableTestBase
import org.junit.Test

class PythonOverWindowAggregateTest extends TableTestBase {

  @Test
  def testPandasRangeOverWindowAggregate(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int, Long)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val func = new PandasAggregateFunction

    val resultTable = sourceTable
      .window(
        Over
          partitionBy 'b
          orderBy 'rowtime
          preceding UNBOUNDED_RANGE
          as 'w)
      .select('b, func('a, 'c) over 'w)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testPandasRowsOverWindowAggregate(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int, Long)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val func = new PandasAggregateFunction

    val resultTable = sourceTable
      .window(
        Over
          partitionBy 'b
          orderBy 'rowtime
          preceding 10.rows
          as 'w)
      .select('b, func('a, 'c) over 'w)

    util.verifyExecPlan(resultTable)
  }

  @Test(expected = classOf[TableException])
  def testGeneralRangeOverWindowAggregate(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int, Long)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val func = new TestPythonAggregateFunction

    val resultTable = sourceTable
      .window(
        Over
          partitionBy 'b
          orderBy 'rowtime
          preceding UNBOUNDED_RANGE
          as 'w)
      .select('b, func('a, 'c) over 'w)

    util.verifyExecPlan(resultTable)
  }
}
