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
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.PandasAggregateFunction
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class PythonAggregateTest extends TableTestBase {

  @Test
  def testPandasGroupAggregateWithoutKeys(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val func = new PandasAggregateFunction

    val resultTable = sourceTable.select(func('a, 'c))

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testPandasGroupAggregate(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val func = new PandasAggregateFunction

    val resultTable = sourceTable.groupBy('b)
      .select('b, func('a, 'c))

    util.verifyExecPlan(resultTable)
  }

  @Test(expected = classOf[TableException])
  def testMixedUsePandasAggAndJavaAgg(): Unit = {
    val util = batchTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val func = new PandasAggregateFunction

    val resultTable = sourceTable.groupBy('b)
      .select('b, func('a, 'c), 'a.count())

    util.verifyExecPlan(resultTable)
  }
}
