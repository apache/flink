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

package org.apache.flink.table.planner.plan.stream.table

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.utils.{PythonEmptyTableAggFunc, TableTestBase}

import org.junit.Test

class PythonTableAggregateTest extends TableTestBase {

  @Test
  def testTableAggregate(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val func = new PythonEmptyTableAggFunc

    val resultTable = sourceTable.groupBy('b)
      .flatAggregate(func('a, 'c) as ('d, 'e))
      .select('d, 'e)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testTableAggregateWithoutKeys(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val func = new PythonEmptyTableAggFunc

    val resultTable = sourceTable.flatAggregate(func('a, 'c) as ('d, 'e)).select('d, 'e)

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testTableAggregateMixedWithJavaCalls(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)]("MyTable", 'a, 'b, 'c)
    val func = new PythonEmptyTableAggFunc

    val resultTable = sourceTable.groupBy('b)
      .flatAggregate(func('a, 'c + 1) as ('d, 'e))
      .select('d, 'e)

    util.verifyExecPlan(resultTable)
  }
}
