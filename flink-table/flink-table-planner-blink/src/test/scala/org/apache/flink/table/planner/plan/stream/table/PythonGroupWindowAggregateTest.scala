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
import org.apache.flink.table.planner.runtime.utils.JavaUserDefinedAggFunctions.PandasAggregateFunction
import org.apache.flink.table.planner.utils.TableTestBase

import org.junit.Test

class PythonGroupWindowAggregateTest extends TableTestBase {

  @Test
  def testPandasEventTimeTumblingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int, Long)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val func = new PandasAggregateFunction

    val resultTable = sourceTable
      .window(Tumble over 5.millis on 'rowtime as 'w)
      .groupBy('w, 'b)
      .select('b, 'w.start,'w.end, func('a, 'c))

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testPandasEventTimeTumblingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)](
      "MyTable", 'a, 'b, 'c, 'proctime.proctime)
    val func = new PandasAggregateFunction

    val resultTable = sourceTable
      .window(Tumble over 2.rows on 'proctime as 'w)
      .groupBy('w, 'b)
      .select('b, func('a, 'c))

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testPandasEventTimeSlidingGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int, Long)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val func = new PandasAggregateFunction

    val resultTable = sourceTable
      .window(Slide over 5.millis every 2.millis on 'rowtime as 'w)
      .groupBy('w, 'b)
      .select('b, 'w.start,'w.end, func('a, 'c))

    util.verifyExecPlan(resultTable)
  }

  @Test
  def testPandasEventTimeSlidingGroupWindowOverCount(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int)](
      "MyTable", 'a, 'b, 'c, 'proctime.proctime)
    val func = new PandasAggregateFunction

    val resultTable = sourceTable
      .window(Slide over 5.rows every 2.rows on 'proctime as 'w)
      .groupBy('w, 'b)
      .select('b, func('a, 'c))

    util.verifyExecPlan(resultTable)
  }

  @Test(expected = classOf[TableException])
  def testPandasEventTimeSessionGroupWindowOverTime(): Unit = {
    val util = streamTestUtil()
    val sourceTable = util.addTableSource[(Int, Long, Int, Long)](
      "MyTable", 'a, 'b, 'c, 'rowtime.rowtime)
    val func = new PandasAggregateFunction

    val windowedTable = sourceTable
      .window(Session withGap 7.millis on 'rowtime as 'w)
      .groupBy('w, 'b)
      .select('b, 'w.start, 'w.end, func('a, 'c))
    util.verifyExecPlan(windowedTable)
  }
}
