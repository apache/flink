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
import org.apache.flink.table.planner.utils.{EmptyTableAggFunc, TableTestBase}

import org.junit.Test

class GroupWindowTableAggregateTest extends TableTestBase {

  val util = streamTestUtil()
  val table = util.addTableSource[(Long, Int, Long, Long)]('a, 'b, 'c, 'd.rowtime, 'e.proctime)
  val emptyFunc = new EmptyTableAggFunc

  @Test
  def testSingleWindow(): Unit = {
    val windowedTable = table
      .window(Tumble over 5.milli on 'd as 'w)
      .groupBy('w, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1 + 1, 'w.start, 'w.end)

    util.verifyExecPlan(windowedTable)
  }

  @Test
  def testMultiWindow(): Unit = {
    val windowedTable = table
      .window(Tumble over 50.milli on 'e as 'w1)
      .groupBy('w1, 'c)
      .flatAggregate(emptyFunc('a, 'b))
      .select('w1.proctime as 'proctime, 'c, 'f0, 'f1 + 1 as 'f1)
      .window(Slide over 20.milli every 10.milli on 'proctime as 'w2)
      .groupBy('w2)
      .flatAggregate(emptyFunc('f0))
      .select('w2.start, 'f1)

    util.verifyExecPlan(windowedTable)
  }

  @Test
  def testTimeMaterializer(): Unit = {
    val windowedTable = table
      .window(Tumble over 5.milli on 'd as 'w)
      .groupBy('w, 'e)
      .flatAggregate(emptyFunc('a, 'b))
      .select('f0, 'f1 + 1, 'w.start, 'w.end)

    util.verifyExecPlan(windowedTable)
  }
}
